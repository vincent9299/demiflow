"""demiflow streaming 执行路径：含 AsyncMapOp 的计划的本地流式执行（2026-09-04 新增）。

设计（collect 能力融合一期；拓扑与 collect_v2.chain 同构）：
- 常驻 worker 协程组 + 有界 asyncio.Queue：无序发射（吞吐优先）、
  sentinel 逐级排空、run-to-completion；
- 单事件循环承载全部算子：进程级客户端/限速闸门（如消费方 collect_v2.infra）
  保持单 loop 单例语义，不碎片化；
- 认缺分级：AsyncMapOp.catch 白名单内异常只计数不断链；白名单外异常
  经 watchdog 收敛后终止整链（真异常语义，与 chain 的 annotate/sink 同口径）；
- fn 同步/异步皆可（awaitable 会被等待）；返回 None=认缺丢弃、list=展开；
- FilterOp 折叠：async 算子之间的 filter 变成前级算子的输出后过滤
  （首级之前则是输入前过滤）；其余 sync 算子（limit/select/sort...）
  在 streaming 计划中显式拒绝——流式与惰性两条路径各司其职；
- 取消/中断：Ctrl-C → 停止投喂、finally 执行 on_drain 收尾钩子；
  钩子契约：必须落盘的同步写放钩子最前（await 段在中断路径可能被
  取消截断，进程退出时未关客户端无数据损失风险）；
- 内存上界：每级 队列深度 × 行载荷（载字节级把 queue_depth 收窄到
  该级并发，即 chain 的 q_dl 语义）。
"""

from __future__ import annotations

import asyncio
import contextlib
import inspect
from typing import Any, Callable, Optional

from ..data.plan import AsyncMapOp, FilterOp, LogicalPlan

SENTINEL = object()
_FEED_CHUNK = 256            # 源头同步迭代器的分块拉取粒度（每块一次线程切换）
_WATCHDOG_INTERVAL = 0.2     # 真异常收敛巡检周期（秒）
_DRAIN_TIMEOUT = 2.0         # 取消后等待在飞任务平息的上限（秒）


class StreamStats:
    """流式执行计数（引擎口径；业务口径由管线在算子闭包内自持）。"""

    def __init__(self) -> None:
        self.stages: dict[str, dict[str, int]] = {}
        self.miss: dict[str, int] = {}

    def stage(self, name: str) -> dict[str, int]:
        return self.stages.setdefault(name, {"in": 0, "emitted": 0})

    def add_miss(self, reason: str) -> None:
        self.miss[reason] = self.miss.get(reason, 0) + 1

    @property
    def emitted(self) -> int:
        """末级产出行数（管线最终输出；per-stage 细账看 stage(name)/stages）。"""
        if not self.stages:
            return 0
        return next(reversed(self.stages.values()))["emitted"]

    def summary(self) -> str:
        st = "、".join(f"{k}[in={v['in']}→out={v['emitted']}]"
                      for k, v in self.stages.items())
        miss = "、".join(f"{k}×{v}" for k, v in
                         sorted(self.miss.items(), key=lambda x: -x[1]))
        return st + (f"\n认缺：{miss}" if miss else "")


class _Stage:
    __slots__ = ("name", "fn", "concurrency", "queue_depth", "catch",
                 "pre_filters", "post_filters")

    def __init__(self, name, fn, concurrency, queue_depth, catch,
                 pre_filters, post_filters):
        self.name = name
        self.fn = fn
        self.concurrency = concurrency
        self.queue_depth = queue_depth
        self.catch = catch
        self.pre_filters = pre_filters
        self.post_filters = post_filters


def _materialize(plan: LogicalPlan) -> list[_Stage]:
    """LogicalPlan → streaming 级列表（FilterOp 折叠；其他 sync 算子拒绝）。"""
    stages: list[_Stage] = []
    head_filters: list[Callable] = []
    for op in plan.operations:
        if isinstance(op, AsyncMapOp):
            name = op.label or op.callable.name
            stages.append(_Stage(
                name, op.callable.instantiate(), op.concurrency,
                op.queue_depth or op.concurrency, op.catch,
                head_filters if not stages else [], []))
            head_filters = []
        elif isinstance(op, FilterOp):
            pred = op.callable.instantiate()
            if stages:
                stages[-1].post_filters.append(pred)
            else:
                head_filters.append(pred)
        else:
            raise ValueError(
                f"streaming 路径只支持 map_async 与 filter，"
                f"遇到 {type(op).__name__}（惰性动作走 take/write 系列则不支持本算子组合）")
    if not stages:
        raise ValueError("run_stream 需要至少一个 map_async 算子")
    return stages


async def _call(fn, row):
    out = fn(row)
    if inspect.isawaitable(out):
        out = await out
    return out


def _post_filter(filters, out):
    """输出后过滤：保持 None/list 语义，全滤掉返回 None。"""
    if out is None or not filters:
        return out
    outs = out if isinstance(out, list) else [out]
    kept = [r for r in outs if all(p(r) for p in filters)]
    if not kept:
        return None
    return kept if isinstance(out, list) else kept[0]


def _make_worker(stage: _Stage, q_in: asyncio.Queue, q_out,
                 stats: StreamStats, on_progress, log_every: int):
    name = stage.name

    async def worker():
        st = stats.stage(name)
        while True:
            row = await q_in.get()
            if row is SENTINEL:
                return
            st["in"] += 1
            if any(not p(row) for p in stage.pre_filters):
                stats.add_miss(f"{name}:filtered")
            else:
                try:
                    out = await _call(stage.fn, row)
                except stage.catch as exc:   # noqa: PERF203 - 认缺白名单
                    stats.add_miss(f"{name}:{type(exc).__name__}")
                else:
                    out = _post_filter(stage.post_filters, out)
                    if out is None:
                        stats.add_miss(f"{name}:drop")
                    else:
                        rows = out if isinstance(out, list) else (out,)
                        for r in rows:
                            if q_out is not None:
                                await q_out.put(r)
                            st["emitted"] += 1
            # 进度钩子挂首级消费口径（chain 的「实例进度」同位）
            if (log_every and stage is FIRST_STAGE_REF[0]
                    and st["in"] % log_every == 0 and on_progress is not None):
                await _call(on_progress, stats)

    return worker


FIRST_STAGE_REF: list = [None]   # _arun 内注入；模块级引用避免改 worker 签名


def _local_queue(depth: int):
    """进程内有界队列（默认传输实现；D2 的分布式实现替换此工厂）。"""
    return asyncio.Queue(maxsize=depth)


async def _arun(source_iter, stages, stats, *, on_progress, on_drain,
                log_every, cancellation, queue_factory=None) -> None:
    make_queue = queue_factory or _local_queue
    queues = [make_queue(s.queue_depth) for s in stages]
    FIRST_STAGE_REF[0] = stages[0]

    async def feed():
        it = iter(source_iter)
        while True:
            if cancellation is not None and cancellation.requested:
                break
            chunk = await asyncio.to_thread(
                lambda: [r for _, r in zip(range(_FEED_CHUNK), it)])
            if not chunk:
                break
            for row in chunk:
                await queues[0].put(row)

    stage_tasks: list[list[asyncio.Task]] = []
    for i, s in enumerate(stages):
        q_out = queues[i + 1] if i + 1 < len(stages) else None
        mk = _make_worker(s, queues[i], q_out, stats, on_progress, log_every)
        stage_tasks.append([asyncio.create_task(mk()) for _ in range(s.concurrency)])
    feed_task = asyncio.create_task(feed())
    all_tasks = [feed_task, *(t for g in stage_tasks for t in g)]
    fatal: list[BaseException] = []

    def _cancel_all() -> None:
        for t in all_tasks:
            if not t.done():
                t.cancel()

    async def watchdog():
        """真异常收敛：任一 worker 以白名单外异常退出 → 取消全场并记录。"""
        while True:
            await asyncio.sleep(_WATCHDOG_INTERVAL)
            for t in all_tasks:
                if t.done() and not t.cancelled() and t.exception() is not None:
                    fatal.append(t.exception())
                    _cancel_all()
                    return

    wd = asyncio.create_task(watchdog())
    try:
        with contextlib.suppress(asyncio.CancelledError):
            await feed_task          # 被 watchdog 取消时吞掉，改抛收敛到的真异常
        if fatal:
            raise fatal[0]
        if feed_task.done() and not feed_task.cancelled() \
                and feed_task.exception() is not None:
            raise feed_task.exception()
        # 源头投喂完毕 → sentinel 逐级注入、逐级 join（chain.py 同款收尾）
        for i, group in enumerate(stage_tasks):
            for _ in group:
                await queues[i].put(SENTINEL)
            await asyncio.gather(*group)
    except BaseException:
        _cancel_all()
        with contextlib.suppress(BaseException):
            await asyncio.wait(all_tasks, timeout=_DRAIN_TIMEOUT)
        raise
    finally:
        wd.cancel()
        if on_drain is not None:
            with contextlib.suppress(asyncio.CancelledError):
                await asyncio.shield(_call(on_drain, stats))


def run_stream(source_iter, plan: LogicalPlan, *,
               on_progress=None, on_drain=None, log_every: int = 0,
               cancellation=None, queue_factory=None) -> StreamStats:
    """同步驱动入口：建事件循环跑至完成（或 Ctrl-C/异常终止），返回 StreamStats。

    queue_factory(depth) 是行传输缝（调度层内部，算子/编排零感知）：
    缺省进程内有界队列；分布式实现（如 redis 支撑的跨节点队列）替换
    此工厂即可，行需可序列化、stage 由各 worker 侧自行构造。
    """
    stages = _materialize(plan)
    stats = StreamStats()
    asyncio.run(_arun(source_iter, stages, stats,
                      on_progress=on_progress, on_drain=on_drain,
                      log_every=log_every, cancellation=cancellation,
                      queue_factory=queue_factory))
    return stats


# ---------------------------------------------------------------------------
# 编排串联原语：stage 列表 → 一次跑完（平台管调度与资源收尾）
# ---------------------------------------------------------------------------

def run_stages(ctx, items, stages: list, *,
               concurrency: dict | None = None,
               on_progress=None, on_drain=None, log_every: int = 0,
               cancellation=None, queue_factory=None) -> StreamStats:
    """demiflow 编排入口：stage 列表即管线声明，一步执行到底。

    - stages：StreamStage 规范算子列表（策略默认值在类上声明）；
    - concurrency：{label: (并发, 队列深度|None)} 编排层覆盖（如 CLI 参数）；
    - 退出期（含 Ctrl-C/异常）平台统一收尾资源：LLM 端点客户端 +
      HTTP 双池（on_drain 用户钩子先跑，再关平台资源）；
    - 返回 StreamStats；编排层不再接触 map_stage/run_stream 细节。
    """
    ds = ctx.from_items(list(items))
    for stage in stages:
        if concurrency and stage.label in concurrency:
            stage.concurrency, stage.queue_depth = concurrency[stage.label]
        ds = ds.map_stage(stage)
    try:
        return ds.run_stream(on_progress=on_progress, on_drain=on_drain,
                             log_every=log_every, cancellation=cancellation,
                             queue_factory=queue_factory)
    finally:
        # 退出期统一收尾：算子生命周期钩子（aclose，如持浏览器的抓取算子）
        # → 平台资源（LLM 端点 + HTTP 双池）。KI 路径绑定旧 loop 的资源
        # 由进程退出回收，此处 best-effort。
        import contextlib
        _close_stages(stages)
        with contextlib.suppress(Exception):
            import asyncio as _a
            _a.run(_close_platform())
        from ..collect import llm as _llm, net as _net
        _llm._ENDPOINT_CLIENTS.clear()
        _net._client_direct = _net._client_proxy = None
        _net._dl_client_direct = _net._dl_client_proxy = None
        _net._gates.clear()   # 闸门缓存含 loop 绑定原语，与池同生命周期
                              # （配额循环多轮各自事件循环，跨轮复用会炸）


def _close_stages(stages: list) -> None:
    """规范算子可选 aclose() 钩子（同步/异步皆可，best-effort）。"""
    import contextlib
    import inspect

    async def _all():
        for st in stages:
            fn = getattr(st, "aclose", None)
            if fn is None:
                continue
            r = fn()
            if inspect.isawaitable(r):
                await r

    with contextlib.suppress(Exception):
        import asyncio
        asyncio.run(_all())


async def _close_platform():
    from ..collect import llm as _llm, net as _net
    await _llm.close_all_llm()
    await _net.close_client()
