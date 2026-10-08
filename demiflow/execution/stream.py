"""demiflow 本地流式执行：async/batch 算子及显式流式 map 策略。

设计（collect 能力融合一期；拓扑与 collect_v2.chain 同构）：
- 常驻 worker 协程组 + 有界 asyncio.Queue：无序发射（吞吐优先）、
  sentinel 逐级排空、run-to-completion；
- 单事件循环承载全部算子：进程级客户端/限速闸门（如消费方 collect_v2.infra）
  保持单 loop 单例语义，不碎片化；
- 认缺分级：AsyncMapOp.catch 白名单内异常只计数不断链；白名单外异常
  经 watchdog 收敛后终止整链（真异常语义，与 chain 的 annotate/sink 同口径）；
- async 算子接收同步/异步 fn；显式流式 map 只接收同步 fn；
  返回 None=认缺丢弃、list=展开；
- FilterOp 折叠：async 算子之间的 filter 变成前级算子的输出后过滤
  （首级之前则是输入前过滤）；其余 sync 算子（limit/select/sort...）
  在 streaming 计划中显式拒绝——流式与惰性两条路径各司其职；
- 取消/中断：Ctrl-C → 停止投喂、finally 执行 on_drain 收尾钩子；
  钩子契约：必须落盘的同步写放钩子最前（await 段在中断路径可能被
  取消截断，进程退出时未关客户端无数据损失风险）；
- 队列和在途调用按行数有界，不能据此承诺字节或 RSS 上限。峰值还包括
  reader 预读、源块、每个 worker 的输入/输出和临时对象、writer 缓冲。
  具体所有权与限制见 docs/streaming-map.md。
"""

from __future__ import annotations

import asyncio
import contextlib
import contextvars
import inspect
import time
import traceback
from concurrent.futures import ThreadPoolExecutor
from typing import Any, Callable, Optional

from ..data.plan import AsyncMapOp, BatchMapOp, BoundMapOp, FilterOp, MapOp, FlatMapOp, LogicalPlan, StandardCallable, StreamGroupBatchesOp, StreamPrependOp, is_stream_operation
from .stream_map import StreamMapRuntime

from ..errors import StallError

SENTINEL = object()
_FEED_CHUNK = 256            # 源头同步迭代器的分块拉取粒度（每块一次线程切换）
_WATCHDOG_INTERVAL = 0.2     # 真异常收敛巡检周期（秒）
_DRAIN_TIMEOUT = 2.0         # 取消后等待在飞任务平息的上限（秒）



class StreamStats:
    """流式执行计数（引擎口径；业务口径由管线在算子闭包内自持）。"""

    def __init__(self) -> None:
        self.stages: dict[str, dict[str, int]] = {}
        self.timings = {}
        self.metrics = {}
        self.stage_policies = {}
        self.outputs = {}
        self.miss: dict[str, int] = {}
        self.dead_batches: list = []      # 攒批整批失败内容（活性层，有界）
        self.dead_batches_dropped: int = 0  # 超 1000 批后的只计数不留存

    def stage(self, name: str) -> dict[str, int]:
        return self.stages.setdefault(name, {"in": 0, "emitted": 0})

    def add_miss(self, reason: str) -> None:
        self.miss[reason] = self.miss.get(reason, 0) + 1

    def observe_duration(self, name, seconds):
        from .request_limits import LatencySummary
        if name not in self.timings:
            self.timings[name] = LatencySummary()
        self.timings[name].observe(seconds)

    def timing_summary(self):
        return {name: value.summary() for name, value in self.timings.items()}

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
                 "pre_filters", "post_filters", "hard_timeout",
                 "max_batch", "flush_interval", "execution", "executor", "pending", "flat_map", "grouping", "prefix")

    def __init__(self, name, fn, concurrency, queue_depth, catch,
                 pre_filters, post_filters, hard_timeout=None,
                 max_batch=None, flush_interval=None, execution='inline', flat_map=False):
        self.name = name
        self.fn = fn
        self.concurrency = concurrency
        self.queue_depth = queue_depth
        self.catch = catch
        self.pre_filters = pre_filters
        self.post_filters = post_filters
        self.hard_timeout = hard_timeout
        self.max_batch = max_batch          # 非 None 即攒批级（BatchMapOp）
        self.flush_interval = flush_interval
        self.execution = execution
        self.executor = None
        self.pending = set()
        self.flat_map=flat_map
        self.grouping = None
        self.prefix = None


def _materialize(plan: LogicalPlan) -> list[_Stage]:
    """Lower async actors and their row-wise synchronous maps to bounded stages."""
    if not any(is_stream_operation(op) for op in plan.operations):
        raise ValueError('run_stream requires map_async/batch_map or explicit map streaming options')
    # Validate the whole graph before instantiating any user callable. A map
    # policy must never be silently ignored on an unsupported lowering path.
    for op in plan.operations:
        if not isinstance(op, (AsyncMapOp, BatchMapOp, FilterOp, MapOp, FlatMapOp, BoundMapOp)):
            raise ValueError(f'streaming does not support {type(op).__name__}; materialize the synchronous prefix first')
        if getattr(op, 'native_options', None) is not None:
            raise ValueError('streaming does not support backend_options')
        if isinstance(op, BoundMapOp) and op.stream_options is None:
            raise ValueError('field-bound streaming map requires explicit streaming options')
        if isinstance(op, (MapOp, BoundMapOp)) and op.stream_options is not None:
            op.stream_options.validate_callable(op.callable)
    stages: list[_Stage] = []
    head_filters: list[Callable] = []
    for op in plan.operations:
        if isinstance(op, AsyncMapOp):
            name = op.label or op.callable.name
            stages.append(_Stage(
                name, op.callable.instantiate(), op.concurrency,
                op.queue_depth or op.concurrency, op.catch,
                head_filters if not stages else [], [],
                op.hard_timeout, execution=op.execution))
            if isinstance(op, StreamPrependOp):
                stages[-1].prefix = op
            head_filters = []
        elif isinstance(op, BatchMapOp):
            name = op.label or op.callable.name
            stages.append(_Stage(
                name, op.callable.instantiate(), op.concurrency,
                op.queue_depth or op.concurrency, op.catch,
                head_filters if not stages else [], [],
                op.hard_timeout, op.max_batch, op.flush_interval))
            if isinstance(op, StreamGroupBatchesOp):
                stages[-1].grouping = op
            head_filters = []
        elif isinstance(op, FilterOp):
            pred = op.callable.instantiate()
            if stages:
                stages[-1].post_filters.append(pred)
            else:
                head_filters.append(pred)
        elif isinstance(op, (MapOp, BoundMapOp)) and op.stream_options is not None:
            options = op.stream_options
            stages.append(_Stage(options.label or op.callable.name, StreamMapRuntime(op),
                                 options.concurrency, options.queue_depth or options.concurrency,
                                 options.catch, head_filters if not stages else [], [],
                                 execution=options.execution))
            head_filters = []
        elif isinstance(op, (MapOp,FlatMapOp)):
            # Pure row validation/application can follow a native prompt actor;
            # callers should not have to disguise it as asynchronous work.
            stages.append(_Stage(op.callable.name, StandardCallable(op.callable), 1, 1, (),
                                 head_filters if not stages else [], [],flat_map=isinstance(op,FlatMapOp)))
            head_filters = []
        else:
            raise ValueError(
                f"streaming 路径支持 map_async、batch_map、map、flat_map 与 filter，"
                f"遇到 {type(op).__name__}（惰性动作走 take/write 系列则不支持本算子组合）")
    if not stages:
        raise ValueError("run_stream 需要至少一个 map_async 算子")
    # Repeated functions/prompts are distinct nodes. Collapsing their names
    # merges counts and incorrectly pairs per-node queues with statistics.
    used = set()
    for stage in stages:
        original, index = stage.name, 1
        while stage.name in used:
            index += 1
            stage.name = f'{original}#{index}'
        used.add(stage.name)
    return stages


async def _call(fn, row):
    out = fn(row)
    if inspect.isawaitable(out):
        out = await out
    return out


def _call_sync(fn, row):
    out = fn(row)
    if inspect.isawaitable(out):
        if inspect.iscoroutine(out):
            out.close()
        raise TypeError('thread execution callable returned an awaitable')
    return out


async def _call_stage(stage, row, fn=None):
    fn = stage.fn if fn is None else fn
    if stage.execution == 'inline':
        return await _call(fn, row)
    if stage.executor is None:
        # Per-stage resources keep blocked image reads away from journal I/O,
        # the source feeder and other stages. Workers bound submissions.
        stage.executor = ThreadPoolExecutor(
            max_workers=stage.concurrency, thread_name_prefix='demiflow-stream-' + stage.name)
    context = contextvars.copy_context()
    future = asyncio.get_running_loop().run_in_executor(
        stage.executor, context.run, _call_sync, fn, row)
    stage.pending.add(future)
    def completed(done):
        stage.pending.discard(done)
        # A cancelled worker may no longer await a late failure. Retrieve it
        # without changing what the original waiter observes.
        if not done.cancelled():
            done.exception()
    future.add_done_callback(completed)
    # Cancelling a coroutine must not claim the underlying thread has stopped.
    return await asyncio.shield(future)


async def _drain_threads(stages):
    for stage in stages:
        if stage.executor is not None:
            try:
                await asyncio.gather(*tuple(stage.pending), return_exceptions=True)
            finally:
                stage.executor.shutdown(wait=True, cancel_futures=True)
                stage.executor = None


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
    map_worker = stage.fn.new_worker() if isinstance(stage.fn, StreamMapRuntime) else None

    async def invoke(value, *, batch=False):
        started = time.monotonic()
        try:
            call = _call(stage.fn, value) if batch else _call_stage(stage, value, map_worker)
            return (await asyncio.wait_for(call, stage.hard_timeout)
                    if stage.hard_timeout is not None else await call)
        finally:
            # Service/gate time belongs to the operator; input queue waiting,
            # batch collection and downstream backpressure do not.
            stats.observe_duration(name, time.monotonic() - started)

    async def worker():
        st = stats.stage(name)
        if stage.prefix is not None:
            from .stream_grouping import row_size
            op = stage.prefix
            prefix = iter(op.source_dataset.iter_rows())
            reader_context = contextvars.copy_context()
            # next(default) avoids propagating StopIteration through a Future.
            def advance():
                return next(prefix, SENTINEL)
            async def read_one():
                task = asyncio.create_task(asyncio.to_thread(reader_context.run, advance))
                try:
                    return await asyncio.shield(task)
                except asyncio.CancelledError:
                    await task
                    raise
            try:
                count = 0
                while True:
                    row = await read_one()
                    if row is SENTINEL:
                        break
                    if count >= op.max_rows:
                        raise ValueError('prepend source exceeds max_rows')
                    row_size(row, op.max_row_bytes)
                    count += 1
                    await _emit(row, st)
            finally:
                close = getattr(prefix, 'close', None)
                if close is not None:
                    await asyncio.to_thread(reader_context.run, close)
        if stage.max_batch is not None:
            return await batch_worker(st)   # 攒批级：走批循环
        while True:
            row = await q_in.get()
            if row is SENTINEL:
                return
            st["in"] += 1
            if any(not p(row) for p in stage.pre_filters):
                stats.add_miss(f"{name}:filtered")
            else:
                if map_worker is not None:
                    # Constructor/start failures are lifecycle failures, never
                    # row misses even if catch includes Exception.
                    await map_worker.prepare()
                try:
                    out = await invoke(row)
                except stage.catch as exc:   # noqa: PERF203 - 认缺白名单
                    stats.add_miss(f"{name}:{type(exc).__name__}")
                except asyncio.TimeoutError:
                    if map_worker is not None:
                        # Synchronous map has no enforceable hard timeout.
                        # A callback TimeoutError follows its declared catch.
                        raise
                    stats.add_miss(f"{name}:hard_timeout")   # 活性层：必醒
                else:
                    if stage.flat_map:
                        # Pull one child at a time under downstream backpressure;
                        # never turn an arbitrary expansion iterator into a list.
                        children=iter(out)
                        try:
                            for index,child in enumerate(children):
                                if index and index%256==0:await asyncio.sleep(0)
                                if not all(p(child) for p in stage.post_filters):
                                    stats.add_miss(f"{name}:filtered")
                                    continue
                                if q_out is not None:await q_out.put(child)
                                st["emitted"]+=1
                        finally:
                            close=getattr(children,'close',None)
                            if close is not None:close()
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
            if (log_every and stage is FIRST_STAGE_REF.get()
                    and st["in"] % log_every == 0 and on_progress is not None):
                await _call(on_progress, stats)

    async def _emit(out, st):
        """批输出的展开/后滤/下发（与逐行级同语义）。"""
        if out is None:
            return
        outs = out if isinstance(out, list) else [out]
        for r in outs:
            if not all(p(r) for p in stage.post_filters):
                continue
            if q_out is not None:
                await q_out.put(r)
            st["emitted"] += 1

    def _record_dead(batch, reason):
        if len(stats.dead_batches) < 1000:
            stats.dead_batches.append(
                {"stage": name, "reason": reason, "rows": batch})
        else:
            stats.dead_batches_dropped += 1

    async def batch_worker(st):
        """攒批级 worker：条数满/时间窗/EOF 三触发，fn(list)->list。"""
        if stage.grouping is not None:
            await grouped_worker(st)
            return
        loop = asyncio.get_running_loop()
        while True:
            batch, deadline = [], None
            while True:
                timeout = None
                if batch and stage.flush_interval is not None:
                    timeout = deadline - loop.time()
                    if timeout <= 0:
                        break                        # 时间窗到，刷出
                try:
                    row = await asyncio.wait_for(
                        q_in.get(), None if timeout is None else timeout)
                except asyncio.TimeoutError:
                    break                            # 时间窗到，刷出
                if row is SENTINEL:
                    if batch:
                        await _flush(batch)
                    return                           # 尾批已刷，退役
                if not batch:
                    if stage.flush_interval is not None:
                        deadline = loop.time() + stage.flush_interval
                batch.append(row)
                st["in"] += 1
                if (log_every and stage is FIRST_STAGE_REF.get()
                        and st["in"] % log_every == 0
                        and on_progress is not None):
                    await _call(on_progress, stats)
                if len(batch) >= stage.max_batch:
                    break                            # 条数满，刷出
            if batch:
                await _flush(batch)

    async def grouped_worker(st):
        from .stream_grouping import group_key, row_size
        op, groups, retained = stage.grouping, {}, 0
        loop = asyncio.get_running_loop()

        async def flush(key):
            nonlocal retained
            rows, size, deadline = groups.pop(key)
            retained -= size
            await _flush(rows)

        while True:
            now = loop.time()
            for key in [k for k, (_, _, deadline) in groups.items() if deadline <= now]:
                await flush(key)
            timeout = max(0., min(g[2] for g in groups.values()) - loop.time()) if groups else None
            try:
                row = await asyncio.wait_for(q_in.get(), timeout)
            except asyncio.TimeoutError:
                continue
            if row is SENTINEL:
                for key in list(groups):
                    await flush(key)
                return
            st['in'] += 1
            if any(not predicate(row) for predicate in stage.pre_filters):
                stats.add_miss(f'{name}:filtered')
                continue
            size = row_size(row, op.chunk_bytes)
            key = group_key(row, op.on)
            if key in groups and groups[key][1] + size > op.chunk_bytes:
                await flush(key)
            while groups and (retained + size > op.buffer_bytes or
                              key not in groups and len(groups) >= op.max_groups):
                await flush(next(iter(groups)))
            if key not in groups:
                groups[key] = [[], 0, loop.time() + stage.flush_interval]
            groups[key][0].append(row); groups[key][1] += size; retained += size
            if len(groups[key][0]) >= stage.max_batch:
                await flush(key)

    async def _flush(batch):
        st = stats.stage(name)
        try:
            out = await invoke(batch, batch=True)
        except stage.catch as exc:
            stats.add_miss(f"{name}:{type(exc).__name__}")
            _record_dead(batch, type(exc).__name__)
        except asyncio.TimeoutError:
            stats.add_miss(f"{name}:hard_timeout")
            _record_dead(batch, "hard_timeout")
        else:
            if out is None:
                stats.add_miss(f"{name}:drop")
                _record_dead(batch, "drop")
            else:
                await _emit(out, st)

    return worker


FIRST_STAGE_REF = contextvars.ContextVar('stream_first_stage', default=None)


def _local_queue(depth: int):
    """进程内有界队列（默认传输实现；D2 的分布式实现替换此工厂）。"""
    return asyncio.Queue(maxsize=depth)


async def _arun(source_iter, stages, stats, *, on_progress, on_drain,
                log_every, cancellation, queue_factory=None,
                stall_timeout=None, stop_file=None, source_batch_size=_FEED_CHUNK) -> None:
    from pathlib import Path
    from .request_limits import ServiceStopped
    stop_path = Path(stop_file) if stop_file is not None else None
    if stop_path is not None and await asyncio.to_thread(stop_path.exists):
        raise ServiceStopped('operator_stop_file')
    make_queue = queue_factory or _local_queue
    queues = [make_queue(s.queue_depth) for s in stages]
    for stage in stages:
        stats.stage(stage.name)
        stats.stage_policies[stage.name] = {
            'concurrency': stage.concurrency, 'queue_depth': stage.queue_depth,
            'execution': stage.execution,
            'callable_scope': (stage.fn.operation.stream_options.callable_scope
                               if isinstance(stage.fn, StreamMapRuntime) else 'stage'),
            'catch': [f'{exc.__module__}.{exc.__qualname__}' for exc in stage.catch],
        }
    FIRST_STAGE_REF.set(stages[0])
    # An opt-in astop hook releases execution-owned resources when the actor's
    # last node finishes, before unrelated downstream work drains. Shared actor
    # objects may occur at several nodes; never stop one after its first node.
    from collections import Counter
    owners = {id(stage): getattr(stage.fn, '__self__', stage.fn) for stage in stages}
    remaining = Counter(id(owner) for owner in owners.values())
    stopped = set()

    async def stop_stage(stage):
        key = id(stage)
        if key in stopped:
            return
        stopped.add(key)
        owner = owners[key]
        remaining[id(owner)] -= 1
        stop = getattr(owner, 'astop', None)
        if remaining[id(owner)] == 0 and stop is not None:
            result = stop()
            if inspect.isawaitable(result):
                await result

    async def feed():
        it = iter(source_iter)
        # Resume a generator in the same Context across worker threads.
        feed_context = contextvars.copy_context()
        while True:
            if cancellation is not None and cancellation.requested:
                break
            chunk = await asyncio.to_thread(
                feed_context.run, lambda: [r for _, r in zip(range(source_batch_size), it)])
            if not chunk:
                break
            for row in chunk:
                await queues[0].put(row)
        # Durable operator work is replayed after the live source is exhausted.
        # This keeps recovery independent of upstream re-feeding: each stage
        # may expose opaque recovery_rows(), while the platform remains unaware
        # of row schemas or business rules.
        for index, stage in enumerate(stages):
            owner = getattr(stage.fn, '__self__', stage.fn)
            recover = getattr(owner, 'recovery_rows', None)
            if recover is None:
                continue
            rows = recover()
            if inspect.isawaitable(rows):
                rows = await rows
            for row in rows or ():
                if stage.max_batch is not None and isinstance(row, list):
                    for item in row:
                        await queues[index].put(item)
                else:
                    await queues[index].put(row)

    stage_tasks: list[list[asyncio.Task]] = []
    for i, s in enumerate(stages):
        q_out = queues[i + 1] if i + 1 < len(stages) else None
        stage_tasks.append([asyncio.create_task(
            _make_worker(s, queues[i], q_out, stats, on_progress, log_every)())
            for _ in range(s.concurrency)])
    feed_task = asyncio.create_task(feed())
    all_tasks = [feed_task, *(t for g in stage_tasks for t in g)]
    # Watchdog reports the first failure, but several workers may fail together
    # (e.g. a shared budget). Observe every completed task, including late errors
    # after cancellation; retrieving does not suppress propagation below.
    def observe_completion(task):
        if not task.cancelled():
            task.exception()
    for task in all_tasks:
        task.add_done_callback(observe_completion)
    fatal: list[BaseException] = []

    def service_stop():
        from .request_limits import ServiceStopped
        if not fatal:
            return None
        if isinstance(fatal[0], ServiceStopped):
            return fatal[0]
        # A failed sibling (including local storage) must stop scheduling, but
        # must not abandon an already paid model exchange. Preserve the actual
        # fatal exception for the caller; this reason is only a drain signal.
        return ServiceStopped('pipeline_failed:' + type(fatal[0]).__name__)

    def _cancel_all() -> None:
        for t in all_tasks:
            if not t.done() and not t.cancelling():
                # Stop row scheduling immediately. Native journaled model
                # requests recognize this reason and finish their existing
                # bounded exchange before relinquishing the cancelled worker.
                t.cancel(service_stop())

    async def watchdog():
        """真异常收敛 + 零吞吐停摆检测（活性层，2026-09-14）。

        停摆口径：全局进度（各级 in+emitted 之和）在 stall_timeout 秒内
        零变化 → StallError，message 携带挂起任务栈转储与 net 闸门超龄
        持有诊断。历史四次夜跑静默凝固皆属此类（沿革见 errors.StallError）。
        """
        last_progress, last_tick = -1, time.monotonic()
        while True:
            await asyncio.sleep(_WATCHDOG_INTERVAL)
            if stop_path is not None and await asyncio.to_thread(stop_path.exists):
                fatal.append(ServiceStopped('operator_stop_file'))
                _cancel_all()
                return
            for t in all_tasks:
                if t.cancelled():
                    fatal.append(asyncio.CancelledError('stream worker cancelled'))
                    _cancel_all()
                    return
                if t.done() and not t.cancelled() and t.exception() is not None:
                    fatal.append(t.exception())
                    _cancel_all()
                    return
            if stall_timeout is None:
                continue
            progress = sum(v["in"] + v["emitted"]
                           for v in stats.stages.values())
            now = time.monotonic()
            if progress != last_progress:
                last_progress, last_tick = progress, now
                continue
            if now - last_tick < stall_timeout:
                continue
            fatal.append(StallError(_stall_dump(all_tasks)))
            _cancel_all()
            return

    def _stall_dump(tasks) -> str:
        """停摆现场：挂起任务栈 + net 闸门持有诊断（活性层取证包）。"""
        parts = ["pipeline stalled: task stacks follows"]
        for t in tasks:
            if t.done():
                continue
            frames = t.get_stack(limit=6)
            if not frames:
                continue
            # 纯属性拼行：format_stack 的 linecache 在循环内对挂起帧
            # 会挂死（实测），file:line:func 足够取证
            loc = " <- ".join(
                f"{f.f_code.co_filename.rsplit('/', 1)[-1]}:"
                f"{f.f_lineno}:{f.f_code.co_name}" for f in frames)
            parts.append(f"--- {t.get_name()}: {loc}")
        try:
            _net = __import__("sys").modules.get("demiflow.collect.net")
            # 事件循环内 import 会死锁(实测),故只取已加载引用——
            # 真实采集管线 net 必已常驻,诊断可用;纯流式用例静默跳过
            if _net is not None:
                stale = _net.gate_stalls()
                if stale:
                    parts.append("net gates held: " + repr(stale))
        except Exception:
            pass
        return "\n".join(parts)[:20000]

    wd = asyncio.create_task(watchdog())
    try:
        try:
            await feed_task
        except asyncio.CancelledError:
            # Watchdog cancellation reports its cause; external cancellation
            # must propagate so callers cannot commit a partial materialization.
            if fatal:
                raise fatal[0]
            raise
        if fatal:
            raise fatal[0]
        if feed_task.done() and not feed_task.cancelled() \
                and feed_task.exception() is not None:
            raise feed_task.exception()
        # 源头投喂完毕 → sentinel 逐级注入、逐级 join（chain.py 同款收尾）。
        # 活性层（2026-09-14）：drain 的 sentinel put 可能因工人被看门狗
        # 取消而永久阻塞（满队列无消费者）——put/gather 均走有界等待，
        # 超时且 fatal 非空时立即上抛（原引擎潜伏死角，流式停摆场景首证）。
        async def _abort_check():
            if fatal:
                raise fatal[0]

        for i, group in enumerate(stage_tasks):
            for _ in group:
                while True:
                    try:
                        await asyncio.wait_for(queues[i].put(SENTINEL), 2)
                        break
                    except asyncio.TimeoutError:
                        await _abort_check()
            while True:
                done, _ = await asyncio.wait(set(group), timeout=2)
                if len(done) == len(group):
                    break
                await _abort_check()
            # A stop may cancel every coroutine while their thread calls are
            # still draining. Cancelled tasks have no exception(), so checking
            # only task exceptions would turn a stopped action into success.
            await _abort_check()
            ex = next((t.exception() for t in group
                       if not t.cancelled() and t.exception() is not None), None)
            if ex is not None:
                raise ex
            if any(t.cancelled() for t in group):
                raise asyncio.CancelledError('stream worker cancelled')
            await stop_stage(stages[i])
    except BaseException as exc:
        from .request_limits import ServiceStopped
        if isinstance(exc, Exception) and not fatal:
            fatal.append(exc)
        _cancel_all()
        with contextlib.suppress(BaseException):
            timeout = _DRAIN_TIMEOUT
            if service_stop():
                # Materialized actor stages expose a bound __call__ method;
                # the bounded shutdown contract belongs to its owner.
                timeout = max([timeout] + [
                    getattr(getattr(stage.fn, '__self__', stage.fn),
                            'cancel_drain_timeout_s', 0)
                    for stage in stages
                ])
            _, pending = await asyncio.wait(all_tasks, timeout=timeout)
            if pending:
                for task in pending:
                    task.cancel()  # Hard deadline: ordinary cancellation.
                await asyncio.wait(pending, timeout=_DRAIN_TIMEOUT)
        raise
    finally:
        wd.cancel()
        drain = asyncio.create_task(_drain_threads(stages))
        try:
            await asyncio.shield(drain)
        except asyncio.CancelledError:
            await drain
            raise
        # Cancellation/failure also releases every started network actor. The
        # action-level aclose remains an idempotent fallback for partial start.
        stop_errors = []
        for stage in stages:
            try:
                await stop_stage(stage)
            except Exception as exc:
                stop_errors.append(exc)
        if on_drain is not None:
            with contextlib.suppress(asyncio.CancelledError):
                await asyncio.shield(_call(on_drain, stats))
        if stop_errors:
            raise ExceptionGroup('Stream actor stop failed', stop_errors)


def run_stream(source_iter, plan: LogicalPlan, *,
               on_progress=None, on_drain=None, log_every: int = 0,
               cancellation=None, queue_factory=None, on_start=None, on_close=None, stall_timeout=None,
               stop_file=None, source_batch_size=_FEED_CHUNK) -> StreamStats:
    """同步驱动入口：建事件循环跑至完成（或 Ctrl-C/异常终止），返回 StreamStats。

    queue_factory(depth) 是行传输缝（调度层内部，算子/编排零感知）：
    缺省进程内有界队列；分布式实现（如 redis 支撑的跨节点队列）替换
    此工厂即可，行需可序列化、stage 由各 worker 侧自行构造。
    """
    stats = StreamStats()

    async def execute():
        drained = False
        stages = []
        async def drain(result):
            nonlocal drained
            drained = True
            if on_drain is not None: await _call(on_drain, result)
        try:
            stages = _materialize(plan)
            if on_start is not None:
                result = on_start()
                if inspect.isawaitable(result): await result
            await _arun(source_iter, stages, stats,
                        on_progress=on_progress, on_drain=drain,
                        log_every=log_every, cancellation=cancellation,
                        queue_factory=queue_factory, stall_timeout=stall_timeout, stop_file=stop_file,
                        source_batch_size=source_batch_size)
        finally:
            try:
                if not drained: await drain(stats)
            finally:
                try:
                    errors = []
                    for stage in stages:
                        if isinstance(stage.fn, StreamMapRuntime):
                            try:
                                await stage.fn.aclose()
                            except Exception as exc:
                                errors.append(exc)
                    if errors:
                        raise ExceptionGroup('Stream map cleanup failed', errors)
                finally:
                    if on_close is not None:
                        result = on_close()
                        if inspect.isawaitable(result): await result

    asyncio.run(execute())
    return stats


# run_stages（stage 列表便捷入口）已于 2026-09-07 移除：收尾语义下沉到
# Dataset.run_stream 后，链式声明（from_items().map_async()×N.run_stream()）
# 是唯一编排形态——历史沿革：09-05 作为便捷入口引入，收敛期回归原始
# Dataset API 风格后退役。


def _close_stages(stages: list) -> None:
    """规范算子可选 aclose() 钩子（同步/异步皆可，best-effort）。

    2026-09-07：收尾逻辑下沉到 Dataset.run_stream finally 后，本函数保留
    供外部工具（冒烟脚本等）显式收尾使用。
    """


async def _close_platform():
    from .resource_registry import stream_cleanups
    for callback in stream_cleanups():
        result=callback()
        if inspect.isawaitable(result): await result
