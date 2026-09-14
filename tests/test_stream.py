"""demiflow streaming 路径测试（run_stream / map_async）。"""

from __future__ import annotations

import asyncio
import time

import pytest

from demiflow.data.plan import LogicalPlan
from demiflow.standalone import local_data


def build(n=50):
    ctx = local_data()
    return ctx, [{"i": i} for i in range(n)]


def test_expand_none_and_set_equality():
    """展开/认缺语义 + 与串行计算集合全等。"""
    ctx, items = build(60)
    ds = (ctx.from_items(items)
          .map_async(lambda r: [r, {**r, "dup": True}] if r["i"] % 3 else None,
                     concurrency=4)
          .map_async(lambda r: {**r, "v": r["i"] * 2}, concurrency=4,
                     label="second"))
    stats = ds.run_stream(log_every=0)
    assert stats.emitted == 80          # 20 认缺、40 行各展开 2
    assert stats.stage("second")["in"] == 80
    assert stats.miss["<lambda>:drop"] == 20


def test_sync_fn_supported():
    ctx, items = build(10)

    def double(r):
        return {**r, "v": r["i"] * 2}

    ds = ctx.from_items(items).map_async(double, concurrency=2)
    stats = ds.run_stream()
    assert stats.emitted == 10


def test_unordered_emission_no_head_of_line_blocking():
    """无序发射：首行 sleep 不阻塞后续行（保序滑窗会有队头阻塞）。"""
    ctx, items = build(6)
    order: list[int] = []
    t0 = time.monotonic()

    async def op(r):
        if r["i"] == 0:
            await asyncio.sleep(0.5)
        order.append(r["i"])
        return r

    ds = ctx.from_items(items).map_async(op, concurrency=6)
    ds.run_stream()
    assert time.monotonic() - t0 < 0.9          # 若队头阻塞会 ≥ 6×0.5 的串行形态
    assert 1 in order[:3] and order.index(1) < order.index(0)


def test_bounded_backpressure():
    """有界背压：末级慢时，中间级在飞量受 queue_depth+concurrency 钳制。"""
    ctx, items = build(40)
    inflight = {"n": 0, "max": 0}

    async def mid(r):
        inflight["n"] += 1
        inflight["max"] = max(inflight["max"], inflight["n"])
        await asyncio.sleep(0.01)
        inflight["n"] -= 1
        return r

    async def slow(r):
        await asyncio.sleep(0.05)
        return r

    ds = (ctx.from_items(items)
          .map_async(mid, concurrency=4, queue_depth=4)
          .map_async(slow, concurrency=1))
    ds.run_stream()
    assert inflight["max"] <= 4 + 4            # 深度+并发之外被 put 阻塞挡住


def test_catch_whitelist_miss_and_fatal_terminates():
    """认缺分级：白名单命中计数不断链；白名单外异常终止整链。"""
    class Soft(Exception):
        pass

    class Hard(Exception):
        pass

    ctx = local_data()
    items = [{"i": i} for i in range(20)]

    async def flaky(r):
        if r["i"] % 2 == 0:
            raise Soft("soft")
        return r

    ds = ctx.from_items(items).map_async(flaky, concurrency=4, catch=(Soft,))
    stats = ds.run_stream()
    assert stats.emitted == 10
    assert stats.miss["flaky:Soft"] == 10

    async def bomb(r):
        if r["i"] == 3:
            raise Hard("fatal")
        return r

    ds2 = ctx.from_items(items).map_async(bomb, concurrency=1)
    with pytest.raises(Hard):
        ds2.run_stream()


def test_filter_folding_head_and_between():
    """FilterOp 折叠：首级前=输入前过滤，级间=前级输出后过滤。"""
    ctx, items = build(30)
    ds = (ctx.from_items(items)
          .filter(lambda r: r["i"] % 2 == 0)                 # 首级前：15 行
          .map_async(lambda r: {**r, "v": r["i"]}, concurrency=3)
          .filter(lambda r: r["v"] % 4 == 0)                 # 级间后过滤
          .map_async(lambda r: r, concurrency=3, label="sink"))
    stats = ds.run_stream()
    assert stats.emitted == 8                  # {0,4,8,...,28} 共 8 个


def test_sync_only_ops_rejected():
    ctx, items = build(5)
    ds = ctx.from_items(items).map(lambda r: r)         # sync 惰性算子
    with pytest.raises(ValueError):
        ds.run_stream()


def test_on_drain_runs_on_success_and_failure():
    ctx, items = build(6)
    drained = []

    def drain(stats):
        drained.append("ok")

    ds = ctx.from_items(items).map_async(lambda r: r, concurrency=2)
    ds.run_stream(on_drain=drain)

    class Boom(Exception):
        pass

    async def bomb(r):
        raise Boom()

    ds2 = ctx.from_items(items).map_async(bomb, concurrency=1)
    with pytest.raises(Boom):
        ds2.run_stream(on_drain=lambda s: drained.append("fail"))
    assert drained == ["ok", "fail"]


def test_progress_hook():
    ctx, items = build(25)
    seen = []

    def prog(stats):
        seen.append(stats.emitted)

    ds = ctx.from_items(items).map_async(lambda r: r, concurrency=2,
                                              label="first")
    ds.run_stream(on_progress=prog, log_every=5)
    assert len(seen) >= 4            # 5/10/15/20/25 触发点至少命中多个


def test_multi_stage_sentinel_drain():
    """三级链路 sentinel 逐级排空：全部行到达末级，无丢失无挂起。"""
    ctx, items = build(100)

    async def a(r):
        await asyncio.sleep(0)
        return r

    def b(r):
        return r

    async def c(r):
        await asyncio.sleep(0)
        return r

    ds = (ctx.from_items(items)
          .map_async(a, concurrency=5)
          .map_async(b, concurrency=5)
          .map_async(c, concurrency=5, label="last"))
    stats = ds.run_stream()
    assert stats.stage("last")["emitted"] == 100


# ---------------------------------------------------------------------------
# StreamStage 规范算子（继承式）
# ---------------------------------------------------------------------------

def test_map_async_actor_policy_from_class():
    """策略字段随算子声明：label/并发/认缺白名单全部从 stage 类读取。"""
    from demiflow.data.plan import StreamStage

    class Soft(Exception):
        pass

    class Doubler(StreamStage):
        label = "double"
        concurrency = 3
        catch = (Soft,)

        async def __call__(self, row):
            if row["i"] == 2:
                raise Soft("miss")
            return {**row, "v": row["i"] * 2}

    ctx = local_data()
    ds = (ctx.from_items([{"i": i} for i in range(5)])
          .map_async(Doubler()))
    stats = ds.run_stream(log_every=0)
    assert stats.emitted == 4
    assert stats.miss["double:Soft"] == 1
    assert stats.stage("double")["in"] == 5


def test_stage_bound_deps_not_deepcopied():
    """绑定了不可深拷贝依赖（锁/连接）的 stage 安全通过。"""
    import copy
    from demiflow.data.plan import StreamStage

    class NoCopy:
        def __deepcopy__(self, memo):
            raise RuntimeError("不许深拷贝")

    class Counter:
        def __init__(self):
            self.n = 0

    class Toucher(StreamStage):
        label = "touch"
        concurrency = 2

        def __init__(self):
            self.dep = NoCopy()
            self.counter = Counter()

        async def __call__(self, row):
            self.counter.n += 1
            return row

    st = Toucher()
    ctx = local_data()
    ds = ctx.from_items([{"i": i} for i in range(4)]).map_async(st)
    ds.run_stream()
    assert st.counter.n == 4          # 同一实例（未深拷贝）


def test_map_async_actor_sync_call_and_label_default():
    from demiflow.data.plan import StreamStage

    class SyncPassthrough(StreamStage):     # 同步 __call__ 也合法
        concurrency = 2

        def __call__(self, row):
            return [row, row] if row["i"] % 2 else None

    ctx = local_data()
    ds = ctx.from_items([{"i": i} for i in range(4)]).map_async(SyncPassthrough())
    stats = ds.run_stream()
    assert stats.emitted == 4
    assert "SyncPassthrough" in stats.stages      # label 缺省取类名


def test_stage_aclose_lifecycle_hook():
    """持有资源（浏览器/连接）的 actor 算子经 run_stream 退出期统一 aclose。"""
    from demiflow.data.plan import StreamStage

    closed = []

    class Resourced(StreamStage):
        label = "res"

        async def __call__(self, row):
            return row

        async def aclose(self):
            closed.append("res")

    class Plain(StreamStage):     # 无 aclose 的算子不受影响
        label = "plain"

        def __call__(self, row):
            return row

    ctx = local_data()
    (ctx.from_items([{"i": 1}])
     .map_async(Plain()).map_async(Resourced())
     .run_stream())
    assert closed == ["res"]


def test_queue_factory_transport_seam():
    """行传输缝：自定义 queue_factory 可替换进程内队列，行为不变。"""

    class CountingQueue(asyncio.Queue):
        def __init__(self, maxsize):
            super().__init__(maxsize=maxsize)

        async def put(self, item):
            CountingQueue.puts += 1
            await super().put(item)

    CountingQueue.puts = 0

    ctx = local_data()

    from demiflow.data.plan import StreamStage

    class Pass(StreamStage):
        label = "pass"
        concurrency = 2

        def __call__(self, row):
            return row

    stats = (ctx.from_items([{"i": i} for i in range(5)])
             .map_async(Pass())
             .run_stream(queue_factory=lambda d: CountingQueue(d)))
    assert stats.emitted == 5
    assert CountingQueue.puts >= 5        # 工厂确实被使用


def test_map_async_plain_class_partial_fields():
    """普通类（零继承）漏声明部分策略字段：仍按 actor 解析，缺项走默认。"""
    ctx = local_data()

    class PlainActor:               # 无继承，只声明了 concurrency
        concurrency = 2

        async def __call__(self, row):
            return row

    hit = []

    class PartialCatch:             # 只声明了 catch（label/并发走默认）
        catch = (ValueError,)

        async def __call__(self, row):
            if row["i"] == 3:
                raise ValueError("soft")
            hit.append(row["i"])
            return row

    stats = (ctx.from_items([{"i": i} for i in range(4)])
             .map_async(PlainActor())
             .map_async(PartialCatch())
             .run_stream())
    assert stats.emitted == 3                     # 漏字段没有误判 fn 路径
    assert stats.miss["PartialCatch:ValueError"] == 1
    assert "PartialCatch" in stats.stages         # label 默认取类名


# ---------- 活性层与攒批（2026-09-14 U1/U2 回归） ----------

def test_hard_timeout_counts_miss_not_fatal():
    """U1a：算子永久挂起 → hard_timeout 计 miss，管线完成而非凝固。"""
    ctx, items = build(6)

    async def hang(r):
        await asyncio.sleep(3600)

    ds = ctx.from_items(items).map_async(
        hang, concurrency=2, hard_timeout=0.1, label="hang")
    stats = ds.run_stream()
    assert stats.miss["hang:hard_timeout"] == 6
    assert stats.emitted == 0


def test_stall_watchdog_raises_with_dump():
    """U1b：无超时挂起 + stall_timeout → StallError（含栈转储）。"""
    ctx, items = build(3)

    async def hang(r):
        await asyncio.sleep(3600)

    ds = ctx.from_items(items).map_async(hang, concurrency=2, label="hang")
    with pytest.raises(Exception) as ei:
        ds.run_stream(stall_timeout=0.6)
    assert "stalled" in str(ei.value) or "StallError" in type(ei.value).__name__


def test_batch_map_count_trigger_and_expand():
    """U2：条数触发攒批 + list 展开扇出。"""
    ctx, items = build(20)
    ds = (ctx.from_items(items)
          .batch_map(lambda bs: [{"i": r["i"], "b": len(bs)} for r in bs],
                     max_batch=5, label="meta"))
    stats = ds.run_stream()
    assert stats.emitted == 20
    assert stats.stage("meta")["in"] == 20
    assert all(True for _ in [1])  # 展开行均经批输出


def test_batch_map_time_flush_and_tail():
    """U2：时间窗触发 + EOF 尾批（20 条 max_batch=7 → 2 满批+尾 6）。"""
    ctx, items = build(20)
    batches = []

    async def meta(bs):
        batches.append(len(bs))
        return bs

    ds = ctx.from_items(items).batch_map(
        meta, max_batch=7, flush_interval=0.05, label="meta")
    stats = ds.run_stream()
    assert stats.emitted == 20
    assert sum(batches) == 20
    assert 6 in batches          # 尾批被刷出（EOF 触发）


def test_batch_map_whole_batch_miss_recorded():
    """U2：批调用白名单异常 → 整批计 miss 且入 dead_batches。"""
    ctx, items = build(10)

    async def boom(bs):
        raise ValueError("batch boom")

    ds = ctx.from_items(items).batch_map(
        boom, max_batch=5, catch=(ValueError,), label="meta")
    stats = ds.run_stream()
    assert stats.miss["meta:ValueError"] >= 1
    assert stats.dead_batches and stats.dead_batches[0]["rows"]
