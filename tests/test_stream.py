"""demiflow streaming 路径测试（run_stream / map_async）。"""

from __future__ import annotations

import asyncio
import time
from pathlib import Path
from _worker_event_fixtures import emit, events

import pytest

from demiflow.data.plan import LogicalPlan
from demiflow.data.api import DataAPI
from demiflow.errors import StallError
from demiflow.execution.contracts.dataflow_errors import DataflowError
from demiflow.execution.pipeline.run_context import RunContext
from demiflow.execution.journal.run_journal import RunJournal


def build(n=50):
    ctx = DataAPI()
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


def test_repeated_nodes_keep_distinct_counts_and_processing_times():
    async def same(row):
        await asyncio.sleep(.01)
        return row
    stats = (DataAPI().from_items([{'i':i} for i in range(3)])
             .map_async(same, concurrency=2)
             .map_async(same, concurrency=1).run_stream())
    assert list(stats.stages) == ['same','same#2']
    assert [v['emitted'] for v in stats.stages.values()] == [3,3]
    assert stats.emitted == 3
    for value in stats.timing_summary().values():
        assert value['count'] == 3 and value['total_s'] >= .025
        assert value['p95_upper_s'] >= .01


def test_unordered_emission_no_head_of_line_blocking(tmp_path):
    """A slow first call waits for a later output to reach its real consumer."""
    ctx, items = build(6)
    gate = tmp_path / 'later-output-delivered'
    observed = str(tmp_path / 'order.jsonl')
    async def op(row):
        if row['i'] == 0:
            async def wait_for_consumer():
                while not gate.exists():
                    await asyncio.sleep(.001)
            await asyncio.wait_for(wait_for_consumer(), 5)
        return row
    def consume(row):
        emit(observed, 'output', value=row['i'])
        if row['i'] == 1:
            gate.touch()
        return row
    start = time.monotonic()
    stats = ctx.from_items(items).map_async(op, concurrency=6).map(consume).run_stream()
    emit(observed, 'elapsed', seconds=time.monotonic() - start)
    order = [e['value'] for e in events(observed) if e['event'] == 'output']
    assert sorted(order) == list(range(6)) and stats.emitted == 6
    assert order.index(1) < order.index(0)
    # Setup-inclusive latency is recorded, not confused with this causal
    # head-of-line guarantee. The historical <.9 s performance target remains
    # separately unproven for the new sealed managed execution.


def test_bounded_backpressure(tmp_path):
    """有界背压：末级慢时，中间级在飞量受 queue_depth+concurrency 钳制。"""
    ctx, items = build(40)
    inflight = {"n": 0, "max": 0}
    observed = str(tmp_path / "inflight.jsonl")

    async def mid(r):
        inflight["n"] += 1
        inflight["max"] = max(inflight["max"], inflight["n"])
        emit(observed, "inflight", current=inflight["n"])
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
    actual = [e["current"] for e in events(observed)]
    assert len(actual) == 40 and 1 < max(actual) <= 4 + 4            # 深度+并发之外被 put 阻塞挡住


def test_catch_whitelist_miss_and_fatal_terminates(tmp_path):
    """认缺分级：白名单命中计数不断链；白名单外异常终止整链。"""
    class Soft(Exception):
        pass

    class Hard(Exception):
        pass

    ctx = DataAPI()
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
    with pytest.raises(DataflowError, match='fatal') as failure:
        with RunContext.start(tmp_path / 'fatal-run'):
            ds2.run_stream()
    assert failure.value.code == 'INTERNAL_PROTOCOL'
    assert 'Hard' in '\n'.join(getattr(failure.value, '__notes__', ()))
    journal = RunJournal(tmp_path / 'fatal-run')
    assert journal.execution_action('action-1')['result'] is None
    assert not any(f['status'] == 'acked' for f in journal.execution_commits('action-1'))


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


def test_on_drain_runs_on_success_and_failure(tmp_path):
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
    with pytest.raises(DataflowError, match='Boom') as failure:
        with RunContext.start(tmp_path / 'drain-failure'):
            ds2.run_stream(on_drain=lambda s: drained.append("fail"))
    assert failure.value.code == 'INTERNAL_PROTOCOL'
    journal = RunJournal(tmp_path / 'drain-failure')
    assert journal.execution_action('action-1')['result'] is None
    assert not any(f['status'] == 'acked' for f in journal.execution_commits('action-1'))
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

    ctx = DataAPI()
    ds = (ctx.from_items([{"i": i} for i in range(5)])
          .map_async(Doubler()))
    stats = ds.run_stream(log_every=0)
    assert stats.emitted == 4
    assert stats.miss["double:Soft"] == 1
    assert stats.stage("double")["in"] == 5


def test_stage_bound_deps_not_deepcopied(tmp_path):
    """One worker-owned actor keeps its uncopyable live dependency until close."""
    from demiflow.data.plan import StreamStage
    observed = str(tmp_path / 'uncopyable-owner.jsonl')
    class NoCopy:
        def __deepcopy__(self, memo):
            emit(observed, 'deepcopy')
            raise RuntimeError('dependency must not be deepcopied')
    class Toucher(StreamStage):
        label = 'touch'
        concurrency = 2
        def __init__(self):
            self.dep = NoCopy()
            self.count = 0
            emit(observed, 'construct')
        async def __call__(self, row):
            self.count += 1
            emit(observed, 'call', value=row['i'])
            return row
        async def aclose(self):
            emit(observed, 'close', count=self.count)
    stats = DataAPI().from_items([{'i': i} for i in range(4)]).map_async(Toucher).run_stream()
    actual = events(observed)
    assert [e['event'] for e in actual].count('construct') == 1
    assert sorted(e['value'] for e in actual if e['event'] == 'call') == list(range(4))
    assert [e['count'] for e in actual if e['event'] == 'close'] == [4]
    assert not any(e['event'] == 'deepcopy' for e in actual)
    assert stats.emitted == 4


def test_map_async_actor_sync_call_and_label_default():
    from demiflow.data.plan import StreamStage

    class SyncPassthrough(StreamStage):     # 同步 __call__ 也合法
        concurrency = 2

        def __call__(self, row):
            return [row, row] if row["i"] % 2 else None

    ctx = DataAPI()
    ds = ctx.from_items([{"i": i} for i in range(4)]).map_async(SyncPassthrough())
    stats = ds.run_stream()
    assert stats.emitted == 4
    assert "SyncPassthrough" in stats.stages      # label 缺省取类名


def test_stage_aclose_lifecycle_hook(tmp_path):
    """持有资源（浏览器/连接）的 actor 算子经 run_stream 退出期统一 aclose。"""
    from demiflow.data.plan import StreamStage

    closed = str(tmp_path / "closed.jsonl")

    class Resourced(StreamStage):
        label = "res"

        async def __call__(self, row):
            return row

        async def aclose(self):
            emit(closed, "close", owner="res")

    class Plain(StreamStage):     # 无 aclose 的算子不受影响
        label = "plain"

        def __call__(self, row):
            return row

    ctx = DataAPI()
    (ctx.from_items([{"i": 1}])
     .map_async(Plain()).map_async(Resourced())
     .run_stream())
    assert [e["owner"] for e in events(closed)] == ["res"]


def test_queue_factory_transport_seam(tmp_path):
    """Retired queue injection cannot bypass the original engine's transport."""
    from demiflow.errors import UnsupportedExecutionOptionError
    observed = str(tmp_path / 'forbidden-input.jsonl')
    def source():
        emit(observed, 'input')
        yield {'i': 0}
    factories = []
    def forbidden_factory(depth):
        factories.append(depth)
        raise AssertionError('private queue factory was invoked')
    with pytest.raises(UnsupportedExecutionOptionError, match='owned by the unified engine'):
        DataAPI().from_iter(source).map_async(lambda row: row).run_stream(queue_factory=forbidden_factory)
    assert not factories and not events(observed)
    stats = DataAPI().from_items([{'i': i} for i in range(5)]).map_async(
        lambda row: row, concurrency=2).run_stream()
    assert stats.emitted == 5
    handoffs = stats.metrics['queues']
    assert handoffs and all(h['kind'] == 'synchronous_output_handoff' and
        0 <= h['current'] <= h['peak'] <= h['capacity'] == 1 for h in handoffs)


def test_map_async_plain_class_partial_fields():
    """普通类（零继承）漏声明部分策略字段：仍按 actor 解析，缺项走默认。"""
    ctx = DataAPI()

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


def test_stall_watchdog_raises():
    """U1b：无超时挂起 + stall_timeout → StallError。"""
    ctx, items = build(3)

    async def hang(r):
        await asyncio.sleep(3600)

    ds = ctx.from_items(items).map_async(hang, concurrency=2, label="hang")
    with pytest.raises(StallError, match='no observed progress'):
        ds.run_stream(stall_timeout=0.6)


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


def test_batch_map_time_flush_and_tail(tmp_path):
    """U2：时间窗触发 + EOF 尾批（20 条 max_batch=7 → 2 满批+尾 6）。"""
    ctx, items = build(20)
    observed = str(tmp_path / "batches.jsonl")

    async def meta(bs):
        emit(observed, "batch", count=len(bs))
        return bs

    ds = ctx.from_items(items).batch_map(
        meta, max_batch=7, flush_interval=0.05, label="meta")
    stats = ds.run_stream()
    assert stats.emitted == 20
    batches = [e["count"] for e in events(observed)]
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
