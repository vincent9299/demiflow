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
