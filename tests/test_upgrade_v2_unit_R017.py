"""R017-R020 · local cut barrier arrival identity and fail-closed validation.

The local cut identity is the barrier object; arrivals are isolated by
(stage index, worker slot). Buffered stages aggregate every active worker slot
before align; native row-local keeps its single token; duplicate, foreign and
out-of-range arrivals fail closed instead of silently aligning a partial cut.
"""
import asyncio
from types import SimpleNamespace

import pytest

from demiflow.execution.recovery_contract import RecoveryContractError
from demiflow.execution.stream_barrier import _CheckpointBarrier


def _stage(concurrency=1, **kw):
    # Identity-compared like the real _Stage (slots, no __eq__): a foreign stage
    # must not match a registered one merely by sharing attribute values.
    class _S:
        pass
    s = _S()
    s.concurrency = concurrency
    s.max_batch = kw.get('max_batch')
    s.grouping = kw.get('grouping')
    s.prefix = kw.get('prefix')
    s.native_snapshot = kw.get('native_snapshot', False)
    return s


def _progress(stages):
    aligned = []

    async def align(index, barrier, pending):
        aligned.append((index, pending))

    return SimpleNamespace(
        run=SimpleNamespace(limits={'checkpoint_timeout_s': 5}),
        stats=None, group_ends=list(range(len(stages))),
        align=align, seal=lambda barrier: (lambda: None), aligned=aligned)


def _barrier(stages, progress=None):
    queues = [asyncio.Queue() for _ in stages]
    return _CheckpointBarrier(stages, queues, None, progress=progress), queues


def test_duplicate_worker_slot_arrival_fails_closed():
    async def scenario():
        stage = _stage(concurrency=2, max_batch=3)
        progress = _progress([stage])
        barrier, _ = _barrier([stage], progress)
        # Slot 0 registers but the cut waits for slot 1; the duplicate must be
        # rejected immediately, not after the coordination deadline.
        first = asyncio.create_task(barrier.arrive(stage, [{'id': 0}], worker_slot=0))
        await asyncio.sleep(0.02)
        try:
            with pytest.raises(RecoveryContractError, match='Duplicate'):
                await barrier.arrive(stage, [{'id': 9}], worker_slot=0)
        finally:
            first.cancel()
            await asyncio.gather(first, return_exceptions=True)
    asyncio.run(scenario())


def test_foreign_stage_arrival_fails_closed():
    async def scenario():
        stage = _stage(max_batch=3)
        foreign = _stage(max_batch=3)
        barrier, _ = _barrier([stage], _progress([stage]))
        with pytest.raises(RecoveryContractError, match='foreign stage'):
            await barrier.arrive(foreign, [], worker_slot=0)
    asyncio.run(scenario())


def test_buffered_stage_requires_in_range_worker_slot():
    async def scenario():
        stage = _stage(concurrency=2, max_batch=3)
        barrier, _ = _barrier([stage], _progress([stage]))
        for bad in (None, -1, 2, '0'):
            with pytest.raises(RecoveryContractError, match='worker_slot'):
                await barrier.arrive(stage, [], worker_slot=bad)
    asyncio.run(scenario())


def test_native_row_local_rejects_worker_slot():
    async def scenario():
        stage = _stage(native_snapshot=True)
        barrier, _ = _barrier([stage], _progress([stage]))
        with pytest.raises(RecoveryContractError, match='without a worker slot'):
            await barrier.arrive(stage, None, worker_slot=0)
    asyncio.run(scenario())


def test_concurrent_slots_aggregate_before_align():
    async def scenario():
        stage = _stage(concurrency=2, max_batch=3)
        progress = _progress([stage])
        barrier, queues = _barrier([stage], progress)
        # First slot alone must not align: the cut waits for every active worker.
        first = asyncio.create_task(barrier.arrive(stage, [{'id': 0}], worker_slot=0))
        await asyncio.sleep(0.02)
        assert progress.aligned == []
        second = asyncio.create_task(barrier.arrive(stage, [{'id': 1}], worker_slot=1))
        await asyncio.gather(first, second)
        assert progress.aligned == [(0, {0: [{'id': 0}], 1: [{'id': 1}]})]
    asyncio.run(scenario())


def test_legacy_row_local_single_arrival_aligns_without_slot():
    async def scenario():
        stage = _stage()  # row-wise, no buffer
        progress = _progress([stage])
        barrier, _ = _barrier([stage], progress)
        await barrier.arrive(stage)
        assert progress.aligned == [(0, None)]
    asyncio.run(scenario())


def test_barrier_rejects_arrival_after_publication():
    async def scenario():
        stage = _stage()
        progress = _progress([stage])
        barrier, _ = _barrier([stage], progress)
        await barrier.arrive(stage)
        await barrier.wait()
        with pytest.raises(RecoveryContractError, match='no longer accepts'):
            await barrier.arrive(stage)
    asyncio.run(scenario())