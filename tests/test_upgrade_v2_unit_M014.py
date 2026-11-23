"""M014: drain bounded-waits real in-flight Tasks without owning shutdown."""
import asyncio
from types import SimpleNamespace

import pytest

from demiflow.execution.pipeline.run_context import RunContext
from demiflow.execution.operator_bindings.dataflow_model_journal import NativeEmbeddingRecovery


def _rec(run):
    actor = SimpleNamespace(_journal=None, options={'max_response_bytes': 4096})
    action = SimpleNamespace(context=run, action_id='act-1', object_sinks={})
    return NativeEmbeddingRecovery(actor, action, 'node')


def test_drain_empty_returns(tmp_path):
    with RunContext.start(tmp_path / 'run', limits={'max_requests': 5}) as run:
        asyncio.run(_rec(run).drain(timeout_s=1))


@pytest.mark.parametrize('bad', [0, -1, float('inf'), float('nan'), 'x'])
def test_drain_rejects_invalid_timeout(tmp_path, bad):
    with RunContext.start(tmp_path / 'run', limits={'max_requests': 5}) as run:
        with pytest.raises(ValueError):
            asyncio.run(_rec(run).drain(timeout_s=bad))


def test_drain_waits_for_inflight_completion(tmp_path):
    with RunContext.start(tmp_path / 'run', limits={'max_requests': 5}) as run:
        rec = _rec(run)

        async def exercise():
            started, release = asyncio.Event(), asyncio.Event()

            async def work():
                started.set()
                await release.wait()
                return 'done'

            task = asyncio.ensure_future(work())
            rec.inflight['r1'] = task
            await started.wait()
            drain = asyncio.ensure_future(rec.drain(timeout_s=5))
            await asyncio.sleep(0.02)
            assert not drain.done()  # still waiting on the real task
            release.set()
            await drain
            assert task.done() and task.result() == 'done'

        asyncio.run(exercise())


def test_drain_timeout_keeps_inflight_and_task(tmp_path):
    with RunContext.start(tmp_path / 'run', limits={'max_requests': 5}) as run:
        rec = _rec(run)

        async def exercise():
            release = asyncio.Event()

            async def work():
                await release.wait()

            task = asyncio.ensure_future(work())
            rec.inflight['r1'] = task
            with pytest.raises(TimeoutError):
                await rec.drain(timeout_s=0.05)
            assert 'r1' in rec.inflight  # not cleared on timeout
            assert not task.done()
            release.set()
            await task

        asyncio.run(exercise())


def test_drain_does_not_wait_on_self(tmp_path):
    with RunContext.start(tmp_path / 'run', limits={'max_requests': 5}) as run:
        rec = _rec(run)

        async def exercise():
            current = asyncio.current_task()
            rec.inflight['self'] = current
            await rec.drain(timeout_s=0.2)  # must not deadlock waiting on itself

        asyncio.run(exercise())