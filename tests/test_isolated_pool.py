import asyncio
import os
import time

import pytest

from demiflow.execution.isolated_pool import IsolatedWorkerPool


@pytest.fixture
def identity():
    # A closure is serialized by value; pytest's test modules are not installed
    # in the child interpreter.
    def call(value, delay=0):
        time.sleep(delay)
        return os.getpid(), value
    return call


def test_reuse_recycle_and_concurrency(identity):
    async def check():
        pool = IsolatedWorkerPool({'identity': identity}, workers=2, max_tasks=3)
        try:
            first = await asyncio.gather(*(pool.run('identity', i, delay=.02, timeout_s=10) for i in range(6)))
            from collections import Counter
            assert max(Counter(p for p, _ in first).values()) <= 3
            assert [v for _, v in first] == list(range(6))
            assert pool.metrics['peak_active'] == 2
        finally:
            await pool.aclose()
        assert all(w['process'] is None for w in pool.workers)
        serial = IsolatedWorkerPool({'identity': identity}, workers=1, max_tasks=2)
        try:
            first, _ = await serial.run('identity', 1, timeout_s=10)
            second, _ = await serial.run('identity', 2, timeout_s=10)
            third, _ = await serial.run('identity', 3, timeout_s=10)
            assert first == second and third != first
        finally:
            await serial.aclose()
    asyncio.run(check())


def test_timeout_and_cancellation_reap_and_allow_next_call(identity):
    async def check():
        pool = IsolatedWorkerPool({'identity': identity}, workers=1)
        try:
            old, _ = await pool.run('identity', 1, timeout_s=10)
            with pytest.raises(TimeoutError):
                await pool.run('identity', 2, delay=2, timeout_s=.05)
            with pytest.raises(ProcessLookupError):
                os.kill(old, 0)
            new, _ = await pool.run('identity', 3, timeout_s=10)
            task = asyncio.create_task(pool.run('identity', 4, delay=2, timeout_s=10))
            await asyncio.sleep(.05)
            task.cancel()
            with pytest.raises(asyncio.CancelledError):
                await task
            with pytest.raises(ProcessLookupError):
                os.kill(new, 0)
            assert (await pool.run('identity', 5, timeout_s=10))[1] == 5
        finally:
            await pool.aclose()
    asyncio.run(check())


def test_frame_limit_and_worker_exception_do_not_poison_pool(identity):
    async def check():
        pool = IsolatedWorkerPool({'identity': identity, 'bytes': bytes}, workers=1, max_message_bytes=8192)
        try:
            with pytest.raises(ValueError, match='byte limit'):
                await pool.run('identity', 'x'*9000, timeout_s=10)
            with pytest.raises(ValueError, match='byte limit'):
                await pool.run('bytes', 9000, timeout_s=10)
            assert (await pool.run('identity', 3, timeout_s=10))[1] == 3
        finally:
            await pool.aclose()
    asyncio.run(check())
