"""S012 · _MapWorker.prepare — a started actor is drained if registration fails."""
import asyncio

import pytest

from demiflow.data.plan import AsyncMapOp, CallableSpec
from demiflow.execution.stream_map import _MapWorker


class Lifecycle:
    def __init__(self):
        self.starts = self.stops = self.closes = 0

    async def astart(self):
        self.starts += 1

    async def astop(self):
        self.stops += 1

    async def aclose(self):
        self.closes += 1

    def snapshot_state(self):
        return {}

    def restore_state(self, state):
        pass

    def __call__(self, row):
        return row


class BadStart(Lifecycle):
    async def astart(self):
        self.starts += 1
        raise RuntimeError('astart failed')


class FailingRegister:
    def __init__(self, exc):
        self.exc = exc
        self.validated = None

    def validate(self, fn):
        self.validated = fn

    async def register(self, fn, key):
        raise self.exc


class OkRegister:
    def validate(self, fn):
        pass

    async def register(self, fn, key):
        pass


def worker(actor, recovery):
    op = AsyncMapOp(callable=CallableSpec.create(actor), concurrency=1)
    return _MapWorker(op, recovery_state=recovery, recovery_key='stage')


def test_register_failure_drains_started_actor():
    w = worker(Lifecycle(), FailingRegister(RuntimeError('register failed')))
    with pytest.raises(RuntimeError, match='register failed'):
        asyncio.run(w.prepare())
    fn = w.callable._fn
    assert fn.starts == 1            # astart succeeded
    assert fn.stops == 1 and fn.closes == 1   # cleanup drained it
    assert w.started is False and w.failure is not None


def test_register_cancellation_drains_started_actor():
    w = worker(Lifecycle(), FailingRegister(asyncio.CancelledError()))
    with pytest.raises(asyncio.CancelledError):
        asyncio.run(w.prepare())
    fn = w.callable._fn
    assert fn.starts == 1 and fn.stops == 1 and fn.closes == 1


def test_astart_failure_does_not_double_close():
    w = worker(BadStart(), FailingRegister(RuntimeError('unused')))
    with pytest.raises(RuntimeError, match='astart failed'):
        asyncio.run(w.prepare())
    fn = w.callable._fn
    # astart never completed: action-level cleanup owns stop/close, not prepare.
    assert fn.starts == 1 and fn.stops == 0 and fn.closes == 0
    assert w.failure is not None


def test_successful_prepare_starts_once():
    w = worker(Lifecycle(), OkRegister())
    asyncio.run(w.prepare())
    fn = w.callable._fn
    assert w.started is True and w.failure is None
    assert fn.starts == 1 and fn.stops == 0 and fn.closes == 0


def test_failed_prepare_reraises_without_restart():
    w = worker(Lifecycle(), FailingRegister(RuntimeError('register failed')))
    with pytest.raises(RuntimeError):
        asyncio.run(w.prepare())
    fn = w.callable._fn
    with pytest.raises(RuntimeError, match='register failed'):
        asyncio.run(w.prepare())
    assert fn.starts == 1  # no second astart after a recorded failure


def test_cleanup_hooks_are_idempotent():
    w = worker(Lifecycle(), FailingRegister(RuntimeError('register failed')))
    with pytest.raises(RuntimeError):
        asyncio.run(w.prepare())
    asyncio.run(w.astop())
    asyncio.run(w.aclose())
    fn = w.callable._fn
    assert fn.stops == 1 and fn.closes == 1  # fallback did not re-emit