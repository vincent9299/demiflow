"""Blocking stages must be bounded, leave the event loop live, and drain safely."""
import asyncio
import contextvars
import threading
import time

import pytest

from demiflow.data.api import DataAPI


def test_threads_overlap_and_preserve_context_without_blocking_async_stage():
    context = contextvars.ContextVar('thread-stage-test', default='missing')
    context.set('run-context')
    ready = threading.Barrier(3, timeout=3)
    loop_received = threading.Event()
    names = set()

    def prepare(row):
        assert context.get() == 'run-context'
        names.add(threading.current_thread().name)
        ready.wait()
        if row['i'] != 0:
            assert loop_received.wait(3), 'blocking worker froze the async consumer'
        return [row, {**row, 'duplicate': True}]

    async def consume(row):
        loop_received.set()
        await asyncio.sleep(0)
        return row

    result = (DataAPI().from_items([{'i': i} for i in range(3)])
              .map_async(prepare, execution='thread', concurrency=3, queue_depth=1)
              .map_async(consume, concurrency=1).materialize().take_all())
    assert len(result) == 6
    assert len(names) == 3 and all(n.startswith('demiflow-kernel-call_') for n in names)
    assert not any(t.name in names for t in threading.enumerate())


def test_thread_bound_and_catch_contract():
    lock = threading.Lock()
    active = maximum = 0

    def prepare(row):
        nonlocal active, maximum
        with lock:
            active += 1
            maximum = max(maximum, active)
        try:
            time.sleep(.005)
            if row['i'] == 0:
                raise LookupError('expected missing input')
            return row if row['i'] % 2 else None
        finally:
            with lock:
                active -= 1

    stats = (DataAPI().from_items([{'i': i} for i in range(20)])
             .map_async(prepare, execution='thread', concurrency=3, queue_depth=1,
                        catch=(LookupError,), label='blocking').run_stream())
    assert 1 < maximum <= 3 and active == 0
    assert stats.emitted == 10
    assert stats.miss['blocking:LookupError'] == 1
    assert stats.miss['blocking:drop'] == 9


def test_failure_drains_running_threads_before_actor_close():
    finished = threading.Event()
    started = threading.Barrier(2, timeout=3)
    closed = []

    class Prepare:
        concurrency = 2

        def __call__(self, row):
            started.wait()
            if row['i'] == 0:
                raise ValueError('fatal input')
            time.sleep(.4)
            finished.set()
            return row

        async def aclose(self):
            assert finished.is_set(), 'actor closed while a worker still used it'
            closed.append(True)

    with pytest.raises(ValueError, match='fatal input'):
        (DataAPI().from_items([{'i': 0}, {'i': 1}])
         .map_async(Prepare(), execution='thread').run_stream())
    assert closed == [True]


def test_thread_policy_rejects_unenforceable_timeout_and_async_work():
    source = DataAPI().from_items([{'i': 1}])
    with pytest.raises(ValueError, match='execution'):
        source.map_async(lambda r: r, execution='invalid')
    with pytest.raises(ValueError, match='hard_timeout'):
        source.map_async(lambda r: r, execution='thread', hard_timeout=1)

    async def asynchronous(row):
        return row

    with pytest.raises(TypeError, match='synchronous'):
        source.map_async(asynchronous, execution='thread')
    with pytest.raises(TypeError, match='awaitable'):
        source.map_async(lambda r: asynchronous(r), execution='thread').run_stream()
