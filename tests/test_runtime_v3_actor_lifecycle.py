"""Real actor owners, bounded cancellation, and stop-before-close ordering."""
import asyncio
import functools
import os
import threading
import time
from types import SimpleNamespace

import pytest

from demiflow import data
from demiflow.execution.contracts.dataflow_errors import DataflowError
from demiflow.execution.kernels.async_instance import AsyncInstance
from _worker_event_fixtures import emit, events


class PlainActor:
    def __init__(self, path):
        self.path = str(path)

    def snapshot_state(self):
        return {}

    def restore_state(self, state):
        pass

    async def astart(self):
        emit(self.path, "start")

    async def astop(self):
        emit(self.path, "stop")

    async def aclose(self):
        emit(self.path, "close")

    async def __call__(self, row, plus=0):
        emit(self.path, "call")
        return {"n": row["n"] + plus}


@pytest.mark.parametrize("form", ["instance", "method", "partial", "class", "args"])
def test_public_plain_actor_owner_is_the_lifecycle_owner(tmp_path, form):
    path = tmp_path / "events"
    actor = PlainActor(path)
    with data.local_execution():
        ds = data.from_items([{"n": 1}, {"n": 2}])
        if form == "class":
            ds = ds.map(PlainActor, fn_constructor_args=[str(path)])
        elif form == "args":
            ds = ds.map(actor.__call__, fn_kwargs={"plus": 0})
        else:
            target = {"instance": actor, "method": actor.__call__,
                      "partial": functools.partial(actor.__call__, plus=0)}[form]
            ds = ds.map_async(target)
        assert ds.take_all() == [{"n": 1}, {"n": 2}]
    observed = events(path)
    assert [e["event"] for e in observed] == ["start", "call", "call", "stop", "close"]
    assert all(e["pid"] != os.getpid() for e in observed)


def services(*, timeout_ms=2000):
    return SimpleNamespace(bounds=SimpleNamespace(timeout_ms=timeout_ms), check_stop=lambda: None)


@pytest.mark.parametrize("fail_at", [None, "start", "stop", "close"])
def test_lifecycle_is_once_and_failed_hook_does_not_skip_close(fail_at):
    seen = []
    class Actor:
        async def astart(self):
            seen.append("start")
            if fail_at == "start":
                raise ValueError("start")
        async def astop(self):
            seen.append("stop")
            if fail_at == "stop":
                raise ValueError("stop")
        async def aclose(self):
            seen.append("close")
            if fail_at == "close":
                raise ValueError("close")
        def __call__(self, row):
            return row
    actor = Actor()
    instance = AsyncInstance(actor.__call__, services(), owner=actor)
    if fail_at == "start":
        with pytest.raises(ValueError, match="start"):
            instance.start()
    else:
        instance.start()
        instance.start()
    thread = instance.thread
    if fail_at in ("stop", "close"):
        with pytest.raises(ValueError, match=fail_at):
            instance.close()
    else:
        instance.close()
    instance.close()
    assert seen == ["start", "stop", "close"]
    assert not thread.is_alive()


def test_cancelled_future_finishes_its_finally_before_actor_close():
    from concurrent.futures import ThreadPoolExecutor
    seen = []
    entered = threading.Event()
    class Actor:
        async def __call__(self, row):
            entered.set()
            try:
                await asyncio.Event().wait()
            finally:
                await asyncio.sleep(.05)
                seen.append("call-drained")
        async def astop(self):
            seen.append("stop")
        async def aclose(self):
            seen.append("close")
    actor = Actor()
    instance = AsyncInstance(actor.__call__, services(), owner=actor)
    with ThreadPoolExecutor(1) as pool:
        future = pool.submit(instance, {})
        assert entered.wait(2)
        instance.request_stop()
        with pytest.raises(DataflowError, match="CANCELLED"):
            future.result(2)
        instance.close()
    assert seen == ["call-drained", "stop", "close"]


def test_background_task_drains_before_actor_resources_close():
    seen = []
    class Actor:
        async def astart(self):
            async def background():
                try:
                    await asyncio.Event().wait()
                finally:
                    await asyncio.sleep(.01)
                    seen.append("background-drained")
            self.task = asyncio.create_task(background())
        async def astop(self):
            seen.append("stop")
        async def aclose(self):
            assert self.task.done()
            seen.append("close")
        def __call__(self, row):
            return row
    actor = Actor()
    instance = AsyncInstance(actor, services())
    instance.start()
    instance.close()
    assert seen == ["stop", "background-drained", "close"]


def test_cancelled_to_thread_finishes_before_resources_close():
    from concurrent.futures import ThreadPoolExecutor
    started = threading.Event()
    finished = threading.Event()
    seen = []
    class Actor:
        async def __call__(self, row):
            def blocking():
                started.set()
                time.sleep(.15)
                seen.append("blocking-drained")
                finished.set()
            return await asyncio.to_thread(blocking)
        async def astop(self):
            seen.append("stop")
        async def aclose(self):
            assert finished.is_set()
            # Cleanup hooks may themselves need the still-live executor.
            await asyncio.to_thread(seen.append, "close")
    actor = Actor()
    instance = AsyncInstance(actor, services())
    with ThreadPoolExecutor(1) as pool:
        future = pool.submit(instance, {})
        assert started.wait(2)
        instance.request_stop()
        with pytest.raises(DataflowError, match="CANCELLED"):
            future.result(2)
        instance.close()
    assert sorted(seen) == ["blocking-drained", "close", "stop"]
    assert seen.index("blocking-drained") < seen.index("close")
    assert not any(t.is_alive() and t.name.startswith("demiflow-operator-async-work")
                   for t in threading.enumerate())


def test_kernel_does_not_close_actor_before_blocking_call_exits():
    from test_runtime_v3_k import services as kernel_services, spec, program, rec, InputPort, OperatorKernel
    started, release = threading.Event(), threading.Event()
    seen = []
    class Actor:
        def __call__(self, row):
            started.set()
            assert release.wait(2)
            seen.append("call-drained")
            return row
        async def astop(self):
            seen.append("stop")
        async def aclose(self):
            seen.append("close")
    svc = kernel_services(programs={"fn": program(Actor())})
    # A short close deadline is a controlled assertion, not a business timeout.
    svc.bounds = SimpleNamespace(**{**vars(svc.bounds), "timeout_ms": 30})
    kernel = OperatorKernel(spec("MapOp", {"program": "fn", "concurrency": 2},
                                inputs=[InputPort("input", "incremental")]), services=svc, partition_id="p0")
    kernel.start()
    list(kernel.push("input", rec({"v": 1})))
    assert started.wait(1)
    try:
        with pytest.raises(DataflowError, match="did not drain"):
            kernel.close()
        assert seen == []
    finally:
        release.set()
    deadline = time.monotonic() + 2
    while not seen and time.monotonic() < deadline:
        time.sleep(.005)
    kernel.close()
    assert seen == ["call-drained", "stop", "close"]


def test_undrained_async_thread_reports_failure_and_can_finish_cleanup_later():
    from concurrent.futures import ThreadPoolExecutor
    started, release = threading.Event(), threading.Event()
    seen = []
    class Actor:
        async def __call__(self, row):
            def blocking():
                started.set()
                assert release.wait(2)
                seen.append("drained")
            return await asyncio.to_thread(blocking)
        async def astop(self):
            seen.append("stop")
        async def aclose(self):
            seen.append("close")
    instance = AsyncInstance(Actor(), services(timeout_ms=30))
    with ThreadPoolExecutor(1) as pool:
        future = pool.submit(instance, {})
        assert started.wait(2)
        instance.request_stop()
        with pytest.raises(DataflowError):
            future.result(2)
        try:
            with pytest.raises(DataflowError, match="threads did not drain"):
                instance.close()
            assert seen == ["stop"] and not instance._closed
        finally:
            release.set()
        instance.close()
    assert seen == ["stop", "drained", "close"]
