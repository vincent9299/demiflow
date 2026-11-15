"""One event loop for an operator's async lifecycle and calls, inside its worker."""
from __future__ import annotations

import asyncio
import concurrent.futures
import inspect
import threading
from contextvars import copy_context
import time

from ..contracts.dataflow_errors import DataflowError


class _CancelledCall:
    """Carry callback cancellation across Future without losing its cause."""
    def __init__(self, error):
        self.error = error


class _AsyncWorkPool(concurrent.futures.ThreadPoolExecutor):
    """Keep the real futures that cancelling asyncio.to_thread cannot stop."""
    def __init__(self):
        super().__init__(thread_name_prefix="demiflow-operator-async-work")
        self._active = set()
        self._active_lock = threading.Lock()

    def submit(self, fn, /, *args, **kwargs):
        future = super().submit(copy_context().run, fn, *args, **kwargs)
        from ...resources.capacity import track_owned_thread
        for thread in tuple(self._threads):
            track_owned_thread(thread)
        with self._active_lock:
            self._active.add(future)
        def done(completed):
            with self._active_lock:
                self._active.discard(completed)
        future.add_done_callback(done)
        return future

    def drain(self, timeout):
        deadline = time.monotonic() + timeout
        while True:
            with self._active_lock:
                pending = tuple(self._active)
            if not pending:
                return
            _, unfinished = concurrent.futures.wait(pending, timeout=max(0, deadline - time.monotonic()))
            if unfinished:
                raise DataflowError("DEADLINE_EXCEEDED", "async operator threads did not drain before actor cleanup")


class AsyncInstance:
    def __init__(self, target, services, *, timeout=None, owner=None, await_calls=True):
        self.target = target
        self.owner = target if owner is None else owner
        self.services = services
        self.await_calls = await_calls
        self.timeout = timeout or services.bounds.timeout_ms / 1000
        self.loop = None
        self.thread = None
        self._loop_lock = threading.Lock()
        self._async_work = None
        self.native_scope = None
        self.native_call_ordinal = 0
        self._stopping = threading.Event()
        self._tasks = set()  # Owned exclusively by this instance's event loop.
        self._started = False
        self._stop_called = False
        self._close_called = False
        self._closed = False
        self._owner_task = None
        self._owner_release = None
        self._lifecycle_failure = None
        self.validation = None
        self.validation_args = ()
        self.validation_kwargs = {}

    def __getattr__(self, name):
        return getattr(self.target, name)

    def resolve(self, value, *, closing=False):
        if not closing:
            try:
                self.check_lifecycle()
            except BaseException:
                if inspect.iscoroutine(value):
                    value.close()
                raise
        if not inspect.isawaitable(value):
            return value
        if self.loop is None:
            with self._loop_lock:
                if self.loop is None:
                    self.loop = asyncio.new_event_loop()
                    self._async_work = _AsyncWorkPool()
                    self.loop.set_default_executor(self._async_work)
                    self.thread = threading.Thread(target=copy_context().run, args=(self.loop.run_forever,),
                                                   name="demiflow-operator-loop", daemon=True)
                    self.thread.start()
                    from ...resources.capacity import track_owned_thread
                    track_owned_thread(self.thread)
        async def wait():
            task = asyncio.current_task()
            if not closing:
                self._tasks.add(task)
            try:
                if not closing and self._stopping.is_set():
                    if inspect.iscoroutine(value):
                        value.close()
                    raise DataflowError("CANCELLED", "operator is stopping")
                return await value
            except asyncio.CancelledError as exc:
                # asyncio's Future bridge otherwise replaces this with an empty
                # concurrent.futures.CancelledError. Cancellation stays local,
                # data-only failure transport later preserves the known type.
                return _CancelledCall(exc)
            finally:
                self._tasks.discard(task)
        future = asyncio.run_coroutine_threadsafe(wait(), self.loop)
        deadline = time.monotonic() + self.timeout
        try:
            while True:
                if not closing:
                    if self._stopping.is_set():
                        raise DataflowError("CANCELLED", "operator is stopping")
                    self.services.check_stop()
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    raise DataflowError("DEADLINE_EXCEEDED", "operator call exceeded its deadline")
                try:
                    result = future.result(timeout=min(.05, remaining))
                except concurrent.futures.TimeoutError:
                    if not future.done():
                        continue
                    result = future.result()
                if isinstance(result, _CancelledCall):
                    raise result.error
                return result
        except BaseException:
            future.cancel()
            raise

    def start(self):
        if self._closed or self._stopping.is_set():
            raise DataflowError("INVALID_CONTRACT", "cannot start a closed operator")
        if self._started:
            return
        # Even a partially failed start is owned and cleaned exactly once.
        self._started = True
        hook = getattr(self.owner, "astart", None)
        if hook is not None:
            async def start_owner():
                started = asyncio.Event()
                self._owner_release = asyncio.Event()
                async def lifetime():
                    try:
                        result = hook()
                        if inspect.isawaitable(result):
                            await result
                        started.set()
                        await self._owner_release.wait()
                    except BaseException as exc:
                        self._lifecycle_failure = exc
                        started.set()
                self._owner_task = asyncio.create_task(lifetime())
                await started.wait()
                self.check_lifecycle()
            self.resolve(start_owner())

    def check_lifecycle(self):
        if self._lifecycle_failure is not None:
            raise self._lifecycle_failure

    def __call__(self, value):
        self.check_lifecycle()
        if self._stopping.is_set():
            raise DataflowError("CANCELLED", "operator is stopping")
        if self.validation is not None:
            self.validation.inputs((value, *self.validation_args), self.validation_kwargs)
        result = self.target(value)
        if not self.await_calls and inspect.isawaitable(result):
            if inspect.iscoroutine(result):
                result.close()
            raise TypeError('map synchronous callable returned an awaitable; use map_async')
        result = self.resolve(result)
        return self.validation.output(result) if self.validation is not None else result

    def request_stop(self):
        """Interrupt async calls before waiting for invocation threads to drain."""
        self._stopping.set()

    def _drain(self, *, background=False):
        if self.loop is None:
            return
        async def cancel_pending():
            tasks = (asyncio.all_tasks() if background else set(self._tasks))
            tasks.discard(asyncio.current_task())
            tasks.discard(self._owner_task)
            for task in tasks:
                # Repeated cancellation would interrupt the very finally block
                # whose completion makes it safe to release actor resources.
                if not task.cancelling():
                    task.cancel()
            if tasks:
                await asyncio.gather(*tasks, return_exceptions=True)
        self.resolve(cancel_pending(), closing=True)

    def close(self):
        if self._closed:
            return
        self.request_stop()
        # A cancelled concurrent Future can be done before its coroutine's
        # finally block. Await that block before touching actor resources.
        self._drain()
        first_error = None
        try:
            hook = getattr(self.owner, "astop", None)
            if not self._stop_called:
                self._stop_called = True
                try:
                    if hook is not None:
                        self.resolve(hook(), closing=True)
                except BaseException as exc:
                    first_error = exc
            # Background work created by astart must also finish before aclose.
            # If it cannot drain, do not pretend it is safe to close resources.
            self._drain(background=True)
            if self._async_work is not None:
                self._async_work.drain(self.timeout)
            hook = getattr(self.owner, "aclose", None) or getattr(self.owner, "close", None)
            if not self._close_called:
                self._close_called = True
                try:
                    if hook is not None:
                        self.resolve(hook(), closing=True)
                except BaseException as exc:
                    first_error = first_error or exc
        finally:
            if self.loop is not None:
                if self._owner_task is not None:
                    async def end_owner():
                        self._owner_release.set()
                        await self._owner_task
                    self.resolve(end_owner(), closing=True)
                self._drain(background=True)
                self._async_work.drain(self.timeout)
                self.loop.call_soon_threadsafe(self.loop.stop)
                self.thread.join(timeout=self.timeout)
                if self.thread.is_alive():
                    raise DataflowError("DEADLINE_EXCEEDED", "operator loop did not stop")
                self.loop.close()
                self._async_work.shutdown(wait=True, cancel_futures=True)
                self.loop = None
        self._closed = True
        if first_error is not None:
            raise first_error
