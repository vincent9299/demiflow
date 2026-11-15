"""One event loop for an operator's async lifecycle and calls, inside its worker."""
from __future__ import annotations

import asyncio
import concurrent.futures
import inspect
import threading
import time

from ..contracts.dataflow_errors import DataflowError


class _AsyncWorkPool(concurrent.futures.ThreadPoolExecutor):
    """Keep the real futures that cancelling asyncio.to_thread cannot stop."""
    def __init__(self):
        super().__init__(thread_name_prefix="demiflow-operator-async-work")
        self._active = set()
        self._active_lock = threading.Lock()

    def submit(self, fn, /, *args, **kwargs):
        future = super().submit(fn, *args, **kwargs)
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
    def __init__(self, target, services, *, timeout=None, owner=None):
        self.target = target
        self.owner = target if owner is None else owner
        self.services = services
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

    def __getattr__(self, name):
        return getattr(self.target, name)

    def resolve(self, value, *, closing=False):
        if not inspect.isawaitable(value):
            return value
        if self.loop is None:
            with self._loop_lock:
                if self.loop is None:
                    self.loop = asyncio.new_event_loop()
                    self._async_work = _AsyncWorkPool()
                    self.loop.set_default_executor(self._async_work)
                    self.thread = threading.Thread(target=self.loop.run_forever,
                                                   name="demiflow-operator-loop", daemon=True)
                    self.thread.start()
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
                    return future.result(timeout=min(.05, remaining))
                except concurrent.futures.TimeoutError:
                    if future.done():
                        return future.result()
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
            self.resolve(hook())

    def __call__(self, value):
        if self._stopping.is_set():
            raise DataflowError("CANCELLED", "operator is stopping")
        return self.resolve(self.target(value))

    def request_stop(self):
        """Interrupt async calls before waiting for invocation threads to drain."""
        self._stopping.set()

    def _drain(self, *, background=False):
        if self.loop is None:
            return
        async def cancel_pending():
            tasks = (asyncio.all_tasks() if background else set(self._tasks))
            tasks.discard(asyncio.current_task())
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
