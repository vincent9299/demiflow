"""One event loop for an operator's async lifecycle and calls, inside its worker."""
from __future__ import annotations

import asyncio
import concurrent.futures
import inspect
import threading
import time

from ..contracts.dataflow_errors import DataflowError


class AsyncInstance:
    def __init__(self, target, services, *, timeout=None):
        self.target = target
        self.services = services
        self.timeout = timeout or services.bounds.timeout_ms / 1000
        self.loop = None
        self.thread = None

    def __getattr__(self, name):
        return getattr(self.target, name)

    def resolve(self, value, *, closing=False):
        if not inspect.isawaitable(value):
            return value
        if self.loop is None:
            self.loop = asyncio.new_event_loop()
            self.thread = threading.Thread(target=self.loop.run_forever,
                                           name="demiflow-operator-loop", daemon=True)
            self.thread.start()
        async def wait():
            return await value
        future = asyncio.run_coroutine_threadsafe(wait(), self.loop)
        deadline = time.monotonic() + self.timeout
        try:
            while True:
                if not closing:
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
        hook = getattr(self.target, "astart", None)
        if hook is not None:
            self.resolve(hook())

    def __call__(self, value):
        return self.resolve(self.target(value))

    def close(self):
        try:
            hook = getattr(self.target, "aclose", None) or getattr(self.target, "close", None)
            if hook is not None:
                self.resolve(hook(), closing=True)
        finally:
            if self.loop is not None:
                async def cancel_pending():
                    tasks = [task for task in asyncio.all_tasks() if task is not asyncio.current_task()]
                    for task in tasks:
                        task.cancel()
                    if tasks:
                        await asyncio.gather(*tasks, return_exceptions=True)
                try:
                    self.resolve(cancel_pending(), closing=True)
                finally:
                    self.loop.call_soon_threadsafe(self.loop.stop)
                    self.thread.join(timeout=self.timeout)
                    if not self.thread.is_alive():
                        self.loop.close()
                    self.loop = None
