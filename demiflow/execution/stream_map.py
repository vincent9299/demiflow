"""Action-owned callable instances for explicitly configured streaming maps.

Scheduling, queues and thread draining remain in execution.stream. This module
only binds existing row/field callables and owns their lifecycle. A logical
worker may use different pool threads on successive calls: isolation is not
thread affinity. Constructor arguments and fn_args/kwargs retain their existing
reference semantics and are not recursively cloned per worker.
"""
import asyncio
import inspect

from ..data.plan import BoundCallable, BoundMapOp, StandardCallable


class _MapWorker:
    def __init__(self, operation):
        self.operation = operation
        self.callable = None
        self.started = False
        self.failure = None
        self.start_lock = asyncio.Lock()
        self.stopped = False
        self.closed = False

    async def prepare(self):
        # Shared stage instances need exactly one constructor/start, including
        # when several workers reach their first input concurrently.
        async with self.start_lock:
            if self.failure is not None:
                raise self.failure
            if not self.started:
                try:
                    self.callable = (BoundCallable(self.operation)
                                     if isinstance(self.operation, BoundMapOp)
                                     else StandardCallable(self.operation.callable))
                    await self._hook('astart')
                    self.started = True
                except BaseException as exc:
                    self.failure = exc
                    raise

    async def _hook(self, name):
        if self.callable is not None:
            hook = getattr(self.callable._fn, name, None)
            if hook is not None:
                result = hook()
                if inspect.isawaitable(result):
                    await result

    def __call__(self, row):
        result = self.callable(row)
        if inspect.isawaitable(result):
            if inspect.iscoroutine(result):
                result.close()
            raise TypeError('map synchronous callable returned an awaitable; use map_async')
        return result

    async def astop(self):
        if not self.stopped:
            self.stopped = True
            await self._hook('astop')

    async def aclose(self):
        if not self.closed:
            self.closed = True
            await self._hook('aclose')


class StreamMapRuntime:
    def __init__(self, operation):
        self.operation = operation
        self.workers = []

    def new_worker(self):
        if self.operation.stream_options.callable_scope == 'stage' and self.workers:
            return self.workers[0]
        worker = _MapWorker(self.operation)
        self.workers.append(worker)
        return worker

    async def _all(self, hook):
        errors = []
        for worker in self.workers:
            try:
                await getattr(worker, hook)()
            except Exception as exc:
                errors.append(exc)
        if errors:
            raise ExceptionGroup(f'Stream map {hook} failed', errors)

    async def astop(self):
        await self._all('astop')

    async def aclose(self):
        await self._all('aclose')
