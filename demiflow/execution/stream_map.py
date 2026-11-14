"""Action-owned callable instances for explicitly configured streaming maps.

Scheduling, queues and thread draining remain in execution.stream. This module
only binds existing row/field callables and owns their lifecycle. A logical
worker may use different pool threads on successive calls: isolation is not
thread affinity. Constructor arguments and fn_args/kwargs retain their existing
reference semantics and are not recursively cloned per worker.
"""
import asyncio
import contextlib
import inspect

from ..data.plan import AsyncMapOp, BatchMapOp, BoundCallable, BoundMapOp, StandardCallable


class _MapWorker:
    def __init__(self, operation, recovery_state=None, recovery_key=None):
        self.operation = operation
        self.callable = None
        self.started = False
        self.failure = None
        self.start_lock = asyncio.Lock()
        self.stopped = False
        self.closed = False
        self.recovery_state, self.recovery_key = recovery_state, recovery_key

    async def prepare(self):
        # Shared stage instances need exactly one constructor/start, including
        # when several workers reach their first input concurrently.
        async with self.start_lock:
            if self.failure is not None:
                raise self.failure
            if not self.started:
                started = False
                try:
                    self.callable = (BoundCallable(self.operation)
                                     if isinstance(self.operation, BoundMapOp)
                                     else StandardCallable(self.operation.callable))
                    from .checked import current
                    checked = current()
                    if checked is not None:
                        checked.actor_created(self.callable._fn, checked.operation_bindings.get(id(self.operation)))
                    if self.recovery_state is not None:
                        self.recovery_state.validate(self.callable._fn)
                    await self._hook('astart')
                    started = True
                    if self.recovery_state is not None:
                        await self.recovery_state.register(self.callable._fn, self.recovery_key)
                    self.started = True
                except BaseException as exc:
                    self.failure = exc
                    if started:
                        # An instance whose astart succeeded but whose registration
                        # failed/cancelled must be released here; the action-level
                        # stop/aclose is only an idempotent fallback. Shield so a
                        # cancelled registration still drains the started actor.
                        async def release():
                            await self.astop()
                            await self.aclose()
                        task = asyncio.create_task(release())
                        try:
                            await asyncio.shield(task)
                        except asyncio.CancelledError:
                            with contextlib.suppress(BaseException):
                                await task
                        except BaseException:
                            pass
                    raise

    async def _hook(self, name):
        if self.callable is not None:
            hook = getattr(self.callable._fn, name, None)
            if hook is not None:
                from .checked import current
                checked = current()
                completed = False
                try:
                    result = hook()
                    if inspect.isawaitable(result):
                        await result
                    completed = True
                finally:
                    if checked is not None:
                        checked.actor_lifecycle(self.callable._fn, {'astart': 'started', 'astop': 'stopped', 'aclose': 'closed'}[name], completed)

    def __call__(self, row):
        result = self.callable(row)
        if inspect.isawaitable(result) and not isinstance(self.operation, (AsyncMapOp, BatchMapOp)):
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
        options = getattr(operation, 'stream_options', None)
        self.scope = options.callable_scope if options is not None else 'stage'
        self.recovery_state = self.recovery_key = None

    def new_worker(self):
        if self.scope == 'stage' and self.workers:
            return self.workers[0]
        worker = _MapWorker(self.operation, self.recovery_state,
                            (self.recovery_key + '-worker-' + str(len(self.workers)))
                            if self.recovery_key is not None else None)
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
