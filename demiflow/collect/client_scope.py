"""Borrow loop-local operator clients until their last owner closes."""
import asyncio
from contextlib import contextmanager
from contextvars import ContextVar
from functools import partial
from threading import RLock

_current = ContextVar('demiflow_operator_client_borrows', default=None)


class ClientScope:
    def __init__(self):
        self._borrows = {}
        self._lock = RLock()
        self.closed = False

    @contextmanager
    def active(self):
        with self._lock:
            if self.closed:
                raise RuntimeError('Operator client scope is closed')
        token = _current.set(self)
        try:
            yield
        finally:
            _current.reset(token)

    def borrow(self, pool, key, create):
        with self._lock:
            if self.closed:
                raise RuntimeError('Operator client scope is closed')
            token = (pool, key)
            if token not in self._borrows:
                self._borrows[token] = pool.acquire(key, self, create)
            return self._borrows[token][0]

    async def close(self):
        with self._lock:
            self.closed = True
            releases = tuple(self._borrows.values())
            self._borrows.clear()
        errors = []
        for _, release in reversed(releases):
            try:
                await release()
            except BaseException as exc:
                errors.append(exc)
        if errors:
            raise BaseExceptionGroup('Operator client release failed', errors)


class ClientPool:
    """One provider's clients. Unscoped callers own their explicit close."""
    def __init__(self, close=None):
        self._entries = {}
        self._close = close
        self._lock = RLock()

    def get(self, key, create):
        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            loop = None
        key = (loop, key)
        scope = _current.get()
        if scope is None:
            return self.acquire(key, None, create)[0]
        return scope.borrow(self, key, create)

    def acquire(self, key, scope, create):
        with self._lock:
            entry = self._entries.get(key)
            if entry is None:
                entry = (create(), set())
                self._entries[key] = entry
            value, owners = entry
            owners.add(scope)
            return value, partial(self._release, key, entry, scope)

    async def _release(self, key, entry, owner):
        with self._lock:
            if self._entries.get(key) is not entry:
                return
            value, owners = entry
            owners.discard(owner)
            if owners:
                return
            del self._entries[key]
        if self._close is not None:
            await self._close(value)

    async def close_unscoped(self):
        loop = asyncio.get_running_loop()
        with self._lock:
            pending = [(key, entry) for key, entry in self._entries.items()
                       if key[0] in (None, loop) and None in entry[1]]
        for key, entry in pending:
            await self._release(key, entry, None)

    def items(self):
        with self._lock:
            return [(key, value) for (_, key), (value, _) in self._entries.items()]
