"""A bounded ordered deployment, reusing the ordinary service lease protocol."""
import asyncio
from contextlib import asynccontextmanager

from .contracts import ModelServiceError, validate_service, service_request_slot


class ServiceGroup:
    """Dependencies first, public endpoint last; acquisition is lazy.

    The ordered list is deliberately not a second graph execution engine.
    Dependencies may themselves be shared declarations. Releasing their lease
    follows their own ownership policy.
    """
    def __init__(self, services):
        if not isinstance(services, (list, tuple)) or not 1 <= len(services) <= 16:
            raise ValueError('ServiceGroup requires 1..16 ordered services')
        for service in services:
            if service is None: raise TypeError('ServiceGroup entries must be service declarations')
            validate_service(service)
            if isinstance(service, ServiceGroup):
                raise ValueError('Nested ServiceGroup declarations are not supported')
        self.services = tuple(services)

    def bind(self, model=None):
        owners = [service.bind(model if index == len(self.services)-1 else None)
                  for index, service in enumerate(self.services)]
        return _GroupLease(owners)


class _GroupLease:
    def __init__(self, owners):
        self.owners = owners
        self._startup = None
        self._closed = False

    @property
    def base_url(self):
        return self.owners[-1].base_url

    async def ensure_ready(self):
        if self._closed:
            raise ModelServiceError('Service group lease is closed')
        if self._startup is None:
            self._startup = asyncio.create_task(self._start())
        await asyncio.shield(self._startup)
        # Owners check process health even after their initial startup.
        for owner in self.owners:
            await owner.ensure_ready()

    async def _start(self):
        try:
            for owner in self.owners:
                await owner.ensure_ready()
        except BaseException as error:
            try: await self._release(rollback=True)
            except BaseException as cleanup:
                if hasattr(error,'add_note'): error.add_note('Service rollback also failed: '+str(cleanup))
            raise

    @asynccontextmanager
    async def request_slot(self):
        async with service_request_slot(self.owners[-1]) as endpoint:
            yield endpoint or self.base_url

    async def _release(self, rollback=False):
        errors = []
        for owner in reversed(self.owners):
            try:
                close = getattr(owner,'rollback',owner.aclose) if rollback else owner.aclose
                await close()
            except BaseException as error:
                errors.append(error)
        if errors:
            raise errors[0]

    async def aclose(self):
        await self._close()

    async def rollback(self):
        await self._close(rollback=True)

    async def _close(self, rollback=False):
        if self._closed:
            return
        self._closed = True
        if self._startup is not None:
            if not self._startup.done():
                self._startup.cancel()
            await asyncio.gather(self._startup, return_exceptions=True)
        await self._release(rollback=rollback)
