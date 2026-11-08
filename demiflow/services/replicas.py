"""Select one independent endpoint per HTTP call; reuse ordinary owner lifecycle.

No proxy, model batching, retries, journals or row executor live here. Replica
admission is one call per instance. Caller concurrency bounds waiting tasks.
"""
import asyncio
from collections import deque
from contextlib import asynccontextmanager

from .contracts import ModelServiceError, service_request_slot, validate_service
from .group import _GroupLease


class ReplicaService:
    def __init__(self, services):
        if not isinstance(services,(tuple,list)) or not 2<=len(services)<=16:
            raise ValueError('ReplicaService requires 2..16 independent services')
        self.services=tuple(validate_service(service) for service in services)
        if any(service is None for service in self.services):
            raise TypeError('Replica entries must be service declarations')

    def bind(self, model=None):
        if model is not None:
            raise ValueError('ReplicaService is bound through model.deployment')
        return _ReplicaLease([service.bind() for service in self.services])


class _ReplicaLease(_GroupLease):
    def __init__(self, owners):
        super().__init__(owners)
        self.available=deque(range(len(owners)))
        self.condition=asyncio.Condition()
        self.accepting=True

    @property
    def base_url(self):
        # For diagnostics/backward compatibility only. Calls obtain their actual
        # endpoint from request_slot; no mutable "current endpoint" is shared.
        return self.owners[0].base_url

    @property
    def base_urls(self):
        return tuple(owner.base_url for owner in self.owners)

    @asynccontextmanager
    async def request_slot(self):
        async with self.condition:
            await self.condition.wait_for(lambda:not self.accepting or self.available)
            if not self.accepting: raise ModelServiceError('Replica service is closed')
            index=self.available.popleft()
        try:
            owner=self.owners[index]
            async with service_request_slot(owner) as endpoint:
                yield endpoint or owner.base_url
        finally:
            async with self.condition:
                self.available.append(index)
                self.condition.notify_all()

    async def _close(self, rollback=False):
        async with self.condition:
            self.accepting=False
            self.condition.notify_all()
        await super()._close(rollback=rollback)
