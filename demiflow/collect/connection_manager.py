"""Execution-owned HTTP clients and bounded, asynchronous pool maintenance.

HTTPX/httpcore still own sockets and request-to-connection assignment. This
manager owns client lifetimes, aggregate declared capacity and idle cleanup.
Native search workers own their curl/browser pools; those are deliberately not
reported as part of this manager's HTTPX connection budget.
"""
import asyncio
import math
from dataclasses import asdict, dataclass

import httpx


@dataclass(frozen=True)
class ConnectionPolicy:
    max_clients: int = 128
    max_total_connections: int = 2048
    max_connections_per_client: int = 128
    keepalive_expiry_s: float = 5.
    maintenance_interval_s: float = 5.
    close_timeout_s: float = 10.

    def __post_init__(self):
        for key, ceiling in (('max_clients', 4096),
                             ('max_total_connections', 65536),
                             ('max_connections_per_client', 4096)):
            value = getattr(self, key)
            if type(value) is not int or not 1 <= value <= ceiling:
                raise ValueError('Invalid connection policy: ' + key)
        for key in ('keepalive_expiry_s', 'maintenance_interval_s', 'close_timeout_s'):
            value = getattr(self, key)
            if type(value) not in (int, float) or not math.isfinite(value) or not 0 < value <= 3600:
                raise ValueError('Invalid connection policy: ' + key)
        if self.maintenance_interval_s < .01:
            raise ValueError('Connection maintenance interval must be at least 0.01 seconds')


def connection_policy(value=None):
    if value is None:
        return ConnectionPolicy()
    if isinstance(value, ConnectionPolicy):
        return value
    if not isinstance(value, dict):
        raise TypeError('connection_policy must be a mapping or ConnectionPolicy')
    return ConnectionPolicy(**value)


class ConnectionCapacityError(RuntimeError):
    """Local client/connection budget, not a remote website or proxy failure."""


class ConnectionManager:
    def __init__(self, policy=None):
        self.policy = connection_policy(policy)
        self.clients = {}
        self.loop = None
        self.maintenance = None
        self.closing = None
        self.closed = False
        self.failure = None
        self.reserved = 0
        self.metrics = dict(clients_created=0, clients_closed=0, peak_clients=0,
                            peak_reserved_connections=0, maintenance_runs=0,
                            idle_connections_closed=0, close_errors=0)

    def start(self):
        loop = asyncio.get_running_loop()
        if self.closed or self.closing is not None:
            raise RuntimeError('connection_manager_closed')
        if self.loop is not None and self.loop is not loop:
            raise RuntimeError('Connection manager belongs to another event loop')
        if self.failure is not None:
            raise RuntimeError('connection_maintenance_failed:' + self.failure)
        if self.loop is None:
            self.loop = loop
            self.maintenance = loop.create_task(self._maintain(), name='demiflow-connection-maintenance')

    def client(self, key, *, proxy, capacity, keepalive, timeout, headers):
        self.start()
        if key in self.clients:
            return self.clients[key]['client']
        if type(capacity) is not int or not 1 <= capacity <= self.policy.max_connections_per_client:
            raise ConnectionCapacityError('connection_capacity_per_client')
        if type(keepalive) is not int or not 0 <= keepalive <= capacity:
            raise ValueError('Invalid idle connection capacity')
        if len(self.clients) >= self.policy.max_clients:
            raise ConnectionCapacityError('connection_client_capacity')
        if self.reserved + capacity > self.policy.max_total_connections:
            raise ConnectionCapacityError('connection_total_capacity')
        from .http_transport import RetrievalTransport
        transport = RetrievalTransport(proxy=proxy, limits=httpx.Limits(
            max_connections=capacity, max_keepalive_connections=keepalive,
            keepalive_expiry=self.policy.keepalive_expiry_s))
        client = httpx.AsyncClient(transport=transport, timeout=timeout,
            follow_redirects=False, trust_env=False, headers=headers)
        self.clients[key] = dict(client=client, transport=transport, capacity=capacity,
                                 lock=asyncio.Lock())
        self.reserved += capacity
        self.metrics['clients_created'] += 1
        self.metrics['peak_clients'] = max(self.metrics['peak_clients'], len(self.clients))
        self.metrics['peak_reserved_connections'] = max(self.metrics['peak_reserved_connections'], self.reserved)
        return client

    async def maintain_once(self):
        # Both dimensions are policy-bounded. No URL/request history is retained.
        for entry in tuple(self.clients.values()):
            async with entry['lock']:
                if entry['client'].is_closed:
                    continue
                async with asyncio.timeout(self.policy.close_timeout_s):
                    self.metrics['idle_connections_closed'] += await entry['transport'].reap_idle_connections()
        self.metrics['maintenance_runs'] += 1

    async def _maintain(self):
        try:
            while True:
                await asyncio.sleep(self.policy.maintenance_interval_s)
                await self.maintain_once()
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            # Keep diagnostics bounded and credential-free, and fail the next
            # admission/close instead of silently losing the maintenance task.
            self.failure = type(exc).__name__

    async def close_client(self, client):
        found = next(((key, entry) for key, entry in self.clients.items()
                      if entry['client'] is client), None)
        if found is None:
            # Compatibility for explicitly supplied test/custom clients.
            async with asyncio.timeout(self.policy.close_timeout_s):
                await client.aclose()
            return
        key, entry = found
        # Retain capacity until close succeeds: a failed close must not create
        # room for unlimited replacement clients while old sockets remain.
        async with entry['lock']:
            try:
                async with asyncio.timeout(self.policy.close_timeout_s):
                    await client.aclose()
            except BaseException:
                self.metrics['close_errors'] += 1
                raise
            if self.clients.get(key) is entry:
                del self.clients[key]
                self.reserved -= entry['capacity']
                self.metrics['clients_closed'] += 1

    def snapshot_metrics(self):
        values = [entry['transport'].connection_counts() for entry in self.clients.values()]
        return {
            **self.metrics, 'scope': 'httpx_clients_in_this_execution',
            'policy': asdict(self.policy), 'clients': len(self.clients),
            'reserved_connection_capacity': self.reserved,
            'connections': sum(v['connections'] for v in values),
            'active_connections': sum(v['active_connections'] for v in values),
            'idle_connections': sum(v['idle_connections'] for v in values),
            'waiting_requests': sum(v['waiting_requests'] for v in values),
            'closed': self.closed,
            'maintenance_running': self.maintenance is not None and not self.maintenance.done(),
            'maintenance_error': self.failure,
        }

    async def _close(self):
        self.closed = True
        if self.maintenance is not None:
            self.maintenance.cancel()
            await asyncio.gather(self.maintenance, return_exceptions=True)
        # At most max_clients close tasks, each with its own timeout; independent
        # slow closes do not multiply the shutdown deadline by the client count.
        results = await asyncio.gather(*(self.close_client(entry['client'])
            for entry in tuple(self.clients.values())), return_exceptions=True)
        errors = [error for error in results if isinstance(error, Exception)]
        if self.failure is not None:
            errors.append(RuntimeError('connection_maintenance_failed:' + self.failure))
        if errors:
            raise ExceptionGroup('Connection cleanup failed', errors)

    async def aclose(self):
        if self.loop is not None and self.loop is not asyncio.get_running_loop():
            raise RuntimeError('Connection manager belongs to another event loop')
        if self.closing is None:
            self.closing = asyncio.create_task(self._close())
        try:
            await asyncio.shield(self.closing)
        except asyncio.CancelledError:
            await self.closing
            raise


class WebOperatorLifecycle:
    """Optional lifecycle protocol; custom sessions retain their old contract."""
    _session_lease = None

    async def astart(self):
        acquire = getattr(self.session, 'acquire_operator', None)
        if acquire is not None:
            await acquire(self)
            self._session_lease = asyncio.current_task()

    async def astop(self):
        if self._session_lease is asyncio.current_task():
            self._session_lease = None
            await self.session.release_operator(self)

    async def aclose(self):
        await self.astop()
