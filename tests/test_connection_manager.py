"""Real connection boundaries and execution-owned maintenance tasks."""
import asyncio
import threading
from contextlib import contextmanager
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import httpx
import pytest

from demiflow.collect.connection_manager import ConnectionManager, ConnectionCapacityError
from demiflow.collect.web import WebClient


@contextmanager
def local_server():
    class Handler(BaseHTTPRequestHandler):
        protocol_version = 'HTTP/1.1'
        def do_GET(self):
            self.send_response(200)
            self.send_header('Content-Length', '4')
            self.end_headers()
            self.wfile.write(b'body')
        def log_message(self, *args):
            pass
    server = ThreadingHTTPServer(('127.0.0.1', 0), Handler)
    server.daemon_threads = True
    thread = threading.Thread(target=lambda: server.serve_forever(poll_interval=.02), daemon=True)
    thread.start()
    try:
        yield 'http://127.0.0.1:' + str(server.server_address[1])
    finally:
        server.shutdown()
        server.server_close()
        thread.join(2)
        assert not thread.is_alive()


def client(manager, key='one', capacity=1):
    return manager.client(key, proxy=None, capacity=capacity, keepalive=capacity,
                          timeout=httpx.Timeout(1), headers={})


async def test_real_idle_socket_is_reaped_without_another_request():
    manager = ConnectionManager({'keepalive_expiry_s': .02, 'maintenance_interval_s': .01})
    with local_server() as url:
        c = client(manager)
        try:
            response = await c.get(url)
            assert response.content == b'body'
            assert manager.snapshot_metrics()['idle_connections'] == 1
            async with asyncio.timeout(1):
                while manager.snapshot_metrics()['idle_connections']:
                    await asyncio.sleep(.01)
            assert manager.snapshot_metrics()['idle_connections_closed'] == 1
            assert not c.is_closed
            assert (await c.get(url)).content == b'body'
        finally:
            await manager.aclose()
    assert c.is_closed and manager.maintenance.done()
    assert manager.snapshot_metrics()['reserved_connection_capacity'] == 0
    assert manager.snapshot_metrics()['clients_closed'] == 1


async def test_declared_capacity_is_bounded_and_released_on_retirement():
    manager = ConnectionManager({'max_clients': 2, 'max_total_connections': 2,
                                 'max_connections_per_client': 2})
    try:
        first = client(manager, capacity=2)
        assert client(manager, capacity=2) is first
        with pytest.raises(ConnectionCapacityError, match='total_capacity'):
            client(manager, 'two')
        with pytest.raises(ConnectionCapacityError, match='per_client'):
            client(manager, 'huge', capacity=3)
        assert manager.snapshot_metrics()['clients_created'] == 1
        await manager.close_client(first)
        second = client(manager, 'two', capacity=2)
        assert not second.is_closed and first.is_closed
        assert manager.snapshot_metrics()['reserved_connection_capacity'] == 2
    finally:
        await manager.aclose()
    assert not manager.clients and manager.maintenance.done()


async def test_request_cancellation_and_close_leave_no_owned_connections():
    entered = asyncio.Event()
    release = asyncio.Event()
    handlers = set()
    async def serve(reader, writer):
        task = asyncio.current_task()
        handlers.add(task)
        try:
            await reader.readuntil(b'\r\n\r\n')
            entered.set()
            await release.wait()
        finally:
            writer.close()
            await writer.wait_closed()
            handlers.discard(task)
    server = await asyncio.start_server(serve, '127.0.0.1', 0)
    manager = ConnectionManager()
    c = client(manager)
    request = asyncio.create_task(c.get('http://127.0.0.1:' + str(server.sockets[0].getsockname()[1])))
    try:
        await asyncio.wait_for(entered.wait(), 1)
        assert manager.snapshot_metrics()['active_connections'] == 1
        request.cancel()
        with pytest.raises(asyncio.CancelledError):
            await request
        await manager.aclose()
        assert c.is_closed and manager.maintenance.done()
        assert manager.snapshot_metrics()['connections'] == 0
    finally:
        release.set()
        await manager.aclose()
        server.close()
        await server.wait_closed()
        await asyncio.gather(*tuple(handlers), return_exceptions=True)


async def test_local_pool_timeout_does_not_cool_proxy_exit(tmp_path):
    web = WebClient(cache_path=tmp_path/'calls.sqlite', object_directory=tmp_path/'objects',
        fetch_proxy_routes={'*': {'pool': [{'name': 'fixture', 'proxy': 'http://proxy.example:3128',
            'concurrency': 1, 'interval_s': 0}], 'health_scope': 'host'}},
        host_interval_s=0, retries=2)
    calls = []
    async def hop(*args, **kwargs):
        calls.append(1)
        raise httpx.PoolTimeout('local wait only')
    web._hop = hop
    try:
        result = await web._get('https://example.org/a')
        assert result['status'] == 'capacity_error' and len(calls) == 1
        with web._db() as db:
            assert db.execute('select count(*) from fetch_route_host_health').fetchone()[0] == 0
        assert web.fetch_route_pools['*'].routes[0]['busy'] == 0
    finally:
        await web.aclose()


async def test_failed_close_does_not_free_capacity(tmp_path, monkeypatch):
    manager = ConnectionManager({'max_total_connections': 1})
    c = client(manager)
    real_close = c.aclose
    async def fail():
        raise OSError('fixture close failed')
    monkeypatch.setattr(c, 'aclose', fail)
    try:
        with pytest.raises(OSError):
            await manager.close_client(c)
        with pytest.raises(ConnectionCapacityError):
            client(manager, 'replacement')
        assert manager.snapshot_metrics()['reserved_connection_capacity'] == 1
    finally:
        monkeypatch.setattr(c, 'aclose', real_close)
        await manager.aclose()


async def test_idle_batch_closes_independently_within_one_deadline(monkeypatch):
    """Several slow idle closes must not multiply the pool deadline."""
    manager = ConnectionManager({'close_timeout_s': .08, 'maintenance_interval_s': 60})
    c = client(manager, capacity=4)
    entry = next(iter(manager.clients.values()))
    closed = []
    class SlowIdle:
        async def aclose(self):
            await asyncio.sleep(.04)
            closed.append(self)
    detached = [SlowIdle() for _ in range(4)]
    monkeypatch.setattr(entry['transport']._pool, '_assign_requests_to_connections', lambda: detached)
    try:
        await manager.maintain_once()
        assert len(closed) == 4
        assert manager.snapshot_metrics()['idle_connections_closed'] == 4
    finally:
        await manager.aclose()


async def test_idle_close_failure_still_closes_other_detached_sockets(monkeypatch):
    manager = ConnectionManager({'maintenance_interval_s': 60})
    client(manager, capacity=2)
    entry = next(iter(manager.clients.values()))
    closed = []
    class Idle:
        def __init__(self, fail): self.fail = fail
        async def aclose(self):
            if self.fail: raise OSError('fixture idle close failed')
            await asyncio.sleep(.01)
            closed.append(self)
    monkeypatch.setattr(entry['transport']._pool, '_assign_requests_to_connections',
                        lambda: [Idle(True), Idle(False)])
    try:
        with pytest.raises(ExceptionGroup, match='Idle connection cleanup failed'):
            await manager.maintain_once()
        assert len(closed) == 1
    finally:
        await manager.aclose()
