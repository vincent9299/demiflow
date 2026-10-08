"""Owned HTTPX transport with failed CONNECT/TLS setup cleanup.

httpcore 1.0.9 can leave a tunnel ACTIVE after TLS setup fails, even when its
socket has closed. Such entries never return pool capacity (upstream #921).
This local adapter does not patch installed modules or change TLS validation.
HTTPX 0.28 / httpcore 1.0 behavior is covered by real-socket regression tests.
"""
import asyncio
import httpcore
import httpx
from httpcore._async.http_proxy import AsyncTunnelHTTPConnection
from httpcore._synchronization import AsyncShieldCancellation


class _Tunnel(AsyncTunnelHTTPConnection):
    async def handle_async_request(self, request):
        try:
            return await super().handle_async_request(request)
        except BaseException:
            if not self._connected:
                # Includes cancellation/timeout during the handshake. Closing
                # only the network stream leaves the owning connection ACTIVE.
                with AsyncShieldCancellation():
                    await self.aclose()
            raise


class _ProxyPool(httpcore.AsyncHTTPProxy):
    def create_connection(self, origin):
        if origin.scheme == b'http':
            return super().create_connection(origin)
        return _Tunnel(
            proxy_origin=self._proxy_url.origin, proxy_headers=self._proxy_headers,
            remote_origin=origin, ssl_context=self._ssl_context,
            proxy_ssl_context=self._proxy_ssl_context,
            keepalive_expiry=self._keepalive_expiry, http1=self._http1,
            http2=self._http2, network_backend=self._network_backend,
        )


class RetrievalTransport(httpx.AsyncHTTPTransport):
    """Keep HTTPX's request/error mapping and explicit finite pool limits."""
    def __init__(self, *, proxy=None, limits):
        declaration=httpx.Proxy(proxy) if isinstance(proxy,(str,httpx.URL)) else proxy
        if declaration is None or declaration.url.scheme not in ('http','https'):
            super().__init__(proxy=proxy,limits=limits,trust_env=False)
            return
        self._pool=_ProxyPool(
            proxy_url=httpcore.URL(scheme=declaration.url.raw_scheme,
                host=declaration.url.raw_host,port=declaration.url.port,
                target=declaration.url.raw_path),
            proxy_auth=declaration.raw_auth,proxy_headers=declaration.headers.raw,
            proxy_ssl_context=declaration.ssl_context,
            ssl_context=httpx.create_ssl_context(verify=True,trust_env=False),
            max_connections=limits.max_connections,
            max_keepalive_connections=limits.max_keepalive_connections,
            keepalive_expiry=limits.keepalive_expiry,
        )

    def connection_counts(self):
        # These httpcore extension points are covered with the same pinned
        # HTTPX/httpcore compatibility tests as CONNECT/TLS failure cleanup.
        connections = self._pool.connections
        opened = [connection for connection in connections if not connection.is_closed()]
        idle = sum(connection.is_idle() for connection in opened)
        return {'connections': len(opened), 'idle_connections': idle,
                'active_connections': len(opened) - idle,
                'waiting_requests': sum(request.is_queued() for request in self._pool._requests)}

    async def reap_idle_connections(self):
        # Let httpcore apply its own expiry/idle rules and wake queued requests.
        # Never infer that a legitimate in-flight connection is stale from age.
        with self._pool._optional_thread_lock:
            closing = self._pool._assign_requests_to_connections()
        # The pool has already detached these connections. Serial shutdown can
        # consume the whole maintenance deadline on one slow socket and leave
        # every later detached socket unclosed. The list is bounded by the
        # declared per-client capacity; attempt every independent close within
        # the same deadline and retain failures for the owner's diagnostics.
        results = await asyncio.gather(*(connection.aclose() for connection in closing),
                                       return_exceptions=True)
        errors = [result for result in results if isinstance(result, BaseException)]
        if errors:
            raise BaseExceptionGroup('Idle connection cleanup failed', errors)
        return len(closing)
