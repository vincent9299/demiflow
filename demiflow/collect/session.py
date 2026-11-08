"""Lazy search resources; raw object acquisition belongs to Dataset.fetch."""
import asyncio
import inspect
import math
from .web import WebClient


class WebSession:
    def __init__(self, *, service=None, search=None, search_request_interval_s=0, search_reuse_configs=(),
                 search_reuse_session_pools=(),
                 search_routes=(), search_route_cooldown_s=1800, search_route_attempts=2,
                 search_route_failure_limit=2, search_adaptive=None, search_session_pool=None,
                 search_failure_scope="pool",
                 search_fallback=None, connection_policy=None,
                 proxy_policies=None, **options):
        if proxy_policies is not None:
            if search_routes or search_session_pool:
                raise ValueError('Choose proxy_policies or legacy proxy declarations')
            from .proxy_policy import compile_proxy_policies
            compiled = compile_proxy_policies(proxy_policies)
            selected = compiled['selected']
            search_routes = selected.get('search_routes', ())
            search_session_pool = selected.get('search_session_pool')
        from .native_search import SearchConfig
        if isinstance(search, dict):
            search = SearchConfig.from_mapping(search)
        if search is not None and (service is not None or options.get("search_url")):
            raise ValueError("Choose native search or an explicit legacy HTTP backend")
        if search is None and service is None and not options.get("search_url"):
            search = SearchConfig()
        if search is not None and not isinstance(search, SearchConfig):
            raise TypeError("search must be SearchConfig")
        if search is not None:
            legacy = [k for k in options if k.startswith('search_') and k != 'search_url']
            if legacy:
                raise ValueError('Native search options belong in SearchConfig: ' + ', '.join(legacy))
        import copy
        self.search_config = copy.deepcopy(search)
        from .search_reuse import completed_reuse_configs
        self.search_reuse_configs = completed_reuse_configs(search, search_reuse_configs)
        from .search_routes import route_declarations, historical_pool_references
        self.search_routes = route_declarations(search,search_routes)
        self.search_reuse_session_pools = historical_pool_references(search, search_reuse_session_pools)
        from .search_sessions import session_pool_policy
        self.search_session_pool=session_pool_policy(search_session_pool)
        if self.search_reuse_session_pools and not (self.search_routes or self.search_session_pool):
            raise ValueError('Historical search pool reuse requires current search routes')
        from .search_fallback import fallback_policy
        self.search_fallback=fallback_policy(search_fallback)
        if self.search_fallback and not (self.search_routes or self.search_session_pool):
            raise ValueError('Search fallback requires a primary route pool')
        if type(search_route_cooldown_s) not in (int,float) or not math.isfinite(search_route_cooldown_s) or not 0<search_route_cooldown_s<=86400:
            raise ValueError('Invalid search route cooldown')
        if type(search_route_attempts) is not int or search_route_attempts<1:
            raise ValueError('Invalid search route attempt limit')
        if self.search_routes and not self.search_session_pool and search_route_attempts>len(self.search_routes):
            raise ValueError('Route attempt limit exceeds declared routes')
        self.search_route_cooldown_s=search_route_cooldown_s
        self.search_route_attempts=search_route_attempts
        if type(search_route_failure_limit) is not int or search_route_failure_limit<1:
            raise ValueError('Invalid search route failure limit')
        self.search_route_failure_limit=search_route_failure_limit
        if search_failure_scope not in {'pool', 'source'}:
            raise ValueError('search_failure_scope must be pool or source')
        if search_failure_scope == 'source' and (not self.search_routes or self.search_session_pool):
            raise ValueError('Source failure isolation requires a static search route pool')
        self.search_failure_scope=search_failure_scope
        from .search_admission import adaptive_policy
        self.search_adaptive=adaptive_policy(search_adaptive)
        if self.search_adaptive and not (self.search_routes or self.search_session_pool):
            raise ValueError('Adaptive search requires an explicit route pool')
        if type(search_request_interval_s) not in (int, float) or not math.isfinite(search_request_interval_s) or search_request_interval_s < 0:
            raise ValueError('search_request_interval_s must be finite and nonnegative')
        if search_request_interval_s and search is None:
            raise ValueError('search_request_interval_s requires native search')
        # Runtime-wide HTTP admission is scheduling, not request identity. This
        # layer can tighten pacing without invalidating completed search receipts.
        self.search_request_interval_s = search_request_interval_s
        self.native = None
        # Declaration validates arguments without creating directories/clients.
        if '_connection_manager' in options:
            raise ValueError('Connection manager is owned by operator execution')
        inspect.signature(WebClient).bind(**options)
        for key, value in options.items():
            if key.endswith(('_concurrency', '_bytes', '_s')) or key in {'redirects','retries','failure_limit'}:
                if type(value) not in (int, float) or not math.isfinite(value) or value < 0:
                    raise ValueError('Invalid retrieval option: '+key)
                if key not in {'host_interval_s','search_interval_s','retry_delay_s','redirects','retries'} and not value:
                    raise ValueError('Retrieval option must be positive: '+key)
        if service is not None and hasattr(service,'search_url'):
            if options['search_url'] != service.search_url: raise ValueError('Search URL differs from managed profile')
            options['search_profile'] = service.fingerprint
            options['search_engines'] = list(service.engines)
        self.options = dict(options)
        self.service = service
        self.client = None
        self.owner = None
        from .connection_manager import connection_policy as normalize_connection_policy
        self.connection_policy = normalize_connection_policy(connection_policy)
        self.connections = None
        self._operator_owners = set()
        self._operator_task = None
        self._closing_task = None
        self._last_metrics = None

    def _ensure_connections(self):
        if self._closing_task is not None:
            if not self._closing_task.done():
                raise RuntimeError('WebSession is closing')
            self._closing_task.result()
        if self.connections is None or self.connections.closed:
            from .connection_manager import ConnectionManager
            self.connections = ConnectionManager(self.connection_policy)
            self._closing_task = None
            self._last_metrics = None
        self.connections.start()
        return self.connections

    async def acquire_operator(self, owner):
        if self._operator_task is not None and self._operator_task is not asyncio.current_task():
            raise RuntimeError('WebSession is already owned by another action')
        self._ensure_connections()
        if len(self._operator_owners) >= 4096 and id(owner) not in self._operator_owners:
            raise ValueError('Too many network operators sharing one WebSession')
        self._operator_owners.add(id(owner))
        self._operator_task = asyncio.current_task()

    async def release_operator(self, owner):
        if id(owner) not in self._operator_owners:
            return
        if self._operator_task is not asyncio.current_task():
            raise RuntimeError('WebSession operator belongs to another action')
        self._operator_owners.remove(id(owner))
        if not self._operator_owners:
            await self.aclose()

    def _client(self):
        if self.client is None:
            self.client = WebClient(**self.options, _connection_manager=self._ensure_connections())
            if self.service is not None:
                self.owner = self.service.bind()
                self.client.before_search = self.owner.ensure_ready
                self.client.search_slots = getattr(self.owner,'request_slot',None)
        return self.client

    async def search(self, query, *, fallback_parameters=None, **parameters):
        self._ensure_connections()
        if fallback_parameters is not None and self.search_fallback is None:
            raise ValueError('Fallback parameters require a fallback declaration')
        if self.search_config is not None:
            if self.native is None:
                from .native_search import NativeSearchSession
                self.native = NativeSearchSession(cache_path=self.options['cache_path'], config=self.search_config,
                    execution_admission=getattr(self,'_native_execution_admission',None),
                    control_bytes=getattr(self,'_native_control_bytes',None),max_hosts=getattr(self,'_native_max_hosts',None))
                if self.search_routes or self.search_session_pool:
                    from .search_routes import SearchRoutePool
                    pool_type=SearchRoutePool
                    extra={}
                    if self.search_session_pool:
                        from .search_sessions import RenewableSearchRoutePool
                        pool_type=RenewableSearchRoutePool
                        extra['session_pool']=self.search_session_pool
                    self.native=pool_type(cache_path=self.options['cache_path'],config=self.search_config,
                        routes=self.search_routes,reuse_configs=self.search_reuse_configs,
                        reuse_session_pools=self.search_reuse_session_pools,
                        cooldown_s=self.search_route_cooldown_s,max_route_attempts=self.search_route_attempts,
                        failure_limit=self.search_route_failure_limit,adaptive=self.search_adaptive,
                        failure_scope=self.search_failure_scope,**extra)
                elif self.search_reuse_configs:
                    from .search_reuse import ReceiptReuseSearchSession
                    self.native = ReceiptReuseSearchSession(cache_path=self.options['cache_path'],
                        config=self.search_config,reuse_configs=self.search_reuse_configs)
                if self.search_adaptive is None:
                    self.native.http_gate.interval_s = self.search_request_interval_s
                if self.search_fallback:
                    from .search_fallback import FallbackSearchSession
                    self.native=FallbackSearchSession(self.native,cache_path=self.options['cache_path'],
                                                     policy=self.search_fallback)
            if self.search_fallback:
                return await self.native.search(query,fallback_parameters=fallback_parameters,**parameters)
            return await self.native.search(query, **parameters)
        if self.service is None and not self.options.get('search_profile'):
            raise ValueError('External search requires an explicit deployment profile revision')
        return await self._client().search(query, **parameters)

    def snapshot_metrics(self):
        if self._last_metrics is not None:
            return self._last_metrics
        result = self.client.snapshot_metrics() if self.client else {'search_requests':0}
        result['operator_users'] = len(self._operator_owners)
        result['connections'] = self.connections.snapshot_metrics() if self.connections is not None else None
        if self.native is not None:
            native = self.native.snapshot_metrics()
            result['native_search'] = native
            result['search_requests'] = native['source_attempts']
            result['search_queries'] = native['search_requests']
            result['search_http_requests'] = native['http_requests']
            result['reused'] = result.get('reused', 0) + native['reused']
        return result

    async def _close(self):
        errors = []
        for resource in (self.native, self.client, self.owner):
            if resource is not None:
                try:
                    await resource.aclose()
                except BaseException as exc:
                    errors.append(exc)
        if self.connections is not None:
            try:
                await self.connections.aclose()
            except BaseException as exc:
                errors.append(exc)
        self._operator_owners.clear()
        self._operator_task = None
        try:
            self._last_metrics = self.snapshot_metrics()
        except Exception as exc:
            errors.append(exc)
            self._last_metrics = {'metrics_error': type(exc).__name__, 'operator_users': 0,
                'connections': self.connections.snapshot_metrics() if self.connections is not None else None}
        self.client = None
        self.owner = None
        self.native = None
        if errors:
            raise errors[0]

    async def aclose(self):
        if self.connections is not None and self.connections.loop is not None and self.connections.loop is not asyncio.get_running_loop():
            raise RuntimeError('WebSession belongs to another event loop')
        if self._operator_task is not None and self._operator_task is not asyncio.current_task():
            raise RuntimeError('WebSession is owned by another action')
        if self._closing_task is None:
            self._closing_task = asyncio.create_task(self._close())
        try:
            await asyncio.shield(self._closing_task)
        except asyncio.CancelledError:
            await self._closing_task
            raise


async def bounded(items, function, concurrency, *, return_exceptions=False):
    """Consume an ordered row-local sequence with O(concurrency) tasks.

    ``return_exceptions`` is an execution concern: it lets a durable operator
    finish sibling tasks before propagating a failed child.
    """
    if type(concurrency) is not int or concurrency < 1:
        raise ValueError('bounded concurrency must be a positive integer')
    results = [None] * len(items)
    iterator = iter(enumerate(items))
    async def worker():
        for index, item in iterator:
            try:
                results[index] = await function(item)
            except BaseException as exc:
                if return_exceptions:
                    results[index] = exc
                    continue
                raise
    tasks = [asyncio.create_task(worker()) for _ in range(min(concurrency, len(items)))]
    try:
        if return_exceptions:
            await asyncio.gather(*tasks, return_exceptions=True)
            return results
        await asyncio.gather(*tasks)
        return results
    except BaseException:
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        raise


async def isolated(function, *args, timeout_s, **kwargs):
    from demiflow.execution.isolation import run_isolated
    task = asyncio.create_task(asyncio.to_thread(run_isolated, function, *args, timeout_s=timeout_s, **kwargs))
    try:
        return await asyncio.shield(task)
    except asyncio.CancelledError:
        await task
        raise
