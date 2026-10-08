"""Optional source fallback with immutable query selections.

Existing primary selections are preserved unless an exact failed selection is
explicitly authorized for fallback recovery. The original failure remains saved.
A selected fallback stays selected after restart or primary recovery. No circuit,
source receipt or model quota is reset.
"""
import asyncio
import copy
import json
import re
import time

from .native_search import NativeSearchSession, SearchConfig
from .native_search.config import digest
from demiflow.execution.request_limits import ServiceStopped


TRANSIENT_STOPS = frozenset({
    'search_recovery_probe_limit', 'search_recovery_episode_limit',
    'all_search_routes_cooling_down', 'search_session_pool_exhausted',
    'search_recovery_paused',
})


def fallback_policy(value):
    if value is None:
        return None
    if not isinstance(value, dict) or set(value) - {'search', 'max_pending', 'max_result_bytes',
            'routes', 'route_attempts', 'route_cooldown_s', 'route_failure_limit',
            'recover_primary_selections', 'adaptive', 'session_pool'}:
        raise ValueError('Invalid native search fallback declaration')
    declaration = value.get('search')
    if (not isinstance(declaration, dict) or
            not isinstance(declaration.get('engines'), (list, tuple)) or
            not 1 <= len(declaration['engines']) <= 8):
        raise ValueError('Fallback requires 1..8 explicit search engines')
    routes_value = value.get('routes', [])
    if not isinstance(routes_value, (list, tuple)) or len(routes_value)>16:
        raise ValueError('Fallback permits at most 16 declared routes')
    search = SearchConfig.from_mapping(value.get('search'))
    if search.workers>8 or search.request_concurrency>128 or search.max_bytes>8*1024*1024 or search.max_results>500:
        raise ValueError('Fallback native source resource declaration exceeds bounds')
    search.source_configs()
    pending = value.get('max_pending', 128)
    size = value.get('max_result_bytes', 16 * 1024 * 1024)
    if type(pending) is not int or not 1 <= pending <= 1024:
        raise ValueError('Fallback max_pending must be 1..1024')
    if type(size) is not int or not 1024 <= size <= 64 * 1024 * 1024:
        raise ValueError('Fallback max_result_bytes must be 1 KiB..64 MiB')
    from .search_routes import route_declarations
    routes = route_declarations(search, routes_value)
    from .search_sessions import session_pool_policy
    session_pool = session_pool_policy(value.get('session_pool'))
    from .search_admission import adaptive_policy
    adaptive = adaptive_policy(value.get('adaptive'))
    if adaptive is not None and (not (routes or session_pool) or adaptive['max_concurrency'] > search.request_concurrency):
        raise ValueError('Fallback adaptive admission requires routes and cannot exceed source HTTP concurrency')
    attempts = value.get('route_attempts', 1)
    cooldown = value.get('route_cooldown_s', 60.)
    failures = value.get('route_failure_limit', 2)
    capacity = session_pool['size'] if session_pool else max(1,len(routes))
    if type(attempts) is not int or not 1 <= attempts <= capacity:
        raise ValueError('Invalid fallback route attempt bound')
    if type(cooldown) not in (int, float) or not 0 < cooldown <= 86400:
        raise ValueError('Invalid fallback route cooldown')
    if type(failures) is not int or not 1 <= failures <= 100:
        raise ValueError('Invalid fallback route failure bound')
    recovery = value.get('recover_primary_selections', [])
    if (not isinstance(recovery, (list, tuple)) or len(recovery) > 4096
            or any(not isinstance(k, str) or re.fullmatch(r'[0-9a-f]{64}', k) is None for k in recovery)
            or len(set(recovery)) != len(recovery)):
        raise ValueError('recover_primary_selections permits at most 4096 distinct SHA256 selection identities')
    return {'search': search.snapshot(), 'max_pending': pending, 'max_result_bytes': size,
            'routes': routes, 'route_attempts': attempts, 'route_cooldown_s': cooldown,
            'route_failure_limit': failures,
            **({'session_pool':session_pool} if session_pool is not None else {}),
            **({'adaptive': adaptive} if adaptive is not None else {}),
            **({'recover_primary_selections': list(recovery)} if recovery else {})}


def encoded(value, limit):
    # Avoid an unbounded second serialized copy before checking its size.
    result = bytearray()
    for chunk in json.JSONEncoder(ensure_ascii=False, separators=(',', ':')).iterencode(value):
        data = chunk.encode()
        if len(data) > limit - len(result):
            raise ValueError('Search fallback receipt exceeds byte budget')
        result.extend(data)
    return result.decode()


class FallbackSearchSession:
    def __init__(self, primary, *, cache_path, policy):
        self.primary = primary
        self.policy = fallback_policy(policy)
        if not hasattr(primary, 'selected_result'):
            raise ValueError('Search fallback requires a primary route pool')
        self.secondary = NativeSearchSession(cache_path=cache_path,
            config=SearchConfig.from_mapping(self.policy['search']))
        pooled = self.policy['routes'] or self.policy.get('session_pool')
        if pooled:
            from .search_routes import SearchRoutePool
            pool_type=SearchRoutePool
            extra={}
            if self.policy.get('session_pool'):
                from .search_sessions import RenewableSearchRoutePool
                pool_type=RenewableSearchRoutePool
                extra['session_pool']=self.policy['session_pool']
            self.secondary = pool_type(cache_path=cache_path,
                config=SearchConfig.from_mapping(self.policy['search']), routes=self.policy['routes'],
                max_route_attempts=self.policy['route_attempts'], cooldown_s=self.policy['route_cooldown_s'],
                failure_limit=self.policy['route_failure_limit'], adaptive=self.policy.get('adaptive'),**extra)
        self.db_session = (self.secondary.routes[0]['session'] if pooled else self.secondary)
        self.inflight = {}
        self.lock = asyncio.Lock()
        self.initialized = False
        self.closed = False
        self.metrics = dict(search_requests=0, selected_fallback_reuses=0,
                            preserved_primary_selections=0, fallback_queries=0,
                            recovered_primary_selections=0)

    async def initialize(self):
        async with self.lock:
            if self.closed:
                raise RuntimeError('Search fallback session is closed')
            if self.initialized:
                return
            await self.primary.initialize()
            await self.secondary.initialize()
            def setup():
                with self.db_session._db() as db:
                    db.execute('CREATE TABLE IF NOT EXISTS native_search_fallback_results '
                        '(identity TEXT PRIMARY KEY, value TEXT NOT NULL, observed_at REAL NOT NULL)')
            await self.database(setup)
            self.initialized = True

    async def database(self, function):
        task = asyncio.create_task(asyncio.to_thread(function))
        try:
            return await asyncio.shield(task)
        except asyncio.CancelledError:
            await task
            raise

    async def search(self, query, *, fallback_parameters=None, **parameters):
        if not isinstance(query, str) or not query.strip() or len(query) > 32768:
            raise ValueError('Fallback query requires 1..32768 characters')
        if set(parameters) - {'language', 'pageno', 'safesearch', 'time_range'}:
            raise ValueError('Fallback supports language, page, safesearch and time range parameters')
        if any(not isinstance(v, (str, int, type(None))) or isinstance(v, str) and len(v)>128
               for v in parameters.values()):
            raise ValueError('Invalid or oversized fallback query parameter')
        if fallback_parameters is None:
            fallback_parameters = {}
        if not isinstance(fallback_parameters, dict) or set(fallback_parameters) - {'language'}:
            raise ValueError('Fallback request overrides permit only an explicit language')
        if any(not isinstance(v, str) or len(v)>128 for v in fallback_parameters.values()):
            raise ValueError('Invalid fallback request language')
        await self.initialize()
        secondary_parameters = {**parameters, **fallback_parameters}
        normalized, _ = self.db_session.parameters(query, **secondary_parameters)
        primary_identity = self.primary.selection_identity(query, parameters)
        secondary_identity = (self.secondary.selection_identity(query, secondary_parameters)
            if self.policy['routes'] or self.policy.get('session_pool') else digest([self.secondary.profile, normalized]))
        identity = digest(['native-search-fallback-1', primary_identity,
                           secondary_identity])
        entry = self.inflight.get(identity)
        if entry is None:
            if len(self.inflight) >= self.policy['max_pending']:
                raise ServiceStopped('search_fallback_pending_limit')
            entry = {'task': asyncio.create_task(self._search(identity, primary_identity,
                query, parameters, secondary_parameters)), 'waiters': 0}
            self.inflight[identity] = entry
        entry['waiters'] += 1
        try:
            return copy.deepcopy(await asyncio.shield(entry['task']))
        finally:
            entry['waiters'] -= 1
            if not entry['waiters']:
                if not entry['task'].done():
                    entry['task'].cancel()
                    await asyncio.gather(entry['task'], return_exceptions=True)
                self.inflight.pop(identity, None)

    async def _search(self, identity, primary_identity, query, parameters, secondary_parameters):
        self.metrics['search_requests'] += 1
        def read():
            with self.db_session._db() as db:
                row = db.execute('SELECT length(cast(value as blob)) FROM native_search_fallback_results '
                                 'WHERE identity=?', (identity,)).fetchone()
                if row is None:
                    return None
                if row[0] > self.policy['max_result_bytes']:
                    raise ValueError('Stored search fallback receipt exceeds byte budget')
                return json.loads(db.execute('SELECT value FROM native_search_fallback_results '
                                             'WHERE identity=?', (identity,)).fetchone()[0])
        saved = await self.database(read)
        if saved is not None:
            self.metrics['selected_fallback_reuses'] += 1
            return saved['result']
        selected = await self.primary.selected_result(query, **parameters)
        recover = (selected is not None and selected.get('status') == 'search_failed'
                   and primary_identity in self.policy.get('recover_primary_selections', ()))
        if selected is not None and not recover:
            # Let the original pool retain its counters and exact receipt.
            self.metrics['preserved_primary_selections'] += 1
            return await self.primary.search(query, **parameters)
        primary_outcome = {'selection_identity': primary_identity}
        recovery = getattr(self.primary, 'recovery', None)
        if recover:
            # Explicit recovery changes the selected evidence source, not the
            # original failed receipt or the primary circuit. Authentication
            # and configuration stops remain fatal even with an allowlist.
            if recovery and recovery.value.get('fatal') and recovery.value['fatal'] not in TRANSIENT_STOPS:
                raise ServiceStopped(recovery.value['fatal'])
            primary_outcome.update(status=selected['status'], profile=selected.get('profile'),
                                   recovery='explicit_failed_primary_selection')
            self.metrics['recovered_primary_selections'] += 1
        elif recovery and recovery.value.get('fatal'):
            reason = recovery.value['fatal']
            if reason not in TRANSIENT_STOPS:
                raise ServiceStopped(reason)
            primary_outcome['stop_reason'] = reason
        else:
            try:
                result = await self.primary.search(query, wait_for_recovery=False, **parameters)
                if result['status'] != 'search_failed':
                    return result
                primary_outcome.update(status=result['status'], profile=result.get('profile'))
            except ServiceStopped as exc:
                if str(exc) not in TRANSIENT_STOPS:
                    raise
                primary_outcome['stop_reason'] = str(exc)
        self.metrics['fallback_queries'] += 1
        result = await self.secondary.search(query, **secondary_parameters)
        value = encoded({'primary': primary_outcome, 'result': result}, self.policy['max_result_bytes'])
        def save():
            with self.db_session._db() as db:
                db.execute('INSERT OR IGNORE INTO native_search_fallback_results VALUES (?,?,?)',
                           (identity, value, time.time()))
                # Concurrent writers must return the first durable selection.
                size, = db.execute('SELECT length(cast(value as blob)) FROM native_search_fallback_results '
                                   'WHERE identity=?', (identity,)).fetchone()
                if size > self.policy['max_result_bytes']:
                    raise ValueError('Stored search fallback receipt exceeds byte budget')
                return json.loads(db.execute('SELECT value FROM native_search_fallback_results '
                                             'WHERE identity=?', (identity,)).fetchone()[0])['result']
        return await self.database(save)

    def snapshot_metrics(self):
        primary, secondary = self.primary.snapshot_metrics(), self.secondary.snapshot_metrics()
        result = {**primary, 'primary_search': primary, 'fallback_search': secondary,
                  'fallback_selection': dict(self.metrics)}
        for key in ('source_attempts', 'http_requests', 'reused', 'http_active', 'source_active'):
            result[key] = primary.get(key, 0) + secondary.get(key, 0)
        result['reused'] += self.metrics['selected_fallback_reuses']
        result['search_requests'] = self.metrics['search_requests']
        return result

    async def aclose(self):
        self.closed = True
        tasks = [entry['task'] for entry in self.inflight.values()]
        for task in tasks:
            task.cancel()
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)
        try:
            await self.primary.aclose()
        finally:
            await self.secondary.aclose()
