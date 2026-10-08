"""Run-owned native search route scheduling, pacing and durable cooldowns."""
import asyncio
import copy
import json
import re
import time
from contextlib import AsyncExitStack, asynccontextmanager

from demiflow.execution.request_limits import RequestGate, ServiceStopped
from .native_search import SearchConfig
from .native_search.config import digest, public
from .search_reuse import ReceiptReuseSearchSession
from .search_admission import SearchAdmission, ResizableAdmission, adaptive_policy, admission_identity_policy
from .search_recovery import SearchRecovery
from .search_errors import RouteLeaseExpired

NON_ROUTE_FAILURES = {'render_required', 'consent_required', 'request_budget', 'resource_limit', 'lease_expired'}


def historical_pool_references(config, values):
    """Explicit old pool references authorize receipts, never old transports."""
    from .search_reuse import completed_reuse_configs
    if not isinstance(values, (list, tuple)) or len(values) > 16:
        raise ValueError('Search receipt reuse permits at most 16 historical pools')
    result = []
    for value in values:
        if (not isinstance(value, dict) or set(value) != {'identity', 'search'}
                or not isinstance(value['identity'], str)
                or re.fullmatch(r'[0-9a-f]{64}', value['identity']) is None):
            raise ValueError('Historical pool requires its identity and original search declaration')
        prior = completed_reuse_configs(config, [value['search']])[0]
        result.append({'identity': value['identity'], 'search': prior.snapshot()})
    if len({r['identity'] for r in result}) != len(result):
        raise ValueError('Historical search pool references must be distinct')
    return result


def receipt_statuses(result):
    return {'lease_expired' if r.get('local_lease_expired') else r.get('status')
            for r in result.get('engine_receipts', [])}


def route_declarations(config, routes):
    if not isinstance(routes,(list,tuple)):
        raise TypeError('search_routes must be a list')
    names=set();result=[]
    for item in routes:
        if not isinstance(item,dict) or set(item)-{'name','proxy','interval_s','initial_cooldown_until','reuse_connections'}:
            raise ValueError('Invalid search route declaration fields')
        name=item.get('name');interval=item.get('interval_s',10.)
        if not isinstance(name,str) or not name or name in names:
            raise ValueError('Search route names must be unique and nonempty')
        import math
        if type(interval) not in (int,float) or not math.isfinite(interval) or interval<0:
            raise ValueError('Invalid search route interval')
        if 'proxy' not in item:
            raise ValueError('Search route requires an explicit proxy declaration')
        source=SearchConfig.from_mapping({**config.snapshot(),'proxy':item['proxy']})
        entry={'name':name,'proxy':public(source.proxy),'interval_s':interval}
        if 'reuse_connections' in item:
            if type(item['reuse_connections']) is not bool:
                raise ValueError('reuse_connections must be boolean')
            entry['reuse_connections']=item['reuse_connections']
        if 'initial_cooldown_until' in item:
            until=item['initial_cooldown_until']
            if type(until) not in (int,float) or not math.isfinite(until) or until<0:
                raise ValueError('Initial route cooldown must be a nonnegative Unix timestamp')
            entry['initial_cooldown_until']=until
        names.add(name);result.append(entry)
    return result


class _RouteGate:
    def __init__(self,owner,route):
        self.owner,self.route=owner,route
        self.gate=RequestGate(1,interval_s=route['interval_s'])
        self.reservations={}
    @property
    def peak(self):return self.gate.peak
    @property
    def active(self):return self.gate.active
    async def wait_ready(self):
        # A route is exclusively leased by choose(). Wait before the native
        # worker starts its deadline, without delaying completed cache replay.
        await asyncio.sleep(max(0.,self.route['next_ready']-time.monotonic()))
    @asynccontextmanager
    async def reserve_first(self):
        # Pace before the source worker starts its deadline. Keep start order
        # until its first HTTP so variable worker startup cannot create bursts.
        task=asyncio.current_task()
        async with AsyncExitStack() as stack:
            permit=await stack.enter_async_context(self.owner.http_gate.reserve())
            self.owner.check_admission()
            self.owner.check_route(self.route)
            self.reservations[task]=(permit,stack)
            try:
                yield
            finally:
                self.reservations.pop(task,None)
    @asynccontextmanager
    async def enter(self):
        async with self.gate.enter():
            reserved=self.reservations.pop(asyncio.current_task(),None)
            permit,stack=reserved if reserved else (self.owner.http_gate.enter(),None)
            try:
                # Further HTTP hops still acquire a distinct, paced permit.
                self.owner.check_admission()
                self.owner.check_route(self.route)
                async with permit:
                    self.owner.check_admission()
                    self.owner.check_route(self.route)
                    self.route['next_ready']=time.monotonic()+self.route['interval_s']
                    self.gate.next_start=self.route['next_ready']
                    yield
            finally:
                if stack is not None:await stack.aclose()


class _RoutedSession(ReceiptReuseSearchSession):
    async def call(self,context,message):
        if message.get('op')=='search':
            await self.http_gate.wait_ready()
            # Bound active source executions before their worker deadlines
            # start. A slow route must not spend another route's execution
            # budget waiting for the shared HTTP capacity. HTTP gates still
            # enforce pacing and every actual hop's admission separately.
            async with self.execution_gate:
                self.check_admission()
                async with self.http_gate.reserve_first():
                    return await super().call(context,message)
        return await super().call(context,message)


class SearchRoutePool:
    def __init__(self,*,cache_path,config,routes,reuse_configs=(),cooldown_s=1800,max_route_attempts=2,
                 failure_limit=2,adaptive=None,reuse_session_pools=(),failure_scope='pool'):
        if failure_scope not in {'pool', 'source'}:
            raise ValueError('failure_scope must be pool or source')
        self.failure_scope=failure_scope
        self.reuse_session_pools = historical_pool_references(config, reuse_session_pools)
        policy=adaptive_policy(adaptive)
        self.admission=SearchAdmission(policy) if policy is not None else None
        self.admission_lock=asyncio.Lock()
        self.http_gate=RequestGate(policy['max_concurrency'] if policy else config.request_concurrency,
                                  interval_s=policy['initial_interval_s'] if policy else 0)
        self.execution_gate=(ResizableAdmission(policy['initial_concurrency']) if policy
                             else asyncio.Semaphore(config.request_concurrency))
        if type(failure_limit) is not int or failure_limit<1:
            raise ValueError('Route failure limit must be a positive integer')
        self.failure_limit=failure_limit
        self.query_gate=RequestGate(1,failures=config.query_failure_limit or 5)
        self.recovery=(SearchRecovery(policy,failure_limit=self.query_gate.failures,
            capacity=lambda:self.admission.concurrency,inventory=self.recovery_inventory,persist=self.persist_recovery,
            recovered=self.recovered_admission)
            if failure_scope=='pool' and policy and policy['transient_failure_action']=='pause' else None)
        self.cooldown_s=cooldown_s;self.max_route_attempts=max_route_attempts
        self.routes=[];self.condition=asyncio.Condition();self.cursor=0
        self.initialized=False;self.initialize_lock=asyncio.Lock();self.closed=False
        self.queries=0;self.route_attempts=0
        self.cooldown_waits=0;self.cooldown_wait_s=0.
        self.selected_reuses=0
        self.expired_before_http=0
        self.inflight_queries={}
        self.extra_reuse_profiles=set()
        declarations=route_declarations(config,routes)
        if not declarations:raise ValueError('Search route pool cannot be empty')
        if type(max_route_attempts) is not int or not 1<=max_route_attempts<=len(declarations):
            raise ValueError('Route attempt limit must be within the declared route count')
        if not isinstance(cooldown_s,(int,float)) or not 0<cooldown_s<=86400:
            raise ValueError('Route cooldown must be positive and at most one day')
        configs=[SearchConfig.from_mapping({**config.snapshot(),'proxy':r['proxy']}) for r in declarations]
        for declaration,source in zip(declarations,configs):
            session=_RoutedSession(cache_path=cache_path,config=source,
                reuse_configs=[*reuse_configs,*configs])
            # The pool owns the consecutive-query breaker. Per-source pauses
            # still apply, but a second query breaker per route would retain
            # a stopped state after that route's timed cooldown expires.
            session.query_gate=None
            session.execution_gate=self.execution_gate
            session.check_admission=self.check_admission
            route={**declaration,'session':session,'busy':False,'next_ready':0.,'until':0.,'failures':0,'last_status':'','source_health':{}}
            session.http_gate=_RouteGate(self,route)
            self.routes.append(route)

    async def initialize(self):
        async with self.initialize_lock:
            if self.closed:raise RuntimeError('Search route pool is closed')
            if self.initialized:return
            for route in self.routes:
                await route['session'].initialize()
            def restore():
                with self.routes[0]['session']._db() as db:
                    db.execute('''CREATE TABLE IF NOT EXISTS native_search_route_health (
                        profile TEXT PRIMARY KEY, name TEXT, until REAL NOT NULL, failures INTEGER NOT NULL, status TEXT)''')
                    db.execute('''CREATE TABLE IF NOT EXISTS native_search_route_events (
                        id INTEGER PRIMARY KEY, profile TEXT, name TEXT, query_key TEXT, status TEXT, observed_at REAL)''')
                    db.execute('''CREATE TABLE IF NOT EXISTS native_search_route_results (
                        identity TEXT PRIMARY KEY, value TEXT NOT NULL)''')
                    for route in self.routes:
                        until=route.get('initial_cooldown_until',0)
                        if until>time.time():
                            # Carry an explicit preflight quarantine into the
                            # run without fabricating a source request/event.
                            db.execute('INSERT INTO native_search_route_health VALUES(?,?,?,?,?) '
                                'ON CONFLICT(profile) DO UPDATE SET until=MAX(until,excluded.until)',
                                (route['session'].profile,route['name'],until,0,'preflight_cooldown'))
                    return dict((p,(until,failures,status)) for p,until,failures,status in db.execute(
                        'SELECT profile,until,failures,status FROM native_search_route_health'))
            saved=await asyncio.to_thread(restore)
            for route in self.routes:
                value=saved.get(route['session'].profile)
                if value and self.failure_scope=='pool':
                    route['until'],route['failures'],route['last_status']=value
                elif self.failure_scope=='source':
                    route['until']=route.get('initial_cooldown_until',0)
            if self.failure_scope=='source':
                def restore_sources():
                    with self.routes[0]['session']._db() as db:
                        db.execute('CREATE TABLE IF NOT EXISTS native_search_source_route_health '
                            '(profile TEXT, source TEXT, until REAL, failures INTEGER, status TEXT, '
                            'PRIMARY KEY(profile,source))')
                        for route in self.routes:
                            session=route['session']
                            allowed={json.dumps([network,source['name']]) for network in
                                ['default',*session.config.networks] for source in session.sources}
                            for key,until,failures,status in db.execute(
                                'SELECT source,until,failures,status FROM native_search_source_route_health WHERE profile=?',
                                (session.profile,)):
                                if key in allowed:
                                    route['source_health'][key]={'until':until,'failures':failures,'last_status':status}
                await asyncio.to_thread(restore_sources)
            if self.reuse_session_pools:
                def history():
                    profiles = set()
                    with self.routes[0]['session']._db() as db:
                        exists = db.execute("SELECT 1 FROM sqlite_master WHERE type='table' "
                                            "AND name='native_search_session_history'").fetchone()
                        if not exists:
                            raise ValueError('Declared historical search pool journal is missing')
                        for ref in self.reuse_session_pools:
                            count = 0
                            for profile, in db.execute('SELECT profile FROM native_search_session_history WHERE pool=?',
                                                       (ref['identity'],)):
                                count += 1
                                if count > 100000 or len(profiles) >= 100000:
                                    raise ValueError('Historical search receipt profiles exceed 100000')
                                if not isinstance(profile, str) or re.fullmatch(r'[0-9a-f]{64}', profile) is None:
                                    raise ValueError('Invalid historical search profile')
                                profiles.add(profile)
                            if not count:
                                raise ValueError('Declared historical search pool has no saved profiles')
                    return profiles
                profiles = await asyncio.to_thread(history)
                self.extra_reuse_profiles.update(profiles)
                shared_profiles = tuple(sorted(self.allowed_profiles()))
                for route in self.routes:
                    route['session'].reuse_profiles = shared_profiles
            if self.admission:
                self.admission_key=digest(['adaptive-search-1' if self.failure_scope=='pool' else 'adaptive-search-source-1', admission_identity_policy(self.admission.policy),
                    sorted(r['session'].profile for r in self.routes)])
                def restore_admission():
                    with self.routes[0]['session']._db() as db:
                        db.execute('CREATE TABLE IF NOT EXISTS native_search_admission '
                            '(identity TEXT PRIMARY KEY, state_json TEXT NOT NULL, observed_at REAL NOT NULL)')
                        db.execute('CREATE TABLE IF NOT EXISTS native_search_admission_events '
                            '(id INTEGER PRIMARY KEY, identity TEXT NOT NULL, event_json TEXT NOT NULL)')
                        row=db.execute('SELECT state_json FROM native_search_admission WHERE identity=?',
                                       (self.admission_key,)).fetchone()
                        return json.loads(row[0]) if row else None
                previous=await asyncio.to_thread(restore_admission)
                if previous is not None:self.admission.restore(previous)
                await self.execution_gate.resize(self.admission.concurrency)
                self.http_gate.interval_s=self.admission.interval_s
                if self.recovery:
                    def restore_recovery():
                        with self.routes[0]['session']._db() as db:
                            db.execute('CREATE TABLE IF NOT EXISTS native_search_recovery '
                                '(identity TEXT PRIMARY KEY, state_json TEXT NOT NULL, observed_at REAL NOT NULL)')
                            db.execute('CREATE TABLE IF NOT EXISTS native_search_recovery_events '
                                '(id INTEGER PRIMARY KEY, identity TEXT NOT NULL, event_json TEXT NOT NULL)')
                            row=db.execute('SELECT state_json FROM native_search_recovery WHERE identity=?',
                                           (self.admission_key,)).fetchone()
                            return json.loads(row[0]) if row else None
                    previous=await asyncio.to_thread(restore_recovery)
                    self.recovery.restore(previous)
                    if previous is None and self.admission.stopped_until>time.time():
                        self.recovery.value.update(mode='paused',paused_until=self.admission.stopped_until,
                            last_reason='restored_search_pool_failure_window')
                        await self.persist_recovery(self.recovery.state(),None)
            self.initialized=True

    async def observe_admission(self, result, *, success, congestion=False):
        if not self.admission:return
        if self.admission.policy['adjustment_scope']=='query':return
        latencies=[h['elapsed_s'] for r in result.get('engine_receipts',[])
                   for a in r.get('attempts',[]) for h in a.get('http',[])
                   if isinstance(h.get('elapsed_s'),(int,float))]
        async with self.admission_lock:
            event=self.admission.observe(success=success,congestion=congestion,
                                         latency_s=max(latencies) if latencies else None)
            await self.execution_gate.resize(self.admission.concurrency)
            self.http_gate.interval_s=self.admission.interval_s
            await self.persist_admission(event)

    async def observe_query_admission(self, *, success, result=None):
        if not self.admission or self.admission.policy['failure_window_scope']!='query':return
        async with self.admission_lock:
            if self.admission.policy['adjustment_scope']=='query':
                latencies=[h['elapsed_s'] for r in (result or {}).get('engine_receipts',[])
                           for a in r.get('attempts',[]) for h in a.get('http',[])
                           if isinstance(h.get('elapsed_s'),(int,float))]
                adjustment=self.admission.observe(success=success,
                    latency_s=max(latencies) if latencies else None)
                await self.execution_gate.resize(self.admission.concurrency)
                self.http_gate.interval_s=self.admission.interval_s
                await self.persist_admission(adjustment)
            event=self.admission.observe_query(success=success)
            await self.persist_admission(event)

    def allowed_profiles(self):
        profiles = set(self.extra_reuse_profiles)
        seen = set()
        for route in self.routes:
            profiles.add(route['session'].profile)
            reused = route['session'].reuse_profiles
            if reused is not None and id(reused) not in seen:
                profiles.update(reused)
                seen.add(id(reused))
        return profiles

    async def observe_route(self, route, result, *, success, status):
        """Optional lifecycle extension; observations are fresh attempts only."""

    async def release_route(self, route):
        """Called after the exclusive lease drains, including cancellation."""

    def recovery_inventory(self):
        routes=[r for r in self.routes if r['until']<=time.time()]
        return {'viable':len(routes),'leased':sum(r['busy'] for r in routes),
                'paced':sum(r['next_ready']>time.monotonic() for r in routes)}

    def check_route(self, route):
        """Optional lease validation immediately before actual HTTP admission."""

    async def persist_admission(self,event):
        state=self.admission.state()
        def persist():
            with self.routes[0]['session']._db() as db:
                db.execute('INSERT OR REPLACE INTO native_search_admission VALUES (?,?,?)',
                    (self.admission_key,json.dumps(state),time.time()))
                if event is not None:
                    db.execute('INSERT INTO native_search_admission_events(identity,event_json) VALUES (?,?)',
                               (self.admission_key,json.dumps(event)))
        await asyncio.to_thread(persist)

    async def persist_recovery(self,state,event):
        def persist():
            with self.routes[0]['session']._db() as db:
                db.execute('INSERT OR REPLACE INTO native_search_recovery VALUES (?,?,?)',
                           (self.admission_key,json.dumps(state),time.time()))
                if event is not None:
                    db.execute('INSERT INTO native_search_recovery_events(identity,event_json) VALUES (?,?)',
                               (self.admission_key,json.dumps(event)))
        await asyncio.to_thread(persist)

    async def recovered_admission(self):
        async with self.admission_lock:
            # Start a new observation window, retaining the full event history.
            # Old outage samples must not immediately retrip a successful probe.
            self.admission.stopped_until=0.
            self.admission.failure_window=[];self.admission.samples=[]
            self.admission.since=time.time()
            await self.persist_admission({'observed_at':time.time(),'reason':'search_recovery_probe_succeeded'})

    async def choose(self,tried,source_key=None):
        def until(route):
            return max(route['until'],route.get('source_health',{}).get(source_key,{}).get('until',0))
        waited=0.
        wait_limit=self.admission.policy['route_cooldown_wait_s'] if self.admission else 0.
        async with self.condition:
            while True:
                if self.closed:raise ServiceStopped('search_route_pool_closed')
                self.check_admission()
                candidates=[(i,r) for i,r in enumerate(self.routes) if i not in tried and until(r)<=time.time()]
                if not candidates:
                    untils=[until(r) for i,r in enumerate(self.routes) if i not in tried]
                    delay=max(0.,min(untils)-time.time()) if untils else float('inf')
                    remaining=wait_limit-waited
                    if remaining<=0 or delay>remaining:
                        raise ServiceStopped('all_search_routes_cooling_down')
                    # Only wait for an already scheduled near-term release.
                    # This issues no HTTP and does not reset the query's route
                    # allowance or any persisted cooldown. Notifications and
                    # cancellation interrupt it; repeated extensions share one
                    # finite wait budget for this acquisition.
                    started=time.monotonic();self.cooldown_waits+=1
                    try:
                        await asyncio.wait_for(self.condition.wait(),timeout=max(.001,delay))
                    except asyncio.TimeoutError:
                        pass
                    finally:
                        elapsed=time.monotonic()-started
                        waited+=elapsed;self.cooldown_wait_s+=elapsed
                    continue
                idle=[(i,r) for i,r in candidates if not r['busy']]
                if idle:
                    now=time.monotonic()
                    i,route=min(idle,key=lambda x:(max(now,x[1]['next_ready']),
                                                  (x[0]-self.cursor)%len(self.routes)))
                    route['busy']=True;self.cursor=(i+1)%len(self.routes)
                    return i,route
                await self.condition.wait()

    def check_admission(self):
        if self.closed:raise ServiceStopped('search_route_pool_closed')
        if self.failure_scope=='source':return
        if self.recovery:self.recovery.check()
        elif self.admission and self.admission.stopped_until>time.time():
            raise ServiceStopped('search_pool_failure_window')

    def record(self,route,query,parameters,status,source_key=None):
        with route['session']._db() as db:
            db.execute('INSERT INTO native_search_route_events(profile,name,query_key,status,observed_at) VALUES(?,?,?,?,?)',
                (route['session'].profile,route['name'],digest([query,parameters]),status,time.time()))
            if source_key is not None:
                health=route.get('source_health',{}).get(source_key,{})
                db.execute('INSERT OR REPLACE INTO native_search_source_route_health VALUES(?,?,?,?,?)',
                    (route['session'].profile,source_key,health.get('until',0),health.get('failures',0),health.get('last_status','')))
                return
            db.execute('INSERT OR REPLACE INTO native_search_route_health VALUES(?,?,?,?,?)',
                (route['session'].profile,route['name'],route['until'],route['failures'],route['last_status']))

    def selection_identity(self, query, parameters):
        from .native_search.config import runtime_id
        semantic=self.routes[0]['session'].config.snapshot();semantic.pop('proxy')
        return digest([runtime_id(),semantic,query,parameters])

    async def restore_results(self, entries, *, provenance):
        """Freeze completed results from an explicit fixed consumer receipt.

        This repairs journals predating failed-query selections. It performs no
        HTTP, never replaces an existing selection, and verifies each original
        engine receipt against the declared profiles and durable source log.
        """
        if not isinstance(provenance, str) or not provenance.strip():
            raise ValueError('Restored query selections require fixed provenance')
        await self.initialize()
        allowed=self.allowed_profiles()
        checked=[]
        for entry in entries:
            query,parameters,result=entry['query'],entry['parameters'],entry['result']
            normalized,_=self.routes[0]['session'].parameters(query,**parameters)
            if (result.get('runtime')!='native-search-1' or result.get('profile') not in allowed
                    or json.loads(result['parameters_json'])!=normalized or not result.get('engine_receipts')):
                raise ValueError('Restored result does not match declared request semantics')
            for receipt in result['engine_receipts']:
                if receipt['receipt_id'] not in {digest([p,receipt['engine'],query,normalized]) for p in allowed}:
                    raise ValueError('Restored source receipt identity mismatch')
            checked.append((self.selection_identity(query,parameters),copy.deepcopy(result)))
        def commit():
            with self.routes[0]['session']._db() as db:
                # Validate every item before publishing any migration row.
                for _,result in checked:
                    for receipt in result['engine_receipts']:
                        row=db.execute('SELECT value FROM native_search WHERE key=?',(receipt['receipt_id'],)).fetchone()
                        if row is None or json.loads(row[0])['status']!=receipt['status']:
                            raise ValueError('Restored source receipt is missing or has a different outcome')
                db.execute('CREATE TABLE IF NOT EXISTS native_search_selection_restores '
                           '(identity TEXT PRIMARY KEY, provenance TEXT NOT NULL, observed_at REAL NOT NULL)')
                inserted=0
                for identity,result in checked:
                    cursor=db.execute('INSERT OR IGNORE INTO native_search_route_results VALUES (?,?)',
                                      (identity,json.dumps(result,ensure_ascii=False)))
                    if cursor.rowcount:
                        inserted+=1
                        db.execute('INSERT INTO native_search_selection_restores VALUES (?,?,?)',
                                   (identity,provenance,time.time()))
                return {'checked':len(checked),'inserted':inserted,'existing':len(checked)-inserted}
        return await asyncio.to_thread(commit)

    async def selected_result(self,query,**parameters):
        """Read an existing selection without HTTP or changing recovery state."""
        await self.initialize()
        identity=self.selection_identity(query,parameters)
        allowed=self.allowed_profiles()
        def read():
            with self.routes[0]['session']._db() as db:
                size=db.execute('SELECT length(cast(value as blob)) FROM native_search_route_results '
                                'WHERE identity=?',(identity,)).fetchone()
                if size is None:return None
                if size[0]>64*1024*1024:raise ValueError('Selected search result exceeds 64 MiB')
                row=db.execute('SELECT value FROM native_search_route_results WHERE identity=?',(identity,)).fetchone()
                value=json.loads(row[0])
                return value if value.get('profile') in allowed else None
        return await asyncio.to_thread(read)

    async def search(self,query,*,wait_for_recovery=True,**parameters):
        if type(wait_for_recovery) is not bool:raise ValueError('wait_for_recovery must be boolean')
        identity=self.selection_identity(query,parameters)
        key=(identity,wait_for_recovery)
        entry=self.inflight_queries.get(key)
        if entry is None:
            entry={'task':asyncio.create_task(self._search(query,parameters,identity,wait_for_recovery)), 'waiters':0}
            self.inflight_queries[key]=entry
        entry['waiters']+=1
        try:
            return copy.deepcopy(await asyncio.shield(entry['task']))
        finally:
            entry['waiters']-=1
            if not entry['waiters']:
                if not entry['task'].done():
                    entry['task'].cancel()
                    await asyncio.gather(entry['task'],return_exceptions=True)
                self.inflight_queries.pop(key,None)

    async def _search(self,query,parameters,identity,wait_for_recovery=True):
        await self.initialize();self.queries+=1
        allowed=self.allowed_profiles()
        def selected_result(value=None):
            with self.routes[0]['session']._db() as db:
                row=db.execute('SELECT value FROM native_search_route_results WHERE identity=?',(identity,)).fetchone()
                saved=json.loads(row[0]) if row else None
                if saved is not None and saved.get('profile') in allowed:return saved
                if value is not None:
                    db.execute('INSERT OR REPLACE INTO native_search_route_results VALUES (?,?)',
                        (identity,json.dumps(value,ensure_ascii=False)))
                return value
        saved=await asyncio.to_thread(selected_result)
        if saved is not None:
            self.selected_reuses+=1
            return saved
        if self.recovery:
            while True:
                async with self.recovery.admit(wait_for_recovery=wait_for_recovery) as lease:
                    self.check_admission()
                    try:return await self._search_routes(query,parameters,selected_result,lease)
                    except ServiceStopped as exc:
                        if str(exc) not in {'all_search_routes_cooling_down','search_session_pool_exhausted'}:raise
                        # No completed query exists here. Keep its position in
                        # the stream and bound local availability recovery too.
                        await self.recovery.observe(lease,success=False,unavailable=str(exc))
        return await self._search_routes(query,parameters,selected_result,None)

    async def _search_routes(self,query,parameters,selected_result,recovery_lease):
        source_key=None
        if self.failure_scope=='source':
            normalized,sources=self.routes[0]['session'].parameters(query,**parameters)
            if len(sources)!=1:
                raise ValueError('Source failure isolation requires one engine per search request')
            source_key=json.dumps([normalized['network'],sources[0]['name']])
        else:self.query_gate.check()
        def prior_selection():
            with self.routes[0]['session']._db() as db:
                row=db.execute('''SELECT profile FROM native_search_route_events
                    WHERE query_key=? AND status IN ('ok','no_results','reused:ok','reused:no_results')
                    ORDER BY id DESC LIMIT 1''',(digest([query,parameters]),)).fetchone()
                return row[0] if row else None
        preferred=await asyncio.to_thread(prior_selection)
        tried=set();result=None;completed_result=None;query_fresh=False;attempt=0;expired=0
        while attempt<self.max_route_attempts:
            try:
                i,route=await self.choose(tried,source_key) if source_key is not None else await self.choose(tried)
            except ServiceStopped as exc:
                if source_key is not None and str(exc)=='all_search_routes_cooling_down':
                    if result is None:
                        result={'status':'deferred','retryable':True,'reason':str(exc),'candidates':[],
                                'engine_receipts':[],'attempts':[]}
                    break
                if result is not None:break
                raise
            tried.add(i);self.route_attempts+=1
            attempt+=1
            if preferred:
                normalized,_=route['session'].parameters(query,**parameters)
                route['session'].preferred_reuse_profiles[digest([query,normalized])]=preferred
            before_attempts=route['session'].metrics.get('source_attempts',0)
            before_http=route['session'].metrics.get('http_requests',0)
            try:
                result=await route['session'].search(query,**parameters)
                if any(r.get('attempts') for r in result.get('engine_receipts',[])):
                    completed_result=result
                statuses=receipt_statuses(result)
                usable=bool(result.get('candidates')) or result['status'] in {'ok','no_results'}
                bad=next((s for s in ('captcha','rate_limited','access_denied','authentication_error','configuration_error') if s in statuses),None)
                status=bad or result['status']
                fresh=route['session'].metrics.get('source_attempts',0)>before_attempts
                query_fresh=query_fresh or fresh
                if statuses and statuses <= NON_ROUTE_FAILURES:
                    # Rendering/declared-resource failures survive as failed
                    # evidence, but replacing an otherwise healthy IP cannot
                    # repair them. Do not retire or slow the proxy pool.
                    await asyncio.to_thread(self.record,route,query,parameters,
                        next(iter(sorted(statuses))) if fresh else 'reused:'+next(iter(sorted(statuses))),source_key)
                    break
                # Cached receipts retain their failure/success semantics for
                # this query; they are not new observations of route health.
                # In particular replay must not restart an expired cooldown
                # or use an old success to reset consecutive fresh failures.
                if fresh:
                    health=(route['source_health'].setdefault(source_key,{'until':0,'failures':0,'last_status':''})
                            if source_key is not None else route)
                    health['last_status']=status
                    if usable:health['failures']=0
                    else:health['failures']+=1
                    if bad or health['failures']>=self.failure_limit:
                        health['until']=time.time()+self.cooldown_s
                        for receipt in result.get('engine_receipts',[]):
                            delay=receipt.get('retry_after_s') or 0
                            health['until']=max(health['until'],time.time()+delay)
                    await self.observe_admission(result,success=usable,congestion=bool(bad))
                    await self.observe_route(route,result,success=usable,status=status)
                await asyncio.to_thread(self.record,route,query,parameters,status if fresh else 'reused:'+status,source_key)
                if self.recovery and bad in {'authentication_error','configuration_error'}:
                    await self.recovery.stop('search_authentication_or_configuration')
                if usable:break
            except RouteLeaseExpired:
                # No external request was granted, so this did not consume a
                # target attempt. Retire locally and reacquire, with a separate
                # small bound to prevent an endlessly churning scheduler.
                if route['session'].metrics.get('http_requests',0)!=before_http:
                    raise ServiceStopped('search_lease_expired_after_http')
                expired+=1;self.expired_before_http+=1
                await asyncio.to_thread(self.record,route,query,parameters,'lease_expired_before_http')
                if expired>2:raise ServiceStopped('search_lease_expiry_reacquisition_limit')
                attempt-=1;tried.discard(i)
                continue
            except ServiceStopped:
                if source_key is not None:raise
                # Shared shutdown is not an observation of each waiting route
                # and must not cascade into synthetic target failures.
                self.check_admission()
                fresh=route['session'].metrics.get('source_attempts',0)>before_attempts
                query_fresh=query_fresh or fresh
                if fresh:
                    route['until']=time.time()+self.cooldown_s;route['last_status']='service_stopped'
                    await self.observe_route(route,{},success=False,status='service_stopped')
                await asyncio.to_thread(self.record,route,query,parameters,'service_stopped' if fresh else 'reused:service_stopped')
                result={'status':'search_failed','reason':'route service stopped','candidates':[],
                        'attempts':[],'engine_receipts':[],'response_json':'{}','runtime':'native-search-1',
                        'profile':route['session'].profile,'parameters_json':json.dumps(parameters)}
            finally:
                if (not route.get('reuse_connections',True)
                        and route['session'].metrics.get('source_attempts',0)>before_attempts):
                    # The route lease is exclusive and search() has awaited
                    # all adapter calls. Closing idle workers drops their TCP
                    # pools; a later query gets fresh connections. Stored
                    # responses, request identity and within-query redirects
                    # are unchanged. Providers decide the next exit address.
                    await asyncio.gather(*(w.close() for w in route['session'].workers))
                async with self.condition:
                    route['busy']=False;self.condition.notify_all()
                await self.release_route(route)
        # A failed, completed query has exhausted this run's route allowance
        # too. Restarting or adding a route must not silently buy another query
        # and change an already submitted model payload. Save before a circuit
        # raises; interrupted synthetic service stops have no source receipt.
        if source_key is not None and (result.get('retryable') or
                receipt_statuses(result)=={'suspended'} and
                not any(r.get('attempts') for r in result.get('engine_receipts',[]))):
            if completed_result is not None:
                result=completed_result
            else:
                return {**result,'retryable':True}
        if result.get('engine_receipts') or result['status'] in {'ok','no_results'}:
            allowed=self.allowed_profiles()
            result=await asyncio.to_thread(selected_result,result)
        usable=bool(result.get('candidates')) or result['status'] in {'ok','no_results'}
        statuses=receipt_statuses(result)
        if statuses and statuses <= NON_ROUTE_FAILURES:
            return result
        if query_fresh:await self.observe_query_admission(success=usable,result=result)
        self.check_admission()
        if self.recovery:
            if query_fresh:
                await self.recovery.observe(recovery_lease,success=usable,
                    window_stop=self.admission.stopped_until>time.time())
        elif source_key is None:self.query_gate.result(success=usable,transient=not usable)
        return result

    def snapshot_metrics(self):
        snapshots={r['name']:r['session'].snapshot_metrics() for r in self.routes}
        totals={key:sum(s.get(key,0) for s in snapshots.values()) for key in
                ['source_attempts','http_requests','reused','reused_previous_proxy','reused_selected_receipt','worker_starts','worker_count','source_active']}
        totals['reused']+=self.selected_reuses
        return {**totals,'failure_scope':self.failure_scope,'selected_query_reuses':self.selected_reuses,
            'search_requests':self.queries,'route_attempts':self.route_attempts,
            'route_cooldown_waits':self.cooldown_waits,'route_cooldown_wait_s':self.cooldown_wait_s,
            'lease_expired_before_http':self.expired_before_http,
            'http_peak':self.http_gate.peak,'http_active':self.http_gate.active,
            'adaptive_admission':self.admission.snapshot() if self.admission else None,
            'transient_recovery':self.recovery.snapshot() if self.recovery else None,
            'query_consecutive_failures':self.recovery.value['consecutive'] if self.recovery else self.query_gate.consecutive,
            'query_stopped':self.recovery.value['fatal'] if self.recovery else self.query_gate.stopped,
            'routes':{r['name']:{'cooldown_until':r['until'],'last_status':r['last_status'],
                'consecutive_failures':r['failures'],'source_health':dict(r.get('source_health',{})),'interval_s':r['interval_s'],'metrics':snapshots[r['name']]} for r in self.routes}}

    async def aclose(self):
        async with self.condition:
            self.closed=True;self.condition.notify_all()
        tasks=[entry['task'] for entry in self.inflight_queries.values()]
        for task in tasks:task.cancel()
        await asyncio.gather(*tasks,return_exceptions=True)
        await asyncio.gather(*(r['session'].aclose() for r in self.routes))
