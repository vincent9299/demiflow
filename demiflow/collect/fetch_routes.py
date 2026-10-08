"""Domain-scoped fetch proxy pools; no vendor or business source policy.

Each HTTP hop leases one declared route. Retries remain owned by WebClient;
the pool never adds attempts. Health is durable across scheduling changes.
"""
import asyncio
import hashlib
import json
import math
import time
from contextlib import asynccontextmanager

from demiflow.execution.request_limits import ServiceStopped


def proxy_routes(value, *, default_session_pool=None):
    from .web import normalized_blocked_domains
    from .proxy import proxy_declaration
    from .fetch_sessions import fetch_session_policy
    if value is None:value={}
    if not isinstance(value,dict):raise ValueError('fetch_proxy_routes must map domains to proxy declarations')
    if default_session_pool is not None:
        if '*' in value:
            raise ValueError('Choose fetch_session_pool or a default fetch_proxy_routes entry')
        value={**value,'*':{'session_pool':default_session_pool}}
    def declaration(route):
        if isinstance(route,dict) and 'session_pool' in route:
            if set(route)!={'session_pool'}:
                raise ValueError('A primary fetch session pool cannot also declare a static or fallback pool')
            return {'session_pool':fetch_session_policy(route['session_pool'])}
        return pool_declaration(route) if isinstance(route,dict) and 'pool' in route else proxy_declaration(route)
    return {('*' if domain == '*' else normalized_blocked_domains([domain])[0]):
            declaration(route)
            for domain,route in value.items()}


def pool_declaration(value):
    from .proxy import proxy_declaration
    if set(value)-{'pool','cooldown_s','failure_limit','reuse_completed',
                   'transient_cooldown_s','cooldown_wait_s','fallback_session_pool',
                   'health_scope','max_host_health_entries','rotate_on_rate_limit'}:
        raise ValueError('Unknown fetch proxy pool option')
    routes=value['pool']
    if not isinstance(routes,(list,tuple)) or not 1<=len(routes)<=128:
        raise ValueError('Fetch proxy pool must contain 1 to 128 routes')
    names=set();result=[]
    for route in routes:
        if not isinstance(route,dict) or set(route)-{'name','proxy','interval_s','concurrency','reuse_connections'}:
            raise ValueError('Fetch route accepts name, proxy, interval_s, concurrency and reuse_connections')
        name=route.get('name');interval=route.get('interval_s',1.)
        concurrency=route.get('concurrency',1)
        reuse_connections=route.get('reuse_connections',True)
        if type(reuse_connections) is not bool:raise ValueError('reuse_connections must be boolean')
        if type(concurrency) is not int or not 1<=concurrency<=64:
            raise ValueError('Fetch route concurrency must be 1..64')
        if not isinstance(name,str) or not name or name in names:
            raise ValueError('Fetch route names must be unique and nonempty')
        if type(interval) not in (int,float) or not math.isfinite(interval) or interval<0:
            raise ValueError('Invalid fetch route interval')
        if 'proxy' not in route or route['proxy'] is None:
            raise ValueError('Fetch pool route requires an explicit proxy')
        names.add(name)
        result.append({'name':name,'proxy':proxy_declaration(route['proxy']),
                       'interval_s':interval,'concurrency':concurrency,
                       'reuse_connections':reuse_connections})
    cooldown=value.get('cooldown_s',1800.)
    failures=value.get('failure_limit',2)
    if type(cooldown) not in (int,float) or not math.isfinite(cooldown) or not 0<cooldown<=86400:
        raise ValueError('Invalid fetch route cooldown')
    if type(failures) is not int or failures<1:
        raise ValueError('Invalid fetch route failure limit')
    transient_cooldown=value.get('transient_cooldown_s',cooldown)
    wait=value.get('cooldown_wait_s',0.)
    if type(transient_cooldown) not in (int,float) or not math.isfinite(transient_cooldown) or not 0<transient_cooldown<=86400:
        raise ValueError('Invalid transient fetch route cooldown')
    if type(wait) not in (int,float) or not math.isfinite(wait) or not 0<=wait<=3600:
        raise ValueError('Invalid fetch route cooldown wait')
    reuse=value.get('reuse_completed',[])
    if not isinstance(reuse,(list,tuple)) or len(reuse)>128:
        raise ValueError('reuse_completed must contain at most 128 previous proxy declarations')
    from .fetch_sessions import fetch_session_policy
    extra={'fallback_session_pool':fetch_session_policy(value['fallback_session_pool'])} if 'fallback_session_pool' in value else {}
    scope=value.get('health_scope','pool');host_limit=value.get('max_host_health_entries',4096)
    if scope not in {'pool','host'}:raise ValueError('Fetch health scope must be pool or host')
    if type(host_limit) is not int or not 1<=host_limit<=65536:
        raise ValueError('Fetch host health limit must be 1..65536')
    rotate=value.get('rotate_on_rate_limit',False)
    if type(rotate) is not bool or rotate and scope!='host':
        raise ValueError('Rate-limit route rotation requires host-scoped health')
    return {'pool':result,'cooldown_s':cooldown,'failure_limit':failures,**extra,
            'transient_cooldown_s':transient_cooldown,'cooldown_wait_s':wait,
            'health_scope':scope,'max_host_health_entries':host_limit,
            'rotate_on_rate_limit':rotate,
            'reuse_completed':[{'session_pool':fetch_session_policy(p['session_pool'])}
                if isinstance(p,dict) and set(p)=={'session_pool'} else proxy_declaration(p) for p in reuse]}


def transport_identity(domain,declaration):
    from .native_search.config import public,resolve
    if isinstance(declaration,dict) and 'session_pool' in declaration:
        from .fetch_sessions import policy_identity
        declaration={'session_pool':policy_identity(declaration['session_pool'])}
    elif isinstance(declaration,dict) and 'pool' in declaration:
        # Pacing, health and prior receipts do not change request semantics.
        from .fetch_sessions import policy_identity
        extra={'fallback_session_pool':policy_identity(declaration['fallback_session_pool'])} if 'fallback_session_pool' in declaration else {}
        declaration={'pool':[{'name':r['name'],'proxy':r['proxy']} for r in declaration['pool']],**extra}
    signature=hashlib.sha256(json.dumps(resolve(declaration),sort_keys=True).encode()).hexdigest()
    return {'domain':domain,'declaration':public(declaration),'transport_digest':signature}


class FetchRoutePool:
    def __init__(self,web,domain,declaration):
        self.web=web;self.domain=domain;self.declaration=declaration
        self.routes=[];self.condition=asyncio.Condition();self.cursor=0
        self.cooldown_waiters=0;self.cooldown_waits=0;self.cooldown_wait_seconds=0.
        self.host_scoped=declaration.get('health_scope','pool')=='host'
        with web._db() as db:
            db.execute('''CREATE TABLE IF NOT EXISTS fetch_route_health (
                identity TEXT PRIMARY KEY, domain TEXT, name TEXT, until REAL,
                failures INTEGER, status TEXT)''')
            db.execute('''CREATE TABLE IF NOT EXISTS fetch_route_events (
                id INTEGER PRIMARY KEY, identity TEXT, domain TEXT, name TEXT,
                host TEXT, status TEXT, http_status INTEGER, elapsed_s REAL, observed_at REAL)''')
            db.execute('''CREATE TABLE IF NOT EXISTS fetch_route_host_health (
                identity TEXT, host TEXT, domain TEXT, until REAL, failures INTEGER,
                status TEXT, expires_at REAL, PRIMARY KEY(identity,host))''')
            db.execute('CREATE INDEX IF NOT EXISTS fetch_route_host_health_domain ON fetch_route_host_health(domain,expires_at)')
        for route in declaration['pool']:
            identity=hashlib.sha256(json.dumps(transport_identity(domain,route['proxy']),sort_keys=True).encode()).hexdigest()
            with web._db() as db:
                saved=db.execute('SELECT until,failures,status FROM fetch_route_health WHERE identity=?',(identity,)).fetchone()
            until,failures,status=saved if saved and not self.host_scoped else (0.,0,'')
            self.routes.append({**route,'identity':identity,'until':until,'failures':failures,
                'status':status,'busy':0,'peak_active':0,'next_ready':0.,'requests':0})

    @asynccontextmanager
    async def lease(self,host=None):
        if self.host_scoped and (not isinstance(host,str) or not host or len(host)>253):
            raise ValueError('Host-scoped fetch lease requires a bounded destination')
        cooldown_deadline=None
        async with self.condition:
            while True:
                wall_now=time.time()
                if self.host_scoped:
                    # At most 128 scalar records are loaded for this host; the
                    # route concurrency and pacing remain global across hosts.
                    with self.web._db() as db:
                        held={identity:until for identity,until in db.execute(
                            'SELECT identity,until FROM fetch_route_host_health WHERE host=? AND identity IN ('+
                            ','.join('?' for _ in self.routes)+')',
                            (host,*(r['identity'] for r in self.routes)))}
                    deadlines=[held.get(r['identity'],0.) for r in self.routes]
                else:deadlines=[r['until'] for r in self.routes]
                healthy=[(i,r) for i,r in enumerate(self.routes) if deadlines[i]<=wall_now]
                if not healthy:
                    now=time.monotonic()
                    if cooldown_deadline is None:
                        cooldown_deadline=now+self.declaration.get('cooldown_wait_s',0.)
                    remaining=cooldown_deadline-now
                    if remaining<=0:raise ServiceStopped('all_fetch_routes_cooling_down:'+self.domain)
                    delay=min(remaining,min(deadlines)-wall_now)
                    self.cooldown_waiters+=1;self.cooldown_waits+=1
                    try:
                        # Release the condition while waiting. No route is
                        # leased and no HTTP/extra retry is started. Repeated
                        # notifications/cooldown extensions share one deadline.
                        try:await asyncio.wait_for(self.condition.wait(),timeout=delay)
                        except asyncio.TimeoutError:pass
                    finally:
                        self.cooldown_waiters-=1
                        self.cooldown_wait_seconds+=time.monotonic()-now
                    continue
                idle=[(i,r) for i,r in healthy if r['busy']<r.get('concurrency',1)]
                if idle:
                    now=time.monotonic()
                    i,route=min(idle,key=lambda x:(max(now,x[1]['next_ready']),
                                                  (x[0]-self.cursor)%len(self.routes)))
                    delay=route['next_ready']-now
                    if delay>0:
                        # Do not reserve a paced route: its health may change
                        # before the next start. Recheck after every wakeup.
                        try:await asyncio.wait_for(self.condition.wait(),timeout=delay)
                        except asyncio.TimeoutError:pass
                        continue
                    route['busy']+=1
                    route['peak_active']=max(route['peak_active'],route['busy'])
                    route['next_ready']=now+route['interval_s']
                    self.cursor=(i+1)%len(self.routes);break
                await self.condition.wait()
        try:
            yield route
        finally:
            async with self.condition:
                route['busy']-=1;self.condition.notify_all()

    def record(self,route,host,status,code,elapsed,retry_after_s=0):
        route['requests']+=1;route['status']=status
        if self.host_scoped:
            return self.record_host(route,host,status,code,elapsed,retry_after_s)
        transient=status=='network_error' or code is not None and code>=500
        blocked=code in (403,429)
        if code is not None and 200<=code<400:
            route['failures']=0
        elif blocked or transient:
            route['failures']+=1
        if blocked or transient and route['failures']>=self.declaration['failure_limit']:
            cooldown=(self.declaration['cooldown_s'] if blocked else
                      self.declaration.get('transient_cooldown_s',self.declaration['cooldown_s']))
            route['until']=max(route['until'],time.time()+max(cooldown,retry_after_s))
        with self.web._db() as db:
            db.execute('INSERT OR REPLACE INTO fetch_route_health VALUES (?,?,?,?,?,?)',
                (route['identity'],self.domain,route['name'],route['until'],route['failures'],route['status']))
            db.execute('INSERT INTO fetch_route_events(identity,domain,name,host,status,http_status,elapsed_s,observed_at) VALUES (?,?,?,?,?,?,?,?)',
                (route['identity'],self.domain,route['name'],host,status,code,elapsed,time.time()))

    def record_host(self,route,host,status,code,elapsed,retry_after_s):
        now=time.time();blocked=code in (403,429)
        transient=status=='network_error' or code is not None and code>=500
        with self.web._db() as db:
            db.execute('BEGIN IMMEDIATE')
            previous=db.execute('SELECT until,failures,expires_at FROM fetch_route_host_health WHERE identity=? AND host=?',
                                (route['identity'],host)).fetchone()
            until,failures,expires=previous or (0.,0,0.)
            if expires<=now:until,failures=0.,0
            if code is not None and 200<=code<400:failures=0
            elif blocked or transient:failures+=1
            if blocked or transient and failures>=self.declaration['failure_limit']:
                cooldown=self.declaration['cooldown_s'] if blocked else self.declaration['transient_cooldown_s']
                until=max(until,now+max(cooldown,retry_after_s))
            if until or failures:
                # Failed observations below the threshold also expire, so
                # one isolated failure per old host cannot fill the journal.
                expires=until or now+self.declaration['transient_cooldown_s']
                if previous is None:
                    db.execute('DELETE FROM fetch_route_host_health WHERE domain=? AND expires_at<=?',(self.domain,now))
                    count=db.execute('SELECT count(*) FROM fetch_route_host_health WHERE domain=?',(self.domain,)).fetchone()[0]
                    if count>=self.declaration['max_host_health_entries']:
                        raise ServiceStopped('fetch_host_health_capacity')
                db.execute('INSERT OR REPLACE INTO fetch_route_host_health VALUES (?,?,?,?,?,?,?)',
                    (route['identity'],host,self.domain,until,failures,status,expires))
            elif previous is not None:
                db.execute('DELETE FROM fetch_route_host_health WHERE identity=? AND host=?',(route['identity'],host))
            db.execute('INSERT INTO fetch_route_events(identity,domain,name,host,status,http_status,elapsed_s,observed_at) VALUES (?,?,?,?,?,?,?,?)',
                (route['identity'],self.domain,route['name'],host,status,code,elapsed,now))

    def snapshot_metrics(self):
        return {r['name']:{'requests':r['requests'],'cooldown_until':r['until'],
            'consecutive_failures':r['failures'],'last_status':r['status'],
            'interval_s':r['interval_s'],'active':bool(r['busy']),
            'active_requests':r['busy'],'peak_active':r['peak_active'],
            'concurrency':r.get('concurrency',1),
            'pool_cooldown_waiters':self.cooldown_waiters,
            'pool_cooldown_waits':self.cooldown_waits,
            'pool_cooldown_wait_seconds':self.cooldown_wait_seconds} for r in self.routes}
