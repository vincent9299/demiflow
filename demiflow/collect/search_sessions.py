"""Bounded renewable route leases; all network work shares native admission.

Factories are consumer declarations: they receive an opaque generation token and
return a Secret-only proxy declaration. No provider or target-specific rules live
here. The source runtime and completed query identities remain unchanged.
"""
import asyncio
import copy
import importlib
import json
import math
import time
import uuid
from collections import Counter

from .native_search import SearchConfig
from .native_search.config import digest, public, validate_secrets
from .native_search.config import validate_language
from .search_routes import SearchRoutePool, _RoutedSession, _RouteGate, route_declarations, RouteLeaseExpired
from demiflow.execution.request_limits import ServiceStopped


DEFAULTS = dict(size=32, ttl_s=300., interval_s=5., refill_interval_s=.5,
    replacement_delay_s=2., creation_window_s=60., max_creations_per_window=96,
    min_healthy=8, shortfall_s=30.,
    acquisition_timeout_s=60., factory=None, factory_options={},
    max_size=None, capacity_reserve_ratio=1.25,
    background_maintenance=False, worker_reserve=0, worker_prepare_concurrency=4,
    worker_language=None, prefer_unused_fallback=False)


def session_pool_policy(value):
    if value is None:return None
    if not isinstance(value,dict) or set(value)-set(DEFAULTS):
        raise ValueError('Invalid search session pool fields')
    result={**copy.deepcopy(DEFAULTS),**copy.deepcopy(value)}
    factory=result['factory']
    if not isinstance(factory,str) or ':' not in factory or not all(factory.split(':')):
        raise ValueError('Session factory requires an importable module:function')
    if not isinstance(result['factory_options'],dict):
        raise ValueError('Session factory options must be a mapping')
    # A factory must accept secret environment *names*, never resolved values.
    validate_secrets(result['factory_options'])
    json.dumps(result['factory_options'])
    for key,n in result.items():
        if key in {'factory','factory_options'}:continue
        if key=='worker_language':
            if n is not None:validate_language(n)
            continue
        if key in {'background_maintenance','prefer_unused_fallback'}:
            if type(n) is not bool:raise ValueError(key+' must be boolean')
            continue
        if key=='worker_reserve':
            if type(n) is not int or not 0<=n<=256:raise ValueError('worker_reserve must be within 0..256')
            continue
        if key=='max_size':
            if n is not None and (type(n) is not int or not result['size']<=n<=256):
                raise ValueError('max_size must be None or an integer from size to 256')
            continue
        if type(n) not in (int,float) or not math.isfinite(n) or n<=0:
            raise ValueError('Session pool setting must be positive: '+key)
        if isinstance(DEFAULTS[key],int) and type(n) is not int:
            raise ValueError('Session pool integer required: '+key)
    if not 1<=result['min_healthy']<=result['size']<=256:
        raise ValueError('Invalid session pool size/healthy bounds')
    if result['max_creations_per_window']<result['size']:
        raise ValueError('Creation window must accommodate the initial pool')
    if result['ttl_s']>86400 or result['acquisition_timeout_s']>300:
        raise ValueError('Session lifetime/acquisition timeout exceeds bound')
    if not 1<=result['capacity_reserve_ratio']<=4:
        raise ValueError('Session capacity reserve must be within 1..4')
    if result['worker_reserve'] and not result['background_maintenance']:
        raise ValueError('Worker preparation requires background maintenance')
    if result['worker_prepare_concurrency']>32:
        raise ValueError('Worker preparation concurrency exceeds bound')
    return result


class RenewableSearchRoutePool(SearchRoutePool):
    """Bounded renewable pool; static declarations are reuse history only.

    A slot can have just one live generation. Expired/bad generations drain and
    close before replacement. Durable history authorizes their original source
    profiles for replay, including failed completed aggregate queries.
    """
    def __init__(self,*,session_pool,**kwargs):
        self.policy=session_pool_policy(session_pool)
        requested_attempts=kwargs.pop('max_route_attempts',2)
        if type(requested_attempts) is not int or not 1<=requested_attempts<=self.policy['size']:
            raise ValueError('Attempt limit exceeds renewable pool size')
        # An anchor initializes the journal and stable admission identity. It
        # never receives fresh searches; only generated leases can be chosen.
        previous=route_declarations(kwargs['config'],kwargs.get('routes',()))
        kwargs['reuse_configs']=[*kwargs.get('reuse_configs',()),
            *[SearchConfig.from_mapping({**kwargs['config'].snapshot(),'proxy':r['proxy']}) for r in previous]]
        kwargs['routes']=[dict(name='session_journal_anchor',proxy=public(kwargs['config'].proxy))]
        super().__init__(max_route_attempts=1,**kwargs)
        self.max_route_attempts=requested_attempts
        self.anchor_count=1
        self.base_config=kwargs['config']
        self.worker_language=self.policy['worker_language'] or self.base_config.language
        if self.policy['worker_reserve'] and self.worker_language is None:
            raise ValueError('Declare worker_language when request languages are not fixed in SearchConfig')
        self.lifecycle_lock=asyncio.Lock()
        self.lease_lock=asyncio.Lock()
        self.pool_initialized=False
        self.history=set()
        self.retired_metrics=Counter()
        self.state=dict(slots={},created=[],next_create=0.,short_since=None,
                        blocked_until=0.,created_total=0,retired_total=0)
        self.manager_task=None
        self.manager_error=''
        self.prepare_tasks=set();self.close_tasks=set()
        self.prepared_total=0

    async def initialize(self):
        await super().initialize()
        async with self.lifecycle_lock:
            if self.pool_initialized:return
            # Includes the salted base proxy identity: a credential/provider
            # change cannot silently authorize unrelated history. Generation
            # changes leave the shared admission/circuit identity untouched.
            self.pool_key=digest(['renewable-search-1',self.policy['factory'],self.policy['factory_options'],
                                 [r['session'].profile for r in self.routes]])
            module,name=self.policy['factory'].split(':',1)
            self.factory=getattr(importlib.import_module(module),name)
            def restore():
                with self.routes[0]['session']._db() as db:
                    db.execute('CREATE TABLE IF NOT EXISTS native_search_session_pools '
                               '(identity TEXT PRIMARY KEY, state_json TEXT NOT NULL, observed_at REAL NOT NULL)')
                    db.execute('CREATE TABLE IF NOT EXISTS native_search_session_history '
                               '(pool TEXT NOT NULL, token TEXT NOT NULL, profile TEXT NOT NULL, '
                               'created_at REAL NOT NULL, PRIMARY KEY(pool,token))')
                    row=db.execute('SELECT state_json FROM native_search_session_pools WHERE identity=?',
                                   (self.pool_key,)).fetchone()
                    history=[p for (p,) in db.execute('SELECT profile FROM native_search_session_history WHERE pool=?',
                                                    (self.pool_key,))]
                    return json.loads(row[0]) if row else None,history
            previous,history=await asyncio.to_thread(restore)
            if previous is not None:self.state=previous
            self.history.update(history);self.extra_reuse_profiles.update(history)
            # Rebuild bounded slot state, not every historical generation.
            # History keeps profiles, not proxy values or passwords.
            for slot,entry in sorted(self.state['slots'].items(),key=lambda x:int(x[0])):
                route=await self.make_route(int(slot),entry)
                self.routes.append(route)
            self.refresh_reuse()
            self.pool_initialized=True

    def refresh_reuse(self):
        profiles=tuple(sorted(self.allowed_profiles()))
        for route in self.routes:
            route['session'].reuse_profiles=profiles

    async def make_route(self,slot,entry):
        declaration=self.factory(token=entry['token'],ttl_s=entry['ttl_s'],
                                 options=copy.deepcopy(self.policy['factory_options']))
        # Native validation rejects inline authenticated URLs. A public receipt
        # contains only environment references supplied by the factory.
        declaration=route_declarations(self.base_config,[dict(name='session_'+entry['token'],
            proxy=declaration,interval_s=self.policy['interval_s'],reuse_connections=True)])[0]
        source=SearchConfig.from_mapping({**self.base_config.snapshot(),'proxy':declaration['proxy']})
        session=_RoutedSession(cache_path=self.routes[0]['session'].path,config=source,
                               reuse_configs=[])
        session.query_gate=None;session.execution_gate=self.execution_gate
        session.check_admission=self.check_admission
        route={**declaration,'session':session,'slot':slot,
               'busy':False,'next_ready':0.,'until':0.,'failures':0,'last_status':'',
               'entry':entry,'published':True,'preparing':False}
        session.http_gate=_RouteGate(self,route)
        try:
            await session.initialize()
            # The anchor already verified historical declarations. Repeating
            # that work for every generation makes replenishment scale as the
            # product of pool size and history size. Reuse the verified set.
            session.reuse_profiles=sorted(self.allowed_profiles()-{session.profile})
            if entry.get('profile') not in (None,session.profile):
                raise ValueError('Session factory changed a persisted generation identity')
            entry['profile']=session.profile
            if entry.get('retired'):await session.aclose()
        except BaseException:
            await session.aclose();raise
        return route

    async def save_state(self,created=None):
        value=json.dumps(self.state)
        def save():
            with self.routes[0]['session']._db() as db:
                if created:
                    db.execute('INSERT INTO native_search_session_history VALUES (?,?,?,?)',
                               (self.pool_key,created['token'],created['profile'],created['created_at']))
                db.execute('INSERT OR REPLACE INTO native_search_session_pools VALUES (?,?,?)',
                           (self.pool_key,value,time.time()))
        await asyncio.to_thread(save)

    def check_admission(self):
        super().check_admission()
        if self.manager_error:raise ServiceStopped(self.manager_error)
        if self.state['blocked_until']>time.time():
            raise ServiceStopped('search_session_authentication_or_configuration')

    def healthy_count(self,now):
        return sum(not r['entry']['retired'] and r['entry']['healthy']
                   and r['entry']['expires_at']>now for r in self.routes[self.anchor_count:])

    def recovery_inventory(self):
        now=time.time()
        routes=[r for r in self.routes[self.anchor_count:] if r['published']
                and not r['entry']['retired'] and r['entry']['expires_at']>now and r['until']<=now]
        healthy=sum(r['entry']['healthy'] for r in routes)
        # Busy/paced leases still belong to the usable inventory. Cold leases
        # are eligible first attempts, explicitly separate from proven health.
        return {'viable':len(routes),'healthy':healthy,'untried':len(routes)-healthy,
                'leased':sum(r['busy'] for r in routes),
                'paced':sum(r['next_ready']>time.monotonic() for r in routes)}

    def check_route(self,route):
        if route['entry']['retired'] or route['entry']['expires_at']<=time.time():
            raise RouteLeaseExpired('search_session_lease_expired')

    def lease_limit(self):
        return self.admission.concurrency if self.admission else self.base_config.request_concurrency

    def expiry_margin(self,route):
        # Reserve time for worker startup, its bounded request and shared HTTP
        # pacing. A short TTL still leaves most of its lifetime usable.
        estimated=self.base_config.startup_timeout_s+self.base_config.timeout_s+self.http_gate.interval_s*(self.lease_limit()+1)
        return min(route['entry']['ttl_s']/4,estimated)

    def target_size(self):
        p=self.policy
        if p['max_size'] is None:return p['size']
        # Maintain enough independent leases for the already admitted global
        # pace, plus bounded spare capacity. This never raises HTTP admission.
        if self.http_gate.interval_s<=0:return p['max_size']
        needed=math.ceil(p['interval_s']/self.http_gate.interval_s*p['capacity_reserve_ratio'])
        return min(p['max_size'],max(p['size'],needed))

    async def retire(self,route,reason,now):
        entry=route['entry']
        if entry['retired']:return
        entry.update(retired=reason,healthy=False,replace_after=max(now+self.policy['replacement_delay_s'],route['until']))
        self.state['retired_total']+=1
        # Caller never closes a leased route. Its finally hook drains it first.
        if not route['busy'] and not route['preparing']:await self.close_route(route)

    async def close_route(self,route):
        if not self.policy['background_maintenance']:
            await route['session'].aclose();return
        if route.get('close_scheduled'):return
        route['close_scheduled']=True
        async def close():
            try:await route['session'].aclose()
            except Exception:self.manager_error='search_session_cleanup_failed'
        task=asyncio.create_task(close());self.close_tasks.add(task)
        task.add_done_callback(self.close_tasks.discard)

    def worker_ready(self,route):
        context=('default',self.worker_language)
        return any(w.process is not None and w.process.returncode is None and w.context==context
                   for w in route['session'].workers)

    async def prepare_worker(self,route):
        # Bootstrap only: no target probe, HTTP attempt, TLS readiness claim or
        # invented health result. The native queue hands this worker to search.
        session=route['session'];worker=None
        try:
            worker=await session.available.get()
            await worker.start(('default',self.worker_language))
            self.prepared_total+=1
        except asyncio.CancelledError:raise
        except Exception:self.manager_error='search_session_worker_preparation_failed'
        finally:
            if worker is not None:session.available.put_nowait(worker)
            route['preparing']=False
            if route['entry']['retired']:await self.close_route(route)
            async with self.condition:self.condition.notify_all()

    def schedule_preparation(self):
        clock=time.monotonic();now=time.time()
        live=[r for r in self.routes[self.anchor_count:] if r['published'] and not r['entry']['retired']
              and not r['busy'] and r['next_ready']<=clock
              and r['entry']['expires_at']>now+self.expiry_margin(r)]
        prepared=sum(r['preparing'] or self.worker_ready(r) for r in live)
        allowance=min(self.policy['worker_reserve']-prepared,
                      self.policy['worker_prepare_concurrency']-len(self.prepare_tasks))
        for route in [r for r in live if not r['busy'] and not r['preparing'] and not self.worker_ready(r)][:max(0,allowance)]:
            route['preparing']=True
            task=asyncio.create_task(self.prepare_worker(route));self.prepare_tasks.add(task)
            task.add_done_callback(self.prepare_tasks.discard)

    async def manage(self):
        try:
            while not self.closed:
                await self.maintain()
                self.schedule_preparation()
                async with self.condition:self.condition.notify_all()
                await asyncio.sleep(self.policy['refill_interval_s'])
        except asyncio.CancelledError:raise
        except ServiceStopped as exc:self.manager_error=str(exc)
        except Exception:self.manager_error='search_session_maintenance_failed'
        finally:
            async with self.condition:self.condition.notify_all()

    async def maintain(self):
        async with self.lifecycle_lock:
            if self.closed:raise ServiceStopped('search_route_pool_closed')
            self.check_admission()
            now=time.time();p=self.policy;s=self.state
            target=self.target_size();ceiling=p['max_size'] or p['size']
            for route in self.routes[self.anchor_count:]:
                deadline=now if route['busy'] or route['preparing'] else now+self.expiry_margin(route)
                if route['entry']['expires_at']<=deadline or route['slot']>=ceiling:
                    await self.retire(route,'expired' if route['slot']<ceiling else 'pool_shrunk',now)
            healthy=self.healthy_count(now)
            if healthy<p['min_healthy']:
                if s['short_since'] is None:s['short_since']=now
            else:
                s['short_since']=None
            s['created']=[v for v in s['created'] if now-v<p['creation_window_s']]
            # One refill per tick after the initial allocation. Factories are
            # local declaration-only operations: probes use normal requests.
            existing={r['slot']:r for r in self.routes[self.anchor_count:]}
            missing=[i for i in range(target) if i not in existing]
            replace=[i for i,r in existing.items() if i<target and r['entry']['retired'] and not r['busy'] and not r['preparing']
                     and r['entry']['replace_after']<=now]
            choices=missing+replace
            allowance=p['max_creations_per_window']-len(s['created'])
            take=min(len(missing),p['size'],allowance) if not existing else min(1,allowance)
            if now<s['next_create']:take=0
            for slot in choices[:take]:
                entry=dict(token=uuid.uuid4().hex,created_at=now,ttl_s=p['ttl_s'],expires_at=now+p['ttl_s'],
                           retired='',healthy=False,replace_after=0.)
                route=await self.make_route(slot,entry)
                route['published']=False
                old=existing.get(slot)
                if old:
                    await self.close_route(old)
                    for k,v in old['session'].snapshot_metrics().items():
                        if type(v) in (int,float):self.retired_metrics[k]+=v
                    self.routes[self.routes.index(old)]=route
                else:self.routes.append(route)
                s['slots'][str(slot)]=entry;s['created'].append(now);s['created_total']+=1
                s['next_create']=now+p['refill_interval_s']
                self.history.add(entry['profile']);self.extra_reuse_profiles.add(entry['profile'])
                # Persist a new identity before it can issue its first request.
                await self.save_state(created=entry)
                route['published']=True
            if choices[:take]:self.refresh_reuse()
            await self.save_state()

    async def choose(self,tried):
        # FIFO acquisition prevents a steady stream of new rows from starving
        # a waiter. Normal contention is backpressure, not pool exhaustion.
        async with self.lease_lock:
            return await self._choose(tried)

    async def _choose(self,tried):
        unavailable_s=0.
        if self.policy['background_maintenance'] and self.manager_task is None:
            # Start only for fresh work. Cache inspection/replay creates no
            # generations or workers and does not alter the persisted pool.
            self.manager_task=asyncio.create_task(self.manage())
        while True:
            if not self.policy['background_maintenance']:await self.maintain()
            async with self.condition:
                self.check_admission()
                if self.closed:raise ServiceStopped('search_route_pool_closed')
                now=time.time()
                eligible=[(i,r) for i,r in enumerate(self.routes) if i>=self.anchor_count
                            and i not in tried and r['published'] and not r['entry']['retired']
                            and r['entry']['expires_at']>now+self.expiry_margin(r)]
                # Background maintenance may not have run yet after restoring
                # a pool. Enforce the same idle-lease margin at acquisition;
                # otherwise a nearly expired generation can become busy before
                # the maintenance task gets a chance to retire it.
                # Lease only work that can execute now. Waiting rows must not
                # pin an entire pool while the global source gate is small.
                leased=sum(r['busy'] for r in self.routes[self.anchor_count:])
                clock=time.monotonic()
                candidates=[(i,r) for i,r in eligible if not r['busy'] and not r['preparing'] and r['next_ready']<=clock]
                if leased>=self.lease_limit():candidates=[]
                if candidates:
                    clock=time.monotonic()
                    i,route=min(candidates,key=lambda x:(bool(x[1]['entry']['healthy']) if tried and self.policy['prefer_unused_fallback'] else False,
                                                        not self.worker_ready(x[1]) if self.policy['worker_reserve'] else False,
                                                        max(clock,x[1]['next_ready']),
                                                        (x[0]-self.cursor)%len(self.routes)))
                    route['busy']=True;self.cursor=(i+1)%len(self.routes)
                    return i,route
                remaining=self.policy['acquisition_timeout_s']-unavailable_s
                if not eligible and remaining<=0:raise ServiceStopped('search_session_pool_exhausted')
                began=time.monotonic()
                wait_s=.5 if eligible else min(.5,remaining)
                try:await asyncio.wait_for(self.condition.wait(),timeout=wait_s)
                except asyncio.TimeoutError:pass
                finally:
                    if not eligible:unavailable_s+=time.monotonic()-began

    async def observe_route(self,route,result,*,success,status):
        async with self.lifecycle_lock:
            now=time.time()
            if status in {'authentication_error','configuration_error'}:
                self.state['blocked_until']=max(self.state['blocked_until'],now+300.)
            if success and status not in {'captcha','rate_limited','access_denied','authentication_error','configuration_error'}:
                route['entry']['healthy']=True
            else:
                # Honor Retry-After and cooldown before replacing identities.
                await self.retire(route,status,now)
            await self.save_state()

    async def release_route(self,route):
        async with self.lifecycle_lock:
            # A new owner may have acquired the slot after the previous owner
            # published busy=False. Only that new owner may close its lease.
            if route['busy']:return
            if route['entry']['retired'] or route['entry']['expires_at']<=time.time():
                await self.retire(route,'expired',time.time())
                await self.close_route(route)
                await self.save_state()

    def snapshot_metrics(self):
        result=super().snapshot_metrics()
        for key in ('source_attempts','http_requests','reused','reused_previous_proxy','reused_selected_receipt','worker_starts'):
            result[key]=result.get(key,0)+self.retired_metrics[key]
        now=time.time()
        primary=self.routes[self.anchor_count:]
        result['session_pool']={**{k:self.state[k] for k in ('created_total','retired_total','short_since','blocked_until')},
            'target':self.target_size(),'maximum':self.policy['max_size'] or self.policy['size'],'ttl_s':self.policy['ttl_s'],
            'sustained_shortfall':self.healthy_count(now)<self.policy['min_healthy'] and self.state['short_since'] is not None and now-self.state['short_since']>=self.policy['shortfall_s'],
            'healthy':self.healthy_count(now),'leased':sum(r['busy'] for r in primary),
            'cold':sum(not r['entry']['healthy'] and not r['entry']['retired'] for r in primary),
            'retired_slots':sum(bool(r['entry']['retired']) for r in primary),
            'profiles_preserved':len(self.history)}
        result['session_pool'].update(background_maintenance=self.policy['background_maintenance'],
            worker_ready=sum(self.worker_ready(r) for r in primary if not r['entry']['retired']),
            worker_ready_spares=sum(self.worker_ready(r) for r in primary if not r['entry']['retired']
                and not r['busy'] and r['next_ready']<=time.monotonic() and r['entry']['expires_at']>now+self.expiry_margin(r)),
            worker_preparing=len(self.prepare_tasks),worker_prepared_total=self.prepared_total,
            cleanup_pending=len(self.close_tasks),manager_error=self.manager_error)
        return result

    async def aclose(self):
        if self.manager_task:
            self.manager_task.cancel()
            await asyncio.gather(self.manager_task,return_exceptions=True)
        tasks=list(self.prepare_tasks)
        for task in tasks:task.cancel()
        await asyncio.gather(*tasks,return_exceptions=True)
        await super().aclose()
        await asyncio.gather(*list(self.close_tasks),return_exceptions=True)
