"""Renewable leases preserve evidence, backoff and aggregate admission."""
import asyncio
import json
import time

import pytest

from demiflow.collect.native_search import SearchConfig, NativeSearchSession
from demiflow.collect.search_sessions import RenewableSearchRoutePool, session_pool_policy
from demiflow.execution.request_limits import ServiceStopped
from test_search_route_pool import fixture


def factory(*,token,ttl_s,options):
    return 'http://s'+token+'.example:3128'


def test_background_replenishment_runs_without_new_queries(tmp_path,monkeypatch):
    fixture(monkeypatch)
    async def run():
        session=pool(tmp_path,background_maintenance=True)
        try:
            await session.search('initial')
            route=session.routes[1]
            await session.retire(route,'captcha',time.time());await session.save_state()
            async def replaced():
                while session.routes[1] is route:await asyncio.sleep(.005)
            await asyncio.wait_for(replaced(),1)
            assert session.state['created_total']==3 and route['session'].closed
            assert session.snapshot_metrics()['source_attempts']==1
        finally:await session.aclose()
        assert session.manager_task.done() and not session.close_tasks
    asyncio.run(run())


def test_background_cleanup_does_not_delay_fallback(tmp_path,monkeypatch):
    release=asyncio.Event();closing=asyncio.Event();calls=[]
    original_close=NativeSearchSession.aclose
    async def close(self):
        if getattr(self,'test_slow_close',False):
            closing.set();await release.wait()
        await original_close(self)
    async def call(self,context,message):
        if message['op']=='merge':return {'results':[r for g in message['groups'] for r in g['results']]}
        calls.append(message['query'])
        failed=len(calls)==1
        if failed:self.test_slow_close=True
        return {'status':'captcha' if failed else 'ok','reason':'fixture',
            'results':[] if failed else [{'url':'https://source.example/good','title':'good','content':'body'}],
            'http':[{'host':'search.example','http_status':302 if failed else 200,'elapsed_s':.001}]}
    monkeypatch.setattr(NativeSearchSession,'call',call)
    monkeypatch.setattr(NativeSearchSession,'aclose',close)
    async def run():
        session=pool(tmp_path,background_maintenance=True)
        try:
            result=await asyncio.wait_for(session.search('recover'),1)
            assert result['status']=='ok' and len(calls)==2 and closing.is_set()
            assert not release.is_set() and session.close_tasks
        finally:release.set();await session.aclose()
    asyncio.run(run())


def test_preparation_is_local_and_replay_does_not_start_manager(tmp_path,monkeypatch):
    from demiflow.collect.native_search.runtime import Worker
    from types import SimpleNamespace
    fixture(monkeypatch);starts=[]
    async def start(self,context):
        starts.append(context);self.context=context;self.process=SimpleNamespace(returncode=None)
    async def close(self):self.process=None;self.context=None
    monkeypatch.setattr(Worker,'start',start);monkeypatch.setattr(Worker,'close',close)
    async def run():
        first=pool(tmp_path,background_maintenance=True,worker_reserve=2)
        try:
            result=await first.search('initial')
            async def ready():
                while first.snapshot_metrics()['session_pool']['worker_ready']<2:await asyncio.sleep(.005)
            await asyncio.wait_for(ready(),1)
            assert starts==[('default','all')]*2
            assert first.snapshot_metrics()['source_attempts']==1
        finally:await first.aclose()
        before=len(starts)
        second=pool(tmp_path,background_maintenance=True,worker_reserve=2)
        try:
            assert await second.search('initial')==result
            assert second.manager_task is None and len(starts)==before
            assert second.snapshot_metrics()['http_requests']==0
        finally:await second.aclose()
    asyncio.run(run())


def test_request_language_is_explicit_for_prestarted_workers(tmp_path):
    args=dict(cache_path=tmp_path/'cache.sqlite',config=SearchConfig(engines=['google']),routes=[],
        session_pool=dict(factory=__name__+':factory',size=2,min_healthy=1,
                          background_maintenance=True,worker_reserve=2))
    with pytest.raises(ValueError,match='worker_language'):RenewableSearchRoutePool(**args)
    args['session_pool']['worker_language']='all'
    session=RenewableSearchRoutePool(**args)
    assert session.worker_language=='all'


def test_native_queue_reuses_prestarted_worker_for_request_language(tmp_path,monkeypatch):
    from demiflow.collect.native_search.runtime import Worker
    from types import SimpleNamespace
    starts=[]
    async def start(self,context):
        if self.process is not None and self.context==context:return
        starts.append(context);self.context=context;self.process=SimpleNamespace(returncode=None)
    async def close(self):self.process=None;self.context=None
    async def call(self,context,message):
        await self.start(context)
        if message['op']=='merge':return {'results':[r for g in message['groups'] for r in g['results']]}
        return {'status':'ok','reason':'','results':[{'url':'https://example.org/one','title':'one','content':'body'}],
                'http':[{'host':'search.example','http_status':200,'elapsed_s':.001}]}
    monkeypatch.setattr(Worker,'start',start);monkeypatch.setattr(Worker,'close',close);monkeypatch.setattr(Worker,'call',call)
    async def run():
        session=RenewableSearchRoutePool(cache_path=tmp_path/'cache.sqlite',
            config=SearchConfig(engines=['google'],workers=4,source_interval_s=0),routes=[],
            session_pool=dict(factory=__name__+':factory',size=2,min_healthy=1,
                background_maintenance=True,worker_reserve=2,worker_language='all'))
        try:
            await session.initialize();await session.maintain()
            for r in session.routes[1:]:r['preparing']=True;await session.prepare_worker(r)
            assert len(starts)==2
            assert (await session.search('first',language='all'))['status']=='ok'
            assert len(starts)==2  # search and merge both take the prepared worker
        finally:await session.aclose()
    asyncio.run(run())


def test_fallback_prefers_unused_route_over_previously_successful_route(tmp_path,monkeypatch):
    fixture(monkeypatch)
    async def run():
        session=pool(tmp_path,prefer_unused_fallback=True)
        try:
            await session.initialize();await session.maintain()
            session.routes[1]['entry']['healthy']=True
            session.cursor=1
            # A nonempty tried set marks the bounded fallback selection.
            index,route=await session.choose({99})
            assert index==2 and not route['entry']['healthy']
            route['busy']=False
        finally:await session.aclose()
    asyncio.run(run())


def test_busy_and_cooling_workers_do_not_fill_ready_reserve(tmp_path,monkeypatch):
    from demiflow.collect.native_search.runtime import Worker
    from types import SimpleNamespace
    started=[]
    async def start(self,context):
        started.append(self);self.context=context;self.process=SimpleNamespace(returncode=None)
    async def close(self):self.context=None;self.process=None
    monkeypatch.setattr(Worker,'start',start);monkeypatch.setattr(Worker,'close',close)
    async def run():
        session=pool(tmp_path,background_maintenance=True,worker_reserve=1)
        try:
            await session.initialize();await session.maintain()
            first,second=session.routes[1:]
            await session.prepare_worker(first)
            first['next_ready']=time.monotonic()+60
            session.schedule_preparation()
            await asyncio.gather(*list(session.prepare_tasks))
            assert session.worker_ready(second) and len(started)==2
            assert session.snapshot_metrics()['session_pool']['worker_ready_spares']==1
            second['busy']=True
            assert session.snapshot_metrics()['session_pool']['worker_ready_spares']==0
            second['busy']=False
        finally:await session.aclose()
    asyncio.run(run())


def pool(tmp_path,*,request_concurrency=2,adaptive=None,**policy):
    return RenewableSearchRoutePool(cache_path=tmp_path/'cache.sqlite',
        config=SearchConfig(engines=['google'],language='all',source_interval_s=0,request_concurrency=request_concurrency),
        routes=[],max_route_attempts=2,cooldown_s=.02,failure_limit=1,adaptive=adaptive,
        session_pool=dict(factory=__name__+':factory',size=2,min_healthy=1,ttl_s=10,
                          interval_s=.001,refill_interval_s=.001,replacement_delay_s=.001,
                          acquisition_timeout_s=.2,**policy))


def test_primary_only_connection_reuse_and_expiry(tmp_path,monkeypatch):
    calls=fixture(monkeypatch)
    async def run():
        session=pool(tmp_path)
        try:
            await session.search('first')
            await session.search('second')
            tokens=[r['entry']['token'] for r in session.routes[1:]]
            sessions=[r['session'] for r in session.routes[1:]]
            await session.search('third')
            assert calls[0][0]==calls[2][0] and all(not s.closed for s in sessions)
            assert session.state['created_total']==2
            assert all('session_journal_anchor'!=r['name'] for r in session.routes[1:])
            for r in session.routes[1:]:r['entry']['expires_at']=time.time()-1
            await session.maintain()
            assert all(s.closed for s in sessions)
            await asyncio.sleep(.01)
            await session.search('after expiry')
            assert any(r['entry']['token'] not in tokens for r in session.routes[1:])
            assert session.snapshot_metrics()['source_attempts']==4
        finally:await session.aclose()
    asyncio.run(run())


def test_failed_completed_query_replays_after_retirement_and_restart(tmp_path,monkeypatch):
    calls=[]
    async def call(self,context,message):
        if message['op']=='merge':return {'results':[]}
        if message['op']=='search':
            calls.append(message['query'])
            return {'status':'captcha','reason':'fixture','results':[],
                    'http':[{'host':'search.example','http_status':302,'elapsed_s':.001}]}
    monkeypatch.setattr(NativeSearchSession,'call',call)
    async def run():
        first=pool(tmp_path)
        result=await first.search('blocked')
        assert len(calls)==2 and first.state['retired_total']==2
        key=first.admission_key if first.admission else None
        await asyncio.sleep(.03);await first.maintain()
        assert first.state['created_total']==3
        assert await first.search('blocked')==result and len(calls)==2
        await first.aclose()
        second=pool(tmp_path)
        try:
            assert await second.search('blocked')==result and len(calls)==2
            assert second.state['created_total']==3
            assert len(second.history)==3
        finally:await second.aclose()
    asyncio.run(run())


def test_creation_budget_and_backoff_survive_restart(tmp_path,monkeypatch):
    fixture(monkeypatch)
    async def run():
        first=pool(tmp_path,max_creations_per_window=2)
        await first.search('initial')
        for r in first.routes[1:]:
            r['until']=time.time()+30
            await first.retire(r,'rate_limited',time.time())
        await first.save_state();await first.aclose()
        second=pool(tmp_path,max_creations_per_window=2)
        try:
            with pytest.raises(ServiceStopped,match='pool_exhausted'):
                await second.search('later')
            assert second.state['created_total']==2 and second.route_attempts==0
        finally:await second.aclose()
    asyncio.run(run())


def test_expired_leased_session_drains_before_replacement(tmp_path,monkeypatch):
    fixture(monkeypatch)
    async def run():
        session=pool(tmp_path)
        try:
            await session.initialize();i,route=await session.choose(set())
            old=route['session'];route['entry']['expires_at']=time.time()-1
            await session.maintain()
            assert route['entry']['retired']=='expired' and not old.closed
            assert session.routes[i] is route
            route['busy']=False;await session.release_route(route)
            assert old.closed
            await asyncio.sleep(.01);await session.maintain()
            assert session.routes[i] is not route
        finally:await session.aclose()
    asyncio.run(run())


def test_auth_error_blocks_new_sessions_and_preserves_result(tmp_path,monkeypatch):
    calls=[]
    async def call(self,context,message):
        if message['op']=='merge':return {'results':[]}
        calls.append(message['query'])
        return {'status':'authentication_error','reason':'fixture','results':[],
                'http':[{'host':'search.example','http_status':407,'elapsed_s':.001}]}
    monkeypatch.setattr(NativeSearchSession,'call',call)
    async def run():
        session=pool(tmp_path)
        try:
            with pytest.raises(ServiceStopped,match='authentication'):
                await session.search('auth')
            assert len(calls)==1
            assert (await session.search('auth'))['status']=='search_failed'
            with pytest.raises(ServiceStopped,match='authentication'):
                await session.search('next')
            assert session.state['created_total']==2 and len(calls)==1
        finally:await session.aclose()
    asyncio.run(run())


@pytest.mark.parametrize('value',[{'size':0},{'size':1,'min_healthy':2},{'ttl_s':float('inf')},
                                  {'max_creations_per_window':1},{'factory':'missing'},
                                  {'factory_options':{'password':'inline'}}, {'acquisition_timeout_s':301}])
def test_invalid_policy(value):
    with pytest.raises(ValueError):session_pool_policy({'factory':__name__+':factory',**value})


def test_size_and_ttl_changes_preserve_success_and_failed_query_history(tmp_path,monkeypatch):
    calls=fixture(monkeypatch)
    async def run():
        first=pool(tmp_path)
        result=await first.search('saved')
        old_key=first.pool_key;old_profiles=set(first.history)
        await first.aclose()
        # Adjusting scheduling must not silently discard receipt authorization.
        second=pool(tmp_path)
        second.policy.update(size=3,ttl_s=20)
        try:
            assert await second.search('saved')==result and len(calls)==1
            assert second.pool_key==old_key and old_profiles<=second.history
            assert all(r['entry']['ttl_s']==10 for r in second.routes[1:])
            await second.search('new')
            assert second.state['created_total']==3
        finally:await second.aclose()
    asyncio.run(run())


def test_cancelled_query_releases_lease_and_does_not_exceed_shared_gate(tmp_path,monkeypatch):
    calls=[];active=0;peak=0;entered=asyncio.Event()
    async def call(self,context,message):
        nonlocal active,peak
        if message['op']=='merge':return {'results':[]}
        async with self.http_gate.enter():
            calls.append(message['query']);active+=1;peak=max(peak,active);entered.set()
            try:await asyncio.sleep(.15)
            finally:active-=1
            return {'status':'no_results','reason':'','results':[],
                    'http':[{'host':'search.example','http_status':200,'elapsed_s':.15}]}
    monkeypatch.setattr(NativeSearchSession,'call',call)
    async def run():
        session=pool(tmp_path)
        try:
            tasks=[asyncio.create_task(session.search(str(i))) for i in range(2)]
            await entered.wait();tasks[0].cancel()
            await asyncio.gather(*tasks,return_exceptions=True)
            assert peak<=session.base_config.request_concurrency
            assert not any(r['busy'] for r in session.routes) and not session.inflight_queries
            assert session.state['created_total']==2
        finally:await session.aclose()
    asyncio.run(run())


def test_pool_grows_for_shared_pace_without_raising_http_admission(tmp_path,monkeypatch):
    fixture(monkeypatch)
    async def run():
        session=pool(tmp_path,max_size=5,capacity_reserve_ratio=1.25)
        session.policy['interval_s']=.02
        session.http_gate.interval_s=.005
        try:
            await session.initialize()
            assert session.target_size()==5
            await session.maintain()
            assert session.state['created_total']==2  # bounded initial burst
            for _ in range(3):
                await asyncio.sleep(.01);await session.maintain()
            assert session.state['created_total']==5
            assert session.http_gate.interval_s==.005
            assert session.snapshot_metrics()['session_pool']['maximum']==5
            before=[r['session'] for r in session.routes[1:]]
            # A slower shared gate stops refilling excess slots; healthy leases
            # are retained until expiry, avoiding resize/reconnect oscillation.
            session.http_gate.interval_s=.1
            await session.maintain()
            assert session.target_size()==2 and not any(s.closed for s in before)
            await session.search('new')
            assert session.state['created_total']==5
        finally:await session.aclose()
    asyncio.run(run())


def test_generation_initialization_does_not_recompute_historical_configs(tmp_path,monkeypatch):
    fixture(monkeypatch)
    calls=[];original=NativeSearchSession._initialize
    def initialize(self):
        calls.append(str(self.config.proxy));return original(self)
    monkeypatch.setattr(NativeSearchSession,'_initialize',initialize)
    async def run():
        session=pool(tmp_path)
        try:
            await session.initialize();calls.clear()
            await session.maintain()
            assert len(calls)==2
            # Profile membership/precedence is shared across live sessions;
            # keep one immutable sequence instead of copying it per slot.
            assert session.routes[1]['session'].reuse_profiles is session.routes[2]['session'].reuse_profiles
        finally:await session.aclose()
    asyncio.run(run())


@pytest.mark.parametrize('value',[{'max_size':4},{'max_size':257},{'capacity_reserve_ratio':.5}])
def test_invalid_elastic_policy(value):
    with pytest.raises(ValueError):session_pool_policy({'factory':__name__+':factory',**value})


def test_busy_healthy_pool_is_fifo_backpressure_not_exhaustion(tmp_path,monkeypatch):
    fixture(monkeypatch)
    async def run():
        session=pool(tmp_path)
        session.policy['acquisition_timeout_s']=.02
        try:
            await session.initialize()
            a=await session.choose(set());b=await session.choose(set())
            order=[]
            async def get(n):
                i,r=await session.choose(set());order.append(n)
                async with session.condition:r['busy']=False;session.condition.notify_all()
            one=asyncio.create_task(get(1));await asyncio.sleep(.01)
            two=asyncio.create_task(get(2))
            await asyncio.sleep(.08)  # greater than the exhaustion timeout
            assert not one.done() and not two.done()
            async with session.condition:a[1]['busy']=False;session.condition.notify_all()
            await asyncio.wait_for(asyncio.gather(one,two),1)
            assert order==[1,2]
            b[1]['busy']=False
        finally:await session.aclose()
    asyncio.run(run())


def test_previous_release_cannot_close_newly_leased_expired_slot(tmp_path,monkeypatch):
    fixture(monkeypatch)
    async def run():
        session=pool(tmp_path)
        try:
            await session.initialize();_,route=await session.choose(set())
            route['entry']['expires_at']=time.time()-1
            # Simulate the prior owner's delayed release hook after a new owner
            # has acquired the slot. The current owner drains before closure.
            await session.release_route(route)
            assert not route['session'].closed
            route['busy']=False;await session.release_route(route)
            assert route['session'].closed
        finally:await session.aclose()
    asyncio.run(run())


def test_expiry_before_http_reacquires_without_consuming_query_attempt(tmp_path,monkeypatch):
    calls=[]
    async def call(self,context,message):
        if message['op']=='merge':return {'results':[]}
        calls.append(message['query'])
        if len(calls)==1:self.http_gate.route['entry']['expires_at']=time.time()-1
        async with self.http_gate.enter():
            self.metrics['http_requests']+=1
            return {'status':'no_results','reason':'','results':[],
                    'http':[{'host':'search.example','http_status':200,'elapsed_s':.001}]}
    monkeypatch.setattr(NativeSearchSession,'call',call)
    async def run():
        session=pool(tmp_path);session.max_route_attempts=1
        try:
            assert (await session.search('one network attempt'))['status']=='no_results'
            assert len(calls)==2 and session.snapshot_metrics()['http_requests']==1
            assert session.expired_before_http==1 and session.query_gate.consecutive==0
        finally:await session.aclose()
    asyncio.run(run())


def test_continuous_pre_http_expiry_stops_without_counting_target_failures(tmp_path,monkeypatch):
    async def call(self,context,message):
        if message['op']=='merge':return {'results':[]}
        self.http_gate.route['entry']['expires_at']=time.time()-1
        async with self.http_gate.enter():raise AssertionError('No HTTP may be granted')
    monkeypatch.setattr(NativeSearchSession,'call',call)
    async def run():
        session=pool(tmp_path)
        try:
            with pytest.raises(ServiceStopped,match='expiry_reacquisition_limit'):
                await session.search('bounded local renewal')
            assert session.expired_before_http==3 and session.snapshot_metrics()['http_requests']==0
            assert session.query_gate.consecutive==0 and not any(r['busy'] for r in session.routes)
        finally:await session.aclose()
    asyncio.run(run())


def test_expiry_after_first_hop_preserves_receipt_and_other_queries(tmp_path,monkeypatch):
    from demiflow.collect.search_errors import RouteLeaseExpired
    calls=[]
    async def call(self,context,message):
        if message['op']=='merge':return {'results':[]}
        calls.append(message['query'])
        async with self.http_gate.enter():
            self.metrics['http_requests']+=1
        evidence=[{'host':'search.example','http_status':200,'elapsed_s':.001}]
        if message['query']=='expired second hop':
            self.http_gate.route['entry']['expires_at']=time.time()-1
            try:
                async with self.http_gate.enter():raise AssertionError('Expired lease admitted HTTP')
            except RouteLeaseExpired as exc:
                exc.native_http_receipts=evidence
                raise
        return {'status':'no_results','reason':'','results':[], 'http':evidence}
    monkeypatch.setattr(NativeSearchSession,'call',call)
    async def run():
        session=pool(tmp_path)
        try:
            result=await session.search('expired second hop')
            assert result['status']=='search_failed'
            receipt=result['engine_receipts'][0]
            assert receipt['status']=='interrupted' and receipt['local_lease_expired']
            assert receipt['attempts'][0]['http'][0]['http_status']==200
            assert session.query_gate.consecutive==0
            assert await session.search('expired second hop')==result
            assert (await session.search('next query'))['status']=='no_results'
            assert calls==['expired second hop','next query']
            assert session.snapshot_metrics()['http_requests']==2
        finally:await session.aclose()
        restarted=pool(tmp_path)
        try:
            assert await restarted.search('expired second hop')==result
            assert restarted.snapshot_metrics()['http_requests']==0
        finally:await restarted.aclose()
    asyncio.run(run())


def test_queued_rows_do_not_pin_leases_beyond_execution_capacity(tmp_path,monkeypatch):
    entered=asyncio.Event();release=asyncio.Event()
    async def call(self,context,message):
        if message['op']=='merge':return {'results':[]}
        async with self.http_gate.enter():
            self.metrics['http_requests']+=1;entered.set();await release.wait()
            return {'status':'no_results','reason':'','results':[],
                    'http':[{'host':'search.example','http_status':200,'elapsed_s':.001}]}
    monkeypatch.setattr(NativeSearchSession,'call',call)
    async def run():
        session=pool(tmp_path,request_concurrency=1)
        try:
            first=asyncio.create_task(session.search('one'));await entered.wait()
            second=asyncio.create_task(session.search('two'));await asyncio.sleep(.04)
            assert sum(r['busy'] for r in session.routes)==1
            release.set();results=await asyncio.wait_for(asyncio.gather(first,second),2)
            assert all(r['status']=='no_results' for r in results)
        finally:release.set();await session.aclose()
    asyncio.run(run())


def test_global_circuit_does_not_retire_every_waiting_lease(tmp_path,monkeypatch):
    entered=asyncio.Event();release=asyncio.Event();calls=[]
    async def call(self,context,message):
        if message['op']=='merge':return {'results':[]}
        calls.append(message['query'])
        # The sibling is now queued before worker startup/HTTP admission.
        # Both calls still have to observe the shared circuit without being
        # recorded as two independent route failures.
        entered.set()
        await release.wait()
        async with self.http_gate.enter():raise AssertionError('Circuit must block HTTP')
    monkeypatch.setattr(NativeSearchSession,'call',call)
    async def run():
        session=pool(tmp_path,adaptive={'initial_concurrency':2,'max_concurrency':2})
        tasks=[]
        try:
            tasks=[asyncio.create_task(session.search(str(i))) for i in range(2)]
            await asyncio.wait_for(entered.wait(),2)
            session.admission.stopped_until=time.time()+60;release.set()
            results=await asyncio.wait_for(asyncio.gather(*tasks,return_exceptions=True),3)
            assert all(isinstance(r,ServiceStopped) and 'failure_window' in str(r) for r in results)
            assert session.state['retired_total']==0 and session.admission.total_samples==0
            assert session.query_gate.consecutive==0 and not any(r['busy'] for r in session.routes)
            with session.routes[0]['session']._db() as db:
                assert db.execute("select count(*) from native_search_route_events where status='service_stopped'").fetchone()[0]==0
        finally:
            release.set()
            await asyncio.gather(*tasks,return_exceptions=True)
            await session.aclose()
    asyncio.run(run())


@pytest.mark.parametrize('background', [False, True])
def test_near_expiry_idle_lease_is_retired_before_admission(tmp_path,monkeypatch,background):
    calls=fixture(monkeypatch)
    async def run():
        session=pool(tmp_path,background_maintenance=background)
        try:
            await session.initialize();await session.maintain()
            route=session.routes[1];old_profile=route['session'].profile
            route['entry']['expires_at']=time.time()+.1
            assert (await session.search('fresh generation'))['status']=='ok'
            assert route['entry']['retired']=='expired' and route['session'].closed
            assert session.expired_before_http==0 and len(calls)==1
            assert old_profile in session.history
        finally:await session.aclose()
    asyncio.run(run())
