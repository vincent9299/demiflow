"""Automatic capacity changes must preserve bounds, receipts, and cancellation."""
import asyncio
import json
import time
import pytest

from demiflow.collect.search_admission import SearchAdmission, ResizableAdmission, adaptive_policy
from demiflow.collect.search_routes import SearchRoutePool
from demiflow.collect.native_search import SearchConfig, NativeSearchSession
from demiflow.collect.session import WebSession


def test_unpaced_admission_adjusts_capacity_without_enabling_pacing():
    policy=dict(min_interval_s=0,initial_interval_s=0,max_interval_s=0,
                initial_concurrency=4,max_concurrency=8)
    control=SearchAdmission(policy,now=0)
    event=control.observe(success=False,congestion=True,now=1)
    assert event['after']=={'concurrency':2,'interval_s':0}
    restored=SearchAdmission(policy);restored.restore(control.state())
    assert restored.interval_s==0
    for invalid in ({'min_interval_s':0},{'min_interval_s':-1},
                    {'min_interval_s':0,'initial_interval_s':0,'max_interval_s':1}):
        with pytest.raises(ValueError):adaptive_policy(invalid)


def test_healthy_ramp_congestion_shrink_and_recovery():
    policy=dict(initial_concurrency=2,max_concurrency=3,min_samples=2,window_s=10,recovery_s=20)
    control=SearchAdmission(policy,now=100)
    assert control.observe(success=True,latency_s=1,now=105) is None
    event=control.observe(success=True,latency_s=2,now=110)
    assert event['reason']=='healthy_window' and control.concurrency==3
    interval=control.interval_s
    event=control.observe(success=False,congestion=True,latency_s=1,now=111)
    assert event['reason']=='source_congestion' and control.concurrency==1
    assert control.interval_s>interval
    control.observe(success=True,latency_s=1,now=120)
    assert control.observe(success=True,latency_s=1,now=121) is None
    assert control.concurrency==1
    control.observe(success=True,latency_s=1,now=130)
    control.observe(success=True,latency_s=1,now=132)
    assert control.concurrency==2
    restored=SearchAdmission(policy,now=200);restored.restore(control.state())
    assert restored.state()==control.state()


def test_network_failure_ratio_and_slow_success_shrink():
    control=SearchAdmission(dict(initial_concurrency=4,min_samples=4,window_s=10),now=0)
    control.observe(success=True,latency_s=1,now=1)
    assert control.observe(success=False,latency_s=1,now=2) is None
    control.observe(success=True,latency_s=1,now=3)
    assert control.observe(success=True,latency_s=1,now=4)['reason']=='network_failure_ratio'
    assert control.concurrency==2
    for t in [5,6,7]:control.observe(success=True,latency_s=15,now=t)
    assert control.observe(success=True,latency_s=15,now=15)['reason']=='http_latency'
    assert control.concurrency==1


def test_resizing_drains_inflight_and_cancelled_waiter_releases_nothing():
    async def run():
        gate=ResizableAdmission(2)
        entered=[];release=asyncio.Event()
        async def work(index):
            async with gate:
                entered.append(index)
                await release.wait()
        a,b=[asyncio.create_task(work(i)) for i in [1,2]]
        await asyncio.sleep(.01)
        await gate.resize(1)
        c=asyncio.create_task(work(3));await asyncio.sleep(.01)
        c.cancel()
        with pytest.raises(asyncio.CancelledError):await c
        assert entered==[1,2] and gate.active==2
        release.set();await asyncio.gather(a,b)
        assert gate.active==0
        async with gate:assert gate.active==1
        assert gate.active==0
    asyncio.run(run())


@pytest.mark.parametrize('value',[{'max_concurrency':0},{'initial_interval_s':float('nan')},
    {'min_concurrency':True},{'unknown':3},{'min_interval_s':4},{'min_samples':1},
    {'failure_window_scope':[]},{'failure_window_scope':'route'},
    {'adjustment_scope':'route'},{'adjustment_scope':'query'},
    {'decrease_min_samples':0},{'decrease_min_samples':2.5},
    {'route_cooldown_wait_s':-1},{'route_cooldown_wait_s':301},
    {'route_cooldown_wait_s':True},{'route_cooldown_wait_s':float('inf')}])
def test_invalid_policy_is_rejected_before_io(value):
    with pytest.raises(ValueError):adaptive_policy(value)


def test_live_parallel_http_cached_replay_and_persisted_shrink(tmp_path,monkeypatch):
    calls=[];active=0;peak=0;bad=False
    async def call(self,context,message):
        nonlocal active,peak
        if message['op']=='merge':return {'results':[r for g in message['groups'] for r in g['results']]}
        async with self.http_gate.enter():
            active+=1;peak=max(peak,active)
            try:
                await asyncio.sleep(.06)
                calls.append(message['query'])
                status='captcha' if bad else 'ok'
                return {'status':status,'reason':'','results':[] if bad else [
                    {'url':'https://example.org/'+message['query'],'title':'t','content':'c'}],
                    'http':[{'host':'search.example','http_status':302 if bad else 200,'elapsed_s':.06}]}
            finally:active-=1
    monkeypatch.setattr(NativeSearchSession,'call',call)
    policy=dict(initial_concurrency=2,max_concurrency=4,initial_interval_s=.005,min_interval_s=.001,
                max_interval_s=1,window_s=60,min_samples=20)
    config=SearchConfig(engines=['google'],language='all',request_concurrency=1,source_interval_s=0)
    routes=[{'name':str(i),'proxy':'http://r'+str(i)+'.example:1','interval_s':0} for i in range(4)]
    def pool():return SearchRoutePool(cache_path=tmp_path/'cache.sqlite',config=config,routes=routes,
        adaptive=policy,max_route_attempts=1,failure_limit=1)
    async def run():
        nonlocal bad
        initial=SearchRoutePool(cache_path=tmp_path/'cache.sqlite',config=config,routes=routes,max_route_attempts=1)
        try:chosen=await initial.search('cached')
        finally:await initial.aclose()
        adaptive=pool()
        try:
            before=len(calls)
            assert (await adaptive.search('cached'))==chosen and len(calls)==before
            assert adaptive.admission.total_samples==0
            results=await asyncio.gather(*(adaptive.search(str(i)) for i in range(4)))
            assert all(r['status']=='ok' for r in results) and peak==2
            assert adaptive.admission.total_samples==4
            bad=True
            assert (await adaptive.search('bad'))['status']=='search_failed'
            assert adaptive.admission.concurrency==1
            saved=adaptive.admission.state()
        finally:await adaptive.aclose()
        restored=pool()
        try:
            assert (await restored.search('cached'))==chosen
            assert restored.admission.state()==saved
            assert restored.execution_gate.limit==1
            with restored.routes[0]['session']._db() as db:
                assert db.execute('select count(*) from native_search_admission_events').fetchone()[0]==1
        finally:await restored.aclose()
    asyncio.run(run())


def test_query_circuit_keeps_attempt_failures_for_capacity_without_stopping_recovered_queries():
    control=SearchAdmission(dict(initial_concurrency=8,max_concurrency=12,
        decrease_on_single_congestion=False,decrease_min_samples=24,min_samples=24,
        decrease_failure_ratio=.3,failure_window_scope='query',
        failure_window_limit=3,failure_window_ratio=.4),now=100)
    for i in range(8):
        control.observe(success=i>=3,congestion=i<3,latency_s=1,now=101+i)
    assert control.concurrency==8 and control.failure_window==[]
    for i in range(8,24):
        control.observe(success=i%3!=0,congestion=i%3==0,latency_s=1,now=101+i)
        control.observe_query(success=True,now=101+i)
    assert control.concurrency==4 and control.total_samples==24
    assert control.stopped_until==0
    assert len(control.failure_window)==16 and not any(v[1] for v in control.failure_window)
    for t in [125,127]:
        control.observe_query(success=False,now=t)
        control.observe_query(success=True,now=t+1)
    # Three failed queries are too few when most completed queries recovered.
    assert control.observe_query(success=False,now=129) is None
    for t in range(130,140):
        event=control.observe_query(success=False,now=t)
        if event:break
    assert event['reason']=='pool_failure_window' and event['failure_window_scope']=='query'
    assert control.stopped_until>t
    restored=SearchAdmission(control.policy);restored.restore(control.state())
    assert restored.state()==control.state()


def test_query_circuit_counts_bounded_fallback_once_and_replays_while_stopped(tmp_path,monkeypatch):
    from collections import Counter
    from demiflow.execution.request_limits import ServiceStopped
    calls=Counter()
    async def call(self,context,message):
        if message['op']=='merge':return {'results':[r for g in message['groups'] for r in g['results']]}
        query=message['query'];calls[query]+=1
        failed=query.startswith('bad') or (query.startswith('recover') and calls[query]==1)
        async with self.http_gate.enter():
            return {'status':'captcha' if failed else 'ok','reason':'fixture',
                'results':[] if failed else [{'url':'https://example.org/x','title':'x'}],
                'http':[{'host':'search.example','http_status':302 if failed else 200,'elapsed_s':.01}]}
    monkeypatch.setattr(NativeSearchSession,'call',call)
    def pool():return SearchRoutePool(cache_path=tmp_path/'cache.sqlite',
        config=SearchConfig(engines=['google'],language='all',source_interval_s=0),
        routes=[{'name':str(i),'proxy':'http://r'+str(i)+'.example:1','interval_s':0} for i in range(12)],
        max_route_attempts=2,adaptive=dict(failure_window_scope='query',failure_window_limit=2,
        failure_window_ratio=.4,decrease_on_single_congestion=False,
        initial_interval_s=.001,min_interval_s=.001,max_interval_s=.01))
    async def run():
        session=pool()
        try:
            recovered=await session.search('recover1')
            assert recovered['status']=='ok' and calls['recover1']==2
            assert session.admission.total_samples==2
            assert len(session.admission.failure_window)==1 and session.admission.failure_window[0][1] is False
            failed=await session.search('bad1')
            assert failed['status']=='search_failed' and calls['bad1']==2
            assert session.admission.stopped_until==0
            await session.search('good1')
            with pytest.raises(ServiceStopped,match='search_pool_failure_window'):
                await session.search('bad2')
            assert calls['bad2']==2 and session.admission.total_samples==7
            assert len(session.admission.failure_window)==4
            stopped=session.admission.state()
            before=calls.copy()
            assert await session.search('recover1')==recovered
            assert await session.search('bad1')==failed
            assert (await session.search('bad2'))['status']=='search_failed'
            assert session.admission.state()==stopped and calls==before
        finally:await session.aclose()
        restored=pool()
        try:
            assert (await restored.search('bad2'))['status']=='search_failed'
            assert restored.admission.state()==stopped
            with pytest.raises(ServiceStopped,match='search_pool_failure_window'):
                await restored.search('new')
            assert calls==before and restored.route_attempts==0
        finally:await restored.aclose()
    asyncio.run(run())


def test_web_session_policy_is_lazy_and_does_not_override_adaptive_pacing(tmp_path,monkeypatch):
    async def search(self,query,**parameters):return self.http_gate.interval_s
    monkeypatch.setattr(SearchRoutePool,'search',search)
    async def run():
        session=WebSession(search={'engines':['google']},search_routes=[{'name':'r','proxy':None}],
            search_route_attempts=1,search_adaptive={'initial_interval_s':3},
            search_request_interval_s=9,cache_path=tmp_path/'absent/cache',object_directory=tmp_path/'objects')
        assert session.native is None and not (tmp_path/'absent').exists()
        try:assert await session.search('x')==3
        finally:await session.aclose()
    asyncio.run(run())


def test_preflight_quarantine_skips_bad_route_without_fabricating_a_request(tmp_path):
    async def run():
        pool=SearchRoutePool(cache_path=tmp_path/'health.sqlite',config=SearchConfig(engines=['google']),
            routes=[{'name':'bad','proxy':None,'initial_cooldown_until':time.time()+60},
                    {'name':'good','proxy':'http://good.example:1'}],max_route_attempts=1)
        try:
            await pool.initialize()
            i,route=await pool.choose(set())
            assert route['name']=='good'
            with route['session']._db() as db:
                assert db.execute('select count(*) from native_search_route_events').fetchone()[0]==0
                assert db.execute('select status from native_search_route_health').fetchone()[0]=='preflight_cooldown'
        finally:await pool.aclose()
    asyncio.run(run())


def test_nonconsecutive_pool_failures_stop_and_recover_after_bound():
    control=SearchAdmission(dict(failure_window_limit=3,failure_window_s=20,
        failure_window_ratio=.4,stop_s=40),now=100)
    for t in [101,102]:
        control.observe(success=False,congestion=True,now=t)
        control.observe(success=True,latency_s=1,now=t+.1)
    event=control.observe(success=False,congestion=True,now=103)
    assert event['reason']=='pool_failure_window' and control.stopped_until==143
    restored=SearchAdmission(control.policy);restored.restore(control.state())
    assert restored.stopped_until==143
    restored.observe(success=True,latency_s=1,now=144)
    assert restored.failure_window==[[144,False]]
    assert restored.stopped_until<144


def test_stale_successes_cannot_increase_after_long_pause():
    control=SearchAdmission(dict(min_samples=3,window_s=10,recovery_s=20),now=0)
    control.observe(success=True,latency_s=1,now=1)
    control.observe(success=True,latency_s=1,now=2)
    restored=SearchAdmission(control.policy);restored.restore(control.state())
    assert restored.observe(success=True,latency_s=1,now=100) is None
    assert restored.concurrency==2 and len(restored.samples)==1


def test_exit_fault_can_be_isolated_without_global_shrink_but_pool_still_stops():
    control=SearchAdmission(dict(initial_concurrency=8,max_concurrency=12,
        decrease_on_single_congestion=False,failure_window_limit=3,
        failure_window_ratio=.5),now=100)
    assert control.observe(success=False,congestion=True,latency_s=1,now=101) is None
    assert control.concurrency==8
    control.observe(success=True,latency_s=1,now=102)
    control.observe(success=False,congestion=True,latency_s=1,now=103)
    event=control.observe(success=False,congestion=True,latency_s=1,now=104)
    assert event['reason']=='pool_failure_window' and control.stopped_until>104


def test_pool_window_persists_receipts_before_stopping(tmp_path,monkeypatch):
    from demiflow.execution.request_limits import ServiceStopped
    async def call(self,context,message):
        if message['op']=='merge':return {'results':[r for g in message['groups'] for r in g['results']]}
        failed=message['query'].startswith('bad')
        async with self.http_gate.enter():
            return {'status':'captcha' if failed else 'ok','reason':'fixture',
                'results':[] if failed else [{'url':'https://example.org/x','title':'x'}],
                'http':[{'host':'search.example','http_status':302 if failed else 200,'elapsed_s':.01}]}
    monkeypatch.setattr(NativeSearchSession,'call',call)
    def pool():return SearchRoutePool(cache_path=tmp_path/'cache.sqlite',
        config=SearchConfig(engines=['google'],language='all',source_interval_s=0),
        routes=[{'name':str(i),'proxy':'http://r'+str(i)+'.example:1','interval_s':0} for i in range(5)],
        max_route_attempts=1,adaptive=dict(failure_window_limit=2,failure_window_ratio=.4,
        initial_interval_s=.001,min_interval_s=.001,max_interval_s=.01))
    async def run():
        session=pool()
        try:
            assert (await session.search('bad1'))['status']=='search_failed'
            saved=await session.search('good1')
            with pytest.raises(ServiceStopped,match='search_pool_failure_window'):
                await session.search('bad2')
            with session.routes[0]['session']._db() as db:
                assert db.execute("select count(*) from native_search_route_events where status='captcha'").fetchone()[0]==2
                assert db.execute("select count(*) from native_search where json_extract(value,'$.status')='captcha'").fetchone()[0]==2
        finally:await session.aclose()
        restored=pool()
        try:
            assert await restored.search('good1')==saved
            with pytest.raises(ServiceStopped,match='search_pool_failure_window'):
                await restored.search('new')
            assert restored.route_attempts==0
        finally:await restored.aclose()
    asyncio.run(run())
