"""Recover a live stream without retrying completed queries or losing budgets."""
import asyncio
import json
import time

import pytest

from demiflow.collect.native_search import NativeSearchSession, SearchConfig
from demiflow.collect.search_admission import adaptive_policy, admission_identity_policy
from demiflow.collect.search_routes import SearchRoutePool
from demiflow.execution.request_limits import ServiceStopped


def policy(**options):
    return dict(adjustment_scope='query',failure_window_scope='query',
        transient_failure_action='pause',initial_interval_s=0,min_interval_s=0,max_interval_s=0,
        min_concurrency=2,initial_concurrency=2,max_concurrency=2,
        pause_s=.08,pause_max_s=.2,pause_max_probes=2,pause_max_episodes=3,
        **options)


def pool(tmp_path,*,adaptive=None,failures=1):
    return SearchRoutePool(cache_path=tmp_path/'recovery.sqlite',
        config=SearchConfig(engines=['google'],language='all',source_interval_s=0,
                            suspend_s=.001,query_failure_limit=failures),
        routes=[{'name':n,'proxy':'http://'+n+'.example:3128','interval_s':0} for n in ['a','b']],
        cooldown_s=.001,max_route_attempts=1,failure_limit=1,adaptive=adaptive or policy())


def mock_source(monkeypatch,respond):
    calls=[]
    async def call(self,context,message):
        if message['op']=='merge':return {'results':[r for g in message['groups'] for r in g['results']]}
        async with self.http_gate.enter():
            calls.append((message['query'],time.monotonic()))
            status=await respond(message['query'])
            return {'status':status,'reason':'fixture',
                'results':[{'url':'https://source.example/'+message['query'],'title':'result','content':'body'}] if status=='ok' else [],
                'http':[{'host':'search.example','http_status':200 if status=='ok' else None,'elapsed_s':.001}]}
    monkeypatch.setattr(NativeSearchSession,'call',call)
    return calls


async def until(predicate):
    async with asyncio.timeout(2):
        while not predicate():await asyncio.sleep(.002)


def test_live_pause_cache_bypass_single_probe_and_resume(monkeypatch,tmp_path):
    async def run():
        probe=asyncio.Event();release=asyncio.Event()
        async def respond(query):
            if query=='bad':return 'network_error'
            if not probe.is_set():probe.set();await release.wait()
            return 'ok'
        calls=mock_source(monkeypatch,respond);s=pool(tmp_path)
        try:
            failed=await s.search('bad')
            assert failed['status']=='search_failed' and s.recovery.value['mode']=='paused'
            tasks=[asyncio.create_task(s.search(q)) for q in ['probe','cancelled','next']]
            assert await asyncio.wait_for(s.search('bad'),.05)==failed
            await asyncio.sleep(.02)
            assert len(calls)==1
            tasks[1].cancel()
            with pytest.raises(asyncio.CancelledError):await tasks[1]
            await asyncio.wait_for(probe.wait(),1)
            await asyncio.sleep(.02)
            assert len(calls)==2 and s.recovery.active==1
            release.set()
            assert all(v['status']=='ok' for v in await asyncio.gather(tasks[0],tasks[2]))
            assert {q for q,_ in calls}=={'bad','probe','next'}
            assert s.recovery.value['recoveries']==1 and s.admission.stopped_until==0
            assert s.admission.total_samples==3 and s.recovery.active==0
        finally:release.set();await s.aclose()
    asyncio.run(run())


def test_failure_window_restores_and_failed_probes_stop_with_saved_receipts(monkeypatch,tmp_path):
    async def respond(query):return 'network_error'
    calls=mock_source(monkeypatch,respond)
    async def run():
        p=policy(failure_window_limit=2)
        s=pool(tmp_path,adaptive=p,failures=50)
        for q in ['bad1','bad2']:assert (await s.search(q))['status']=='search_failed'
        saved=s.recovery.state();identity=s.admission_key
        assert saved['mode']=='paused' and saved['last_reason']=='search_pool_failure_window'
        await s.aclose()
        resumed=pool(tmp_path,adaptive=p,failures=50)
        try:
            await resumed.initialize()
            assert resumed.admission_key==identity and resumed.recovery.state()==saved
            before=len(calls)
            assert (await resumed.search('bad1'))['status']=='search_failed' and len(calls)==before
            assert (await resumed.search('probe1'))['status']=='search_failed'
            with pytest.raises(ServiceStopped,match='search_recovery_probe_limit'):
                await resumed.search('probe2')
            assert (await resumed.search('probe2'))['status']=='search_failed'
            with pytest.raises(ServiceStopped,match='search_recovery_probe_limit'):
                await resumed.search('never_sent')
            assert [q for q,_ in calls]==['bad1','bad2','probe1','probe2']
            with resumed.routes[0]['session']._db() as db:
                events=[json.loads(r[0]) for r in db.execute('SELECT event_json FROM native_search_recovery_events')]
            assert events[-1]['reason']=='search_recovery_probe_limit'
        finally:await resumed.aclose()
    asyncio.run(run())


def test_inflight_success_drains_without_reopening_and_probe_waits_for_drain(monkeypatch,tmp_path):
    async def run():
        started=asyncio.Event();release=asyncio.Event()
        async def respond(q):
            if q=='inflight':started.set();await release.wait()
            return 'network_error' if q=='bad' else 'ok'
        calls=mock_source(monkeypatch,respond);s=pool(tmp_path)
        try:
            old=asyncio.create_task(s.search('inflight'));await started.wait()
            await s.search('bad')
            pending=asyncio.create_task(s.search('probe'))
            await asyncio.sleep(.12)
            assert [q for q,_ in calls]==['inflight','bad']
            release.set();await old
            assert (await pending)['status']=='ok'
            assert s.recovery.value['recoveries']==1 and s.recovery.value['pauses']==1
        finally:release.set();await s.aclose()
    asyncio.run(run())


def test_repeated_outages_have_durable_episode_limit(monkeypatch,tmp_path):
    async def respond(q):return 'network_error' if q.startswith('bad') else 'ok'
    mock_source(monkeypatch,respond)
    async def run():
        s=pool(tmp_path)
        try:
            for i in range(3):
                await s.search('bad'+str(i));await s.search('recover'+str(i))
            with pytest.raises(ServiceStopped,match='search_recovery_episode_limit'):
                await s.search('bad3')
            assert s.recovery.value['pauses']==3 and s.recovery.value['recoveries']==3
        finally:await s.aclose()
    asyncio.run(run())


@pytest.mark.parametrize('status',['authentication_error','configuration_error'])
def test_credentials_configuration_still_stop_immediately(monkeypatch,tmp_path,status):
    async def respond(q):return status
    calls=mock_source(monkeypatch,respond)
    async def run():
        s=pool(tmp_path)
        try:
            with pytest.raises(ServiceStopped,match='search_authentication_or_configuration'):
                await s.search('auth')
            assert len(calls)==1 and s.recovery.value['pauses']==0
        finally:await s.aclose()
    asyncio.run(run())


def test_no_available_routes_waits_without_http_then_resumes(monkeypatch,tmp_path):
    async def respond(q):return 'ok'
    calls=mock_source(monkeypatch,respond)
    async def run():
        s=pool(tmp_path)
        try:
            await s.initialize()
            for r in s.routes:r['until']=time.time()+.04
            task=asyncio.create_task(s.search('after_cooldown'))
            await until(lambda:s.recovery.value['mode']=='paused')
            assert not calls
            assert (await task)['status']=='ok' and len(calls)==1
            assert s.recovery.value['recoveries']==1
        finally:await s.aclose()
    asyncio.run(run())


def test_renewable_manager_survives_pause_and_cancelled_probe(monkeypatch,tmp_path):
    from test_search_session_pool import pool as renewable_pool
    async def run():
        started=asyncio.Event();release=asyncio.Event()
        async def respond(q):
            if q=='cancel_probe':started.set();await release.wait()
            return 'network_error' if q.startswith('bad') else 'ok'
        mock_source(monkeypatch,respond)
        s=renewable_pool(tmp_path,adaptive=policy(failure_window_limit=2),background_maintenance=True)
        try:
            await s.search('bad1');await s.search('bad2')
            assert s.recovery.value['mode']=='paused'
            cancelled=asyncio.create_task(s.search('cancel_probe'))
            await asyncio.wait_for(started.wait(),1);cancelled.cancel()
            with pytest.raises(asyncio.CancelledError):await cancelled
            await until(lambda:s.recovery.active==0)
            assert not s.manager_error and not s.manager_task.done()
            assert s.recovery.value['failed_probes']==0
            assert (await s.search('recover'))['status']=='ok'
            assert not s.manager_error
        finally:release.set();await s.aclose()
    asyncio.run(run())


def test_recovery_does_not_change_admission_identity_or_accept_unbounded_policy():
    p=policy();disabled={**p,'transient_failure_action':'stop'}
    assert admission_identity_policy(adaptive_policy(p))==admission_identity_policy(adaptive_policy(disabled))
    for invalid in [{'transient_failure_action':'retry'}, {'pause_max_probes':0},
                    {'pause_max_episodes':0}, {'pause_s':0}, {'pause_max_s':4000},
                    {'transient_failure_action':'pause'}, {'pause_route_inventory':'cold'},
                    {'pause_route_inventory':None}]:
        with pytest.raises(ValueError):adaptive_policy(invalid)


def test_healthy_inventory_does_not_count_refilled_cold_leases(monkeypatch, tmp_path):
    async def respond(q):return 'network_error'
    mock_source(monkeypatch, respond)
    async def run():
        p=policy(pause_trigger='route_shortage', pause_route_inventory='healthy',
                 pause_min_routes=8, pause_shortfall_s=.02)
        s=pool(tmp_path, adaptive=p, failures=3)
        s.recovery.inventory=lambda:dict(viable=128, healthy=0, untried=128, leased=0, paced=0)
        try:
            await s.search('bad1')
            await asyncio.sleep(.03)
            await s.search('bad2')
            assert s.recovery.value['mode']=='running'
            await s.search('bad3')
            assert s.recovery.value['mode']=='paused'
            assert s.recovery.value['last_reason']=='search_route_shortage'
            assert s.recovery.value['last_inventory']['viable']==128
        finally:await s.aclose()
    asyncio.run(run())


def test_healthy_inventory_keeps_busy_paced_capacity_and_static_compatibility(monkeypatch, tmp_path):
    async def respond(q):return 'network_error'
    mock_source(monkeypatch, respond)
    async def run():
        p=policy(pause_trigger='route_shortage', pause_route_inventory='healthy',
                 pause_min_routes=8, pause_shortfall_s=.001)
        s=pool(tmp_path, adaptive=p)
        inventory=dict(viable=128, healthy=8, untried=120, leased=8, paced=8)
        s.recovery.inventory=lambda:inventory.copy()
        try:
            for index in range(6):
                await s.search('bad'+str(index))
                await asyncio.sleep(.002)
            assert s.recovery.value['mode']=='running' and s.recovery.value['short_since'] is None
            inventory.clear();inventory.update(viable=8, leased=8, paced=8)
            await s.search('static_bad')
            assert s.recovery.value['mode']=='running' and s.recovery.value['short_since'] is None
        finally:await s.aclose()
    asyncio.run(run())


def test_healthy_inventory_policy_preserves_admission_identity():
    base=adaptive_policy(policy(pause_trigger='route_shortage'))
    healthy=adaptive_policy({**base, 'pause_route_inventory':'healthy'})
    assert admission_identity_policy(base)==admission_identity_policy(healthy)


def test_query_failures_do_not_pause_while_routes_remain_available(monkeypatch,tmp_path):
    async def respond(q):return 'network_error' if q.startswith('bad') else 'ok'
    calls=mock_source(monkeypatch,respond)
    async def run():
        p=policy(pause_trigger='route_shortage',pause_min_routes=1,
                 pause_shortfall_s=.001,failure_window_limit=2)
        s=pool(tmp_path,adaptive=p)
        try:
            for i in range(8):
                assert (await s.search('bad'+str(i)))['status']=='search_failed'
                await asyncio.sleep(.005)
                assert s.recovery.value['mode']=='running'
            assert s.recovery.value['pauses']==0 and s.recovery.value['consecutive']==8
            assert s.admission.stopped_until>time.time()
            assert (await s.search('good'))['status']=='ok' and len(calls)==9
        finally:await s.aclose()
    asyncio.run(run())


def test_depleted_inventory_pauses_only_after_sustained_shortfall(monkeypatch,tmp_path):
    async def respond(q):return 'network_error' if q.startswith('bad') else 'ok'
    calls=mock_source(monkeypatch,respond)
    async def run():
        s=pool(tmp_path,adaptive=policy(pause_trigger='route_shortage',pause_min_routes=2,pause_shortfall_s=.01))
        s.cooldown_s=.05
        try:
            await s.search('bad1')
            assert s.recovery.value['mode']=='running' and s.recovery.value['short_since']
            await asyncio.sleep(.015)
            await s.search('bad2')
            assert s.recovery.value['mode']=='paused'
            assert s.recovery.value['last_reason']=='search_route_shortage'
            assert (await s.search('recovered'))['status']=='ok' and len(calls)==3
        finally:await s.aclose()
    asyncio.run(run())


def test_renewable_inventory_distinguishes_busy_paced_expired_and_untried(monkeypatch,tmp_path):
    from test_search_session_pool import pool as renewable_pool
    async def run():
        s=renewable_pool(tmp_path,adaptive=policy(pause_trigger='route_shortage'))
        try:
            await s.initialize();await s.maintain()
            first,second=s.routes[1:]
            first['busy']=True;first['entry']['healthy']=True
            second['next_ready']=time.monotonic()+60
            assert s.recovery_inventory()==dict(viable=2,healthy=1,untried=1,leased=1,paced=1)
            first['busy']=False;first['entry']['retired']='network_error'
            assert s.recovery_inventory()['viable']==1
            second['entry']['expires_at']=time.time()-1
            assert s.recovery_inventory()['viable']==0
        finally:await s.aclose()
    asyncio.run(run())
