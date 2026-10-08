import asyncio
import json
import time

import pytest

from demiflow.collect.native_search import NativeSearchSession, SearchConfig
from demiflow.collect.search_routes import SearchRoutePool
from demiflow.collect.search_fallback import FallbackSearchSession, fallback_policy
from demiflow.execution.request_limits import ServiceStopped


def primary(path):
    return SearchRoutePool(cache_path=path,
        config=SearchConfig(engines=['google'], language='all', source_interval_s=0,
                           suspend_s=0, failure_limit=100, query_failure_limit=100),
        routes=[{'name':'first','proxy':None,'interval_s':0}],
        cooldown_s=.001, max_route_attempts=1, failure_limit=100)


def policy(**overrides):
    return {'search':{'engines':['yandex'],'language':'all','source_interval_s':0,
                      'suspend_s':0,'query_failure_limit':100}, **overrides}


def renewable_factory(*,token,ttl_s,options):
    return 'http://session'+token+'.example:3128'


def test_fallback_renewable_pool_replay_and_cleanup(monkeypatch,tmp_path):
    calls=fixture(monkeypatch)
    from demiflow.collect.search_sessions import RenewableSearchRoutePool
    spec={'factory':'test_search_fallback:renewable_factory','factory_options':{},
          'size':2,'max_size':2,'ttl_s':300,'interval_s':.001,'min_healthy':1,
          'background_maintenance':True,'refill_interval_s':.01}
    async def run():
        path=tmp_path/'renewable.sqlite'
        session=FallbackSearchSession(primary(path),cache_path=path,
            policy=policy(session_pool=spec,route_attempts=2))
        try:
            assert isinstance(session.secondary,RenewableSearchRoutePool)
            result=await session.search('requires-fallback')
            assert result['status']=='ok'
            before=list(calls)
            assert await session.search('requires-fallback')==result
            assert calls==before
            assert session.secondary.state['created_total']==2
        finally:await session.aclose()
        assert session.secondary.closed and session.secondary.manager_task.done()
        # Reopening preserves the selected result; it does not buy another query.
        again=FallbackSearchSession(primary(path),cache_path=path,policy=policy(session_pool=spec,route_attempts=2))
        try:
            assert await again.search('requires-fallback')==result
            assert calls==before
        finally:await again.aclose()
    asyncio.run(run())


def fixture(monkeypatch, *, block=None):
    calls=[]
    async def call(self, context, message):
        if message['op']=='merge':
            return {'results':[r for g in message['groups'] for r in g['results']]}
        engine=message['engine'];query=message['query']
        calls.append((engine, query, message['parameters']['language']))
        if block and engine=='yandex':
            await block.wait()
        ok=engine=='yandex' or query=='good'
        return {'status':'ok' if ok else 'network_error','reason':'fixture',
            'results':[{'url':'https://reference.example/'+query,'title':query,'content':'body'}] if ok else [],
            'http':[{'host':'search.example','http_status':200 if ok else None,'elapsed_s':.001}]}
    monkeypatch.setattr(NativeSearchSession,'call',call)
    return calls


def test_existing_good_and_failed_selections_unchanged_new_failure_uses_fallback(monkeypatch,tmp_path):
    calls=fixture(monkeypatch)
    async def run():
        path=tmp_path/'search.sqlite';old=primary(path)
        good=await old.search('good');failed=await old.search('old-failure')
        await old.aclose()
        session=FallbackSearchSession(primary(path),cache_path=path,policy=policy())
        try:
            assert await session.search('good')==good
            assert await session.search('old-failure')==failed
            assert len(calls)==2
            new=await session.search('new', fallback_parameters={'language':'zh-CN'})
            assert new['status']=='ok' and json.loads(new['parameters_json'])['language']=='zh-CN'
            assert calls[-2:]==[('google','new','all'),('yandex','new','zh-CN')]
            assert await session.search('new',fallback_parameters={'language':'zh-CN'})==new
            assert len(calls)==4
        finally:
            await session.aclose()
        resumed=FallbackSearchSession(primary(path),cache_path=path,policy=policy())
        try:
            assert await resumed.search('new',fallback_parameters={'language':'zh-CN'})==new
            assert len(calls)==4  # primary failure and fallback choice both survive
            with resumed.secondary._db() as db:
                value=json.loads(db.execute('SELECT value FROM native_search_fallback_results').fetchone()[0])
            assert value['primary']['status']=='search_failed'
            assert value['result']==new
        finally:
            await resumed.aclose()
    asyncio.run(run())


def test_transient_stop_can_fallback_but_authentication_stop_propagates(monkeypatch,tmp_path):
    calls=fixture(monkeypatch)
    async def run():
        path=tmp_path/'search.sqlite';p=primary(path)
        async def stopped(*args,**kwargs):raise ServiceStopped('search_recovery_probe_limit')
        p.search=stopped
        session=FallbackSearchSession(p,cache_path=path,policy=policy())
        try:
            assert (await session.search('new'))['status']=='ok'
            assert calls==[('yandex','new','all')]
            async def auth(*args,**kwargs):raise ServiceStopped('search_authentication_or_configuration')
            p.search=auth
            with pytest.raises(ServiceStopped,match='authentication'):
                await session.search('never-fallback')
            assert len(calls)==1
        finally:await session.aclose()
    asyncio.run(run())


def test_shared_query_cancellation_and_distinct_pending_bound(monkeypatch,tmp_path):
    async def run():
        release=asyncio.Event();calls=fixture(monkeypatch,block=release)
        path=tmp_path/'search.sqlite'
        session=FallbackSearchSession(primary(path),cache_path=path,policy=policy(max_pending=1))
        a=asyncio.create_task(session.search('same'));b=asyncio.create_task(session.search('same'))
        try:
            async with asyncio.timeout(2):
                while len(calls)<2:await asyncio.sleep(.001)
            with pytest.raises(ServiceStopped,match='pending_limit'):
                await session.search('other')
            a.cancel()
            with pytest.raises(asyncio.CancelledError):await a
            release.set();assert (await b)['status']=='ok'
            assert len(calls)==2 and not session.inflight
        finally:
            release.set();await session.aclose()
    asyncio.run(run())


def test_stored_receipt_byte_bound_and_lazy_declaration(monkeypatch,tmp_path):
    calls=fixture(monkeypatch)
    assert fallback_policy(policy())['max_pending']==128
    for bad in [policy(max_pending=0), policy(max_result_bytes=2), {'unknown':1}]:
        with pytest.raises((ValueError,TypeError)):fallback_policy(bad)
    async def run():
        path=tmp_path/'search.sqlite'
        session=FallbackSearchSession(primary(path),cache_path=path,policy=policy(max_result_bytes=1024))
        assert not path.exists()
        try:
            # A deliberately small budget must reject a real normalized receipt.
            with pytest.raises(ValueError,match='byte budget'):
                await session.search('oversize')
            with session.secondary._db() as db:
                assert db.execute('SELECT count(*) FROM native_search_fallback_results').fetchone()[0]==0
        finally:await session.aclose()
    asyncio.run(run())


def test_secondary_route_pool_keeps_same_fallback_selection(monkeypatch,tmp_path):
    calls=fixture(monkeypatch)
    async def run():
        path=tmp_path/'search.sqlite'
        settings=policy(routes=[{'name':'a','proxy':None,'interval_s':0},
                                {'name':'b','proxy':None,'interval_s':0}],route_attempts=2)
        s=FallbackSearchSession(primary(path),cache_path=path,policy=settings)
        try:
            first=await s.search('new',fallback_parameters={'language':'en-US'})
            assert first['status']=='ok'
        finally:await s.aclose()
        s=FallbackSearchSession(primary(path),cache_path=path,policy=settings)
        try:
            assert await s.search('new',fallback_parameters={'language':'en-US'})==first
            assert len(calls)==2
            with pytest.raises(ValueError,match='32768'):await s.search('x'*32769)
            with pytest.raises(ValueError,match='supports'):await s.search('q',engine_data={})
        finally:await s.aclose()
    asyncio.run(run())


def test_fallback_adaptive_wait_cancellation_and_selection_reuse(monkeypatch,tmp_path):
    from test_search_recovery import policy as recovery_policy
    calls=fixture(monkeypatch)
    async def run():
        path=tmp_path/'search.sqlite'
        settings=policy(routes=[{'name':'a','proxy':None,'interval_s':0}],
                        adaptive=recovery_policy(route_cooldown_wait_s=.5))
        p=primary(path)
        async def stopped(*args,**kwargs):raise ServiceStopped('search_recovery_probe_limit')
        p.search=stopped
        s=FallbackSearchSession(p,cache_path=path,policy=settings)
        try:
            await s.initialize()
            route=s.secondary.routes[0];route['until']=time.time()+.06
            task=asyncio.create_task(s.search('cancelled'))
            await asyncio.sleep(.015)
            assert not calls and not task.done()
            task.cancel()
            with pytest.raises(asyncio.CancelledError):await task
            assert not s.inflight and not route['busy']
            first=await asyncio.wait_for(s.search('next'),1)
            assert first['status']=='ok' and calls==[('yandex','next','all')]
            assert s.secondary.cooldown_waits>=1
        finally:await s.aclose()
        # Scheduling declarations neither invalidate the selected result nor
        # spend source calls on restart.
        settings.pop('adaptive')
        s=FallbackSearchSession(primary(path),cache_path=path,policy=settings)
        try:
            assert await s.search('next')==first
            assert calls==[('yandex','next','all')]
        finally:await s.aclose()
    asyncio.run(run())


def test_fallback_pauses_and_resumes_inside_same_session(monkeypatch,tmp_path):
    from test_search_recovery import policy as recovery_policy, mock_source
    async def respond(q):return 'network_error' if q=='bad' else 'ok'
    calls=mock_source(monkeypatch,respond)
    async def run():
        path=tmp_path/'search.sqlite';p=primary(path)
        async def stopped(*args,**kwargs):raise ServiceStopped('search_recovery_probe_limit')
        p.search=stopped
        settings=policy(routes=[{'name':'a','proxy':None,'interval_s':0}],
            adaptive=recovery_policy(route_cooldown_wait_s=.1),route_failure_limit=1,route_cooldown_s=.01)
        settings['search']['query_failure_limit']=1
        s=FallbackSearchSession(p,cache_path=path,policy=settings)
        try:
            failed=await s.search('bad')
            assert failed['status']=='search_failed'
            assert s.secondary.recovery.value['mode']=='paused'
            assert await s.search('bad')==failed and len(calls)==1
            assert (await asyncio.wait_for(s.search('healthy'),1))['status']=='ok'
            assert s.secondary.recovery.value['recoveries']==1
            assert [q for q,_ in calls]==['bad','healthy']
        finally:await s.aclose()
    asyncio.run(run())


@pytest.mark.parametrize('settings', [
    policy(adaptive={}),
    policy(routes=[{'name':'a','proxy':None}],adaptive={'max_concurrency':129}),
    policy(routes=[{'name':'a','proxy':None}],adaptive={'route_cooldown_wait_s':301}),
])
def test_fallback_adaptive_bounds(settings):
    with pytest.raises(ValueError):fallback_policy(settings)


def test_paused_primary_does_not_delay_fallback_and_due_probe_can_recover(monkeypatch,tmp_path):
    from test_search_recovery import pool
    calls=fixture(monkeypatch)
    async def run():
        p=pool(tmp_path);path=tmp_path/'recovery.sqlite'
        await p.search('first-bad')
        p.recovery.value['paused_until']=time.time()+60
        previous=p.recovery.state()
        s=FallbackSearchSession(p,cache_path=path,policy=policy())
        try:
            result=await asyncio.wait_for(s.search('use-backup',language='all'),1)
            assert result['status']=='ok' and p.recovery.state()==previous
            assert calls[-1][0]=='yandex' and len(calls)==2
            p.recovery.value['paused_until']=0
            assert (await s.search('good',language='all'))['status']=='ok'
            assert p.recovery.value['mode']=='running' and p.recovery.value['recoveries']==1
            assert calls[-1][0]=='google'
            assert await s.search('use-backup',language='all')==result
            assert len(calls)==3
        finally:await s.aclose()
    asyncio.run(run())


def test_exact_failed_primary_recovery_preserves_original_and_survives_declaration_removal(monkeypatch,tmp_path):
    calls=fixture(monkeypatch)
    async def run():
        path=tmp_path/'search.sqlite';p=primary(path)
        failed=await p.search('failed',language='all')
        unlisted=await p.search('unlisted',language='all')
        good=await p.search('good',language='all')
        identities=[p.selection_identity(q,{'language':'all'}) for q in ['failed','good']]
        await p.aclose()
        s=FallbackSearchSession(primary(path),cache_path=path,
            policy=policy(recover_primary_selections=identities))
        try:
            recovered=await s.search('failed',language='all',fallback_parameters={'language':'zh-CN'})
            assert recovered['status']=='ok' and calls[-1]==('yandex','failed','zh-CN')
            assert await s.primary.selected_result('failed',language='all')==failed
            assert await s.search('unlisted',language='all')==unlisted
            assert await s.search('good',language='all')==good
            assert len(calls)==4 and s.metrics['recovered_primary_selections']==1
            with s.db_session._db() as db:
                value=json.loads(db.execute('SELECT value FROM native_search_fallback_results').fetchone()[0])
            assert value['primary']['selection_identity']==identities[0]
            assert value['primary']['recovery']=='explicit_failed_primary_selection'
            assert value['primary']['profile']==failed['profile']
        finally:await s.aclose()
        # Recovery is an explicit durable choice, not a flag to pay again each
        # time the operator changes configuration or restarts the graph.
        s=FallbackSearchSession(primary(path),cache_path=path,policy=policy())
        try:
            assert await s.search('failed',language='all',fallback_parameters={'language':'zh-CN'})==recovered
            assert len(calls)==4
        finally:await s.aclose()
    asyncio.run(run())


def test_recovery_allowlist_does_not_override_authentication_stop(monkeypatch,tmp_path):
    from types import SimpleNamespace
    calls=fixture(monkeypatch)
    async def run():
        path=tmp_path/'search.sqlite';p=primary(path)
        failed=await p.search('failed')
        identity=p.selection_identity('failed',{})
        p.recovery=SimpleNamespace(value={'fatal':'search_authentication_or_configuration'})
        s=FallbackSearchSession(p,cache_path=path,policy=policy(recover_primary_selections=[identity]))
        try:
            with pytest.raises(ServiceStopped,match='authentication'):
                await s.search('failed')
            assert calls==[('google','failed','all')]
            assert await p.selected_result('failed')==failed
        finally:await s.aclose()
    asyncio.run(run())


def test_failed_recovery_is_not_automatically_retried(monkeypatch,tmp_path):
    calls=fixture(monkeypatch)
    async def run():
        path=tmp_path/'search.sqlite';p=primary(path)
        original=await p.search('failed')
        identity=p.selection_identity('failed',{})
        s=FallbackSearchSession(p,cache_path=path,policy=policy(recover_primary_selections=[identity]))
        async def unavailable(*args,**kwargs):return original
        s.secondary.search=unavailable
        try:
            assert await s.search('failed')==original
            async def should_not_run(*args,**kwargs):raise AssertionError('Repeated failed recovery')
            s.secondary.search=should_not_run
            assert await s.search('failed')==original
            assert s.metrics['fallback_queries']==1 and len(calls)==1
        finally:await s.aclose()
    asyncio.run(run())


@pytest.mark.parametrize('keys',['a'*64,[1],['bad'],['a'*64]*2,[f'{i:064x}' for i in range(4097)]])
def test_recovery_allowlist_has_declared_resource_and_identity_bounds(keys):
    with pytest.raises(ValueError,match='4096'):
        fallback_policy(policy(recover_primary_selections=keys))
