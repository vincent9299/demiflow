"""Source outages must neither stop siblings nor mark unsent work complete."""
import asyncio
import json
import time

import pytest
from demiflow.collect.native_search import NativeSearchSession, SearchConfig
from demiflow.collect.search_routes import SearchRoutePool
from demiflow.collect.operators import SearchWeb
from demiflow.execution.request_limits import ServiceStopped
from test_search_recovery import policy


def pool(tmp_path, scope='source', cooldown=60):
    return SearchRoutePool(cache_path=tmp_path/'cache.sqlite',
        config=SearchConfig(engines=['google','bing'],language='all',source_interval_s=0,
                            suspend_s=.001,query_failure_limit=1),
        routes=[{'name':n,'proxy':'http://'+n+'.example:3128','interval_s':0} for n in ['a','b']],
        adaptive=policy(route_cooldown_wait_s=0),failure_scope=scope,
        cooldown_s=cooldown,max_route_attempts=2,failure_limit=1)


def source(monkeypatch, status):
    calls=[]
    async def call(self,context,message):
        if message['op']=='merge':return {'results':[r for g in message['groups'] for r in g['results']]}
        async with self.http_gate.enter():
            calls.append((message['engine'],message['query']))
            outcome=status if message['engine']=='google' else 'ok'
            return {'status':outcome,'reason':'fixture','results':[
                {'url':'https://example.org/'+message['query'],'title':'result'}] if outcome=='ok' else [],
                'http':[{'host':'fixture','http_status':200 if outcome=='ok' else 503,'elapsed_s':.001}]}
    monkeypatch.setattr(NativeSearchSession,'call',call)
    return calls


@pytest.mark.parametrize('status',['network_error','timeout','captcha','rate_limited','authentication_error','configuration_error'])
def test_source_quarantine_survives_restart_and_siblings_continue(tmp_path,monkeypatch,status):
    calls=source(monkeypatch,status)
    async def run():
        s=pool(tmp_path)
        failed=await s.search('bad',engines=['google'])
        assert failed['status']=='search_failed' and len(calls)==2
        assert (await s.search('unsent',engines=['google']))['retryable'] is True
        assert len(calls)==2
        assert (await s.search('healthy',engines=['bing']))['status']=='ok'
        await s.aclose()
        s=pool(tmp_path)
        try:
            before=len(calls)
            assert await s.search('bad',engines=['google'])==failed
            assert (await s.search('unsent',engines=['google']))['retryable'] is True
            assert len(calls)==before
            assert (await s.search('healthy2',engines=['bing']))['status']=='ok'
            assert not s.snapshot_metrics()['query_stopped']
            assert s.http_gate.peak<=2
        finally:await s.aclose()
    asyncio.run(run())


def test_legacy_global_stop_does_not_poison_new_scope_or_reset_receipts(tmp_path,monkeypatch):
    calls=source(monkeypatch,'authentication_error')
    async def run():
        old=pool(tmp_path,scope='pool')
        with pytest.raises(ServiceStopped):await old.search('old_failure',engines=['google'])
        identity=old.admission_key
        await old.aclose()
        s=pool(tmp_path)
        try:
            assert (await s.search('new',engines=['bing']))['status']=='ok'
            assert s.admission_key!=identity
            with s.routes[0]['session']._db() as db:
                state=json.loads(db.execute('SELECT state_json FROM native_search_recovery WHERE identity=?',(identity,)).fetchone()[0])
            assert state['fatal']=='search_authentication_or_configuration'
        finally:await s.aclose()
    asyncio.run(run())


def test_deferred_checkpoint_recovers_without_upstream_and_skips_completed(tmp_path,monkeypatch):
    calls=source(monkeypatch,'network_error')
    async def run():
        s=pool(tmp_path,cooldown=.05)
        ledger=tmp_path/'tasks.sqlite'
        actor=SearchWeb('requests','results',s,None,5,2,checkpoint=ledger)
        await actor({'requests':[{'request_id':'bad','query':'bad','engines':['google']}]})
        out=await actor({'requests':[
            {'request_id':'later','query':'later','engines':['google']},
            {'request_id':'good','query':'good','engines':['bing']}]})
        assert out['results'][0]['retryable'] and out['results'][1]['status']=='ok'
        assert actor._checkpoint.stats()=={'completed':2,'retryable':1}
        await s.aclose()
        await asyncio.sleep(.06)
        source(monkeypatch,'ok')
        s=pool(tmp_path,cooldown=.05)
        actor=SearchWeb('requests','results',s,None,5,2,checkpoint=ledger)
        try:
            recovered=actor.recovery_rows()
            assert [r['requests'][0]['request_id'] for r in recovered]==['later']
            assert (await actor(recovered[0]))['results'][0]['status']=='ok'
            assert actor._checkpoint.stats()=={'completed':3}
        finally:await s.aclose()
    asyncio.run(run())


def test_source_scope_rejects_multi_engine_and_propagates_real_shutdown(tmp_path,monkeypatch):
    source(monkeypatch,'ok')
    async def run():
        s=pool(tmp_path)
        with pytest.raises(ValueError,match='one engine'):await s.search('ambiguous')
        await s.aclose()
        with pytest.raises(RuntimeError,match='closed'):await s.search('new',engines=['bing'])
    asyncio.run(run())
