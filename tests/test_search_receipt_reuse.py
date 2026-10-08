"""Transport changes reuse exact evidence and preserve unknown reservations."""
import asyncio
import copy
import json
import pytest
from demiflow.collect.native_search import NativeSearchSession, SearchConfig
from demiflow.collect.native_search.config import digest
from demiflow.collect.search_reuse import ReceiptReuseSearchSession, completed_reuse_configs


def test_declaration_rejects_semantic_or_runtime_setting_changes():
    current = SearchConfig(engines=['google'],proxy='http://new.example:3128')
    previous = SearchConfig(engines=['google'],proxy='http://old.example:3128')
    assert completed_reuse_configs(current,[previous])[0].proxy==previous.proxy
    with pytest.raises(ValueError,match='only an outgoing proxy'):
        completed_reuse_configs(current,[SearchConfig(engines=['google'],language='en')])


def test_prior_success_keeps_one_receipt_and_failure_is_not_reused(tmp_path,monkeypatch):
    old_config=SearchConfig(engines=['google'],proxy='http://old.example:3128')
    new_config=SearchConfig(engines=['google'],proxy='http://new.example:3128')
    fallback=[]
    async def ordinary_source(self,source,query,parameters):
        fallback.append(query)
        return {'status':'ordinary_new_transport','engine':source['name']}
    monkeypatch.setattr(NativeSearchSession,'source',ordinary_source)
    async def run():
        old=NativeSearchSession(cache_path=tmp_path/'receipts.sqlite',config=old_config)
        old._initialize()
        params,sources=old.parameters('ok',language='all');source=sources[0]
        key=digest([old.profile,source['name'],'ok',params])
        saved={'status':'ok','results':[],'receipt_id':key,'attempts':[{'status':'ok','http_status':200}]}
        old._cache(key,saved)
        failed=digest([old.profile,source['name'],'failed',params]);old._cache(failed,{'status':'captcha','attempts':[{'http_status':302}]})
        new=ReceiptReuseSearchSession(cache_path=old.path,config=new_config,reuse_configs=[old_config])
        try:
            await new.initialize()
            assert new.profile!=old.profile
            first=await new.source(source,'ok',params)
            again=await new.source(source,'ok',params)
            assert first==again=={'engine':source['name'],**saved}
            assert new.metrics['http_requests']==0 and new.metrics['reused_previous_proxy']==2
            result=await new.source(source,'failed',params)
            assert result['status']=='ordinary_new_transport' and fallback==['failed']
            with new._db() as db:
                assert db.execute('select count(*) from native_search').fetchone()[0]==2
                assert db.execute('select count(*) from native_search_reuse').fetchone()[0]==1
                assert json.loads(db.execute('select value from native_search where key=?',(failed,)).fetchone()[0])['status']=='captcha'
            # Current reservations must never be hidden by an older success.
            current_key=digest([new.profile,source['name'],'ok',params])
            new._cache(current_key,{'status':'interrupted','attempts':[]})
            assert (await new.source(source,'ok',params))['status']=='ordinary_new_transport'
        finally:
            await new.aclose();await old.aclose()
    asyncio.run(run())


def test_large_profile_lookup_keeps_declared_priority_and_ignores_failures(tmp_path):
    session=ReceiptReuseSearchSession(cache_path=tmp_path/'many.sqlite',
        config=SearchConfig(engines=['google']),reuse_configs=[])
    session._initialize()
    keys=[str(i) for i in range(805)]
    session._cache(keys[0],{'status':'captcha'})
    session._cache(keys[403],{'status':'ok','receipt_id':keys[403]})
    session._cache(keys[804],{'status':'ok','receipt_id':keys[804]})
    assert session.first_completed(keys)==(keys[403],{'status':'ok','receipt_id':keys[403]})
    assert session.first_completed(list(reversed(keys)))==(keys[804],{'status':'ok','receipt_id':keys[804]})
    assert session.first_completed([]) is None


def test_unknown_prior_reservation_survives_proxy_change_without_http(tmp_path,monkeypatch):
    old_config=SearchConfig(engines=['google'],proxy='http://old.example:3128')
    new_config=SearchConfig(engines=['google'],proxy='http://new.example:3128')
    async def forbidden_call(*args,**kwargs):
        raise AssertionError('Unknown prior request must not reach HTTP')
    monkeypatch.setattr(NativeSearchSession,'call',forbidden_call)
    async def run():
        old=NativeSearchSession(cache_path=tmp_path/'unknown.sqlite',config=old_config)
        old._initialize()
        params,sources=old.parameters('unknown',language='all');source=sources[0]
        key=digest([old.profile,source['name'],'unknown',params])
        saved={'status':'interrupted','results':[],'receipt_id':key,
               'attempts':[{'status':'interrupted','http':[{'http_status':None}]}]}
        old._cache(key,saved)
        new=ReceiptReuseSearchSession(cache_path=old.path,config=new_config,reuse_configs=[old_config])
        try:
            await new.initialize()
            assert await new.source(source,'unknown',params)=={'engine':source['name'],**saved}
            assert await new.source(source,'unknown',params)=={'engine':source['name'],**saved}
            assert new.metrics['http_requests']==new.metrics['source_attempts']==0
            with new._db() as db:
                assert db.execute('SELECT count(*) FROM native_search').fetchone()[0]==1
                assert json.loads(db.execute('SELECT value FROM native_search WHERE key=?',(key,)).fetchone()[0])==saved
        finally:
            await new.aclose();await old.aclose()
    asyncio.run(run())


def test_completed_evidence_wins_over_unknown_across_lookup_chunks(tmp_path):
    session=ReceiptReuseSearchSession(cache_path=tmp_path/'priority.sqlite',
        config=SearchConfig(engines=['google']),reuse_configs=[])
    session._initialize()
    keys=[str(i) for i in range(805)]
    unknown={'status':'interrupted','attempts':[]}
    session._cache(keys[0],unknown)
    assert session.first_completed(keys,preserve_interrupted=True)==(keys[0],unknown)
    success={'status':'ok','receipt_id':keys[804]}
    session._cache(keys[804],success)
    assert session.first_completed(keys,preserve_interrupted=True)==(keys[804],success)


def test_expiry_after_http_keeps_unknown_when_transport_changes(tmp_path,monkeypatch):
    from demiflow.collect.search_errors import RouteLeaseExpired
    calls=[]
    async def lost_after_http(self,context,message):
        calls.append(message['query']);self.metrics['http_requests']+=1
        error=RouteLeaseExpired('fixture after HTTP')
        error.native_http_receipts=[{'http_status':None}]
        raise error
    monkeypatch.setattr(NativeSearchSession,'call',lost_after_http)
    old_config=SearchConfig(engines=['google'],proxy='http://old.example:3128')
    new_config=SearchConfig(engines=['google'],proxy='http://new.example:3128')
    async def run():
        old=ReceiptReuseSearchSession(cache_path=tmp_path/'after_http.sqlite',config=old_config,reuse_configs=[])
        new=ReceiptReuseSearchSession(cache_path=old.path,config=new_config,reuse_configs=[old_config])
        try:
            await old.initialize();params,sources=old.parameters('unknown',language='all')
            original=await old.source(sources[0],'unknown',params)
            assert original['local_lease_expired']
            assert original['attempts'][0]['http']==[{'http_status':None}]
            key=digest([old.profile,sources[0]['name'],'unknown',params])
            assert (await old.database(key))['status']=='interrupted'
            await new.initialize()
            result=await new.source(sources[0],'unknown',params)
            assert result['status']=='interrupted' and result['receipt_id']==key
            assert calls==['unknown'] and new.metrics['http_requests']==0
        finally:await new.aclose();await old.aclose()
    asyncio.run(run())
