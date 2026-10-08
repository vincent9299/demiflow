"""Declared host exclusions must prevent I/O and remain distinct from failure."""
import asyncio
import json
import httpx
import pytest
from demiflow.collect.session import WebSession
from demiflow.collect.web import WebClient, normalized_blocked_domains


class Body(httpx.AsyncByteStream):
    async def __aiter__(self):
        yield b'<main><h1>Manual</h1><p>Technical source body.</p></main>'


def test_policy_declaration_validates_without_io(tmp_path):
    path=tmp_path/'absent/cache.sqlite'
    session=WebSession(cache_path=path,object_directory=tmp_path/'objects',blocked_domains=['SHOP.Example.','shop.example'])
    assert session.options['blocked_domains']==['shop.example'] and not path.exists()
    for invalid in ['shop.example',['https://shop.example'],['shop.example:443'],['*.example'],[''],['bad..example'],['shop.example/path']]:
        with pytest.raises(ValueError):normalized_blocked_domains(invalid)


def test_direct_exclusion_is_durable_without_network_or_shared_lookup(tmp_path):
    async def run():
        web=WebClient(cache_path=tmp_path/'cache.sqlite',object_directory=tmp_path/'objects',blocked_domains=['shop.example'])
        async def forbidden(*args,**kwargs):raise AssertionError('Blocked host attempted network')
        web._get=forbidden
        try:
            first=await web.fetch('https://www.shop.example/item')
            second=await web.fetch('https://www.shop.example/item')
            assert first==second and first['status']=='excluded' and first['attempts']==[]
            assert web.metrics['fetch_attempts']==0 and web.metrics['library_hits']==0
            assert web.blocked_domain('https://notshop.example/') is None
            assert web.blocked_domain('https://shop.example.evil.test/') is None
            assert web.blocked_domain('https://SHOP.EXAMPLE.:443/')=='shop.example'
            assert any('blocked_domain:shop.example' in value for value, in web._db().execute('select value from cache'))
        finally:await web.aclose()
    asyncio.run(run())


def test_redirect_target_is_excluded_before_connection_without_retry(tmp_path):
    calls=[]
    def handle(request):
        calls.append(str(request.url))
        return httpx.Response(302,headers={'location':'https://shop.example/item'},stream=Body())
    async def run():
        web=WebClient(cache_path=tmp_path/'cache.sqlite',object_directory=tmp_path/'objects',blocked_domains=['shop.example'],host_interval_s=0,retries=1)
        web.client=httpx.AsyncClient(transport=httpx.MockTransport(handle))
        try:
            result=await web.fetch('https://source.example/redirect')
            assert result['status']=='excluded' and result['reason']=='blocked_domain:shop.example'
            assert calls==['https://source.example/redirect']
            assert web.metrics['fetch_attempts']==1 and web.metrics['http_hops']==1
            assert len(result['attempts'])==1
        finally:await web.aclose()
    asyncio.run(run())


def test_cached_success_is_preserved_but_not_served_by_blocked_run(tmp_path,monkeypatch):
    monkeypatch.setattr('demiflow.collect.web.run_isolated',lambda fn,*a,timeout_s=None,**kw:fn(*a,**kw))
    calls=[]
    def handle(request):
        calls.append(str(request.url))
        return httpx.Response(200,headers={'content-type':'text/html'},stream=Body())
    async def run(blocked):
        web=WebClient(cache_path=tmp_path/'cache.sqlite',object_directory=tmp_path/'objects',blocked_domains=blocked,host_interval_s=0)
        web.client=httpx.AsyncClient(transport=httpx.MockTransport(handle))
        try:return await web.fetch('https://shop.example/item')
        finally:await web.aclose()
    before=asyncio.run(run([]));blocked=asyncio.run(run(['shop.example']));again=asyncio.run(run([]))
    assert before['status']=='ok' and blocked['status']=='excluded' and again==before
    assert len(calls)==1


def test_ordered_path_rules_keep_manuals_and_exclude_shopping(tmp_path):
    web=WebClient(cache_path=tmp_path/'cache.sqlite',object_directory=tmp_path/'objects',
        blocked_domains=['market.example'],fetch_url_rules=[
            {'name':'manuals','action':'allow','path_pattern':r'(?i)/(manuals|support)/'},
            {'name':'shopping','action':'exclude','path_pattern':r'(?i)^/(?:[a-z]{2}(?:-[a-z]{2})?/)?(products|shop|collections)(/|$)'}])
    assert web.exclusion_reason('https://maker.example/products/one')=='url_rule:shopping'
    assert web.exclusion_reason('https://maker.example/products/support/manual.pdf') is None
    assert web.exclusion_reason('https://encyclopedia.example/wiki/Products') is None
    assert web.exclusion_reason('https://market.example/support/manual.pdf')=='blocked_domain:market.example'


def test_domain_scoped_allow_does_not_allow_same_path_on_a_shop(tmp_path):
    session=WebSession(cache_path=tmp_path/'absent/cache.sqlite',object_directory=tmp_path/'objects',
        fetch_url_rules=[{'name':'institutional_collection','action':'allow',
            'domains':['Museum.Example.'],'path_pattern':r'/collections/'},
            {'name':'shopping','action':'exclude','path_pattern':r'/collections/'}])
    assert session.options['fetch_url_rules'][0]['domains']==['museum.example']
    assert not (tmp_path/'absent').exists()
    web=WebClient(cache_path=tmp_path/'cache.sqlite',object_directory=tmp_path/'objects',
        fetch_url_rules=session.options['fetch_url_rules'])
    assert web.exclusion_reason('https://archive.museum.example/zh-hans/collections/item') is None
    assert web.exclusion_reason('https://shop.example/zh-hans/collections/item')=='url_rule:shopping'
    assert web.exclusion_reason('https://museum.example.evil.test/collections/item')=='url_rule:shopping'
    for domains in ([],None,['*.example'],['https://museum.example']):
        with pytest.raises(ValueError):
            WebSession(cache_path=tmp_path/'absent/cache.sqlite',object_directory=tmp_path/'objects',
                fetch_url_rules=[{'name':'bad','action':'allow','path_pattern':'/', 'domains':domains}])
