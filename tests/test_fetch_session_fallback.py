import asyncio
import io
import os
import sqlite3

import httpx
import pytest
from PIL import Image
from demiflow import data
from demiflow.collect.web import WebClient
from demiflow.collect.session import WebSession
from demiflow.collect.fetch_sessions import FetchSessionPool,fetch_session_policy
from demiflow.collect.image_library import ImageLibrary
from demiflow.execution.request_limits import ServiceStopped


def factory(*,token,ttl_s,options):
    key='FETCH_GENERATION_'+token.upper()
    os.environ[key]=os.environ[options['base_env']]
    return {'secret_env':key}


def policy(**extra):
    return {'factory':'test_fetch_session_fallback:factory','factory_options':{'base_env':'FETCH_TEST_BASE'},
        'identity_envs':['FETCH_TEST_BASE'],'size':1,'ttl_s':300,'interval_s':.001,
        'max_creations_per_window':4,**extra}


def routes(**extra):
    return {'*':{'pool':[{'name':'static','proxy':'http://static.example:3128','interval_s':0}],
        'cooldown_wait_s':0,'fallback_session_pool':policy(**extra)}}


@pytest.fixture
def proxy_env(monkeypatch):
    monkeypatch.setenv('FETCH_TEST_BASE','http://fixture:password@proxy.example:3128')


def install(monkeypatch,static=429,dynamic=200,payload=b'image',headers=None):
    calls=[]
    async def get(self,url,selected_route=None):
        renewable=selected_route and '_fetch_session_pool' in selected_route
        kind='session' if renewable else 'static'
        key=('mock',kind)
        holder=selected_route if renewable else self.route_clients
        cache_key='client' if renewable else key
        if not holder.get(cache_key):
            def handler(request):
                calls.append(kind)
                code=dynamic if renewable else static
                if code=='network':raise httpx.ConnectError('fixture')
                return httpx.Response(code,stream=httpx.ByteStream(payload),headers={'content-type':'image/png',**(headers or {})})
            holder[cache_key]=httpx.AsyncClient(transport=httpx.MockTransport(handler))
        return holder[cache_key]
    monkeypatch.setattr(WebClient,'_fetch_client',get)
    return calls


@pytest.mark.parametrize('status',[403,429,503,'network'])
async def test_static_failure_uses_one_session_attempt_and_preserves_both(tmp_path,monkeypatch,proxy_env,status):
    calls=install(monkeypatch,static=status)
    web=WebClient(cache_path=tmp_path/'run.sqlite',object_directory=tmp_path/'objects',
        retries=0,host_interval_s=0,fetch_proxy_routes=routes())
    try:
        result=await web._get('https://images.example/a')
        assert result['status']=='ok' and calls==['static','session']
        assert [r['route_kind'] for r in result['attempts']]==['static','session']
        assert [r['attempt'] for r in result['attempts']]==[1,2]
        assert result['attempts'][0]['status']!='ok'
        assert web.fetch_session_pools['*'].metrics['created']==1
    finally:await web.aclose()
    assert not any(k.startswith('FETCH_GENERATION_') for k in os.environ)


@pytest.mark.parametrize('status',[200,404])
async def test_success_and_permanent_failure_do_not_create_session(tmp_path,monkeypatch,proxy_env,status):
    calls=install(monkeypatch,static=status)
    web=WebClient(cache_path=tmp_path/'run.sqlite',object_directory=tmp_path/'objects',
        retries=0,host_interval_s=0,fetch_proxy_routes=routes())
    try:
        result=await web._get('https://images.example/a')
        assert calls==['static'] and not web.fetch_session_pools
        assert result['status']==('ok' if status==200 else 'http_error')
    finally:await web.aclose()


async def test_long_retry_after_does_not_bypass_server_wait_with_new_session(tmp_path,monkeypatch,proxy_env):
    calls=install(monkeypatch,headers={'retry-after':'120'})
    web=WebClient(cache_path=tmp_path/'run.sqlite',object_directory=tmp_path/'objects',
        retries=0,host_interval_s=0,fetch_proxy_routes=routes())
    try:
        result=await web._get('https://images.example/a')
        assert result['status']=='rate_limited' and calls==['static']
        assert result['attempts'][0]['retry_after_s']==120 and not web.fetch_session_pools
    finally:await web.aclose()


async def test_attempt_cap_includes_fallback(tmp_path,monkeypatch,proxy_env):
    calls=install(monkeypatch)
    web=WebClient(cache_path=tmp_path/'cap.sqlite',object_directory=tmp_path/'objects',
        retries=0,fetch_attempt_limit=1,host_interval_s=0,fetch_proxy_routes=routes())
    try:
        result=await web._get('https://images.example/a')
        assert result['status']=='rate_limited' and calls==['static']
        assert len(result['attempts'])==1 and not web.fetch_session_pools
    finally:await web.aclose()


def test_dataset_download_fallback_and_completed_replay(tmp_path,monkeypatch,proxy_env):
    buf=io.BytesIO();Image.new('RGB',(8,8),'blue').save(buf,format='PNG')
    calls=install(monkeypatch,payload=buf.getvalue())
    def run():
        web=WebSession(cache_path=tmp_path/'run.sqlite',object_directory=tmp_path/'objects',
            image_library=ImageLibrary(str(tmp_path/'objects'),str(tmp_path/'index.sqlite')),
            retries=0,host_interval_s=0,fetch_proxy_routes=routes())
        out=[]
        data.from_items([{'requests':[{'request_id':'a','url':'https://images.example/a'}]}]).fetch_images(
            requests='requests',output='images',session=web).map(lambda r:out.append(r) or r).run_stream()
        return out[0]['images'][0]['result']
    first=run();assert first['status']=='ok' and first['width']==8
    assert run()==first and calls==['static','session']


async def test_generation_expiry_closes_client_and_creation_budget_survives_restart(tmp_path,monkeypatch,proxy_env):
    calls=install(monkeypatch,dynamic=429)
    kwargs=dict(cache_path=tmp_path/'run.sqlite',object_directory=tmp_path/'objects',
        retries=0,host_interval_s=0,fetch_proxy_routes=routes(max_creations_per_window=1))
    web=WebClient(**kwargs)
    try:
        assert (await web._get('https://images.example/a'))['status']=='rate_limited'
        route=web.fetch_session_pools['*'].slots[0]
        result=await web._get('https://images.example/b')
        assert result['reason']=='fetch_session_creation_budget'
        assert route['client'].is_closed and calls==['static','session']
    finally:await web.aclose()
    again=WebClient(**kwargs)
    try:assert (await again._get('https://images.example/c'))['reason']=='fetch_session_creation_budget'
    finally:await again.aclose()


async def test_cancellation_releases_lease_and_expiry_replaces_generation(tmp_path,proxy_env):
    web=WebClient(cache_path=tmp_path/'run.sqlite',object_directory=tmp_path/'objects')
    pool=FetchSessionPool(web,'*',policy(ttl_s=.02))
    started=asyncio.Event()
    async def holder():
        async with pool.lease():started.set();await asyncio.sleep(30)
    task=asyncio.create_task(holder());await started.wait();task.cancel()
    with pytest.raises(asyncio.CancelledError):await task
    assert pool.snapshot_metrics()['active']==0
    first=pool.slots[0]['name'];await asyncio.sleep(.03)
    async with pool.lease() as route:assert route['name']!=first
    await pool.aclose();await web.aclose()
    assert not any(k.startswith('FETCH_GENERATION_') for k in os.environ)


def test_declaration_is_lazy_bounded_and_identity_changes_with_credentials(tmp_path,monkeypatch):
    settings=routes()
    web=WebSession(cache_path=tmp_path/'unused.sqlite',object_directory=tmp_path/'objects',fetch_proxy_routes=settings)
    assert not tmp_path.joinpath('unused.sqlite').exists()
    for bad in [{'size':65},{'max_creations_per_window':4097},{'acquisition_timeout_s':301},{'ttl_s':0}]:
        with pytest.raises(ValueError):fetch_session_policy(policy(**bad))
    from demiflow.collect.fetch_routes import transport_identity,proxy_routes
    monkeypatch.setenv('FETCH_TEST_BASE','http://a:one@proxy.example')
    first=transport_identity('*',proxy_routes(settings)['*'])
    monkeypatch.setenv('FETCH_TEST_BASE','http://a:two@proxy.example')
    assert transport_identity('*',proxy_routes(settings)['*'])!=first
