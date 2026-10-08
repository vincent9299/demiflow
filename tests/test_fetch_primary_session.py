"""Primary download sessions use the same bounded Dataset transport lifecycle."""
import asyncio
import io
import json
import os

import httpx
import pytest
from PIL import Image

from demiflow import data
from demiflow.collect.session import WebSession
from demiflow.collect.web import WebClient
from demiflow.collect.image_library import ImageLibrary
from demiflow.execution.request_limits import ServiceStopped
from test_fetch_session_fallback import install, policy, proxy_env


def test_symmetric_session_entries_are_lazy_and_independent(tmp_path,monkeypatch):
    monkeypatch.delenv('FETCH_TEST_BASE',raising=False)
    search={'factory':'test_fetch_session_fallback:factory',
            'factory_options':{'base_env':'FETCH_TEST_BASE'},'size':1,'min_healthy':1}
    web=WebSession(cache_path=tmp_path/'absent.sqlite',object_directory=tmp_path/'objects',
                   search_session_pool=search,fetch_session_pool=policy())
    assert web.search_session_pool['size']==1
    assert web.options['fetch_proxy_routes']['*']['session_pool']['size']==1
    assert not (tmp_path/'absent.sqlite').exists()
    assert web.native is None and web.image_client is None


@pytest.mark.parametrize('extra',[
    {'fetch_proxy_routes':{'*':None}},
    {'fetch_proxy_url':'http://fixed.example:3128'},
    {'fetch_proxy_routes':{'images.example':{'session_pool':policy(),'pool':[]}}},
])
def test_ambiguous_primary_configuration_fails_before_io(tmp_path,extra):
    with pytest.raises(ValueError):
        WebSession(cache_path=tmp_path/'absent.sqlite',object_directory=tmp_path/'objects',
                   fetch_session_pool=policy(),**extra)
    assert not (tmp_path/'absent.sqlite').exists()


def test_primary_session_dataset_and_canonical_replay(tmp_path,monkeypatch,proxy_env):
    body=io.BytesIO();Image.new('RGB',(8,8),'green').save(body,format='PNG')
    calls=install(monkeypatch,payload=body.getvalue())
    def run(alias):
        route=({'fetch_session_pool':policy()} if alias else
               {'fetch_proxy_routes':{'*':{'session_pool':policy()}}})
        web=WebSession(cache_path=tmp_path/'requests.sqlite',object_directory=tmp_path/'objects',
            image_library=ImageLibrary(str(tmp_path/'objects'),str(tmp_path/'images.sqlite')),
            retries=0,host_interval_s=0,**route)
        rows=[]
        data.from_items([{'requests':[{'request_id':'one','url':'https://images.example/original'}]}]).fetch_images(
            requests='requests',output='images',session=web).map(lambda r:rows.append(r) or r).run_stream()
        return rows[0]['images'][0]['result']
    first=run(True)
    assert first['status']=='ok' and first['width']==8 and calls==['session']
    assert run(False)==first and calls==['session']
    assert not any(k.startswith('FETCH_GENERATION_') for k in os.environ)


async def test_429_replaces_generation_within_explicit_attempts(tmp_path,monkeypatch,proxy_env):
    generations=[];clients=[]
    async def get(self,url,selected_route=None):
        assert selected_route and '_fetch_session_pool' in selected_route
        if selected_route['client'] is None:
            generations.append(selected_route['name'])
            code=429 if len(generations)==1 else 200
            selected_route['client']=httpx.AsyncClient(transport=httpx.MockTransport(
                lambda request:httpx.Response(code,stream=httpx.ByteStream(b'body'),headers={'retry-after':'3600'})))
            clients.append(selected_route['client'])
        return selected_route['client']
    monkeypatch.setattr(WebClient,'_fetch_client',get)
    web=WebClient(cache_path=tmp_path/'requests.sqlite',object_directory=tmp_path/'objects',
        fetch_session_pool=policy(),host_interval_s=0,retries=1,retry_delay_s=30)
    try:
        result=await asyncio.wait_for(web._get('https://images.example/a'),1)
        assert result['status']=='ok'
        assert [r['http_status'] for r in result['attempts']]==[429,200]
        assert len(set(generations))==2 and clients[0].is_closed
        assert not web.fetch_route_pools
    finally:await web.aclose()
    assert all(c.is_closed for c in clients)


async def test_creation_budget_is_durable_for_primary_sessions(tmp_path,monkeypatch,proxy_env):
    calls=install(monkeypatch,dynamic=429)
    args=dict(cache_path=tmp_path/'requests.sqlite',object_directory=tmp_path/'objects',
        fetch_session_pool=policy(max_creations_per_window=1),host_interval_s=0,retries=1)
    for _ in range(2):
        web=WebClient(**args)
        try:
            with pytest.raises(ServiceStopped,match='fetch_session_creation_budget'):
                await web._get('https://images.example/a')
        finally:await web.aclose()
    assert calls==['session']


async def test_domain_overrides_are_rechecked_on_redirect(tmp_path,monkeypatch,proxy_env):
    calls=[]
    async def get(self,url,selected_route=None):
        session=selected_route is not None and '_fetch_session_pool' in selected_route
        def handle(request):
            calls.append(('session' if session else 'fixed',request.url.host))
            if request.url.path=='/jump':
                target='company.example' if session else 'images.example'
                return httpx.Response(302,stream=httpx.ByteStream(b''),headers={'location':f'https://{target}/original'})
            return httpx.Response(200,stream=httpx.ByteStream(b'body'))
        if session:
            if selected_route['client'] is None:
                selected_route['client']=httpx.AsyncClient(transport=httpx.MockTransport(handle))
            return selected_route['client']
        if self.client is None:self.client=httpx.AsyncClient(transport=httpx.MockTransport(handle))
        return self.client
    monkeypatch.setattr(WebClient,'_fetch_client',get)
    web=WebClient(cache_path=tmp_path/'requests.sqlite',object_directory=tmp_path/'objects',
        fetch_session_pool=policy(),fetch_proxy_routes={'company.example':None},
        host_interval_s=0,retries=0)
    try:
        assert (await web._get('https://images.example/jump'))['status']=='ok'
        assert (await web._get('https://company.example/jump'))['status']=='ok'
        assert calls==[('session','images.example'),('fixed','company.example'),
                       ('fixed','company.example'),('session','images.example')]
    finally:await web.aclose()


async def test_primary_session_identity_excludes_limits_but_tracks_credentials(tmp_path,monkeypatch,proxy_env):
    web=WebClient(cache_path=tmp_path/'requests.sqlite',object_directory=tmp_path/'objects',
                  fetch_session_pool=policy())
    try:
        first=web.proxy_identity('https://images.example/a')
        web.proxy_routes['*']['session_pool']['interval_s']=10
        assert web.proxy_identity('https://images.example/a')==first
        assert 'password' not in json.dumps(first)
        monkeypatch.setenv('FETCH_TEST_BASE','http://fixture:changed@proxy.example:3128')
        assert web.proxy_identity('https://images.example/a')!=first
    finally:await web.aclose()


async def test_close_cancels_pending_acquisition_without_resetting_receipt(tmp_path):
    entered=asyncio.Event();never=asyncio.Event();calls=[]
    async def acquire():
        calls.append(1);entered.set();await never.wait()
        return {'status':'ok'}
    args=dict(cache_path=tmp_path/'requests.sqlite',object_directory=tmp_path/'objects')
    web=WebClient(**args)
    request=asyncio.create_task(web._once('fetch_image',['reserved'],acquire))
    await asyncio.wait_for(entered.wait(),1)
    await asyncio.wait_for(web.aclose(),1)
    with pytest.raises(asyncio.CancelledError):await request
    assert all(task.done() for task in web.inflight.values())
    resumed=WebClient(**args)
    try:
        result=await resumed._once('fetch_image',['reserved'],acquire)
        assert result['status']=='interrupted' and calls==[1]
    finally:await resumed.aclose()
