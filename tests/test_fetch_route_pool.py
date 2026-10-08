"""Domain routing, bounded retries, durable health and exact receipt replay."""
import asyncio
import copy
import json
import time
import httpx
import pytest
from demiflow.collect.web import WebClient
from demiflow.collect.session import WebSession
from demiflow.collect.fetch_routes import pool_declaration,FetchRoutePool
from demiflow.execution.request_limits import ServiceStopped


def policy(interval=0):
    return {'pool':[{'name':name,'proxy':'http://'+name+'.example:3128','interval_s':interval}
                    for name in ('one','two')],'cooldown_s':60,'failure_limit':2}


@pytest.mark.parametrize('reuse,expected_connections',[(True,1),(False,2)])
async def test_static_pool_connection_reuse_is_explicit(tmp_path,reuse,expected_connections):
    peers=set();handlers=set()
    async def serve(reader,writer):
        task=asyncio.current_task();handlers.add(task)
        peers.add(writer.get_extra_info('peername'))
        try:
            async with asyncio.timeout(3):
                while True:
                    await reader.readuntil(b'\r\n\r\n')
                    writer.write(b'HTTP/1.1 200 OK\r\nContent-Length: 4\r\n\r\nbody')
                    await writer.drain()
        except (asyncio.IncompleteReadError,TimeoutError):pass
        finally:
            writer.close();await writer.wait_closed();handlers.discard(task)
    server=await asyncio.start_server(serve,'127.0.0.1',0)
    port=server.sockets[0].getsockname()[1]
    web=WebClient(cache_path=tmp_path/'requests.sqlite',object_directory=tmp_path/'objects',
        retries=0,host_interval_s=0,fetch_proxy_routes={'*':{'pool':[{
            'name':'local','proxy':f'http://127.0.0.1:{port}','interval_s':0,
            'concurrency':2,'reuse_connections':reuse}]}})
    try:
        for path in ('one','two'):
            result=await asyncio.wait_for(web._get('http://source.example/'+path),2)
            assert result['status']=='ok' and result['body']==b'body'
        assert len(peers)==expected_connections
        assert not web.fetch_session_pools
    finally:
        await web.aclose();server.close();await server.wait_closed()
        await asyncio.gather(*tuple(handlers),return_exceptions=True)


class Body(httpx.AsyncByteStream):
    async def __aiter__(self):yield b'<main><h1>Reference</h1><p>Document body.</p></main>'


def response(code=200,**headers):
    return httpx.Response(code,headers={'content-type':'text/html',**headers},stream=Body())


def client(tmp_path,route=None,**kw):
    return WebClient(cache_path=tmp_path/'fetch.sqlite',object_directory=tmp_path/'objects',
        host_interval_s=0,fetch_proxy_routes={'source.example':route or policy()},**kw)


def install(web,handler):
    for name in ('one','two'):
        web.route_clients[('source.example',name)]=httpx.AsyncClient(
            transport=httpx.MockTransport(lambda req,n=name:handler(n,req)))


def test_pool_declarations_are_lazy_and_secret_only(tmp_path,monkeypatch):
    monkeypatch.delenv('ABSENT_POOL_SECRET',raising=False)
    p=policy();p['pool'][0]['proxy']={'secret_env':'ABSENT_POOL_SECRET'}
    web=WebSession(cache_path=tmp_path/'absent.sqlite',object_directory=tmp_path/'objects',fetch_proxy_routes={'source.example':p})
    assert not (tmp_path/'absent.sqlite').exists()
    assert web.options['fetch_proxy_routes']['source.example']['pool'][0]['proxy']=={'secret_env':'ABSENT_POOL_SECRET'}
    for bad in ({'pool':[]},{**policy(),'cooldown_s':float('nan')},{**policy(),'failure_limit':0},
                {**policy(),'pool':[{'name':'a','proxy':'http://user:password@proxy.example'}]}):
        with pytest.raises(ValueError):pool_declaration(bad)


def test_domain_and_redirect_reselect_route(tmp_path):
    async def run():
        web=client(tmp_path,retries=0);calls=[]
        def handle(name,request):
            calls.append((name,request.url.host))
            return response(302,location='https://dynamic.example/article') if request.url.path=='/redirect' else response()
        install(web,handle)
        web.proxy_routes['dynamic.example']='http://dynamic.example:3128'
        web.route_clients['dynamic.example']=httpx.AsyncClient(transport=httpx.MockTransport(lambda req:handle('dynamic',req)))
        web.client=httpx.AsyncClient(transport=httpx.MockTransport(lambda req:handle('default',req)))
        try:
            assert (await web._get('https://source.example/redirect'))['status']=='ok'
            assert (await web._get('https://source.example/page'))['status']=='ok'
            assert (await web._get('https://source.example.evil/page'))['status']=='ok'
            assert calls==[('one','source.example'),('dynamic','dynamic.example'),('two','source.example'),('default','source.example.evil')]
        finally:await web.aclose()
    asyncio.run(run())


def test_rate_limit_cools_only_one_route_and_survives_restart(tmp_path):
    async def first():
        web=client(tmp_path,retries=1);calls=[]
        def handle(name,request):
            calls.append(name)
            return response(429,**{'retry-after':'120'}) if name=='one' else response()
        install(web,handle)
        try:
            assert (await web._get('https://source.example/a'))['status']=='rate_limited'
            assert calls==['one']  # Retry-After exceeds existing bounded retry policy.
            assert (await web._get('https://source.example/b'))['status']=='ok'
            assert calls==['one','two']
            assert web.fetch_route_pools['source.example'].routes[0]['until']>=time.time()+119
        finally:await web.aclose()
    async def resumed():
        p=policy();p['pool'][0]['interval_s']=.01
        web=client(tmp_path,p,retries=0);calls=[]
        install(web,lambda name,req:(calls.append(name),response())[1])
        try:
            await web._get('https://source.example/c');assert calls==['two']
        finally:await web.aclose()
    asyncio.run(first());asyncio.run(resumed())


def test_404_does_not_disable_proxy_and_retry_limit_is_unchanged(tmp_path):
    async def run():
        web=client(tmp_path,retries=1,retry_delay_s=0);calls=[]
        install(web,lambda name,req:(calls.append(name),response(404 if req.url.path=='/missing' else 503))[1])
        try:
            result=await web._get('https://source.example/missing')
            assert len(result['attempts'])==1 and calls==['one']
            assert all(not r['until'] for r in web.fetch_route_pools['source.example'].routes)
            result=await web._get('https://source.example/unavailable')
            assert len(result['attempts'])==2 and len(calls)==3
        finally:await web.aclose()
    asyncio.run(run())


def test_all_routes_cooling_returns_url_failure_without_http(tmp_path):
    async def run():
        web=client(tmp_path,retries=0)
        install(web,lambda name,req:response(403))
        try:
            for _ in range(2):assert (await web._get('https://source.example/a'))['status']=='http_error'
            before=web.metrics['http_hops']
            result=await web._get('https://source.example/b')
            assert result['status']=='route_unavailable'
            assert result['reason']=='all_fetch_routes_cooling_down:source.example'
            assert web.metrics['http_hops']==before
        finally:await web.aclose()
    asyncio.run(run())


def test_route_pacing_precedes_deadline_and_limits_per_proxy_concurrency(tmp_path):
    async def run():
        p=policy(.07);p['pool']=p['pool'][:1]
        web=client(tmp_path,p,timeout_s=.03,retries=0,host_concurrency=5)
        active=0;peak=0;starts=[]
        async def handle(req):
            nonlocal active,peak
            starts.append(time.monotonic());active+=1;peak=max(active,peak)
            await asyncio.sleep(.005);active-=1;return response()
        web.route_clients[('source.example','one')]=httpx.AsyncClient(transport=httpx.MockTransport(handle))
        try:
            values=await asyncio.gather(*(web._get('https://source.example/'+str(i)) for i in range(3)))
            assert all(v['status']=='ok' for v in values) and peak==1
            assert all(b-a>=.06 for a,b in zip(starts,starts[1:]))
        finally:await web.aclose()
    asyncio.run(run())


@pytest.mark.parametrize('code',[200,404])
def test_explicit_completed_reuse_preserves_success_and_failure_without_http(tmp_path,monkeypatch,code):
    monkeypatch.setattr('demiflow.collect.web.run_isolated',lambda fn,*a,timeout_s=None,**kw:fn(*a,**kw))
    old='http://old.example:3128';calls=[]
    async def initial():
        web=client(tmp_path,old,retries=0)
        web.route_clients['source.example']=httpx.AsyncClient(transport=httpx.MockTransport(
            lambda req:(calls.append(str(req.url)),response(code))[1]))
        try:return await web.fetch('https://source.example/article')
        finally:await web.aclose()
    before=asyncio.run(initial())
    async def replay():
        p=policy();p['reuse_completed']=[old]
        web=client(tmp_path,p,retries=0)
        async def forbidden(*args,**kwargs):raise AssertionError('Replay attempted HTTP')
        web._get=forbidden
        try:
            assert await web.fetch('https://source.example/article')==before
            assert web.metrics['reused_previous_proxy']==1
            assert web.metrics['http_hops']==0
        finally:await web.aclose()
    asyncio.run(replay());assert len(calls)==1


def test_cancellation_releases_lease_and_domains_keep_separate_health(tmp_path):
    async def run():
        web=client(tmp_path);p=pool_declaration(policy())
        one=FetchRoutePool(web,'source.example',p)
        one.routes[0]['next_ready']=time.monotonic()+60
        one.routes[1]['until']=time.time()+60
        task=asyncio.create_task(one.lease().__aenter__())
        await asyncio.sleep(.01);task.cancel()
        with pytest.raises(asyncio.CancelledError):await task
        assert not one.routes[0]['busy']
        one.record(one.routes[0],'source.example','rate_limited',429,1)
        other=FetchRoutePool(web,'other.example',p)
        assert all(not r['until'] for r in other.routes)
        await web.aclose()
    asyncio.run(run())


def test_proxy_identity_excludes_scheduling_and_secret_values(tmp_path,monkeypatch):
    monkeypatch.setenv('POOL_SECRET','http://account:hidden-value@proxy.example:443')
    p=policy();p['pool'][0]['proxy']={'secret_env':'POOL_SECRET'}
    web=client(tmp_path,p)
    before=web.proxy_identity('https://source.example/a')
    web.proxy_routes['source.example']['pool'][0]['interval_s']=100
    web.proxy_routes['source.example']['cooldown_s']=100
    web.proxy_routes['source.example']['transient_cooldown_s']=2
    web.proxy_routes['source.example']['cooldown_wait_s']=60
    assert web.proxy_identity('https://source.example/a')==before
    assert 'hidden-value' not in json.dumps(before)


def test_cold_pool_waits_without_http_then_resumes_inside_same_action(tmp_path):
    async def run():
        p={**policy(),'failure_limit':1,'transient_cooldown_s':.1,'cooldown_wait_s':.5}
        web=client(tmp_path,p,retries=0,timeout_s=.03);calls=[]
        install(web,lambda name,req:(calls.append(name),response())[1])
        pool=FetchRoutePool(web,'source.example',web.proxy_routes['source.example'])
        web.fetch_route_pools['source.example']=pool
        for route in pool.routes:pool.record(route,'source.example','network_error',None,.1)
        try:
            started=time.monotonic();task=asyncio.create_task(web._get('https://source.example/a'))
            await asyncio.sleep(.01)
            assert not calls and pool.cooldown_waiters==1 and not any(r['busy'] for r in pool.routes)
            assert (await task)['status']=='ok'
            assert time.monotonic()-started>=.07 and len(calls)==1
            assert pool.cooldown_waiters==0 and pool.cooldown_wait_seconds>=.07
            assert not any(r['busy'] for r in pool.routes)
        finally:await web.aclose()
    asyncio.run(run())


def test_cold_pool_total_wait_bound_survives_notifications_and_extensions(tmp_path):
    async def run():
        web=client(tmp_path,{**policy(),'cooldown_wait_s':.06},retries=0);calls=[]
        install(web,lambda name,req:(calls.append(name),response())[1])
        pool=FetchRoutePool(web,'source.example',web.proxy_routes['source.example'])
        web.fetch_route_pools['source.example']=pool
        for route in pool.routes:route['until']=time.time()+60
        async def extend():
            for _ in range(5):
                await asyncio.sleep(.01)
                async with pool.condition:
                    for route in pool.routes:route['until']+=1
                    pool.condition.notify_all()
        try:
            started=time.monotonic();updates=asyncio.create_task(extend())
            assert (await web._get('https://source.example/a'))['status']=='route_unavailable'
            await updates
            assert .05<=time.monotonic()-started<.5
            assert not calls and web.metrics['http_hops']==0 and pool.cooldown_waiters==0
        finally:await web.aclose()
    asyncio.run(run())


def test_cold_pool_cancel_releases_waiters_without_erasing_persistent_cooldown(tmp_path):
    async def run():
        p={**policy(),'cooldown_wait_s':1,'transient_cooldown_s':.01}
        web=client(tmp_path,p,retries=0)
        pool=FetchRoutePool(web,'source.example',web.proxy_routes['source.example'])
        for route in pool.routes:pool.record(route,'source.example','rate_limited',429,.1,120)
        try:
            task=asyncio.create_task(pool.lease().__aenter__());await asyncio.sleep(.01)
            assert pool.cooldown_waiters==1
            task.cancel()
            with pytest.raises(asyncio.CancelledError):await task
            assert pool.cooldown_waiters==0 and not any(r['busy'] for r in pool.routes)
            resumed=FetchRoutePool(web,'source.example',pool_declaration(p))
            assert all(r['until']>=time.time()+119 for r in resumed.routes)
        finally:await web.aclose()
    asyncio.run(run())


def test_transient_cooldown_is_separate_from_blocking_and_retry_after(tmp_path):
    async def run():
        p={**policy(),'failure_limit':1,'transient_cooldown_s':2}
        web=client(tmp_path,p);pool=FetchRoutePool(web,'source.example',pool_declaration(p))
        try:
            now=time.time();pool.record(pool.routes[0],'source.example','network_error',None,.1)
            assert now+2<=pool.routes[0]['until']<now+3
            pool.record(pool.routes[1],'source.example','http_error',503,.1,10)
            assert now+10<=pool.routes[1]['until']<now+11
            pool.record(pool.routes[0],'source.example','http_error',403,.1)
            assert now+60<=pool.routes[0]['until']<now+61
            pool.record(pool.routes[1],'source.example','rate_limited',429,.1,120)
            assert now+120<=pool.routes[1]['until']<now+121
        finally:await web.aclose()
    asyncio.run(run())


@pytest.mark.parametrize('extra',[
    {'transient_cooldown_s':0},{'transient_cooldown_s':True},
    {'transient_cooldown_s':float('inf')},{'transient_cooldown_s':86401},
    {'cooldown_wait_s':-1},{'cooldown_wait_s':True},
    {'cooldown_wait_s':float('nan')},{'cooldown_wait_s':3601},
    {'pool':[{'name':str(i),'proxy':'http://one.example:3128'} for i in range(129)]},
])
def test_invalid_cooldown_wait_policy(extra):
    with pytest.raises(ValueError):pool_declaration({**policy(),**extra})


def test_wildcard_host_cooldown_isolated_and_durable(tmp_path):
    p={**policy(),'health_scope':'host'}
    async def run(resumed=False):
        web=WebClient(cache_path=tmp_path/'hosts.sqlite',object_directory=tmp_path/'objects',
            host_interval_s=0,retries=0,fetch_proxy_routes={'*':p});calls=[]
        def handle(name,req):
            calls.append((name,req.url.host))
            return response(429,**{'retry-after':'120'}) if req.url.host=='limited.example' else response()
        for name in ('one','two'):
            web.route_clients[('*',name)]=httpx.AsyncClient(transport=httpx.MockTransport(lambda req,n=name:handle(n,req)))
        try:
            if not resumed:
                for _ in range(2):assert (await web._get('https://limited.example/a'))['status']=='rate_limited'
            before=len(calls)
            assert (await web._get('https://limited.example/b'))['status']=='route_unavailable'
            assert len(calls)==before
            assert (await web._get('https://healthy.example/a'))['status']=='ok'
            pool=web.fetch_route_pools['*']
            assert all(r['until']==0 for r in pool.routes)
            with web._db() as db:
                health=list(db.execute('select host,until from fetch_route_host_health'))
                assert len(health)==2 and all(host=='limited.example' and until>time.time()+119 for host,until in health)
            assert not web.fetch_session_pools
        finally:await web.aclose()
    asyncio.run(run());asyncio.run(run(True))


def test_host_health_failure_threshold_capacity_and_expiry(tmp_path):
    async def run():
        p=pool_declaration({**policy(),'health_scope':'host','max_host_health_entries':1})
        web=client(tmp_path);pool=FetchRoutePool(web,'*',p);r=pool.routes[0]
        try:
            pool.record(r,'first.example','network_error',None,.1)
            async with pool.lease(host='first.example') as selected:assert selected is r
            pool.record(r,'first.example','network_error',None,.1)
            with web._db() as db:
                until,failures=db.execute('select until,failures from fetch_route_host_health').fetchone()
                assert until>time.time() and failures==2
            with pytest.raises(ServiceStopped,match='fetch_host_health_capacity'):
                pool.record(r,'second.example','rate_limited',429,.1)
            with web._db() as db:
                assert db.execute('select count(*) from fetch_route_host_health').fetchone()[0]==1
                db.execute('update fetch_route_host_health set until=0,expires_at=0')
            pool.record(r,'second.example','rate_limited',429,.1)
            with web._db() as db:
                assert db.execute('select host from fetch_route_host_health').fetchone()[0]=='second.example'
        finally:await web.aclose()
    asyncio.run(run())


def test_redirect_into_cold_pool_is_a_url_result_not_action_stop(tmp_path):
    async def run():
        web=WebClient(cache_path=tmp_path/'redirect.sqlite',object_directory=tmp_path/'objects',
            host_interval_s=0,retries=0,fetch_proxy_routes={'*':{**policy(),'health_scope':'host'},
                                                         'local.example':'http://company.example:3128'})
        web.route_clients['local.example']=httpx.AsyncClient(transport=httpx.MockTransport(
            lambda req:response(302,location='https://cold.example/image')))
        pool=FetchRoutePool(web,'*',web.proxy_routes['*']);web.fetch_route_pools['*']=pool
        for route in pool.routes:pool.record(route,'cold.example','rate_limited',429,.1)
        try:
            result=await web._get('https://local.example/image')
            assert result['status']=='route_unavailable' and len(result['attempts'])==1
            assert web.metrics['http_hops']==1
        finally:await web.aclose()
    asyncio.run(run())


def test_rate_limit_rotation_uses_other_exit_immediately_and_scopes_retry_after(tmp_path):
    async def run():
        p={**policy(),'health_scope':'host','rotate_on_rate_limit':True}
        web=WebClient(cache_path=tmp_path/'rotate.sqlite',object_directory=tmp_path/'objects',
            host_interval_s=0,retries=1,retry_delay_s=30,fetch_proxy_routes={'*':p});calls=[]
        def handle(name,req):
            calls.append((name,req.url.host))
            return response(429,**{'retry-after':'120'}) if name=='one' and req.url.host=='limited.example' else response()
        for name in ('one','two'):
            web.route_clients[('*',name)]=httpx.AsyncClient(transport=httpx.MockTransport(lambda req,n=name:handle(n,req)))
        try:
            result=await asyncio.wait_for(web._get('https://limited.example/a'),.5)
            assert result['status']=='ok' and calls==[('one','limited.example'),('two','limited.example')]
            assert result['attempts'][0]['retry_after_s']==120 and len(result['attempts'])==2
            assert (await web._get('https://healthy.example/b'))['status']=='ok'
            assert calls[-1]==('one','healthy.example')
            with web._db() as db:
                until=db.execute('select until from fetch_route_host_health where host=?',('limited.example',)).fetchone()[0]
                assert until>time.time()+119
        finally:await web.aclose()
    asyncio.run(run())


def test_rotation_exhausts_only_limited_host_and_keeps_other_host_running(tmp_path):
    async def run():
        p={**policy(),'health_scope':'host','rotate_on_rate_limit':True}
        web=WebClient(cache_path=tmp_path/'all-cold.sqlite',object_directory=tmp_path/'objects',
            host_interval_s=0,retries=5,fetch_proxy_routes={'*':p});calls=[]
        def handle(name,req):
            calls.append((name,req.url.host))
            return response(429,**{'retry-after':'120'}) if req.url.host=='limited.example' else response()
        for name in ('one','two'):
            web.route_clients[('*',name)]=httpx.AsyncClient(transport=httpx.MockTransport(lambda req,n=name:handle(n,req)))
        try:
            result=await asyncio.wait_for(web._get('https://limited.example/a'),.5)
            assert result['status']=='route_unavailable'
            assert calls==[('one','limited.example'),('two','limited.example')]
            assert [r['status'] for r in result['attempts']]==['rate_limited','rate_limited','route_unavailable']
            assert (await web._get('https://healthy.example/a'))['status']=='ok'
            assert len(calls)==3 and not web.fetch_session_pools
        finally:await web.aclose()
    asyncio.run(run())


def test_default_static_pool_routes_unknown_hosts_and_redirects(tmp_path):
    async def run():
        web=WebClient(cache_path=tmp_path/'default.sqlite',object_directory=tmp_path/'objects',
            retries=0,host_interval_s=0,fetch_proxy_routes={'*':policy(),'local.example':None})
        calls=[]
        def handle(name,request):
            calls.append((name,request.url.host))
            return response(302,location='https://second.example/img') if request.url.path=='/redirect' else response()
        for name in ('one','two'):
            web.route_clients[('*',name)]=httpx.AsyncClient(transport=httpx.MockTransport(lambda req,n=name:handle(n,req)))
        web.route_clients['local.example']=httpx.AsyncClient(transport=httpx.MockTransport(lambda req:handle('direct',req)))
        try:
            assert web.route_for('https://unknown.example/a')=='*'
            assert web.route_for('https://sub.local.example/a')=='local.example'
            assert (await web._get('https://first.example/redirect'))['status']=='ok'
            assert (await web._get('https://local.example/a'))['status']=='ok'
            assert calls==[('one','first.example'),('two','second.example'),('direct','local.example')]
            assert web.proxy_identity('https://first.example/a')['domain']=='*'
        finally:await web.aclose()
    asyncio.run(run())


def test_configurable_route_parallelism_keeps_pacing_and_releases_cancellation(tmp_path):
    async def run():
        p=policy(.02);p['pool']=p['pool'][:1];p['pool'][0]['concurrency']=2
        web=client(tmp_path,p,retries=0,timeout_s=1,host_concurrency=8)
        active=peak=0;starts=[];release=asyncio.Event()
        async def handle(req):
            nonlocal active,peak
            active+=1;peak=max(peak,active);starts.append(time.monotonic())
            try:await release.wait();return response()
            finally:active-=1
        web.route_clients[('source.example','one')]=httpx.AsyncClient(transport=httpx.MockTransport(handle))
        tasks=[asyncio.create_task(web._get('https://source.example/'+str(i))) for i in range(5)]
        try:
            async with asyncio.timeout(2):
                while len(starts)<2:await asyncio.sleep(.005)
            await asyncio.sleep(.03)
            assert peak==2 and len(starts)==2 and starts[1]-starts[0]>=.018
            tasks[0].cancel()
            with pytest.raises(asyncio.CancelledError):await tasks[0]
            async with asyncio.timeout(2):
                while len(starts)<3:await asyncio.sleep(.005)
            release.set()
            assert all(v['status']=='ok' for v in await asyncio.gather(*tasks[1:]))
            pool=web.fetch_route_pools['source.example']
            assert pool.routes[0]['busy']==0 and pool.snapshot_metrics()['one']['peak_active']==2
        finally:
            release.set()
            for task in tasks:
                if not task.done():task.cancel()
            await asyncio.gather(*tasks,return_exceptions=True);await web.aclose()
    asyncio.run(run())


def test_parallel_route_waiter_rechecks_cooldown_before_sending(tmp_path):
    async def run():
        p=policy(.1);p['pool']=p['pool'][:1];p['pool'][0]['concurrency']=2
        web=client(tmp_path,p,retries=0,host_concurrency=4);calls=[]
        async def handle(req):
            calls.append(str(req.url));await asyncio.sleep(.02);return response(429)
        web.route_clients[('source.example','one')]=httpx.AsyncClient(transport=httpx.MockTransport(handle))
        try:
            results=await asyncio.gather(web._get('https://source.example/a'),web._get('https://source.example/b'),return_exceptions=True)
            assert len(calls)==1 and {v['status'] for v in results}=={'rate_limited','route_unavailable'}
            assert not web.fetch_route_pools['source.example'].routes[0]['busy']
        finally:await web.aclose()
    asyncio.run(run())


def test_route_concurrency_is_bounded_and_not_receipt_identity(tmp_path):
    web=client(tmp_path);before=web.proxy_identity('https://source.example/a')
    web.proxy_routes['source.example']['pool'][0]['concurrency']=8
    assert web.proxy_identity('https://source.example/a')==before
    for n in [0,65,True,1.5]:
        p=policy();p['pool'][0]['concurrency']=n
        with pytest.raises(ValueError,match='concurrency'):pool_declaration(p)
