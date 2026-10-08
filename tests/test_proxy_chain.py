"""Generic proxy chaining, isolation, lifecycle, and credential-safe identity."""
import asyncio
import base64
import json
from urllib.parse import urlsplit,unquote
import pytest
from demiflow.collect.proxy import ProxyPool,proxy_declaration,proxy_routes
from demiflow.collect.native_search import SearchConfig
from demiflow.collect.native_search.runtime import NativeSearchSession
from demiflow.collect.web import WebClient


def test_proxy_policy_declarations_are_lazy_and_secret_only(monkeypatch):
    monkeypatch.delenv('PROXY_CHAIN_TEST',raising=False)
    declaration={'chain':['http://proxy.example:3128',{'secret_env':'PROXY_CHAIN_TEST'}],'allowed_domains':['Target.Example']}
    cfg=SearchConfig.from_mapping({'engines':['google'],'proxy':declaration})
    assert cfg.snapshot()['proxy']['chain'][1]=={'secret_env':'PROXY_CHAIN_TEST'}
    assert cfg.snapshot()['proxy']['allowed_domains']==['target.example']
    assert proxy_routes({'TARGET.example':{'secret_env':'PROXY_CHAIN_TEST'}})['target.example'].env=='PROXY_CHAIN_TEST'
    for bad in ['http://user:password@proxy.example',{'chain':['http://a'],'allowed_domains':['target.example']},
                {'chain':['http://a','http://b'],'allowed_domains':[]}]:
        with pytest.raises(ValueError):proxy_declaration(bad)


async def read_head(reader):
    return await reader.readuntil(b'\r\n\r\n')


async def relay_request(url, destination, *, auth=True):
    p=urlsplit(url);reader,writer=await asyncio.open_connection(p.hostname,p.port)
    value=base64.b64encode((unquote(p.username)+':'+unquote(p.password)).encode()).decode()
    header=('Proxy-Authorization: Basic '+value+'\r\n') if auth else ''
    writer.write(('CONNECT '+destination+' HTTP/1.1\r\nHost: '+destination+'\r\n'+header+'\r\n').encode());await writer.drain()
    return reader,writer,await read_head(reader)


def test_chain_forwards_auth_only_to_proxy_and_closes_tunnels():
    async def run():
        received=[];tasks=set()
        async def upstream(reader,writer):
            task=asyncio.current_task();tasks.add(task)
            try:
                received.append(await read_head(reader));writer.write(b'HTTP/1.1 200 OK\r\n\r\n');await writer.drain()
                received.append(await read_head(reader));writer.write(b'HTTP/1.1 200 OK\r\n\r\n');await writer.drain()
                payload=await reader.readexactly(4);writer.write(payload.upper());await writer.drain()
                await reader.read()
            finally:
                writer.close();await writer.wait_closed();tasks.discard(task)
        server=await asyncio.start_server(upstream,'127.0.0.1',0)
        port=server.sockets[0].getsockname()[1]
        pool=ProxyPool(timeout_s=2,max_connections=1)
        try:
            url=await pool.transport({'chain':[f'http://127.0.0.1:{port}','http://sample:example_password@next.example:8080'],'allowed_domains':['target.example']})
            r,w,status=await relay_request(url,'target.example:443')
            assert b'200' in status
            w.write(b'ping');await w.drain();assert await r.readexactly(4)==b'PING'
            assert b'CONNECT next.example:8080' in received[0] and b'Proxy-Authorization' not in received[0]
            assert b'CONNECT target.example:443' in received[1]
            assert base64.b64encode(b'sample:example_password') in received[1]
            w.close();await w.wait_closed()
            await pool.aclose()
            assert not any(relay.tasks for relay in pool.relays.values())
            assert pool.snapshot_metrics()['tunnels']==1
        finally:
            await pool.aclose();server.close();await server.wait_closed()
            await asyncio.gather(*tasks,return_exceptions=True)
    asyncio.run(run())


def test_relay_is_authenticated_and_destination_scoped():
    async def run():
        pool=ProxyPool(timeout_s=1)
        try:
            url=await pool.transport({'chain':['http://127.0.0.1:1','http://unused.example:8080'],'allowed_domains':['target.example']})
            for target,auth,code in [('target.example:443',False,b'407'),('other.example:443',True,b'403'),('target.example.evil:443',True,b'403'),('target.example:80',True,b'403')]:
                r,w,status=await relay_request(url,target,auth=auth)
                assert code in status
                if not auth: assert b'Proxy-Authenticate: Basic realm="demiflow"\r\n' in status
                w.close();await w.wait_closed()
            assert pool.snapshot_metrics().get('tunnels',0)==0
        finally:await pool.aclose()
    asyncio.run(run())


def test_route_matching_and_search_identity_survive_new_relay_port(tmp_path,monkeypatch):
    monkeypatch.setenv('PROXY_CHAIN_TEST','http://sample:example_password@second.example:8080')
    policy={'chain':['http://127.0.0.1:1',{'secret_env':'PROXY_CHAIN_TEST'}],'allowed_domains':['target.example']}
    web=WebClient(cache_path=tmp_path/'fetch.sqlite',object_directory=tmp_path/'objects',fetch_proxy_url='http://company.example:3128',
                  fetch_proxy_routes={'target.example':policy,'special.target.example':'http://special.example:3128'})
    assert web.route_for('https://sub.target.example/a')=='target.example'
    assert web.route_for('https://special.target.example/a')=='special.target.example'
    assert web.route_for('https://target.example.evil/a') is None
    assert web.proxy_identity('https://other.example/a')=='http://company.example:3128'
    identity=json.dumps(web.proxy_identity('https://target.example/a'))
    assert 'example_password' not in identity and 'secret_env' in identity
    cfg=SearchConfig.from_mapping({'engines':['google'],'proxy':policy})
    async def run():
        values=[]
        for _ in range(2):
            session=NativeSearchSession(cache_path=tmp_path/'search.sqlite',config=cfg)
            try:
                await session.initialize();values.append((session.profile,session.worker_config(('default','en'))['outgoing']['proxies']['all://']))
            finally:await session.aclose()
        return values
    a,b=asyncio.run(run())
    assert a[0]==b[0] and a[1]!=b[1]
    assert 'example_password' not in (tmp_path/'search.sqlite').read_bytes().decode('latin1')


def test_explicit_all_destinations_proxy_is_separate_from_domain_blocking():
    from demiflow.collect.proxy import ConnectRelay
    from demiflow.collect.web import normalized_blocked_domains
    declaration=proxy_declaration({'chain':['http://upstream.example:3128','http://static.example:8080'],
                                   'allowed_domains':['*']})
    relay=ConnectRelay(declaration)
    assert relay.allowed('newly-discovered.example')
    with pytest.raises(ValueError):normalized_blocked_domains(['*'])


@pytest.mark.parametrize('method',['GET','HEAD'])
def test_plain_http_uses_same_chain_and_last_proxy_credentials(method):
    import httpx
    async def run():
        received=[];tasks=set()
        async def upstream(reader,writer):
            task=asyncio.current_task();tasks.add(task)
            try:
                received.append(await read_head(reader))
                writer.write(b'HTTP/1.1 200 Connection Established\r\n\r\n');await writer.drain()
                received.append(await read_head(reader))
                writer.write(b'HTTP/1.1 200 OK\r\nContent-Length: 4\r\nConnection: close\r\n\r\n')
                if method=='GET':writer.write(b'foto')
                await writer.drain()
            finally:
                writer.close();await writer.wait_closed();tasks.discard(task)
        server=await asyncio.start_server(upstream,'127.0.0.1',0)
        port=server.sockets[0].getsockname()[1];pool=ProxyPool(timeout_s=2,max_connections=1)
        try:
            url=await pool.transport({'chain':[f'http://127.0.0.1:{port}',
                'http://last:proxy_password@last.example:8080'],'allowed_domains':['images.example']})
            async with httpx.AsyncClient(proxy=url,trust_env=False,timeout=2) as client:
                response=await client.request(method,'http://images.example/original?q=%E5%9B%BE',
                    headers={'Connection':'keep-alive, x-hop','x-hop':'discard','X-Material':'retain'})
            assert response.status_code==200
            assert response.content==(b'foto' if method=='GET' else b'')
            assert received[0].startswith(b'CONNECT last.example:8080 HTTP/1.1')
            forwarded=received[1].lower()
            assert forwarded.startswith((method+' http://images.example/original?q=%e5%9b%be HTTP/1.1').lower().encode())
            assert b'host: images.example\r\n' in forwarded
            assert b'connection: close\r\n' in forwarded and b'x-hop:' not in forwarded
            assert b'x-material: retain\r\n' in forwarded
            assert base64.b64encode(b'last:proxy_password') in received[1]
            assert pool.relays[next(iter(pool.relays))].auth.encode() not in received[1]
            assert pool.snapshot_metrics()['http_forwards']==1
        finally:
            await pool.aclose();server.close();await server.wait_closed()
            await asyncio.gather(*tasks,return_exceptions=True)
        assert not any(relay.tasks for relay in pool.relays.values())
    asyncio.run(run())


def test_plain_http_checks_destination_and_never_forwards_pipelined_request():
    async def run():
        heads=[];tails=[];tasks=set()
        async def upstream(reader,writer):
            task=asyncio.current_task();tasks.add(task)
            try:
                await read_head(reader)
                writer.write(b'HTTP/1.1 200 Connection Established\r\n\r\n');await writer.drain()
                heads.append(await read_head(reader))
                writer.write(b'HTTP/1.1 200 OK\r\nContent-Length: 0\r\nConnection: close\r\n\r\n')
                await writer.drain()
                try:tails.append(await asyncio.wait_for(reader.read(4096),.05))
                except asyncio.TimeoutError:tails.append(b'')
            finally:
                writer.close();await writer.wait_closed();tasks.discard(task)
        server=await asyncio.start_server(upstream,'127.0.0.1',0)
        port=server.sockets[0].getsockname()[1];pool=ProxyPool(timeout_s=1,max_connections=1)
        try:
            url=await pool.transport({'chain':[f'http://127.0.0.1:{port}','http://last.example:8080'],
                'allowed_domains':['images.example']})
            parsed=urlsplit(url)
            auth=base64.b64encode((unquote(parsed.username)+':'+unquote(parsed.password)).encode())
            for target,code in [('http://evil.example/a',b'403'),('http://images.example:8080/a',b'403'),
                                ('http://user@images.example/a',b'403'),('https://images.example/a',b'403'),
                                ('http://images.example/a',b'200')]:
                reader,writer=await asyncio.open_connection(parsed.hostname,parsed.port)
                writer.write(b'GET '+target.encode()+b' HTTP/1.1\r\nHost: images.example\r\nProxy-Authorization: Basic '+auth+
                    b'\r\n\r\nGET http://evil.example/escape HTTP/1.1\r\nHost: evil.example\r\n\r\n')
                await writer.drain();assert code in await read_head(reader)
                await reader.read();writer.close();await writer.wait_closed()
            assert len(heads)==1 and tails==[b'']
            assert pool.snapshot_metrics()['destination_excluded']==4
        finally:
            await pool.aclose();server.close();await server.wait_closed()
            await asyncio.gather(*tasks,return_exceptions=True)
    asyncio.run(run())
