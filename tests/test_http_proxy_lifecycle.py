"""Real proxy sockets: failed TLS setup must not exhaust reusable capacity."""
import asyncio
import pytest

from demiflow.collect.web import WebClient


@pytest.mark.parametrize('failure',['invalid_tls','handshake_timeout'])
async def test_failed_connect_tls_releases_capacity_for_other_origins(tmp_path,failure):
    handlers=set();methods=[]
    async def serve(reader,writer):
        task=asyncio.current_task();handlers.add(task)
        try:
            async with asyncio.timeout(2):
                head=await reader.readuntil(b'\r\n\r\n')
                method=head.split(b' ',1)[0];methods.append(method)
                if method==b'CONNECT':
                    writer.write(b'HTTP/1.1 200 Connection Established\r\n\r\n')
                    if failure=='invalid_tls':writer.write(b'invalid TLS bytes')
                    else:
                        await writer.drain()
                        await reader.read(8192)  # bounded ClientHello
                        await reader.read(8192)  # wait for handshake cancellation/EOF
                else:
                    writer.write(b'HTTP/1.1 200 OK\r\nContent-Length: 4\r\nConnection: close\r\n\r\nbody')
                await writer.drain()
        except (OSError,asyncio.IncompleteReadError,TimeoutError):pass
        finally:
            writer.close();await writer.wait_closed();handlers.discard(task)
    server=await asyncio.start_server(serve,'127.0.0.1',0)
    proxy=f'http://127.0.0.1:{server.sockets[0].getsockname()[1]}'
    web=WebClient(cache_path=tmp_path/'requests.sqlite',object_directory=tmp_path/'objects',
        retries=0,timeout_s=.3,connect_timeout_s=.15,host_interval_s=0,
        fetch_proxy_routes={'*':{'pool':[{'name':'local','proxy':proxy,'interval_s':0,
            'concurrency':2,'reuse_connections':False}], 'health_scope':'host'}})
    try:
        for host in ('failed-one.example','failed-two.example'):
            result=await web._get('https://'+host+'/image')
            assert result['status']=='network_error'
        result=await web._get('http://healthy.example/image')
        assert result['status']=='ok' and result['body']==b'body'
        assert methods==[b'CONNECT',b'CONNECT',b'GET']
    finally:
        await web.aclose();server.close();await server.wait_closed()
        await asyncio.gather(*tuple(handlers),return_exceptions=True)
