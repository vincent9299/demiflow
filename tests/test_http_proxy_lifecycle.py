"""Real proxy sockets: failed TLS setup must not exhaust reusable capacity."""
import asyncio
import pytest

from demiflow.collect.connection_manager import ConnectionManager
import httpx


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
    manager=ConnectionManager()
    client=manager.client('proxy',proxy=proxy,capacity=2,keepalive=0,
        timeout=httpx.Timeout(.3,connect=.15),headers={})
    try:
        for host in ('failed-one.example','failed-two.example'):
            with pytest.raises(httpx.TransportError):await client.get('https://'+host+'/image')
        result=await client.get('http://healthy.example/image')
        assert result.status_code==200 and result.content==b'body'
        assert methods==[b'CONNECT',b'CONNECT',b'GET']
    finally:
        await manager.aclose();server.close();await server.wait_closed()
        await asyncio.gather(*tuple(handlers),return_exceptions=True)
