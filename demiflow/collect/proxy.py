"""Run-owned proxy chains with explicit destinations and Secret references.

HTTPS uses CONNECT without terminating target TLS. Plain HTTP GET/HEAD travels
through the same chain, with a single request per connection. The relay never
logs authorization or chooses proxies outside the declared chain.
"""
from __future__ import annotations
import asyncio
import base64
import json
import secrets
from collections import Counter
from urllib.parse import urlsplit, unquote, quote


def proxy_declaration(value):
    from .native_search.config import Secret
    if value is None or isinstance(value, Secret):
        return value
    if isinstance(value, dict) and set(value)=={'secret_env'}:
        return Secret(value['secret_env'])
    if isinstance(value, str):
        parsed=urlsplit(value)
        if parsed.scheme not in {'http','https'} or not parsed.hostname or parsed.username or parsed.password:
            raise ValueError('Proxy URLs require HTTP(S); credentials must use Secret')
        return value
    if isinstance(value, dict) and set(value)=={'chain','allowed_domains'}:
        from .web import normalized_blocked_domains
        chain=value['chain']
        if not isinstance(chain,(list,tuple)) or not 2<=len(chain)<=4:
            raise ValueError('Proxy chain must declare 2 to 4 hops')
        hops=[proxy_declaration(item) for item in chain]
        if any(not isinstance(item,(str,Secret)) for item in hops):
            raise ValueError('Nested or empty proxy chains are unsupported')
        domains=(('*',) if value['allowed_domains'] == ['*'] else normalized_blocked_domains(value['allowed_domains']))
        if not domains:raise ValueError('Proxy chains require explicit allowed_domains')
        return {'chain':hops,'allowed_domains':list(domains)}
    raise ValueError('Invalid proxy declaration')


def proxy_routes(value):
    from .web import normalized_blocked_domains
    if value is None:return {}
    if not isinstance(value,dict):raise ValueError('fetch_proxy_routes must map domains to proxy declarations')
    return {normalized_blocked_domains([domain])[0]:proxy_declaration(route) for domain,route in value.items()}


class ConnectRelay:
    def __init__(self, declaration, *, timeout_s=30, max_connections=16):
        self.hops=[urlsplit(value) for value in declaration['chain']]
        if any(p.scheme!='http' or not p.hostname for p in self.hops):
            raise ValueError('CONNECT chains currently require HTTP proxy hops')
        self.domains=tuple(declaration['allowed_domains'])
        self.timeout_s=timeout_s
        self.slots=asyncio.Semaphore(max_connections)
        self.auth=base64.b64encode(('demiflow:'+secrets.token_urlsafe(32)).encode()).decode()
        self.server=None;self.tasks=set();self.metrics=Counter()

    async def start(self):
        self.server=await asyncio.start_server(self.handle,'127.0.0.1',0,limit=16384)
        port=self.server.sockets[0].getsockname()[1]
        token=base64.b64decode(self.auth).decode().split(':',1)[1]
        return 'http://demiflow:'+quote(token,safe='')+'@127.0.0.1:'+str(port)

    def allowed(self, hostname):
        hostname=hostname.rstrip('.').lower()
        return '*' in self.domains or any(hostname==domain or hostname.endswith('.'+domain) for domain in self.domains)

    async def read_head(self, reader):
        body=await reader.readuntil(b'\r\n\r\n')
        if len(body)>8192:raise ValueError('header_limit')
        lines=body.decode('latin1').split('\r\n')
        fields={}
        for line in lines[1:]:
            if ':' in line:
                key,value=line.split(':',1);fields[key.lower()]=value.strip()
        return lines[0],fields

    async def connect(self, writer, reader, proxy, authority):
        authorization=self.proxy_authorization(proxy)
        writer.write(('CONNECT '+authority+' HTTP/1.1\r\nHost: '+authority+'\r\n'+authorization+'\r\n').encode())
        await writer.drain()
        line,_=await self.read_head(reader)
        fields=line.split(' ',2)
        if len(fields)<2 or fields[1]!='200':
            code=fields[1] if len(fields)>1 and fields[1].isdigit() else 'invalid'
            raise ConnectionError('proxy_status_'+code)

    @staticmethod
    def proxy_authorization(proxy):
        if proxy.username is None:return ''
        pair=unquote(proxy.username)+':'+unquote(proxy.password or '')
        return 'Proxy-Authorization: Basic '+base64.b64encode(pair.encode()).decode()+'\r\n'

    def forward_head(self, parts, headers, target):
        # Only bodyless retrieval is supported. No subsequent client bytes are
        # forwarded, so a pipelined request cannot bypass destination checks.
        if headers.get('transfer-encoding') or headers.get('content-length','0')!='0':
            raise ValueError('HTTP_request_body_unsupported')
        removed={'proxy-authorization','proxy-connection','connection','keep-alive',
                 'te','trailer','transfer-encoding','upgrade','host','content-length'}
        removed.update(v.strip().lower() for v in headers.get('connection','').split(','))
        fields=''.join(k+': '+v+'\r\n' for k,v in headers.items() if k not in removed)
        # The last hop is an HTTP forward proxy: use absolute-form with its
        # own credentials, never the loopback relay's authentication header.
        return (f'{parts[0]} {parts[1]} HTTP/1.1\r\nHost: {target.netloc}\r\n'
                +fields+self.proxy_authorization(self.hops[-1])
                +'Connection: close\r\n\r\n').encode('latin1')

    async def pump(self, reader, writer):
        while True:
            async with asyncio.timeout(self.timeout_s):chunk=await reader.read(65536)
            if not chunk:return
            writer.write(chunk)
            async with asyncio.timeout(self.timeout_s):await writer.drain()

    async def handle(self, reader, writer):
        task=asyncio.current_task();self.tasks.add(task);upstream=None;tunnels=[];established=False
        try:
            async with self.slots:
                async with asyncio.timeout(self.timeout_s):
                    line,headers=await self.read_head(reader)
                    parts=line.split(' ')
                    if len(parts)!=3 or parts[0] not in {'CONNECT','GET','HEAD'}:
                        raise ValueError('unsupported_proxy_method')
                    if not secrets.compare_digest(headers.get('proxy-authorization',''),'Basic '+self.auth):
                        self.metrics['unauthorized']+=1
                        # Browsers discover the authentication scheme from this
                        # challenge; curl may already send credentials on CONNECT.
                        writer.write(b'HTTP/1.1 407 Proxy Authentication Required\r\nProxy-Authenticate: Basic realm="demiflow"\r\nContent-Length: 0\r\nConnection: close\r\n\r\n');await writer.drain();return
                    tunnel=parts[0]=='CONNECT'
                    target=urlsplit('//'+parts[1] if tunnel else parts[1])
                    permitted_transport=(target.port==443 if tunnel else
                        target.scheme=='http' and target.port in (None,80) and not target.fragment)
                    if not target.hostname or not permitted_transport or target.username or not self.allowed(target.hostname):
                        self.metrics['destination_excluded']+=1
                        writer.write(b'HTTP/1.1 403 Forbidden\r\nContent-Length: 0\r\n\r\n');await writer.drain();return
                    forward=None if tunnel else self.forward_head(parts,headers,target)
                    first=self.hops[0]
                    remote,upstream=await asyncio.open_connection(first.hostname,first.port or 80,limit=16384)
                    for i,proxy in enumerate(self.hops):
                        if not tunnel and i==len(self.hops)-1:break
                        destination=(self.hops[i+1].hostname+':'+str(self.hops[i+1].port or 80)
                                     if i+1<len(self.hops) else parts[1])
                        await self.connect(upstream,remote,proxy,destination)
                    if tunnel:
                        writer.write(b'HTTP/1.1 200 Connection Established\r\n\r\n');await writer.drain()
                        self.metrics['tunnels']+=1
                    else:
                        upstream.write(forward);await upstream.drain()
                        self.metrics['http_forwards']+=1
                    established=True
                if not tunnel:
                    await self.pump(remote,writer)
                    return
                tunnels=[asyncio.create_task(self.pump(reader,upstream)),asyncio.create_task(self.pump(remote,writer))]
                done,_=await asyncio.wait(tunnels,return_when=asyncio.FIRST_COMPLETED)
                for finished in done:finished.result()
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            self.metrics['error:'+type(exc).__name__]+=1
            # No upstream text or credentials are exposed to clients or logs.
            if not established:
                try:
                    writer.write(b'HTTP/1.1 502 Bad Gateway\r\nContent-Length: 0\r\n\r\n');await writer.drain()
                except Exception:pass
        finally:
            for pending in tunnels:pending.cancel()
            await asyncio.gather(*tunnels,return_exceptions=True)
            for stream in (upstream,writer):
                if stream:
                    stream.close()
                    try:await stream.wait_closed()
                    except (OSError,ConnectionError):pass
            self.tasks.discard(task)

    async def aclose(self):
        if self.server:
            self.server.close();await self.server.wait_closed()
        tasks=list(self.tasks)
        for task in tasks:task.cancel()
        await asyncio.gather(*tasks,return_exceptions=True)


class ProxyPool:
    def __init__(self, *, timeout_s=30, max_connections=16):
        self.options={'timeout_s':timeout_s,'max_connections':max_connections}
        self.relays={};self.urls={};self.lock=asyncio.Lock()

    async def transport(self, resolved):
        if not isinstance(resolved,dict):return resolved
        key=json.dumps(resolved,sort_keys=True)
        async with self.lock:
            if key not in self.urls:
                relay=ConnectRelay(resolved,**self.options)
                self.urls[key]=await relay.start();self.relays[key]=relay
            return self.urls[key]

    def snapshot_metrics(self):
        totals=Counter()
        for relay in self.relays.values():totals.update(relay.metrics)
        return dict(totals)

    async def aclose(self):
        await asyncio.gather(*(relay.aclose() for relay in self.relays.values()))
