"""Verified, bounded HTTP/COS representation streaming into a native writer."""
from email.utils import parsedate_to_datetime
import ipaddress
import logging
from contextvars import ContextVar
import ssl
import time
from urllib.parse import quote,unquote, urljoin, urlsplit

import httpx

from .connection_manager import ConnectionManager, ConnectionPolicy
from .cosio import COSCreds, COSIO
from .raw_contract import origin, wire_quantum
from .web import URLPolicy
from demiflow.execution.contracts.recovery_contract import RecoveryContractError
from demiflow.services.request_limits import RequestGate
from ..secrets import resolve, Secret


class Outcome(Exception):
    def __init__(self, reason, *, retryable=False, retry_after=0, partial=0):
        self.reason, self.retryable, self.retry_after, self.partial = reason, retryable, retry_after, partial
        super().__init__(reason)


def peer_certificate_error(error):
    seen = set()
    pending = [error]
    while pending and len(seen) < 32:
        exc = pending.pop()
        if id(exc) in seen:
            continue
        seen.add(id(exc))
        if isinstance(exc, ssl.SSLCertVerificationError):
            return True
        pending.extend(e for e in (exc.__cause__,exc.__context__) if e is not None)
    return False


_RAW_HTTP_LOGS = ContextVar('demiflow_raw_http_logs',default=False)


class _RawHTTPLogFilter(logging.Filter):
    def filter(self,record):
        # HTTPX logs full request URLs at INFO; HTTPcore DEBUG includes response
        # headers such as Location. Keep those locators within transport memory.
        # Other operations in the process retain their normal logging behavior.
        return not _RAW_HTTP_LOGS.get()


_RAW_LOG_FILTER = _RawHTTPLogFilter()


def _protect_transport_logs():
    for name in ('httpx','httpcore.http11','httpcore.http2','httpcore.connection','httpcore.proxy','httpcore.socks'):
        logger=logging.getLogger(name)
        if _RAW_LOG_FILTER not in logger.filters:logger.addFilter(_RAW_LOG_FILTER)


class RawBodyReader:
    """A short-read view over HTTPcore's bounded raw receive window.

    The receive window may read ahead at most 64 KiB. The fetch loop requests
    min(chunk, remaining+1) from this view; the sentinel never reaches the file.
    """
    def __init__(self,response,grant,account):
        self.iterator=response.aiter_raw().__aiter__()
        self.grant,self.account=grant,account
        self.buffer=b'';self.offset=0

    async def read(self,maximum):
        if not 1<=maximum<=1024**2:raise RecoveryContractError('Invalid bounded raw read')
        if self.offset==len(self.buffer):
            await self.grant(65536)
            try:self.buffer=await self.iterator.__anext__()
            except StopAsyncIteration:return b''
            self.offset=0
            self.account(len(self.buffer))
            if len(self.buffer)>65536:raise RecoveryContractError('Raw transport receive window exceeds 64 KiB')
        end=min(len(self.buffer),self.offset+maximum)
        value=self.buffer[self.offset:end]
        self.offset=end
        return value


class Transfer:
    def __init__(self, policy, concurrency, io):
        self.policy, self.concurrency, self.io = policy, concurrency, io
        self.manager = None
        self.gates = {}
        self.url_policy = URLPolicy()

    def open(self):
        p = self.policy
        # Invalid local CA paths/configuration fail before any request/receipt.
        _protect_transport_logs()
        if p.http.ca_file is None:self.tls=ssl.create_default_context()
        else:
            import os,stat
            fd=os.open(p.http.ca_file,os.O_RDONLY|os.O_NOFOLLOW|os.O_NONBLOCK)
            with os.fdopen(fd,'rb') as certificate:
                size=os.fstat(certificate.fileno())
                if not stat.S_ISREG(size.st_mode) or size.st_size>1024**2:
                    raise ValueError('Local CA bundle exceeds its regular-file byte budget')
                encoded=certificate.read(1024**2+1)
                if len(encoded)>1024**2:raise ValueError('Local CA bundle exceeds its byte budget')
            self.tls=ssl.create_default_context(cadata=encoded.decode('ascii') if b'-----BEGIN CERTIFICATE-----' in encoded else encoded)
        self.credential_headers = {k for k,v in p.http.headers.items() if type(v) is Secret}
        self.headers = resolve(p.http.headers)
        for value in self.headers.values():
            if type(value) is not str or len(value.encode('utf8')) > 4096 or any(ord(c)<32 or ord(c)>126 for c in value):
                raise ValueError('Invalid resolved header')
        self.proxy = resolve(p.http.proxy)
        if self.proxy is not None:
            try:
                proxy = urlsplit(self.proxy)
                valid = len(self.proxy)<=4096 and proxy.scheme in {'http','https','socks5','socks5h'} and bool(proxy.hostname) and not proxy.fragment
                proxy.port
            except (ValueError,TypeError):
                valid = False
            if not valid:
                raise ValueError('Invalid declared proxy resource')
        self.signers = {}
        for s in p.cos:
            self.signers[s.bucket] = (s,COSIO(COSCreds(s.secret_id.resolve(),s.secret_key.resolve()),
                                                    urlsplit(s.endpoint).netloc,retries=1),
                                      s.session_token.resolve() if s.session_token else None)
        self.manager = ConnectionManager(ConnectionPolicy(max_clients=1,max_total_connections=self.concurrency,
                                              max_connections_per_client=self.concurrency))
        self.client = self.manager.client('raw',proxy=self.proxy,capacity=self.concurrency,keepalive=self.concurrency,
            timeout=httpx.Timeout(connect=p.connect_timeout_s,read=p.read_idle_timeout_s,
                                  write=p.connect_timeout_s,pool=p.connect_timeout_s),
            headers={'accept-encoding':'identity'},verify=self.tls)

    async def aclose(self):
        if self.manager is not None:
            await self.manager.aclose()
        self.headers = {}
        self.signers = {}

    def _gate(self, url):
        host = origin(url)
        if host not in self.gates:
            if len(self.gates) >= self.policy.max_hosts:
                raise RecoveryContractError('Fetch host resource count exceeded')
            self.gates[host] = RequestGate(self.policy.per_host_concurrency)
        return self.gates[host]

    def _url(self, request):
        uri = request['uri']
        headers = {'accept-encoding':'identity'}
        p = urlsplit(uri)
        source_version = request.get('source_version',{})
        if p.scheme == 'cos':
            if p.netloc not in self.signers:
                raise ValueError('COS bucket has no declared credential resource')
            resource, signer, token = self.signers[p.netloc]
            params = {}
            if source_version.get('kind') == 'cos_version_id':
                params['versionId'] = source_version['value']
            path = quote(unquote(p.path or '/'),safe='/~')
            # The signer, URL and parameter declaration use one exact set.
            headers['authorization'] = signer._sign('GET',path,params)
            if token:
                headers['x-cos-security-token'] = token
            uri = resource.endpoint + path
            if params:
                uri += '?' + '&'.join(quote(k,safe='')+'='+quote(v,safe='') for k,v in sorted(params.items()))
        else:
            initial = origin(uri)
            headers.update({k:v for k,v in self.headers.items() if self.policy.http.credential_origin in (None,initial)})
        if source_version.get('kind') == 'http_etag':
            headers['if-match'] = source_version['value']
        return uri,headers

    def _screen(self, uri):
        if self.url_policy.exclusion_reason(uri) is not None:
            raise Outcome('invalid_redirect')
        # Literal private targets need an explicit resource declaration. DNS and
        # access permissions remain subject to native platform authorization.
        if not self.policy.http.allow_private:
            host = urlsplit(uri).hostname
            try:
                address = ipaddress.ip_address(host)
            except ValueError:
                if host.lower() in {'localhost','localhost.localdomain'}:
                    raise ValueError('Private HTTP origin requires allow_private') from None
            else:
                if not address.is_global:
                    raise ValueError('Private HTTP origin requires allow_private')

    async def attempt(self, request, writer, budget, attempt):
        p = self.policy
        url, base_headers = self._url(request)
        initial_origin = origin(url)
        quantum = wire_quantum(writer.maximum,p.redirects)
        grant_no = granted = used = requests = 0
        partial = 0
        async def grant(extra, request_count):
            nonlocal grant_no,granted,requests
            amount = granted
            if used + extra > amount:
                amount = ((used+extra+quantum-1)//quantum)*quantum
            if amount != granted or request_count != requests:
                grant_no = await self.io.run(budget.extend,attempt,grant_no,amount,request_count)
                granted,requests = amount,request_count
        log_token=_RAW_HTTP_LOGS.set(True)
        try:
            for hops in range(p.redirects+1):
                try:self._screen(url)
                except ValueError:
                    if hops:raise Outcome('invalid_redirect') from None
                    raise
                hop_origin = origin(url)
                headers = dict(base_headers)
                if hop_origin != initial_origin:
                    headers = {k:v for k,v in headers.items() if k not in self.credential_headers | {'authorization','cookie','proxy-authorization','x-cos-security-token'}}
                # No cookie jar leakage across hosts or successive task rows.
                try:
                    req = self.client.build_request('GET',url,headers=headers)
                except (httpx.InvalidURL,UnicodeError,ValueError):
                    raise Outcome('invalid_redirect') from None
                req.headers.pop('cookie',None)
                if hop_origin == initial_origin and 'cookie' in headers:
                    req.headers['cookie'] = headers['cookie']
                async with self._gate(url).enter():
                    await grant(0,requests+1)
                    response = await self.client.send(req,stream=True,follow_redirects=False)
                    self.client.cookies.clear()
                    try:
                        status = response.status_code
                        if status in {301,302,303,307,308}:
                            location = response.headers.get('location')
                            if hops == p.redirects or not location or len(location.encode('utf8')) > p.max_request_bytes:
                                raise Outcome('invalid_redirect')
                            try:
                                target = urljoin(url,location)
                                if len(target.encode('utf8')) > p.max_request_bytes:
                                    raise Outcome('invalid_redirect')
                                parsed = urlsplit(target)
                                target_origin = origin(target)
                            except ValueError:
                                raise Outcome('invalid_redirect') from None
                            if parsed.fragment or url.startswith('https://') and not target.startswith('https://'):
                                raise Outcome('invalid_redirect')
                            if target_origin != initial_origin and target_origin not in p.http.redirect_origins:
                                raise Outcome('redirect_origin_denied')
                            url = target
                            continue
                        if status in {401,403}:
                            raise Outcome('ephemeral_access_denied' if hops else 'access_denied')
                        if status in {404,410}:
                            raise Outcome('not_found')
                        if status == 412:
                            raise Outcome('source_version_mismatch')
                        if status in {408,425,429} or 500 <= status <= 599:
                            delay = 0
                            retry = response.headers.get('retry-after','')
                            if len(retry) <= 128:
                                try:
                                    delay = max(0,float(retry))
                                except ValueError:
                                    try:
                                        delay = max(0,parsedate_to_datetime(retry).timestamp()-time.time())
                                    except (ValueError,TypeError,OverflowError):
                                        pass
                            raise Outcome('remote_transient',retryable=True,retry_after=min(delay,p.max_retry_after_s))
                        if 400 <= status <= 499:
                            raise Outcome('http_client_error')
                        if status != 200:
                            raise Outcome('unsupported_response')
                        version = request.get('source_version',{})
                        if version.get('kind') == 'http_etag' and response.headers.get('etag') != version['value']:
                            raise Outcome('source_version_mismatch')
                        if version.get('kind') == 'cos_version_id' and response.headers.get('x-cos-version-id') != version['value']:
                            raise Outcome('source_version_mismatch')
                        cl = response.headers.get('content-length')
                        if cl is not None:
                            if not cl.isascii() or not cl.isdecimal() or len(cl)>20:
                                raise Outcome('invalid_content_length')
                            cl = int(cl)
                            if cl > writer.maximum:
                                raise Outcome('size_mismatch' if 'expected_size' in request else 'object_too_large')
                            if 'expected_size' in request and cl != request['expected_size']:
                                raise Outcome('size_mismatch')
                            writer.maximum = cl
                        def account(amount):
                            nonlocal used
                            used+=amount
                        reader=RawBodyReader(response,lambda amount:grant(amount,requests),account)
                        while True:
                            chunk=await reader.read(min(p.chunk_bytes,writer.maximum-writer.size+1))
                            if not chunk:break
                            if writer.size + len(chunk) > writer.maximum:
                                remaining_bytes = writer.maximum-writer.size
                                if remaining_bytes:
                                    await self.io.run(writer.write,chunk[:remaining_bytes])
                                raise Outcome('size_mismatch' if 'expected_size' in request else 'object_too_large',partial=writer.size)
                            await self.io.run(writer.write,chunk)
                            partial = writer.size
                        if cl is not None and writer.size != cl or 'expected_size' in request and writer.size != request['expected_size']:
                            raise Outcome('size_mismatch',partial=writer.size)
                        if 'expected_sha256' in request and writer.digest.hexdigest() != request['expected_sha256']:
                            raise Outcome('sha256_mismatch',partial=writer.size)
                        def header(name):
                            value = response.headers.get(name)
                            return value if value is not None and len(value.encode('utf8')) <= 1024 else None
                        return dict(uri=request['uri'],final_uri=request['uri'] if hops == 0 else None,
                            final_origin=hop_origin,locator_redacted=bool(hops),redirect_hops=hops,
                            http_status=status,content_type=header('content-type'),content_encoding=header('content-encoding'),
                            etag=header('etag'),cos_version_id=header('x-cos-version-id'),
                            source_version=request.get('source_version'),
                            pinned=bool(request.get('expected_sha256') or version.get('kind') in {'http_etag','cos_version_id'}))
                    finally:
                        await response.aclose()
        except httpx.ConnectError as exc:
            if peer_certificate_error(exc):
                raise Outcome('tls_peer_rejected',partial=partial) from None
            raise Outcome('connect_error',retryable=True,partial=partial) from None
        except (httpx.TimeoutException,httpx.NetworkError,httpx.RemoteProtocolError):
            raise Outcome('transport_interrupted',retryable=True,partial=partial) from None
        finally:
            # Response close and blocking writer jobs have drained before refund.
            try:await self.io.run(budget.finish_attempt,attempt,used)
            finally:_RAW_HTTP_LOGS.reset(log_token)
