"""Search HTTP transport plus independent URI/exclusion policy primitives.

Object acquisition belongs to Dataset.fetch. This module cannot download,
cache or parse documents/images. The retained cache stores search responses.
"""
from __future__ import annotations
import asyncio
import hashlib
import json
import random
import re
import sqlite3
import time
import zlib
from datetime import datetime, timezone
from contextlib import nullcontext
from email.utils import parsedate_to_datetime
from pathlib import Path
from urllib.parse import urlsplit, urlunsplit
import httpx
from demiflow.services.request_limits import RequestGate, ServiceStopped
from demiflow.execution.journal.run_telemetry import LatencySummary
from .connection_manager import ConnectionManager, ConnectionCapacityError


def normalized_url(url):
    try:
        if not isinstance(url,str) or any(ord(c)<32 or ord(c)==127 for c in url): return None
        p = urlsplit(url)
        if p.scheme not in {'http','https'} or not p.hostname or p.username or p.password:
            return None
        p.port
        return urlunsplit((p.scheme.lower(),p.netloc.lower(),p.path or '/',p.query,''))
    except (TypeError,ValueError): return None


def retry_after(value):
    if not value: return None
    try: return max(0.,float(value))
    except ValueError:
        try: return max(0., (parsedate_to_datetime(value)-datetime.now(timezone.utc)).total_seconds())
        except (TypeError,ValueError,OverflowError): return None


class RetrievalFailure(Exception):
    pass


def normalized_blocked_domains(domains):
    if domains is None:
        return ()
    if not isinstance(domains, (list, tuple)):
        raise ValueError('blocked_domains must be a list of host names')
    result = []
    for domain in domains:
        if not isinstance(domain, str) or not domain or domain != domain.strip():
            raise ValueError('blocked_domains entries must be host names')
        candidate = domain.rstrip('.').lower()
        if any(c in candidate for c in '/:@?#*') or any(c.isspace() for c in candidate):
            raise ValueError('blocked_domains entries must be host names without schemes, ports or wildcards')
        try:
            candidate = candidate.encode('idna').decode('ascii')
        except UnicodeError:
            raise ValueError('Invalid blocked domain') from None
        if not candidate or any(not part or part.startswith('-') or part.endswith('-') or
                any(not (c.isalnum() or c == '-') for c in part) for part in candidate.split('.')):
            raise ValueError('Invalid blocked domain')
        result.append(candidate)
    return tuple(sorted(set(result)))


def normalized_url_rules(rules):
    if rules is None:return []
    if not isinstance(rules,(list,tuple)):
        raise ValueError('fetch_url_rules must be a list')
    result=[];names=set()
    for rule in rules:
        required={'name','action','path_pattern'}
        if not isinstance(rule,dict) or not required<=set(rule) or set(rule)-required-{'domains'}:
            raise ValueError('URL rule requires name, action and path_pattern; optional domains')
        if not isinstance(rule['name'],str) or not rule['name'] or rule['name'] in names:
            raise ValueError('URL rule names must be nonempty and unique')
        if rule['action'] not in {'allow','exclude'} or not isinstance(rule['path_pattern'],str) or not rule['path_pattern']:
            raise ValueError('Invalid URL rule action or pattern')
        try:re.compile(rule['path_pattern'])
        except re.error:raise ValueError('Invalid URL rule regular expression') from None
        value=dict(rule)
        if 'domains' in rule:
            value['domains']=list(normalized_blocked_domains(rule['domains']))
            if not value['domains']:raise ValueError('Scoped URL rule requires nonempty domains')
        names.add(rule['name']);result.append(value)
    return result


class URLPolicy:
    """Pure declared URL screening, shared by candidate selection and fetch.

    Construct once per bounded action; no HTTP client, journal, or directory is
    created. Domain exclusions take precedence over ordered path exceptions.
    """
    def __init__(self, *, blocked_domains=None, fetch_url_rules=None):
        if blocked_domains is not None and len(blocked_domains)>4096:
            raise ValueError('URL policy exceeds 4096 domains')
        if fetch_url_rules is not None and len(fetch_url_rules)>256:
            raise ValueError('URL policy exceeds 256 path rules')
        self.blocked_domains=normalized_blocked_domains(blocked_domains)
        self.url_rule_specs=normalized_url_rules(fetch_url_rules)
        self.url_rules=[(r['name'],r['action'],re.compile(r['path_pattern']),r.get('domains'))
                        for r in self.url_rule_specs]

    def blocked_domain(self, url):
        hostname = (urlsplit(url).hostname or '').rstrip('.').lower()
        try:hostname = hostname.encode('idna').decode('ascii')
        except UnicodeError:return None
        return next((domain for domain in self.blocked_domains
                     if hostname == domain or hostname.endswith('.' + domain)), None)

    def exclusion_reason(self, url):
        if not isinstance(url,str) or len(url)>16384 or not normalized_url(url):
            return 'invalid_url'
        domain=self.blocked_domain(url)
        if domain:return 'blocked_domain:'+domain
        parsed=urlsplit(url);path=parsed.path
        try:host=(parsed.hostname or '').rstrip('.').lower().encode('idna').decode('ascii')
        except UnicodeError:return 'invalid_url'
        for name,action,pattern,domains in self.url_rules:
            if domains and not any(host==d or host.endswith('.'+d) for d in domains):continue
            if pattern.search(path):return 'url_rule:'+name if action=='exclude' else None
        return None


class WebClient:
    """Lower-level search service client; no object acquisition/cache branch."""
    def __init__(self, *, cache_path, object_directory=None, search_url=None, search_language='all',
                 search_concurrency=8, timeout_s=30, connect_timeout_s=10, max_bytes=2*1024*1024,
                 redirects=3, retries=1, retry_delay_s=2, failure_limit=5, search_engines=None,
                 search_profile="external-default", search_interval_s=0,
                 connection_policy=None, _connection_manager=None):
        # object_directory is a declaration-only compatibility argument for
        # existing search sessions. No object directory is opened or written.
        self.connections=_connection_manager or ConnectionManager(connection_policy)
        self._owns_connections=_connection_manager is None
        self._closing_task=None
        self.path=str(cache_path);Path(self.path).parent.mkdir(parents=True,exist_ok=True)
        self.search_url,self.language=search_url,search_language
        self.search_engines=tuple(search_engines or ())
        self.search_profile=search_profile
        self.before_search=self.search_slots=None
        self.search_gate=RequestGate(search_concurrency,failures=failure_limit,interval_s=search_interval_s)
        self.inflight={}
        self.timeout_s,self.connect_timeout_s=timeout_s,connect_timeout_s
        self.max_bytes=max_bytes
        self.redirects,self.retries,self.retry_delay_s=redirects,retries,retry_delay_s
        self.client=None
        with self._db() as db:
            db.execute('CREATE TABLE IF NOT EXISTS cache (key TEXT PRIMARY KEY, value TEXT NOT NULL)')
        self.metrics={'search_requests':0,'http_hops':0,'reused':0,'wire_bytes':0,'decoded_bytes':0}
        self.latencies={};self.outcomes={}

    def snapshot_metrics(self):
        gate=self.search_gate
        return {**self.metrics,'outcomes':dict(self.outcomes),
                'connections':self.connections.snapshot_metrics(),
                'latency':{key:value.summary() for key,value in self.latencies.items()},
                'admission':{'search':{'requests':gate.admitted,'peak':gate.peak,'wait_s':gate.wait_s,
                    'consecutive_failures':gate.consecutive,'stopped':gate.stopped}}}

    def _db(self):
        db = sqlite3.connect(self.path, timeout=30)
        db.execute('PRAGMA journal_mode=DELETE'); db.execute('PRAGMA synchronous=FULL')
        return db


    def _cached(self,key,value=None):
        with self._db() as db:
            if value is None:
                row = db.execute('SELECT value FROM cache WHERE key=?',(key,)).fetchone()
                return json.loads(row[0]) if row else None
            db.execute('INSERT INTO cache VALUES (?,?) ON CONFLICT(key) DO UPDATE SET value=excluded.value',(key,json.dumps(value,ensure_ascii=False)))


    async def _once(self, kind, identity, operation):
        key = hashlib.sha256(json.dumps([kind, identity],sort_keys=True,ensure_ascii=False).encode()).hexdigest()
        if key in self.inflight:
            self.metrics['reused'] += 1
            return await asyncio.shield(self.inflight[key])
        async def execute():
            saved = await asyncio.to_thread(self._cached,key)
            if saved is not None:
                self.metrics['reused'] += 1
                return saved
            await asyncio.to_thread(self._cached,key,{'status':'interrupted','reason':'Prior request was reserved without a complete outcome; its attempt quota is not reset','attempts':[]})
            started=time.monotonic()
            try:
                result = await operation()
            except BaseException:
                self.outcomes[kind+':interrupted_or_fatal']=self.outcomes.get(kind+':interrupted_or_fatal',0)+1
                raise
            finally:
                self.latencies.setdefault(kind,LatencySummary()).observe(time.monotonic()-started)
            outcome=kind+':'+result['status']
            self.outcomes[outcome]=self.outcomes.get(outcome,0)+1
            # Persist exhausted failures too: replaying one run must not reset
            # its attempt quota. They remain failures, never successful evidence.
            await asyncio.to_thread(self._cached,key,result)
            return result
        task = self.inflight[key] = asyncio.create_task(execute())
        try: return await asyncio.shield(task)
        finally:
            if task.done(): self.inflight.pop(key,None)


    def _client(self,search=True):
        if search is not True:raise ValueError('WebClient only transports search responses')
        self.connections.start()
        if self.client is None:
            capacity=self.search_gate.concurrency
            self.client=self.connections.client((id(self),'search'),proxy=None,
                capacity=capacity,keepalive=min(20,capacity),
                timeout=httpx.Timeout(self.timeout_s,connect=self.connect_timeout_s),
                headers={'User-Agent':'demiflow-evidence/1','Accept-Encoding':'gzip, deflate'})
        return self.client

    async def _body(self,response,totals):
        encoding = response.headers.get('content-encoding','identity').lower()
        if encoding not in {'identity','gzip','deflate'}: raise RetrievalFailure('unsupported_content_encoding')
        decoder = zlib.decompressobj(16+zlib.MAX_WBITS if encoding=='gzip' else zlib.MAX_WBITS) if encoding!='identity' else None
        out = bytearray()
        async for chunk in response.aiter_raw(chunk_size=64*1024):
            totals[0] += len(chunk); self.metrics['wire_bytes'] += len(chunk)
            if totals[0] > self.max_bytes: raise RetrievalFailure('wire_bytes_exceeded')
            decoded = decoder.decompress(chunk, self.max_bytes-totals[1]+1) if decoder else chunk
            totals[1] += len(decoded); self.metrics['decoded_bytes'] += len(decoded)
            if totals[1] > self.max_bytes or (decoder and decoder.unconsumed_tail):
                raise RetrievalFailure('decoded_bytes_exceeded')
            out.extend(decoded)
        if decoder:
            last = decoder.flush(self.max_bytes-totals[1]+1)
            totals[1] += len(last); self.metrics['decoded_bytes'] += len(last)
            if totals[1]>self.max_bytes or not decoder.eof or decoder.unused_data: raise RetrievalFailure('invalid_or_oversize_compressed_body')
            out.extend(last)
        return bytes(out)


    async def _attempt(self,url,*,params=None,search=True):
        if search is not True:raise ValueError('WebClient only transports search responses')
        totals=[0,0]
        deadline=None
        for hop in range(self.redirects+1):
            # Preserve search pacing outside the first network deadline. Later
            # redirect admission shares the first hop's deadline, as before.
            async with (asyncio.timeout_at(deadline) if deadline is not None else nullcontext()):
                async with self.search_gate.enter():
                    if deadline is None:deadline=time.monotonic()+self.timeout_s
                    async with asyncio.timeout_at(deadline):
                        result=await self._hop(url,totals,params,search=True)
            code,headers,body,final=result
            if code not in {301,302,303,307,308}:return result
            if hop==self.redirects:raise RetrievalFailure('redirect_limit')
            from urllib.parse import urljoin
            url=normalized_url(urljoin(final,headers.get('location','')))
            if not url:raise RetrievalFailure('invalid_redirect')
            params=None
        raise AssertionError('unreachable')

    async def _hop(self,url,totals,params=None,*,search=True):
        if search is not True:raise ValueError('WebClient only transports search responses')
        self.metrics['http_hops']+=1
        async with self._client().stream('GET',url,params=params) as response:
            body=await self._body(response,totals)
            return response.status_code,dict(response.headers),body,str(response.url)

    async def _get(self,url,*,params=None,search=True):
        if search is not True:raise ValueError('WebClient only transports search responses')
        records=[]
        for attempt in range(self.retries+1):
            started=time.monotonic();self.metrics['search_requests']+=1
            code=None;headers={};body=b'';final=url
            try:
                code,headers,body,final=await self._attempt(url,params=params)
                status='ok' if 200<=code<300 else ('rate_limited' if code==429 else 'http_error')
                transient=code==429 or code>=500
                self.search_gate.result(transient=transient,
                    fatal=f'search authentication/configuration HTTP {code}' if code in {400,401,403,404} else '')
            except (httpx.PoolTimeout,ConnectionCapacityError) as exc:
                status='capacity_error';transient=False;headers={'error':type(exc).__name__}
            except (httpx.TransportError,TimeoutError) as exc:
                status='network_error';transient=True;headers={'error':type(exc).__name__}
                self.search_gate.result(transient=True)
            except (RetrievalFailure,zlib.error,httpx.InvalidURL) as exc:
                status='content_error';transient=False;headers={'error':str(exc)}
            records.append({'attempt':attempt+1,'status':status,'http_status':code,'reason':headers.get('error',''),
                'elapsed_s':time.monotonic()-started,
                **({'retry_after_s':retry_after(headers['retry-after'])} if code==429 and 'retry-after' in headers else {})})
            self.latencies.setdefault('search_attempt',LatencySummary()).observe(records[-1]['elapsed_s'])
            if status=='ok':return {'status':status,'reason':'','attempts':records,'body':body,'headers':headers,'final_url':final}
            delay=retry_after(headers.get('retry-after')) if code==429 else None
            if not transient or attempt==self.retries or (delay is not None and delay>60):
                return {'status':status,'reason':headers.get('error',f'HTTP {code}'),'attempts':records}
            await asyncio.sleep(delay if delay is not None else self.retry_delay_s+random.uniform(0,.25))
        raise AssertionError('unreachable')

    async def search(self,query, **parameters):
        allowed = {"language", "pageno", "safesearch", "time_range", "engines", "categories"}
        if parameters.keys() - allowed:
            raise ValueError("Unsupported legacy HTTP search parameters")
        from .searxng import normalize_response, ADAPTER_VERSION
        async def execute():
            if self.before_search is not None: await self.before_search()
            params={'q':query,'format':'json','language':self.language}
            if self.search_engines: params['engines']=','.join(self.search_engines)
            params.update({k: ','.join(v) if k in {'engines','categories'} else v for k,v in parameters.items()})
            # Do not send categories=general: SearXNG unions it with engines.
            async with (self.search_slots() if self.search_slots else nullcontext()):
                result=await self._get(self.search_url,params=params,search=True)
            if result['status']!='ok': return result
            try:
                parsed=normalize_response(json.loads(result['body']),normalized_url)
            except (ValueError,KeyError,TypeError) as exc:
                parsed={'status':'invalid_search_response','reason':str(exc),'candidates':[]}
            try:
                self.search_gate.result(success=parsed['status'] in {'ok','no_results'},
                                        transient=parsed['status'] in {'search_incomplete','invalid_search_response'})
            except ServiceStopped:
                # Preserve the triggering receipt and cache it before refusing
                # later admissions. Do not replace known failures by an
                # "interrupted" reservation or retry an HTTP-200 engine error.
                pass
            return {**parsed,'attempts':result['attempts']}
        return await self._once('search',[ADAPTER_VERSION,self.search_profile,self.search_engines,self.search_url,
            self.language,query,parameters,self.max_bytes,self.redirects,self.retries,self.timeout_s],execute)


    async def _close(self):
        tasks=list(self.inflight.values())
        for task in tasks:
            if not task.done():task.cancel()
        if tasks:await asyncio.gather(*tasks,return_exceptions=True)
        errors=[]
        if self.client is not None:
            try:await self.connections.close_client(self.client)
            except BaseException as exc:errors.append(exc)
        if self._owns_connections:
            try:await self.connections.aclose()
            except BaseException as exc:errors.append(exc)
        if errors:raise errors[0]

    async def aclose(self):
        if self._closing_task is None:
            self._closing_task=asyncio.create_task(self._close())
        try:await asyncio.shield(self._closing_task)
        except asyncio.CancelledError:
            await self._closing_task
            raise
