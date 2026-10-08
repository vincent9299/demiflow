"""Bounded HTTP retrieval and run-local durable successful-result reuse.

This module reports transport/parse outcomes. It does not score names, decide
source authority, or contain concept-specific query/document quotas.
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
from contextlib import nullcontext, asynccontextmanager
from email.utils import parsedate_to_datetime
from pathlib import Path
from urllib.parse import urlsplit, urlunsplit
import httpx
from demiflow.collect.documents import DocumentError, PARSER_VERSION, store_document
from demiflow.execution.isolation import run_isolated
from demiflow.execution.request_limits import RequestGate, ServiceStopped, LatencySummary
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


class BodyRejected(Exception):
    """A declared content filter rejected a successful HTTP response."""
    def __init__(self, reason, metadata):
        super().__init__(reason)
        self.metadata = metadata
        self.http_status = None
        self.final_url = None


class DomainBlocked(Exception):
    """A declared acquisition exclusion, distinct from transport failure."""


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


class WebClient(URLPolicy):
    def __init__(self, *, cache_path, object_directory, search_url=None, search_language='all',
                 search_concurrency=8, fetch_concurrency=16, host_concurrency=2, host_interval_s=1,
                 timeout_s=30, connect_timeout_s=10, parse_timeout_s=30, max_bytes=2*1024*1024,
                 max_document_bytes=8*1024*1024, redirects=3, retries=1, retry_delay_s=2,
                 failure_limit=5, fetch_proxy_url=None, search_engines=None, search_profile="external-default",
                 search_interval_s=0, document_library=None, blocked_domains=None, fetch_proxy_routes=None,
                 fetch_url_rules=None, pdf_parser=None, fetch_session_pool=None,
                 connection_policy=None, fetch_attempt_limit=None, _connection_manager=None):
        if fetch_attempt_limit is not None and (type(fetch_attempt_limit) is not int
                or not 1 <= fetch_attempt_limit <= retries + 1):
            raise ValueError('fetch_attempt_limit must be between 1 and retries + 1')
        # An operational cap may only tighten the existing request allowance.
        # Exclude it from receipt identity so changing it cannot renew attempts.
        self.fetch_attempt_limit = fetch_attempt_limit
        from .document_library import DocumentLibrary
        if document_library is not None and not isinstance(document_library, DocumentLibrary):
            raise TypeError('document_library must be a DocumentLibrary declaration')
        if document_library is not None and Path(cache_path).resolve() == Path(document_library.index_path):
            raise ValueError('Run journal and shared document index must be separate')
        self.document_library = document_library
        from .pdf_text import pdf_policy
        self.pdf_parser=pdf_policy(pdf_parser)
        super().__init__(blocked_domains=blocked_domains,fetch_url_rules=fetch_url_rules)
        from .proxy import ProxyPool
        from .fetch_routes import proxy_routes
        if fetch_session_pool is not None and fetch_proxy_url is not None:
            raise ValueError('Choose fetch_session_pool or fetch_proxy_url as the default fetch transport')
        self.proxy_routes = proxy_routes(fetch_proxy_routes,default_session_pool=fetch_session_pool)
        self.proxy_pool = ProxyPool(timeout_s=timeout_s, max_connections=fetch_concurrency)
        self.route_clients = {}; self.route_lock = asyncio.Lock()
        self.fetch_route_pools = {}
        self.fetch_session_pools = {}
        self.connections = _connection_manager or ConnectionManager(connection_policy)
        self._owns_connections = _connection_manager is None
        self._closing_task = None
        self.path = str(cache_path); Path(self.path).parent.mkdir(parents=True,exist_ok=True)
        self.object_directory = document_library.object_directory if document_library else str(object_directory)
        self.search_url, self.language = search_url, search_language
        self.search_engines = tuple(search_engines or ())
        self.search_profile = search_profile
        self.before_search = None
        self.search_slots = None
        self.search_gate = RequestGate(search_concurrency, failures=failure_limit, interval_s=search_interval_s)
        self.fetch_gate = RequestGate(fetch_concurrency)
        self.host_concurrency, self.host_interval_s = host_concurrency, host_interval_s
        self.hosts, self.inflight = {}, {}
        self.timeout_s,self.connect_timeout_s,self.parse_timeout_s = timeout_s,connect_timeout_s,parse_timeout_s
        self.max_bytes,self.max_document_bytes = max_bytes,max_document_bytes
        self.redirects,self.retries,self.retry_delay_s = redirects,retries,retry_delay_s
        self.client = None; self.proxy_client = None; self.fetch_proxy_url = fetch_proxy_url
        with self._db() as db:
            db.execute('CREATE TABLE IF NOT EXISTS cache (key TEXT PRIMARY KEY, value TEXT NOT NULL)')
        self.metrics = {'search_requests':0,'fetch_attempts':0,'http_hops':0,'reused':0,'wire_bytes':0,'decoded_bytes':0}
        self.metrics.update(library_hits=0, library_misses=0, library_registrations=0)
        self.latencies = {}; self.outcomes = {}

    def snapshot_metrics(self):
        return {**self.metrics, 'outcomes':dict(self.outcomes),
                'connections':self.connections.snapshot_metrics(),
                'proxy_chains':self.proxy_pool.snapshot_metrics(),
                'fetch_proxy_pools':{d:p.snapshot_metrics() for d,p in self.fetch_route_pools.items()},
                'fetch_session_pools':{d:p.snapshot_metrics() for d,p in self.fetch_session_pools.items()},
                'latency':{key:value.summary() for key,value in self.latencies.items()},
                'admission':{key:{'requests':gate.admitted,'peak':gate.peak,'wait_s':gate.wait_s,
                                  'consecutive_failures':gate.consecutive,'stopped':gate.stopped}
                             for key,gate in [('search',self.search_gate),('fetch',self.fetch_gate)]}}

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

    def _client(self,search=False):
        self.connections.start()
        if self.fetch_proxy_url and not search:
            if self.proxy_client is None:
                capacity=self.fetch_gate.concurrency
                self.proxy_client=self.connections.client((id(self),'proxy'),proxy=self.fetch_proxy_url,
                    capacity=capacity,keepalive=min(20,capacity),
                    timeout=httpx.Timeout(self.timeout_s,connect=self.connect_timeout_s),
                    headers={'User-Agent':'demiflow-evidence/1','Accept-Encoding':'gzip, deflate'})
            return self.proxy_client
        if self.client is None:
            capacity=max(self.fetch_gate.concurrency,self.search_gate.concurrency)
            self.client=self.connections.client((id(self),'direct'),proxy=None,
                capacity=capacity,keepalive=min(20,capacity),
                timeout=httpx.Timeout(self.timeout_s,connect=self.connect_timeout_s),
                headers={'User-Agent':'demiflow-evidence/1','Accept-Encoding':'gzip, deflate'})
        return self.client

    async def _body(self,response,totals,inspector=None):
        encoding = response.headers.get('content-encoding','identity').lower()
        if encoding not in {'identity','gzip','deflate'}: raise RetrievalFailure('unsupported_content_encoding')
        decoder = zlib.decompressobj(16+zlib.MAX_WBITS if encoding=='gzip' else zlib.MAX_WBITS) if encoding!='identity' else None
        out = bytearray()
        async for chunk in response.aiter_raw(**({'chunk_size': 16*1024} if inspector is not None else {})):
            totals[0] += len(chunk); self.metrics['wire_bytes'] += len(chunk)
            if totals[0] > self.max_bytes: raise RetrievalFailure('wire_bytes_exceeded')
            decoded = decoder.decompress(chunk, self.max_bytes-totals[1]+1) if decoder else chunk
            totals[1] += len(decoded); self.metrics['decoded_bytes'] += len(decoded)
            if totals[1] > self.max_bytes or (decoder and decoder.unconsumed_tail):
                raise RetrievalFailure('decoded_bytes_exceeded')
            if inspector is not None:inspector.feed(decoded)
            out.extend(decoded)
        if decoder:
            last = decoder.flush(self.max_bytes-totals[1]+1)
            totals[1] += len(last); self.metrics['decoded_bytes'] += len(last)
            if totals[1]>self.max_bytes or not decoder.eof or decoder.unused_data: raise RetrievalFailure('invalid_or_oversize_compressed_body')
            if inspector is not None:inspector.feed(last)
            out.extend(last)
        return bytes(out)

    async def _attempt(self,url,*,params=None,search=False,fallback=False,body_filter=None):
        filter_options = {'body_filter': body_filter} if body_filter is not None else {}
        gate = self.search_gate if search else self.fetch_gate
        totals = [0,0]
        async def admitted_hop(target,params=None,deadline=None):
            # Wait for the host before occupying shared HTTP capacity. Release
            # both permits between redirects: holding the global permit while
            # another request holds the next host can deadlock a full pool.
            # Initial admission is outside the deadline; every redirect wait
            # and transfer shares the first hop's original deadline.
            host_gate=self.hosts.setdefault(urlsplit(target).hostname,
                RequestGate(self.host_concurrency,interval_s=self.host_interval_s))
            async with (asyncio.timeout_at(deadline) if deadline is not None else nullcontext()):
                async with (nullcontext() if search else host_gate.enter()):
                    async with gate.enter():
                        return await self._routed_hop(target,totals,params,search=search,
                            **({'deadline':deadline} if deadline is not None else {}),
                            **filter_options,**({'fallback':True} if fallback else {}))
        result,deadline=await admitted_hop(url,params=params)
        for hop in range(self.redirects+1):
            code,headers,body,final=result
            if code not in {301,302,303,307,308}: return result
            if hop==self.redirects: raise RetrievalFailure('redirect_limit')
            from urllib.parse import urljoin
            nxt=normalized_url(urljoin(final,headers.get('location','')))
            if not nxt: raise RetrievalFailure('invalid_redirect')
            result,_=await admitted_hop(nxt,deadline=deadline)
        raise AssertionError('unreachable')

    @asynccontextmanager
    async def _proxy_lease(self,url,search,fallback=False):
        domain=None if search else self.route_for(url)
        declaration=self.proxy_routes.get(domain)
        session_policy=(declaration.get('session_pool') or
            (declaration.get('fallback_session_pool') if fallback else None)) if isinstance(declaration,dict) else None
        if session_policy:
            if domain not in self.fetch_session_pools:
                from .fetch_sessions import FetchSessionPool
                self.fetch_session_pools[domain]=FetchSessionPool(self,domain,session_policy)
            pool=self.fetch_session_pools[domain]
            async with pool.lease() as route:yield pool,route
            return
        if isinstance(declaration,dict) and 'pool' in declaration:
            if domain not in self.fetch_route_pools:
                from .fetch_routes import FetchRoutePool
                self.fetch_route_pools[domain]=FetchRoutePool(self,domain,declaration)
            pool=self.fetch_route_pools[domain]
            async with pool.lease(host=urlsplit(url).hostname) as route:yield pool,route
        else:yield None,None

    async def _routed_hop(self,url,totals,params=None,*,search=False,deadline=None,fallback=False,body_filter=None):
        if not search:
            excluded=self.exclusion_reason(url)
            if excluded:raise DomainBlocked(excluded)
        # Initial route pacing precedes the attempt's network deadline. Redirect
        # routing retains that deadline, as do redirected host admission waits.
        async with self._proxy_lease(url,search,fallback) as (pool,route):
            deadline=deadline if deadline is not None else time.monotonic()+self.timeout_s
            started=time.monotonic();status='interrupted';code=None;delay=0
            try:
                async with asyncio.timeout_at(deadline):
                    result=await self._hop(url,totals,params,search=search,selected_route=route,
                                          **({'body_filter': body_filter} if body_filter is not None else {}))
                code,headers,_,_=result
                status='ok' if 200<=code<400 else ('rate_limited' if code==429 else 'http_error')
                delay=retry_after(headers.get('retry-after')) or 0
                return result,deadline
            except BodyRejected as exc:
                # A valid transport must not be penalized for image selection.
                status='ok';code=exc.http_status;raise
            except (httpx.PoolTimeout,ConnectionCapacityError):
                status='capacity_error';raise
            except (httpx.TransportError,TimeoutError):
                status='network_error';raise
            except (RetrievalFailure,zlib.error,httpx.InvalidURL):
                status='content_error';raise
            finally:
                if pool is not None:
                    # Persist before releasing the lease so a sibling cannot
                    # select the just-failed route before its cooldown applies.
                    pool.record(route,urlsplit(url).hostname,status,code,time.monotonic()-started,delay)

    async def _hop(self,url,totals,params=None,*,search=False,selected_route=None,body_filter=None):
        if not search:
            excluded = self.exclusion_reason(url)
            if excluded:
                raise DomainBlocked(excluded)
        self.metrics['http_hops'] += 1
        client = self._client(search=True) if search else await self._fetch_client(url,selected_route)
        async with client.stream('GET',url,params=params) as response:
            inspector = body_filter() if body_filter is not None and 200 <= response.status_code < 300 else None
            try:
                body=await self._body(response,totals,**({'inspector': inspector} if inspector is not None else {}))
            except BodyRejected as exc:
                exc.http_status=response.status_code;exc.final_url=str(response.url)
                raise
            return response.status_code,dict(response.headers),body,str(response.url)

    async def _get(self,url,*,params=None,search=False,body_filter=None):
        filter_options = {'body_filter': body_filter} if body_filter is not None else {}
        declaration=self.proxy_routes.get(self.route_for(url)) if not search else None
        enabled=isinstance(declaration,dict) and bool(declaration.get('fallback_session_pool'))
        from demiflow.execution.request_limits import ServiceStopped
        try:
            primary=await self._get_attempts(url,params=params,search=search,**filter_options)
        except ServiceStopped as exc:
            if not enabled or not str(exc).startswith('all_fetch_routes_cooling_down:'):raise
            primary={'status':'route_unavailable','reason':'all_static_routes_cooling_down','attempts':[]}
        if (not enabled or primary['status']=='ok' or
                self.fetch_attempt_limit is not None and len(primary['attempts'])>=self.fetch_attempt_limit):
            return primary
        last=primary['attempts'][-1] if primary['attempts'] else {}
        code=last.get('http_status')
        if primary['status'] not in {'network_error','route_unavailable'} and code not in (403,429) and not (code is not None and code>=500):
            return primary
        delay=last.get('retry_after_s') if code==429 else None
        if delay is not None:
            if delay>60:return primary
            if delay>0:await asyncio.sleep(delay)
        # Exactly one additional acquisition attempt, using the separately
        # declared session pool. It cannot recursively trigger another fallback.
        try:
            secondary=await self._get_attempts(url,params=params,search=False,fallback=True,**filter_options)
        except ServiceStopped as exc:
            if not str(exc).startswith('fetch_session_'):raise
            secondary={'status':'route_unavailable','reason':str(exc),'attempts':[]}
        attempts=[{**r,'route_kind':'static'} for r in primary['attempts']]
        attempts.extend({**r,'attempt':len(primary['attempts'])+i+1,'route_kind':'session'}
            for i,r in enumerate(secondary['attempts']))
        return {**secondary,'attempts':attempts}

    async def _get_attempts(self,url,*,params=None,search=False,fallback=False,body_filter=None):
        records=[]
        retries=0 if fallback else self.retries
        if not search and self.fetch_attempt_limit is not None:
            retries=min(retries,self.fetch_attempt_limit-1)
        for attempt in range(retries+1):
            started=time.monotonic(); self.metrics['search_requests' if search else 'fetch_attempts']+=1
            code=None; headers={}; body=b''; final=url; rejection=None
            try:
                code,headers,body,final=await self._attempt(url,params=params,search=search,
                    **({'body_filter': body_filter} if body_filter is not None else {}),**({'fallback':True} if fallback else {}))
                status='ok' if 200<=code<300 else ('rate_limited' if code==429 else 'http_error')
                transient=code==429 or code>=500
                if search:
                    # A successful HTTP envelope can still contain only engine
                    # failures. Reset health only after parsing usable results
                    # or a genuine empty result in search().
                    self.search_gate.result(transient=transient,
                        fatal=f'search authentication/configuration HTTP {code}' if code in {400,401,403,404} else '')
            except BodyRejected as exc:
                status='filtered';transient=False;headers={'error':str(exc)}
                code=exc.http_status;final=exc.final_url;rejection=exc.metadata
            except DomainBlocked as exc:
                status='excluded'; transient=False; headers={'error':str(exc)}
            except (httpx.PoolTimeout,ConnectionCapacityError) as exc:
                status='capacity_error';transient=False;headers={'error':type(exc).__name__}
            except (httpx.TransportError,TimeoutError) as exc:
                status='network_error'; transient=True
                if search: self.search_gate.result(transient=True)
                headers={'error':type(exc).__name__}
            except (RetrievalFailure,zlib.error,httpx.InvalidURL) as exc:
                status='content_error'; transient=False; headers={'error':str(exc)}
            except ServiceStopped as exc:
                # A target's declared routes may all be cooling, including
                # after a redirect. This is a per-URL outcome; resource/global
                # service stops still propagate and stop the action.
                if not str(exc).startswith('all_fetch_routes_cooling_down:'):raise
                status='route_unavailable';transient=False;headers={'error':str(exc)}
            records.append({'attempt':attempt+1,'status':status,'http_status':code,'reason':headers.get('error',''),
                            'elapsed_s':time.monotonic()-started,
                            **({'retry_after_s':retry_after(headers['retry-after'])} if code==429 and 'retry-after' in headers else {})})
            self.latencies.setdefault('search_attempt' if search else 'fetch_attempt',LatencySummary()).observe(records[-1]['elapsed_s'])
            if status=='ok': return {'status':'ok','reason':'','attempts':records,'body':body,'headers':headers,'final_url':final}
            if status=='filtered':
                return {'status':status,'reason':headers['error'],'attempts':records,'final_url':final,**rejection}
            delay=retry_after(headers.get('retry-after')) if code==429 else None
            pool_policy=self.proxy_routes.get(self.route_for(final)) if not search and not fallback else None
            rotate=(code==429 and isinstance(pool_policy,dict) and
                    (pool_policy.get('rotate_on_rate_limit',False) or pool_policy.get('session_pool')))
            if rotate and attempt<retries:
                # Static health quarantines the failed route/host; a renewable
                # pool retires its failed generation. Acquire another route
                # immediately, within the same explicit retry allowance. If all
                # routes are cooling, lease() applies only this host's wait bound.
                continue
            if not transient or attempt==retries or (delay is not None and delay>60):
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

    def route_for(self, url):
        hostname = (urlsplit(url).hostname or '').rstrip('.').lower()
        return next((domain for domain in sorted(self.proxy_routes,key=len,reverse=True)
                     if domain != '*' and (hostname==domain or hostname.endswith('.'+domain))),
                    '*' if '*' in self.proxy_routes else None)

    def proxy_identity(self, url):
        route = self.route_for(url)
        if route is None:
            return self.fetch_proxy_url
        from .fetch_routes import transport_identity
        return transport_identity(route,self.proxy_routes[route])

    async def _fetch_client(self, url, selected_route=None):
        self.connections.start()
        if selected_route is not None and '_fetch_session_pool' in selected_route:
            return await selected_route['_fetch_session_pool'].client(selected_route)
        route = self.route_for(url)
        if route is None:
            return self._client()
        key=(route,selected_route['name']) if selected_route is not None else route
        declaration=selected_route['proxy'] if selected_route is not None else self.proxy_routes[route]
        if isinstance(declaration,dict) and ('pool' in declaration or 'session_pool' in declaration):
            raise RuntimeError('Fetch proxy pool requires a leased route')
        async with self.route_lock:
            if key not in self.route_clients:
                from .native_search.config import resolve
                proxy = await self.proxy_pool.transport(resolve(declaration))
                capacity=selected_route.get('concurrency',1) if selected_route is not None else self.fetch_gate.concurrency
                keepalive=(min(20,capacity) if selected_route is None or selected_route.get('reuse_connections',True) else 0)
                self.route_clients[key]=self.connections.client((id(self),'route',key),proxy=proxy,
                    capacity=capacity,keepalive=keepalive,
                    timeout=httpx.Timeout(self.timeout_s,connect=self.connect_timeout_s),
                    headers={'User-Agent':'demiflow-evidence/1','Accept-Encoding':'gzip, deflate'})
            return self.route_clients[key]

    def _completed_fetch_from_previous_proxy(self,url,identity,*,operation='fetch',transport_index=8,previous_identities=()):
        domain=self.route_for(url);declaration=self.proxy_routes.get(domain)
        proxies=declaration.get('reuse_completed',[]) if isinstance(declaration,dict) else []
        if not proxies and not previous_identities:
            return None
        from .fetch_routes import transport_identity
        def cache_key(value):
            return hashlib.sha256(json.dumps([operation,value],sort_keys=True,ensure_ascii=False).encode()).hexdigest()
        key=cache_key(identity)
        if self._cached(key) is not None:return None
        def candidates():
            for previous in [identity,*previous_identities]:
                if previous is not identity:yield previous
                for prior in proxies:
                    before=list(previous);before[transport_index]=transport_identity(domain,prior)
                    yield before
        for before in candidates():
            prior_key=cache_key(before);saved=self._cached(prior_key)
            if saved is None or saved.get('status') not in {
                'ok','http_error','network_error','rate_limited','content_error','parse_error','excluded','capacity_error',
                'interrupted','filtered','image_error','integrity_error','library_busy','route_unavailable'}:
                continue
            # Explicit completed-receipt replay preserves old terminal failures
            # as failures too, including unknown reservations. A proxy
            # migration must not silently rerun them.
            with self._db() as db:
                db.execute('CREATE TABLE IF NOT EXISTS fetch_proxy_reuse (request_key TEXT PRIMARY KEY, source_key TEXT, observed_at REAL)')
                db.execute('INSERT OR IGNORE INTO fetch_proxy_reuse VALUES (?,?,?)',(key,prior_key,time.time()))
            return saved
        return None

    async def fetch(self,url):
        excluded_reason = self.exclusion_reason(url)
        if excluded_reason:
            async def excluded():
                return {'url':url,'status':'excluded','reason':excluded_reason,
                        'document_ref':None,'raw_ref':None,'attempts':[]}
            # Exclusions neither overwrite successful receipts nor consume a
            # transport attempt; cached/shared documents obey the same policy.
            return await self._once('fetch_policy',[url,list(self.blocked_domains),self.url_rule_specs],excluded)
        async def download():
            result=await self._get(url)
            if result['status']!='ok': return {**result,'url':url,'document_ref':None}
            source={'final_url':result['final_url'],'content_type':result['headers'].get('content-type',''),
                    'retrieved_at':datetime.now(timezone.utc).isoformat()}
            from demiflow.objects import LocalObjectStore
            raw_ref=(await asyncio.to_thread(LocalObjectStore(self.object_directory).put,result['body'])).to_dict()
            return {**source,'status':'ok','reason':'','raw_ref':raw_ref,'attempts':result['attempts']}
        transport_identity=[url,self.max_bytes,self.redirects,self.retries,self.timeout_s,self.proxy_identity(url)]
        async def execute():
            result=await self._once('download',transport_identity,download)
            if result['status']!='ok': return result
            raw_ref=result['raw_ref']
            source={k:result[k] for k in ('final_url','content_type','retrieved_at')}
            from demiflow.objects import ObjectRef, open_object
            def load_raw():
                reference=ObjectRef(**raw_ref)
                with open_object(reference.uri) as stream: body=stream.read(self.max_bytes+1)
                if len(body)>self.max_bytes or hashlib.sha256(body).hexdigest()!=reference.sha256:
                    raise DocumentError('raw_snapshot_integrity_failure')
                return body
            try:
                body=await asyncio.to_thread(load_raw)
                task=asyncio.create_task(asyncio.to_thread(run_isolated,store_document,self.object_directory,body,
                    url=url,**source,
                    **({'pdf_parser':self.pdf_parser} if self.pdf_parser is not None else {}),
                    max_normalized_bytes=self.max_document_bytes,timeout_s=self.parse_timeout_s))
                try: ref=await asyncio.shield(task)
                except asyncio.CancelledError:
                    await task; raise
                return {**source,'status':'ok','reason':'','url':url,'document_ref':ref,'raw_ref':raw_ref,'attempts':result['attempts']}
            except (DocumentError,TimeoutError) as exc:
                return {**source,'status':'parse_error','reason':str(exc),'url':url,'document_ref':None,'raw_ref':raw_ref,'attempts':result['attempts']}
        async def acquire():
            library = self.document_library
            if library is None:
                return await execute()
            # Run journal reuse occurs outside this function. A repeated run
            # retains its exact refs even if the shared library has newer data.
            async with library.claim(url):
                saved = await asyncio.to_thread(library.lookup, url, max_bytes=self.max_bytes,
                                                max_document_bytes=self.max_document_bytes)
                if saved is not None:
                    self.metrics['library_hits'] += 1
                    return saved
                self.metrics['library_misses'] += 1
                result = await execute()
                if result['status'] != 'ok':
                    return result
                task = asyncio.create_task(asyncio.to_thread(library.register,
                    {'document_ref':result['document_ref']}, max_bytes=self.max_bytes,
                    max_document_bytes=self.max_document_bytes, origin='download'))
                try:
                    registered = await asyncio.shield(task)
                except asyncio.CancelledError:
                    await task
                    raise
                self.metrics['library_registrations'] += 1
                return {**registered, 'attempts':result['attempts']}
        async def acquire_receipt():
            from .document_library import DocumentLibraryBusy
            try:
                return await acquire()
            except DocumentLibraryBusy as exc:
                return {'url':url,'status':'library_busy','reason':str(exc),
                        'document_ref':None,'raw_ref':None,'attempts':[]}
        identity = [url,self.max_bytes,self.max_document_bytes,self.redirects,self.retries,self.timeout_s,self.parse_timeout_s,PARSER_VERSION,self.proxy_identity(url)]
        if self.document_library is not None:
            identity.append(self.document_library.identity)
        if self.pdf_parser is not None:
            from .pdf_text import pdf_identity
            identity.append(pdf_identity(self.pdf_parser))
        result = await asyncio.to_thread(self._completed_fetch_from_previous_proxy,url,identity)
        if result is None:
            result = await self._once('fetch',identity,acquire_receipt)
        else:
            self.metrics['reused']+=1
            self.metrics['reused_previous_proxy']=self.metrics.get('reused_previous_proxy',0)+1
        excluded_reason = self.exclusion_reason(result.get('final_url') or url)
        if excluded_reason:
            return {'url':url,'status':'excluded','reason':excluded_reason,
                    'document_ref':None,'raw_ref':None,'attempts':[]}
        return result

    async def _close(self):
        tasks=list(self.inflight.values())
        # The owning action has ended: do not let queued host/route admission
        # begin fresh HTTP while shutdown waits for every retry chain to drain.
        # Reserved but incomplete requests remain interrupted in the journal.
        for task in tasks:
            if not task.done():task.cancel()
        if tasks: await asyncio.gather(*tasks,return_exceptions=True)
        resources=[*self.fetch_session_pools.values(),self.proxy_pool]
        clients=list(self.route_clients.values())+[c for c in (self.client,self.proxy_client) if c is not None]
        results=await asyncio.gather(*(resource.aclose() for resource in resources),
            *(self.connections.close_client(client) for client in clients),return_exceptions=True)
        if self._owns_connections:
            try:await self.connections.aclose()
            except BaseException as exc:results.append(exc)
        for result in results:
            if isinstance(result,BaseException):raise result

    async def aclose(self):
        if self._closing_task is None:
            self._closing_task=asyncio.create_task(self._close())
        try:await asyncio.shield(self._closing_task)
        except asyncio.CancelledError:
            await self._closing_task
            raise
