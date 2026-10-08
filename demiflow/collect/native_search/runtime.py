"""Bounded native source orchestration, durable attempts and auditable outcomes."""
from __future__ import annotations

import asyncio
from contextlib import AsyncExitStack, contextmanager
import copy
import fcntl
import hashlib
import hmac
import json
import os
from pathlib import Path
import signal
import sqlite3
import sys
import tempfile
import time

from demiflow.execution.request_limits import RequestGate
from .config import SearchConfig, baseline_id, RUNTIME_VERSION, runtime_id, digest, resolve, public, validate_language


class WorkerError(RuntimeError):
    pass


class Worker:
    def __init__(self, owner):
        self.owner = owner
        self.process = None
        self.context = None
        self.directory = None

    async def close(self):
        proc, self.process = self.process, None
        directory, self.directory = self.directory, None
        self.context = None
        try:
            if proc is not None:
                if proc.returncode is None:
                    # Browser children may start their own process groups.
                    # Only descendants of this exact owned worker are stopped.
                    import psutil
                    try:
                        descendants = psutil.Process(proc.pid).children(recursive=True)
                    except psutil.NoSuchProcess:
                        descendants = []
                    for child in reversed(descendants):
                        try:
                            child.kill()
                        except psutil.NoSuchProcess:
                            pass
                    # SIGKILL is asynchronous; do not report cleanup complete
                    # while an owned detached browser is still exiting.
                    if descendants:
                        await asyncio.to_thread(psutil.wait_procs, descendants, timeout=5)
                    try:
                        os.killpg(proc.pid, signal.SIGKILL)
                    except ProcessLookupError:
                        pass
                waiter = asyncio.create_task(proc.wait())
                try:
                    await asyncio.shield(waiter)
                except asyncio.CancelledError:
                    await waiter
                    raise
        finally:
            if directory is not None:
                directory.cleanup()

    async def send(self, message):
        self.process.stdin.write(json.dumps(message, ensure_ascii=False).encode() + b'\n')
        await self.process.stdin.drain()

    async def read(self):
        line = await self.process.stdout.readline()
        if not line:
            raise WorkerError('worker_exited')
        try:
            result = json.loads(line)
        except (ValueError, UnicodeError):
            raise WorkerError('invalid_worker_protocol') from None
        if result.get('event') == 'fatal':
            raise WorkerError('worker_bootstrap_' + result['reason'])
        return result

    async def start(self, context):
        if self.process is not None and self.context == context:
            return
        await self.close()
        config = self.owner.config
        # No proxy credentials in argv, environment additions, stdout or stderr.
        env = dict(os.environ)
        for key in list(env):
            if key.lower() in ('http_proxy', 'https_proxy', 'all_proxy', 'no_proxy') or key.startswith('SEARXNG_'):
                env.pop(key)
        # In an installed wheel this is its site-packages; editable development
        # uses this checkout. Never infer a business tree or external environment.
        env['PYTHONPATH'] = str(Path(__file__).resolve().parents[3])
        self.directory = tempfile.TemporaryDirectory(prefix='demiflow-search-worker-')
        self.process = await asyncio.create_subprocess_exec(
            sys.executable, '-m', 'demiflow.collect.native_search.worker',
            stdin=asyncio.subprocess.PIPE, stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.DEVNULL, env=env, start_new_session=True,
            limit=config.max_bytes * 16 + 1024 * 1024)
        self.context = context
        self.owner.metrics['worker_starts'] += 1
        try:
            async with asyncio.timeout(config.startup_timeout_s):
                await self.send({**self.owner.worker_config(context), 'worker_directory': self.directory.name})
                message = await self.read()
                if message.get('event') != 'ready':
                    raise WorkerError('worker_not_ready')
        except BaseException:
            await self.close()
            raise

    async def call(self, context, message):
        await self.start(context)
        stack = None
        observed_http = []
        http_started = None
        try:
            source_limit = next((s.get('timeout', self.owner.config.timeout_s) for s in self.owner.sources
                                 if s['name'] == message.get('engine')), self.owner.config.timeout_s)
            async with asyncio.timeout(min(source_limit, self.owner.config.timeout_s)):
                await self.send(message)
                while True:
                    result = await self.read()
                    event = result.get('event')
                    if event == 'http_open':
                        if stack is not None:
                            raise WorkerError('nested_http_admission')
                        stack = AsyncExitStack()
                        host_gate = self.owner.host_gate(result['host'])
                        # Acquire host first so paced hosts don't consume global capacity.
                        await stack.enter_async_context(host_gate.enter())
                        await stack.enter_async_context(self.owner.http_gate.enter())
                        self.owner.metrics['http_requests'] += 1
                        http_started = time.monotonic()
                        observed_http.append({'host': result['host'], 'method': result['method'],
                                              'http_status': None, 'bytes': None, 'elapsed_s': 0.0})
                        await self.send({'event': 'http_grant'})
                    elif event == 'http_close':
                        if stack is None:
                            raise WorkerError('unbalanced_http_admission')
                        observed_http[-1] = result['receipt']
                        http_started = None
                        await stack.aclose()
                        stack = None
                    elif event == 'result':
                        if stack is not None:
                            raise WorkerError('adapter_returned_before_http_completed')
                        return result['value']
                    else:
                        raise WorkerError('invalid_worker_event')
        except BaseException as exc:
            if http_started is not None:
                observed_http[-1]['elapsed_s'] = time.monotonic() - http_started
            exc.native_http_receipts = observed_http
            await self.close()
            raise
        finally:
            if stack is not None:
                await stack.aclose()


class NativeSearchSession:
    """Run-owned native backend; normally constructed by WebSession(search=...)."""
    def __init__(self, *, cache_path, config: SearchConfig):
        self.config = copy.deepcopy(config)
        self.sources = self.config.source_configs()
        self.path = Path(cache_path)
        self.lock_dir = Path(str(self.path) + '.locks')
        self.workers = [Worker(self) for _ in range(config.workers)]
        self.available = asyncio.Queue(maxsize=config.workers)
        for worker in self.workers:
            self.available.put_nowait(worker)
        self.http_gate = RequestGate(config.request_concurrency)
        self.query_gate = (RequestGate(1, failures=config.query_failure_limit)
                           if config.query_failure_limit is not None else None)
        self.hosts = {}
        self.source_gates = {}
        self.health = {}
        self.inflight = {}
        self.active_calls = set()
        self.closed = False
        self.initialized = False
        self._initialize_lock = asyncio.Lock()
        from ..proxy import ProxyPool
        # Chromium maintains idle/speculative CONNECT tunnels in addition to
        # the admitted HTTP exchange. A single tunnel slot can deadlock its
        # authentication/reconnection. Keep this finite and separate from HTTP.
        browser_workers = config.workers if any(s.get('backend') == 'browser' for s in self.sources) else 0
        proxy_connections = max(config.request_concurrency,
            browser_workers * config.browser['proxy_connections_per_worker'])
        self.proxy_pool = ProxyPool(timeout_s=config.timeout_s, max_connections=proxy_connections)
        self.metrics = {'search_requests': 0, 'source_attempts': 0, 'http_requests': 0,
                        'reused': 0, 'worker_starts': 0, 'source_peak': 0, 'source_active': 0}

    @contextmanager
    def _db(self):
        db = sqlite3.connect(self.path, timeout=30)
        try:
            db.execute('PRAGMA synchronous=FULL')
            with db:
                yield db
        finally:
            db.close()

    async def database(self, *args):
        # Cancellation must not release a file claim while its SQLite writer
        # is still committing on another thread.
        task = asyncio.create_task(asyncio.to_thread(self._cache, *args))
        try:
            return await asyncio.shield(task)
        except asyncio.CancelledError:
            await task
            raise

    def _initialize(self):
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.lock_dir.mkdir(mode=0o700, exist_ok=True)
        with self._db() as db:
            db.execute('CREATE TABLE IF NOT EXISTS native_meta (key TEXT PRIMARY KEY, value TEXT)')
            db.execute('CREATE TABLE IF NOT EXISTS native_search (key TEXT PRIMARY KEY, value TEXT NOT NULL)')
            db.execute('INSERT OR IGNORE INTO native_meta VALUES (?, ?)', ('salt', os.urandom(32).hex()))
            salt = db.execute('SELECT value FROM native_meta WHERE key=?', ('salt',)).fetchone()[0]
        os.chmod(self.path, 0o600)
        self.resolved_sources = resolve(self.sources)
        self.resolved_proxy = resolve(self.config.proxy)
        self.resolved_networks = resolve(self.config.networks)
        for name, network in self.resolved_networks.items():
            if not isinstance(network, dict):
                raise ValueError('Search network must be an outgoing configuration: ' + name)
        secret_identity = hmac.new(bytes.fromhex(salt), json.dumps([
            self.resolved_sources, self.resolved_proxy, self.resolved_networks], sort_keys=True).encode(), hashlib.sha256).hexdigest()
        self.profile = digest([runtime_id(), self.config.snapshot(), secret_identity])
        self.redactions = []
        def find(declaration, actual):
            from .config import Secret
            if isinstance(declaration, Secret):
                self.redactions.append(actual)
                from urllib.parse import urlsplit, unquote
                if '://' in actual:
                    parsed = urlsplit(actual)
                    if parsed.password:
                        self.redactions.append(unquote(parsed.password))
                    if parsed.username and parsed.password:
                        import base64
                        pair = unquote(parsed.username) + ':' + unquote(parsed.password)
                        self.redactions.extend([pair, base64.b64encode(pair.encode()).decode()])
            elif isinstance(declaration, dict):
                for k, v in declaration.items():
                    find(v, actual[k])
            elif isinstance(declaration, (tuple, list)):
                for a, b in zip(declaration, actual):
                    find(a, b)
        find(self.sources, self.resolved_sources)
        find(self.config.proxy, self.resolved_proxy)
        find(self.config.networks, self.resolved_networks)
        self.redactions.sort(key=len, reverse=True)

    async def initialize(self):
        async with self._initialize_lock:
            if self.closed:
                raise RuntimeError('Native search session is closed')
            if not self.initialized:
                await asyncio.to_thread(self._initialize)
                self.transport_proxy = await self.proxy_pool.transport(self.resolved_proxy)
                if self.transport_proxy and self.transport_proxy != self.resolved_proxy:
                    self.redactions.append(self.transport_proxy)
                self.initialized = True

    def scrub(self, value, *, record=False, protocol=False):
        if isinstance(value, str):
            for secret in self.redactions:
                if len(secret) >= 4:
                    value = value.replace(secret, '[REDACTED]')
                elif value == secret:
                    value = '[REDACTED]'
            return value
        if isinstance(value, list):
            return [self.scrub(v, record=record, protocol=protocol) for v in value]
        if isinstance(value, dict):
            # Preserve only explicitly identified platform records / IPC tags.
            # Arbitrary result data can also have fields named status or runtime.
            structural = {'status', 'receipt_id', 'profile', 'runtime'} if record else set()
            if protocol:
                structural.add('__native_type__')
            return {k: v if k in structural else self.scrub(v,
                record=record and k in ('attempts', 'engine_receipts'), protocol=protocol)
                for k, v in value.items()}
        return value

    def worker_config(self, context):
        network, language = context
        outgoing = {'proxies': {'all://': self.transport_proxy} if self.transport_proxy else {},
                    'max_redirects': self.config.max_redirects}
        outgoing.update(self.resolved_networks.get(network, {}))
        return {'sources': self.resolved_sources, 'outgoing': outgoing,
                'browser': self.config.browser,
                'timeout_s': self.config.timeout_s, 'max_bytes': self.config.max_bytes,
                'max_results': self.config.max_results, 'max_redirects': self.config.max_redirects}

    def host_gate(self, host):
        if host not in self.hosts:
            self.hosts[host] = RequestGate(self.config.host_concurrency, interval_s=self.config.host_interval_s)
        return self.hosts[host]

    async def call(self, context, message):
        if self.closed:
            raise RuntimeError('Native search session is closed')
        task = asyncio.current_task()
        self.active_calls.add(task)
        worker = None
        try:
            first = await self.available.get()
            idle = [first]
            while not self.available.empty():
                idle.append(self.available.get_nowait())
            worker = next((w for w in idle if w.context == context),
                          next((w for w in idle if w.process is None), first))
            for candidate in idle:
                if candidate is not worker:
                    self.available.put_nowait(candidate)
            return await worker.call(context, message)
        finally:
            if worker is not None:
                self.available.put_nowait(worker)
            self.active_calls.discard(task)

    def _cache(self, key, value=None):
        with self._db() as db:
            if value is None:
                row = db.execute('SELECT value FROM native_search WHERE key=?', (key,)).fetchone()
                return json.loads(row[0]) if row else None
            db.execute('INSERT INTO native_search VALUES (?, ?) ON CONFLICT(key) DO UPDATE SET value=excluded.value',
                       (key, json.dumps(value, ensure_ascii=False)))

    async def once(self, key, execute):
        entry = self.inflight.get(key)
        if entry is None:
            async def run():
                fd = os.open(self.lock_dir / key, os.O_CREAT | os.O_RDWR, 0o600)
                try:
                    while True:
                        try:
                            fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
                            break
                        except BlockingIOError:
                            await asyncio.sleep(.02)
                    saved = await self.database(key)
                    if saved is not None:
                        # A refused admission spent no attempt. It may execute
                        # after the pause; exhausted/uncertain attempts never do.
                        expired_pause = (saved.get('status') == 'suspended' and not saved.get('attempts')
                                         and saved.get('resume_at', float('inf')) <= time.time())
                        if not expired_pause:
                            self.metrics['reused'] += 1
                            return saved
                    reserved = {'status': 'interrupted', 'reason': 'Reserved request did not commit; attempt budget is not reset',
                                'results': [], 'attempts': [], 'receipt_id': key}
                    await self.database(key, reserved)
                    result = self.scrub(await execute(key), record=True, protocol=True)
                    result['receipt_id'] = key
                    await self.database(key, result)
                    return result
                finally:
                    os.close(fd)
            entry = {'task': asyncio.create_task(run()), 'waiters': 0}
            self.inflight[key] = entry
        else:
            self.metrics['reused'] += 1
        entry['waiters'] += 1
        try:
            return copy.deepcopy(await asyncio.shield(entry['task']))
        finally:
            entry['waiters'] -= 1
            if not entry['waiters']:
                if not entry['task'].done():
                    entry['task'].cancel()
                    await asyncio.gather(entry['task'], return_exceptions=True)
                if self.inflight.get(key) is entry:
                    self.inflight.pop(key)

    async def source(self, source, query, parameters):
        name = source['name']
        key = digest([self.profile, name, query, parameters])
        context = (parameters['network'], parameters['language'])
        health_key = (parameters['network'], name)
        async def execute(receipt_id):
            gate = self.source_gates.setdefault(health_key, RequestGate(self.config.source_concurrency, interval_s=self.config.source_interval_s))
            async with gate.enter():
                failures, until = self.health.get(health_key, (0, 0))
                if until > time.monotonic():
                    return {'status': 'suspended', 'reason': 'Source failure pause', 'results': [], 'attempts': [],
                            'retry_after_s': until - time.monotonic(), 'resume_at': time.time() + until - time.monotonic()}
                attempts = []
                for attempt in range(self.config.retries + 1):
                    started = time.monotonic()
                    self.metrics['source_attempts'] += 1
                    self.metrics['source_active'] += 1
                    self.metrics['source_peak'] = max(self.metrics['source_peak'], self.metrics['source_active'])
                    try:
                        result = await self.call(context, {'op': 'search', 'engine': name,
                                                          'query': query, 'parameters': parameters})
                    except asyncio.CancelledError as exc:
                        interrupted = {'attempt': attempt + 1, 'status': 'interrupted', 'http_status': None,
                                       'reason': 'Cancelled source attempt', 'elapsed_s': time.monotonic() - started,
                                       'http': getattr(exc, 'native_http_receipts', [])}
                        await self.database(receipt_id, self.scrub({
                            'status': 'interrupted', 'reason': 'Cancelled; attempt budget is not reset',
                            'results': [], 'attempts': [*attempts, interrupted], 'receipt_id': receipt_id}, record=True))
                        raise
                    except TimeoutError as exc:
                        result = {'status': 'timeout', 'reason': 'Worker deadline exceeded', 'results': [],
                                  'http': getattr(exc, 'native_http_receipts', [])}
                    except WorkerError as exc:
                        result = {'status': 'worker_error', 'reason': str(exc), 'results': [],
                                  'http': getattr(exc, 'native_http_receipts', [])}
                    finally:
                        self.metrics['source_active'] -= 1
                    if source.get('backend'):
                        result.setdefault('backend', source['backend'])
                    attempts.append({'attempt': attempt + 1, 'status': result['status'], 'http_status': (result.get('http') or [{}])[-1].get('http_status'),
                                     'reason': result.get('reason', ''), 'elapsed_s': time.monotonic() - started,
                                     'http': result.get('http', [])})
                    result['attempts'] = attempts
                    # Persist progress as uncertain until the entire source result commits.
                    await self.database(receipt_id, self.scrub({
                        'status': 'interrupted', 'reason': 'Attempt reserved; source result did not commit',
                        'results': [], 'attempts': attempts, 'receipt_id': receipt_id}, record=True))
                    status = result['status']
                    if status in {'render_required', 'consent_required', 'request_budget', 'resource_limit'}:
                        # A different exit cannot add JS support or enlarge the
                        # declared browser budget. Preserve failure without
                        # attributing it to network health or retrying.
                        break
                    if status in ('ok', 'no_results'):
                        # A success already in flight must not lift a pause
                        # imposed by another concurrent request.
                        self.health[health_key] = (0, self.health.get(health_key, (0, 0))[1])
                        break
                    failures, previous_until = self.health.get(health_key, (0, 0))
                    failures += 1
                    pause = status in ('captcha', 'rate_limited', 'access_denied', 'authentication_error', 'configuration_error', 'dependency_error') or failures >= self.config.failure_limit
                    from demiflow.collect.web import retry_after
                    delay = retry_after((result.get('http') or [{}])[-1].get('retry_after')) or 0
                    self.health[health_key] = (failures, max(previous_until, time.monotonic() + max(self.config.suspend_s, delay) if pause else 0))
                    # Captchas and auth failures are never automatically reissued.
                    transient = status in ('network_error', 'timeout') or (status == 'http_error' and (attempts[-1]['http_status'] or 0) >= 500)
                    if self.health[health_key][1] > time.monotonic() or not transient or attempt == self.config.retries:
                        break
                    await asyncio.sleep(self.config.retry_delay_s)
                return result
        result = await self.once(key, execute)
        return {'engine': name, **result}

    def parameters(self, query, **options):
        if not isinstance(query, str) or not query.strip():
            raise ValueError('Search query must be a nonempty string')
        allowed = {'language', 'pageno', 'safesearch', 'time_range', 'engine_data', 'engines', 'categories', 'network'}
        if options.keys() - allowed:
            raise ValueError('Unknown search parameters: ' + ', '.join(sorted(options.keys() - allowed)))
        p = {'language': self.config.language, 'pageno': 1, 'safesearch': 0, 'time_range': None,
             'engine_data': {}, 'network': 'default', **options}
        validate_language(p['language'])
        if type(p['pageno']) is not int or p['pageno'] < 1:
            raise ValueError('pageno must be a positive integer')
        if type(p['safesearch']) is not int or p['safesearch'] not in (0, 1, 2):
            raise ValueError('safesearch must be 0, 1 or 2')
        if p['time_range'] not in (None, 'day', 'week', 'month', 'year'):
            raise ValueError('Unsupported time_range')
        if p['network'] != 'default' and p['network'] not in self.config.networks:
            raise ValueError('Unknown search network')
        if not isinstance(p['engine_data'], dict):
            raise ValueError('engine_data must be a mapping')
        selected = p.pop('engines', None)
        categories = p.pop('categories', None)
        names = {s['name'] for s in self.sources}
        if isinstance(selected, (list, tuple)) and all(isinstance(n, str) for n in selected):
            selected = [n.lower() for n in selected]
        if selected is not None and (not isinstance(selected, (list, tuple)) or not selected or set(selected) - names):
            raise ValueError('Requested engines must be declared in SearchConfig')
        if categories is not None and (not isinstance(categories, (list, tuple)) or not categories):
            raise ValueError('categories must be a nonempty list')
        sources = [s for s in self.sources if (selected is None or s['name'] in selected) and
                   (categories is None or set(categories).intersection(s.get('categories', ['general'])))]
        if not sources:
            raise ValueError('No declared sources match this request')
        # Check serializability and detach from caller mutations.
        return json.loads(json.dumps(p)), sources

    async def search(self, query, **options):
        if self.query_gate is not None:
            self.query_gate.check()
        parameters, sources = self.parameters(query, **options)
        await self.initialize()
        self.metrics['search_requests'] += 1
        from demiflow.collect.session import bounded
        groups = await bounded(sources, lambda s: self.source(s, query, parameters), self.config.workers)
        successes = [g for g in groups if g['status'] in ('ok', 'no_results')]
        failures = [g for g in groups if g['status'] not in ('ok', 'no_results')]
        payload = {'results': [], 'infoboxes': [], 'answers': [], 'suggestions': [],
                   'corrections': [], 'engine_data': {}, 'paging': False}
        aggregation_failure = None
        if successes:
            try:
                capabilities = {g['engine']: g.get('capabilities', {}) for g in successes}
                payload = await self.call((parameters['network'], parameters['language']), {
                    'op': 'merge', 'sources': [{'name': s['name'], 'weight': s.get('weight', 1.0),
                                              'paging': capabilities.get(s['name'], {}).get('paging', False),
                                              'categories': s.get('categories', ['general'])} for s in sources],
                    'groups': [{'engine': g['engine'], 'results': g.get('results', [])} for g in successes]})
                if payload.get('status'):
                    aggregation_failure = payload['status']
            except (WorkerError, TimeoutError):
                aggregation_failure = 'aggregation_failed'
        from demiflow.collect.searxng import normalize_response
        from demiflow.collect.web import normalized_url
        result = normalize_response(payload, normalized_url)
        if failures:
            result['status'] = 'partial' if successes else 'search_failed'
            result['reason'] = '; '.join(g['engine'] + ':' + g['status'] for g in failures)
        elif payload.get('answers') or payload.get('results') or payload.get('infoboxes'):
            result['status'] = 'ok'
            if not result['candidates']:
                result['reason'] = 'Source results are preserved in response_json; no HTTP document candidate'
        if aggregation_failure:
            result['status'] = aggregation_failure
            result['reason'] = 'Source receipts are retained; result aggregation did not complete'
        # Preserve full source results, scores, answers, images, engine_data, etc.
        result['response_json'] = json.dumps(self.scrub(payload), ensure_ascii=False, sort_keys=True)
        result['engine_receipts'] = [{k: v for k, v in g.items() if k not in ('results', 'http')} for g in groups]
        result['attempts'] = [a for g in groups for a in g.get('attempts', [])]
        result['runtime'] = RUNTIME_VERSION
        result['profile'] = self.profile
        result['parameters_json'] = json.dumps(parameters, ensure_ascii=False, sort_keys=True)
        if self.query_gate is not None:
            # Source attempts have already been committed. Empty healthy searches
            # are valid; an empty fallback while other sources fail is not proof
            # that retrieval is working. Preserve the fatal service boundary so
            # streaming callers drain in-flight model calls instead of assigning
            # a concept-level verdict to a system outage.
            usable = bool(payload.get('answers') or payload.get('results') or payload.get('infoboxes'))
            failed = bool(aggregation_failure or (failures and not usable))
            neutral = bool(failures) and not successes and all(g['status'] in {
                'render_required','consent_required','request_budget','resource_limit'} for g in failures)
            if not neutral:
                self.query_gate.result(success=not failed, transient=failed)
        return self.scrub(result, record=True)

    def snapshot_metrics(self):
        return {**self.metrics, 'http_peak': self.http_gate.peak,
                'query_consecutive_failures': self.query_gate.consecutive if self.query_gate else 0,
                'query_stopped': self.query_gate.stopped if self.query_gate else '',
                'http_active': self.http_gate.active, 'worker_count': sum(w.process is not None for w in self.workers),
                'hosts': {h: {'peak': g.peak, 'active': g.active, 'admitted': g.admitted} for h, g in self.hosts.items()}}

    async def aclose(self):
        self.closed = True
        tasks = {e['task'] for e in self.inflight.values()} | self.active_calls
        tasks.discard(asyncio.current_task())
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        await asyncio.gather(*(w.close() for w in self.workers))
        await self.proxy_pool.aclose()
