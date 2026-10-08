import asyncio
from collections import Counter
from contextlib import contextmanager
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
from pathlib import Path
import socket
import threading
import time
from urllib.parse import urlsplit, parse_qs

import pytest
from demiflow import data
from demiflow.collect import SearchConfig, Secret
from demiflow.collect.native_search import NativeSearchSession
from demiflow.collect.session import WebSession, bounded


@pytest.fixture
def server():
    calls = []
    state = {'active': 0, 'peak': 0}
    lock = threading.Lock()
    class Handler(BaseHTTPRequestHandler):
        protocol_version = 'HTTP/1.1'
        def log_message(self, *args):
            pass
        def do_GET(self):
            parsed = urlsplit(self.path)
            params = parse_qs(parsed.query)
            q = params.get('q', [''])[0]
            with lock:
                state['active'] += 1
                state['peak'] = max(state['peak'], state['active'])
                calls.append({'path': parsed.path, 'params': params, 'port': self.client_address[1],
                              'time': time.monotonic(), 'source': self.headers.get('X-Source'),
                              'authorization': self.headers.get('Authorization'),
                              'auth': self.headers.get('Proxy-Authorization')})
            try:
                if q.startswith('slow'):
                    time.sleep(.5)
                if q.startswith('hang'):
                    time.sleep(3)
                if q == 'reset':
                    self.connection.shutdown(socket.SHUT_RDWR)
                    self.connection.close()
                    return
                code = 302 if (q == 'loop' or q == 'redirect' and parsed.path != '/final') else 200
                payload = {'results': [{'url': 'https://example.org/shared', 'title': 'Shared', 'content': q},
                                       {'url': 'https://example.org/' + str(self.headers.get('X-Source')), 'title': 'Other'}]}
                if q in ('empty', 'captcha', 'rate'):
                    payload = {'results': []} if q == 'empty' else {'error': q}
                if q == 'typed':
                    payload = {'typed': True}
                if q in ('status429', 'status401', 'status503'):
                    code = int(q[-3:])
                body = json.dumps(payload).encode()
                if q == 'oversize':
                    body = b'x' * 100000
                if q == 'malformed':
                    body = b'<bad json>'
                self.send_response(code)
                if code == 302:
                    self.send_header('Location', 'http://localhost:' + str(self.server.server_port) + '/final?' + parsed.query)
                self.send_header('Content-Type', 'application/json')
                self.send_header('Content-Length', str(len(body)))
                self.end_headers()
                self.wfile.write(body)
            except (BrokenPipeError, ConnectionResetError):
                pass
            finally:
                with lock:
                    state['active'] -= 1
    http = ThreadingHTTPServer(('127.0.0.1', 0), Handler)
    thread = threading.Thread(target=http.serve_forever, daemon=True)
    thread.start()
    yield 'http://127.0.0.1:' + str(http.server_port), calls, state
    http.shutdown()
    http.server_close()
    thread.join()


@pytest.fixture
def engines(server, monkeypatch):
    monkeypatch.syspath_prepend(str(Path(__file__).parent / 'native_search_fixtures'))
    return tuple({'name': name, 'module': 'adapter', 'revision': 'fixture-v1', 'base_url': server[0] + '/' + name,
                  'enable_http': True, 'weight': 2 if name == 'beta' else 1} for name in ('alpha', 'beta'))


def session(tmp_path, engines, **kwargs):
    return NativeSearchSession(cache_path=tmp_path / 'cache.sqlite', config=SearchConfig(
        engines=engines, language='en', host_interval_s=0, **kwargs))


async def test_native_merge_cache_and_language(server, engines, tmp_path):
    s = session(tmp_path, engines, workers=2)
    try:
        result = await s.search('oak', language='zh-CN', pageno=2, safesearch=2, time_range='week')
        assert result['status'] == 'ok', result
        assert result['candidates'][0]['engines'] == ['alpha', 'beta']
        raw = json.loads(result['response_json'])
        assert raw['results'][0]['score'] == 8  # (1*2)*2 occurrences * (1+1)
        assert raw['paging'] is True
        assert all(c['params']['language'] == ['zh-CN'] and c['params']['page'] == ['2'] for c in server[1])
        assert len(server[1]) == 2
        await s.search('oak', language='zh-CN', pageno=2, safesearch=2, time_range='week')
        assert len(server[1]) == 2
        assert s.snapshot_metrics()['reused'] == 2
    finally:
        await s.aclose()
    t = session(tmp_path, engines, workers=2)
    try:
        again = await t.search('oak', language='zh-CN', pageno=2, safesearch=2, time_range='week')
        assert again == result and len(server[1]) == 2
    finally:
        await t.aclose()


@pytest.mark.parametrize('query,status', [('empty', 'no_results'), ('captcha', 'captcha'),
    ('rate', 'rate_limited'), ('status429', 'rate_limited'), ('malformed', 'parse_error'), ('typed', 'ok')])
async def test_outcome_types(server, engines, tmp_path, query, status):
    s = session(tmp_path, engines[:1], retries=2)
    try:
        result = await s.search(query)
        assert result['engine_receipts'][0]['status'] == status, result
        assert result['status'] == (status if status in ('ok', 'no_results') else 'search_failed')
        assert len(server[1]) == 1
        if query == 'typed':
            assert json.loads(result['response_json'])['results'][0]['publishedDate'].startswith('2026-01-02')
        if status == 'captcha':
            pause = await s.search('another')
            assert pause['engine_receipts'][0]['status'] == 'suspended'
            assert len(server[1]) == 1
    finally:
        await s.aclose()


async def test_partial_and_unsupported(server, engines, tmp_path):
    config = (engines[0], {**engines[1], 'paging': False})
    s = session(tmp_path, config)
    try:
        result = await s.search('tree', pageno=2)
        assert result['status'] == 'partial', result
        assert [r['status'] for r in result['engine_receipts']] == ['ok', 'unsupported_parameters']
        assert len(server[1]) == 1
    finally:
        await s.aclose()


async def test_query_circuit_empty_fallback_and_durable_receipts(server, engines, tmp_path):
    from demiflow.execution.request_limits import ServiceStopped
    import sqlite3
    config = (engines[0], {**engines[1], 'paging': False})
    s = session(tmp_path, config, query_failure_limit=2)
    try:
        # A healthy empty query is not a service failure.
        assert (await s.search('empty'))['status'] == 'no_results'
        assert s.snapshot_metrics()['query_consecutive_failures'] == 0
        # One source fails, but a usable fallback keeps the pipeline running.
        assert (await s.search('tree', pageno=2))['candidates']
        assert s.snapshot_metrics()['query_consecutive_failures'] == 0
        assert (await s.search('empty', pageno=2))['status'] == 'partial'
        assert s.snapshot_metrics()['query_consecutive_failures'] == 1
        with pytest.raises(ServiceStopped):
            await s.search('empty', pageno=3)
        before = len(server[1])
        with pytest.raises(ServiceStopped):
            await s.search('must not dispatch')
        assert len(server[1]) == before
        assert s.snapshot_metrics()['query_stopped']
        with sqlite3.connect(s.path) as db:
            receipts = [json.loads(r[0]) for r in db.execute('SELECT value FROM native_search')]
        assert sum(r['status'] == 'no_results' for r in receipts) >= 4
        assert any(r['status'] == 'unsupported_parameters' for r in receipts)
    finally:
        await s.aclose()


async def test_query_circuit_counts_suspended_sources(server, engines, tmp_path):
    from demiflow.execution.request_limits import ServiceStopped
    s = session(tmp_path, engines[:1], query_failure_limit=2)
    try:
        assert (await s.search('status429'))['status'] == 'search_failed'
        with pytest.raises(ServiceStopped):
            await s.search('source is suspended')
        assert len(server[1]) == 1
        assert s.snapshot_metrics()['query_consecutive_failures'] == 2
    finally:
        await s.aclose()


@pytest.mark.parametrize('value', [0, -1, True, 1.5])
def test_query_failure_limit_validation(value):
    with pytest.raises(ValueError, match='query_failure_limit'):
        SearchConfig(query_failure_limit=value)


async def test_connection_reuse_and_request_limits(server, engines, tmp_path):
    s = session(tmp_path, engines, workers=2, request_concurrency=1)
    try:
        await asyncio.gather(*(s.search('slow' + str(i)) for i in range(4)))
        assert len(server[1]) == 8
        assert len({c['port'] for c in server[1]}) <= 2
        assert s.snapshot_metrics()['http_peak'] == 1
        assert server[2]['peak'] == 1
    finally:
        await s.aclose()


async def test_cancellation_releases_process_and_reservation(server, engines, tmp_path):
    s = session(tmp_path, engines[:1], workers=1)
    task = asyncio.create_task(s.search('hang'))
    while not server[1]:
        await asyncio.sleep(.02)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert s.http_gate.active == 0 and all(w.process is None for w in s.workers)
    await s.aclose()
    t = session(tmp_path, engines[:1], workers=1)
    try:
        result = await t.search('hang')
        assert result['engine_receipts'][0]['status'] == 'interrupted'
        attempt = result['engine_receipts'][0]['attempts'][0]
        assert attempt['status'] == 'interrupted'
        assert attempt['http'][0]['host'] == '127.0.0.1'
        assert attempt['http'][0]['http_status'] is None
        assert len(server[1]) == 1
    finally:
        await t.aclose()


async def test_timeout_and_response_bound(server, engines, tmp_path):
    s = session(tmp_path, engines[:1], workers=1, timeout_s=.2, max_bytes=4096)
    try:
        result = await s.search('hang')
        assert result['engine_receipts'][0]['status'] == 'timeout', result
        assert result['engine_receipts'][0]['attempts'][0]['http'][0]['host'] == '127.0.0.1'
        assert s.http_gate.active == 0
        result = await s.search('oversize')
        assert result['engine_receipts'][0]['status'] == 'response_too_large', result
    finally:
        await s.aclose()


async def test_cross_session_claim_and_configuration_isolation(server, engines, tmp_path):
    a = session(tmp_path, engines[:1])
    b = session(tmp_path, engines[:1])
    try:
        x, y = await asyncio.gather(a.search('slow'), b.search('slow'))
        assert x == y and len(server[1]) == 1
        await asyncio.gather(a.search('new', language='zh'), b.search('new', language='ja'))
        assert {c['params']['language'][0] for c in server[1]} == {'en', 'zh', 'ja'}
    finally:
        await asyncio.gather(a.aclose(), b.aclose())


async def test_one_cancelled_waiter_does_not_cancel_other(server, engines, tmp_path):
    s = session(tmp_path, engines[:1])
    try:
        a = asyncio.create_task(s.search('slow'))
        b = asyncio.create_task(s.search('slow'))
        while not server[1]:
            await asyncio.sleep(.02)
        a.cancel()
        with pytest.raises(asyncio.CancelledError):
            await a
        assert (await b)['status'] == 'ok'
        assert len(server[1]) == 1
    finally:
        await s.aclose()


async def test_bounded_does_not_create_task_per_input():
    baseline = len(asyncio.all_tasks())
    peaks = []
    async def work(x):
        peaks.append(len(asyncio.all_tasks()) - baseline)
        await asyncio.sleep(0)
        return x + 1
    assert await bounded(list(range(10000)), work, 3) == list(range(1, 10001))
    assert max(peaks) <= 3


def test_public_dataset_entry_and_schema(server, engines, tmp_path):
    from demiflow.collect.contracts import SEARCH_RESULT
    import pyarrow as pa
    s = WebSession(cache_path=tmp_path/'cache.sqlite', object_directory=tmp_path/'objects',
                   search=SearchConfig(engines=engines, host_interval_s=0))
    assert not tmp_path.joinpath('cache.sqlite').exists()
    rows = []
    stats = (data.from_items([{'id': 'row', 'requests': [
        {'request_id': 'a', 'query': 'tree', 'language': 'zh', 'bindings': ['opaque']},
        {'request_id': 'b', 'query': 'tree', 'language': 'en', 'bindings': ['opaque']}]}])
        .search_web(requests='requests', output='found', session=s, request_concurrency=2, concurrency=2, queue_depth=2)
        .map(lambda row: rows.append(row) or row).run_stream())
    assert len(rows) == 1 and len(rows[0]['found']) == 2
    assert all(r['status'] == 'ok' for r in rows[0]['found'])
    assert pa.array(rows[0]['found'], type=SEARCH_RESULT)[0].as_py()['engine_receipts']
    metrics = stats.metrics['resources']['WebSession:0']
    assert metrics['search_requests'] == 4 and metrics['search_queries'] == 2
    assert metrics['search_http_requests'] == 4
    assert s.native is None  # Dataset owns cleanup


def test_explicit_language_and_secret_declaration(engines, tmp_path, monkeypatch):
    with pytest.raises(ValueError, match='Secret'):
        SearchConfig(proxy='http://user:password@example.org:8080')
    with pytest.raises(ValueError, match='Secret'):
        SearchConfig(engines=({'name': 'private', 'engine': 'google', 'api_key': 'secret'},))
    with pytest.raises(ValueError, match='Secret'):
        SearchConfig(engines=({'name': 'private', 'engine': 'google', 'headers': {'X-Api-Key': 'secret'}},))
    with pytest.raises(ValueError, match='Secret'):
        SearchConfig(engines=({'name': 'private', 'engine': 'google', 'base_url': 'https://example.org?key=secret'},))
    cfg = SearchConfig(engines=engines, proxy=Secret('TEST_NATIVE_PROXY'))
    assert 'secret_env' in json.dumps(cfg.snapshot())
    s = NativeSearchSession(cache_path=tmp_path/'cache.sqlite', config=cfg)
    with pytest.raises(ValueError, match='language'):
        s.parameters('中文')
    assert not tmp_path.joinpath('cache.sqlite').exists()


def test_json_search_configuration_roundtrip_is_lazy(tmp_path, monkeypatch):
    monkeypatch.delenv('TEST_JSON_SEARCH_PROXY', raising=False)
    declaration = {'engines':['wikisearch'],'language':'zh-CN',
                   'proxy':{'secret_env':'TEST_JSON_SEARCH_PROXY'}}
    config = SearchConfig.from_mapping(declaration)
    assert config.proxy == Secret('TEST_JSON_SEARCH_PROXY')
    assert SearchConfig.from_mapping(config.snapshot()) == config
    web = WebSession(search=declaration, cache_path=tmp_path/'cache.sqlite', object_directory=tmp_path/'objects')
    assert web.search_config == config and web.native is None
    assert not (tmp_path/'cache.sqlite').exists()
    with pytest.raises(ValueError, match='language'):
        SearchConfig.from_mapping({'language':'not_a_locale'})


def test_historical_http_result_projection():
    """Replay saved public result envelopes; no new requests or business decisions."""
    from demiflow.collect.searxng import normalize_response
    from demiflow.collect.web import normalized_url
    fixtures = json.loads((Path(__file__).parent/'native_search_fixtures/historical_responses.json').read_text())
    for fixture in fixtures:
        assert normalize_response(fixture['raw'], normalized_url) == fixture['expected']


async def test_proxy_credentials_and_language_are_isolated(server, engines, tmp_path, monkeypatch):
    import base64
    host = server[0].removeprefix('http://')
    monkeypatch.setenv('NATIVE_TEST_PROXY_A', 'http://alice:alpha-pass@' + host)
    monkeypatch.setenv('NATIVE_TEST_PROXY_B', 'http://bob:beta-pass@' + host)
    # This hostname cannot resolve; every request must use its declared proxy.
    source = ({**engines[0], 'base_url': 'http://search.invalid/alpha'},)
    a = session(tmp_path, source, proxy=Secret('NATIVE_TEST_PROXY_A'))
    b = session(tmp_path, source, proxy=Secret('NATIVE_TEST_PROXY_B'))
    try:
        ra, rb = await asyncio.gather(a.search('query', language='zh'), b.search('query', language='ja'))
        assert ra['status'] == rb['status'] == 'ok', (ra, rb)
        assert ra['profile'] != rb['profile']
        expected = {'zh': 'alice:alpha-pass', 'ja': 'bob:beta-pass'}
        assert len(server[1]) == 2
        for call in server[1]:
            assert base64.b64decode(call['auth'].split()[1]).decode() == expected[call['params']['language'][0]]
        text = json.dumps([ra, rb, a.config.snapshot(), b.config.snapshot()])
        stored = tmp_path.joinpath('cache.sqlite').read_bytes()
        for secret in ('alice', 'alpha-pass', 'bob', 'beta-pass'):
            assert secret not in text and secret.encode() not in stored
    finally:
        await asyncio.gather(a.aclose(), b.aclose())


async def test_network_override_separates_cache(server, engines, tmp_path, monkeypatch):
    host = server[0].removeprefix('http://')
    monkeypatch.setenv('NATIVE_TEST_NETWORK_A', 'http://a:pass-a@' + host)
    monkeypatch.setenv('NATIVE_TEST_NETWORK_B', 'http://b:pass-b@' + host)
    source = ({**engines[0], 'base_url': 'http://search.invalid/alpha'},)
    s = session(tmp_path, source, networks={
        'a': {'proxies': {'all://': Secret('NATIVE_TEST_NETWORK_A')}},
        'b': {'proxies': {'all://': Secret('NATIVE_TEST_NETWORK_B')}}})
    try:
        ra, rb = await asyncio.gather(s.search('same', network='a'), s.search('same', network='b'))
        assert ra['status'] == rb['status'] == 'ok'
        assert len(server[1]) == 2
        assert server[1][0]['auth'] != server[1][1]['auth']
        await s.search('same', network='a')
        assert len(server[1]) == 2
    finally:
        await s.aclose()


async def test_source_failure_does_not_suspend_other_pipeline(server, engines, tmp_path):
    a = session(tmp_path, engines[:1], failure_limit=1)
    b = session(tmp_path/'other', engines[:1], failure_limit=1)
    try:
        await a.search('captcha')
        assert (await a.search('new'))['engine_receipts'][0]['status'] == 'suspended'
        assert (await b.search('new'))['status'] == 'ok'
    finally:
        await asyncio.gather(a.aclose(), b.aclose())


async def test_suspended_admission_can_resume_without_resetting_spent_attempts(server, engines, tmp_path):
    s = session(tmp_path, engines[:1], suspend_s=.2)
    try:
        failure = await s.search('captcha')
        paused = await s.search('healthy')
        assert paused['engine_receipts'][0]['status'] == 'suspended'
        await asyncio.sleep(.25)
        assert (await s.search('healthy'))['status'] == 'ok'
        again = await s.search('captcha')
        assert again == failure and len(server[1]) == 2
    finally:
        await s.aclose()


@pytest.mark.parametrize('retries,expected', [(0, 1), (1, 2)])
async def test_retry_budget_has_no_hidden_reconnect(server, engines, tmp_path, retries, expected):
    s = session(tmp_path, engines[:1], retries=retries, retry_delay_s=0)
    try:
        result = await s.search('reset')
        assert result['engine_receipts'][0]['status'] == 'network_error', result
        assert len(server[1]) == expected
        assert len(result['engine_receipts'][0]['attempts']) == expected
        await s.search('reset')
        assert len(server[1]) == expected
    finally:
        await s.aclose()


async def test_redirect_hops_release_hosts_and_obey_bound(server, engines, tmp_path):
    s = session(tmp_path, engines[:1], workers=1, request_concurrency=1, host_concurrency=1, max_redirects=2)
    try:
        result = await s.search('redirect')
        assert result['status'] == 'ok', result
        assert len(server[1]) == 2
        http = result['engine_receipts'][0]['attempts'][0]['http']
        assert [h['host'] for h in http] == ['127.0.0.1', 'localhost']
        result = await s.search('loop')
        assert result['status'] == 'search_failed'
        assert len(server[1]) == 5
        assert all(g.active == 0 for g in s.hosts.values())
    finally:
        await s.aclose()


async def test_actual_http_pacing_not_only_query_pacing(server, engines, tmp_path):
    s = NativeSearchSession(cache_path=tmp_path/'cache.sqlite', config=SearchConfig(
        engines=engines, language='en', host_interval_s=.12, host_concurrency=2, workers=2))
    try:
        await asyncio.gather(*(s.search(str(i)) for i in range(3)))
        starts = sorted(c['time'] for c in server[1])
        assert len(starts) == 6
        assert min(b-a for a,b in zip(starts, starts[1:])) >= .10
    finally:
        await s.aclose()


async def test_short_secret_cannot_corrupt_protocol(server, engines, tmp_path, monkeypatch):
    monkeypatch.setenv('NATIVE_TEST_KEY', 'ok')
    source = ({**engines[0], 'api_key': Secret('NATIVE_TEST_KEY')},)
    s = session(tmp_path, source)
    try:
        result = await s.search('ok')
        assert result['status'] == 'ok'
        assert result['engine_receipts'][0]['status'] == 'ok'
        assert result['candidates'][0]['snippet'] == '[REDACTED]'
        kvmap = next(r['kvmap'] for r in json.loads(result['response_json'])['results'] if r.get('kvmap'))
        assert kvmap == {
            'status': '[REDACTED]', 'runtime': '[REDACTED]', '__native_type__': '[REDACTED]'}
    finally:
        await s.aclose()


@pytest.mark.parametrize('query,status,attempts', [('status401','authentication_error',1), ('status503','http_error',2)])
async def test_http_auth_and_server_retry_classification(server, engines, tmp_path, query, status, attempts):
    s = session(tmp_path, engines[:1], retries=1, retry_delay_s=0)
    try:
        result = await s.search(query)
        assert result['engine_receipts'][0]['status'] == status, result
        assert len(server[1]) == attempts
    finally:
        await s.aclose()


async def test_concurrent_failures_accumulate_before_suspension(server, engines, tmp_path):
    s = session(tmp_path, engines[:1], source_concurrency=2, failure_limit=2)
    try:
        await asyncio.gather(s.search('malformed'), s.search('status401'))
        assert s.health[('default','alpha')][0] == 2
        result = await s.search('new')
        assert result['engine_receipts'][0]['status'] == 'suspended'
        assert len(server[1]) == 2
    finally:
        await s.aclose()


@pytest.mark.parametrize('result_type', ['MainResult', 'KeyValue'])
async def test_offline_sqlite_source_paging_and_result_types(tmp_path, result_type):
    import sqlite3
    db_path = tmp_path/'source.sqlite'
    with sqlite3.connect(db_path) as db:
        db.execute('create table documents (title text, url text, content text)')
        db.executemany('insert into documents values (?,?,?)', [('oak one','https://example.org/one','first'),
                                                               ('oak two','https://example.org/two','second')])
    source = ({'name':'local catalog','engine':'sqlite','database':str(db_path),
               'query_str':'select title,url,content from documents where title like :wildcard order by title',
               'result_type':result_type,'limit':1},)
    s = session(tmp_path, source, workers=1)
    try:
        result = await s.search('oak', pageno=2)
        assert result['status'] == 'ok', result
        raw = json.loads(result['response_json'])
        assert len(raw['results']) == 1
        assert raw['paging'] is True
        if result_type == 'MainResult':
            assert result['candidates'][0]['url'] == 'https://example.org/two'
        else:
            assert raw['results'][0]['kvmap']['title'] == 'oak two'
        assert s.metrics['http_requests'] == 0
    finally:
        await s.aclose()
    assert all(w.directory is None for w in s.workers)


async def test_redirect_does_not_forward_origin_credentials(server, engines, tmp_path, monkeypatch):
    monkeypatch.setenv('NATIVE_ORIGIN_TOKEN', 'origin-token-private')
    source = ({**engines[0], 'api_key': Secret('NATIVE_ORIGIN_TOKEN')},)
    s = session(tmp_path, source, workers=1)
    try:
        assert (await s.search('redirect'))['status'] == 'ok'
        assert server[1][0]['authorization'] == 'Bearer origin-token-private'
        assert server[1][1]['authorization'] is None
    finally:
        await s.aclose()


async def test_adapter_helper_requests_use_same_admission_and_receipt(server, engines, tmp_path):
    s = session(tmp_path, engines[:1], request_concurrency=1, workers=1)
    try:
        result = await s.search('multi')
        assert result['status'] == 'ok', result
        assert len(server[1]) == 4
        assert len(result['engine_receipts'][0]['attempts'][0]['http']) == 4
        assert s.metrics['source_attempts'] == 1 and s.metrics['http_requests'] == 4
        assert s.http_gate.peak == 1
    finally:
        await s.aclose()


async def test_authenticated_source_url_uses_auth_without_leaking_userinfo(server, engines, tmp_path, monkeypatch):
    import base64
    host = server[0].removeprefix('http://')
    monkeypatch.setenv('NATIVE_BASIC_ORIGIN', 'http://source-user:source-password@' + host + '/alpha')
    source = ({**engines[0], 'base_url': Secret('NATIVE_BASIC_ORIGIN')},)
    s = session(tmp_path, source, workers=1)
    try:
        result = await s.search('redirect')
        assert result['status'] == 'ok'
        first = server[1][0]['authorization']
        assert base64.b64decode(first.split()[1]).decode() == 'source-user:source-password'
        assert server[1][1]['authorization'] is None
        assert 'source-password' not in json.dumps(result)
        assert b'source-password' not in tmp_path.joinpath('cache.sqlite').read_bytes()
    finally:
        await s.aclose()
