"""Cross-operator resource declarations, bounded blocking work and compatibility."""
import asyncio
from dataclasses import replace
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
import threading
import time

import pytest
from demiflow import data
from demiflow.embeddings import embedding_execution_config
from demiflow.inference import request_admission_config
from demiflow.execution.blocking_io import BlockingIOPool
from demiflow.execution.request_limits import RequestGate
from test_embeddings import server as embedding_server
from test_map_prompt_async import server as prompt_server, prompt, PACK


def test_execution_configuration_round_trip_and_copy(tmp_path):
    policy = dict(initial_concurrency=1, max_concurrency=2)
    options = {'sqlite_journal': {'path': tmp_path / 'calls.sqlite'}, 'profile_path': tmp_path / 'profile.json'}
    cfg = embedding_execution_config(concurrency=3, request_policy=policy, options=options)
    assert embedding_execution_config(**json.loads(json.dumps(cfg))) == cfg
    cfg['request_policy']['max_concurrency'] = 1
    cfg['options']['sqlite_journal']['path'] = 'other'
    assert policy['max_concurrency'] == 2 and options['sqlite_journal']['path'] == tmp_path / 'calls.sqlite'
    assert not list(tmp_path.iterdir())


@pytest.mark.parametrize('kwargs', [
    {'concurrency': True}, {'prefetch_batches': -1}, {'batch_size': 0},
    {'queue_depth': 0}, {'flush_interval': float('inf')}, {'max_requests': False},
    {'options': {'prepare_workers': 0}}, {'options': {'response_workers': True}},
    {'options': {'keepalive_expiry_s': float('nan')}},
    {'options': {'batch_request_bytes': 2048, 'max_request_bytes': 1024}},
    {'options': {'batch_decode_pixels': 0}}, {'options': {'collect_journal_totals': 'false'}},
    {'concurrency': 2, 'request_policy': {'max_concurrency': 4}},
])
def test_invalid_execution_budgets_rejected_before_resources(kwargs):
    with pytest.raises(ValueError):
        embedding_execution_config(**kwargs)


@pytest.mark.parametrize('kind', ['prompt', 'embedding'])
def test_declared_admission_bounds_fresh_calls_and_excludes_replay(kind, prompt_server, embedding_server, tmp_path):
    policy = request_admission_config({'initial_concurrency': 2, 'max_concurrency': 2}, concurrency=4)
    options = {'sqlite_journal': {'path': str(tmp_path / 'calls.sqlite')}, 'collect_journal_totals': False}
    if kind == 'prompt':
        prompt_server['respond'] = lambda *_: (time.sleep(.04) or {'result': 'ok'})
        ds = prompt(data.from_items([{'item': i} for i in range(8)]), concurrency=4,
                    request_policy=policy, options=options)
        state, key = prompt_server, 'requests'
    else:
        embedding_server['delay'] = .04
        ds = data.from_items([{'text': str(i)} for i in range(8)]).map_embeddings(
            model=embedding_server['model'], inputs={'text': 'text'},
            **embedding_execution_config(batch_size=1, concurrency=4,
                request_policy=policy, options=options))
        state, key = embedding_server, 'bodies'
    first = ds.run_stream()
    admission = next(iter(first.metrics['models'].values()))['shared_admission']
    assert first.emitted == len(state[key]) == admission['requests'] == 8
    assert 1 < state['peak'] <= 2 and admission['adaptive']['fresh_responses'] == 8
    second = ds.run_stream()
    model = next(iter(second.metrics['models'].values()))
    assert second.emitted == 8 and len(state[key]) == 8 and model['reused'] == 8
    assert model['shared_admission']['requests'] == model['shared_admission']['adaptive']['fresh_responses'] == 0
    assert second.metrics['native_journal_totals_skipped']


@pytest.mark.parametrize('kind', ['prompt', 'embedding'])
def test_policy_and_explicit_shared_gate_conflict_before_http(kind, prompt_server, embedding_server):
    kwargs = dict(concurrency=2, request_gate=RequestGate(2),
                  request_policy={'initial_concurrency': 1, 'max_concurrency': 2})
    with pytest.raises(ValueError, match='Choose request_policy or request_gate'):
        if kind == 'prompt':
            prompt(data.from_items([{'item': 1}]), **kwargs)
        else:
            data.from_items([{'text': 'a'}]).map_embeddings(model=embedding_server['model'],
                inputs={'text': 'text'}, **kwargs)
    assert not prompt_server['requests'] and not embedding_server['bodies']


def test_same_execution_configuration_works_for_two_model_identities(embedding_server):
    cfg = embedding_execution_config(batch_size=2, concurrency=2, prefetch_batches=1,
                                    options={'batch_request_bytes': 1024})
    for name in ('encoder-a', 'encoder-b'):
        embedding_server['response'] = lambda response, name=name: {**response, 'model': name}
        rows = data.from_items([{'text': str(i)} for i in range(3)]).map_embeddings(
            model=replace(embedding_server['model'], name=name), inputs={'text': 'text'}, **cfg).materialize().take_all()
        assert len(rows) == 3 and all(len(row['embedding']) == 3 for row in rows)
    assert {body['model'] for body in embedding_server['bodies']} == {'encoder-a', 'encoder-b'}


@pytest.mark.asyncio
async def test_blocking_pool_admits_before_submit_and_drains_repeated_cancellation():
    pool = BlockingIOPool(1, name='test-bounded-inference')
    started, release = threading.Event(), threading.Event()
    executed = []
    def blocking():
        started.set()
        assert release.wait(5)
        executed.append('committed')
    active = asyncio.create_task(pool.run(blocking))
    assert await asyncio.to_thread(started.wait, 5)
    queued = asyncio.create_task(pool.run(lambda: executed.append('queued')))
    await asyncio.sleep(.02)
    assert len(pool._pending) == 1
    queued.cancel()
    with pytest.raises(asyncio.CancelledError):
        await queued
    active.cancel()
    await asyncio.sleep(.01)
    active.cancel()
    await asyncio.sleep(.01)
    assert not active.done() and len(pool._pending) == 1
    release.set()
    with pytest.raises(asyncio.CancelledError):
        await active
    assert executed == ['committed'] and not pool._pending
    assert await pool.run(lambda: 7) == 7
    await pool.aclose()
    assert pool._executor is None
    with pytest.raises(RuntimeError, match='closed'):
        await pool.run(lambda: None)


@pytest.mark.asyncio
async def test_prompt_response_pool_progresses_while_journal_pool_is_busy(prompt_server):
    from demiflow.operator_llm.client import AsyncOperatorLLMClient
    from demiflow.operator_llm.parser import parse_prompt_pack
    client = AsyncOperatorLLMClient(parse_prompt_pack(PACK).prompt_definitions['enrich'].model,
                                   {'io_workers': 1, 'response_workers': 1})
    started, release = threading.Event(), threading.Event()
    def blocking():
        started.set()
        assert release.wait(5)
    task = asyncio.create_task(client._journal_pool.run(blocking))
    try:
        assert await asyncio.to_thread(started.wait, 5)
        name = await asyncio.wait_for(client._response_pool.run(lambda: threading.current_thread().name), 1)
        assert name.startswith('demiflow-prompt-response')
    finally:
        release.set()
        await task
        await client.aclose()
    assert all(pool._executor is None for pool in (client._journal_pool, client._prepare_pool, client._response_pool))


def test_prompt_real_calls_use_owned_pools_and_skip_historical_audit(prompt_server, tmp_path, monkeypatch):
    from demiflow.operator_llm import client as clients, call_ref
    from demiflow.operator_llm.sqlite_journal import SQLitePromptJournal
    observed, closed = {}, []
    def record(name, function):
        def wrapped(*args, **kwargs):
            observed.setdefault(name, []).append(threading.current_thread().name)
            return function(*args, **kwargs)
        return wrapped
    monkeypatch.setattr(clients.AsyncOperatorLLMClient, 'prepare', record('prepare', clients.AsyncOperatorLLMClient.prepare))
    monkeypatch.setattr(clients.AsyncOperatorLLMClient, 'decode', record('decode', clients.AsyncOperatorLLMClient.decode))
    monkeypatch.setattr(clients, '_response_body', record('parse', clients._response_body))
    monkeypatch.setattr(SQLitePromptJournal, 'reserve', record('reserve', SQLitePromptJournal.reserve))
    original_close = clients.AsyncOperatorLLMClient.aclose
    async def close(client):
        await original_close(client)
        closed.append(client)
    monkeypatch.setattr(clients.AsyncOperatorLLMClient, 'aclose', close)
    def forbidden(*args, **kwargs):
        raise AssertionError('Historical journal audit was not requested')
    monkeypatch.setattr(call_ref, 'journal_totals', forbidden)
    ds = prompt(data.from_items([{'item': i} for i in range(2)]), concurrency=2,
                options={'sqlite_journal': {'path': str(tmp_path / 'calls.sqlite')},
                         'collect_journal_totals': False})
    for _ in range(2):
        assert ds.run_stream().emitted == 2
    assert len(prompt_server['requests']) == 2 and len(closed) == 2
    for name, prefix in [('prepare', 'prepare'), ('decode', 'response'), ('parse', 'response'), ('reserve', 'journal')]:
        assert observed[name] and all(thread.startswith('demiflow-prompt-' + prefix) for thread in observed[name])
    assert all(pool._executor is None for client in closed
               for pool in (client._prepare_pool, client._journal_pool, client._response_pool))


@pytest.mark.parametrize('expiry,expected_connections', [(0, 3), (5, 1)])
def test_prompt_keepalive_policy_controls_actual_connections(expiry, expected_connections, monkeypatch):
    ports = set()
    class Handler(BaseHTTPRequestHandler):
        protocol_version = 'HTTP/1.1'
        def do_POST(self):
            self.rfile.read(int(self.headers['Content-Length']))
            ports.add(self.client_address[1])
            body = json.dumps({'choices': [{'message': {'content': '{"result":"ok"}'}}]}).encode()
            self.send_response(200)
            self.send_header('Content-Length', str(len(body)))
            self.end_headers()
            self.wfile.write(body)
        def log_message(self, *args):
            pass
    http = ThreadingHTTPServer(('127.0.0.1', 0), Handler)
    worker = threading.Thread(target=http.serve_forever, daemon=True)
    worker.start()
    monkeypatch.setenv('TEST_PROMPT_URL', f'http://127.0.0.1:{http.server_port}/v1')
    monkeypatch.setenv('TEST_PROMPT_KEY', 'test')
    try:
        ds = prompt(data.from_items([{'item': i} for i in range(3)]),
                    options={'keepalive_expiry_s': expiry}, concurrency=1)
        assert ds.run_stream().emitted == 3 and len(ports) == expected_connections
    finally:
        http.shutdown()
        http.server_close()
        worker.join()
