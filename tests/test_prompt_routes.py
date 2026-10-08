"""Real HTTP routes overlap, preserve one call per row and isolate a failed provider."""
import json
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

from demiflow import data
from demiflow.operator_llm.parser import parse_prompt_pack
from test_map_prompt_async import PACK, server
import pytest


def test_routes_overlap_and_isolate_failed_provider(tmp_path, monkeypatch):
    calls, finished, lock = [], [], threading.Lock()
    active, peaks = {}, {}
    class Handler(BaseHTTPRequestHandler):
        def do_POST(self):
            body = json.loads(self.rfile.read(int(self.headers['Content-Length'])))
            model = body['model']
            with lock:
                calls.append(body)
                active[model] = active.get(model, 0)+1
                peaks[model] = max(peaks.get(model, 0), active[model])
            time.sleep(.8 if model == 'slow' else .02)
            status = 401 if model == 'broken' else 200
            result = {'error': {'message': 'fixture unauthorized'}} if status == 401 else {
                'choices': [{'message': {'content': json.dumps({'result': model})}}]}
            raw = json.dumps(result).encode()
            self.send_response(status); self.send_header('Content-Length', str(len(raw)))
            self.send_header('Content-Type', 'application/json'); self.end_headers(); self.wfile.write(raw)
            with lock:
                active[model] -= 1
                finished.append(model)
        def log_message(self, *args): pass
    server = ThreadingHTTPServer(('127.0.0.1', 0), Handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True); thread.start()
    monkeypatch.setenv('TEST_PROMPT_URL', f'http://127.0.0.1:{server.server_port}/v1')
    monkeypatch.setenv('TEST_PROMPT_KEY', 'fixture')
    rows = [{'item': i, 'route': route} for i, route in enumerate(['slow', 'broken', 'fast', 'fast', 'fast'])]
    routes = {name: {'config': parse_prompt_pack(PACK.replace('mock-model', name)),
        'concurrency': 1, 'options': {'trust_env': False,
        'sqlite_journal': {'path': str(tmp_path/'calls.sqlite'), 'max_requests': 5}}}
        for name in ['slow', 'broken', 'fast']}
    try:
        graph = data.from_items(rows).map_prompt_async('enrich', config=parse_prompt_pack(PACK),
            routes=routes, route='route', isolate_route_failures=True, concurrency=5, queue_depth=1,
            inputs={'payload': 'item'}, output='answer', call_output='call', error_output='error')
        saved = graph.checkpoint(tmp_path/'results.jsonl', version='v1').take_all()
        assert len(saved) == 5 and len(calls) == 5
        assert finished.index('slow') > finished.index('fast')
        assert all(value == 1 for value in peaks.values())
        assert sum(r.get('answer') == 'fast' for r in saved) == 3
        error = next(r['error'] for r in saved if r['route'] == 'broken')
        assert error['route_unavailable'] is True
        assert all('reasoning_effort' not in c for c in calls)
        replay = graph.checkpoint(tmp_path/'replay.jsonl', version='v1').take_all()
        assert len(replay) == 5 and len(calls) == 5
    finally:
        server.shutdown(); server.server_close(); thread.join()


def test_unlimited_journal_keeps_old_calls_and_only_sends_missing_rows(server, tmp_path):
    import sqlite3
    path = tmp_path/'calls.sqlite'
    def graph(items, limit):
        return data.from_items([{'item': v} for v in items]).map_prompt_async('enrich',
            config=parse_prompt_pack(PACK), inputs={'payload':'item'}, output='answer',
            error_output='error', options={'trust_env':False,
                'sqlite_journal': {'path':str(path), 'max_requests':limit}})
    first = graph([1,2], 1).checkpoint(tmp_path/'limited.jsonl', version='1').take_all()
    assert len(server['requests']) == 1 and sum('error' in r for r in first) == 1
    resumed = graph([1,2,3], None).checkpoint(tmp_path/'unlimited.jsonl', version='1').take_all()
    assert len(server['requests']) == 3 and all(r['answer']=='ok' for r in resumed)
    with sqlite3.connect(path) as db:
        assert db.execute("select value from journal_state where name='request_count'").fetchone()[0] == '3'


def test_two_service_total_capacity_keeps_each_route_bounded(server):
    pack = parse_prompt_pack(PACK)
    routes = {'first': {'concurrency': 256}, 'shared': {'concurrency': 55}}
    graph = data.from_items([{'item': 1, 'route': 'first'}, {'item': 2, 'route': 'shared'}])
    kwargs = dict(config=pack, routes=routes, route='route', concurrency=311,
                  queue_depth=2, inputs={'payload': 'item'}, output='answer',
                  options={'trust_env': False}, max_requests=2)
    saved = graph.map_prompt_async('enrich', **kwargs).run_stream()
    assert len(server['requests']) == 2
    with pytest.raises(ValueError, match='1..512'):
        graph.map_prompt_async('enrich', **{**kwargs, 'concurrency': 513})
    with pytest.raises(ValueError, match='1..256'):
        graph.map_prompt_async('enrich', **{**kwargs, 'routes': {'first': {'concurrency': 257}}})
