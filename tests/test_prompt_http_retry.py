"""Real HTTP/SSE: bounded retries preserve accounting, old evidence and cancellation."""
import asyncio
import json
import sqlite3
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import pytest
from demiflow.data.api import DataAPI
from demiflow.operator_llm.call_ref import journal_totals, read_call
from demiflow.operator_llm.http_retry import http_error_retry_policy
from demiflow.operator_llm.runtime import AsyncOperatorLLMRuntime, InProcessOperatorLLMCoordinator
from demiflow.operator_llm.parser import parse_prompt_pack
from demiflow.operator_llm.sqlite_journal import SQLitePromptJournal
from test_map_prompt_async import PACK, prompt
from test_prompt_http_stream import wire


@pytest.fixture
def retry_server(monkeypatch):
    state = {'statuses': [401, 401, 200], 'requests': [], 'done': True}
    class Handler(BaseHTTPRequestHandler):
        protocol_version = 'HTTP/1.1'
        def do_POST(self):
            body = json.loads(self.rfile.read(int(self.headers['Content-Length'])))
            index = len(state['requests']);state['requests'].append(body)
            code = state['statuses'][min(index, len(state['statuses'])-1)]
            raw = wire('ok',done=state['done']) if code == 200 else state.get('error_body',b'{"error":{"message":"Invalid API key"}}')
            self.send_response(code)
            self.send_header('Content-Type', 'text/event-stream' if code == 200 else 'application/json')
            self.send_header('Content-Length', str(len(raw)));self.end_headers()
            self.wfile.write(raw);self.wfile.flush()
        def log_message(self, *args):pass
    server = ThreadingHTTPServer(('127.0.0.1',0),Handler)
    thread = threading.Thread(target=server.serve_forever,daemon=True);thread.start()
    monkeypatch.setenv('TEST_PROMPT_URL',f'http://127.0.0.1:{server.server_port}/v1')
    monkeypatch.setenv('TEST_PROMPT_KEY','fixture')
    yield state
    server.shutdown();server.server_close();thread.join()


def options(path, limit=8, delays=None):
    return {'stream':True,'gateway':'litellm','trust_env':False,
        'sqlite_journal':{'path':str(path),'max_requests':limit},
        'http_error_retry':{'statuses':[401],'max_retries':2,'backoff_s':delays or [0,0]}}


def test_cancellation_during_backoff_preserves_complete_error_without_new_reservation(retry_server,tmp_path,monkeypatch):
    entered=asyncio.Event();original_sleep=asyncio.sleep
    async def sleep(delay):
        if delay==17:
            entered.set();await asyncio.Future()
        else:await original_sleep(delay)
    path=tmp_path/'calls.sqlite'
    coordinator=InProcessOperatorLLMCoordinator(max_requests=8)
    rt=AsyncOperatorLLMRuntime(parse_prompt_pack(PACK),coordinator,options(path,delays=[17,18]))
    async def run():
        monkeypatch.setattr(asyncio,'sleep',sleep)
        task=asyncio.create_task(rt.call('enrich',{'payload':'a'}))
        await asyncio.wait_for(entered.wait(),5);task.cancel()
        with pytest.raises(asyncio.CancelledError):await task
        await rt.aclose()
    asyncio.run(run())
    assert len(retry_server['requests'])==1 and not coordinator._reservations
    assert journal_totals(path)['requests']==1
    with sqlite3.connect(path) as db:
        assert db.execute('SELECT count(*) FROM recovery_events').fetchone()[0]==0
        assert db.execute('SELECT json_extract(response_json,\'$.status_code\') FROM calls').fetchone()[0]==401


@pytest.mark.parametrize('value',[
    {},{'statuses':[200],'max_retries':2,'backoff_s':[0,0]},
    {'statuses':[401],'max_retries':True,'backoff_s':[0]},
    {'statuses':[401],'max_retries':6,'backoff_s':[0]*6},
    {'statuses':[401],'max_retries':2,'backoff_s':[float('inf'),0]},
])
def test_invalid_retry_policy(value):
    with pytest.raises(ValueError):http_error_retry_policy(value)


def test_retry_requires_explicit_durable_budget_before_any_http(retry_server,tmp_path):
    cfg=options(tmp_path/'calls.sqlite');cfg['sqlite_journal'].pop('max_requests')
    with pytest.raises(ValueError,match='finite nonnegative'):
        prompt(DataAPI().from_items([{'item':'a'}]),options=cfg).run_stream()
    assert not retry_server['requests']


def test_stale_retry_cannot_replace_new_success(tmp_path):
    path=tmp_path/'calls.sqlite';journal=SQLitePromptJournal(path,max_requests=4)
    request={'payload':'unchanged'}
    try:
        journal.reserve(request)
        old=journal.response(request,{'status_code':401,'body':{'error':'bad'},'elapsed_s':1})
        journal.reserve_http_retry(request,expected_attempt=1,expected_status=401,max_attempts=6)
        new=journal.response(request,{'status_code':200,'body':{},'elapsed_s':1})
        with pytest.raises(ValueError,match='no longer matches'):
            journal.reserve_http_retry(request,expected_attempt=1,expected_status=401,max_attempts=6)
        assert read_call(old['response_ref'])['status_code']==401
        assert read_call(new['response_ref'])['status_code']==200
        assert journal_totals(path)['requests']==2
    finally:journal.close()
