"""Real HTTP/SSE: bounded retries preserve accounting, old evidence and cancellation."""
import asyncio
import json
import sqlite3
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import pytest
from demiflow.data.api import DataAPI
from demiflow.operator_llm.call_ref import journal_totals, read_call
from demiflow.operator_llm.errors import PromptResponseContractError
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


def test_declared_content_error_stays_in_its_row_without_retry_or_stopping_others(retry_server,tmp_path):
    retry_server.update(statuses=[400,200],error_body=b'{"error":{"code":"content_blocked","message":"Input rejected"}}')
    path=tmp_path/'calls.sqlite';cfg=options(path)
    cfg.pop('http_error_retry');cfg['nonfatal_http_errors']=[{'status':400,'code':'content_blocked'}]
    def run():
        return prompt(DataAPI().from_items([{'item':'blocked'},{'item':'allowed'}]),options=cfg,
                      concurrency=1,error_output='error',call_output='call').take_all()
    rows=run()
    assert rows[0]['error']['call']['http_status']==400
    assert rows[0]['error']['call']['provider_error_code']=='content_blocked'
    assert 'answer' not in rows[0] and rows[1]['answer']=='ok'
    assert len(retry_server['requests'])==2
    again=run()
    assert again[0]['error']['call']['reused'] and len(retry_server['requests'])==2
    assert journal_totals(path)['requests']==2


@pytest.mark.parametrize('status',[429,500,503])
def test_pressure_status_is_not_retried_and_replay_neither_resends_nor_recounts(retry_server,tmp_path,status):
    retry_server['statuses']=[status]
    path=tmp_path/'pressure.sqlite'
    def run():
        return prompt(DataAPI().from_items([{'item':'a'}]),options=options(path,limit=1),
                      error_output='error',max_requests=1).take_all()
    rows=run()
    assert rows[0]['error']['call']['http_status']==status and 'answer' not in rows[0]
    assert len(retry_server['requests'])==1 and journal_totals(path)['requests']==1
    again=run()
    assert again[0]['error']['call']['reused']
    assert len(retry_server['requests'])==1 and journal_totals(path)['requests']==1


def test_declared_retries_succeed_keep_every_attempt_and_replay_without_requests(retry_server,tmp_path):
    path=tmp_path/'calls.sqlite';ctx=DataAPI()
    def run(api):
        return prompt(api.from_items([{'item':'a'},{'item':'b'}]),options=options(path),
                      concurrency=1,call_output='call',max_requests=4).take_all()
    rows=run(ctx)
    assert [r['answer'] for r in rows]==['ok','ok']
    attempts=rows[0]['call']['attempts']
    assert [a['http_status'] for a in attempts]==[401,401,200]
    assert [read_call(a['response_ref'])['status_code'] for a in attempts]==[401,401,200]
    assert attempts[-1]['response_ref']['attempt']==3
    assert retry_server['requests'][0]==retry_server['requests'][1]==retry_server['requests'][2]
    assert all(r['num_retries']==0 and r['fallbacks']==[] for r in retry_server['requests'])
    totals=journal_totals(path)
    assert (totals['requests'],totals['responses'],totals['http_errors'],totals['http_successes'])==(4,4,2,2)
    usage=ctx.prompt_usage()
    assert usage['provider_requests_started']==4 and usage['provider_requests_failed']==2
    assert usage['provider_requests_completed']==2
    again=run(DataAPI())
    assert len(retry_server['requests'])==4 and all(r['call']['reused'] for r in again)


def test_incomplete_stream_and_unlisted_http_status_are_never_retried(retry_server,tmp_path):
    retry_server['statuses']=[200];retry_server['done']=False
    rows=prompt(DataAPI().from_items([{'item':'a'}]),options=options(tmp_path/'stream.sqlite'),error_output='error').take_all()
    assert rows[0]['error']['category']=='incomplete_response' and len(retry_server['requests'])==1
    retry_server['statuses']=[403]
    with pytest.raises(PromptResponseContractError,match='HTTP 403'):
        prompt(DataAPI().from_items([{'item':'b'}]),options=options(tmp_path/'403.sqlite')).run_stream()
    assert len(retry_server['requests'])==2
