"""Real HTTP/SSE: bounded retries preserve accounting, old evidence and cancellation."""
import asyncio
import json
import sqlite3
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import pytest
from demiflow.data.api import DataAPI
from demiflow.execution.request_limits import RequestGate, ServiceStopped
from demiflow.operator_llm.call_ref import journal_totals, read_call
from demiflow.operator_llm.errors import PromptBudgetExceededError, PromptResponseContractError
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


def test_declared_content_error_is_retained_without_retry_or_stopping_other_rows(retry_server,tmp_path):
    retry_server.update(statuses=[400,200],error_body=b'{"error":{"code":"content_blocked","message":"Input rejected"}}')
    path=tmp_path/'calls.sqlite';cfg=options(path)
    cfg.update(http_error_retry=None,nonfatal_http_errors=[{'status':400,'code':'content_blocked'}])
    gate=RequestGate(1)
    ds=prompt(DataAPI().from_items([{'item':'blocked'},{'item':'allowed'}]),options=cfg,
        concurrency=1,request_gate=gate,error_output='error',call_output='call')
    saved=ds.checkpoint(tmp_path/'rows.jsonl',version='v1').take_all()
    assert saved[0]['error']['call']['http_status']==400
    assert saved[0]['error']['call']['provider_error_code']=='content_blocked'
    assert saved[1]['answer']=='ok' and not gate.stopped and gate.admitted==2
    assert len(retry_server['requests'])==2
    replay=ds.checkpoint(tmp_path/'replay.jsonl',version='v1').take_all()
    assert replay[0]['error']['call']['reused'] and len(retry_server['requests'])==2
    assert journal_totals(path)['requests']==2


@pytest.mark.parametrize('status,code,rules',[
    (400,'content_blocked',[]),
    (400,'invalid_parameter',[{'status':400,'code':'content_blocked'}]),
    (401,'content_blocked',[{'status':400,'code':'content_blocked'}]),
])
def test_nonfatal_error_requires_exact_explicit_status_and_code(retry_server,tmp_path,status,code,rules):
    retry_server.update(statuses=[status],error_body=json.dumps({'error':{'code':code}}).encode())
    cfg=options(tmp_path/'calls.sqlite');cfg.update(http_error_retry=None,nonfatal_http_errors=rules)
    with pytest.raises(ServiceStopped,match=f'HTTP {status}'):
        prompt(DataAPI().from_items([{'item':'a'}]),options=cfg,request_gate=RequestGate(1),error_output='error').run_stream()
    assert len(retry_server['requests'])==1


@pytest.mark.parametrize('status',[429,500,503])
def test_http_pressure_bypasses_noise_window_without_retrying_or_recounting_replay(retry_server,tmp_path,status):
    from demiflow.execution.adaptive_requests import AdaptiveRequestGate
    retry_server['statuses']=[status]
    gate=AdaptiveRequestGate(dict(initial_concurrency=4,max_concurrency=4,
        transient_min_failures=3,transient_failure_ratio=.5))
    path=tmp_path/'pressure.sqlite'
    ds=prompt(DataAPI().from_items([{'item':'a'}]),options=options(path,limit=1),
        request_gate=gate,error_output='error',max_requests=1)
    rows=ds.checkpoint(tmp_path/'first.jsonl',version='first').take_all()
    assert rows[0]['error']['call']['http_status']==status
    assert gate.capacity==2 and gate.last_reason=='backpressure' and gate.admitted==1
    assert journal_totals(path)['requests']==1
    ds.checkpoint(tmp_path/'replay.jsonl',version='replay').take_all()
    assert len(retry_server['requests'])==1 and gate.capacity==4 and gate.admitted==0


def test_retries_succeed_without_stopping_next_row_and_all_attempts_replay(retry_server,tmp_path):
    path=tmp_path/'calls.sqlite';ctx=DataAPI();gate=RequestGate(1)
    ds=prompt(ctx.from_items([{'item':'a'},{'item':'b'}]),options=options(path),
        concurrency=1,request_gate=gate,call_output='call',max_requests=4)
    rows=ds.checkpoint(tmp_path/'a.jsonl',version='a').take_all()
    assert [r['answer'] for r in rows]==['ok','ok']
    assert gate.admitted==4 and not gate.stopped
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
    again=ds.checkpoint(tmp_path/'b.jsonl',version='b').take_all()
    assert len(retry_server['requests'])==4 and all(r['call']['reused'] for r in again)


def test_three_401s_stop_and_restart_does_not_reset_attempt_ceiling(retry_server,tmp_path):
    retry_server['statuses']=[401];path=tmp_path/'calls.sqlite'
    gate=RequestGate(1)
    def build():return prompt(DataAPI().from_items([{'item':'a'}]),options=options(path),request_gate=gate)
    with pytest.raises(ServiceStopped,match='HTTP 401'):build().run_stream()
    assert len(retry_server['requests'])==3
    gate=RequestGate(1)
    # A completed cached error does not send another request or provide fresh
    # authentication evidence; the consumer can retain its per-row failure.
    with pytest.raises(PromptResponseContractError):build().run_stream()
    assert not gate.stopped and gate.admitted==0
    assert len(retry_server['requests'])==3
    with sqlite3.connect(path) as db:
        key,request=db.execute('SELECT request_key,request_json FROM calls').fetchone()
        assert db.execute('SELECT count(*) FROM recovery_events').fetchone()[0]==2
    journal=SQLitePromptJournal(path,max_requests=8)
    try:
        with pytest.raises(PromptBudgetExceededError,match='per-request'):
            journal.reserve_http_retry(json.loads(request),expected_attempt=3,expected_status=401,max_attempts=3)
        assert journal.read(key)['status_code']==401
    finally:journal.close()


@pytest.mark.parametrize('status',[400,401,403,404])
def test_cached_http_error_does_not_stop_other_rows_or_automatically_retry(retry_server,tmp_path,status):
    retry_server['statuses']=[status]
    path=tmp_path/'calls.sqlite';seed=options(path);seed['http_error_retry']=None
    with pytest.raises(ServiceStopped):
        prompt(DataAPI().from_items([{'item':'old'}]), options=seed,
               request_gate=RequestGate(1),error_output='error').run_stream()
    assert len(retry_server['requests'])==1
    retry_server['statuses']=[200]
    gate=RequestGate(1)
    rows=prompt(DataAPI().from_items([{'item':'old'},{'item':'new'}]),
                options=options(path),request_gate=gate,error_output='error',call_output='call',
                concurrency=1).checkpoint(tmp_path/'result.jsonl',version='v1').take_all()
    by_item={r['item']:r for r in rows}
    error=by_item['old']['error']
    assert error['call']['reused'] is True
    assert error['call']['http_status']==status
    assert read_call(error['call']['response_ref'])['status_code']==status
    assert by_item['new']['answer']=='ok'
    assert not gate.stopped and gate.admitted==1 and len(retry_server['requests'])==2
    assert journal_totals(path)['requests']==2


@pytest.mark.parametrize('durable_limit,node_limit',[(2,8),(8,2)])
def test_either_budget_stops_before_extra_http_and_preserves_last_error(retry_server,tmp_path,durable_limit,node_limit):
    path=tmp_path/'calls.sqlite'
    ds=prompt(DataAPI().from_items([{'item':'a'}]),options=options(path,durable_limit),
        max_requests=node_limit,request_gate=RequestGate(1))
    with pytest.raises(PromptBudgetExceededError):ds.run_stream()
    assert len(retry_server['requests'])==2
    assert journal_totals(path)['requests']==2
    with sqlite3.connect(path) as db:
        assert db.execute('SELECT count(*) FROM calls WHERE response_json IS NULL').fetchone()[0]==0
        assert db.execute('SELECT count(*) FROM recovery_events').fetchone()[0]==1


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


def test_incomplete_stream_and_unlisted_http_status_never_retry(retry_server,tmp_path):
    retry_server['statuses']=[200];retry_server['done']=False
    ds=prompt(DataAPI().from_items([{'item':'a'}]),options=options(tmp_path/'stream.sqlite'),error_output='error')
    rows=ds.checkpoint(tmp_path/'a.jsonl',version='a').take_all()
    assert rows[0]['error']['category']=='incomplete_response' and len(retry_server['requests'])==1
    retry_server['statuses']=[403]
    with pytest.raises(ServiceStopped,match='HTTP 403'):
        prompt(DataAPI().from_items([{'item':'b'}]),options=options(tmp_path/'403.sqlite'),request_gate=RequestGate(1)).run_stream()
    assert len(retry_server['requests'])==2


@pytest.mark.parametrize('value',[
    {},{'statuses':[200],'max_retries':2,'backoff_s':[0,0]},
    {'statuses':[401],'max_retries':True,'backoff_s':[0]},
    {'statuses':[401],'max_retries':6,'backoff_s':[0]*6},
    {'statuses':[401],'max_retries':2,'backoff_s':[float('inf'),0]},
])
def test_invalid_retry_policy(value):
    with pytest.raises(ValueError):http_error_retry_policy(value)


@pytest.mark.parametrize('eventual_success',[True,False])
def test_five_retries_six_total_attempts(retry_server,tmp_path,eventual_success):
    retry_server['statuses']=[401]*5+([200] if eventual_success else [401])
    path=tmp_path/'calls.sqlite';cfg=options(path)
    cfg['http_error_retry']={'statuses':[401],'max_retries':5,'backoff_s':[0]*5}
    gate=RequestGate(1);ds=prompt(DataAPI().from_items([{'item':'a'}]),options=cfg,request_gate=gate,call_output='call')
    if eventual_success:
        row=ds.checkpoint(tmp_path/'a.jsonl',version='a').take_all()[0]
        assert row['answer']=='ok' and row['call']['response_ref']['attempt']==6
        assert [a['http_status'] for a in row['call']['attempts']]==[401]*5+[200]
        assert not gate.stopped
    else:
        with pytest.raises(ServiceStopped,match='HTTP 401'):ds.run_stream()
        assert gate.stopped
    assert len(retry_server['requests'])==gate.admitted==6
    assert journal_totals(path)['requests']==6


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
