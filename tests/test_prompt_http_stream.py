"""Real socket lifecycle and bounded SSE failures through Dataset.map_prompt_async."""
import asyncio
import json
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import httpx
import pytest
from demiflow.data.api import DataAPI
from demiflow.operator_llm.client import AsyncOperatorLLMClient
from demiflow.operator_llm.errors import PromptStreamError
from demiflow.operator_llm.http_stream import ChatCompletionStream
from demiflow.operator_llm.model import OperatorLLMRequest, PromptModel, TextPart
from demiflow.operator_llm.call_ref import PromptRecordRef, journal_totals
from demiflow.operator_llm.journal import UncertainPromptCall
from demiflow.execution.request_limits import RequestGate
from test_map_prompt_async import PACK, prompt


def event(value):
    return ('data: ' + (value if isinstance(value, str) else json.dumps(value, ensure_ascii=False)) + '\r\n\r\n').encode()


def chunk(content='', finish=None):
    return {'id':'fixture', 'model':'mock-model', 'choices':[{'index':0, 'delta':{'content':content}, 'finish_reason':finish}]}


def wire(answer='中文', *, done=True, finish='stop'):
    return (b'\xef\xbb\xbf: heartbeat\r\n\r\n' + event(chunk('{"result":'))
        + event(chunk(json.dumps(answer, ensure_ascii=False)+'}', finish))
        + event({'choices':[], 'usage':{'prompt_tokens':11,'completion_tokens':7}})
        + (event('[DONE]') if done else b''))


def test_utf8_crlf_bom_cross_byte_boundaries_and_usage_snapshot():
    parser=ChatCompletionStream({})
    for i, byte in enumerate(wire()):parser.feed(bytes([byte]), i/100)
    body=parser.complete()
    assert json.loads(body['choices'][0]['message']['content'])=={'result':'中文'}
    assert body['usage']=={'prompt_tokens':11,'completion_tokens':7}
    assert parser.metrics['first_event_s'] < parser.metrics['first_content_s'] + .01


@pytest.mark.parametrize('raw', [wire(done=False), wire(finish='length'), event('[DONE]'),
    event('{bad json'), event({'error':{'message':'failure'}}),
    event({'choices':[{'index':1,'delta':{'content':'wrong'}}]}),
    event({'choices':[{'index':0,'delta':{'tool_calls':[{}]}}]}),
    event(chunk('{}','stop'))+event(chunk('late'))+event('[DONE]'),
])
def test_incomplete_or_unsupported_never_succeeds(raw):
    parser=ChatCompletionStream({})
    with pytest.raises(PromptStreamError):
        parser.feed(raw, 1)
        parser.complete()


def test_size_and_event_limits_and_multiline_data():
    parser=ChatCompletionStream({})
    parser.feed(b'data: {"choices":\ndata: []}\n\n', 0)
    assert parser.metrics['events_received']==1
    for options, raw in [({'max_response_bytes':10},b'x'*11),
                         ({'max_event_bytes':10},b'data: '+'中'.encode()*4),
                         ({'max_stream_events':1},event(chunk())*2)]:
        with pytest.raises(PromptStreamError):ChatCompletionStream(options).feed(raw,0)


@pytest.fixture
def sse_server(monkeypatch):
    state={'requests':[], 'ports':[], 'active':0, 'peak':0, 'delay':.005, 'done':True, 'hold':False}
    lock=threading.Lock()
    class Handler(BaseHTTPRequestHandler):
        protocol_version='HTTP/1.1'
        def do_POST(self):
            body=json.loads(self.rfile.read(int(self.headers['Content-Length'])))
            answer=body['messages'][-1]['content']
            with lock:
                index=len(state['requests'])
                state['requests'].append(body);state['ports'].append(self.client_address[1]);state['active']+=1
                state['peak']=max(state['peak'],state['active'])
            try:
                raw=(state['respond'](body,index) if 'respond' in state
                     else wire(answer,done=state['done']))
                self.send_response(200);self.send_header('Content-Type','text/event-stream')
                self.send_header('Content-Length',str(len(raw)));self.end_headers()
                for offset in range(0,len(raw),47):
                    self.wfile.write(raw[offset:offset+47]);self.wfile.flush()
                    time.sleep(.5 if state['hold'] else state['delay'])
            except (BrokenPipeError,ConnectionResetError):pass
            finally:
                with lock:state['active']-=1
        def log_message(self,*args):pass
    server=ThreadingHTTPServer(('127.0.0.1',0),Handler)
    thread=threading.Thread(target=server.serve_forever,daemon=True);thread.start()
    monkeypatch.setenv('TEST_PROMPT_URL', f'http://127.0.0.1:{server.server_port}/v1')
    monkeypatch.setenv('TEST_PROMPT_KEY','fixture')
    yield state
    server.shutdown();server.server_close();thread.join()


@pytest.mark.parametrize('adaptive',[False,True])
def test_concurrent_rows_pool_reuse_gate_lifetime_and_replay(sse_server,tmp_path,adaptive):
    if adaptive:
        from demiflow.execution.adaptive_requests import AdaptiveRequestGate
        gate=AdaptiveRequestGate(dict(initial_concurrency=2,max_concurrency=2),state_path=tmp_path/'gate.json')
    else:
        gate=RequestGate(2)
    options={'stream':True,'gateway':'litellm','trust_env':False,
             'sqlite_journal':{'path':str(tmp_path/'calls.sqlite'),'max_requests':6}}
    ds=prompt(DataAPI().from_items([{'item':i} for i in range(6)]),concurrency=3,
              request_gate=gate, options=options,call_output='call')
    rows=ds.checkpoint(tmp_path/'a.jsonl',version='a').take_all()
    assert sorted(json.loads(r['answer']) for r in rows)==list(range(6))
    assert 1<sse_server['peak']<=2 and gate.peak==2
    assert len(set(sse_server['ports']))<=2  # sockets survive successive requests
    assert all(r['call']['stream_complete'] and not r['call']['reused'] for r in rows)
    assert all(r['call']['usage']['completion_tokens']==7 for r in rows)
    assert all(b['num_retries']==b['max_retries']==0 and b['fallbacks']==[] for b in sse_server['requests'])
    fresh_count=gate.fresh_responses if adaptive else None
    reused=ds.checkpoint(tmp_path/'b.jsonl',version='b').take_all()
    assert len(sse_server['requests'])==6 and all(r['call']['reused'] for r in reused)
    if adaptive:
        assert fresh_count==6
        assert gate.fresh_responses==0  # new action resets observations; replay never enters the gate
    assert journal_totals(tmp_path/'calls.sqlite')['input_tokens']==66


def test_truncation_saved_once_never_schema_retried_or_automatically_requeued(sse_server,tmp_path):
    sse_server['done']=False
    options={'stream':True,'trust_env':False,'sqlite_journal':{'path':str(tmp_path/'calls.sqlite')}}
    ds=prompt(DataAPI().from_items([{'item':'case'}]),pack=PACK.replace('schema_retries: 0','schema_retries: 1'),
              options=options,error_output='error')
    failed=ds.checkpoint(tmp_path/'a.jsonl',version='a').take_all()[0]['error']
    assert failed['category']=='incomplete_response'
    assert 'partial_response' not in failed['call']
    ref=failed['call']['error_ref']
    saved=PromptRecordRef(**ref).read()
    assert json.loads(saved['call']['partial_response']['choices'][0]['message']['content'])['result']
    assert saved['call']['usage']['completion_tokens']==7
    assert journal_totals(tmp_path/'calls.sqlite')['output_tokens']==7
    again=ds.checkpoint(tmp_path/'b.jsonl',version='b').take_all()[0]['error']
    assert again['category']=='uncertain_call' and len(sse_server['requests'])==1


def test_read_idle_and_total_timeouts_are_distinct(sse_server,tmp_path):
    for name, options in [('read_idle',{'read_timeout_s':.06,'timeout_s':1}),
                          ('total',{'read_timeout_s':1,'timeout_s':.06})]:
        sse_server['hold']=True
        ds=prompt(DataAPI().from_items([{'item':name}]),options={'stream':True,'trust_env':False,**options,
                  'sqlite_journal':{'path':str(tmp_path/(name+'.sqlite'))}},error_output='error')
        row=ds.checkpoint(tmp_path/(name+'.jsonl'),version='v').take_all()[0]
        call=row['error']['call']
        assert call['timeout_phase']==name and not call['stream_complete']
    assert len(sse_server['requests'])==2


def test_cancel_closes_response_client_and_saves_partial(monkeypatch,tmp_path):
    monkeypatch.setenv('TEST_PROMPT_KEY','fixture')
    closed=[];received=asyncio.Event()
    class Body(httpx.AsyncByteStream):
        async def __aiter__(self):
            yield event(chunk('{"result":'))
            received.set()
            await asyncio.Event().wait()
        async def aclose(self):closed.append('response')
    async def exercise():
        model=PromptModel('mock-model','openai_compatible',base_url='http://fixture/v1',api_key_env='TEST_PROMPT_KEY')
        client=AsyncOperatorLLMClient(model,{'stream':True,'sqlite_journal':{'path':str(tmp_path/'cancel.sqlite')}})
        client.client=httpx.AsyncClient(transport=httpx.MockTransport(lambda r:httpx.Response(200,stream=Body(),headers={'content-type':'text/event-stream'})))
        request=OperatorLLMRequest('test','v1',model.name,(TextPart('input'),))
        task=asyncio.create_task(client.execute(request))
        await received.wait();task.cancel()
        with pytest.raises(asyncio.CancelledError):await task
        with pytest.raises(UncertainPromptCall):client.lookup(request)
        await client.aclose()
        assert client.client.is_closed
    asyncio.run(exercise())
    assert closed==['response']
    assert journal_totals(tmp_path/'cancel.sqlite')['transport_errors']==1


def test_invalid_transport_configuration_fails_at_declaration():
    for options in ({'stream':'yes'}, {'gateway':'unknown'}, {'read_timeout_s':0},
                    {'max_connections':1,'max_keepalive_connections':2},
                    {'stream':True,'request_options':{'stream':True}},
                    {'stream':True,'request_options':{'n':2}},
                    {'offline_store':{'path':'unused'},'stream':True}):
        with pytest.raises(ValueError):prompt(DataAPI().from_items([]),options=options)


def test_cancel_during_response_commit_preserves_cache(monkeypatch,tmp_path):
    from demiflow.operator_llm.sqlite_journal import SQLitePromptJournal
    monkeypatch.setenv('TEST_PROMPT_KEY','fixture')
    writing=threading.Event();release=threading.Event();original=SQLitePromptJournal.response
    def slow_commit(self,*args):
        writing.set();assert release.wait(2)
        return original(self,*args)
    monkeypatch.setattr(SQLitePromptJournal,'response',slow_commit)
    class Body(httpx.AsyncByteStream):
        async def __aiter__(self):yield wire()
    async def exercise():
        model=PromptModel('mock-model','openai_compatible',base_url='http://fixture/v1',api_key_env='TEST_PROMPT_KEY')
        client=AsyncOperatorLLMClient(model,{'stream':True,'sqlite_journal':{'path':str(tmp_path/'commit.sqlite')}})
        client.client=httpx.AsyncClient(transport=httpx.MockTransport(lambda r:httpx.Response(200,stream=Body(),headers={'content-type':'text/event-stream'})))
        request=OperatorLLMRequest('test','v1',model.name,(TextPart('input'),))
        task=asyncio.create_task(client.execute(request))
        assert await asyncio.to_thread(writing.wait,2)
        task.cancel();release.set()
        with pytest.raises(asyncio.CancelledError):await task
        assert client.lookup(request).metadata['reused']
        await client.aclose()
    asyncio.run(exercise())
    assert journal_totals(tmp_path/'commit.sqlite')['responses']==1
    assert journal_totals(tmp_path/'commit.sqlite')['transport_errors']==0


@pytest.mark.parametrize('request_timeout,expected_responses', [(2, 1), (.1, 0)])
@pytest.mark.parametrize('failure_kind', ['service', 'sqlite'])
def test_service_stop_drains_paid_response_but_does_not_publish_partial_success(
        monkeypatch, tmp_path, request_timeout, expected_responses, failure_kind):
    import sqlite3
    from demiflow.execution.request_limits import ServiceStopped
    error = (ServiceStopped('search engine unavailable') if failure_kind == 'service'
             else sqlite3.OperationalError('database is locked'))
    import demiflow.execution.stream as execution
    monkeypatch.setenv('TEST_PROMPT_URL','http://fixture/v1')
    monkeypatch.setenv('TEST_PROMPT_KEY','fixture')
    monkeypatch.setattr(execution,'_DRAIN_TIMEOUT',.01)
    monkeypatch.setattr(execution,'_WATCHDOG_INTERVAL',.01)
    original=httpx.AsyncClient;state={'received':None,'requests':0,'closed':False}
    class Body(httpx.AsyncByteStream):
        async def __aiter__(self):
            yield event(chunk('{"result":'))
            state['received'].set()
            await asyncio.sleep(.4)  # survives watchdog cancellation and old drain deadline
            yield event(chunk('"saved"}','stop'))+event('[DONE]')
        async def aclose(self):state['closed']=True
    def handler(request):
        state['requests']+=1
        return httpx.Response(200,stream=Body(),headers={'content-type':'text/event-stream'})
    monkeypatch.setattr(httpx,'AsyncClient',lambda **kw:original(**{**kw,'transport':httpx.MockTransport(handler)}))
    async def upstream(row):
        if state['received'] is None:state['received']=asyncio.Event()
        if row['item']=='stop':
            await state['received'].wait()
            raise error
        return row
    path=tmp_path/'drain.sqlite';published=[]
    ds=prompt(DataAPI().from_items([{'item':'paid'},{'item':'stop'}]).map_async(upstream,concurrency=2),
        options={'stream':True,'timeout_s':request_timeout,'sqlite_journal':{'path':str(path)}})
    with pytest.raises(type(error), match=str(error)) as caught:
        ds.map(lambda row:published.append(row) or row).run_stream()
    assert caught.value is error
    totals=journal_totals(path)
    assert state['requests']==1 and state['closed'] and not published
    assert totals['responses'] == expected_responses
    assert totals['transport_errors'] == 1 - expected_responses


def test_raw_stream_log_opt_in_and_pool_wait_timeout(sse_server,tmp_path):
    sse_server['hold']=True
    options={'stream':True,'trust_env':False,'max_connections':1,'pool_timeout_s':.03,'read_timeout_s':.07,
             'stream_log_dir':str(tmp_path/'raw'),'sqlite_journal':{'path':str(tmp_path/'pool.sqlite')}}
    ds=prompt(DataAPI().from_items([{'item':'a'},{'item':'b'}]),concurrency=2,options=options,error_output='error')
    rows=ds.checkpoint(tmp_path/'pool.jsonl',version='v').take_all()
    assert {r['error']['call']['timeout_phase'] for r in rows}=={'pool','read_idle'}
    assert len(sse_server['requests'])==1
    logs=list((tmp_path/'raw').glob('*.sse'))
    assert len(logs)==2 and any(p.stat().st_size for p in logs)


@pytest.mark.parametrize('status,body,content_type,expected',[
    (408,b'{"error":{"message":"upstream 524"}}','application/json','provider_error'),
    (200,event({'error':{'message':'upstream disconnected'}}),'text/event-stream','incomplete_response'),
    (200,b'{"choices":[]}','application/json','incomplete_response'),
])
def test_failure_evidence_and_no_hidden_retries(monkeypatch,tmp_path,status,body,content_type,expected):
    monkeypatch.setenv('TEST_PROMPT_URL','http://fixture/v1');monkeypatch.setenv('TEST_PROMPT_KEY','fixture')
    requests=[];original=httpx.AsyncClient
    class Body(httpx.AsyncByteStream):
        async def __aiter__(self):yield body
    def handler(request):
        requests.append(request)
        return httpx.Response(status,stream=Body(),headers={'content-type':content_type})
    monkeypatch.setattr(httpx,'AsyncClient',lambda **kw:original(**{**kw,'transport':httpx.MockTransport(handler)}))
    options={'stream':True,'sqlite_journal':{'path':str(tmp_path/'failure.sqlite')}}
    row=prompt(DataAPI().from_items([{'item':'test'}]),options=options,error_output='error').checkpoint(tmp_path/'a.jsonl',version='a').take_all()[0]
    assert row['error']['category']==expected and len(requests)==1 and 'answer' not in row
    call=row['error']['call']
    ref=call['response_ref'] if status!=200 else call['error_ref']
    saved=PromptRecordRef(**ref).read()
    assert ('upstream' in json.dumps(saved)) or 'Expected text/event-stream' in json.dumps(saved)
    assert not (tmp_path/'raw').exists()


def test_pool_timeouts_do_not_change_identity_but_wire_stream_option_does(monkeypatch):
    monkeypatch.setenv('TEST_PROMPT_KEY','fixture')
    model=PromptModel('mock-model','openai_compatible',base_url='http://fixture/v1',api_key_env='TEST_PROMPT_KEY')
    request=OperatorLLMRequest('test','v1',model.name,(TextPart('input'),))
    plain=AsyncOperatorLLMClient(model).record(request)
    tuned=AsyncOperatorLLMClient(model,{'stream':False,'max_connections':2,'read_timeout_s':25}).record(request)
    streamed=AsyncOperatorLLMClient(model,{'stream':True}).record(request)
    assert plain.request_key==tuned.request_key and plain.request_key!=streamed.request_key
    assert 'stream' not in plain['payload'] and 'num_retries' not in plain['payload']
