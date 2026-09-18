"""Native prompt actor parity, async concurrency, failure accounting and lifecycle."""
import asyncio
import json
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import pytest
from demiflow.standalone import local_data
from demiflow.operator_llm.parser import parse_prompt_pack
from demiflow.operator_llm.errors import PromptBudgetExceededError, PromptResponseContractError
from demiflow.operator_llm.runtime import PromptActor
from demiflow.data.plan import AsyncMapOp

PACK = '''
schema_version: demiflow_prompt_pack_v2
prompts:
  enrich:
    version: v1
    model:
      name: mock-model
      transport: openai_compatible
      base_url_env: TEST_PROMPT_URL
      api_key_env: TEST_PROMPT_KEY
    schema_retries: 0
    response_schema:
      type: object
      additionalProperties: false
      required: [result]
      properties:
        result: {type: string}
    template: |
      {{ payload | json }}
'''


@pytest.fixture
def server(monkeypatch):
    state={'requests':[], 'active':0, 'peak':0, 'respond':lambda body,index: {'result':'ok'}, 'status':200}
    lock=threading.Lock()
    class Handler(BaseHTTPRequestHandler):
        def do_POST(self):
            body=json.loads(self.rfile.read(int(self.headers['Content-Length'])))
            with lock:
                index=len(state['requests']);state['requests'].append({'body':body,'path':self.path,'headers':dict(self.headers)})
                state['active']+=1;state['peak']=max(state['peak'],state['active'])
            try:
                answer=state['respond'](body,index)
                value=json.dumps({'choices':[{'message':{'content':json.dumps(answer)}}],
                                  'usage':{'prompt_tokens':10,'completion_tokens':2}}).encode()
                self.send_response(state['status']);self.send_header('Content-Type','application/json');self.end_headers()
                try:self.wfile.write(value)
                except (BrokenPipeError,ConnectionResetError):pass
            finally:
                with lock:state['active']-=1
        def log_message(self,*args):pass
    srv=ThreadingHTTPServer(('127.0.0.1',0),Handler)
    worker=threading.Thread(target=srv.serve_forever,daemon=True);worker.start()
    monkeypatch.setenv('TEST_PROMPT_URL',f'http://127.0.0.1:{srv.server_port}/v1')
    monkeypatch.setenv('TEST_PROMPT_KEY','test')
    yield state
    srv.shutdown();srv.server_close();worker.join()


def context(pack=PACK, **kw):return local_data(prompt_packs={'p.yaml':parse_prompt_pack(pack)},**kw)
def prompt(ds, **kw):return ds.map_prompt_async('enrich',config='p.yaml',inputs={'payload':'item'},output='answer',**kw)


def test_numbered_images_reach_http_in_order_and_journal_replay(server,tmp_path):
    pack=PACK.replace('{{ payload | json }}','{{ payload | json }}\n      {{ pictures | numbered_image }}')
    parsed=parse_prompt_pack(pack)
    assert parsed.prompt_definitions['enrich'].input_modalities==('text','image')
    pictures=['data:image/png;base64,AAAA','data:image/png;base64,BBBB']
    for attempt in range(2):
        ctx=context(pack,prompt_options={'journal_dir':str(tmp_path/'calls')})
        rows=(ctx.from_items([{'item':{'task':'compare'},'pixels':pictures}])
              .map_prompt_async('enrich',config='p.yaml',inputs={'payload':'item','pictures':'pixels'},output='answer')
              .checkpoint(tmp_path/f'out{attempt}.jsonl',version='v1').take_all())
        assert rows[0]['answer']=='ok'
    assert len(server['requests'])==1
    content=server['requests'][0]['body']['messages'][1]['content']
    for index,url in enumerate(pictures,1):
        pos=next(i for i,part in enumerate(content) if part.get('image_url',{}).get('url')==url)
        assert content[pos-1]=={'type':'text','text':f'\nImage {index}:\n'}


def test_sync_async_contract_parity_and_checkpoint_reuse(server,tmp_path):
    ctx=context();items=[{'id':i,'item':{'x':i}} for i in range(3)]
    sync=ctx.from_items(items).map_prompt('enrich',config='p.yaml',inputs={'payload':'item'},output='answer').take_all()
    stream=prompt(ctx.from_items(items),concurrency=2,queue_depth=1)
    assert isinstance(stream._stages[-1],PromptActor)
    assert isinstance(stream._plan.operations[-1],AsyncMapOp)
    async_rows=stream.checkpoint(tmp_path/'rows.jsonl',version='v1').take_all()
    assert sorted(sync,key=lambda r:r['id'])==sorted(async_rows,key=lambda r:r['id'])
    assert len(server['requests'])==6
    stream.checkpoint(tmp_path/'rows.jsonl',version='v1').take_all()
    assert len(server['requests'])==6
    assert ctx.prompt_usage()['provider_requests_completed']==6


def test_real_concurrency_and_same_loop_close(server,monkeypatch):
    from demiflow.operator_llm.client import AsyncOperatorLLMClient
    loops=[];closed=[];original=AsyncOperatorLLMClient.execute;close=AsyncOperatorLLMClient.aclose
    async def execute(self,request):loops.append(asyncio.get_running_loop());return await original(self,request)
    async def aclose(self):closed.append(asyncio.get_running_loop());await close(self)
    monkeypatch.setattr(AsyncOperatorLLMClient,'execute',execute);monkeypatch.setattr(AsyncOperatorLLMClient,'aclose',aclose)
    def respond(body,index):time.sleep(.08);return {'result':'ok'}
    server['respond']=respond
    ds=prompt(context().from_items([{'item':i} for i in range(8)]),concurrency=3,queue_depth=1)
    assert ds.run_stream().emitted==8
    assert 1<server['peak']<=3
    assert len(closed)==1 and all(loop is closed[0] for loop in loops)
    assert ds._stages[-1]._runtime._clients=={}
    # Same plan can run in a new event loop with a fresh client after closure.
    assert ds.run_stream().emitted==8
    assert len(closed)==2 and closed[0] is not closed[1]


def test_schema_retry_shared_usage_and_feedback(server):
    server['respond']=lambda b,i:{'wrong':'x'} if i==0 else {'result':'ok'}
    ctx=context(PACK.replace('schema_retries: 0','schema_retries: 1'),max_prompt_requests=2)
    assert prompt(ctx.from_items([{'item':1}])).run_stream().emitted==1
    usage=ctx.prompt_usage()
    assert usage['provider_requests_failed']==1 and usage['provider_requests_completed']==1
    assert usage['input_tokens']==20 and usage['output_tokens']==4
    assert 'previous response failed' in server['requests'][1]['body']['messages'][0]['content']


def test_budget_counts_sync_async_and_retries(server):
    ctx=context(max_prompt_requests=1)
    ctx.from_items([{'item':1}]).map_prompt('enrich',config='p.yaml',inputs={'payload':'item'},output='out').take_all()
    with pytest.raises(PromptBudgetExceededError):prompt(ctx.from_items([{'item':2}])).run_stream()
    assert len(server['requests'])==1


def test_invalid_bindings_rejected_without_request(server):
    ds=context().from_items([{'item':1}])
    with pytest.raises(PromptResponseContractError):ds.map_prompt_async('enrich',config='p.yaml',inputs=['bad'],output='x')
    with pytest.raises(PromptResponseContractError):ds.map_prompt_async('enrich',config='p.yaml',inputs={'payload':'item'},outputs={'bad':'x'})
    with pytest.raises(KeyError):prompt(context().from_items([{}])).run_stream()
    assert not server['requests']


def test_http_errors_not_retried_and_catch_is_explicit(server):
    import httpx
    server['status']=503
    ctx=context()
    with pytest.raises(httpx.HTTPStatusError):prompt(ctx.from_items([{'item':1}])).run_stream()
    assert len(server['requests'])==1
    stats=prompt(ctx.from_items([{'item':2}]),catch=(httpx.HTTPStatusError,)).run_stream()
    assert stats.emitted==0 and sum(stats.miss.values())==1
    assert ctx.prompt_usage()['provider_requests_failed']==2
    assert len(server['requests'])==2


def test_schema_failure_does_not_publish_checkpoint(server,tmp_path):
    server['respond']=lambda b,i:{'result':99}
    ctx=context()
    with pytest.raises(PromptResponseContractError):prompt(ctx.from_items([{'item':1}])).checkpoint(tmp_path/'failed.jsonl',version='v1')
    assert not (tmp_path/'failed.jsonl').exists()
    assert list(tmp_path.glob('*.partial'))
    assert ctx.prompt_usage()['provider_requests_failed']==1


def test_images_and_multiple_output_mapping(server):
    pack=PACK.replace('required: [result]','required: [result, n]').replace('        result: {type: string}', '        result: {type: string}\n        n: {type: integer}').replace('{{ payload | json }}','{{ payload | json }}\n      {{ picture | image }}')
    server['respond']=lambda b,i:{'result':'ok','n':2}
    seen=[]
    (context(pack).from_items([{'item':1,'image':'https://example.org/a.png','keep':True}])
     .map_prompt_async('enrich',config='p.yaml',inputs={'payload':'item','picture':'image'},outputs={'result':'answer','n':'count'})
     .map_async(lambda row:seen.append(row) or row).run_stream())
    assert seen[0]['answer']=='ok' and seen[0]['count']==2 and seen[0]['keep']
    parts=server['requests'][0]['body']['messages'][1]['content']
    assert any(p.get('image_url',{}).get('url')=='https://example.org/a.png' for p in parts)


def test_azure_async_transport(server):
    pack=PACK.replace('transport: openai_compatible','transport: azure_openai\n      api_version: 2024-02-01')
    prompt(context(pack).from_items([{'item':1}])).run_stream()
    request=server['requests'][0]
    assert '/openai/deployments/mock-model/chat/completions?api-version=2024-02-01' in request['path']
    assert request['headers']['api-key']=='test'


def test_cancelled_request_releases_reservation_and_closes_on_loop(monkeypatch):
    from demiflow.operator_llm import runtime
    from demiflow.operator_llm.runtime import AsyncOperatorLLMRuntime,InProcessOperatorLLMCoordinator
    entered=asyncio.Event();closed=[]
    class Client:
        async def execute(self,request):entered.set();await asyncio.Future()
        async def aclose(self):closed.append(asyncio.get_running_loop())
    monkeypatch.setattr(runtime,'create_async_operator_llm_client',lambda model:Client())
    coordinator=InProcessOperatorLLMCoordinator();rt=AsyncOperatorLLMRuntime(parse_prompt_pack(PACK),coordinator)
    async def run():
        task=asyncio.create_task(rt.call('enrich',{'payload':1}));await entered.wait();task.cancel()
        with pytest.raises(asyncio.CancelledError):await task
        await rt.aclose()
        assert closed==[asyncio.get_running_loop()]
    asyncio.run(run())
    assert coordinator.usage().requests_failed==1 and not coordinator._reservations


def test_schema_retry_cannot_exceed_budget(server):
    server['respond']=lambda b,i:{'result':False}
    ctx=context(PACK.replace('schema_retries: 0','schema_retries: 1'),max_prompt_requests=1)
    with pytest.raises(PromptBudgetExceededError):prompt(ctx.from_items([{'item':1}])).run_stream()
    assert len(server['requests'])==1
    assert ctx.prompt_usage()['provider_requests_failed']==1


def test_sync_transport_has_no_hidden_http_retry(server):
    import requests
    server['status']=503
    ctx=context()
    with pytest.raises(requests.HTTPError):
        ctx.from_items([{'item':1}]).map_prompt('enrich',config='p.yaml',inputs={'payload':'item'},output='out').take_all()
    assert len(server['requests'])==1


def test_stream_failure_cancels_inflight_prompt_and_closes_actor(monkeypatch):
    from demiflow.operator_llm import runtime
    class Client:
        def __init__(self):self.entered=0;self.ready=asyncio.Event();self.closed=False
        async def execute(self,request):
            self.entered+=1;first=self.entered==1
            if self.entered==2:self.ready.set()
            await self.ready.wait()
            if first:raise RuntimeError('request failed')
            await asyncio.Future()
        async def aclose(self):self.closed=True
    clients=[]
    def make(model):
        client=Client();clients.append(client);return client
    monkeypatch.setattr(runtime,'create_async_operator_llm_client',make)
    ctx=context();ds=prompt(ctx.from_items([{'item':1},{'item':2}]),concurrency=2)
    with pytest.raises(RuntimeError,match='request failed'):ds.run_stream()
    assert clients[0].closed
    usage=ctx.prompt_usage()
    assert usage['provider_requests_started']==2 and usage['provider_requests_failed']==2
    assert not ctx._executor._operator_llm_coordinator._reservations


def test_missing_pack_and_unsupported_executor_fail_at_declaration():
    with pytest.raises(ValueError,match='not loaded'):
        prompt(local_data().from_items([]))
    ds=context().from_items([]);ds._executor=object()
    with pytest.raises(NotImplementedError):prompt(ds)


def test_bundle_discovers_async_prompt_configuration(tmp_path,monkeypatch):
    from types import SimpleNamespace
    import demiflow.pipeline
    import demiflow.execution.pipeline_sources
    from demiflow.operator_llm.parser import load_referenced_prompt_packs
    pipeline=tmp_path/'pipeline';pipeline.mkdir()
    source=pipeline/'main.py'
    source.write_text('data.map_prompt_async("enrich", config="p.yaml", inputs=["payload"], output="answer")')
    (pipeline/'p.yaml').write_text(PACK)
    monkeypatch.setattr(demiflow.pipeline,'discover_pipeline_definition',lambda root:SimpleNamespace(entrypoint='main.py'))
    monkeypatch.setattr(demiflow.execution.pipeline_sources,'reachable_pipeline_sources',lambda root,entrypoint:[source])
    assert set(load_referenced_prompt_packs(tmp_path))=={'p.yaml'}


def test_durable_replay_budget_and_uncertain_call(server,tmp_path):
    from demiflow.operator_llm.journal import UncertainPromptCall
    options={'journal_dir':str(tmp_path/'calls')}
    ctx=context(max_prompt_requests=1,prompt_options=options)
    row=prompt(ctx.from_items([{'item':1}]),call_output='call').checkpoint(tmp_path/(__import__("uuid").uuid4().hex+".jsonl"),version="test").take_all()[0]
    assert row['answer']=='ok' and not row['call']['reused']
    request=json.loads(__import__('pathlib').Path(row['call']['request_path']).read_text())
    assert 'test' not in json.dumps(request)  # never archive authorization headers
    fresh=context(max_prompt_requests=1,prompt_options=options)
    replay=prompt(fresh.from_items([{'item':1}]),call_output='call').checkpoint(tmp_path/(__import__("uuid").uuid4().hex+".jsonl"),version="test").take_all()[0]
    assert replay['call']['reused'] and len(server['requests'])==1
    assert fresh.prompt_usage()['provider_requests_started']==0
    with pytest.raises(PromptBudgetExceededError):prompt(fresh.from_items([{'item':2}])).checkpoint(tmp_path/(__import__("uuid").uuid4().hex+".jsonl"),version="test").take_all()
    __import__('pathlib').Path(row['call']['response_path']).unlink()
    with pytest.raises(UncertainPromptCall):prompt(context(prompt_options=options).from_items([{'item':1}])).checkpoint(tmp_path/(__import__("uuid").uuid4().hex+".jsonl"),version="test").take_all()
    assert len(server['requests'])==1


def test_prompt_skip_and_error_rows_keep_original_data(server,tmp_path):
    server['status']=503
    options={'journal_dir':str(tmp_path/'calls')}
    items=[{'item':1,'skip':True},{'item':2,'skip':False}]
    result=prompt(context(prompt_options=options).from_items(items),when=lambda r:not r['skip'],error_output='error').checkpoint(tmp_path/(__import__("uuid").uuid4().hex+".jsonl"),version="test").take_all()
    assert result[0]==items[0]
    assert result[1]['item']==2 and result[1]['error']['type']=='PromptResponseContractError'
    assert result[1]['error']['call']['response_path']
    prompt(context(prompt_options=options).from_items(items),when=lambda r:not r['skip'],error_output='error').checkpoint(tmp_path/(__import__("uuid").uuid4().hex+".jsonl"),version="test").take_all()
    assert len(server['requests'])==1
