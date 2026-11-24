"""Native prompt actor parity, async concurrency, failure accounting and lifecycle."""
import asyncio
import json
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import pytest
from demiflow.data.api import DataAPI
from demiflow.operator_llm.parser import parse_prompt_pack
from demiflow.operator_llm.errors import PromptBudgetExceededError, PromptResponseContractError

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


def prompt(ds, *, pack=PACK, **kw):return ds.map_prompt_async('enrich',config=parse_prompt_pack(pack),inputs={'payload':'item'},output='answer',**kw)


def test_schema_retry_shared_usage_and_feedback(server):
    server['respond']=lambda b,i:{'wrong':'x'} if i==0 else {'result':'ok'}
    ctx=DataAPI()
    assert prompt(ctx.from_items([{'item':1}]), pack=PACK.replace('schema_retries: 0','schema_retries: 1'), max_requests=2).run_stream().emitted==1
    usage=ctx.prompt_usage()
    assert usage['provider_requests_failed']==1 and usage['provider_requests_completed']==1
    assert usage['input_tokens']==20 and usage['output_tokens']==4
    assert 'previous response failed' in server['requests'][1]['body']['messages'][0]['content']


def test_sync_and_async_nodes_have_independent_budgets(server):
    ctx=DataAPI()
    sync=ctx.from_items([{'item':1}]).map_prompt('enrich', config=parse_prompt_pack(PACK),
        inputs={'payload':'item'}, output='out', max_requests=1)
    sync.take_all()
    stream=prompt(ctx.from_items([{'item':2}]), max_requests=1)
    stream.run_stream()
    assert len(server['requests'])==2
    with pytest.raises(PromptBudgetExceededError):sync.take_all()
    with pytest.raises(PromptBudgetExceededError):stream.run_stream()
    assert len(server['requests'])==2


def test_invalid_bindings_rejected_without_request(server):
    ds=DataAPI().from_items([{'item':1}])
    with pytest.raises(PromptResponseContractError):ds.map_prompt_async('enrich',config=parse_prompt_pack(PACK),inputs=['bad'],output='x')
    with pytest.raises(PromptResponseContractError):ds.map_prompt_async('enrich',config=parse_prompt_pack(PACK),inputs={'payload':'item'},outputs={'bad':'x'})
    with pytest.raises(KeyError):prompt(DataAPI().from_items([{}])).run_stream()
    assert not server['requests']


def test_azure_async_transport(server):
    pack=PACK.replace('transport: openai_compatible','transport: azure_openai\n      api_version: 2024-02-01')
    prompt(DataAPI().from_items([{'item':1}]), pack=pack).run_stream()
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
    ctx=DataAPI()
    with pytest.raises(PromptBudgetExceededError):prompt(ctx.from_items([{'item':1}]), pack=PACK.replace('schema_retries: 0','schema_retries: 1'), max_requests=1).run_stream()
    assert len(server['requests'])==1
    assert ctx.prompt_usage()['provider_requests_failed']==1


@pytest.mark.parametrize('method', ['map_prompt_async', 'agentmap_async'])
def test_bundle_discovers_async_prompt_configuration(tmp_path,monkeypatch,method):
    from types import SimpleNamespace
    import demiflow.pipeline
    import demiflow.execution.seal.pipeline_sources
    from demiflow.operator_llm.parser import load_referenced_prompt_packs
    pipeline=tmp_path/'pipeline';pipeline.mkdir()
    source=pipeline/'main.py'
    source.write_text(f'data.{method}("enrich", config="p.yaml", inputs=["payload"], output="answer")')
    (pipeline/'p.yaml').write_text(PACK)
    if method == 'agentmap_async':
        from test_agent_config import entries
        agent_path, _, _ = entries(pipeline)
        (pipeline/'p.yaml').write_text(agent_path.read_text())
    monkeypatch.setattr(demiflow.pipeline,'discover_pipeline_definition',lambda root:SimpleNamespace(entrypoint='main.py'))
    monkeypatch.setattr(demiflow.execution.seal.pipeline_sources,'reachable_pipeline_sources',lambda root,entrypoint:[source])
    assert set(load_referenced_prompt_packs(tmp_path))=={'p.yaml'}


def test_explicit_sqlite_lifetime_budget_survives_new_node(server,tmp_path):
    options={'sqlite_journal':{'path':str(tmp_path/'calls.sqlite'),'max_requests':1}}
    prompt(DataAPI().from_items([{'item':1}]),options=options,max_requests=1).run_stream()
    prompt(DataAPI().from_items([{'item':1}]),options=options,max_requests=1).run_stream()
    assert len(server['requests'])==1
    with pytest.raises(PromptBudgetExceededError,match='Persistent'):
        prompt(DataAPI().from_items([{'item':2}]),options=options,max_requests=1).run_stream()
    assert len(server['requests'])==1


@pytest.mark.parametrize('method', ['map_prompt', 'map_prompt_async'])
def test_nodes_bind_separate_models_options_and_budgets(server, method):
    """同一链的两次模型调用各有配置和上限；后来修改 options 不改变已声明节点。"""
    data = DataAPI()
    first_options = {'request_options': {'max_tokens': 13}}
    ds = getattr(data.from_items([{'item': 1}]), method)(
        'enrich', config=parse_prompt_pack(PACK.replace('mock-model', 'model-a')),
        options=first_options, max_requests=1, inputs={'payload': 'item'}, output='first',
    )
    ds = getattr(ds, method)(
        'enrich', config=parse_prompt_pack(PACK.replace('mock-model', 'model-b')),
        options={'request_options': {'max_tokens': 29}}, max_requests=1,
        inputs={'payload': 'first'}, output='second',
    )
    first_options['request_options']['max_tokens'] = 999
    assert ds.materialize().take_all() == [{'item': 1, 'first': 'ok', 'second': 'ok'}]
    assert [(r['body']['model'], r['body']['max_tokens']) for r in server['requests']] == [
        ('model-a', 13), ('model-b', 29),
    ]


@pytest.mark.parametrize('method', ['map_prompt', 'map_prompt_async'])
def test_node_budget_covers_all_concurrent_workers(server, method):
    data = DataAPI(workers=4, block_size=1)
    kwargs = {'concurrency': 4} if method == 'map_prompt_async' else {}
    ds = getattr(data.from_items([{'item': n} for n in range(8)]), method)(
        'enrich', config=parse_prompt_pack(PACK), inputs={'payload': 'item'}, output='answer',
        max_requests=2, **kwargs,
    )
    with pytest.raises(PromptBudgetExceededError):
        ds.materialize()
    assert len(server['requests']) <= 2
    assert data.prompt_usage()['provider_requests_reserved'] == 2


def test_config_is_a_real_yaml_path_and_is_bound_at_declaration(server, tmp_path, monkeypatch):
    path = tmp_path / 'prompts' / 'enrich.yaml'
    path.parent.mkdir()
    path.write_text(PACK)
    monkeypatch.chdir(tmp_path)
    ds = DataAPI().from_items([{'item': 1}]).map_prompt(
        'enrich', config='prompts/enrich.yaml', inputs={'payload': 'item'}, output='answer',
    )
    path.write_text('invalid replacement')
    assert ds.take_all()[0]['answer'] == 'ok'


@pytest.mark.parametrize('limit', [-1, True, 1.5])
def test_invalid_node_budget_is_rejected_at_declaration(limit):
    with pytest.raises(ValueError, match='max_requests'):
        prompt(DataAPI().from_items([]), max_requests=limit)
