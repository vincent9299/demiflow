"""CPU contracts for native tensor stages and the shared performance preset."""
from concurrent.futures import ThreadPoolExecutor
import threading

import pytest

from demiflow.services.cuda_graphs import ExecutionGate, RequestLease
from demiflow.services.deployment import deployment_config
from demiflow.services.profiles import build_service
from demiflow.services.native_graphs import tree_map, tree_pairs, signature


def test_two_reader_upgrades_do_not_deadlock_or_allow_unprotected_gpu_work():
    gate = ExecutionGate(); ready = threading.Barrier(2); events = []
    def call(index):
        def sync(): events.append(('sync', index))
        with RequestLease(gate, sync) as lease:
            ready.wait(5)
            with lease.exclusive():
                assert not gate.readers and gate.writing
                events.append(('writer', index))
            assert gate.readers >= 1 and not gate.writing
    with ThreadPoolExecutor(max_workers=2) as pool:
        futures = [pool.submit(call, i) for i in range(2)]
        for future in futures: future.result(10)
    assert len([e for e in events if e[0]=='writer']) == 2
    assert not gate.readers and not gate.writing
    with pytest.raises(ValueError):
        with RequestLease(gate, lambda:None) as lease:
            with lease.exclusive(): raise ValueError('native failure')
    assert not gate.readers and not gate.writing


@pytest.mark.parametrize('profile', ['pplx','amos','d3','autotrust-jev'])
def test_performance_recipe_is_inert_and_resolves_to_backend_execution(profile, tmp_path):
    cfg = deployment_config(dict(profile=profile, weights='/weights', revision='test', gpus=[0],
                                 performance='optimized', directory=str(tmp_path)))
    manifest = dict(id='test',python='/python',ports=[21000,21001],model_contract={},runtime={},adapter_version='x')
    service = build_service('test',cfg,manifest)
    if profile == 'autotrust-jev':
        assert 'request_pipelining' not in cfg['options'] and 'max_num_seqs' not in cfg['options']
        assert '--jev-options' in service.command and '--cuda-graphs' in service.command
        assert '--backend' not in service.command
        assert cfg['options']['cuda_graphs']['memory_limit_mib']==75776
    else:
        assert cfg['options']['cuda_graphs']['streams']==2
        assert '--cuda-graphs' in service.command
    assert not list(tmp_path.iterdir())


def test_unsafe_preset_combinations_fail_without_starting_anything():
    base=dict(profile='autotrust-jev',weights='/weights',revision='test',gpus=[0],performance='optimized')
    for changes in [dict(options={'request_pipelining':False}),dict(profile='vllm'),dict(performance='fast'),
                    dict(batch_evidence='/proof.json',batch_max_size=2)]:
        with pytest.raises(ValueError): deployment_config({**base,**changes})


def test_tensor_tree_preserves_structure_and_constant_values():
    tree=([1,2],{'x':None,'lengths':(3,4)})
    assert tree_map(tree,lambda x:x)==tree
    assert list(tree_pairs(tree,tree))==[(1,1),(2,2),(None,None),(3,3),(4,4)]
    class Torch:
        class Tensor: pass
    assert signature((1,2),Torch)!=signature((2,1),Torch)
    assert signature([1,2],Torch)!=signature((1,2),Torch)
    with pytest.raises(ValueError): signature(object(),Torch)


def test_native_capture_references_overlap_before_exclusive_capture():
    import threading
    from types import SimpleNamespace
    from demiflow.services.native_graphs import NativeGraphAdapter
    from demiflow.services.cuda_graphs import GraphCache, graph_config
    adapter=NativeGraphAdapter.__new__(NativeGraphAdapter)
    adapter.config=graph_config({'min_observations':1});adapter.gate=ExecutionGate()
    adapter.cache=GraphCache(adapter.config);adapter.closed=False;adapter.failed=None
    adapter._estimate=lambda n:1024
    adapter.torch=SimpleNamespace(Tensor=type('Tensor',(),{}))
    adapter.stats=dict(hits=0,native=0,checks=0,mismatches=0)
    adapter.lanes=[dict(graph_allowed=True,stream=SimpleNamespace(synchronize=lambda:None)) for _ in range(2)]
    entered=threading.Barrier(2);leaving=threading.Barrier(2);captured=[]
    def native(value):
        assert not adapter.gate.writing
        entered.wait(5);assert adapter.gate.readers==2;leaving.wait(5)
        return value
    def capture(index,*args):
        assert adapter.gate.writing and adapter.gate.readers==0
        captured.append(index);return None
    adapter._capture=capture
    def execute(index):
        with RequestLease(adapter.gate,lambda:None) as lease:
            adapter.lanes[index]['lease']=lease
            return adapter.call(index,'body',native,(index,),{},128)
    with ThreadPoolExecutor(max_workers=2) as pool:
        futures=[pool.submit(execute,i) for i in range(2)]
        assert [f.result(6) for f in futures]==[0,1]
    assert sorted(captured)==[0,1] and adapter.gate.readers==0


def test_tensor_graph_mismatch_returns_native_without_disabling_independent_lanes():
    from types import SimpleNamespace
    from demiflow.services.native_graphs import NativeGraphAdapter
    from demiflow.services.cuda_graphs import GraphCache, graph_config
    adapter=NativeGraphAdapter.__new__(NativeGraphAdapter)
    adapter.config=graph_config({'verify_every':1}); adapter.parallel=2
    adapter.cache=GraphCache(adapter.config); adapter.gate=ExecutionGate()
    adapter.closed=False; adapter.failed=None; adapter.stats=dict(hits=0,native=0,checks=0,mismatches=0)
    class Tensor: pass
    freed=[]
    adapter.torch=SimpleNamespace(Tensor=Tensor,cuda=SimpleNamespace(empty_cache=lambda:None))
    arguments=((1,),{});shape=('stage',signature(arguments,adapter.torch))
    adapter.cache.put((0,shape),SimpleNamespace(uses=0,bytes=1,close=lambda:freed.append(True)))
    adapter._estimate=lambda tokens:1024
    adapter._replay=lambda *args: [1.001]
    with RequestLease(adapter.gate,lambda:None) as lease:
        adapter.lanes=[dict(graph_allowed=True,lease=lease,stream=SimpleNamespace(synchronize=lambda:None))]
        assert adapter.call(0,'stage',lambda x:[1.0],(1,),{},1)==[1.0]
    assert adapter.cache.disabled and not adapter.cache.entries and freed==[True]
    assert adapter.stats['mismatches']==1 and adapter.parallel==2


def test_tensor_audits_do_not_stop_other_independent_requests():
    from types import SimpleNamespace
    from demiflow.services.native_graphs import NativeGraphAdapter
    from demiflow.services.cuda_graphs import GraphCache, graph_config
    adapter=NativeGraphAdapter.__new__(NativeGraphAdapter)
    adapter.config=graph_config({'verify_every':1}); adapter.parallel=2
    adapter.cache=GraphCache(adapter.config); adapter.gate=ExecutionGate()
    adapter.closed=False; adapter.failed=None
    adapter.stats=dict(hits=0,native=0,checks=0,mismatches=0)
    adapter.torch=SimpleNamespace(Tensor=type('Tensor',(),{}))
    adapter.lanes=[dict(graph_allowed=True,stream=SimpleNamespace(synchronize=lambda:None)) for _ in range(2)]
    arguments=((1,),{}); shape=('decoder',signature(arguments,adapter.torch))
    ready=threading.Barrier(2)
    def native(x):
        ready.wait(5)
        assert adapter.gate.readers==2 and not adapter.gate.writing
        ready.wait(5)
        return [x]
    adapter._replay=lambda *args:[1]
    for i in range(2):
        adapter.cache.put((i,shape),SimpleNamespace(uses=0,bytes=1,close=lambda:None))
    def work(i):
        with RequestLease(adapter.gate,lambda:None) as lease:
            adapter.lanes[i]['lease']=lease
            return adapter.call(i,'decoder',native,(1,),{},1)
    with ThreadPoolExecutor(max_workers=2) as pool:
        assert list(pool.map(work,range(2)))==[[1],[1]]
    assert adapter.stats==dict(hits=2,native=2,checks=2,mismatches=0)


def test_d3_prepared_request_preserves_native_positional_contract_and_restores_on_error():
    from types import SimpleNamespace
    from demiflow.services.native_graphs import D3Backend
    original=lambda *args: 'original'
    model=SimpleNamespace(prepare=original)
    request={'state':{'text':'x'},'questions':{'q':{'type':'noul'}}}
    plan=object()
    def answer(body):
        assert model.prepare(body['state'],body['questions'],[],[]) is plan
        raise ValueError('native refusal')
    decider=SimpleNamespace(model=model,answer=answer)
    with pytest.raises(ValueError,match='native refusal'):
        D3Backend.execute(None,decider,request,plan)
    assert model.prepare is original


def test_d3_admission_counts_recurrent_state_and_retained_convolution_storage():
    from types import SimpleNamespace
    from demiflow.services.native_graphs import D3Backend
    backend=D3Backend.__new__(D3Backend)
    config=SimpleNamespace(layer_types=['linear_attention','full_attention'],linear_num_value_heads=2,
        linear_key_head_dim=8,linear_value_head_dim=8,linear_num_key_heads=1,num_key_value_heads=1,head_dim=8)
    backend.backbone=SimpleNamespace(config=SimpleNamespace(text_config=config))
    fixed=2*2*8*8*4
    assert backend.workspace_extra(0)==fixed
    assert backend.workspace_extra(100)-fixed==2*100*((2*1*8+2*8)*2+2*1*8*2)
    plan=SimpleNamespace(states={'q1':100,'q2':100})
    assert backend.admission_extra(plan,999)==backend.workspace_extra(100)


@pytest.mark.asyncio
async def test_single_performance_report_cannot_enable_batch_and_detects_usage_drift(tmp_path):
    import asyncio
    import json
    from demiflow.services.model_worker import make_server
    from demiflow.services.quality import benchmark,read_evidence
    class Adapter:
        parallel=2
        def __init__(self): self.calls=0
        def single(self, request):
            self.calls+=1
            return {'answers':{'q':{'type':'noul','noul':.5}},
                    'usage':{'input_tokens':1 if self.calls<=203 else 2,'output_tokens':0}}
        def execution_status(self): return {'mode':'cpu_test'}
    server=make_server(Adapter(),'fixture',0,4)
    thread=threading.Thread(target=server.serve_forever);thread.start()
    cases=[{'state':str(i),'questions':{'q':{'type':'noul','instructions':'is valid?'}}} for i in range(200)]
    try:
        report=await benchmark(f'http://127.0.0.1:{server.server_port}/v1','fixture',cases,max_size=1,parallel=2,probability_atol=0)
    finally:
        await asyncio.to_thread(server.shutdown);server.server_close();thread.join()
    assert report['schema_version']=='demiflow_single_performance_v1'
    assert not report['passed'] and report['usage_mismatches']==200
    assert report['tested_batch_sizes']==[] and report['partner_rounds']==0
    assert set(report['metrics'])=={'single','parallel_single'}
    path=tmp_path/'report.json';path.write_text(json.dumps(report))
    with pytest.raises(ValueError):read_evidence(path,report['identity'],2)


@pytest.mark.parametrize('invalid',[{},[],True,1,None])
def test_performance_preset_rejects_non_string_values(invalid):
    with pytest.raises(ValueError,match='performance'):
        deployment_config(dict(profile='amos',weights='/weights',revision='v1',gpus=[0],performance=invalid))


def test_launch_recipe_accepts_frozen_options_from_real_prompt_model(tmp_path):
    from demiflow.operator_llm.model import parse_prompt_model
    raw=dict(profile='amos',weights='/weights',revision='r1',gpus=[0],performance='optimized')
    model=parse_prompt_model({'name':'fixture','deployment':raw},label='model')
    manifest=dict(id='fixture',python='/python',ports=[21000],model_contract={},runtime={},adapter_version='x')
    service=build_service('fixture',model.deployment,manifest)
    import json
    value=service.command[service.command.index('--cuda-graphs')+1]
    assert json.loads(value)['streams']==2


def test_jev_pipeline_keeps_engine_configuration_and_blocks_every_batch_entry(tmp_path):
    from demiflow.operator_llm.model import parse_prompt_model
    from demiflow.operator_llm.client import AsyncOperatorLLMClient
    from demiflow.services.model_adapters import JEVAdapter
    from demiflow.services.model_worker import make_server
    raw=dict(profile='autotrust-jev',weights='/weights',revision='r1',gpus=[0],
             options={'max_num_seqs':8,'request_pipelining':True})
    cfg=deployment_config(raw)
    assert cfg['options']['max_num_seqs']==8
    model=parse_prompt_model({'name':'fixture','deployment':cfg},label='model')
    with pytest.raises(ValueError,match='independent single'):
        AsyncOperatorLLMClient(model,{'systemone_batch':{'max_size':2}})
    adapter=object.__new__(JEVAdapter); adapter.backend_serialized=True
    identity={'model_contract':{'options':{'request_pipelining':True}}}
    for options in [{'identity':None},{'identity':identity,'validation_batch_size':2},
                    {'identity':identity,'batch_evidence':'/unused.json'},
                    {'identity':identity,'batch_max_size':2}]:
        with pytest.raises(ValueError,match='Preparation pipelining'):
            make_server(adapter,'fixture',0,2,backend_serialized=True,**options)
