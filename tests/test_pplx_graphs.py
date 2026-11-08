"""CPU contracts for graph admission, memory policy and capture exclusion."""
from concurrent.futures import ThreadPoolExecutor
import threading
import time
from types import SimpleNamespace

import pytest

from demiflow.services.pplx_graphs import ExecutionGate, GraphCache, graph_config, MiB


def test_capture_references_overlap_and_only_capture_drains_readers():
    from demiflow.services.pplx_graphs import PplxGraphAdapter
    adapter=PplxGraphAdapter.__new__(PplxGraphAdapter)
    adapter.config=graph_config({'min_observations':1});adapter.gate=ExecutionGate()
    adapter.cache=GraphCache(adapter.config);adapter.closed=False;adapter.failed=None
    adapter.lanes=[{'stream':0},{'stream':1}];adapter._estimate=lambda n:1024
    adapter.stats=dict(checks=0,mismatches=0,hits=0)
    entered=threading.Barrier(2);completed=threading.Barrier(2);captures=[]
    def native(lane,chunks):
        assert not adapter.gate.writing
        entered.wait(5);assert adapter.gate.readers==2;completed.wait(5)
        return [[[lane['stream']]]]
    def capture(index,chunk):
        assert adapter.gate.writing and adapter.gate.readers==0
        captures.append(index);return None
    adapter._native=native;adapter._capture=capture
    chunk=SimpleNamespace(length=128,keys=['q'],pixel_values=None,image_grid_thw=None,mm_token_type_ids=None)
    with ThreadPoolExecutor(max_workers=2) as pool:
        futures=[pool.submit(adapter._forward,i,[chunk]) for i in range(2)]
        assert [f.result(6) for f in futures]==[[[[0]]],[[[1]]]]
    assert sorted(captures)==[0,1] and adapter.gate.readers==0


@pytest.mark.parametrize('value',[True,[],{'streams':0},{'streams':True},{'max_entries':257},
    {'unknown':1},{'max_graph_tokens':8192,'max_input_tokens':100},
    {'cache_memory_mib':1024,'memory_limit_mib':1024},{'verify_every':0}])
def test_graph_limits_reject_invalid_or_unbounded_configuration(value):
    with pytest.raises(ValueError): graph_config(value)


def test_graph_policy_bounds_frequency_lru_bytes_and_capture_work():
    cfg=graph_config({'max_entries':2,'cache_memory_mib':64,'min_observations':2,'max_captures':2})
    cache=GraphCache(cfg); closed=[]
    def entry(name,size=24*MiB): return SimpleNamespace(bytes=size,close=lambda:closed.append(name))
    assert not cache.observe((100,1)) and cache.observe((100,1))
    for i in range(100): cache.observe((i,1))
    assert len(cache.history)==8
    cache.make_room(24*MiB); cache.put((0,'a'),entry('a'))
    cache.make_room(24*MiB); cache.put((0,'b'),entry('b'))
    assert cache.get((0,'a')) is not None
    cache.make_room(24*MiB); cache.put((0,'c'),entry('c'))
    assert closed==['b'] and list(cache.entries)==[(0,'a'),(0,'c')] and cache.bytes==48*MiB
    cache.make_room(50*MiB)
    assert closed==['b','a','c'] and cache.bytes==0
    assert not cache.admit(65*MiB)
    cache.captures=2; assert not cache.admit(1)
    cache.captures=0; cache.capture_seconds=cfg['capture_budget_s']; assert not cache.admit(1)
    cache.clear(); assert cache.bytes==0


def test_capture_drains_readers_and_blocks_new_gpu_work_until_complete():
    gate=ExecutionGate(); readers_ready=threading.Barrier(3); release=threading.Event()
    writer_entered=threading.Event(); release_writer=threading.Event(); events=[]
    def reader():
        with gate.enter(): readers_ready.wait(timeout=5); release.wait(timeout=5)
    def writer():
        with gate.enter(exclusive=True):
            events.append('writer'); writer_entered.set(); release_writer.wait(timeout=5)
    def late_reader():
        with gate.enter(): events.append('late-reader')
    with ThreadPoolExecutor(max_workers=4) as executor:
        first=[executor.submit(reader) for _ in range(2)]; readers_ready.wait(timeout=5)
        capture=executor.submit(writer)
        end=time.monotonic()+5
        while not gate.waiting and time.monotonic()<end: time.sleep(.001)
        assert gate.waiting==1
        late=executor.submit(late_reader)
        assert not writer_entered.is_set(); release.set(); assert writer_entered.wait(timeout=5)
        assert events==['writer']; release_writer.set()
        for future in first+[capture,late]: future.result(timeout=5)
    assert events==['writer','late-reader'] and gate.readers==0 and not gate.writing
    with pytest.raises(RuntimeError):
        with gate.enter(exclusive=True): raise RuntimeError('capture failed')
    with gate.enter(): assert gate.readers==1


def test_graph_declaration_is_inert_and_rejects_batch_and_replica_mix(tmp_path):
    from test_model_deployment import config
    from demiflow.services.deployment import deployment_config
    from demiflow.services.profiles import build_service
    from demiflow.operator_llm.model import parse_prompt_model
    from demiflow.operator_llm.client import AsyncOperatorLLMClient
    cfg=deployment_config({**config(tmp_path),'options':{'cuda_graphs':{}}})
    assert cfg['options']['cuda_graphs']['max_entries']==32
    model=parse_prompt_model({'name':'fixture','deployment':cfg},label='model')
    with pytest.raises(ValueError,match='independent single'):
        AsyncOperatorLLMClient(model,{'systemone_batch':{'max_size':2}})
    assert not (tmp_path/'services').exists()
    for changes in ({'gpus':[0,1],'replicas':2},{'batch_evidence':'/proof.json','batch_max_size':2},
                    {'request_concurrency':1}):
        with pytest.raises(ValueError): deployment_config({**cfg,**changes})
    manifest={'id':'fixture','python':'/python','ports':[21000],'model_contract':{},'runtime':{},'adapter_version':'x'}
    service=build_service('fixture',cfg,manifest)
    assert '--cuda-graphs' in service.command
    with pytest.raises(ValueError,match='batch validation'):
        build_service('fixture',cfg,manifest,validation_batch_size=2)


def test_encoding_admission_precedes_native_materialization():
    from demiflow.services.pplx_graphs import check_encoding_budget
    check_encoding_budget({'state':'small','questions':{'q':{}}},128)
    with pytest.raises(ValueError,match='byte budget'):
        check_encoding_budget({'state':'汉'*10000,'questions':{'q':{}}},128)
    with pytest.raises(ValueError,match='expansion'):
        check_encoding_budget({'state':'x'*200000,'questions':{str(i):{} for i in range(128)}},8192)


def test_graph_quality_failure_returns_native_and_clears_all_entries():
    from demiflow.services.pplx_graphs import PplxGraphAdapter
    adapter=PplxGraphAdapter.__new__(PplxGraphAdapter)
    adapter.config=graph_config({'verify_every':1}); adapter.parallel=2
    adapter.cache=GraphCache(adapter.config); adapter.gate=ExecutionGate()
    adapter.closed=False; adapter.failed=None
    adapter.lanes=[{'stream':None}]; adapter.stats={'checks':0,'mismatches':0,'hits':0}
    cleared=[]
    adapter.torch=SimpleNamespace(cuda=SimpleNamespace(empty_cache=lambda:cleared.append('allocator')))
    chunk=SimpleNamespace(length=128,keys=['q'],pixel_values=None,image_grid_thw=None,mm_token_type_ids=None)
    adapter.cache.put((0,(128,1)),SimpleNamespace(bytes=1024,uses=0,close=lambda:cleared.append('graph')))
    adapter._native=lambda *args: [[[1.,2.]]]
    adapter._replay=lambda *args: [[[1.,3.]]]
    adapter._estimate=lambda *args: 1024
    assert adapter._forward(0,[chunk])==[[[1.,2.]]]
    assert adapter.cache.disabled and adapter.cache.bytes==0 and adapter.parallel==2
    assert cleared==['graph','allocator'] and adapter.stats['mismatches']==1
    assert adapter._forward(0,[chunk])==[[[1.,2.]]]
    assert adapter.stats['checks']==1


def test_periodic_audits_keep_independent_lanes_running():
    from demiflow.services.pplx_graphs import PplxGraphAdapter
    adapter=PplxGraphAdapter.__new__(PplxGraphAdapter)
    adapter.config=graph_config({'verify_every':1}); adapter.parallel=2
    adapter.cache=GraphCache(adapter.config); adapter.gate=ExecutionGate()
    adapter.closed=False; adapter.failed=None
    adapter.lanes=[{'stream':None}, {'stream':None}]
    adapter.stats=dict(checks=0,mismatches=0,hits=0)
    ready=threading.Barrier(2)
    def native(*args):
        ready.wait(5)
        assert adapter.gate.readers==2 and not adapter.gate.writing
        ready.wait(5)
        return [[[1.,2.]]]
    adapter._native=native; adapter._replay=lambda *args:[[[1.,2.]]]
    chunk=SimpleNamespace(length=128,keys=['q'],pixel_values=None,image_grid_thw=None,mm_token_type_ids=None)
    for index in range(2):
        adapter.cache.put((index,(128,1)),SimpleNamespace(bytes=1024,uses=0,close=lambda:None))
    with ThreadPoolExecutor(max_workers=2) as pool:
        values=list(pool.map(lambda i:adapter._forward(i,[chunk]),range(2)))
    assert values==[[[[1.,2.]]],[[[1.,2.]]]]
    assert adapter.stats==dict(checks=2,mismatches=0,hits=2)


def test_failed_shared_audit_drains_other_replays_before_freeing():
    from demiflow.services.pplx_graphs import PplxGraphAdapter
    adapter=PplxGraphAdapter.__new__(PplxGraphAdapter)
    adapter.config=graph_config({'verify_every':2}); adapter.parallel=2
    adapter.cache=GraphCache(adapter.config); adapter.gate=ExecutionGate()
    adapter.closed=False; adapter.failed=None
    adapter.lanes=[{'stream':0},{'stream':1}]
    adapter.stats=dict(checks=0,mismatches=0,hits=0)
    entered=threading.Event(); release=threading.Event(); freed=[]
    def close():
        assert adapter.gate.writing and adapter.gate.readers==0
        freed.append(True)
    adapter.torch=SimpleNamespace(cuda=SimpleNamespace(empty_cache=lambda:None))
    adapter._native=lambda *a:[[[1.]]]
    def replay(entry, chunk, stream):
        if stream==1:
            entered.set(); assert release.wait(5)
            return [[[1.]]]
        return [[[2.]]]
    adapter._replay=replay
    chunk=SimpleNamespace(length=128,keys=['q'],pixel_values=None,image_grid_thw=None,mm_token_type_ids=None)
    for i in range(2):
        adapter.cache.put((i,(128,1)),SimpleNamespace(bytes=1024,uses=1-i,close=close))
    with ThreadPoolExecutor(max_workers=2) as pool:
        other=pool.submit(adapter._forward,1,[chunk]); assert entered.wait(5)
        audit=pool.submit(adapter._forward,0,[chunk])
        try:
            deadline=time.monotonic()+5
            while not adapter.gate.waiting and time.monotonic()<deadline: time.sleep(.001)
            assert adapter.cache.disabled and adapter.gate.waiting and not freed
        finally: release.set()
        assert audit.result(5)==other.result(5)==[[[1.]]]
    assert len(freed)==2 and adapter.stats['mismatches']==1


@pytest.mark.parametrize('gpu_error',[False,True])
def test_close_drains_active_call_rejects_waiter_and_gpu_failure_invalidates_instance(gpu_error):
    from contextlib import nullcontext
    from demiflow.services.pplx_graphs import PplxGraphAdapter
    adapter=PplxGraphAdapter.__new__(PplxGraphAdapter)
    adapter.config=graph_config({'streams':1}); adapter.parallel=1; adapter.gate=ExecutionGate()
    adapter.cache=GraphCache(adapter.config); adapter.closed=False; adapter.accepting=True; adapter.failed=None
    adapter.condition=threading.Condition(); adapter.encoding=threading.Lock()
    from collections import deque
    adapter.pending=deque(); adapter.stats=dict(queue_wait_seconds=0.,queue_wait_max_seconds=0.)
    adapter.workspace_used=0; adapter.workspace_limit=10; adapter.validate=lambda value:value
    adapter._estimate=lambda *args:1
    adapter.torch=SimpleNamespace(inference_mode=nullcontext,cuda=SimpleNamespace(
        stream=lambda _:nullcontext(),synchronize=lambda:None,empty_cache=lambda:None))
    adapter.decider=SimpleNamespace(encoder=SimpleNamespace(plan=lambda *args:[SimpleNamespace(length=10,keys=['q'])]),max_length=100)
    entered=threading.Event(); release=threading.Event(); waiting=threading.Event()
    def decide(request):
        entered.set(); assert release.wait(5)
        if gpu_error: raise RuntimeError('GPU failed')
        return 'native-output'
    adapter.lanes=[{'model':SimpleNamespace(decide=decide),'stream':None,'busy':False,'executor':ThreadPoolExecutor(max_workers=1)}]
    def queued():
        waiting.set()
        return adapter.single({'questions':{'q':{}}})
    with ThreadPoolExecutor(max_workers=3) as pool:
        first=pool.submit(adapter.single,{'questions':{'q':{}}}); assert entered.wait(5)
        second=pool.submit(queued); assert waiting.wait(5)
        if not gpu_error:
            close=pool.submit(adapter.close)
            end=time.monotonic()+5
            while adapter.accepting and time.monotonic()<end: time.sleep(.001)
            assert not adapter.accepting and not close.done()
        release.set()
        if gpu_error:
            with pytest.raises(RuntimeError,match='GPU failed'): first.result(5)
            with pytest.raises(RuntimeError,match='GPU execution failed'): second.result(5)
            adapter.close()
        else:
            assert first.result(5)=='native-output'
            with pytest.raises(RuntimeError,match='clos'): second.result(5)
            close.result(5)
    assert adapter.workspace_used==0 and adapter.closed and not adapter.pending


def test_http_exposes_graph_downgrade_and_failed_health():
    import httpx
    from demiflow.services.model_worker import make_server
    class GraphAdapter:
        parallel=2
        failed=None
        def execution_status(self): return {'graph_disabled':self.parallel==1}
    adapter=GraphAdapter(); server=make_server(adapter,'fixture',0,2)
    thread=threading.Thread(target=server.serve_forever); thread.start()
    try:
        with httpx.Client(base_url=f'http://127.0.0.1:{server.server_port}',trust_env=False) as client:
            caps=client.get('/v1/models').json()['data'][0]['capabilities']
            assert caps['parallel']==2 and not caps['batch']['supported']
            adapter.parallel=1
            assert client.get('/v1/models').json()['data'][0]['capabilities']['parallel']==1
            adapter.failed='GPU execution failed'
            assert client.get('/health').status_code==503
    finally: server.shutdown(); server.server_close(); thread.join()


def test_infrequent_shapes_cannot_evict_hot_graphs():
    cache=GraphCache(graph_config({'max_entries':2,'cache_memory_mib':64,'min_observations':2}))
    closed=[]
    for _ in range(10): cache.observe((0,'hot'))
    for _ in range(3): cache.observe((0,'warm'))
    for shape in ['hot','warm']:
        cache.put((0,shape),SimpleNamespace(bytes=20*MiB,close=lambda shape=shape:closed.append(shape)))
    for _ in range(2): cache.observe((0,'cold'))
    assert not cache.admit(20*MiB,(0,'cold'))
    for _ in range(2): cache.observe((0,'cold'))
    assert cache.admit(20*MiB,(0,'cold'))
    cache.make_room(20*MiB)
    assert closed==['warm'] and cache.get((0,'hot')) is not None
    cache.history[(0,'hot')]=65535; cache.observe((0,'hot')); assert cache.history[(0,'hot')]==65535


def test_cold_lane_copy_does_not_inherit_hot_lane_frequency():
    cache=GraphCache(graph_config({'max_entries':2,'cache_memory_mib':64}))
    closed=[]
    for key, frequency in [((0,'same'),40),((1,'same'),2),((1,'other'),8)]:
        for _ in range(frequency): cache.observe(key)
    for key in [(0,'same'),(1,'same')]:
        cache.put(key,SimpleNamespace(bytes=20*MiB,close=lambda key=key:closed.append(key)))
    assert cache.admit(20*MiB,(1,'other'))
    cache.make_room(20*MiB)
    assert closed==[(1,'same')] and cache.get((0,'same')) is not None
    # Both lanes can still acquire independently hot graphs for the same input.
    for _ in range(40): cache.observe((1,'same'))
    assert cache.admit(20*MiB,(1,'same'))


def test_gpu_admission_is_fifo_and_does_not_starve_older_requests():
    from collections import deque
    from demiflow.services.cuda_graphs import BoundedExecution
    adapter=BoundedExecution()
    adapter.config=graph_config({'streams':1});adapter.closed=False;adapter.accepting=True;adapter.failed=None
    adapter.condition=threading.Condition();adapter.pending=deque()
    adapter.workspace_used=0;adapter.workspace_limit=1;adapter._estimate=lambda _:1
    adapter.cache=GraphCache(adapter.config);adapter.stats=dict(queue_wait_seconds=0.,queue_wait_max_seconds=0.)
    executor=ThreadPoolExecutor(max_workers=1)
    adapter.lanes=[dict(busy=False,executor=executor)]
    first=threading.Event();release=threading.Event();order=[]
    def execute(index,request,plan):
        order.append(request)
        if request==0:
            first.set();assert release.wait(5)
        return request
    adapter._execute=execute
    try:
        with ThreadPoolExecutor(max_workers=5) as callers:
            futures=[callers.submit(adapter._submit,0,None,1)];assert first.wait(5)
            for i in range(1,5):
                futures.append(callers.submit(adapter._submit,i,None,1))
                deadline=time.monotonic()+2
                while len(adapter.pending)!=i and time.monotonic()<deadline:time.sleep(.001)
                assert len(adapter.pending)==i
            release.set()
            assert [f.result(5) for f in futures]==list(range(5))
        assert order==list(range(5)) and not adapter.pending and adapter.workspace_used==0
        assert adapter.stats['queue_wait_max_seconds']>0
    finally:
        release.set();executor.shutdown(wait=True)
def test_graph_pool_accounting_uses_reserved_counters_and_rejects_missing_data():
    from types import SimpleNamespace
    from demiflow.services.cuda_graphs import graph_pool_bytes
    pool=(100001,0)
    data={'reserved_bytes_by_private_pools':{pool:{'all':{'current':8192}}}}
    cuda=SimpleNamespace(memory_stats_as_nested_dict=lambda:data,
                         memory_snapshot=lambda *a,**k:pytest.fail('Unsafe filtered snapshot was called'))
    graph=SimpleNamespace(pool=lambda:pool)
    assert graph_pool_bytes(SimpleNamespace(cuda=cuda),graph)==8192
    data.clear()
    with pytest.raises(RuntimeError,match='reserved bytes'):graph_pool_bytes(SimpleNamespace(cuda=cuda),graph)

