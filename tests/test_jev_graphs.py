"""CPU quality and ownership boundaries for the versioned JEV backend."""
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
import threading
from types import SimpleNamespace

import pytest

from demiflow.services.cuda_graphs import ExecutionGate, GraphCache, graph_config
from demiflow.services.deployment import deployment_config
from demiflow.services.jev_graphs import JEVGraphAdapter
from demiflow.services.jev_runtime import TensorClone
from demiflow.services.profiles import component_suffixes


def test_metadata_clone_preserves_aliases_and_rejects_before_tensor_allocation():
    class Tensor:
        copies=0
        def __init__(self, size): self.size=size
        def numel(self): return self.size
        def element_size(self): return 4
        def clone(self):
            Tensor.copies+=1
            return Tensor(self.size)
    @dataclass
    class Metadata:
        items: dict
    source=Tensor(4)
    clone=TensorClone(SimpleNamespace(Tensor=Tensor),budget=16)
    result=clone(Metadata({'first':source,'alias':source}))
    assert result.items['first'] is result.items['alias'] and result.items['first'] is not source
    assert clone.bytes==16 and Tensor.copies==1
    with pytest.raises(ValueError,match='copy budget'): clone(Tensor(1))
    assert Tensor.copies==1
    with pytest.raises(ValueError,match='Unreviewed'): clone(object())


def adapter_fixture():
    adapter=JEVGraphAdapter.__new__(JEVGraphAdapter)
    adapter.config=graph_config({'verify_every':1});adapter.parallel=2
    adapter.cache=GraphCache(adapter.config);adapter.gate=ExecutionGate()
    adapter.stats=dict(native=0,hits=0,checks=0,mismatches=0)
    adapter.closed=False;adapter.failed=None;adapter.condition=threading.Condition()
    adapter.lanes=[{'stream':SimpleNamespace(synchronize=lambda:None)} for _ in range(2)]
    adapter.torch=SimpleNamespace(cuda=SimpleNamespace(empty_cache=lambda:None),equal=lambda a,b:a==b)
    adapter._estimate=lambda tokens:1
    return adapter


def test_graph_mismatch_returns_native_and_truthfully_disables_parallel_replay():
    adapter=adapter_fixture();closed=[]
    entry=SimpleNamespace(uses=0,bytes=1,close=lambda:closed.append(True))
    adapter.cache.entries[(0,(3,2))]=entry;adapter.cache.bytes=1
    adapter._native=lambda *args,**kwargs:([-.25,-1.5],(None,None,'native-hidden'))
    adapter._replay=lambda *args:[-.5,-1.5]
    assert adapter._score(0,'prompt',[1,2,3],2)==[-.25,-1.5]
    assert adapter.cache.disabled and not adapter.cache.entries and closed==[True]
    assert adapter.parallel==1 and adapter.stats['mismatches']==1


def test_two_verified_graphs_can_replay_at_the_same_time_without_native_runner():
    adapter=adapter_fixture();adapter.config['verify_every']=64
    for i in range(2):
        adapter.cache.entries[(i,(3,2))]=SimpleNamespace(uses=0)
    ready=threading.Barrier(2);seen=[]
    def replay(entry,ids):
        seen.append(adapter.gate.readers);ready.wait(5)
        return [-.25,-1.5]
    adapter._replay=replay
    adapter._native=lambda *args,**kwargs:pytest.fail('Verified replay must not invoke native scheduling')
    with ThreadPoolExecutor(2) as pool:
        futures=[pool.submit(adapter._score,i,'prompt',[1,2,3],2) for i in range(2)]
        assert [f.result(10) for f in futures]==[[-.25,-1.5]]*2
    assert max(seen)==2


def test_question_expansion_rejected_before_gpu_submission():
    adapter=adapter_fixture();adapter.config['max_input_tokens']=4
    adapter.protocol=SimpleNamespace(_prepare=lambda *a,**k:[('q',{},'text',2,5,[1]*5)])
    adapter._submit=lambda *a,**k:pytest.fail('Oversized row must not reach GPU admission')
    with pytest.raises(ValueError,match='question expansion'):
        adapter.single({'state':'x','questions':{'q':{'type':'noul','instructions':'?'}}})


def test_jev_optimized_has_one_owned_component_and_rejects_unreviewed_layouts():
    base=dict(profile='autotrust-jev',weights='/weights',revision='v1',gpus=[0],performance='optimized')
    assert component_suffixes(deployment_config(base))==['engine']
    assert component_suffixes(deployment_config({**base,'performance':'native'}))==['engine','wrapper']
    for values in [{'gpus':[0,1]}, {'options':{'dtype':'float16'}},
                   {'options':{'max_model_len':65536}}, {'options':{'max_num_seqs':128}}]:
        with pytest.raises(ValueError,match='JEV CUDA graphs'):deployment_config({**base,**values})


def test_prepared_jev_graph_deployment_owns_one_port_and_revalidates_runtime(tmp_path, monkeypatch):
    import sys
    from copy import deepcopy
    from demiflow.services import prepare
    weights=tmp_path/'weights';weights.mkdir()
    (weights/'jev_format.py').write_text('# formatter fixture\n')
    (weights/'vllm_readout.json').write_text('{}')
    versions={'packages':{'vllm':'0.31.0','transformers':'5.17.0','torch':'2.13.0',
                         'startlux-decide':'1.1.0','flashinfer-python':'0.7.0.post1',
                         'nvidia-cutlass-dsl':'4.7.1'}}
    monkeypatch.setattr(prepare,'_versions',lambda *a,**k:deepcopy(versions))
    cfg=deployment_config(dict(profile='autotrust-jev',weights=str(weights),revision='test',
        gpus=[0],performance='optimized',python=sys.executable,directory=str(tmp_path/'services')))
    manifest=prepare.prepare_deployment('jev',cfg)
    assert len(manifest['ports'])==1 and prepare.read_prepared('jev',cfg)==manifest
    versions['packages']['flashinfer-python']='changed'
    with pytest.raises(ValueError,match='runtime changed'): prepare.prepare_deployment('jev',cfg)
