"""Known reducer initial/copy/encoding allocations share the admitted grant."""
import json
import sys
from types import SimpleNamespace

import pytest

from demiflow import data
from demiflow.execution.run_context import RunContext
from demiflow.execution.contracts.dataflow_errors import DataflowError
from demiflow.execution.sealed_plans import load_sealed_plan
from demiflow.execution.kernels import reducer_memory


def _flow(reducer,initial,*,region):
    left=data.from_items([{'k':1,'i':0},{'k':1,'i':1}])
    if region:
        left=left.join(data.from_items([{'k':1,'r':0}]),on='k').join(
            data.from_items([{'k':1,'z':0}]),on='k')
    return left.reduce_by_key('k',reducer,initial=initial)


def test_large_initial_is_admitted_before_copy_on_general_and_region_paths(tmp_path,monkeypatch):
    from demiflow.execution.dataflow import DataflowExecution
    state_bytes=256*1024
    for region in (False,True):
        for memory in (1024*1024,4*1024*1024):
            directory=tmp_path/f'{region}-{memory}'
            directory.mkdir()
            log=directory/'callbacks.jsonl'
            initial={'k':1,'pad':[0]*((state_bytes-4096)//8),'n':0}
            def reducer(state,row):
                value={'k':1,'pad':[0]*((state_bytes-4096)//8),'n':state['n']+1}
                with log.open('a') as target:
                    target.write(json.dumps({'known_live':sum(sys.getsizeof(x['pad']) for x in (initial,state,value)),
                        'ids':[id(x['pad']) for x in (initial,state,value)]})+'\n')
                return value if row['i']==0 else {'k':1,'n':value['n']}
            limits={'memory_bytes':memory,'max_row_bytes':4096,'max_batch_bytes':4096,'max_state_bytes':state_bytes}
            launches=[]
            launch=DataflowExecution._prepare_launch
            def observed(execution,*args,**kwargs):
                launches.append(True)
                return launch(execution,*args,**kwargs)
            with monkeypatch.context() as patch:
                patch.setattr(DataflowExecution,'_prepare_launch',observed)
                if memory==1024*1024:
                    with pytest.raises(DataflowError) as failure:
                        with RunContext.start(directory/'run',limits=limits),data.local_execution(workers=2):
                            _flow(reducer,initial,region=region).take_all()
                    assert failure.value.code in ('RESOURCE_INFEASIBLE','BUDGET_EXCEEDED')
                    assert launches==[] and not log.exists()
                else:
                    with RunContext.start(directory/'run',limits=limits) as run,data.local_execution(workers=2):
                        assert _flow(reducer,initial,region=region).take_all()==[{'k':1,'n':2}]
                        bundle=load_sealed_plan(run.directory,'action-1',version='v3')
                        occurrence=next(n.occurrence_id for n in bundle['graph'].nodes if n.kernel.kind=='reduce')
                        stage=next(s for s in bundle['physical'].stages if occurrence in s.occurrences)
                        facts=[json.loads(x) for x in log.read_text().splitlines()]
                        assert len(facts)==2 and all(len(set(x['ids']))==3 for x in facts)
                        assert max(x['known_live'] for x in facts)<=stage.resident_bytes
                        assert max(x['known_live'] for x in facts)>376832  # original false admission
                        assert all(c['status']=='acked' for c in run.journal.execution_commits('action-1'))
                        assert stage.stage_id.startswith('relational-v1-') is region
            assert initial['n']==0 and initial['pad']==[0]*((state_bytes-4096)//8)


def test_none_initial_reserves_replacement_state_without_charging_full_initial(tmp_path):
    state_bytes=256*1024
    for region in (False,True):
        directory=tmp_path/str(region);directory.mkdir()
        log=directory/'callbacks.jsonl'
        def reducer(state,row):
            value={'k':1,'pad':bytearray(state_bytes-4096),'n':(state or {}).get('n',0)+1}
            with log.open('a') as target:
                target.write(json.dumps({'known_live':sum(sys.getsizeof(x['pad']) for x in (state,value) if x is not None)})+'\n')
            return value if row['i']==0 else {'k':1,'n':value['n']}
        limits={'memory_bytes':1024*1024,'max_row_bytes':4096,'max_batch_bytes':4096,'max_state_bytes':state_bytes}
        with RunContext.start(directory/'run',limits=limits) as run,data.local_execution(workers=2):
            assert _flow(reducer,None,region=region).take_all()==[{'k':1,'n':2}]
            bundle=load_sealed_plan(run.directory,'action-1',version='v3')
            node=next(n for n in bundle['graph'].nodes if n.kernel.kind=='reduce')
            declared=reducer_memory.contract(node.kernel)
            assert declared['initial_bytes']==declared['initial_copy_bytes']==0
            stage=next(s for s in bundle['physical'].stages if node.occurrence_id in s.occurrences)
            known=max(json.loads(x)['known_live'] for x in log.read_text().splitlines())
            assert 376832<known<=stage.resident_bytes<1024*1024
            assert all(c['status']=='acked' for c in run.journal.execution_commits('action-1'))


def test_state_resident_is_checked_before_platform_serialization(monkeypatch):
    from test_runtime_v3_c import bounds
    memory=reducer_memory.declare(None,bounds(max_state_bytes=4096,max_resident_bytes=1024*1024))
    calls=[]
    def forbidden(*args,**kwargs):
        calls.append(True)
        raise AssertionError('serialization must not start before resident admission')
    monkeypatch.setattr(reducer_memory.pickle,'Pickler',forbidden)
    with pytest.raises(DataflowError,match='retained objects'):
        reducer_memory.check_state({'pad':b'x'*8192},memory,serialized_limit=16384)
    with pytest.raises(DataflowError,match='max_state_bytes before encoding'):
        reducer_memory.check_state({'pad':'é'*3000},{**memory,'state_resident_bytes':16384},serialized_limit=4096)
    assert calls==[]


def test_serialized_ceiling_remains_distinct_from_heap_reservation():
    from test_runtime_v3_c import bounds
    import pickle
    memory=reducer_memory.declare(None,bounds(max_state_bytes=4096,max_resident_bytes=1024*1024))
    value={'pad':b'x'*3000}
    assert len(pickle.dumps(value,protocol=pickle.HIGHEST_PROTOCOL))<4096
    reducer_memory.check_state(value,memory,serialized_limit=4096)
    with pytest.raises(DataflowError,match='max_state_bytes'):
        reducer_memory.check_state({'pad':b'x'*6000},{**memory,'state_resident_bytes':16384},serialized_limit=4096)


def test_changed_initial_is_rejected_before_deepcopy(monkeypatch):
    from demiflow.data.plan import CallableSpec
    from demiflow.execution.contracts.dataflow import LoadedProgram
    from demiflow.execution.kernels import relational
    from demiflow.resources.contracts import ResourceProfile
    from test_runtime_v3_c import bounds,make
    from test_runtime_v3_finite_borrow import record
    budget=bounds(max_row_bytes=4096,max_batch_bytes=4096,max_state_bytes=65536,max_resident_bytes=1024*1024)
    initial={'pad':[0]*64}
    memory=reducer_memory.declare(initial,budget)
    spec=make('KernelSpec',kind='reduce',configuration={'on':['k'],'occurrence_id':'reduce-test',
        'reducer_program':'reduce',reducer_memory.FIELD:memory})
    grant=reducer_memory.resident_bytes(memory,budget)
    stage=make('StageSpec',resident_bytes=grant,profile=ResourceProfile(requests={'cpu_millis':1000,'memory_bytes':grant}))
    program=CallableSpec.create(lambda state,row:{'k':row['k']},fn_args=(initial,))
    svc=SimpleNamespace(bounds=budget,assignment=SimpleNamespace(stages=(stage,)),
        programs={'reduce':LoadedProgram('callable',program,'0'*64)},check_stop=lambda:None)
    initial['pad'].extend([0]*2000)
    copies=[]
    def forbidden(*args,**kwargs):
        copies.append(True)
        raise AssertionError('copy must not start before admission')
    monkeypatch.setattr(relational.copy,'deepcopy',forbidden)
    row={'k':1}
    with pytest.raises(DataflowError):
        list(relational.reduce_ordered_groups(iter([('key',row,record(row))]),spec=spec,services=svc))
    assert copies==[]


def test_yielded_peers_are_alias_aware_and_release_on_finish_error_and_close():
    from demiflow.data.plan import CallableSpec
    from demiflow.execution.contracts.dataflow import LoadedProgram
    from demiflow.execution.kernels.operator import OperatorKernel
    from demiflow.execution.kernels.relational import reduce_ordered_groups
    from demiflow.resources.contracts import ResourceProfile
    from test_runtime_v3_c import bounds,make
    from test_runtime_v3_finite_borrow import record
    budget=bounds(max_row_bytes=4096,max_batch_bytes=4096,max_state_bytes=65536,max_resident_bytes=1024*1024)
    initial={'shared':b'x'*1000}
    memory=reducer_memory.declare(initial,budget)
    out=record({'k':1})
    peers=reducer_memory.yielded_peers(initial,initial,out,memory,budget)
    assert peers==reducer_memory.measure_tree(initial,max_bytes=65536,max_nodes=100).retained
    for mode in ('finish','error','close'):
        def reducer(state,row):
            if mode=='error' and row['k']==3:
                raise ValueError('controlled third-group failure')
            return {'k':row['k'],'shared':state['shared']}
        spec=make('KernelSpec',kind='reduce',configuration={'on':['k'],'occurrence_id':'reduce-test',
            'reducer_program':'reduce',reducer_memory.FIELD:memory})
        grant=reducer_memory.resident_bytes(memory,budget)
        stage=make('StageSpec',resident_bytes=grant,profile=ResourceProfile(requests={'cpu_millis':1000,'memory_bytes':grant}))
        program=CallableSpec.create(reducer,fn_args=(initial,))
        svc=SimpleNamespace(bounds=budget,store=None,assignment=SimpleNamespace(stages=(stage,)),
            programs={'reduce':LoadedProgram('callable',program,'0'*64)},check_stop=lambda:None)
        owner=OperatorKernel(spec,services=svc,partition_id='p0')
        rows=[{'k':i} for i in (1,2,3)]
        folded=reduce_ordered_groups(iter((str(row['k']),row,record(row)) for row in rows),
            spec=spec,services=svc,retain=owner._retain_reducer_peers)
        assert owner.retained_resident_bytes==0
        first=next(folded)
        assert dict(first.inline)=={'k':1,'shared':initial['shared']}
        assert 7*4096<owner.retained_resident_bytes<grant
        if mode=='close':
            folded.close()
        elif mode=='error':
            with pytest.raises(ValueError,match='third-group'):
                list(folded)
        else:
            assert [r.inline['k'] for r in folded]==[2,3]
        assert owner.retained_resident_bytes==0


def test_numpy_view_charges_its_unique_owner_before_serialization(monkeypatch):
    import numpy as np
    from test_runtime_v3_c import bounds
    owner=np.arange(8192,dtype=np.int64)
    view=owner[::8192]
    assert view.nbytes==8 and owner.nbytes==65536
    roots=[view,owner,view]
    measured=reducer_memory.measure_tree(roots,max_bytes=128*1024,max_nodes=100)
    assert measured.retained==sys.getsizeof(roots)+sys.getsizeof(view)+sys.getsizeof(owner)
    compact=view.copy()
    assert reducer_memory.measure_tree(compact,max_bytes=4096,max_nodes=100).retained==sys.getsizeof(compact)
    memory=reducer_memory.declare(None,bounds(max_state_bytes=4096,max_resident_bytes=1024*1024))
    calls=[]
    original=reducer_memory.pickle.Pickler
    def forbidden(*args,**kwargs):
        calls.append(True)
        raise AssertionError('pickle cannot start while a retained owner exceeds the grant')
    monkeypatch.setattr(reducer_memory.pickle,'Pickler',forbidden)
    with pytest.raises(DataflowError,match='retained objects'):
        reducer_memory.check_state({'a':view},memory,serialized_limit=4096)
    assert calls==[]
    monkeypatch.setattr(reducer_memory.pickle,'Pickler',original)
    reducer_memory.check_state({'a':view},{**memory,'state_resident_bytes':128*1024},serialized_limit=4096)
    buffer=bytearray(65536)
    from_buffer=np.frombuffer(buffer,dtype=np.uint8,count=1)
    retained=reducer_memory.measure_tree(from_buffer,max_bytes=128*1024,max_nodes=100).retained
    assert retained==sys.getsizeof(from_buffer)+sys.getsizeof(from_buffer.base)+sys.getsizeof(buffer)


def test_legacy_unfinished_reduce_validates_original_grant_without_rewriting_seal(tmp_path,monkeypatch):
    from demiflow.data.plan import CallableSpec
    from demiflow.execution.contracts.dataflow import LoadedProgram
    from demiflow.execution.contracts.dataflow_codec import _thaw_json
    from demiflow.execution.kernels import relational
    from demiflow.resources.contracts import ResourceProfile
    from test_runtime_v3_artifact_accounting import store_fixture
    from test_runtime_v3_c import bounds,make
    from test_runtime_v3_finite_borrow import record
    budget=bounds(max_row_bytes=4096,max_batch_bytes=4096,max_state_bytes=65536,max_resident_bytes=1024*1024)
    initial={'pad':[0]*4096,'n':0}
    for grant in (65536,512*1024):
        calls=[];copies=[];encodes=[];reads=[]
        def reducer(state,row):
            calls.append(row['k'])
            return {'k':row['k'],'n':state['n']+1}
        spec=make('KernelSpec',kind='reduce',configuration={'on':['k'],'occurrence_id':'legacy-reduce',
            'reducer_program':'reduce'})
        stage=make('StageSpec',stage_id='original-legacy-stage',resident_bytes=grant,
            profile=ResourceProfile(requests={'cpu_millis':1000,'memory_bytes':grant}))
        original_config=json.dumps(_thaw_json(spec.configuration),sort_keys=True)
        initial_program=CallableSpec.create(reducer,fn_args=(initial,))
        with store_fixture(tmp_path/str(grant)) as (_,store,_):
            svc=SimpleNamespace(bounds=budget,store=store,assignment=SimpleNamespace(stages=(stage,)),
                programs={'reduce':LoadedProgram('callable',initial_program,'0'*64)},check_stop=lambda:None)
            original_copy=relational.copy.deepcopy
            original_pickle=reducer_memory.pickle.Pickler
            def copy(*args,**kwargs):
                copies.append(True)
                return original_copy(*args,**kwargs)
            def pickle(*args,**kwargs):
                encodes.append(True)
                return original_pickle(*args,**kwargs)
            def records():
                for i in range(2):
                    reads.append(i)
                    yield record({'k':1,'i':i},i)
            with monkeypatch.context() as patch:
                patch.setattr(relational.copy,'deepcopy',copy)
                patch.setattr(reducer_memory.pickle,'Pickler',pickle)
                if grant==65536:
                    with pytest.raises(DataflowError,match='known copies exceed'):
                        list(relational.reduce_partition(records(),spec=spec,services=svc))
                    assert reads==copies==calls==encodes==[]
                else:
                    actual=list(relational.reduce_partition(records(),spec=spec,services=svc))
                    assert [dict(r.inline) for r in actual]==[{'k':1,'n':2}]
                    assert reads==[0,1] and calls==[1,1] and len(copies)==1 and len(encodes)==2
            assert json.dumps(_thaw_json(spec.configuration),sort_keys=True)==original_config
            assert reducer_memory.FIELD not in spec.configuration
            assert svc.assignment.stages==(stage,) and stage.stage_id=='original-legacy-stage'
            assert stage.resident_bytes==stage.profile.requests['memory_bytes']==grant
    assert initial=={'pad':[0]*4096,'n':0}
