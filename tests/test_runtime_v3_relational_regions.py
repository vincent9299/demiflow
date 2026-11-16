"""Real Dataset values, original seals, and measured relational reuse."""
from pathlib import Path

from demiflow import data
from demiflow.execution.run_context import RunContext
from demiflow.execution.sealed_plans import load_sealed_plan


def build_chain():
    left = data.from_items([{'k': 1, 'v': 'a'}, {'k': 2, 'v': 'b'}, {'k': 1, 'v': 'c'}])
    right = data.from_items([{'k': 1, 'r': 10}, {'k': 1, 'r': 11}, {'k': 2, 'r': 20}])
    last = data.from_items([{'k': 1, 'z': 7}, {'k': 2, 'z': 8}])
    return left.join(right, on='k').join(last, on='k').reduce_by_key(
        'k', lambda s, r: {'k': r['k'], 'values': (s or {}).get('values', []) + [(r['v'], r['r'], r['z'])]})


def test_real_chain_preserves_values_order_and_explicit_region(tmp_path):
    with RunContext.start(tmp_path/'run') as run:
        with data.local_execution(workers=2, worker_mode='thread', partitions=3, batch_rows=2):
            rows = build_chain().take_all()
        assert rows == [{'k': 1, 'values': [('a', 10, 7), ('a', 11, 7), ('c', 10, 7), ('c', 11, 7)]},
                        {'k': 2, 'values': [('b', 20, 8)]}]
        bundle = load_sealed_plan(run.directory, 'action-1', version='v3')
        graph, physical = bundle['graph'], bundle['physical']
        regions = [s for s in physical.stages if s.stage_id.startswith('relational-v1-')]
        assert len(regions) == 1
        kinds = {n.occurrence_id: n.kernel.kind for n in graph.nodes}
        assert [kinds[o] for o in regions[0].occurrences] == ['join', 'join', 'reduce']
        assert regions[0].profile.requests['memory_bytes'] < 256*1024**2
        facts = run.journal.execution_commits('action-1')
        assert facts and all(f['status'] == 'acked' for f in facts)


def test_region_planner_preserves_legacy_disable_isolation_and_budget():
    from dataclasses import replace
    import pytest
    from demiflow.execution.contracts.dataflow import ActionRequest
    from demiflow.execution.dataflow_api import _CompileContext
    from demiflow.execution.run_context import RunAction
    from demiflow.planning.dataflow_compile import lower_action
    from demiflow.planning.dataflow_place import partition_stages
    from demiflow.planning.dataflow_policy import resolve_runtime_policy
    from demiflow.planning.dataflow_relational_regions import fuse_relational_regions, interpret_region
    from demiflow.resources.contracts import ResourceProfile
    from test_runtime_v3_finite_fusion import HOSTS
    ds = build_chain()
    action = RunAction(_CompileContext(), 'region-compile', {})
    request = ActionRequest(kind='take_all', source=ds._source, plan=ds._plan,
        arguments={}, sink=None, programs=(), provider='local', action=action)
    policy, bounds = resolve_runtime_policy({}, {}, provider='local')
    graph = lower_action(request, bounds=bounds).graph
    stages = partition_stages(graph, policy=policy, bounds=bounds)
    fused = fuse_relational_regions(graph, stages, policy=policy, bounds=bounds, hosts=HOSTS)
    regions = [s for s in fused if s.stage_id.startswith('relational-v1-')]
    assert len(regions) == 1
    assert all(interpret_region(graph, stage) is None for stage in stages)
    assert interpret_region(graph, regions[0]).occurrences == regions[0].occurrences
    assert fuse_relational_regions(graph, stages, policy=replace(policy, fusion=False), bounds=bounds, hosts=HOSTS) == stages
    assert fuse_relational_regions(graph, stages, policy=policy, bounds=replace(bounds,max_resident_bytes=65536),hosts=HOSTS) == stages
    blocked = tuple(replace(s,profile=ResourceProfile(requests=dict(s.profile.requests),
        requirements={'isolation':'process'})) for s in stages)
    assert fuse_relational_regions(graph,blocked,policy=policy,bounds=bounds,hosts=HOSTS)==blocked
    from demiflow.execution.contracts.dataflow_errors import DataflowError
    with pytest.raises(DataflowError,match='sealed relational region'):
        interpret_region(graph,replace(regions[0],stage_id='relational-v1-'+'0'*64))


def test_region_original_final_ack_replay_does_not_repeat_reducer(tmp_path):
    import pytest
    from demiflow.data.api import DataAPI
    from demiflow.execution.dataflow_checkpoint import DataflowCheckpointCoordinator
    calls = tmp_path/'calls.txt'
    def reducer(state,row):
        with calls.open('a') as output:
            output.write(str(row['v'])+'\n')
        return {'k':row['k'],'values':(state or {}).get('values',[])+[row['v']]}
    def flow():
        return data.from_items([{'k':1,'v':i} for i in range(4)]).join(
            data.from_items([{'k':1,'r':2}]),on='k').join(
            data.from_items([{'k':1,'z':3}]),on='k').reduce_by_key('k',reducer)
    class Interrupted(Exception):
        pass
    with pytest.raises(Interrupted):
        with RunContext.start(tmp_path/'run') as run, data.local_execution(workers=2) as local:
            reference=run.ref
            rows=flow().take_all()
            assert rows==[{'k':1,'values':[0,1,2,3]}]
            from _native_relational_fixtures import native_region_evidence
            _,_,counts=native_region_evidence(local)
            assert len(counts)==1
            original=run.journal.execution_commits('action-1')
            original_calls=calls.read_text()
            raise Interrupted('after action ACK before enclosing run completion')
    with RunContext.resume(reference) as run, data.local_execution(workers=2) as local:
        assert flow().take_all()==rows
        assert run.journal.execution_commits('action-1')==original
        assert calls.read_text()==original_calls
        assert local.stats['observed_worker_count']==0


def test_region_pure_join_nonfinal_ack_restores_original_input_refs(tmp_path,monkeypatch):
    import pytest
    from demiflow.execution.dataflow_checkpoint import DataflowCheckpointCoordinator
    from demiflow.execution.dataflow_kernel_pack import read_kernel_image
    from demiflow.execution.kernels import state_codec
    def flow():
        return data.from_items([{'k':1,'v':i} for i in range(20)]).join(
            data.from_items([{'k':1,'r':i} for i in range(8)]),on='k').join(
            data.from_items([{'k':1,'z':i} for i in range(2)]),on='k')
    class Interrupted(Exception):pass
    commit=DataflowCheckpointCoordinator.commit
    caught=[]
    def cut(coordinator,*args,**kwargs):
        image=commit(coordinator,*args,**kwargs)
        for stage in coordinator._physical.stages:
            if not stage.stage_id.startswith('relational-v1-'):continue
            occ=stage.occurrences[-1]
            for contribution in image.contributions:
                ref=contribution.operator_states.get(occ)
                if ref is None:continue
                raw=read_kernel_image(coordinator._store,ref,coordinator._physical.bounds,
                                      occurrence=occ,partition=stage.partitions[0])
                state=state_codec.loads(raw,max_bytes=coordinator._physical.bounds.max_state_bytes)
                if not image.cut.final and state.get('finish_cursor',{}).get('emitted',0)>0:
                    caught.append((image.commit_id,coordinator._physical.plan_id))
                    raise Interrupted('actual region output prefix ACKed')
        return image
    limits={'checkpoint_rows':2,'checkpoint_interval_ms':60000}
    with pytest.raises(Interrupted):
        with RunContext.start(tmp_path/'run',limits=limits) as run, data.local_execution(workers=2):
            reference=run.ref
            with monkeypatch.context() as patch:
                patch.setattr(DataflowCheckpointCoordinator,'commit',cut)
                flow().take_all()
    assert len(caught)==1
    with RunContext.resume(reference) as run,data.local_execution(workers=2) as local:
        before=run.journal.execution_commits('action-1')
        actual=flow().take_all()
        assert sorted((r['v'],r['r'],r['z']) for r in actual)==[(v,r,z) for v in range(20) for r in range(8) for z in range(2)]
        after=run.journal.execution_commits('action-1')
        assert after[:len(before)]==before and all(x['status']=='acked' for x in after)
        assert local.stats['plan_id']==caught[0][1]


def test_region_semi_anti_keeps_original_arrow_schema(tmp_path):
    import pyarrow as pa
    from _native_relational_fixtures import native_region_evidence
    schema=pa.schema([pa.field('k',pa.int32(),nullable=False),
                     pa.field('v',pa.large_string())],metadata={b'origin':b'region-source'})
    table=pa.Table.from_pylist([{'k':1,'v':'one'},{'k':2,'v':None},{'k':3,'v':'three'}],schema=schema)
    with RunContext.start(tmp_path/'run'),data.local_execution(workers=2) as local:
        actual=data.from_arrow(table).join(data.from_items([{'k':1},{'k':2}]),on='k',how='semi').join(
            data.from_items([{'k':2}]),on='k',how='anti').take_batch(10,batch_format='pyarrow')
        assert actual.equals(table.slice(0,1),check_metadata=True)
        _,_,counts=native_region_evidence(local)
        assert len(counts)==1 and counts[0]['intermediate_payload_bytes']==0


def _small_budget_region(tmp_path, row_bytes, key_bytes):
    from _native_relational_fixtures import native_region_evidence,oracle_join
    keys=['a'*key_bytes,'b'*key_bytes,None]
    left=[{'k':keys[i%3],'v':i} for i in range(24)]
    right=[{'k':key,'r':j} for key in keys for j in range(2)]
    last=[{'k':key,'z':j} for key in keys for j in range(2)]
    expected={}
    for row in oracle_join(oracle_join(left,right,how='left'),last,how='left'):
        expected.setdefault(row['k'],[]).append((row['v'],row.get('r'),row.get('z')))
    limits={'memory_bytes':256*row_bytes,'max_row_bytes':row_bytes,'max_batch_bytes':2*row_bytes,
            'max_state_bytes':32768,'checkpoint_rows':64}
    with RunContext.start(tmp_path/'run',limits=limits),data.local_execution(workers=2) as local:
        actual=data.from_items(left).join(data.from_items(right),on='k',how='left').join(
            data.from_items(last),on='k',how='left').reduce_by_key('k',lambda state,row:{
                'k':row['k'],'fold':((state or {}).get('fold',17)*131+row['v']*9+(row.get('r') or 0)*3+(row.get('z') or 0))%1000000007,
                'n':(state or {}).get('n',0)+1}).take_all()
        oracle=[]
        for key in [*sorted(k for k in expected if k is not None),None]:
            values=expected[key]; n=len(values); prime=1000000007
            value=(17*pow(131,n,prime)+sum((v*9+(r or 0)*3+(z or 0))*pow(131,n-1-i,prime) for i,(v,r,z) in enumerate(values)))%prime
            oracle.append({'k':key,'fold':value,'n':n})
        assert actual==oracle
        graph,physical,counts=native_region_evidence(local)
        assert len(counts)==1
        measured=counts[0]
        assert measured['sort_runs']>2 and measured['sort_fan_in_limit']==2
        assert measured['sort_window_bytes']==8*row_bytes
        assert measured['sort_run_bytes']==2*row_bytes
        assert 2*(3*measured['sort_frame_bytes']+measured['sort_read_chunk_bytes'])+2*measured['sort_frame_bytes']<=measured['sort_window_bytes']
        region=next(stage for stage in physical.stages if stage.stage_id.startswith('relational-v1-'))
        assert measured['sort_window_bytes']+32768+2*row_bytes+3*row_bytes+16*row_bytes==region.resident_bytes
        assert region.resident_bytes<physical.bounds.max_resident_bytes
        assert measured['partition_builds']==3 and measured['intermediate_key_rebuilds']==0
        assert measured['intermediate_payload_bytes']==0


def test_region_small_budget_multirun_wide_key_nonassociative_fold(tmp_path):
    _small_budget_region(tmp_path/'small',4096,400)
    _small_budget_region(tmp_path/'wide',16384,6144)


def test_sort_subwindow_uses_original_store_and_closes_readers(tmp_path,monkeypatch):
    from dataclasses import replace
    from types import SimpleNamespace
    import pytest
    from demiflow.execution.contracts.dataflow_errors import DataflowError
    from demiflow.execution.kernels.ordering import SortWindow,build_sort_runs,merge_sort_runs
    from test_runtime_v3_artifact_accounting import store_fixture
    from test_runtime_v3_finite_borrow import fixture,record
    from test_runtime_v3_c import make
    with store_fixture(tmp_path) as (_,store,_):
        svc,_=fixture(store)
        svc.bounds=replace(svc.bounds,max_row_bytes=4096,max_resident_bytes=65536)
        spec=make('KernelSpec',kind='SortOp',configuration={'keys':['k']})
        window=SortWindow(32768,8192,4000,64,2,True)
        rows=[record({'k':str(i%5)*400,'unused':i},i) for i in range(30,-1,-1)]
        # Offsets and source provenance survive; only internal carrier payload
        # is omitted. The original immutable runs remain caller-owned.
        runs=build_sort_runs(iter(rows),spec=spec,services=svc,window=window)
        assert len(runs)>2
        real_read=store.read
        activity={'active':0,'peak':0,'chunks':[]}
        def observed(ref,**kwargs):
            activity['chunks'].append(kwargs['chunk_bytes'])
            activity['active']+=1
            activity['peak']=max(activity['peak'],activity['active'])
            try:yield from real_read(ref,**kwargs)
            finally:activity['active']-=1
        monkeypatch.setattr(store,'read',observed)
        result=list(merge_sort_runs(runs,spec=spec,services=svc,window=window))
        assert [item.sequence for item in result]==sorted(range(31),key=lambda i:(str(i%5)*400,i))
        assert all(dict(item.inline)=={} for item in result)
        assert activity['peak']==2 and activity['active']==0 and set(activity['chunks'])=={64}
        assert all(store.inspect(run) for run in runs)
        with pytest.raises(DataflowError,match='sort subwindow'):
            build_sort_runs(iter(rows),spec=spec,services=svc,window=replace(window,resident_bytes=65537))
        with pytest.raises(DataflowError) as failure:
            build_sort_runs(iter([record({'k':'x'*3000})]),spec=spec,services=svc,window=window)
        assert failure.value.code=='BUDGET_EXCEEDED'
        closed=merge_sort_runs(runs,spec=spec,services=svc,window=window)
        next(closed)
        closed.close()
        assert activity['active']==0
