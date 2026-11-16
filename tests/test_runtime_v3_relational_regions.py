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
        return data.from_items([{'k':1,'v':i} for i in range(6)]).join(
            data.from_items([{'k':1,'r':i} for i in range(3)]),on='k').join(
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
        assert sorted((r['v'],r['r'],r['z']) for r in actual)==[(v,r,z) for v in range(6) for r in range(3) for z in range(2)]
        after=run.journal.execution_commits('action-1')
        assert after[:len(before)]==before and all(x['status']=='acked' for x in after)
        assert local.stats['plan_id']==caught[0][1]
