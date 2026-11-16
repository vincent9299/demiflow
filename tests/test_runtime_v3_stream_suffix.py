"""Real bounded streaming suffixes keep native publication and source ownership."""
import json
from pathlib import Path

import pytest

from demiflow import data
from demiflow.execution.dataflow import DataflowExecution
from demiflow.execution.run_context import RunContext


@pytest.fixture(autouse=True)
def _worktree_import():
    import demiflow
    assert Path(demiflow.__file__).resolve().is_relative_to(Path(__file__).resolve().parents[1])


def _capture(monkeypatch):
    plans, allocations = [], []
    prepare, launch = DataflowExecution.prepare, DataflowExecution._prepare_launch
    def preparing(execution):
        result = prepare(execution)
        plans.append((execution._graph, execution._physical))
        return result
    def launching(execution, lease, stage):
        result = launch(execution, lease, stage)
        allocations.append(lease.allocation_id)
        return result
    monkeypatch.setattr(DataflowExecution, 'prepare', preparing)
    monkeypatch.setattr(DataflowExecution, '_prepare_launch', launching)
    return plans, allocations


def _check_profiles(graph, physical, *, shared_cpu=False):
    from demiflow.planning.dataflow_place import partition_stages
    originals = partition_stages(graph, policy=physical.policy, bounds=physical.bounds)
    by_node = {s.occurrences[0]: s for s in originals}
    for stage in physical.stages:
        if len(stage.occurrences) < 2:
            continue
        assert stage.profile.requests['cpu_millis'] == (max if shared_cpu else sum)(
            by_node[o].profile.requests['cpu_millis'] for o in stage.occurrences)
        assert stage.profile.requests['memory_bytes'] == sum(
            max(by_node[o].resident_bytes, by_node[o].profile.requests['memory_bytes'])
            for o in stage.occurrences)
        assert not set(stage.occurrences) & set(graph.sources)


def test_live_native_write_uses_two_allocations_and_final_replay_never_recreates_actor(tmp_path, monkeypatch):
    import lance
    import pyarrow as pa
    events = tmp_path/'actor-events'
    class Actor:
        def __init__(self):
            with events.open('a') as stream: stream.write('construct\n')
        def __call__(self, row):
            with events.open('a') as stream: stream.write('call:'+str(row['n'])+'\n')
            return row
    plans, allocations = _capture(monkeypatch)
    target = tmp_path/'out.lance'
    def pipeline():
        return (data.from_items([{'n':n} for n in range(5)])
            .map(Actor, execution='thread', concurrency=1)
            .save_lance(target, schema=pa.schema([('n',pa.int64())]), key='n', stage='saved', max_batch=2))
    class Finished(Exception): pass
    with pytest.raises(Finished):
        with RunContext.start(tmp_path/'run', limits={'checkpoint_rows':2}) as run, data.local_execution(workers=2):
            ref = run.ref
            result = pipeline().run_stream().outputs['saved']
            commits = run.journal.execution_commits('action-1')
            with run.journal._connection() as db:
                scopes = list(db.execute('SELECT scope_id,declaration FROM runtime_commit_scopes'))
            native = [(sid,json.loads(raw)) for sid,raw in scopes
                      if json.loads(raw).get('schema') == 'demiflow.stream_lance.v3']
            assert native and all(run.journal.execution_commits(sid)[-1]['status']=='acked' for sid,_ in native)
            assert all(d['request']['records'] or d['request']['finish'] for _,d in native)
            raise Finished()
    graph, physical = plans[0]
    assert len(physical.stages) == len(allocations) == 2
    writer = next(n for n in graph.nodes if n.kernel.kind == 'SaveLanceOp')
    assert writer.kernel.effect == 'journaled' and writer.kernel.resource_role == 'io'
    # A locally owned class factory now restores its own automatic state. The
    # separate borrowed callback test below still locks the attempt-only case.
    assert physical.recovery == 'checkpoint'
    _check_profiles(graph, physical)
    before = events.read_text()
    assert before.splitlines() == ['construct'] + ['call:'+str(n) for n in range(5)]
    before_version = lance.dataset(str(target)).version
    with RunContext.resume(ref) as resumed, data.local_execution(workers=2):
        assert pipeline().run_stream().outputs['saved'] == result
        assert resumed.journal.execution_commits('action-1') == commits
    assert len(allocations) == 2 and events.read_text() == before
    assert lance.dataset(str(target)).version == before_version
    assert lance.dataset(**result).to_table().to_pylist() == [{'n':n} for n in range(5)]


def test_nonfinal_live_checkpoint_cannot_recreate_retired_attempt(tmp_path, monkeypatch):
    from demiflow.execution.dataflow_checkpoint import DataflowCheckpointCoordinator
    from demiflow.execution.contracts.dataflow_errors import DataflowError
    events = tmp_path/'calls'
    def callback(row):
        with events.open('a') as stream: stream.write(str(row['n'])+'\n')
        return row
    def pipeline():
        return data.from_items([{'n':n} for n in range(8)]).map(callback, execution='thread', concurrency=1)
    class Interrupted(Exception): pass
    commit = DataflowCheckpointCoordinator.commit
    def interrupt(coordinator, *args, **kwargs):
        image = commit(coordinator, *args, **kwargs)
        if not image.cut.final: raise Interrupted()
        return image
    with pytest.raises(Interrupted):
        with RunContext.start(tmp_path/'run',limits={'checkpoint_rows':1}) as run, data.local_execution(workers=2):
            ref = run.ref
            with monkeypatch.context() as patch:
                patch.setattr(DataflowCheckpointCoordinator, 'commit', interrupt)
                pipeline().take_all()
    before = events.read_text()
    assert 0 < len(before.splitlines()) < 8
    with pytest.raises(DataflowError,match='no final ACKed result'):
        with RunContext.resume(ref), data.local_execution(workers=2):
            pipeline().take_all()
    assert events.read_text() == before


def test_shared_prepend_has_four_real_workers_one_source_and_original_ports(tmp_path, monkeypatch):
    plans, allocations = _capture(monkeypatch)
    seen = tmp_path/'reads'
    def once(row):
        with seen.open('a') as stream: stream.write(str(row['n'])+'\n')
        return row
    with RunContext.start(tmp_path/'run',limits={'checkpoint_rows':2,'timeout_s':15}), data.local_execution(workers=4):
        source = data.from_items([{'n':n} for n in range(1,5)]).map(once)
        assert source.prepend(source.limit(3),max_rows=3).take_all() == [{'n':n} for n in (1,2,3,1,2,3,4)]
    graph, physical = plans[0]
    assert len(physical.stages) == len(allocations) == 4
    assert seen.read_text().splitlines() == ['1','2','3','4']
    prepend = next(n for n in graph.nodes if n.kernel.kind == 'StreamPrependOp')
    stage = next(s for s in physical.stages if prepend.occurrence_id in s.occurrences)
    assert len(stage.occurrences) == 2
    assert {e.input_port for e in graph.edges if e.consumer == prepend.occurrence_id} == {'main','prefix'}
    assert any(e.mode == 'finite_sealed' for e in physical.exchanges)
    assert all(e.mode != 'durable_incremental' for e in physical.exchanges)
    _check_profiles(graph, physical)


def test_completed_live_result_still_rejects_actual_code_drift(tmp_path, monkeypatch):
    from demiflow.execution.recovery_contract import RecoveryContractError
    from demiflow.execution.contracts.dataflow_errors import DataflowError
    _, allocations = _capture(monkeypatch)
    def callback(row): return row
    def pipeline():
        return data.from_items([{'n':0}]).map(callback,execution='thread',concurrency=1)
    class Finished(Exception): pass
    with pytest.raises(Finished):
        with RunContext.start(tmp_path/'run') as run,data.local_execution(workers=2):
            ref=run.ref
            assert pipeline().take_all()==[{'n':0}]
            raise Finished()
    before=len(allocations)
    callback.__code__=(lambda row: {'n':99}).__code__
    with pytest.raises((DataflowError,RecoveryContractError),match='pin|identity|declaration|changed|differ|mismatch'):
        with RunContext.resume(ref),data.local_execution(workers=2):
            pipeline().take_all()
    assert len(allocations)==before


@pytest.mark.parametrize('mode', ['thread','process'])
def test_plain_iterator_map_early_close_keeps_original_demand_in_one_pure_worker(tmp_path, monkeypatch, mode):
    plans, allocations = _capture(monkeypatch)
    reads = tmp_path/'reads'
    def source():
        for n in range(10000):
            with reads.open('a') as stream: stream.write(str(n)+'\n')
            yield {'n':n}
    with RunContext.start(tmp_path/'run') as run,data.local_execution(workers=2,worker_mode=mode,batch_rows=5):
        assert data.from_iter(source).map(lambda row:row).take(1) == [{'n':0}]
    assert 1 <= len(reads.read_text().splitlines()) <= 25
    graph, physical = plans[0]
    assert len(physical.stages) == len(allocations) == 1
    # max_workers is a ceiling: this pure synchronous chain has no native
    # completion or timer that could be starved by the source. All original
    # logical nodes stay visible; actual CPU shares and memory still sums.
    from demiflow.planning.dataflow_place import partition_stages
    originals = partition_stages(graph, policy=physical.policy, bounds=physical.bounds)
    stage = physical.stages[0]
    assert set(stage.occurrences) == {node.occurrence_id for node in graph.nodes}
    assert stage.profile.requests['cpu_millis'] == max(s.profile.requests['cpu_millis'] for s in originals)
    assert stage.profile.requests['memory_bytes'] == sum(
        max(s.resident_bytes, s.profile.requests['memory_bytes']) for s in originals)
    assert _actual_grants(run, allocations) == sorted(
        (s.profile.requests['cpu_millis'], s.profile.requests['memory_bytes'])
        for s in physical.stages)


@pytest.mark.parametrize('change', ['effect','device','budget','fusion'])
def test_suffix_does_not_swallow_unknown_effect_device_or_exceed_grant(tmp_path, change):
    from dataclasses import replace
    import pyarrow as pa
    from demiflow.execution.contracts.dataflow import ActionRequest, HostDescriptor
    from demiflow.execution.contracts.dataflow_codec import _thaw_json
    from demiflow.execution.dataflow_api import _CompileContext
    from demiflow.execution.run_context import RunAction
    from demiflow.planning.dataflow_compile import lower_action
    from demiflow.planning.dataflow_place import partition_stages
    from demiflow.planning.dataflow_policy import resolve_runtime_policy
    from demiflow.planning.dataflow_thread_fusion import fuse_thread_suffix
    from demiflow.resources.contracts import ResourceProfile
    ds = (data.from_items([{'n':0}]).map(lambda r:r,execution='thread')
          .save_lance(tmp_path/'out.lance',schema=pa.schema([('n',pa.int64())]),key='n',stage='out'))
    request = ActionRequest(kind='run_stream',source=ds._source,plan=ds._plan,arguments={},
        sink=None,programs=(),provider='local',action=RunAction(_CompileContext(),'compile',{}))
    policy,bounds = resolve_runtime_policy({'max_active_workers':2,'worker_mode':'thread'},{},provider='local')
    graph = lower_action(request,bounds=bounds).graph
    if change in {'effect','device'}:
        nodes=[]
        for node in graph.nodes:
            if node.kernel.kind=='SaveLanceOp':
                config = _thaw_json(node.kernel.configuration)
                kernel = (replace(node.kernel,configuration=config,effect='unknown_replay_forbidden') if change=='effect' else
                    replace(node.kernel,profile=ResourceProfile(requests=dict(node.kernel.profile.requests),
                        requirements={'isolation':'device'}),configuration=config))
                node=replace(node,kernel=kernel)
            nodes.append(node)
        graph=replace(graph,nodes=tuple(nodes))
    stages=partition_stages(graph,policy=policy,bounds=bounds)
    if change=='budget': bounds=replace(bounds,max_resident_bytes=stages[0].resident_bytes)
    if change=='fusion': policy=replace(policy,fusion=False)
    hosts = (HostDescriptor(host_id='local', provider='local', runtime_revision='test',
        store_domains=(), logical_resources={'cpu_millis': 16000, 'memory_bytes': 2**30},
        isolation_capabilities=()),)
    assert fuse_thread_suffix(graph,stages,policy=policy,bounds=bounds,hosts=hosts)==stages


def _actual_grants(run, allocations):
    with run.journal._connection() as db:
        rows = db.execute('SELECT allocation_id,effective FROM resource_allocations').fetchall()
    assert {row[0] for row in rows} == set(allocations)
    effective = [json.loads(row[1]) for row in rows]
    return sorted((item['cpu_millis'], item['memory_bytes']) for item in effective)


def test_unfused_independent_workers_keep_summed_grants(tmp_path, monkeypatch):
    from dataclasses import replace
    from demiflow.execution import dataflow_api
    from demiflow.planning.dataflow_place import partition_stages
    resolve = dataflow_api.resolve_runtime_policy
    def unfused(*args, **kwargs):
        policy, bounds = resolve(*args, **kwargs)
        return replace(policy, fusion=False), bounds
    monkeypatch.setattr(dataflow_api, 'resolve_runtime_policy', unfused)
    plans, allocations = _capture(monkeypatch)
    with RunContext.start(tmp_path/'run') as run, data.local_execution(workers=3):
        assert data.from_items([{'n':0},{'n':1}]).map(lambda r: {'n':r['n']+1}).take_all() == [{'n':1},{'n':2}]
    graph, physical = plans[0]
    originals = partition_stages(graph, policy=physical.policy, bounds=physical.bounds)
    assert len(originals) == len(physical.stages) == len(allocations) == 3
    assert all(len(s.occurrences) == 1 for s in physical.stages)
    grants = _actual_grants(run, allocations)
    assert grants == sorted((s.profile.requests['cpu_millis'], s.profile.requests['memory_bytes'])
                            for s in originals)
    assert sum(cpu for cpu, _ in grants) == 3000
