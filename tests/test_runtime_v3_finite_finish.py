"""Finite result iteration participates in the same cuts and bounded channels."""
import json
import os
from dataclasses import replace
from pathlib import Path
import pytest
from demiflow import data
from demiflow.execution.run_context import RunContext
from demiflow.execution.action_driver.dataflow import DataflowExecution
from demiflow.execution.checkpoint.coordinator import DataflowCheckpointCoordinator
from demiflow.execution.contracts import state_codec


class Interrupted(Exception):
    pass


def pipeline(left=12, right=3):
    return data.from_items([{'k':0,'i':i} for i in range(left)]).join(
        data.from_items([{'k':0,'j':j} for j in range(right)]), on='k')


def test_finite_join_output_checkpoint_restores_exact_prefix_and_multiplicity(tmp_path, monkeypatch):
    original_event = DataflowExecution._handle_event
    original_commit = DataflowCheckpointCoordinator.commit
    original_launch = DataflowExecution._prepare_launch
    seen, boundary = [], {}
    resumed_stages = []
    def event(execution, event):
        result = original_event(execution,event)
        if event.kind=='terminal':
            seen.append(event.payload['record_id'])
            if len(seen)==5:
                execution.request_checkpoint()
        return result
    def commit(coordinator,*args,**kwargs):
        image=original_commit(coordinator,*args,**kwargs)
        for contribution in image.contributions:
            for ref in contribution.operator_states.values():
                if ref.media_type!='application/x-demiflow-kernel-image-v3':continue
                state=state_codec.loads(b''.join(coordinator._store.read(ref,max_bytes=ref.size_bytes)),
                    max_bytes=coordinator._physical.bounds.max_state_bytes)
                cursor=state.get('finish_cursor')
                if state.get('kind')=='join' and cursor and cursor['emitted']:
                    assert not image.cut.final
                    boundary.update(commit_id=image.commit_id,emitted=cursor['emitted'])
                    raise Interrupted('actual finite output ACK')
        # The original 256 MiB budget may place the join and terminal in
        # separate finite phases. Then the real resumable frontier belongs to
        # the terminal's sealed-input reader, not the already completed join.
        # Require an actual intermediate ACK and the exact consumed prefix;
        # final-only publication is still a failure of this counterexample.
        if len(seen) >= 5 and not image.cut.final:
            publication = json.loads(b''.join(coordinator._store.read(
                image.confirmed_sinks['take_all'], max_bytes=coordinator._physical.bounds.max_control_bytes)))
            prefix = publication['manifest']['total_rows']
            for contribution in image.contributions:
                for ref in contribution.operator_states.values():
                    if ref.media_type != 'application/json':
                        continue
                    frontier = json.loads(b''.join(coordinator._store.read(ref, max_bytes=ref.size_bytes)))
                    reads = frontier.get('finite_reads', {})
                    if not reads:
                        continue
                    consumed = sum(sum(value['positions'].values()) for value in reads.values())
                    if 0 < consumed < 1000:
                        assert consumed == prefix
                        completed = {stage.stage_id for stage in coordinator._physical.stages
                                     if stage.stage_id not in (coordinator.residency or {}).get('active_stages', ())}
                        boundary.update(commit_id=image.commit_id, emitted=consumed, completed=completed)
                        raise Interrupted('actual finite output ACK')
        return image
    with monkeypatch.context() as patch:
        patch.setattr(DataflowExecution,'_handle_event',event)
        patch.setattr(DataflowCheckpointCoordinator,'commit',commit)
        with pytest.raises(Interrupted,match='finite output ACK'):
            with RunContext.start(tmp_path/'run',limits={'checkpoint_rows':64,
                    'checkpoint_interval_ms':60000,'timeout_s':60}) as run,data.local_execution(workers=4):
                run_ref=run.ref
                pipeline(100,10).take_all()
    assert 0 < boundary['emitted'] < 1000
    def launch(execution, lease, stage):
        resumed_stages.append(stage.stage_id)
        return original_launch(execution, lease, stage)
    with monkeypatch.context() as patch:
        patch.setattr(DataflowExecution, '_prepare_launch', launch)
        with RunContext.resume(run_ref) as run,data.local_execution(workers=4):
            rows=pipeline(100,10).take_all()
            assert sorted((r['i'],r['j']) for r in rows)==[(i,j) for i in range(100) for j in range(10)]
            assert len(rows)==1000
            assert any(f['commit_id']==boundary['commit_id'] and f['status']=='acked'
                       for f in run.journal.execution_commits('action-1'))
    assert not set(resumed_stages) & boundary.get('completed', set())


def test_finite_join_memory_transport_does_not_accumulate_per_row_history(tmp_path, monkeypatch):
    # Tighten the actual sealed structural bound at its existing compiler seam.
    # RunContext does not expose a max_pending_events business-budget keyword.
    from demiflow.execution import dataflow_api
    original = dataflow_api.resolve_runtime_policy
    def bounded(*args,**kwargs):
        policy,bounds = original(*args,**kwargs)
        # Exercise a real cross-worker memory channel even when the compiler
        # can co-locate this finite result with its terminal. Keep the existing
        # channel bound and every original multiplicity/frontier assertion.
        return replace(policy,fusion=False),replace(bounds,max_pending_events=64)
    monkeypatch.setattr(dataflow_api,'resolve_runtime_policy',bounded)
    # Explicit small-row windows keep both real workers resident inside the
    # unchanged 256 MiB total. Disabling fusion alone permits a legal finite
    # fallback with the much larger implicit codec windows; that would never
    # exercise this memory-channel counterexample. Inputs/64-event cap stay.
    with RunContext.start(tmp_path/'run',limits={'checkpoint_rows':512,
            'max_row_bytes':64*1024,'max_batch_bytes':256*1024,
            'checkpoint_interval_ms':60000,'timeout_s':60}) as run,data.local_execution(workers=4):
        rows=pipeline(16,16).take_all()
        assert sorted((r['i'],r['j']) for r in rows)==[(i,j) for i in range(16) for j in range(16)]
        assert len(rows)==256
        frontiers=[]
        for path in (run.directory/'.dataflow-store').glob('*/objects/*'):
            raw=path.read_bytes()
            if b'demiflow.worker-frontier.v3' in raw:
                frontiers.append(json.loads(raw))
        assert frontiers
        assert all(len(f.get('consumed_deliveries',[]))<=64 for f in frontiers)
        memory = [f['memory_frontiers'] for f in frontiers if f.get('memory_frontiers')]
        assert memory and max(seq for item in memory for values in item.values() for seq in values.values()) >= 255
        assert max(sum(map(len,item.values())) for item in memory) <= 1
        assert all(not f['consumed_deliveries'] for f in frontiers)


def test_duplicate_memory_delivery_is_rejected_before_second_business_call(tmp_path, monkeypatch):
    from demiflow.execution import dataflow_api
    resolve = dataflow_api.resolve_runtime_policy
    def unfused(*args, **kwargs):
        policy, bounds = resolve(*args, **kwargs)
        return replace(policy, fusion=False), bounds
    # This counterexample requires an actual inter-worker memory delivery;
    # legal synchronous fusion must not erase the injection point.
    monkeypatch.setattr(dataflow_api, 'resolve_runtime_policy', unfused)
    from demiflow.execution.worker.dataflow_graph_runner import WorkerGraphRunner
    from demiflow.execution.contracts.dataflow_errors import DataflowError
    from _worker_event_fixtures import emit, events
    original = WorkerGraphRunner._receive_data
    injected = []
    path = str(tmp_path/'business')
    def observe(row):
        emit(path,'call',i=row['i'],j=row['j'])
        return row
    def receive(runner,command):
        edge = next(e for e in runner.e._graph.edges if e.edge_id==command['edge_id'])
        node = runner.e._node_specs[edge.consumer]
        spec = runner._exchange[edge.edge_id]
        if spec.mode=='memory_incremental' and node.kernel.kind=='MapOp' and not injected:
            injected.append(command['delivery_id'])
            original(runner,command)
            return original(runner,command)
        return original(runner,command)
    monkeypatch.setattr(WorkerGraphRunner,'_receive_data',receive)
    with pytest.raises(DataflowError,match='duplicate or reordered memory delivery'):
        # Tighten only the payload windows to require the intended real memory
        # delivery within the original total budget; never mutate a sealed plan.
        with RunContext.start(tmp_path/'run',limits={'max_row_bytes':64*1024,
                'max_batch_bytes':256*1024}),data.local_execution(workers=5,worker_mode='thread'):
            pipeline(1,1).map(observe).take_all()
    assert len(injected)==1
    assert events(path)==[{'event':'call','pid':os.getpid(),'i':0,'j':0}]


@pytest.mark.parametrize('corruption',[None,'negative','beyond_output','foreign_port','open_port'])
def test_finite_cursor_component_rejects_invalid_recovery_and_keeps_exact_suffix(corruption):
    # Component boundary only: real Kernel/DTO codec, bounded in-memory test
    # artifact store. The public join crash/restore case above covers RunJournal.
    from test_runtime_v3_k import services,spec,rec,InputPort,rows_of
    from demiflow.execution.kernels.operator import OperatorKernel,IMAGE_MEDIA_TYPE
    from demiflow.execution.contracts.dataflow_errors import DataflowError
    svc=services()
    declaration=spec('union',{},inputs=[InputPort('input','finite_complete')])
    kernel=OperatorKernel(declaration,services=svc,partition_id='p0');kernel.start()
    for i in range(4):list(kernel.push('input',rec({'i':i},sequence=i)))
    finishing=iter(kernel.finish('input'))
    assert rows_of([next(finishing)])==[{'i':0}]
    ref=kernel.snapshot()
    image=state_codec.loads(b''.join(svc.store.read(ref,max_bytes=ref.size_bytes)),max_bytes=svc.bounds.max_state_bytes)
    assert image['finish_cursor']=={'port':'input','emitted':1}
    finishing.close()
    resumed=OperatorKernel(declaration,services=svc,partition_id='p0')
    try:
        if corruption:
            if corruption=='negative':image['finish_cursor']['emitted']=-1
            elif corruption=='beyond_output':image['finish_cursor']['emitted']=2
            elif corruption=='foreign_port':image['finish_cursor']['port']='other'
            else:image['ports']['input']['eoi']=False
            raw=state_codec.dumps(image,max_bytes=svc.bounds.max_state_bytes)
            ref=svc.store.put(iter([raw]),max_bytes=len(raw),media_type=IMAGE_MEDIA_TYPE)
            resumed.restore(ref)
            with pytest.raises(DataflowError,match='finite output recovery frontier'):resumed.start()
        else:
            resumed.restore(ref);resumed.start()
            assert rows_of(resumed.finish('input'))==[{'i':1},{'i':2},{'i':3}]
            assert resumed._emitted==4 and resumed._finish_cursor is None
    finally:
        resumed.close();kernel.close()
