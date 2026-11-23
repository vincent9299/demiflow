"""Real stored join rows, original finite ACK/exit and native publication."""
from dataclasses import asdict
import json
import math
from types import SimpleNamespace

import lance
import pyarrow as pa
import pytest

from demiflow import data
from demiflow.execution import RunContext
from demiflow.execution.contracts.dataflow import ArtifactRef
from demiflow.execution.contracts.dataflow_errors import DataflowError
from demiflow.execution.action_driver.dataflow import DataflowExecution
from demiflow.execution.scheduler.dataflow_publication_budget import PublicationReservation, FIELD, SCHEMA, PHASE_SCHEMA, REF_CARRIER
from demiflow.execution.outputs.lance_sink import LanceSink


class Interrupted(Exception):
    pass


def _execution(memory):
    return data.local_execution(workers=1, worker_mode='thread', native_resources={
        'memory_bytes':memory, 'max_rss_bytes':2*1024**3})


def _write(target, rows, right, *, schema=None):
    return data.from_items(rows).join(data.from_items(right), on='k', how='left').write_lance(
        str(target), mode='create', schema=schema, return_receipt=True)


def _assert_physical_publication(execution, observations):
    scheduler = execution._scheduler
    active = {scheduler._lease_stage[a] for a in scheduler._resident_by_allocation}
    reservation = execution._scheduler._publication_reservation
    assert reservation.scoped and active <= reservation.stages
    assert execution._completed_stages and not execution._completed_stages.intersection(active)
    assert scheduler._resident_used + reservation.reserved(active, publishing=True) <= execution._bounds.max_resident_bytes
    observations.append((active, scheduler._resident_used, reservation.bytes))


@pytest.mark.parametrize('memory,copies', [(64*1024**2,450000),(128*1024**2,750000)])
def test_original_wide_join_native_return_loss_reuses_acked_phases(tmp_path, monkeypatch, memory, copies):
    payload = b'full-original\x00' * copies
    rows = [{'k':1,'payload':payload},{'k':2,'payload':None}]
    right = [{'k':1,'tag':'a'},{'k':1,'tag':'b'}]
    schema = pa.schema([('k',pa.int64()),('payload',pa.large_binary()),('tag',pa.string())])
    target, observations, executions = tmp_path/'wide.lance', [], []
    prepare, commit = DataflowExecution.prepare, LanceSink.commit
    def prepared(execution):
        result = prepare(execution)
        assert execution._bounds.max_resident_bytes == memory
        assert execution._bounds.max_row_bytes == 16*1024**2
        assert execution._bounds.max_batch_bytes == 32*1024**2
        executions.append(execution)
        return result
    def lost(sink, staged, context):
        _assert_physical_publication(executions[-1], observations)
        commit(sink, staged, context)
        raise Interrupted('native receipt lost after real publication')
    with monkeypatch.context() as patch:
        patch.setattr(DataflowExecution,'prepare',prepared)
        patch.setattr(LanceSink,'commit',lost)
        with pytest.raises(Interrupted,match='receipt lost'):
            with RunContext.start(tmp_path/'run', limits={'max_row_bytes':16*1024**2,
                    'max_batch_bytes':32*1024**2,'timeout_s':120}) as run, _execution(memory):
                ref = run.ref
                _write(target,rows,right,schema=schema)
    assert observations and lance.dataset(str(target)).version == 1
    before = run.journal.execution_commits('action-1')
    assert before[-1]['status'] == 'prepared'
    assert sum(f['status']=='acked' for f in before) >= 3
    def forbidden(*args, **kwargs):
        pytest.fail('recovery restarted a completed source/join/terminal')
    with monkeypatch.context() as patch:
        patch.setattr(DataflowExecution,'_prepare_launch',forbidden)
        with RunContext.resume(ref) as resumed, _execution(memory):
            receipt = _write(target,rows,right,schema=schema)
            facts = resumed.journal.execution_commits('action-1')
            assert facts[:-1] == before[:-1]
            assert facts[-1]['status'] == 'acked'
    assert receipt.committed_version == lance.dataset(str(target)).version == 1
    actual = lance.dataset(str(target)).to_table().to_pylist()
    assert sorted((r['k'],r['tag'] or '') for r in actual) == [(1,'a'),(1,'b'),(2,'')]
    assert all(r['payload']==payload for r in actual if r['k']==1)
    assert [r['payload'] for r in actual if r['k']==2] == [None]


def _finite_payloads(execution):
    from demiflow.execution.storage.segments import block_chunks
    from demiflow.execution.storage.streams import frame_stream
    from demiflow.execution.contracts.record_codec import decode_frame, decode_document, line_limit
    result = []
    for channel, value in execution._finite_seals.items():
        ref = ArtifactRef(**value) if isinstance(value,dict) else value
        seal = json.loads(b''.join(execution._store.read(ref,max_bytes=execution._bounds.max_control_bytes)))
        for member in seal['producers']:
            stream = frame_stream(block_chunks(execution._store,ArtifactRef(**member['manifest_ref']),execution._bounds),execution._bounds)
            try:
                while line := stream.readline(line_limit(execution._bounds)):
                    raw,item = decode_frame(line,execution._bounds,finite=True)
                    record = decode_document(raw,record_id=item['record_id'],sequence=item['sequence'],bounds=execution._bounds)
                    if record.payload_ref is not None:
                        assert record.payload_ref.media_type == 'application/octet-stream'
                        result.append(record.payload_ref)
            finally:
                stream.close()
    return result


@pytest.mark.parametrize('damage',[None,'bytes','missing'])
def test_join_phase_ack_keeps_payload_roots_and_resumes_or_rejects_corruption(tmp_path,monkeypatch,damage):
    rows = [{'k':1,'typed':(-0.0,1.0),'payload':b'full\x00value'}, {'k':2,'typed':None,'payload':None}]
    right = [{'k':1,'tag':'a'},{'k':1,'tag':'b'}]
    target, first = tmp_path/'small.lance', {}
    prepare, retire = DataflowExecution.prepare, DataflowExecution._retire_phase
    def prepared(execution):
        result = prepare(execution)
        if not first:
            raw = b'private-unrooted-join-phase-orphan'
            first.update(execution=execution, orphan=execution._store.put(iter((raw,)),max_bytes=len(raw),media_type='application/octet-stream'))
        return result
    def interrupted(execution,image):
        stages = execution._phases[execution._phase_index]
        nodes = {n.occurrence_id:n for n in execution._graph.nodes}
        if any(nodes[o].kernel.kind=='join' for s in execution._physical.stages if s.stage_id in stages for o in s.occurrences):
            assert execution._action.context.journal.execution_commit(image.commit_id)['status']=='acked'
            first.update(commit=image.commit_id, facts=execution._action.context.journal.execution_commits('action-1'),
                         completed=set(execution._completed_stages)|set(stages), plan_id=execution._physical.plan_id,
                         payloads=_finite_payloads(execution))
            assert first['payloads']
            raise Interrupted('after original join phase ACK')
        return retire(execution,image)
    limits = {'memory_bytes':64*1024**2,'max_row_bytes':16*1024**2,'max_batch_bytes':32*1024**2,'timeout_s':120}
    with monkeypatch.context() as patch:
        patch.setattr(DataflowExecution,'prepare',prepared)
        patch.setattr(DataflowExecution,'_retire_phase',interrupted)
        with pytest.raises(Interrupted,match='join phase ACK'):
            with RunContext.start(tmp_path/'run',limits=limits) as run, _execution(64*1024**2):
                ref = run.ref
                _write(target,rows,right)
    execution = first['execution']
    assert not target.exists()
    assert execution._published_gc['blocked'] is None and execution._published_gc['removed'] > 0
    assert not execution._store.inspect(first['orphan'])
    assert all(execution._store.inspect(r) for r in first['payloads'])
    from demiflow.execution.contracts.record_codec import _decode_payload
    raw = execution._store._object_path(first['payloads'][0]).read_bytes()
    value = _decode_payload(raw,max_bytes=limits['max_row_bytes'])
    if value['typed'] is not None:
        assert type(value['typed']) is tuple and type(value['payload']) is bytes
        assert math.copysign(1,value['typed'][0]) == -1
    if damage:
        path = execution._store._object_path(first['payloads'][0])
        if damage == 'missing':path.unlink()
        else:path.write_bytes(b'!' + raw[1:])
    launch, launched = DataflowExecution._prepare_launch, []
    def launching(execution,lease,stage):
        assert execution._physical.plan_id == first['plan_id']
        assert stage.stage_id not in first['completed']
        launched.append(stage.stage_id)
        return launch(execution,lease,stage)
    def resume():
        with RunContext.resume(ref) as resumed, _execution(64*1024**2):
            assert resumed.journal.execution_commits('action-1') == first['facts']
            return _write(target,rows,right)
    with monkeypatch.context() as patch:
        patch.setattr(DataflowExecution,'_prepare_launch',launching)
        if damage:
            with pytest.raises(DataflowError,match='absent|reference|digest|bytes|artifact|Artifact'):
                resume()
            assert not target.exists()
        else:
            receipt = resume()
            assert receipt.written_rows == 3 and launched
            actual = lance.dataset(str(target)).to_table().to_pylist()
            assert sorted((r['k'],r['tag'] or '') for r in actual)==[(1,'a'),(1,'b'),(2,'')]
            assert all(r['payload']==b'full\x00value' for r in actual if r['k']==1)
            assert all(math.copysign(1,r['typed'][0])==-1 for r in actual if r['k']==1)
            assert run.journal.execution_commits('action-1')[-1]['status']=='acked'


def test_same_wide_input_smaller_memory_cannot_start_or_publish(tmp_path,monkeypatch):
    def forbidden(*args,**kwargs):pytest.fail('insufficient original envelope reached native publication')
    monkeypatch.setattr(LanceSink,'prepare',forbidden)
    target=tmp_path/'blocked.lance'
    with pytest.raises(DataflowError) as error:
        with RunContext.start(tmp_path/'run',limits={'max_row_bytes':16*1024**2,'max_batch_bytes':32*1024**2}), _execution(32*1024**2):
            _write(target,[{'k':1,'payload':b'full-original\x00'*450000}],[{'k':1}])
    assert error.value.code in {'RESOURCE_INFEASIBLE','BUDGET_EXCEEDED'}
    assert not target.exists()


def test_sealed_phase_reservation_preserves_old_plan_and_unexited_owner():
    bounds=SimpleNamespace(max_control_bytes=8,max_resident_bytes=64)
    stages=[SimpleNamespace(stage_id='s',occurrences=('source',)),SimpleNamespace(stage_id='t',occurrences=('terminal',))]
    def reservation(schema):
        config={} if schema is None else {FIELD:{'schema':schema,'resident_bytes':48}}
        graph=SimpleNamespace(nodes=[SimpleNamespace(kind='terminal',occurrence_id='terminal',kernel=SimpleNamespace(configuration=config))])
        return PublicationReservation(graph,stages,bounds)
    assert reservation(None).reserved(('s',))==8
    assert reservation(SCHEMA).reserved(('s',))==56
    current=reservation(PHASE_SCHEMA)
    assert current.reserved(('s',))==8
    assert current.reserved(('s','t'))==current.reserved(('t',))==56
    with pytest.raises(DataflowError,match='physically resident'):
        current.admit(('s','t'),56,64,publishing=True)
    current.admit(('t',),8,64,publishing=True)


def test_real_ray_stored_join_payload_and_original_final_ack(tmp_path, monkeypatch):
    import tempfile
    ray=pytest.importorskip('ray')
    from demiflow.data.api import _use_executor
    from demiflow.execution.executors.ray import RayDatasetExecutor
    from runtime_v3_support import _RayDataBlocker
    assert not ray.is_initialized()
    target=tmp_path/'ray.lance'
    rows=[{'k':1,'payload':b'full-original\x00'*450000}]
    right=[{'k':1,'tag':'a'}]
    limits={'memory_bytes':64*1024**2,'max_row_bytes':16*1024**2,
            'max_batch_bytes':32*1024**2,'timeout_s':120}
    observed=[]
    prepare=DataflowExecution.prepare
    def prepared(execution):
        result=prepare(execution)
        assert execution._physical.policy.provider=='ray_core'
        assert execution._scheduler._publication_reservation.scoped
        observed.append(execution)
        return result
    monkeypatch.setattr(DataflowExecution,'prepare',prepared)
    executor=RayDatasetExecutor(workers=1)
    with tempfile.TemporaryDirectory(prefix='demiflow-join-ray-',dir='/tmp') as directory:
        try:
            ray.init(address='local',num_cpus=4,num_gpus=0,include_dashboard=False,
                     object_store_memory=100*1024**2,_temp_dir=directory)
            with _RayDataBlocker() as blocker, _use_executor(executor):
                with pytest.raises(Interrupted,match='after action ACK'):
                    with RunContext.start(tmp_path/'run',limits=limits) as run:
                        ref=run.ref
                        receipt=_write(target,rows,right)
                        before=run.journal.execution_commits('action-1')
                        assert before and all(f['status']=='acked' for f in before)
                        assert receipt.written_rows==1
                        raise Interrupted('after action ACK')
                def forbidden(*args,**kwargs):pytest.fail('Ray final ACK recovery launched a worker')
                monkeypatch.setattr(DataflowExecution,'_prepare_launch',forbidden)
                with RunContext.resume(ref) as resumed:
                    receipt=_write(target,rows,right)
                    assert resumed.journal.execution_commits('action-1')==before
                assert receipt.committed_version==lance.dataset(str(target)).version==1
                assert lance.dataset(str(target)).to_table().to_pylist()==[{**rows[0],'tag':'a'}]
                assert not blocker.attempts
        finally:
            executor.close()
            ray.shutdown()
    assert not ray.is_initialized() and observed


def test_writer_metadata_bounds_real_ipc_without_charging_a_fixed_64k():
    import io
    from dataclasses import replace
    from demiflow.planning.dataflow_policy import resolve_runtime_policy
    from demiflow.execution.batch_conversion import native_writer_workspace, admit_batch_conversion
    _, bounds = resolve_runtime_policy({}, {}, provider='local')
    for schema, row in [
        (pa.schema([('n',pa.int64())]), {'n':1}),
        (pa.schema([('nested',pa.list_(pa.struct([('value',pa.binary())])))],
                   metadata={b'note':b'x'*16000}), {'nested':[{'value':b'a'}]}),
    ]:
        table=pa.Table.from_pylist([row],schema=schema)
        batch=table.to_batches()[0]
        actual=io.DEFAULT_BUFFER_SIZE+schema.serialize().size+pa.ipc.get_record_batch_size(batch)-batch.nbytes
        declared=native_writer_workspace(schema,bounds)
        assert declared>=actual
        if schema.metadata:
            with pytest.raises(DataflowError,match='conversion workspace'):
                admit_batch_conversion([row],'pyarrow',schema=schema,
                    bounds=replace(bounds,max_resident_bytes=12000),resident_workspace=True,
                    writer_copies=2,writer_metadata=True)
        else:
            assert declared<16*1024


def test_writer_metadata_covers_nested_variadic_view_buffers_before_construction():
    import io
    from demiflow.planning.dataflow_policy import resolve_runtime_policy
    from demiflow.execution.batch_conversion import admit_batch_conversion
    _,bounds=resolve_runtime_policy({}, {},provider='local')
    schema=pa.schema([('values',pa.list_(pa.binary_view()))])
    rows=[{'values':[b'x'*40000 for _ in range(200)]}]
    admitted=[]
    admit_batch_conversion(rows,'pyarrow',schema=schema,bounds=bounds,
        resident_workspace=True,writer_copies=2,writer_metadata=True,
        observe_writer_workspace=admitted.append)
    table=pa.Table.from_pylist(rows,schema=schema)
    batch=table.to_batches()[0]
    actual=io.DEFAULT_BUFFER_SIZE+schema.serialize().size+pa.ipc.get_record_batch_size(batch)-batch.nbytes
    assert len(admitted)==1 and admitted[0]>=actual
