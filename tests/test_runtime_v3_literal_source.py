"""Large literal data keeps one original store, cursor and resource authority."""
from dataclasses import replace
import hashlib
import json
import math
import pickle
import threading
from types import SimpleNamespace as NS

import pytest

from demiflow.data.sources import ItemsSource
from demiflow.execution.contracts.dataflow import LoadedProgram, ProgramDeclaration, thaw_record_value
from demiflow.execution.contracts.dataflow_errors import DataflowError
from demiflow.execution.dataflow_seal import (_stage_one_program, _code_identity_from_value,
    verify_dataflow_program)
from demiflow.execution.dataflow_sources import snapshot_source, plan_source_splits, open_source_reader
from demiflow.execution.literal_source import items_fingerprint, literal_reference, LiteralSourceReference
from demiflow.execution.recovery_contract import identity, RecoveryContractError
from demiflow.execution.worker.dataflow_graph_runner import WorkerGraphRunner
from test_runtime_v3_artifact_accounting import store_fixture


def example(count=270):
    return tuple({'i': i, 'payload': bytes([i % 251]) * 280,
                  'typed': (b'\x00\xff', [i, -0.0, 0.0], None)} for i in range(count))


def services_for(store, bounds, snapshot, reference):
    return NS(store=store, bounds=bounds, assignment=NS(stages=()),
        programs={snapshot.source_id: LoadedProgram('source', reference, identity(['program']))},
        check_stop=lambda: None)


def read_all(reader):
    rows=[]
    while True:
        poll=reader.next_record()
        if poll.status=='eoi':return rows
        rows.append(thaw_record_value(poll.record.inline))


def test_streamed_literal_fingerprint_preserves_original_pickle_aliases_and_bytes(monkeypatch):
    shared=[b'\x00' * 100000, -0.0, (None, 3)]
    values=(shared, shared, {'bytes': b'\xff', 'float': 0.0})
    expected=pickle.dumps(values, protocol=4)
    monkeypatch.setattr(pickle,'dumps',lambda *_a,**_k: pytest.fail('whole-source bytes copy'))
    digest,size=items_fingerprint(values)
    assert size==len(expected) and digest==hashlib.sha256(expected).hexdigest()


def test_small_literal_has_original_program_and_zero_data_artifacts(tmp_path):
    with store_fixture(tmp_path) as (_,store,_):
        source=ItemsSource(({'x':(b'raw',-0.0)},))
        before=set(store._objects.iterdir())
        snapshot=snapshot_source(source,action=NS(action_id='literal-small'),store=store,bounds=store._bounds)
        assert snapshot.manifest_ref is None and literal_reference(source,store._bounds) is None
        assert set(store._objects.iterdir())==before
        reader=open_source_reader(snapshot,plan_source_splits(snapshot,source,target_partitions=1,bounds=store._bounds)[0],cursor=None,
            services=services_for(store,store._bounds,snapshot,source))
        assert read_all(reader)==[{'x':(b'raw',-0.0)}]
        reader.close()


def test_large_source_program_excludes_payload_without_changing_code_pin(tmp_path):
    with store_fixture(tmp_path) as (_,store,_):
        bounds=replace(store._bounds,max_state_bytes=4096)
        source=ItemsSource(example())
        pin=_code_identity_from_value('source',source)
        declaration=ProgramDeclaration('a'*64,None,source,None,pin)
        ref=_stage_one_program(declaration,store=store,bounds=bounds)
        loaded=verify_dataflow_program(ref,expected_pin=pin,store=store,bounds=bounds)
        assert type(loaded.value) is LiteralSourceReference
        assert loaded.value.count==len(source.items)
        assert loaded.value.items_digest==hashlib.sha256(pickle.dumps(source.items,protocol=4)).hexdigest()
        assert _code_identity_from_value('source',loaded.value)==pin
        manifest=json.loads(b''.join(store.read(ref,max_bytes=bounds.max_control_bytes)))
        assert manifest['payload_ref']['size_bytes']<bounds.max_state_bytes
        assert not hasattr(loaded.value,'items')


def test_split_seek_and_original_cursor_resume_keep_typed_values_and_signed_zero(tmp_path):
    with store_fixture(tmp_path) as (_,store,_):
        bounds=replace(store._bounds,max_state_bytes=4096,max_row_bytes=8192,
            max_batch_bytes=16384,max_pending_events=11)
        source=ItemsSource(example())
        reference=literal_reference(source,bounds)
        snapshot=snapshot_source(source,action=NS(action_id='literal-splits'),store=store,bounds=bounds)
        assert snapshot.manifest_ref is not None
        services=services_for(store,bounds,snapshot,reference)
        splits=plan_source_splits(snapshot,source,target_partitions=3,bounds=bounds)
        actual=[]
        for split in splits:
            reader=open_source_reader(snapshot,split,cursor=None,services=services)
            first=reader.next_record().record
            actual.append(thaw_record_value(first.inline))
            reader.acknowledge_output()
            cursor=reader.snapshot()
            reader.close()
            assert reader.retained_bytes==0
            resumed=open_source_reader(snapshot,split,cursor=cursor,services=services)
            actual.extend(read_all(resumed))
            resumed.close()
        assert actual==list(source.items)
        assert all(type(row['typed']) is tuple and type(row['typed'][1]) is list for row in actual)
        assert all(math.copysign(1,row['typed'][1][1])==-1 and math.copysign(1,row['typed'][1][2])==1 for row in actual)


def test_literal_reader_uses_real_worker_credit_and_releases_buffer_on_failure(tmp_path):
    with store_fixture(tmp_path) as (_,store,_):
        bounds=replace(store._bounds,max_resident_bytes=256*1024,max_control_bytes=4096,
            max_state_bytes=4096,max_row_bytes=8192,max_batch_bytes=16384)
        source=ItemsSource(example(40))
        snapshot=snapshot_source(source,action=NS(action_id='literal-credit'),store=store,bounds=bounds)
        services=services_for(store,bounds,snapshot,literal_reference(source,bounds))
        split=plan_source_splits(snapshot,source,target_partitions=1,bounds=bounds)[0]
        reader=open_source_reader(snapshot,split,cursor=None,services=services)
        runner=WorkerGraphRunner.__new__(WorkerGraphRunner)
        from test_runtime_v3_finite_reader_credit import HeldState
        held=HeldState()
        runner.e=NS(_readers={'source':reader},_kernels={'held':held})
        runner.services,runner.bounds=services,bounds
        runner._condition=threading.Condition();runner._wake_generation=0
        runner._peer=NS(_bytes=0);runner._finite_input=None
        reader._shared_frame_call=runner._frame_call
        assert reader.next_record().status=='record'
        assert reader.retained_bytes>0
        held.retain(bounds.max_resident_bytes-32)
        with pytest.raises(DataflowError,match='resident budget'):
            reader.next_record()
        assert reader.retained_bytes==0
        reader.close()


def test_source_data_quota_rejects_before_snapshot_or_program_can_publish_a_result(tmp_path):
    with store_fixture(tmp_path,ceiling=4096) as (run,store,_):
        bounds=replace(store._bounds,max_state_bytes=4096)
        source=ItemsSource(example())
        with pytest.raises((DataflowError,RecoveryContractError),match='budget|max_bytes|staging|limit'):
            snapshot_source(source,action=NS(action_id='literal-spill'),store=store,bounds=bounds)
        assert not run.journal.execution_commits('action-1')
        assert not list(store._staging.glob('*.part'))


@pytest.mark.parametrize('format',['items','arrow','numpy','pandas'])
def test_materialized_literal_uses_original_handle_digest_and_declared_schema(tmp_path,format):
    from demiflow.data.sources import MaterializedSource
    from demiflow.execution.dataflow_inputs import build_input_handle
    import numpy as np
    import pandas as pd
    import pyarrow as pa
    values=[{'i':i,'value':b'raw\x00'*100} for i in range(40)]
    value={'items':values,'arrow':pa.Table.from_pylist(values),
        'numpy':np.arange(4000,dtype=np.int64).reshape(40,100),
        'pandas':pd.DataFrame(values)}[format]
    handle=build_input_handle(value,format=format)
    source=MaterializedSource(handle,handle.row_count)
    with store_fixture(tmp_path) as (_,store,_):
        bounds=replace(store._bounds,max_state_bytes=4096,max_row_bytes=8192,max_batch_bytes=16384)
        reference=literal_reference(source,bounds)
        assert reference.kind=='materialized'
        original=hashlib.sha256(pickle.dumps(handle,protocol=4)).hexdigest()
        snapshot=snapshot_source(source,action=NS(action_id='materialized-literal'),store=store,bounds=bounds)
        assert snapshot.immutable_identity['handle_digest']==original==reference.items_digest
        services=services_for(store,bounds,snapshot,reference)
        split=plan_source_splits(snapshot,source,target_partitions=1,bounds=bounds)[0]
        reader=open_source_reader(snapshot,split,cursor=None,services=services)
        rows=[];schemas=[]
        while True:
            poll=reader.next_record()
            if poll.status=='eoi':break
            rows.append(thaw_record_value(poll.record.inline));schemas.append(poll.record.schema_ref)
        reader.close()
        if format=='numpy':
            assert len(rows)==40
            assert np.array_equal(np.stack([row['data'] for row in rows]),value)
        else:
            assert rows==values
        assert all(ref is not None for ref in schemas) if format in ('arrow','pandas') else all(ref is None for ref in schemas)


class InterruptedLiteral(Exception):
    pass


def test_real_dataset_literal_original_ack_gc_and_resume(tmp_path,monkeypatch):
    from demiflow import data
    from demiflow.execution import RunContext
    from demiflow.execution.dataflow import DataflowExecution
    from demiflow.execution.dataflow_checkpoint import DataflowCheckpointCoordinator
    from demiflow.execution.contracts.dataflow import ArtifactRef
    from demiflow.execution.dataflow_segments import tree_refs
    rows=list(example())
    limits={'memory_bytes':64*1024**2,'max_row_bytes':8192,'max_batch_bytes':16384,
            'checkpoint_rows':8,'checkpoint_interval_ms':60000}
    prepare,close,commit=DataflowExecution.prepare,DataflowExecution.close,DataflowCheckpointCoordinator.commit
    first={};boundary={}
    def prepared(execution):
        result=prepare(execution)
        if not first:
            source,=execution._physical.sources
            assert source.kind=='materialized' and source.manifest_ref is not None
            manifest=json.loads(b''.join(execution._store.read(source.manifest_ref,
                        max_bytes=execution._bounds.max_control_bytes)))
            root=ArtifactRef(**manifest['data_ref'])
            refs=[source.manifest_ref,*tree_refs(execution._store,root,execution._bounds)]
            orphan=execution._store.put(iter((b'unrooted-literal-test',)),max_bytes=1024,
                                        media_type='application/octet-stream')
            first.update(execution=execution,refs=refs,orphan=orphan,plan_id=execution._physical.plan_id)
        return result
    def closed(execution):
        try:return close(execution)
        finally:
            if execution is first.get('execution'):first['gc']=getattr(execution,'_published_gc',None)
    def committed(coordinator,*args,**kwargs):
        image=commit(coordinator,*args,**kwargs)
        if not image.cut.final and not boundary:
            publication=first['execution']._publication(image)
            prefix=publication['manifest']['total_rows']
            assert 0<prefix<len(rows)
            fact=coordinator._action.context.journal.execution_commit(image.commit_id)
            assert fact['status']=='acked'
            boundary.update(commit_id=image.commit_id,fact=fact,prefix=prefix)
            raise InterruptedLiteral('original literal ACK')
        return image
    with monkeypatch.context() as patch:
        patch.setattr(DataflowExecution,'prepare',prepared)
        patch.setattr(DataflowExecution,'close',closed)
        patch.setattr(DataflowCheckpointCoordinator,'commit',committed)
        with pytest.raises(InterruptedLiteral,match='original literal ACK'):
            with RunContext.start(tmp_path/'run',limits=limits) as run:
                reference=run.ref
                data.from_items(rows).take_all()
    assert boundary and first['gc']['blocked'] is None and first['gc']['removed']>=1
    assert not first['execution']._store.inspect(first['orphan'])
    assert all(first['execution']._store.inspect(ref) for ref in first['refs'])
    with RunContext.resume(reference) as run:
        assert run.journal.execution_commit(boundary['commit_id'])==boundary['fact']
        actual=data.from_items(rows).take_all()
    assert actual==rows
    assert all(math.copysign(1,r['typed'][1][1])==-1 for r in actual)
    # The original final ACK is a read-only result on a subsequent resume.
    def forbidden(*_args,**_kwargs):pytest.fail('completed literal result launched a worker')
    with monkeypatch.context() as patch:
        patch.setattr(DataflowExecution,'_prepare_launch',forbidden)
        with RunContext.resume(reference):
            assert data.from_items(rows).take_all()==rows
