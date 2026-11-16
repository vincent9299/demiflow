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
        held=NS(retained_resident_bytes=0)
        runner.e=NS(_readers={'source':reader},_kernels={'held':held})
        runner.services,runner.bounds=services,bounds
        runner._condition=threading.Condition();runner._wake_generation=0
        runner._peer=NS(_bytes=0);runner._finite_input=None
        reader._shared_frame_call=runner._frame_call
        assert reader.next_record().status=='record'
        assert reader.retained_bytes>0
        held.retained_resident_bytes=bounds.max_resident_bytes-32
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
