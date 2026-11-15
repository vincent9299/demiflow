"""Final sort keeps the original consumer write window while readers are live."""
from contextlib import contextmanager
from dataclasses import replace
import hashlib
import math

import pytest

from demiflow import data
from demiflow.execution.contracts.dataflow import Record
from demiflow.execution.contracts.dataflow_errors import DataflowError
from demiflow.execution.exchange.dataflow_bridge import _encode_payload,_encode_record_document,_decode_record_document
from demiflow.execution.kernels import ordering
from demiflow.execution.kernels.dataflow import _materialize_row
from test_runtime_v3_real_seams import prepared,reader_services
from test_runtime_v3_artifact_files import descriptors


@contextmanager
def sorted_runs(tmp_path,count):
    rows=[{'x':i,'typed':(-0.0,b'\xff'),'nested':[None,i]} for i in range(count,0,-1)]
    with prepared(tmp_path,data.from_items(rows).sort('x')) as (execution,services):
        runtime=reader_services(execution,services)
        spec=next(n.kernel for n in execution._graph.nodes if n.kernel.kind=='SortOp')
        records=[Record(hashlib.sha256(str(i).encode()).hexdigest(),{'partition':0,'offset':i},None,row,
                        len(_encode_payload(row,max_bytes=65536)),i) for i,row in enumerate(rows)]
        runs=tuple(ref for record in records for ref in ordering.build_sort_runs(iter([record]),spec=spec,services=runtime))
        yield runtime,spec,records,runs


@pytest.mark.parametrize('count',[1,5,11])
def test_final_merge_immediately_publishes_each_typed_record_with_live_readers(tmp_path,count):
    with sorted_runs(tmp_path,count) as (runtime,spec,records,runs):
        store=runtime.store;old=store._bounds
        store._bounds=replace(old,max_open_files=10)
        narrow=replace(runtime,bounds=store._bounds)
        iterator=ordering.merge_sort_runs(runs,spec=spec,services=narrow)
        seen=[]
        try:
            for index,record in enumerate(iterator):
                # This is still the live merge; never list(iterator) before put.
                assert 1 < store.open_file_slots <= 5
                assert descriptors(store)<=5
                expected=records[-1-index]
                assert record.record_id==expected.record_id
                assert record.source_position==expected.source_position
                assert record.sequence==expected.sequence
                raw=_encode_record_document(record,max_bytes=65536)
                ref=store.put(iter([raw]),max_bytes=len(raw),media_type='application/vnd.demiflow.record.v3')
                saved=b''.join(store.read(ref,max_bytes=65536))
                restored=_decode_record_document(saved,record_id=record.record_id,sequence=record.sequence,max_bytes=65536)
                row=_materialize_row(restored,narrow)
                assert type(row['typed']) is tuple and row['typed'][1]==b'\xff'
                assert math.copysign(1,row['typed'][0])==-1
                assert row==_materialize_row(expected,narrow)
                seen.append(row['x'])
            assert seen==list(range(1,count+1))
        finally:
            iterator.close();store._bounds=old
        assert store.open_file_slots==1 and descriptors(store)==1


@pytest.mark.parametrize('count,limit',[(1,7),(5,9)])
def test_infeasible_final_read_and_output_windows_fail_before_read(tmp_path,monkeypatch,count,limit):
    with sorted_runs(tmp_path,count) as (runtime,spec,records,runs):
        store=runtime.store;old=store._bounds
        store._bounds=replace(old,max_open_files=limit)
        calls=[]
        def forbidden(*args,**kwargs):
            calls.append(True)
            raise AssertionError('Infeasible final merge opened a reader')
        try:
            with monkeypatch.context() as patch:
                patch.setattr(store,'read',forbidden)
                with pytest.raises(DataflowError,match='input file slots'):
                    next(ordering.merge_sort_runs(runs,spec=spec,services=replace(runtime,bounds=store._bounds)))
            assert not calls and store.open_file_slots==1 and descriptors(store)==1
        finally:store._bounds=old


def test_final_sort_uses_effective_window_of_other_live_view(tmp_path):
    from demiflow.execution.dataflow_store import RunArtifactStore
    with sorted_runs(tmp_path,5) as (runtime,spec,records,runs):
        store=runtime.store
        other=RunArtifactStore(root=store.root,store_id=store.store_id,journal=store._journal,
                               bounds=replace(store._bounds,max_open_files=11))
        iterator=ordering.merge_sort_runs(runs,spec=spec,services=runtime)
        try:
            assert store._bounds.max_open_files==256
            assert store.open_file_slots==2
            assert store.available_file_slots()==other.available_file_slots()==9
            assert store.available_file_slots(maximum=10)==8
            assert store.available_file_slots(maximum=1)==0
            seen=[]
            for record in iterator:
                assert store.open_file_slots<=6 and descriptors(store)<=6
                raw=_encode_record_document(record,max_bytes=65536)
                ref=store.put(iter([raw]),max_bytes=len(raw),media_type='application/vnd.demiflow.record.v3')
                assert ref.size_bytes==len(raw)
                expected=records[-1-len(seen)]
                assert record.record_id==expected.record_id and record.source_position==expected.source_position
                assert _materialize_row(record,runtime)==_materialize_row(expected,runtime)
                seen.append(record)
            assert len(seen)==5
        finally:iterator.close();other.close()
        assert store.open_file_slots==1 and store.available_file_slots()==255


def test_other_live_view_infeasibility_rejects_before_sort_read(tmp_path,monkeypatch):
    from demiflow.execution.dataflow_store import RunArtifactStore
    with sorted_runs(tmp_path,5) as (runtime,spec,records,runs):
        store=runtime.store
        other=RunArtifactStore(root=store.root,store_id=store.store_id,journal=store._journal,
                               bounds=replace(store._bounds,max_open_files=10))
        called=[]
        def forbidden(*args,**kwargs):
            called.append(True)
            raise AssertionError('Infeasible shared window read input')
        try:
            assert store.available_file_slots()==8
            with monkeypatch.context() as patch:
                patch.setattr(store,'read',forbidden)
                with pytest.raises(DataflowError,match='input file slots'):
                    next(ordering.merge_sort_runs(runs,spec=spec,services=runtime))
            assert not called and store.open_file_slots==2
        finally:other.close()
