"""Real artifact descriptors obey the original store's open-file bound."""
from dataclasses import replace
import hashlib
import json
import os
from pathlib import Path
from types import SimpleNamespace

import pytest

from demiflow import data
from demiflow.execution.contracts.dataflow import Record
from demiflow.execution.contracts.dataflow_errors import DataflowError
from demiflow.execution.dataflow_store import RunArtifactStore
from demiflow.execution.exchange.dataflow_bridge import _encode_payload, _encode_record_document, _decode_record_document
from demiflow.execution.kernels.dataflow import _materialize_row
from test_runtime_v3_artifact_accounting import store_fixture,consumed
from test_runtime_v3_published_gc import put
from test_runtime_v3_real_seams import prepared,reader_services


def descriptors(store):
    count=0
    for path in Path('/proc/self/fd').iterdir():
        try:target=os.readlink(path)
        except OSError:continue
        count+=str(store.root) in target
    return count


def bounded_view(store,port,maximum):
    store.close()
    return RunArtifactStore(root=store.root,store_id=store.store_id,journal=port,
                            bounds=replace(port._bounds,max_open_files=maximum))


def test_low_budget_rejects_read_write_scratch_before_any_new_data_file(tmp_path):
    with store_fixture(tmp_path) as (run,original,port):
        ref=put(original,b'preserve')
        store=bounded_view(original,port,2)
        before=consumed(run)
        try:
            for operation in (lambda:list(store.read(ref,max_bytes=100)),
                    lambda:put(store,b'new'),lambda:store.scratch(max_bytes=100,max_io_bytes=100).__enter__()):
                with pytest.raises(DataflowError,match='open file budget'):
                    operation()
                assert store.open_file_slots==1 and descriptors(store)==1
            assert consumed(run)==before and not list(store._staging.iterdir())
        finally:store.close()
        assert store.open_file_slots==0


def test_scratch_and_live_read_are_counted_and_survive_writer_close(tmp_path):
    with store_fixture(tmp_path) as (run,original,port):
        ref=put(original,b'x'*100000)
        store=bounded_view(original,port,6)
        with store.scratch(max_bytes=1000,max_io_bytes=1000) as scratch:
            scratch.append(b'owned')
            reader=store.read(ref,max_bytes=200000)
            assert next(reader)
            assert store.open_file_slots==6 and descriptors(store)==5
            with pytest.raises(DataflowError,match='open file budget'):
                next(store.read(ref,max_bytes=200000))
            store.close()
            assert store.open_file_slots==5 and descriptors(store)==4
            reader.close()
            assert store.open_file_slots==3
        assert store.open_file_slots==0 and descriptors(store)==0


def test_views_and_native_owner_share_real_namespace_budget(tmp_path):
    with store_fixture(tmp_path) as (run,original,port):
        ref=put(original,b'shared-root')
        store=bounded_view(original,port,3)
        native=store.native_owner_lease()
        store.close()
        assert store.open_file_slots==1
        view=RunArtifactStore(root=store.root,store_id=store.store_id,journal=port,bounds=store._bounds)
        try:
            assert view.open_file_slots==2
            with pytest.raises(DataflowError,match='open file budget'):
                next(view.read(ref,max_bytes=100))
            native.close()
            assert b''.join(view.read(ref,max_bytes=100))==b'shared-root'
            assert view.open_file_slots==1
        finally:
            native.close();view.close()
        assert descriptors(store)==0 and view.open_file_slots==0


def test_exception_and_failed_scratch_open_return_reserved_file_windows(tmp_path,monkeypatch):
    with store_fixture(tmp_path) as (run,original,port):
        store=bounded_view(original,port,6)
        def broken():
            yield b'partial'
            raise RuntimeError('business iterator failed')
        try:
            with pytest.raises(RuntimeError,match='business iterator'):
                store.put(broken(),max_bytes=100,media_type='application/octet-stream')
            assert store.open_file_slots==1 and consumed(run)==0
            original_open=os.open
            def fail_pending(path,*args,**kwargs):
                if Path(path).name.startswith('.pending-'):raise OSError('injected data open failure')
                return original_open(path,*args,**kwargs)
            with monkeypatch.context() as patch:
                patch.setattr(os,'open',fail_pending)
                with pytest.raises(OSError,match='data open failure'):
                    with store.scratch(max_bytes=100,max_io_bytes=100):pass
            assert store.open_file_slots==1 and consumed(run)==0
            assert put(store,b'next').size_bytes==4
        finally:store.close()
        assert descriptors(store)==0


def test_live_reader_releases_window_on_corruption_and_generator_end(tmp_path):
    with store_fixture(tmp_path) as (run,original,port):
        ref=put(original,b'original')
        store=bounded_view(original,port,3)
        try:
            assert b''.join(store.read(ref,max_bytes=100))==b'original'
            assert store.open_file_slots==1
            store._object_path(ref).write_bytes(b'altered!')
            with pytest.raises(DataflowError,match='bytes do not match'):
                list(store.read(ref,max_bytes=100))
            assert store.open_file_slots==1 and descriptors(store)==1
        finally:store.close()


def test_real_external_sort_fan_in_accounts_for_read_leases_and_output(tmp_path):
    from demiflow.execution.kernels.ordering import build_sort_runs,merge_sort_runs
    rows=[{'x':i,'typed':(-0.0,b'x')} for i in range(5,0,-1)]
    with prepared(tmp_path,data.from_items(rows).sort('x')) as (execution,services):
        runtime=reader_services(execution,services)
        spec=next(n.kernel for n in execution._graph.nodes if n.kernel.kind=='SortOp')
        records=[Record(hashlib.sha256(str(i).encode()).hexdigest(),{'index':i},None,row,len(_encode_payload(row,max_bytes=65536)),i) for i,row in enumerate(rows)]
        runs=tuple(ref for record in records for ref in build_sort_runs(iter([record]),spec=spec,services=runtime))
        store=runtime.store
        old=store._bounds
        try:
            store._bounds=replace(old,max_open_files=9)
            narrow=replace(runtime,bounds=store._bounds)
            before=consumed(services.action.context)
            with pytest.raises(DataflowError,match='two input file slots'):
                list(merge_sort_runs(runs,spec=spec,services=narrow))
            assert consumed(services.action.context)==before and store.open_file_slots==1
            store._bounds=replace(old,max_open_files=10)
            ordered=list(merge_sort_runs(runs,spec=spec,services=replace(runtime,bounds=store._bounds)))
            assert [_materialize_row(record,runtime) for record in ordered]==list(reversed(rows))
            assert store.open_file_slots==1 and descriptors(store)==1
        finally:store._bounds=old


def test_join_admits_stream_and_external_payload_before_consuming_input(tmp_path):
    from demiflow.execution.kernels.join_spool import join_pairs
    with store_fixture(tmp_path) as (run,original,port):
        row={'k':1,'wide':'x'*1000}
        raw=_encode_payload(row,max_bytes=65536)
        payload=put(original,raw,'application/vnd.demiflow.record.v3')
        record=Record('a'*64,{'index':0},payload,None,len(raw),0)
        document=put(original,_encode_record_document(record,max_bytes=65536)+b'\n')
        right=Record('b'*64,{'index':0},None,{'k':1,'r':2},len(_encode_payload({'k':1,'r':2},max_bytes=65536)),0)
        store=bounded_view(original,port,7)
        runtime=SimpleNamespace(store=store,bounds=store._bounds,assignment=SimpleNamespace(stages=()),check_stop=lambda:None)
        touched=[]
        def source():
            touched.append(True)
            for chunk in store.read(document,max_bytes=65536):
                yield _decode_record_document(chunk.rstrip(b'\n'),record_id='a'*64,sequence=0,max_bytes=65536)
        try:
            with pytest.raises(DataflowError,match='open file budget'):
                list(join_pairs(source(),iter([right]),left_keys=('k',),right_keys=('k',),how='inner',services=runtime))
            assert not touched and store.open_file_slots==1
            store=bounded_view(store,port,8);runtime.store=store;runtime.bounds=store._bounds
            pairs=list(join_pairs(source(),iter([right]),left_keys=('k',),right_keys=('k',),how='inner',services=runtime))
            assert len(pairs)==1 and _materialize_row(pairs[0][0],runtime)==row
            assert _materialize_row(pairs[0][1],runtime)=={'k':1,'r':2}
            assert store.open_file_slots==1 and descriptors(store)==1
        finally:store.close()


def test_higher_budget_view_cannot_bypass_live_low_budget_reader(tmp_path):
    with store_fixture(tmp_path) as (run,original,port):
        ref=put(original,b'live-reader')
        store=bounded_view(original,port,3)
        reader=store.read(ref,max_bytes=100)
        assert next(reader)==b'live-reader'
        try:
            with pytest.raises(DataflowError,match='open file budget'):
                RunArtifactStore(root=store.root,store_id=store.store_id,journal=None,
                                 bounds=port._bounds,readonly=True)
            assert store.open_file_slots==3 and descriptors(store)==3
        finally:reader.close();store.close()
        assert store.open_file_slots==0 and descriptors(store)==0
