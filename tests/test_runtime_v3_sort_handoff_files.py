"""Final sort keeps the original consumer write window while readers are live."""
from contextlib import contextmanager
from dataclasses import replace
import hashlib
import math

import pytest

from demiflow import data
from demiflow.execution.contracts.dataflow import Record
from demiflow.execution.contracts.dataflow_errors import DataflowError
from demiflow.execution.contracts.record_codec import _encode_payload, _encode_record_document, _decode_record_document
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
    from demiflow.execution.storage.objects import RunArtifactStore
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
    from demiflow.execution.storage.objects import RunArtifactStore
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


def test_sort_accounts_for_initial_runs_and_body_during_intermediate_write(tmp_path, monkeypatch):
    """Original runs belong to the caller and coexist with each new output."""
    with sorted_runs(tmp_path, 8) as (runtime, spec, _, runs):
        body = 75_000
        initial = sum(ref.size_bytes for ref in runs)
        first_output = sum(ref.size_bytes for ref in runs[:2])
        narrow = replace(runtime, bounds=replace(runtime.bounds, max_open_files=10,
            max_spill_bytes=body + initial + first_output - 1))
        with monkeypatch.context() as guard:
            def no_write(*args, **kwargs):
                pytest.fail('sort opened a spill writer before accounting for its live inputs')
            guard.setattr(runtime.store, 'put', no_write)
            with pytest.raises(DataflowError, match='merge spill'):
                list(ordering.merge_sort_runs(runs, spec=spec, services=narrow, body_bytes=body))
        assert all(runtime.store.inspect(ref) for ref in runs)
        assert runtime.store.open_file_slots == 1


@pytest.mark.parametrize('end', ['cancel', 'corrupt', 'close'])
def test_wide_sort_releases_its_extent_and_readers_on_every_exit(tmp_path, monkeypatch, end):
    from demiflow.execution.storage.scratch import KernelScratch
    from test_runtime_v3_artifact_accounting import store_fixture
    from test_runtime_v3_c import make
    from test_runtime_v3_finite_borrow import fixture, record

    with store_fixture(tmp_path, ceiling=16 << 20) as (_, store, _):
        services, _ = fixture(store)
        services.bounds = replace(services.bounds, max_resident_bytes=512 << 10,
                                  max_spill_bytes=8 << 20)
        _, metadata, window = ordering.sort_windows(services)
        extents = []
        retained = []
        append = KernelScratch.append
        read_at = KernelScratch.read_at

        def measured(extent, raw):
            extents.append(extent)
            return append(extent, raw)

        def stop_after_body():
            if end == 'cancel' and extents:
                raise DataflowError('CANCELLED', 'cancel during sort')

        def read_body(extent, offset, length):
            raw = read_at(extent, offset, length)
            return bytes([raw[0] ^ 1]) + raw[1:] if end == 'corrupt' else raw

        monkeypatch.setattr(KernelScratch, 'append', measured)
        monkeypatch.setattr(KernelScratch, 'read_at', read_body)
        services.check_stop = stop_after_body
        rows = 24
        records = (record({'k': i % 5, 'body': b'v' * 27_000}, i) for i in range(rows))
        spec = make('KernelSpec', kind='SortOp', configuration={'keys': ['k']})
        result = ordering.sort_with_body_references(records, spec=spec, services=services,
            rows=rows, window=window, retain=retained.append, metadata_bytes=metadata)
        if end == 'close':
            assert next(result).inline['k'] == 0
            result.close()
        else:
            with pytest.raises(DataflowError) as failure:
                next(result)
            assert failure.value.code == ('CANCELLED' if end == 'cancel' else 'CORRUPT_ARTIFACT')
        assert len(extents) == (1 if end == 'cancel' else rows)
        assert len({id(extent) for extent in extents}) == 1
        assert extents[0].fd is None and not extents[0].path.exists()
        assert not retained or retained[-1] == 0
        assert store.open_file_slots == 1


def test_sort_reuses_published_payload_reference_and_falls_back_with_narrow_file_window(tmp_path, monkeypatch):
    from demiflow.execution.storage.scratch import KernelScratch
    from test_runtime_v3_artifact_accounting import store_fixture
    from test_runtime_v3_c import make
    from test_runtime_v3_finite_borrow import fixture, record

    with store_fixture(tmp_path, ceiling=32 << 20) as (_, store, _):
        services, _ = fixture(store)
        services.bounds = replace(services.bounds, max_resident_bytes=2 << 20,
                                  max_row_bytes=131072, max_spill_bytes=20 << 20)
        rows = 40
        inline = []
        for i in range(rows):
            row = {'k': i % 3, 'body': b'w' * 52_000}
            raw = _encode_payload(row, max_bytes=131072)
            inline.append(replace(record({'k': i % 3}, i), inline=row, encoded_bytes=len(raw)))
        spec = make('KernelSpec', kind='SortOp', configuration={'keys': ['k']})
        raw = _encode_payload(dict(inline[0].inline), max_bytes=131072)
        payload = store.put(iter([raw]), max_bytes=len(raw), media_type='application/octet-stream')
        referenced = [replace(row, inline=None, payload_ref=payload, encoded_bytes=len(raw)) for row in inline]

        def no_copy(*args, **kwargs):
            pytest.fail('published bodies and narrow file budgets must not allocate temporary extents')
        monkeypatch.setattr(KernelScratch, '__enter__', no_copy)
        for records, slots in [(referenced, 256), (inline, 10)]:
            services.bounds = replace(services.bounds, max_open_files=slots)
            _, metadata, window = ordering.sort_windows(services)
            result = list(ordering.sort_with_body_references(iter(records), spec=spec, services=services,
                rows=rows, window=window, retain=lambda charge: None, metadata_bytes=metadata))
            if records is referenced:
                assert [rec.payload_ref for rec in result] == [payload] * rows
                assert [rec.sequence for rec in result] == list(range(rows))
                assert b''.join(store.read(payload, max_bytes=len(raw))) == raw
            else:
                assert [r.sequence for r in result] == sorted(range(rows), key=lambda i: i % 3)
                assert all(rec.inline['body'] == b'w' * 52_000 for rec in result)
            assert store.open_file_slots == 1
