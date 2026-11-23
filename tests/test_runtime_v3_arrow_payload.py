"""Arrow batches remain in the original source/graph/commit/recovery path."""
from dataclasses import replace
import tempfile

import lance
import pyarrow as pa
import pytest

from demiflow import data
from demiflow.execution.arrow_payload import ArrowBatchCache, is_arrow_record, row_upper
from demiflow.execution.contracts.dataflow_errors import DataflowError
from demiflow.execution.run_context import RunContext
from _arrow_payload_fixtures import install_arrow_probe
from _worker_event_fixtures import events

LIMITS = dict(memory_bytes=128*1024**2, max_rss_bytes=2*1024**3,
    max_row_bytes=65536, max_batch_bytes=2*1024**2, batch_rows=8,
    checkpoint_rows=1000, checkpoint_interval_ms=60000, timeout_s=90)


def source_table(n=25):
    schema = pa.schema([pa.field('id', pa.int16(), nullable=False),
        pa.field('items', pa.list_(pa.struct([('text', pa.large_string())])), metadata={b'field': b'kept'}),
        pa.field('bytes', pa.large_binary())], metadata={b'origin': b'kept'})
    return pa.Table.from_pylist([{'id':i,'items':None if i%3==0 else [] if i%3==1 else [{'text':'字\n'+str(i)}],
        'bytes':None if i%2 else b'\x00\xff'} for i in range(n)], schema=schema)


def test_row_upper_bounds_original_typed_encoding_without_row_materialization():
    from demiflow.execution.exchange.dataflow_bridge import _encode_payload
    table = source_table().append_column('float', pa.array([-0.0, 0.0, 1e300, -1e-300, None]*5))
    batch = table.to_batches()[0]
    for i, row in enumerate(table.to_pylist()):
        assert len(_encode_payload(row, max_bytes=65536)) <= row_upper(batch, i) <= 65536
    assert row_upper(pa.record_batch([pa.array([float('nan')])], names=['x']), 0) is None
    import datetime
    assert row_upper(pa.record_batch([pa.array([datetime.date(2026,1,1)])], names=['x']), 0) is None


def test_compressed_ipc_is_refused_before_decoded_array_allocation():
    from demiflow.execution.arrow_payload import _uncompressed_ipc
    from test_runtime_v3_s import make_bounds
    batch = source_table(8).to_batches()[0]
    sink = pa.BufferOutputStream()
    with pa.ipc.new_stream(sink, batch.schema, options=pa.ipc.IpcWriteOptions(compression='zstd')) as writer:
        writer.write_batch(batch)
    with pytest.raises(DataflowError, match='compressed Arrow IPC'):
        _uncompressed_ipc(sink.getvalue(), make_bounds())


def test_actual_lance_reader_restores_inside_shared_batch_and_validates_binding(tmp_path):
    # Component recovery boundary: actual reader/codec/Lance data, a test-domain
    # store only. The public interrupted-run test below proves the native ACK.
    from test_runtime_v3_s import FakeStore, make_bounds, make_snapshot, first_split, make_services, drain
    from demiflow.data.sources import LanceSource
    from demiflow.lance.model import LanceScanSpec
    from demiflow.execution.sources.source_readers import LanceReader
    from demiflow.execution.contracts.dataflow import clone_record
    table = source_table(16)
    uri = str(tmp_path / 'source.lance')
    lance.write_dataset(table, uri)
    store = FakeStore()
    bounds = make_bounds(max_row_bytes=65536, max_batch_bytes=2*1024**2)
    plan = LanceSource(LanceScanSpec(uri, version=1, batch_size=8))
    snapshot = make_snapshot(plan, store, bounds=bounds)
    split = first_split(snapshot, plan, bounds=bounds)
    services = make_services(store, plan, snapshot.source_id, bounds=bounds)
    reader = LanceReader(snapshot, services=services)
    reader.open(split, None)
    first = [reader.next_record().record for _ in range(3)]
    assert all(is_arrow_record(r) for r in first)
    assert len({r.payload_ref for r in first}) == 1
    cursor = reader.snapshot()
    assert cursor.offset['row_offset'] == 3 and cursor.offset['arrow_batch']
    assert cursor.pending_refs and cursor.next_sequence == 3
    reader.close()
    restored = LanceReader(snapshot, services=services)
    restored.open(split, cursor)
    tail = drain(restored)
    restored.close()
    cache = ArrowBatchCache(store, bounds)
    assert [cache.row(r) for r in tail] == table.slice(2).to_pylist()
    assert tail[0].record_id == first[-1].record_id
    assert tail[0].sequence == 2 and tail[1].sequence == 3
    with pytest.raises(DataflowError, match='original source occurrence'):
        cache.row(clone_record(first[0], record_id='b'*64))
    with pytest.raises(DataflowError, match='offset/bytes'):
        cache.row(clone_record(first[0], encoded_bytes=1))


def test_tiny_legal_row_uses_original_inline_when_reference_frame_does_not_fit(tmp_path):
    from test_runtime_v3_s import FakeStore, make_bounds, make_snapshot, first_split, make_services
    from demiflow.data.sources import LanceSource
    from demiflow.lance.model import LanceScanSpec
    from demiflow.execution.sources.source_readers import LanceReader
    from demiflow.execution.dataflow_sinks import _encode_record
    uri = str(tmp_path/'source.lance')
    lance.write_dataset(pa.table({'x':[1,2]}), uri)
    store, bounds = FakeStore(), make_bounds(max_row_bytes=600, max_batch_bytes=2*1024**2)
    plan = LanceSource(LanceScanSpec(uri, version=1, batch_size=2))
    snapshot = make_snapshot(plan, store, bounds=bounds)
    services = make_services(store, plan, snapshot.source_id, bounds=bounds)
    reader = LanceReader(snapshot, services=services)
    reader.open(first_split(snapshot, plan, bounds=bounds), None)
    try:
        records = [reader.next_record().record for _ in range(2)]
        assert [dict(record.inline) for record in records] == [{'x':1},{'x':2}]
        assert all(record.payload_ref is None and len(_encode_record(record,bounds)) <= 600
                   for record in records)
    finally:
        reader.close()


@pytest.mark.parametrize('provider', ['process','thread','ray_core'])
def test_real_multiline_copy_keeps_schema_batch_and_ack(tmp_path, monkeypatch, provider):
    from demiflow.data.api import DataAPI
    from demiflow.execution.seal.sealed_plans import load_sealed_plan
    path = install_arrow_probe(tmp_path, monkeypatch)
    table = source_table()
    source, target = str(tmp_path/'source.lance'), str(tmp_path/'target.lance')
    lance.write_dataset(table, source)
    lance.write_dataset(table, source, mode='append')
    ray = None
    with tempfile.TemporaryDirectory(prefix='demiflow-arrow-ray-', dir='/tmp') as ray_temp:
        if provider == 'ray_core':
            ray = pytest.importorskip('ray')
            assert not ray.is_initialized()
            ray.init(address='local', num_cpus=2, num_gpus=0, include_dashboard=False,
                     object_store_memory=80*1024**2, _temp_dir=ray_temp)
            from demiflow.execution.executors.ray import RayDatasetExecutor
            executor = RayDatasetExecutor(workers=2, resource_root=tmp_path, native_resources={'batch_rows':8})
        else:
            from demiflow.execution.executors.local import LocalDatasetExecutor
            from demiflow.execution.local_kernel import LocalKernelOptions
            executor = LocalDatasetExecutor(workers=2, resource_root=tmp_path,
                local_kernel=LocalKernelOptions(workers=2, worker_mode=provider, batch_rows=8,
                                               memory_bytes=LIMITS['memory_bytes']))
        try:
            api = DataAPI(executor)
            with RunContext.start(tmp_path/'run', limits=LIMITS) as run:
                result = api.read_lance(source, version=1, filter='id >= 2', limit=17, batch_size=8).write_lance(
                    target, mode='create', return_receipt=True)
                assert result.status == 'committed' and result.written_rows == 17
                sealed = load_sealed_plan(run.directory, 'action-1', version='v3')
                assert sealed['physical'].policy.provider == ('ray_core' if ray else 'local')
                assert run.journal.execution_commits('action-1')[-1]['status'] == 'acked'
            assert lance.dataset(target).to_table().equals(table.slice(2,17), check_metadata=True)
            proof = events(path)
            pubs = [e for e in proof if e['event']=='publish']
            assert pubs and sum(e['rows'] for e in pubs) == 17 and max(e['rows'] for e in pubs) > 1
            assert all(e['carried'] for e in pubs)
            assert len({e['digest'] for e in pubs}) == len(pubs)
            assert not any(e['event'] in {'python_decode','python_fallback'} for e in proof)
            assert len([e for e in proof if e['event']=='read_batch']) <= 4*len(pubs)
            with run.journal._connection() as db:
                assert db.execute("SELECT COUNT(*) FROM resource_allocations WHERE cleanup!='CONFIRMED'").fetchone()[0] == 0
        finally:
            executor.close()
            if ray is not None:
                ray.shutdown()


def test_mixed_sources_materialize_udf_and_early_close_keep_original_rows(tmp_path, monkeypatch):
    path = install_arrow_probe(tmp_path, monkeypatch, forbid_rows=False)
    source = str(tmp_path/'source.lance')
    table = source_table(16)
    lance.write_dataset(table, source)
    with RunContext.start(tmp_path/'run', limits=LIMITS) as run, data.local_execution(workers=2, batch_rows=8):
        ds = data.read_lance(source, version=1, batch_size=8)
        result = ds.union(data.from_items(table.slice(0,2).to_pylist())).materialize()
        assert result.take_all() == table.to_pylist() + table.slice(0,2).to_pylist()
        assert ds.select_columns(['id']).rename_columns({'id':'n'}).map(lambda row: {'n':row['n']+1}).take_all() == [dict(n=i+1) for i in range(16)]
        iterator = ds.iter_rows()
        assert next(iterator) == table.to_pylist()[0]
        iterator.close()
        result.release()
    assert any(e['event']=='python_decode' for e in events(path))
    with run.journal._connection() as db:
        assert db.execute("SELECT COUNT(*) FROM resource_allocations WHERE cleanup!='CONFIRMED'").fetchone()[0] == 0


def test_medium_binary_payload_keeps_one_artifact_per_real_batch(tmp_path, monkeypatch):
    proof = install_arrow_probe(tmp_path, monkeypatch)
    source, target = str(tmp_path/'input.lance'), str(tmp_path/'output.lance')
    table = pa.table({'id':list(range(16)), 'value':[b'x'*2000]*16})
    lance.write_dataset(table, source)
    with RunContext.start(tmp_path/'run', limits=LIMITS), data.local_execution(workers=2, batch_rows=8):
        data.read_lance(source, version=1, batch_size=8).write_lance(target, mode='create')
    assert lance.dataset(target).to_table().equals(table)
    observed = events(proof)
    pubs = [e for e in observed if e['event']=='publish']
    assert len(pubs)==2 and [e['rows'] for e in pubs]==[8,8]
    assert all(e['carried'] for e in pubs) and len({e['digest'] for e in pubs})==2
    assert not any(e['event'] in {'python_decode','python_fallback'} for e in observed)
    assert sum(e['event']=='read_batch' for e in observed) <= 4*len(pubs)


def test_native_copy_through_limit_preserves_batch_reference(tmp_path, monkeypatch):
    proof = install_arrow_probe(tmp_path, monkeypatch)
    source, target = str(tmp_path/'input.lance'), str(tmp_path/'output.lance')
    table = source_table(24)
    lance.write_dataset(table, source)
    with RunContext.start(tmp_path/'run', limits=LIMITS), data.local_execution(workers=2, batch_rows=8):
        data.read_lance(source, version=1, batch_size=8).limit(11).write_lance(target, mode='create')
    assert lance.dataset(target).to_table().equals(table.slice(0,11), check_metadata=True)
    observed = events(proof)
    assert any(e['event']=='publish' and e['rows']>1 and e['carried'] for e in observed)
    assert not any(e['event'] in {'python_decode','python_fallback'} for e in observed)


def test_original_gc_keeps_shared_batch_reachable_from_real_materialized_result(tmp_path, monkeypatch):
    from demiflow.execution.action_driver.dataflow import DataflowExecution
    from demiflow.execution.storage.gc import collect_published_orphans
    proof = install_arrow_probe(tmp_path, monkeypatch, forbid_rows=False)
    source = str(tmp_path/'input.lance')
    table = source_table(16)
    lance.write_dataset(table, source)
    original, captured = DataflowExecution.prepare, []
    def prepare(execution):
        value = original(execution)
        if not captured:
            orphan = execution._store.put(iter([b'unreachable-gc-evidence']), max_bytes=64,
                                           media_type='application/octet-stream')
            captured.append((execution, orphan))
        return value
    monkeypatch.setattr(DataflowExecution, 'prepare', prepare)
    with RunContext.start(tmp_path/'run', limits=LIMITS) as run, data.local_execution(batch_rows=8):
        materialized = data.read_lance(source, version=1, batch_size=8).materialize()
        try:
            execution, orphan = captured[0]
            digests = {e['digest'] for e in events(proof) if e['event']=='publish' and e['carried']}
            assert digests
            commits = run.journal.execution_commits('action-1')
            result = collect_published_orphans(execution._action, bounds=execution._bounds)
            assert result['blocked'] is None
            assert not execution._store.inspect(orphan)
            from demiflow.execution.storage.gc import _Trace
            reachable = _Trace(run.journal, execution._bounds).walk()
            assert all((execution._store.store_id, digest) in reachable for digest in digests)
            assert materialized.take_all() == table.to_pylist()
            assert run.journal.execution_commits('action-1') == commits
        finally:
            materialized.release()


@pytest.mark.parametrize('kind', ['sort', 'join', 'union', 'udf', 'split'])
def test_nonsequential_graphs_use_original_rows_without_batch_rereads(tmp_path, monkeypatch, kind):
    path = install_arrow_probe(tmp_path, monkeypatch, forbid_rows=False)
    uri = str(tmp_path/'input.lance')
    rows = [{'id':i, 'value':str(i)} for i in range(24)]
    lance.write_dataset(pa.Table.from_pylist(rows), uri)
    if kind == 'split':
        lance.write_dataset(pa.Table.from_pylist(rows), uri, mode='append')
    with RunContext.start(tmp_path/'run', limits=LIMITS), data.local_execution(workers=2, partitions=2, batch_rows=8):
        ds = data.read_lance(uri, batch_size=8)
        if kind == 'sort':
            result, expected = ds.sort('id', descending=True), rows[::-1]
        elif kind == 'join':
            result = ds.join(data.from_items([{'id':i, 'extra':i*2} for i in range(24)]), on='id')
            expected = [dict(row, extra=row['id']*2) for row in rows]
        elif kind == 'union':
            result, expected = ds.union(data.from_items(rows[:3])), rows + rows[:3]
        elif kind == 'udf':
            result = ds.map(lambda row: dict(row, value=row['value']+'!'))
            expected = [dict(row, value=row['value']+'!') for row in rows]
        else:
            result, expected = ds, rows+rows
        actual = result.take_all()
        if kind in ('join', 'split'):
            actual = sorted(actual, key=lambda row: (row['id'], row['value']))
            expected = sorted(expected, key=lambda row: (row['id'], row['value']))
        assert actual == expected
    proof = events(path)
    if kind == 'join':
        # A co-located fixed Lance probe now retains the original source batches.
        # Verify the intended safety boundary: each parent is loaded once, with
        # no interleaved rereads. Other nonsequential graphs still use rows.
        published = [e for e in proof if e['event']=='publish' and e['carried']]
        loaded = [e for e in proof if e['event']=='read_batch']
        assert [e['rows'] for e in published] == [8, 8, 8]
        assert [e['digest'] for e in loaded] == [e['digest'] for e in published]
        assert len([e for e in proof if e['event']=='python_decode']) == len(rows)
        assert not any(e['event']=='python_fallback' for e in proof)
    else:
        assert any(e['event']=='python_fallback' for e in proof)
        assert not any(e['event'] in {'publish','read_batch','python_decode'} for e in proof)


def test_wide_parent_buffers_use_original_rows_within_narrow_window(tmp_path, monkeypatch):
    path = install_arrow_probe(tmp_path, monkeypatch, forbid_rows=False)
    uri = str(tmp_path/'input.lance')
    rows = [{'id':i, 'payload':'z'*12000} for i in range(9)]
    lance.write_dataset(pa.Table.from_pylist(rows), uri)
    limits = {**LIMITS, 'max_row_bytes':65536, 'max_batch_bytes':131072}
    from demiflow.execution import batch_conversion
    admission = batch_conversion.admit_batch_conversion
    conversions = []
    def checked(batch, fmt, **kwargs):
        result = admission(batch, fmt, **kwargs)
        conversions.append((len(batch), result, kwargs))
        # The exact same writer input must still be rejected before its native
        # constructor when its real coexistence workspace is unavailable.
        narrow = {**kwargs, 'bounds':replace(kwargs['bounds'], max_resident_bytes=10000)}
        with pytest.raises(DataflowError, match='batch conversion admission'):
            admission(batch, fmt, **narrow)
        return result
    monkeypatch.setattr(batch_conversion, 'admit_batch_conversion', checked)
    larger = str(tmp_path/'batch8.lance')
    with RunContext.start(tmp_path/'batch8-run', limits=limits), data.local_execution(batch_rows=8):
        data.read_lance(uri, version=1, batch_size=8).write_lance(larger, mode='create')
    assert lance.dataset(larger).to_table().to_pylist() == rows
    assert sum(n for n, _, _ in conversions) == len(rows)
    assert all(0 < n < 8 and used <= maximum for n, (used, maximum), _ in conversions)
    assert all(k['resident_workspace'] and k['writer_copies']==2 for _, _, k in conversions)
    target = str(tmp_path/'output.lance')
    with RunContext.start(tmp_path/'run', limits=limits), data.local_execution(
            batch_rows=1, native_resources={'batch_rows':1}):
        data.read_lance(uri, version=1, batch_size=8).write_lance(target, mode='create')
    assert lance.dataset(target).to_table().to_pylist() == rows
    proof = events(path)
    assert any(e['event']=='publish' and not e['carried'] for e in proof)
    assert any(e['event']=='python_fallback' for e in proof)
    assert not any(e['event']=='publish' and e['carried'] for e in proof)


@pytest.mark.parametrize('kind', ['estimate', 'unsupported'])
def test_conservative_estimate_and_unsupported_type_fallback_do_not_reject_valid_rows(tmp_path, monkeypatch, kind):
    import datetime
    path = install_arrow_probe(tmp_path, monkeypatch, forbid_rows=False)
    uri = str(tmp_path/'input.lance')
    rows = [{'text':'汉'*250}] if kind=='estimate' else [{'when':datetime.date(2026,1,1)}]
    table = pa.Table.from_pylist(rows)
    lance.write_dataset(table, uri)
    limits = {**LIMITS, 'max_row_bytes':2048}
    upper = row_upper(table.to_batches()[0], 0)
    assert upper is None if kind=='unsupported' else upper > limits['max_row_bytes']
    target = str(tmp_path/'output.lance')
    with RunContext.start(tmp_path/'run', limits=limits), data.local_execution(batch_rows=8):
        data.read_lance(uri, version=1).write_lance(target, mode='create')
    assert lance.dataset(target).to_table().to_pylist() == rows
    assert any(e['event']=='publish' and not e['carried'] for e in events(path))
    assert any(e['event']=='python_fallback' for e in events(path))


def test_slow_consumer_and_early_close_release_original_grants(tmp_path, monkeypatch):
    import time
    path = install_arrow_probe(tmp_path, monkeypatch, forbid_rows=False)
    table = source_table(40)
    uri = str(tmp_path/'input.lance')
    lance.write_dataset(table, uri)
    with RunContext.start(tmp_path/'run', limits=LIMITS) as run, data.local_execution(batch_rows=8, workers=2):
        iterator = data.read_lance(uri, version=1, batch_size=8).iter_rows()
        got=[]
        try:
            for row in iterator:
                got.append(row)
                time.sleep(.005)
                if len(got)==9:
                    break
        finally:
            iterator.close()
        assert got == table.slice(0,9).to_pylist()
    proof=events(path)
    assert any(e['event']=='publish' and e['rows']>1 for e in proof)
    assert sum(e['event']=='read_batch' for e in proof) <= 2*sum(e['event']=='publish' for e in proof)
    with run.journal._connection() as db:
        assert db.execute("SELECT COUNT(*) FROM resource_allocations WHERE cleanup!='CONFIRMED'").fetchone()[0] == 0


def test_real_committed_cut_inside_arrow_batch_resumes_once(tmp_path, monkeypatch):
    from demiflow.execution.checkpoint.coordinator import DataflowCheckpointCoordinator
    from demiflow.execution.contracts.dataflow import ArtifactRef
    from demiflow.execution.storage.state_artifacts import StateArtifacts
    class Interrupted(Exception):
        pass
    proof = install_arrow_probe(tmp_path, monkeypatch)
    # Slow the real reader only; no fake graph, source, cut or checkpoint ACK.
    probe = tmp_path/'arrow-probe'/'sitecustomize.py'
    with probe.open('a') as stream:
        stream.write("\nfrom demiflow.execution.source_readers import LanceReader\nimport time\n_original_next=LanceReader.next_record\ndef _slow_next(self):\n    time.sleep(.02)\n    return _original_next(self)\nLanceReader.next_record=_slow_next\n")
    table = source_table(64)
    uri, target = str(tmp_path/'input.lance'), str(tmp_path/'output.lance')
    lance.write_dataset(table, uri)
    original = DataflowCheckpointCoordinator.commit
    boundary = {}
    def stop(coordinator, *args, **kwargs):
        image = original(coordinator, *args, **kwargs)
        if image.cut.final:
            return image
        for cursor in image.source_vector:
            saved = cursor.offset.get('arrow_batch')
            if saved is None:
                continue
            cache = ArrowBatchCache(coordinator._store, coordinator._physical.bounds)
            batch, header = cache.load(ArtifactRef(**saved))
            if header['start'] < cursor.offset['row_offset'] < header['start']+header['count']:
                boundary.update(commit=image.commit_id, offset=cursor.offset['row_offset'])
                raise Interrupted('real ACK within an Arrow batch')
        return image
    limits = {**LIMITS, 'checkpoint_rows':3}
    with pytest.raises(Interrupted, match='within an Arrow batch'):
        with RunContext.start(tmp_path/'run', limits=limits) as run, data.local_execution(batch_rows=8, workers=2):
            ref = run.ref
            with monkeypatch.context() as patch:
                patch.setattr(DataflowCheckpointCoordinator, 'commit', stop)
                data.read_lance(uri, version=1, batch_size=8).write_lance(target, mode='create', return_receipt=True)
    assert boundary and not (tmp_path/'output.lance'/'_versions').exists()
    before=run.journal.execution_commits('action-1')
    assert before[-1]['commit_id']==boundary['commit'] and before[-1]['status']=='acked'
    lance.write_dataset(table, uri, mode='append')  # history must stay on v1
    with RunContext.resume(ref) as resumed, data.local_execution(batch_rows=8, workers=2):
        receipt=data.read_lance(uri, version=1, batch_size=8).write_lance(target, mode='create', return_receipt=True)
        assert receipt.committed_version==1 and receipt.written_rows==64
        after=resumed.journal.execution_commits('action-1')
        assert after[:len(before)]==before and after[-1]['status']=='acked'
        final=StateArtifacts(resumed).read(after[-1]['prepared']['state']['dataflow-cut-v3'])
        assert final['cut']['final']
    assert lance.dataset(target).to_table().equals(table, check_metadata=True)
    assert all(e['event'] not in {'python_fallback','python_decode'} for e in events(proof))


def test_real_store_sha_failure_precedes_native_publication(tmp_path, monkeypatch):
    import json
    from demiflow.execution import dataflow_sinks
    from demiflow.execution.storage.segments import block_chunks
    from demiflow.execution.dataflow_handles import _ChunkStream
    from demiflow.execution.exchange.dataflow_bridge import _decode_record_document
    table = source_table(8)
    uri, target = str(tmp_path/'input.lance'), str(tmp_path/'output.lance')
    lance.write_dataset(table, uri)
    original = dataflow_sinks.publication_blocks
    tampered = []

    def corrupt(manifest, *, store, bounds, **options):
        data_ref = dataflow_sinks._artifact_from_json(manifest['partitions'][0]['data_ref'])
        stream = _ChunkStream(block_chunks(store, data_ref, bounds))
        item = json.loads(stream.readline(bounds.max_row_bytes + 1))
        record=_decode_record_document(item['document'].encode(), record_id=item['record_id'],
            sequence=item['sequence'], max_bytes=bounds.max_row_bytes)
        assert is_arrow_record(record)
        path = store._object_path(record.payload_ref)
        with path.open('r+b') as file:
            file.seek(-1,2)
            original_byte=file.read(1)
            file.seek(-1,2)
            file.write(bytes([original_byte[0]^1]))
        tampered.append(record.payload_ref.digest)
        yield from original(manifest, store=store, bounds=bounds, **options)

    monkeypatch.setattr(dataflow_sinks, 'publication_blocks', corrupt)
    with pytest.raises(DataflowError, match='artifact bytes do not match'):
        with RunContext.start(tmp_path/'run', limits=LIMITS) as run, data.local_execution(batch_rows=8, workers=2):
            data.read_lance(uri, version=1, batch_size=8).write_lance(target, mode='create')
    assert len(tampered)==1 and not (tmp_path/'output.lance'/'_versions').exists()
    assert run.journal.execution_action('action-1')['result'] is None
