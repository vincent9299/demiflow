"""Real block layout, identity-compatible sidecars, and append-only snapshots."""
from dataclasses import asdict, replace
import hashlib
import json

import pytest

from demiflow import data
from demiflow.execution.contracts.dataflow import BlockPartition, Record
from demiflow.execution.contracts.dataflow_codec import encode_wire, decode_wire, _identity_project
from demiflow.execution.contracts.dataflow_errors import DataflowError
from demiflow.execution.contracts.record_codec import _encode_record_document, _decode_record_document, _encode_payload
from demiflow.execution.storage.segments import SegmentForest, segment_refs, block_chunks, RECORDS_MEDIA
from demiflow.execution.pipeline.run_context import RunContext
from test_runtime_v3_c import bounds
from test_runtime_v3_k import FakeStore


def record(i):
    row = {'n': i}
    return Record(hashlib.sha256(str(i).encode()).hexdigest(), {'source': 'original', 'ordinal': i},
                  None, row, len(_encode_payload(row, max_bytes=4096)), i)


def test_partition_sidecar_keeps_old_wire_and_source_identity():
    b = bounds()
    original = record(1)
    raw = encode_wire(original, type_name='Record', bounds=b)
    assert 'block_partition' not in json.loads(raw)['payload']
    assert 'block_partition' not in _identity_project(original)
    assert encode_wire(decode_wire(raw, type_name='Record', bounds=b), type_name='Record', bounds=b) == raw
    tagged = replace(original, block_partition=BlockPartition('a' * 64, 1, 3))
    for decoded in (decode_wire(encode_wire(tagged, type_name='Record', bounds=b), type_name='Record', bounds=b),
                    _decode_record_document(_encode_record_document(tagged, max_bytes=4096),
                        record_id=tagged.record_id, sequence=tagged.sequence, max_bytes=4096)):
        assert decoded == tagged
        assert decoded.source_position == original.source_position
        assert decoded.record_id == original.record_id
    with pytest.raises(DataflowError):
        BlockPartition('a' * 64, 3, 3)


def test_forest_appends_without_rewriting_prior_prefix_and_restores():
    store = FakeStore()
    b = replace(bounds(), max_control_bytes=65536, max_depth=64, max_output_rows=1000)
    forest = SegmentForest(store, b)
    total = b''
    for i in range(65):
        raw = str(i).encode() + b'\n'
        ref = store.put(iter([raw]), max_bytes=len(raw), media_type=RECORDS_MEDIA)
        forest.append(ref, rows=1)
        total += raw
        root = forest.seal()
        assert b''.join(block_chunks(store, root, b)) == total
        assert len(forest.roots) <= 7
        if i == 31:
            restored = SegmentForest(store, b)
            restored.restore(root, rows=32, size=len(total))
            forest = restored
    leaves = tuple(segment_refs(store, root, b))
    assert len(leaves) == 65
    assert sum(ref.size_bytes for ref in leaves) == len(total)
    assert len(store.objects) <= 65 * 3



@pytest.mark.parametrize('rows,blocks', [(7, 3), (2, 5), (0, 4)])
def test_real_materialize_has_requested_blocks_and_preserves_rows(tmp_path, rows, blocks):
    source = [{'n': i} for i in range(rows)]
    with RunContext.start(tmp_path/'run'), data.local_execution(workers=1, memory_bytes=32 << 20):
        result = data.from_items(source).repartition(blocks).map(lambda r: {'n': r['n'] + 1}).materialize()
        try:
            assert result.num_blocks() == blocks
            assert result.take_all() == [{'n': i + 1} for i in range(rows)]
        finally:
            result.release()


def test_real_target_rows_and_shuffle_partition_setting(tmp_path):
    source = [{'n': i} for i in range(9)]
    with RunContext.start(tmp_path/'run'), data.local_execution(workers=1, partitions=4, memory_bytes=32 << 20):
        target = data.from_items(source).repartition(target_num_rows_per_block=2).materialize()
        shuffled = data.from_items(source).random_shuffle(seed=17).materialize()
        try:
            assert target.num_blocks() == 5
            assert target.take_all() == source
            assert shuffled.num_blocks() == 4
            assert sorted(shuffled.take_all(), key=lambda r: r['n']) == source
        finally:
            target.release()
            shuffled.release()


def test_repartition_count_is_admitted_before_worker_launch(tmp_path, monkeypatch):
    from demiflow.execution.action_driver.dataflow import DataflowExecution
    launched = []
    monkeypatch.setattr(DataflowExecution, '_prepare_launch', lambda *args: launched.append(args))
    with pytest.raises(DataflowError, match='block count'):
        with RunContext.start(tmp_path/'run'):
            data.range(1).repartition(4097).materialize()
    assert launched == []


def _block_records(saved):
    from demiflow.execution.storage.streams import _ChunkStream
    handle = saved._source.handle
    for block in handle._blocks:
        stream = _ChunkStream(block_chunks(handle._store, block, handle._store._bounds))
        records = []
        while line := stream.readline(handle._store._bounds.max_row_bytes + 1):
            item = json.loads(line)
            records.append(_decode_record_document(item['document'].encode(), record_id=item['record_id'],
                sequence=item['sequence'], max_bytes=handle._store._bounds.max_row_bytes))
        yield records


def test_real_hash_blocks_colocate_keys_sort_and_keep_multiplicity(tmp_path):
    source = [{'key': i % 4, 'n': i} for i in range(12)]
    with RunContext.start(tmp_path/'run'), data.local_execution(workers=2, memory_bytes=32 << 20):
        saved = data.from_items(source).repartition(3, shuffle=True, keys=['key'], sort=True).materialize()
        try:
            seen = {}
            flat = []
            assert saved.num_blocks() == 3
            for index, records in enumerate(_block_records(saved)):
                keys = [r.inline['key'] for r in records]
                assert keys == sorted(keys)
                for record in records:
                    assert record.block_partition.index == index
                    key = record.inline['key']
                    assert key not in seen or seen[key] == index
                    seen[key] = index
                    assert 'ordinal' not in record.source_position or record.source_position['ordinal'] == record.inline['n']
                    flat.append(dict(record.inline))
            assert sorted(flat, key=lambda r: r['n']) == source
        finally:
            saved.release()


def test_batch_flushes_at_real_block_boundary_and_scalar_is_global(tmp_path):
    def batch(table):
        rows = table.to_pylist()
        return [{'n': row['n'], 'batch': len(rows)} for row in rows]
    with RunContext.start(tmp_path/'run'), data.local_execution(workers=2, memory_bytes=32 << 20):
        saved = data.from_items([{'n': n} for n in range(7)]).repartition(3).map_batches(batch, batch_size=10, batch_format="pyarrow").materialize()
        try:
            assert saved.num_blocks() == 3
            rows = saved.take_all()
            assert [r['batch'] for r in rows] == [3, 3, 3, 2, 2, 2, 2]
        finally:
            saved.release()
        assert data.range(7).repartition(3).count() == 7


def recovery_dataset():
    return data.from_items([{'n': n} for n in range(20)]).repartition(3)


def test_real_partition_manifest_survives_native_committed_recovery(tmp_path, monkeypatch):
    from demiflow.execution.checkpoint.coordinator import DataflowCheckpointCoordinator
    original = DataflowCheckpointCoordinator.commit
    class Interrupted(Exception):
        pass
    def after_final(coordinator, *args, **kwargs):
        image = original(coordinator, *args, **kwargs)
        if image.cut.final:
            raise Interrupted()
        return image
    with pytest.raises(Interrupted):
        with RunContext.start(tmp_path/'run') as run, data.local_execution(workers=2, memory_bytes=32 << 20):
            ref = run.ref
            with monkeypatch.context() as patch:
                patch.setattr(DataflowCheckpointCoordinator, 'commit', after_final)
                recovery_dataset().materialize()
    with RunContext.resume(ref) as run, data.local_execution(workers=2, memory_bytes=32 << 20):
        before = run.journal.execution_commits('action-1')
        saved = recovery_dataset().materialize()
        try:
            assert saved.num_blocks() == 3
            assert list(saved._source.handle.rows()) == [{'n': n} for n in range(20)]
            assert run.journal.execution_commits('action-1') == before
        finally:
            saved.release()


def test_randomize_blocks_preserves_within_block_order_and_empty_blocks(tmp_path):
    with RunContext.start(tmp_path/'run'), data.local_execution(workers=2, memory_bytes=32 << 20):
        saved = data.from_items([{'n': i} for i in range(8)]).repartition(4).randomize_block_order(seed=23).materialize()
        empty = data.from_items([]).repartition(4).randomize_block_order(seed=23).materialize()
        try:
            assert saved.num_blocks() == empty.num_blocks() == 4
            blocks = [[r.inline['n'] for r in records] for records in _block_records(saved)]
            assert sorted(blocks) == [[0, 1], [2, 3], [4, 5], [6, 7]]
            assert list(empty._source.handle.rows()) == []
        finally:
            saved.release()
            empty.release()


def test_ray_core_uses_same_logical_block_manifest(tmp_path, monkeypatch):
    import tempfile
    ray = pytest.importorskip('ray')
    import ray.data
    from demiflow.data.dataset import Dataset
    from demiflow.execution.executors.ray import RayDatasetExecutor
    def forbidden(*args, **kwargs):
        raise AssertionError('Ray Data cannot execute repartition')
    for name in ('from_items', 'from_arrow', 'read_datasource'):
        monkeypatch.setattr(ray.data, name, forbidden)
    try:
        ray.init(address='local', num_cpus=3, include_dashboard=False,
                 object_store_memory=100*1024**2,
                 _temp_dir=tempfile.mkdtemp(prefix='demiflow-ray-blocks-', dir='/tmp'))
        with RunContext.start(tmp_path/'run', limits={'memory_bytes': 32 << 20, 'max_row_bytes': 4096,
                                                     'max_batch_bytes': 1 << 20}):
            source = data.from_items([{'n': n} for n in range(7)]).repartition(3)
            hosted = Dataset(source._source, source._plan, RayDatasetExecutor())
            result = hosted.materialize()
            try:
                assert result.num_blocks() == 3
                assert list(result._source.handle.rows()) == [{'n': n} for n in range(7)]
                assert [len(records) for records in _block_records(result)] == [3, 2, 2]
            finally:
                result.release()
    finally:
        ray.shutdown()
