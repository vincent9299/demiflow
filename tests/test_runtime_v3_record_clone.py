"""Frozen control provenance composes with block metadata without public coercion."""
from dataclasses import replace
from types import MappingProxyType
import hashlib

import lance
import pyarrow as pa
import pytest

from demiflow import data
from demiflow.execution.contracts.dataflow import Record, BlockPartition, thaw_record_value
from demiflow.execution.contracts.dataflow_errors import DataflowError
from demiflow.execution.dataflow_records import clone_record
from demiflow.execution.run_context import RunContext
from _worker_event_fixtures import emit, events


def record(position):
    return Record(record_id=hashlib.sha256(b'record').hexdigest(), source_position=position,
        inline={'business_tuple': (b'bytes', [1, 2]), 'negative_zero': -0.0},
        payload_ref=None, encoded_bytes=64, sequence=4)


def test_clone_thaws_only_validated_control_json_preserving_payload_types():
    original = record({'array': [1, {'nested': [2, 3]}],
        'dataflow_task': {'group': 'original-task', 'ordinal': 2}})
    block = BlockPartition(hashlib.sha256(b'block').hexdigest(), 1, 3)
    changed = clone_record(original, block_partition=block)
    assert changed.record_id == original.record_id and changed.sequence == 4
    assert changed.block_partition == block and original.block_partition is None
    from demiflow.execution.contracts.dataflow_codec import _thaw_json
    assert _thaw_json(changed.source_position) == {'array': [1, {'nested': [2, 3]}],
        'dataflow_task': {'group': 'original-task', 'ordinal': 2}}
    assert type(thaw_record_value(changed.inline)['business_tuple']) is tuple
    assert thaw_record_value(changed.inline) == thaw_record_value(original.inline)
    with pytest.raises(TypeError):
        changed.source_position['dataflow_task']['ordinal'] = 9


@pytest.mark.parametrize('bad', [(1, 2), b'raw', object(), MappingProxyType({'nested': 1})])
def test_public_constructor_and_external_clone_overrides_remain_strict(bad):
    with pytest.raises(DataflowError, match='finite Json'):
        record({'external': bad})
    with pytest.raises(DataflowError, match='finite Json'):
        clone_record(record({'valid': [1]}), source_position={'external': bad})
    with pytest.raises(DataflowError, match='validated Record'):
        clone_record({'not': 'a Record'})


def test_output_record_preserves_nested_json_array_positions():
    from demiflow.execution.kernels.dataflow import _output_record
    from test_runtime_v3_k import services
    original = record({'original': {'coordinates': [2, 4]}})
    result = _output_record(original.record_id, original.source_position,
        {'i': 1}, original.sequence, services())
    assert result.source_position == original.source_position


def test_actual_task_block_batch_sink_and_same_run_recovery(tmp_path):
    path = str(tmp_path / 'calls')
    output = tmp_path / 'result.lance'
    schema = pa.schema([('id', pa.int64()), ('value', pa.int64())])
    def before(row):
        emit(path, 'before', row=row['id'])
        return row
    def after(row):
        emit(path, 'after', row=row['id'])
        return {'id': row['id'], 'value': row['id'] + 10}
    def batch(table):
        emit(path, 'batch', rows=table.num_rows)
        assert table.schema.equals(schema)
        return table
    def build():
        return (data.range(7).map(before).repartition(num_blocks=3)
            .map(after).map_batches(batch, batch_size=2, batch_format='pyarrow'))
    class Interrupted(Exception):
        pass
    with pytest.raises(Interrupted):
        with RunContext.start(tmp_path / 'run') as run, data.local_execution(batch_rows=3):
            ref = run.ref
            build().write_lance(str(output), schema=schema, mode='create')
            commits = run.journal.execution_commits('action-1')
            assert commits[-1]['status'] == 'acked'
            raise Interrupted()
    before_events = events(path)
    assert sorted(event['row'] for event in before_events if event['event'] == 'before') == list(range(7))
    assert sorted(event['row'] for event in before_events if event['event'] == 'after') == list(range(7))
    with RunContext.resume(ref) as run, data.local_execution(batch_rows=3):
        build().write_lance(str(output), schema=schema, mode='create')
        assert run.journal.execution_commits('action-1') == commits
    assert events(path) == before_events
    dataset = lance.dataset(str(output))
    assert dataset.version == 1 and dataset.schema.equals(schema)
    assert sorted(dataset.to_table().to_pylist(), key=lambda row: row['id']) == [
        {'id': i, 'value': i + 10} for i in range(7)]


def test_native_cached_forwarding_keeps_provenance_but_clears_changed_row_schema():
    # Native forwarding component, not a simulated public execution result.
    from demiflow.execution.dataflow_models import _CachedMapKernel
    from demiflow.execution.contracts.dataflow import ArtifactRef
    original = record({'coordinates': [1, 2], 'dataflow_task': {'group': 'g', 'ordinal': 3}})
    block = BlockPartition(hashlib.sha256(b'block').hexdigest(), 0, 2)
    original = clone_record(original, block_partition=block,
        schema_ref=ArtifactRef('a' * 64, 1, 'test-store', 'a' * 64, 'application/x-arrow'))
    kernel = _CachedMapKernel.__new__(_CachedMapKernel)
    kernel._async = False
    kernel._cached = lambda row: {'answer': 7}
    output = list(kernel.push('input', original))[0]
    assert output.source_position == original.source_position
    assert output.block_partition == original.block_partition
    assert output.record_id == original.record_id and output.sequence == original.sequence
    assert output.schema_ref is None and thaw_record_value(output.inline) == {'answer': 7}
