"""Real typed rows must retain their schema across the unified execution."""
import json

import lance
import pyarrow as pa
import pytest

from demiflow import data
from demiflow.execution.run_context import RunContext


def _table():
    schema = pa.schema([pa.field('id', pa.int32(), nullable=False),
        pa.field('items', pa.list_(pa.struct([('text', pa.large_string())])))],
        metadata={b'origin': b'original-schema'})
    return pa.Table.from_pylist([{'id': i, 'items': None if i == 0 else [] if i == 1
        else [{'text': None if i == 2 else str(i)}]} for i in range(8)], schema=schema)


@pytest.mark.parametrize('reader', ['arrow', 'lance', 'parquet'])
def test_schema_survives_worker_exchange_and_materialized_reread(tmp_path, reader):
    table = _table()
    if reader == 'lance':
        lance.write_dataset(table, str(tmp_path / 'input.lance'), max_rows_per_file=2)
    elif reader == 'parquet':
        import pyarrow.parquet as pq
        pq.write_table(table, tmp_path / 'input.parquet', row_group_size=2)
        # Parquet uses its native element field name. Bind the exact stored
        # schema, not a pre-serialization schema that the format normalized.
        table = table.cast(pq.read_schema(tmp_path / 'input.parquet'))
    schema = table.schema
    def check(batch):
        assert batch.schema.equals(schema, check_metadata=True)
        return batch
    with RunContext.start(tmp_path / 'run') as run, data.local_execution(batch_rows=3):
        source = (data.from_arrow(table) if reader == 'arrow' else
                  data.read_lance(str(tmp_path / 'input.lance'), version=1) if reader == 'lance'
                  else data.read_parquet(str(tmp_path / 'input.parquet')))
        materialized = source.map_batches(check, batch_format='pyarrow').materialize()
        actual = materialized.take_batch(8, batch_format='pyarrow')
        assert actual.equals(table, check_metadata=True)
        assert materialized.schema().equals(table.schema, check_metadata=True)
        batches = list(materialized.iter_batches(batch_size=3, batch_format='pyarrow'))
        assert [len(batch) for batch in batches] == [3, 3, 2]
        assert pa.concat_tables(batches).equals(table, check_metadata=True)
        assert run.journal.execution_commits('action-1')[-1]['status'] == 'acked'


def test_exact_global_default_batch_and_native_write(tmp_path):
    table = _table()
    source, target = str(tmp_path / 'input.lance'), str(tmp_path / 'output.lance')
    lance.write_dataset(table, source, max_rows_per_file=2, max_rows_per_group=2)
    lance.write_dataset(table, source, mode='append')
    schema = table.schema
    output_schema = schema.append(pa.field('batch_n', pa.int64()))
    def callback(batch):
        assert batch.schema.equals(schema, check_metadata=True)
        result = batch.append_column('batch_n', pa.array([len(batch) * 2] * len(batch)))
        yield result.slice(0, 1).to_batches()[0]
        yield result.slice(1)
    with RunContext.start(tmp_path / 'run') as run, data.local_execution(batch_rows=3) as session:
        data.read_lance(source, version=1, filter='id >= 1', limit=6, batch_size=2).map_batches(
            callback, batch_format='pyarrow').write_lance(target, mode='create', schema=output_schema)
        assert session.stats['status'] == 'complete'
        assert session.stats['engine'] == 'dataflow_v3'
        assert run.journal.execution_commits('action-1')[-1]['status'] == 'acked'
    expected = table.slice(1, 6).append_column('batch_n', pa.array([6] * 6))
    assert lance.dataset(target).to_table().equals(expected, check_metadata=True)


def test_projection_rename_preserves_declared_field_types(tmp_path):
    table = _table()
    with RunContext.start(tmp_path / 'run'), data.local_execution():
        actual = data.from_arrow(table).select_columns(['items']).rename_columns({'items': 'nested'}).take_batch(
            8, batch_format='pyarrow')
    assert actual.schema.field('nested').type == table.schema.field('items').type
    assert actual.to_pylist() == [{'nested': row['items']} for row in table.to_pylist()]


def test_legacy_record_wire_bytes_stay_unchanged():
    from demiflow.execution.contracts.dataflow import Record, ArtifactRef
    from demiflow.execution.contracts.dataflow_codec import encode_wire, decode_wire
    from demiflow.execution.contracts.record_codec import _encode_payload, _encode_record_document, _decode_record_document
    from demiflow.execution.contracts.record_schema import SCHEMA_MEDIA_TYPE
    from demiflow.planning.dataflow_policy import resolve_runtime_policy
    _, bounds = resolve_runtime_policy({}, {}, provider='local')
    raw = _encode_payload({'x': 1}, max_bytes=4096)
    legacy = Record('a' * 64, {'index': 0}, None, {'x': 1}, len(raw), 0)
    frame = encode_wire(legacy, type_name='Record', bounds=bounds)
    assert 'schema_ref' not in json.loads(frame)['payload']
    assert decode_wire(frame, type_name='Record', bounds=bounds) == legacy
    document = _encode_record_document(legacy, max_bytes=4096)
    assert set(json.loads(document)) == {'v', 'sp', 'eb', 'p', 'r'}
    ref = ArtifactRef('b' * 64, 64, 'store', 'schemas/example', SCHEMA_MEDIA_TYPE)
    typed = Record('a' * 64, {'index': 0}, None, {'x': 1}, len(raw), 0, ref)
    assert decode_wire(encode_wire(typed, type_name='Record', bounds=bounds),
        type_name='Record', bounds=bounds) == typed
    assert _decode_record_document(_encode_record_document(typed, max_bytes=4096),
        record_id=typed.record_id, sequence=0, max_bytes=4096) == typed
