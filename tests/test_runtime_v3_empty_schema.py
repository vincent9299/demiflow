"""Zero rows must not erase a sealed source's declared Arrow schema."""
import lance
import pandas as pd
import pyarrow as pa
import pytest

from demiflow import data
from demiflow.execution.run_context import RunContext


def typed_source(tmp_path, kind):
    schema = pa.schema([pa.field('id', pa.int32(), nullable=False, metadata={b'role': b'key'}),
        pa.field('text', pa.large_string()), pa.field('nested', pa.list_(pa.large_string()))],
        metadata={b'business': b'empty-schema'})
    table = pa.Table.from_batches([], schema=schema)
    if kind == 'arrow':
        return lambda: data.from_arrow(table), schema
    if kind == 'pandas':
        frame = pd.DataFrame({'id': pd.Series([], dtype='Int32'),
            'text': pd.Series([], dtype=pd.StringDtype(storage='pyarrow')),
            'when': pd.Series([], dtype='datetime64[ns]')})
        return lambda: data.from_pandas(frame), pa.Schema.from_pandas(frame, preserve_index=False)
    uri = str(tmp_path / 'empty.lance')
    lance.write_dataset(table, uri)
    return lambda: data.read_lance(uri, version=1), lance.dataset(uri, version=1).schema


@pytest.mark.parametrize('kind', ['arrow', 'pandas', 'lance'])
@pytest.mark.parametrize('action', ['schema', 'take_batch', 'materialize'])
def test_empty_typed_public_result_keeps_exact_schema(tmp_path, kind, action):
    build, expected = typed_source(tmp_path, kind)
    with data.local_execution():
        ds = build()
        if action == 'schema':
            actual = ds.schema()
        elif action == 'take_batch':
            batch = ds.take_batch(3, batch_format='pyarrow')
            assert batch.num_rows == 0
            actual = batch.schema
        else:
            saved = ds.materialize()
            try:
                assert saved.take_all() == []
                actual = saved.schema()
            finally:
                saved.release()
        assert actual.equals(expected, check_metadata=True)


@pytest.mark.parametrize('kind', ['arrow', 'pandas', 'lance'])
def test_empty_typed_materialized_same_run_restore_keeps_schema(tmp_path, kind):
    build, expected = typed_source(tmp_path, kind)
    class Interrupted(Exception):
        pass
    with pytest.raises(Interrupted):
        with RunContext.start(tmp_path / 'run') as run, data.local_execution():
            ref = run.ref
            saved = build().materialize()
            try:
                assert saved.schema().equals(expected, check_metadata=True)
                commits = run.journal.execution_commits('action-1')
                assert commits[-1]['status'] == 'acked'
            finally:
                saved.release()
            raise Interrupted()
    with RunContext.resume(ref) as run, data.local_execution():
        saved = build().materialize()
        try:
            assert saved.schema().equals(expected, check_metadata=True)
            assert saved.take_batch(3, batch_format='pyarrow').schema.equals(expected, check_metadata=True)
            assert run.journal.execution_commits('action-1') == commits
        finally:
            saved.release()


@pytest.mark.parametrize('kind', ['arrow', 'pandas'])
def test_empty_literal_type_change_cannot_reuse_old_result(tmp_path, kind):
    original = pa.schema([('value', pa.int32())])
    changed = pa.schema([('value', pa.large_string())])
    def build(schema):
        table = pa.Table.from_batches([], schema=schema)
        return data.from_arrow(table) if kind == 'arrow' else data.from_pandas(
            pd.DataFrame({'value': pd.Series([], dtype='Int32' if schema == original else 'string')}))
    class Interrupted(Exception):
        pass
    with pytest.raises(Interrupted):
        with RunContext.start(tmp_path / 'run') as run, data.local_execution():
            ref = run.ref
            saved = build(original).materialize()
            saved.release()
            commits = run.journal.execution_commits('action-1')
            raise Interrupted()
    from demiflow.execution.contracts.dataflow_errors import DataflowError
    with pytest.raises(DataflowError, match='schema'):
        with RunContext.resume(ref) as run, data.local_execution():
            saved = build(changed).materialize()
            saved.release()
    assert run.journal.execution_commits('action-1') == commits


def test_filter_projection_keeps_declared_empty_types_and_field_order(tmp_path):
    schema = pa.schema([pa.field('id', pa.int32(), metadata={b'role': b'key'}),
        pa.field('text', pa.large_string()), pa.field('discard', pa.int16())], metadata={b'app': b'v1'})
    table = pa.Table.from_pylist([{'id': 1, 'text': 'x', 'discard': 2}], schema=schema)
    expected = pa.schema([schema.field('text').with_name('body'), schema.field('id')], metadata=schema.metadata)
    with data.local_execution():
        ds = data.from_arrow(table).filter(lambda row: False).drop_columns(['discard']).rename_columns(
            {'text': 'body'}).select_columns(['body', 'id'])
        assert ds.schema().equals(expected, check_metadata=True)
        result = ds.take_batch(batch_format='pyarrow')
        assert result.num_rows == 0 and result.schema.equals(expected, check_metadata=True)


def test_schema_metadata_is_bounded_and_corruption_is_not_inferred():
    from test_runtime_v3_k import Bounds
    from demiflow.execution.dataflow_schema import encode, decode
    from demiflow.execution.contracts.dataflow_errors import DataflowError
    bounds = Bounds(max_row_bytes=64, max_batch_bytes=128, max_control_bytes=128)
    with pytest.raises(DataflowError, match='budget'):
        encode(pa.schema([('a', pa.int8())], metadata={b'wide': b'x' * 512}), bounds=bounds)
    with pytest.raises(DataflowError, match='Invalid sealed'):
        decode('not-arrow', bounds=bounds)
    with pytest.raises(DataflowError, match='bounds'):
        decode('x' * 1000, bounds=bounds)


def test_combined_schema_conflict_rejected_and_legacy_missing_field_preserved():
    # Component check for the original sink-combine authority. No alternate
    # publication or row execution is introduced by this store-boundary fixture.
    from test_runtime_v3_k import services
    from demiflow.execution.storage.record_schema import RecordSchemas
    from demiflow.execution.dataflow_sinks import combine_prepared_sinks, _artifact_json, RECORDS_MEDIA, MANIFEST_MEDIA
    from demiflow.execution.recovery_contract import document
    from demiflow.execution.contracts.dataflow_errors import DataflowError
    svc = services()
    schemas = RecordSchemas(svc)
    data_ref = svc.store.put(iter([b'']), max_bytes=1, media_type=RECORDS_MEDIA)
    def manifest(index, schema):
        value = {'schema': 'demiflow.dataflow_sink_manifest.v3', 'sink_id': 'materialize',
            'partition_id': 'p' + str(index), 'rows': 0, 'bytes': 0, 'columns': ['k'],
            'data_ref': _artifact_json(data_ref)}
        if schema is not None:
            value['schema_ref'] = _artifact_json(schemas.put(schema))
        raw = document(value)
        return svc.store.put(iter([raw]), max_bytes=len(raw), media_type=MANIFEST_MEDIA)
    left = manifest(0, pa.schema([('k', pa.int32())]))
    right = manifest(1, pa.schema([('k', pa.large_string())]))
    with pytest.raises(DataflowError, match='schema drifted'):
        combine_prepared_sinks([left, right], sink_id='materialize', services=svc)
    old = manifest(0, None)
    combined = combine_prepared_sinks([old], sink_id='materialize', services=svc)
    assert 'schema_ref' not in combined


@pytest.mark.parametrize('composition', ['blocks', 'union'])
def test_heterogeneous_inputs_remain_valid_row_collections(composition):
    first = pa.table({'value': pa.array([1], type=pa.int32())})
    second = pa.table({'value': pa.array(['x'], type=pa.large_string())})
    with data.local_execution():
        ds = data.from_arrow([first, second]) if composition == 'blocks' else data.from_arrow(first).union(
            data.from_arrow(second))
        assert sorted(ds.take_all(), key=lambda row: str(row['value'])) == [{'value': 1}, {'value': 'x'}]
