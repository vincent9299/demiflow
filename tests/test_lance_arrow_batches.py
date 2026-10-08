"""Patched Lance accepts legal nested slices directly; admission adds no copies."""
import pyarrow as pa
import pytest

lance = pytest.importorskip('lance')
from demiflow.lance.arrow_batches import bounded_record_batches, fixed_row_tables


def nested_table(rows=37):
    kind = pa.struct([
        ('child', pa.struct([('text', pa.string()), ('flag', pa.bool_())])),
        ('items', pa.list_(pa.struct([('n', pa.int64())]))),
    ])
    values = [None, {'child': None, 'items': []},
        {'child': {'text': None, 'flag': None}, 'items': [None, {'n': 3}]},
        {'child': {'text': 'kept', 'flag': False}, 'items': None}]
    return pa.table({'id': pa.array(range(rows), type=pa.int64()),
        'value': pa.array([values[i % 4] for i in range(rows)], type=kind)})


def test_direct_nested_slices_cross_unaligned_files(tmp_path):
    table = nested_table().slice(1, 35).replace_schema_metadata({'source': 'slice-fixture'})
    # Crucially: no demiflow adapter, concat, copy or file-alignment here.
    batches = table.to_batches(max_chunksize=7)
    assert batches[0].column('value').offset == 1
    ds = lance.write_dataset(pa.RecordBatchReader.from_batches(table.schema, batches),
        str(tmp_path / 'nested.lance'), max_rows_per_file=10, max_rows_per_group=5)
    assert [f.count_rows() for f in ds.get_fragments()] == [10, 10, 10, 5]
    assert ds.to_table().equals(table, check_metadata=True)


def test_direct_nullable_boolean_struct_at_file_boundary(tmp_path):
    mask = pa.array([i % 3 == 0 for i in range(32)])
    flags = pa.array([None if i % 3 == 0 else False for i in range(32)], type=pa.bool_())
    table = pa.table({'payload': pa.StructArray.from_arrays([flags], names=['flag'], mask=mask)})
    ds = lance.write_dataset(table, str(tmp_path / 'unadapted.lance'),
        max_rows_per_file=10, max_rows_per_group=5)
    assert [f.count_rows() for f in ds.get_fragments()] == [10, 10, 10, 2]
    assert ds.to_table().equals(table)


def test_direct_original_sixteen_row_nested_reproducer(tmp_path):
    kind = pa.struct([('x', pa.string()), ('child', pa.struct([('a', pa.string()), ('b', pa.string())]))])
    table = pa.table({'qid': [str(i) for i in range(16)], 'payload': pa.array([
        {'x': str(i), 'child': {'a': 'a', 'b': None}} if i % 3 else None for i in range(16)], type=kind)})
    reader = pa.RecordBatchReader.from_batches(table.schema, table.to_batches(max_chunksize=4))
    ds = lance.write_dataset(reader, str(tmp_path/'sixteen.lance'), data_storage_version='2.2')
    assert ds.to_table().equals(table)


def test_admission_preserves_buffers_and_small_batches_without_concat(monkeypatch):
    batch = pa.record_batch({'id': range(8), 's': ['x'*150, 'b', None, 'c', 'y'*150, '', 'd', 'e']})
    def no_copy(*args, **kwargs):
        raise AssertionError('Admission must not concatenate buffers')
    monkeypatch.setattr(pa, 'concat_arrays', no_copy)
    seen = []
    def source():
        for start in range(0, 8, 2):
            seen.append(start)
            yield batch.slice(start, 2)
    stream = bounded_record_batches(source(), batch.schema, batch_rows=7, batch_bytes=200)
    first = next(stream)
    assert seen == [0]  # No waiting for a full batch of seven rows.
    batches = [first, *stream]
    assert all(b.num_rows <= 2 and b.nbytes <= 200 for b in batches)
    assert all(b.column('s').buffers()[2].address == batch.column('s').buffers()[2].address for b in batches)
    assert pa.Table.from_batches(batches).equals(pa.Table.from_batches([batch]))
    assert next(bounded_record_batches([batch], batch.schema, batch_rows=8, batch_bytes=1000)) is batch


def test_byte_split_single_row_rejection_empty_and_metadata():
    table = pa.table({'s': ['a'*50, 'b'*50, 'c'*50, 'd'*5000]}).replace_schema_metadata({'kept':'yes'})
    stream = bounded_record_batches(table.to_batches(), table.schema, batch_rows=4, batch_bytes=120)
    a, b = next(stream), next(stream)
    assert [a.num_rows, b.num_rows] == [2, 1]
    assert a.schema.equals(table.schema, check_metadata=True)
    with pytest.raises(MemoryError, match='One Arrow row'):
        next(stream)
    assert list(bounded_record_batches([], table.schema)) == []
    with pytest.raises(ValueError, match='schema changed'):
        list(bounded_record_batches(pa.table({'different':[1]}).to_batches(),table.schema))
    with pytest.raises(ValueError):
        list(bounded_record_batches([], table.schema, batch_bytes=0))


def test_exact_callback_groups_use_chunks_and_admit_before_python(monkeypatch):
    table = nested_table(23)
    def no_copy(*args, **kwargs):
        raise AssertionError('Callback grouping must preserve chunks')
    monkeypatch.setattr(pa, 'concat_arrays', no_copy)
    groups = list(fixed_row_tables(table.to_batches(max_chunksize=3), table.schema, batch_rows=5))
    assert [g.num_rows for g in groups] == [5, 5, 5, 5, 3]
    assert groups[0].column(0).num_chunks == 2
    assert pa.concat_tables(groups).equals(table)
    wide = pa.table({'s':['x'*100]*4})
    with pytest.raises(MemoryError, match='callback group'):
        next(fixed_row_tables(wide.to_batches(max_chunksize=2), wide.schema, batch_rows=4, batch_bytes=300))
