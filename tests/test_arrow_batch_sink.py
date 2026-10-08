import lance
import pyarrow as pa
import pytest
from demiflow import data
from demiflow.execution.executors.local import LocalDatasetExecutor


@pytest.mark.parametrize('mode', ['thread', 'process'])
@pytest.mark.parametrize('empty', [False, True])
def test_arrow_sink_preserves_exact_scan_global_batches_and_nested_values(tmp_path, monkeypatch, mode, empty):
    schema = pa.schema([('id', pa.int64()), ('items', pa.list_(pa.struct([('text', pa.large_string())])))])
    table = pa.Table.from_pylist([{'id': i, 'items': None if i == 0 else [] if i == 1 else [{'text': None if i == 2 else str(i)}]} for i in range(8)], schema=schema)
    source, target = str(tmp_path/'source.lance'), str(tmp_path/'target.lance')
    lance.write_dataset(table, source, max_rows_per_file=2, max_rows_per_group=2)
    lance.write_dataset(table, source, mode='append')
    output_schema = schema.append(pa.field('batch_n', pa.int64()))
    def callback(batch, factor):
        assert batch.schema.equals(schema)
        values = pa.array([len(batch) * factor] * len(batch), type=pa.int64())
        result = batch.append_column('batch_n', values)
        yield result.slice(0, 1).to_batches()[0]
        yield result.slice(1)
    def no_rows(*args, **kwargs):
        raise AssertionError('Columnar batch sink must not execute Python row conversion')
    monkeypatch.setattr(LocalDatasetExecutor, '_apply_plan', no_rows)
    with data.local_execution(workers=2, worker_mode=mode, batch_rows=3, temp_directory=str(tmp_path)) as session:
        data.read_lance(source, version=1, filter='false' if empty else 'id >= 1', limit=6, batch_size=2).map_batches(
            callback, batch_format='pyarrow', fn_kwargs={'factor': 2}).write_lance(target, mode='create', schema=output_schema)
        assert session.stats['arrow_batch_sink'] and session.stats['status'] == 'complete'
        assert session.stats['peak_pending_tasks'] <= 4
    expected = table.slice(1, 6).append_column('batch_n', pa.array([6]*6, type=pa.int64()))
    if empty:
        expected = expected.slice(0, 0)
    assert lance.dataset(target).to_table().equals(expected)
    assert not list(tmp_path.glob('demiflow-arrow-*'))


def test_arrow_callback_failure_does_not_publish_or_retry(tmp_path):
    source, target = str(tmp_path/'source.lance'), str(tmp_path/'target.lance')
    table = pa.table({'id': [1, 2, 3, 4]})
    lance.write_dataset(table, source)
    lance.write_dataset(table.slice(0, 1), target)
    calls = tmp_path/'calls.txt'
    def fail(batch):
        with calls.open('a') as stream:
            stream.write(str(batch['id'][0].as_py()) + '\n')
        if batch['id'][0].as_py() == 3:
            raise ValueError('deliberate callback failure')
        return batch
    with data.local_execution(workers=1, worker_mode='thread', temp_directory=str(tmp_path)) as session:
        with pytest.raises(Exception, match='deliberate callback failure'):
            data.read_lance(source, version=1).map_batches(fail, batch_size=2, batch_format='pyarrow').write_lance(
                target, mode='overwrite', expected_version=1, schema=table.schema)
        assert session.stats['status'] == 'failed'
    assert calls.read_text().splitlines() == ['1', '3']
    assert lance.dataset(target).version == 1
    assert lance.dataset(target).to_table().equals(table.slice(0, 1))
    assert not list(tmp_path.glob('demiflow-arrow-*'))
