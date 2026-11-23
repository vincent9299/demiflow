"""Public Arrow delivery keeps the run and native resource ceilings."""
import lance
import pyarrow as pa

from demiflow import data
from demiflow.execution import RunContext


def _write_source(tmp_path, rows=8):
    schema = pa.schema([('id', pa.int64()), ('val', pa.int64())])
    table = pa.Table.from_pylist([{'id': i, 'val': i * 2} for i in range(rows)], schema=schema)
    uri = str(tmp_path / 'source.lance')
    lance.write_dataset(table, uri)
    return uri, table


def test_execute_bound_tightens_real_native_output_batches(tmp_path):
    uri, table = _write_source(tmp_path)
    with RunContext.start(tmp_path/'run'), data.local_execution(
            workers=2, worker_mode='thread', native_resources={'batch_rows': 2}):
        ds = data.read_lance(uri, version=1).select_columns(['id'])
        batches = list(ds.iter_batches(batch_size=2, batch_format='pyarrow'))
    assert batches and all(0 < len(batch) <= 2 for batch in batches)
    got = pa.Table.from_batches([chunk for table in batches for chunk in table.to_batches()])
    assert got.sort_by('id').equals(table.select(['id']).sort_by('id'))
