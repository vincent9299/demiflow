"""Public Arrow callbacks preserve business grouping and publish only complete output."""
import lance
import pyarrow as pa
import pytest

from demiflow import data
from demiflow.execution import RunContext
from demiflow.execution.contracts.dataflow_errors import DataflowError


def _source_table(tmp_path, rows=16):
    schema = pa.schema([('id', pa.int64())])
    table = pa.Table.from_pylist([{'id': i} for i in range(rows)], schema=schema)
    uri = str(tmp_path / 'source.lance')
    lance.write_dataset(table, uri, max_rows_per_file=4, max_rows_per_group=4)
    return uri, table, schema


def _append_n(batch):
    return batch.append_column('batch_n', pa.array([len(batch)] * len(batch), type=pa.int64()))


@pytest.mark.parametrize(('rows', 'group', 'window'), [(16, 4, 3), (11, 3, 2), (7, 1, 5)])
def test_public_callback_groups_across_fragments(tmp_path, rows, group, window):
    uri, table, schema = _source_table(tmp_path, rows=rows)
    target = str(tmp_path/'output')
    with RunContext.start(tmp_path/'run', limits={'batch_rows':window}), data.local_execution(
            workers=1, worker_mode='thread', partitions=1):
        data.read_lance(uri, version=1).map_batches(
            _append_n, batch_format='pyarrow', batch_size=group).write_lance(target, mode='create')
    counts = [min(group, rows - i // group * group) for i in range(rows)]
    expected = table.append_column('batch_n', pa.array(counts, type=pa.int64()))
    assert lance.dataset(target).to_table().sort_by('id').equals(expected)


def test_failed_yielding_callback_cannot_publish_prefix(tmp_path):
    uri, _, _ = _source_table(tmp_path)
    output = tmp_path/'target'
    marker = tmp_path/'called'

    def fail_after_yield(batch):
        with marker.open('a') as called:
            print(len(batch), file=called)
        yield batch
        raise ValueError('stopped after first batch')

    with pytest.raises((ValueError, DataflowError), match='stopped after first batch'):
        with RunContext.start(tmp_path/'run'), data.local_execution(workers=1, worker_mode='thread'):
            data.read_lance(uri, version=1).map_batches(
                fail_after_yield, batch_format='pyarrow', batch_size=2).write_lance(str(output), mode='create')
    assert marker.read_text().splitlines() == ['2']
    assert not (output/'_versions').exists()
    assert not list((tmp_path/'run'/'.dataflow-store').glob('*/staging/.pending-*'))
