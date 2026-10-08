import lance
import pyarrow as pa
import pytest

from demiflow import data
from demiflow.errors import InvalidLanceRequest
from demiflow.lance.model import LanceScanSpec
from demiflow.lance.read import plan_lance_scan_partitions, iter_lance_partition_batches


def test_projection_preserves_null_empty_and_pinned_version(tmp_path):
    uri = str(tmp_path / 'rows.lance')
    schema = pa.schema([('id', pa.int64()), ('items', pa.list_(pa.string()))])
    lance.write_dataset(pa.Table.from_pylist([
        {'id': 1, 'items': None}, {'id': 2, 'items': []}, {'id': 3, 'items': ['a', 'b']}
    ], schema=schema), uri)
    lance.write_dataset(pa.Table.from_pylist([{'id': 4, 'items': ['c']}], schema=schema), uri, mode='append')
    projection = {'id': 'id', 'n': 'array_length(items)'}
    rows = data.read_lance(uri, version=1, projection=projection, batch_size=1).take_all()
    assert rows == [{'id': 1, 'n': None}, {'id': 2, 'n': 0}, {'id': 3, 'n': 2}]
    query = LanceScanSpec(uri, version=1, projection=projection)
    parts = plan_lance_scan_partitions(query, target_partitions=2)
    actual = [r for p in parts for b in iter_lance_partition_batches(p) for r in b.to_pylist()]
    assert sorted(actual, key=lambda r: r['id']) == rows
    assert query.to_dict()['projection'] == projection
    assert query.content_hash != LanceScanSpec(uri, version=1, projection={'n': '1'}).content_hash
    with data.local_execution(workers=2, worker_mode='thread', partitions=2):
        assert sorted(data.read_lance(uri, version=1, projection=projection).take_all(), key=lambda r:r['id']) == rows


@pytest.mark.parametrize('kwargs', [
    {'projection': ['ab']}, {'projection': 'ab'}, {'projection': 42},
    {'projection': {'n': ''}}, {'projection': {'': '1'}},
    {'projection': [('n', '1'), ('n', '2')]},
    {'columns': ['id'], 'projection': {'id': 'id'}},
])
def test_invalid_projection_rejected_before_io(kwargs):
    with pytest.raises(InvalidLanceRequest):
        LanceScanSpec('/tmp/never_opened.lance', **kwargs)


def test_bad_expression_does_not_become_empty_result(tmp_path):
    uri = str(tmp_path / 'rows.lance')
    lance.write_dataset(pa.table({'id': [1]}), uri)
    with pytest.raises(Exception):
        data.read_lance(uri, version=1, projection={'n': 'missing_column + 1'}).take_all()
