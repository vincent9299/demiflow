"""Opaque Python payloads keep 64-bit offsets across native join amplification."""
import json
from pathlib import Path
import pytest

from demiflow.data.api import DataAPI
from demiflow.execution.executors.local import LocalDatasetExecutor

pytestmark = pytest.mark.slow


def test_opaque_join_uses_large_offsets_and_preserves_values(tmp_path):
    source = DataAPI(LocalDatasetExecutor(resource_root=tmp_path))
    opaque = {'bytes': b'\x00\xff' * 131072, 'tuple': ('original', None), 'set': {'x'}}
    left = source.from_items([{'k': 1, 'value': opaque}, {'k': 2, 'value': None}])
    right = source.from_items([{'k': 1, 'tag': 'a'}, {'k': 1, 'tag': 'b'}])
    result = left.join(right, on='k', how='left')
    assert sorted(result.take_all(), key=lambda row: (row['k'], row.get('tag', ''))) == [
        {'k': 1, 'value': opaque, 'tag': 'a'},
        {'k': 1, 'value': opaque, 'tag': 'b'},
        {'k': 2, 'value': None},
    ]
    reports = [s for s in result.execution_metadata().diagnostics['stages'] if s['name'] == 'datafusion']
    assert reports
    # Check the actual native boundary, not the Python input's inferred schema.
    for report in reports:
        query = Path(report['query'])
        worker = json.loads((query / 'worker_result.json').read_text())
        assert 'large_binary' in worker['schema']
        assert not any(line.endswith(': binary') for line in worker['schema'].splitlines())
        options = json.loads((query / 'result.json').read_text())['options']
        assert options['batch_rows'] == 8192
        hydration = worker['opaque_payload_read']
        assert hydration['query_payload_columns'] == 0
        assert hydration['max_batch_bytes'] <= 8 * 2**20
        assert 1 <= worker['writer_batch_rows'] <= options['batch_rows']
        assert worker['writer_batch_rows'] == options['batch_rows']
        assert worker['writer_batch_bytes'] == 8 * 2**20


def test_wide_payload_after_earlier_query_rebounds_native_batches(tmp_path):
    source = DataAPI(LocalDatasetExecutor(resource_root=tmp_path))
    opaque = b'x' * (1024 * 1024)
    # The right grouping runs a narrow native query before producing wide rows.
    # Its session must adopt the new bound for the subsequent outer join.
    right = (source.from_items([{'k': i} for i in range(24)])
        .reduce_by_key('k', lambda _, row: row)
        .map(lambda row: {**row, 'value': opaque}))
    result = source.from_items([{'k': i} for i in range(24)]).join(right, on='k')
    assert sorted(result.take_all(), key=lambda row: row['k']) == [{'k': i, 'value': opaque} for i in range(24)]
    reports = [s for s in result.execution_metadata().diagnostics['stages'] if s['name'] == 'datafusion']
    options = [json.loads((Path(s['query']) / 'result.json').read_text())['options'] for s in reports]
    assert options[0]['batch_rows'] == 8192
    assert options[-1]['batch_rows'] == 8192
    final = json.loads((Path(reports[-1]['query']) / 'worker_result.json').read_text())
    assert final['opaque_payload_read']['max_batch_bytes'] <= 8 * 2**20


def test_indivisible_payload_over_eight_mib_survives_join_and_sink(tmp_path):
    import lance
    import pyarrow as pa
    source = DataAPI(LocalDatasetExecutor(resource_root=tmp_path))
    payload = b'full-original\x00' * 750000
    left = source.from_items([{'k': 1, 'payload': payload}, {'k': 2, 'payload': None}])
    right = source.from_items([{'k': 1, 'tag': 'a'}, {'k': 1, 'tag': 'b'}])
    result = left.join(right, on='k', how='left')
    output = tmp_path / 'wide.lance'
    schema = pa.schema([('k', pa.int64()), ('payload', pa.large_binary()), ('tag', pa.string())])
    result.write_lance(str(output), schema=schema, mode='create')
    rows = lance.dataset(str(output)).to_table().to_pylist()
    assert sorted((row['k'], row['tag'] or '') for row in rows) == [(1, 'a'), (1, 'b'), (2, '')]
    assert all(row['payload'] == payload for row in rows if row['k'] == 1)
    assert [row for row in rows if row['k'] == 2] == [{'k': 2, 'payload': None, 'tag': None}]
    reports = [s for s in result.execution_metadata().diagnostics['stages'] if s['name'] == 'datafusion']
    assert reports
    for item in reports:
        report = json.loads((Path(item['query']) / 'result.json').read_text())
        assert report['options']['max_row_bytes'] > len(payload)
        assert report['worker']['writer_batch_bytes'] == 8 * 2**20


@pytest.mark.parametrize('partitions', [1, 2])
def test_wide_join_hydrates_after_key_only_join_under_small_pool(tmp_path, monkeypatch, partitions):
    """Each input exceeds a 256 MiB pool; verify every payload without collecting it."""
    from dataclasses import replace
    from demiflow.execution import native_resources
    original = native_resources.dataset_session

    def limited(*args, **kwargs):
        session = original(*args, **kwargs)
        session.options = replace(session.options, memory_bytes=256 * 2**20,
            max_rss_bytes=2 * 2**30, threads=2, partitions=partitions, timeout_s=120)
        session.resources = tmp_path / 'limited_resources'
        return session

    monkeypatch.setattr(native_resources, 'dataset_session', limited)
    source = DataAPI(LocalDatasetExecutor(resource_root=tmp_path))
    count = 40000

    def left():
        for i in range(count):
            yield {'k': i, 'left_payload': bytes([i % 251]) * 8192}
        yield {'k': None, 'left_payload': b'null key'}

    def right():
        for i in range(count):
            yield {'k': i, 'right_payload': bytes([i % 251]) * 8192, 'tag': 'base'}
        yield {'k': 0, 'right_payload': b'duplicate', 'tag': 'duplicate'}

    def check(row):
        if row['k'] is None:
            assert row == {'k': None, 'left_payload': b'null key'}
            return {'k': None, 'tag': 'unmatched'}
        expected = bytes([row['k'] % 251]) * 8192
        assert row['left_payload'] == expected
        assert row['right_payload'] == (b'duplicate' if row['tag'] == 'duplicate' else expected)
        return {'k': row['k'], 'tag': row['tag']}

    result = source.from_iter(left).join(source.from_iter(right), on='k', how='left').map(check)
    rows = result.take_all()
    assert len(rows) == count + 2
    assert {row['k'] for row in rows if row['tag'] == 'base'} == set(range(count))
    assert [row for row in rows if row['tag'] == 'duplicate'] == [{'k': 0, 'tag': 'duplicate'}]
    assert [row for row in rows if row['k'] is None] == [{'k': None, 'tag': 'unmatched'}]
    reports = [s for s in result.execution_metadata().diagnostics['stages'] if s['name'] == 'datafusion']
    assert reports
    for item in reports:
        directory = Path(item['query'])
        report = json.loads((directory / 'result.json').read_text())
        assert report['success'] and report['options']['memory_bytes'] == 256 * 2**20
        assert report['options']['partitions'] == partitions
        assert report['worker']['opaque_payload_read']['query_payload_columns'] == 0
        assert report['worker']['opaque_payload_read']['max_batch_bytes'] <= 8 * 2**20
