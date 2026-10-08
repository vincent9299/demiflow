"""Opaque Python payloads keep 64-bit offsets across native join amplification."""
import json
from pathlib import Path

from demiflow.data.api import DataAPI
from demiflow.execution.executors.local import LocalDatasetExecutor


def test_opaque_join_uses_large_offsets_and_preserves_values(tmp_path):
    source = DataAPI(LocalDatasetExecutor(resource_root=tmp_path))
    opaque = {'bytes': b'\x00\xff' * 131072, 'tuple': ('original', None), 'set': {'x'}}
    left = source.from_items([{'k': 1, 'value': opaque}, {'k': 2, 'value': None}])
    right = source.from_items([{'k': 1, 'tag': 'a'}, {'k': 1, 'tag': 'b'}])
    result = left.join(right, on='k', how='left')
    assert result.take_all() == [
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
        assert 1 <= options['batch_rows'] <= 31
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
    assert result.take_all() == [{'k': i, 'value': opaque} for i in sorted(range(24), key=lambda i: json.dumps([i]))]
    reports = [s for s in result.execution_metadata().diagnostics['stages'] if s['name'] == 'datafusion']
    options = [json.loads((Path(s['query']) / 'result.json').read_text())['options'] for s in reports]
    assert options[0]['batch_rows'] == 8192
    assert 1 <= options[-1]['batch_rows'] <= 7
