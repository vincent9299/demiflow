"""Opaque payload fidelity and real bounded key-only native join amplification."""
import json
from pathlib import Path
import pytest

from demiflow.data.api import DataAPI
from demiflow.execution.executors.local import LocalDatasetExecutor
from demiflow.execution.pipeline.run_context import RunContext
from demiflow.execution.contracts.dataflow_errors import DataflowError
from _worker_event_fixtures import events
from _join_spool_fixtures import install_join_spool_probe
from test_dataset_native import sealed_plan

pytestmark = pytest.mark.slow


def test_opaque_join_uses_bounded_key_index_and_preserves_values(tmp_path, monkeypatch):
    proof_path = install_join_spool_probe(tmp_path, monkeypatch)
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
    _, physical, _ = sealed_plan(result)
    assert result.execution_metadata().diagnostics['engine'] == 'dataflow_v3'
    reports = events(proof_path)
    assert len(reports) == 1 and reports[0]['input_rows'] == 4
    assert reports[0]['hydrated_rows'] == 4  # Matching left payload is reused for its duplicate match.
    assert reports[0]['max_index_entries'] == 2
    assert reports[0]['max_index_bytes'] <= reports[0]['index_limit']
    assert reports[0]['max_document_bytes'] <= 8 * 2**20
    assert reports[0]['max_resident_bytes'] == physical.bounds.max_resident_bytes


def test_wide_payload_after_earlier_query_rebounds_native_batches(tmp_path, monkeypatch):
    proof_path = install_join_spool_probe(tmp_path, monkeypatch)
    source = DataAPI(LocalDatasetExecutor(resource_root=tmp_path))
    opaque = b'x' * (1024 * 1024)
    # The right grouping runs a narrow native query before producing wide rows.
    # Its session must adopt the new bound for the subsequent outer join.
    right = (source.from_items([{'k': i} for i in range(24)])
        .reduce_by_key('k', lambda _, row: row)
        .map(lambda row: {**row, 'value': opaque}))
    result = source.from_items([{'k': i} for i in range(24)]).join(right, on='k')
    assert sorted(result.take_all(), key=lambda row: row['k']) == [{'k': i, 'value': opaque} for i in range(24)]
    graph, physical, _ = sealed_plan(result)
    assert [node.kernel.kind for node in graph.nodes if node.kernel.kind in {'join', 'reduce'}] == ['reduce', 'join']
    reports = events(proof_path)
    assert len(reports) == 1 and reports[0]['input_rows'] == 48
    assert reports[0]['hydrated_rows'] == 48
    assert reports[0]['max_index_bytes'] <= reports[0]['index_limit']
    assert reports[0]['max_document_bytes'] <= 8 * 2**20
    assert reports[0]['max_resident_bytes'] == physical.bounds.max_resident_bytes


def test_indivisible_payload_over_eight_mib_survives_join_and_sink(tmp_path, monkeypatch):
    proof_path = install_join_spool_probe(tmp_path, monkeypatch)
    import lance
    import pyarrow as pa
    source = DataAPI(LocalDatasetExecutor(resource_root=tmp_path))
    payload = b'full-original\x00' * 750000
    left = source.from_items([{'k': 1, 'payload': payload}, {'k': 2, 'payload': None}])
    right = source.from_items([{'k': 1, 'tag': 'a'}, {'k': 1, 'tag': 'b'}])
    result = left.join(right, on='k', how='left')
    output = tmp_path / 'wide.lance'
    schema = pa.schema([('k', pa.int64()), ('payload', pa.large_binary()), ('tag', pa.string())])
    # A 9.75 MB binary payload needs both encoded rows and Arrow buffers alive.
    # Preserve the original value: default 16 MiB batch is a negative arm, not a
    # reason to silently enlarge the limit or exempt a single indivisible row.
    with pytest.raises(DataflowError, match='BUDGET_EXCEEDED'):
        with RunContext.start(tmp_path / 'reject-run'):
            result.write_lance(str(output), schema=schema, mode='create')
    assert not (output / '_versions').exists()
    limits = {'memory_bytes': 256 * 2**20, 'max_row_bytes': 16 * 2**20,
              'max_batch_bytes': 32 * 2**20}
    with RunContext.start(tmp_path / 'accept-run', limits=limits):
        result.write_lance(str(output), schema=schema, mode='create')
    rows = lance.dataset(str(output)).to_table().to_pylist()
    assert sorted((row['k'], row['tag'] or '') for row in rows) == [(1, 'a'), (1, 'b'), (2, '')]
    assert all(row['payload'] == payload for row in rows if row['k'] == 1)
    assert [row for row in rows if row['k'] == 2] == [{'k': 2, 'payload': None, 'tag': None}]
    assert lance.dataset(str(output)).schema == schema
    assert lance.dataset(str(output)).version == 1
    _, physical, _ = sealed_plan(result)
    assert physical.bounds.max_resident_bytes == limits['memory_bytes']
    assert physical.bounds.max_row_bytes == limits['max_row_bytes']
    assert physical.bounds.max_batch_bytes == limits['max_batch_bytes']
    reports = events(proof_path)
    assert reports and all(report['max_index_bytes'] <= report['index_limit'] for report in reports)
    assert all(report['max_payload_bytes'] > len(payload) for report in reports)


@pytest.mark.parametrize('partitions', [1, 2])
def test_wide_join_hydrates_after_key_only_join_under_small_pool(tmp_path, monkeypatch, partitions):
    """Each input exceeds a 256 MiB pool; verify every payload without collecting it."""
    from demiflow.execution.pipeline.local_kernel import LocalKernelOptions
    proof_path = install_join_spool_probe(tmp_path, monkeypatch)
    native = dict(memory_bytes=256 * 2**20, max_rss_bytes=2 * 2**30,
                  threads=2, partitions=partitions, timeout_s=120)
    source = DataAPI(LocalDatasetExecutor(resource_root=tmp_path,
        local_kernel=LocalKernelOptions(native_resources=native)))
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
    _, physical, _ = sealed_plan(result)
    assert physical.bounds.max_resident_bytes == native['memory_bytes']
    assert physical.bounds.timeout_ms == 120000
    assert dict(physical.policy.native_limits) == native
    reports = events(proof_path)
    assert len(reports) == 1 and reports[0]['input_rows'] == count * 2 + 2
    assert reports[0]['max_resident_bytes'] == native['memory_bytes']
    assert reports[0]['native_limits'] == native
    assert reports[0]['max_index_bytes'] <= reports[0]['index_limit']
    assert reports[0]['max_document_bytes'] <= 8 * 2**20
