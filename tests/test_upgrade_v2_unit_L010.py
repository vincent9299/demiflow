"""Native session options and public terminal delivery obey the original resource ceilings."""
from types import SimpleNamespace

import lance
import pyarrow as pa
import pytest

from demiflow import data
from demiflow.execution import native_resources as resources
from demiflow.execution import RunContext

GiB = 2**30
MiB = 2**20


def _placement(*, cpu_millis=2000, memory_bytes=2 * GiB, inflight_rows=None,
               inflight_bytes=None, stage_id='native_rel'):
    budget = {}
    if inflight_rows is not None:
        budget['inflight_rows'] = inflight_rows
    if inflight_bytes is not None:
        budget['inflight_bytes'] = inflight_bytes
    return SimpleNamespace(
        execution_id='exec-l010', placement='driver', group_id=None,
        backend='datafusion', stage_ids=(stage_id,),
        profile={'limits': {'cpu_millis': cpu_millis, 'memory_bytes': memory_bytes}},
        programs=(), stage_limits={stage_id: budget}, recovery='final_commit')


@pytest.fixture
def host(monkeypatch, tmp_path):
    monkeypatch.setattr(resources, '_cpu_capacity', lambda: 16)
    monkeypatch.setattr(resources, '_cgroup_memory', lambda: (0, 256 * GiB))
    return SimpleNamespace(resource_root=tmp_path, _local_kernel=None)


def test_native_session_borrows_sealed_placement(host, tmp_path):
    sample = _placement(cpu_millis=2000, memory_bytes=2 * GiB, inflight_rows=100,
                        inflight_bytes=8 * MiB)
    options = resources.dataset_session(host, tmp_path, execution=sample).options
    assert options.max_rss_bytes <= 2 * GiB
    assert options.threads <= 2
    assert options.batch_rows <= 100
    assert options.max_row_bytes <= 8 * MiB


def test_native_session_keeps_default_authority(host, tmp_path):
    options = resources.dataset_session(host, tmp_path).options
    assert (options.memory_bytes, options.max_rss_bytes, options.threads) == (8 * GiB, 24 * GiB, 8)


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
