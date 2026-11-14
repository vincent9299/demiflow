"""L010/L036/L037 native wiring: execute -> Engine -> Engine.session borrows the
same sealed ExecutionPlacement end to end into the real DataFusion authority.

L010 execute(execution=None) forwards to Engine; L036 Engine.__init__ holds it;
L037 Engine.session passes it to native_resources.dataset_session, tightening
the real session options. The bound native query shows a real observable effect
(output batch rows), not a parameter passthrough. No second allocator.
"""
from contextlib import ExitStack
from types import SimpleNamespace

import lance
import pyarrow as pa
import pytest

from demiflow import data
from demiflow.execution import native_resources as resources
from demiflow.execution.dataset_native import Engine, execute

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


def test_engine_holds_and_session_borrows_sealed_placement(host, tmp_path):
    sample = _placement(cpu_millis=2000, memory_bytes=2 * GiB, inflight_rows=100,
                        inflight_bytes=8 * MiB)
    engine = Engine(host, ExitStack(), str(tmp_path), {'stages': []}, execution=sample)
    # L036: the Engine holds the exact placement.
    assert engine.execution is sample
    # L037: the lazily created real session is tightened by that placement.
    options = engine.session.options
    assert options.max_rss_bytes <= 2 * GiB
    assert options.threads <= 2
    assert options.batch_rows <= 100
    assert options.max_row_bytes <= 8 * MiB
    assert engine.session is engine.session  # created once, borrowed thereafter


def test_engine_unbound_session_keeps_default_authority(host, tmp_path):
    engine = Engine(host, ExitStack(), str(tmp_path), {'stages': []})
    assert engine.execution is None
    options = engine.session.options
    assert (options.memory_bytes, options.max_rss_bytes, options.threads) == (8 * GiB, 24 * GiB, 8)


def _write_source(tmp_path, rows=8):
    schema = pa.schema([('id', pa.int64()), ('val', pa.int64())])
    table = pa.Table.from_pylist([{'id': i, 'val': i * 2} for i in range(rows)], schema=schema)
    uri = str(tmp_path / 'source.lance')
    lance.write_dataset(table, uri)
    return uri, table


def test_execute_bound_tightens_real_native_output_batches(tmp_path):
    uri, table = _write_source(tmp_path)
    with data.local_execution(workers=2, worker_mode='thread', temp_directory=str(tmp_path)) as session:
        executor = session._executor
        ds = data.read_lance(uri, version=1).select_columns(['id'])
        sample = _placement(inflight_rows=2, inflight_bytes=8 * MiB)
        bound = list(execute(executor, ds._source, ds._plan, arrow=True, execution=sample))
        assert bound and all(b.num_rows <= 2 for b in bound)
        assert sum(b.num_rows for b in bound) == 8
        unbound = list(execute(executor, ds._source, ds._plan, arrow=True))
        # The sealed inflight_rows=2 really shrinks output batches vs the default.
        assert max(b.num_rows for b in unbound) > 2
    expected = table.select(['id'])
    got = pa.Table.from_batches(bound, schema=expected.schema)
    assert got.sort_by('id').equals(expected.sort_by('id'))