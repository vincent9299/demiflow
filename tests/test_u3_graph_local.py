"""U3 graph business semantics on the sole native v3 execution authority.

The historical graph path ignored executor.workers and admitted one CPU/GiB per
ancestor/target in its own 16-CPU/16-GiB pool. The fixture now declares that same
global ceiling explicitly. Six readers, shared work once, independent slow
branches, actual managed ownership and original ACK data remain required.
"""
import sys

import pytest

from demiflow.data.plan import CallableSpec, LogicalPlan, MapOp, MapStreamOptions
from demiflow.data.sources import ItemsSource
from demiflow.execution.executors.local import LocalDatasetExecutor
from demiflow.execution.run_context import RunContext

pytestmark = pytest.mark.skipif(not sys.platform.startswith("linux"),
                                reason="Local managed units require Linux")


@pytest.fixture(autouse=True)
def pidfds_available():
    from demiflow.resources.process_observation import require_pidfds
    try:
        require_pidfds()
    except (OSError, RuntimeError) as exc:
        pytest.skip("pidfd exit evidence unavailable: " + str(exc))


def _stream_map(fn):
    return LogicalPlan((MapOp(CallableSpec.create(fn),
                                stream_options=MapStreamOptions(concurrency=1)),))


def _graph_spec(n=60, slow_reader="c3"):
    # Local closures: cloudpickle serializes them by value so the independent
    # worker processes need not import this test module.
    def shared_double(row):
        return {**row, "x": row["x"] * 2}

    def passthrough(row):
        return row

    def slow_passthrough(row):
        import time as _time
        _time.sleep(0.005)
        return row

    items = tuple({"x": i} for i in range(n))
    readers = ["c0", "c1", "c2", "c3", "c4", "c5"]
    targets = []
    for rid in readers:
        fn = slow_passthrough if rid == slow_reader else passthrough
        targets.append({"plan": _stream_map(fn), "logical_node_id": "node-" + rid,
                        "occurrence_id": rid + "-p0", "reader_id": rid})
    spec = {
        "source": ItemsSource(items), "source_id": "u3-graph-source",
        "ancestor": {"plan": _stream_map(shared_double),
                     "logical_node_id": "shared", "occurrence_id": "shared-p0"},
        "targets": targets, "routing": "broadcast",
        "exchange_mode": "memory_incremental",
        "credit_budget": {"rows": 64, "bytes": 8 * 1024 ** 2},
        "queue_depth": 4096,
    }
    return spec, readers, n


def _execute(tmp_path, spec, readers, n):
    from _native_graph_fixtures import (HISTORICAL_MEMORY, HISTORICAL_WORKERS,
        assert_native_graph, assert_managed_graph)
    # bbd93ff graph.py:812 used source_rows when graph_spec omitted cut_rows.
    executor = LocalDatasetExecutor(workers=HISTORICAL_WORKERS)
    with RunContext.start(tmp_path / "run", limits={
            "memory_bytes": HISTORICAL_MEMORY, "checkpoint_rows": n,
            "checkpoint_interval_ms": 60000}) as run:
        with run.action({"kind": "materialize", "source": {"fixture": "u3-graph"}}) as execution:
            result = executor.materialize_graph(spec)
            execution.complete(dict(kind="graph-materialized", workers=result["workers"],
                                    egress=result["egress_rows"]))
        assert_native_graph(run, execution.action_id, result, readers=readers, count=n)
        assert_managed_graph(executor, run.directory, result)
    return result


def test_local_graph_six_targets_shared_egress_once(tmp_path):
    spec, readers, n = _graph_spec()
    result = _execute(tmp_path, spec, readers, n)
    # Six reader identities and actual managed workers; 7 was the retired
    # topology's process count, never the meaning of public workers=1.
    assert result['egress_rows'] == n
    assert result['target_rows'] == {reader: n for reader in readers}
    assert all(result['final_arrival'][rid] is not None for rid in ['ancestor', *readers])


def test_local_graph_slow_target_does_not_block_unrelated(tmp_path):
    # A measured sleep does not prove isolation: process startup and host load
    # can consume its whole margin. The slow worker instead refuses its first
    # output until every other branch has processed the complete source.
    entered = tmp_path / "slow-branch-entered"
    release = tmp_path / "slow-branch-released"
    markers = tuple(str(tmp_path / ("complete-" + rid))
                    for rid in ("c0", "c1", "c2", "c4", "c5"))

    def gated_passthrough(row):
        import time as _time
        from pathlib import Path as _Path
        entered.touch()
        deadline = _time.monotonic() + 30
        while not all(_Path(marker).exists() for marker in markers):
            if _time.monotonic() >= deadline:
                raise AssertionError("unrelated graph branches cannot finish while the slow branch waits")
            _time.sleep(0.005)
        release.touch()
        return row

    def fast_passthrough(marker):
        def apply(row):
            import time as _time
            from pathlib import Path as _Path
            if row['x'] == 118:
                # A serial executor cannot pass by finishing fast branches
                # before it ever schedules the slow branch.
                deadline = _time.monotonic() + 30
                while not entered.exists():
                    if _time.monotonic() >= deadline:
                        raise AssertionError("slow graph branch was never scheduled independently")
                    _time.sleep(0.005)
                _Path(marker).touch()
            return row
        return apply

    spec, readers, n = _graph_spec(n=60, slow_reader=None)
    fast = [rid for rid in readers if rid != "c3"]
    for target in spec['targets']:
        rid = target['reader_id']
        fn = gated_passthrough if rid == 'c3' else fast_passthrough(str(tmp_path / ("complete-" + rid)))
        target['plan'] = _stream_map(fn)
    result = _execute(tmp_path, spec, readers, n)
    assert entered.exists() and release.exists()
    assert all((tmp_path / ("complete-" + rid)).exists() for rid in fast)
    assert result["target_rows"] == {rid: n for rid in readers}
    assert result["egress_rows"] == n


def test_public_materialize_uses_default_local_provider(tmp_path):
    items = tuple({"x": i} for i in range(10))

    def double(row):
        return {**row, "x": row["x"] * 2}

    executor = LocalDatasetExecutor(workers=1)
    from demiflow.data.dataset import Dataset
    dataset = Dataset(ItemsSource(items), _stream_map(double), executor)
    with RunContext.start(tmp_path / "run",
                          limits={"checkpoint_rows": 4, "checkpoint_interval_ms": 60000}):
        rows = dataset.materialize().take_all()
    assert rows == [{"x": i * 2} for i in range(10)]
    assert not (tmp_path / "run-resources").exists()