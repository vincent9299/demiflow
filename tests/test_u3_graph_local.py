"""U3 Local multi-worker full-graph acceptance (S3, exit gate 1).

A shared ancestor computes each record once and replicates it at egress into six
1:1 physical channels; six independent target workers consume timely and stage
terminal output. The driver provisions every worker through the real
ResourceManager + LocalResourceProvider + managed process unit, inherits the data
socketpair fds (business payloads never transit the driver), runs the frozen K3
lifecycle per worker and acks each K6-shaped cut contribution.

Exit gate 1: six-target shared expansion executes once (ancestor egress == n, not
6n), a slow target does not block the ancestor or unrelated fast targets, and
records dequeue timely. The single CommitCoordinator commit + full K6 coverage +
whole-graph restore + segment GC are S4; here the driver collects staging
descriptors. The opt-in ``unified_graph`` executor seam (D6) leaves the default
in-driver path byte-unchanged.
"""
import os
import sys
import time

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


def test_local_graph_six_targets_shared_egress_once(tmp_path):
    spec, readers, n = _graph_spec()
    executor = LocalDatasetExecutor(workers=16, unified_graph=True)
    with RunContext.start(tmp_path / "run",
                          limits={"memory_bytes": 16 * 1024 ** 3, "checkpoint_rows": 8, "checkpoint_interval_ms": 60000}) as run:
        with run.action({"kind": "materialize", "source": {"fixture": "u3-graph"}}) as execution:
            result = executor.materialize_graph(spec)
            execution.complete(dict(kind="graph-materialized", workers=result["workers"],
                                    egress=result["egress_rows"]))
        # Ownership never moved: every worker attached under the granted fence.
        with run.journal._connection() as db:
            owner = db.execute("SELECT attempt,fence FROM runtime_owner").fetchone()
        assert owner == (run.attempt, run.fence)

    # Seven independent worker hosts (one ancestor + six targets).
    assert result["workers"] == 7
    # The shared ancestor expanded each record ONCE: egress == n, never 6n.
    assert result["egress_rows"] == n
    # Every target received the full broadcast set and staged it timely.
    assert set(result["target_rows"]) == set(readers)
    for rid in readers:
        assert result["target_rows"][rid] == n, (rid, result["target_rows"][rid])
    # Each worker reached a final cut.
    assert all(result["final_arrival"][rid] is not None for rid in ["ancestor", *readers])

    # Genuine independent worker processes were supervised (birth identity recorded).
    units = tmp_path / "run-resources" / "units"
    records = list(units.glob("*/*/process.json"))
    assert len(records) >= 7, "managed graph worker identities were not recorded"
    import json
    pids = {json.loads(path.read_text())["identity"]["pid"] for path in records}
    assert os.getpid() not in pids
    assert len(pids) >= 7
    # Capacity was released after confirmed exit evidence.
    from demiflow.resources.capacity import LocalCapacityPool
    pool = LocalCapacityPool(tmp_path / "run-resources" / "capacity",
                             capacity={"cpu_millis": 16000, "memory_bytes": 16 * 1024 ** 3})
    assert pool.snapshot()["charged"] == {"cpu_millis": 0, "memory_bytes": 0}


def test_local_graph_slow_target_does_not_block_unrelated(tmp_path):
    spec, readers, n = _graph_spec(n=60, slow_reader="c3")
    executor = LocalDatasetExecutor(workers=16, unified_graph=True)
    with RunContext.start(tmp_path / "run",
                          limits={"memory_bytes": 16 * 1024 ** 3, "checkpoint_rows": 8, "checkpoint_interval_ms": 60000}) as run:
        with run.action({"kind": "materialize", "source": {"fixture": "u3-graph"}}) as execution:
            result = executor.materialize_graph(spec)
            execution.complete(dict(kind="graph-materialized", workers=result["workers"],
                                    egress=result["egress_rows"]))
    arrival = result["final_arrival"]
    fast = [rid for rid in readers if rid != "c3"]
    # The ancestor and every fast target finalized well before the slow target.
    latest_fast = max(arrival[rid] for rid in fast)
    assert arrival["ancestor"] < arrival["c3"]
    assert latest_fast < arrival["c3"]
    assert arrival["c3"] - latest_fast > 0.1, (arrival["c3"], latest_fast)
    # Isolation never lost records: the slow target still received the full set.
    assert result["target_rows"]["c3"] == n
    assert result["egress_rows"] == n


def test_graph_seam_default_off(tmp_path):
    # D6: the default in-driver path never provisions a graph worker host.
    items = tuple({"x": i} for i in range(10))

    def double(row):
        return {**row, "x": row["x"] * 2}

    executor = LocalDatasetExecutor(workers=1)
    assert executor._unified_graph is False
    from demiflow.data.dataset import Dataset
    dataset = Dataset(ItemsSource(items), _stream_map(double), executor)
    with RunContext.start(tmp_path / "run",
                          limits={"memory_bytes": 16 * 1024 ** 3, "checkpoint_rows": 4, "checkpoint_interval_ms": 60000}):
        rows = dataset.materialize().take_all()
    assert rows == [{"x": i * 2} for i in range(10)]
    assert not (tmp_path / "run-resources").exists()