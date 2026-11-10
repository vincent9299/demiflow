"""U3 whole-graph restore acceptance (S4c, exit gate 3 Local; design §4.3/D5).

A worker fault replaces the failed allocation and the WHOLE graph resumes from the
latest acked cut: the ancestor skips the acked source prefix and re-sends post-cut
data, targets stage only post-cut output, and the driver's single commit authority
reconciles any prepared cut before re-provisioning. These tests compose the frozen
recovery contract through the real Local graph rather than re-proving it at unit
level (the U2 CMT/AUT groups already do that). The ancestor SIGKILLs itself once
after a durable acked cut; the driver run exits failed; ``RunContext.resume``
re-enters the same action under a new attempt/fence and proves the acked prefix is
never recomputed or duplicated, commit sequences stay contiguous and acked, and a
completed graph short-circuits re-entry without a second worker generation.
"""
import os
import signal
import sys
from collections import Counter
from pathlib import Path

import pytest

from demiflow.data.plan import CallableSpec, LogicalPlan, MapOp, MapStreamOptions
from demiflow.data.sources import ItemsSource
from demiflow.execution.block_sink import BlockSink
from demiflow.execution.executors.local import LocalDatasetExecutor
from demiflow.execution.recovery_contract import RecoveryContractError
from demiflow.execution.run_context import RunAction, RunContext
from demiflow.resources.capacity import LocalCapacityPool
from demiflow.resources.process_unit import ManagedProcessUnit

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


def _graph_spec(shared_double, n=24, cut_rows=8, readers=("c0", "c1", "c2"),
                mode="durable_incremental"):
    def passthrough(row):
        return row

    items = tuple({"x": i} for i in range(n))
    targets = [{"plan": _stream_map(passthrough), "logical_node_id": "node-" + rid,
                "occurrence_id": rid + "-p0", "reader_id": rid} for rid in readers]
    spec = {
        "source": ItemsSource(items), "source_id": "u3-graph-source",
        "ancestor": {"plan": _stream_map(shared_double),
                     "logical_node_id": "shared", "occurrence_id": "shared-p0"},
        "targets": targets, "routing": "broadcast", "exchange_mode": mode,
        "credit_budget": {"rows": 8, "bytes": 8 * 1024 ** 2},
        "queue_depth": 4096, "cut_rows": cut_rows,
    }
    return spec, list(readers), n


def _committed_rows(run, action_id, rid, facts):
    sink = BlockSink(RunAction(run, action_id + "-graph-" + rid, {"kind": "graph-node"}), rid)
    rows = []
    for fact in facts:
        rows.extend(sink.iter_rows(fact["receipts"][rid]["data"]))
    return rows


def _release_crash_orphans(root):
    """Reconcile capacity for allocations whose driver died before close.

    A crash/fault leaves the allocation BOUND (charged); exit alone never frees
    capacity. The canonical unit-lock + birth-identity evidence confirms the dead
    host and releases it, exactly as the provider's ensure_absent path does.
    """
    root = Path(root)
    pool = LocalCapacityPool(root / "capacity",
                             capacity={"cpu_millis": 16000, "memory_bytes": 16 * 1024 ** 3})
    unit = ManagedProcessUnit(root / "units")
    for record in pool.snapshot()["active"]:
        allocation_id = record["allocation_id"]
        process_json = next((root / "units").glob("*/" + allocation_id + "/process.json"))
        scope_id = process_json.parent.parent.name
        evidence = unit.stop(scope_id, allocation_id, terminate_deadline_ms=2000)
        assert evidence["confirmed"] is True
        pool.release(allocation_id, evidence)
        try:
            os.waitpid(evidence["identity"]["pid"], os.WNOHANG)  # reap our own zombie child
        except (ChildProcessError, OSError):
            pass
    assert pool.snapshot()["charged"] == {"cpu_millis": 0, "memory_bytes": 0}


def test_graph_worker_crash_recovers_acked_prefix_exactly_once(tmp_path):
    crash_marker = tmp_path / "crashed-once"
    trace = tmp_path / "ancestor-trace"

    def shared_double(row):
        # Trace every row the ancestor actually processes, then crash the whole
        # worker once on the first POST-cut row (x==8). With unconfirmed_rows==8 the
        # feed blocks at the cut-8 bound until the driver acks, so the crash lands
        # strictly after cut 8 is durable: rows x==0..7 are the acked prefix.
        with open(trace, "a") as stream:
            stream.write(str(row["x"]) + "\n")
        if row["x"] == 8 and not crash_marker.exists():
            crash_marker.touch()
            os.kill(os.getpid(), signal.SIGKILL)
        return {**row, "x": row["x"] * 2}

    spec, readers, n = _graph_spec(shared_double, n=24, cut_rows=8)
    executor = LocalDatasetExecutor(workers=1, unified_graph=True)
    limits = {"checkpoint_rows": 8, "checkpoint_unconfirmed_rows": 8,
              "checkpoint_interval_ms": 60000}
    # The ancestor dies mid-graph; the driver run exits failed. Recovery re-enters
    # the same action under a new attempt/fence, reconciling the acked cut rather
    # than duplicating it (whole-graph restore, design §4.3/D5).
    with pytest.raises((RecoveryContractError, OSError)):
        with RunContext.start(tmp_path / "run", limits=limits) as run:
            reference = run.ref
            with run.action({"kind": "materialize", "source": {"fixture": "u3-graph-restore"}}) as execution:
                executor.materialize_graph(spec)
                execution.complete(dict(kind="graph-materialized"))
    assert crash_marker.exists(), "the ancestor crash window did not fire"

    with RunContext.resume(reference, limits=limits) as run2:
        assert run2.attempt == 2
        with run2.action({"kind": "materialize", "source": {"fixture": "u3-graph-restore"}}) as execution:
            result = executor.materialize_graph(spec)
            action_id = execution.action_id
            execution.complete(dict(kind="graph-materialized", egress=result["egress_rows"]))
        facts = run2.journal.execution_commits(action_id + "-graph-result")
        sequences = [f["sequence"] for f in facts]
        assert sequences == [1, 2, 3]  # contiguous across the fault, no gap/dup
        assert all(f["status"] == "acked" for f in facts)
        assert len({f["commit_id"] for f in facts}) == len(facts)
        # Output multiset equals the shared expansion applied once: the acked prefix
        # is neither recomputed nor duplicated by the restored generation.
        for rid in readers:
            rows = _committed_rows(run2, action_id, rid, facts)
            assert [r["x"] for r in rows] == [i * 2 for i in range(n)], rid
        with run2.journal._connection() as db:
            owner = db.execute("SELECT attempt,fence FROM runtime_owner").fetchone()
        assert owner == (run2.attempt, run2.fence)

    assert result["egress_rows"] == n
    for rid in readers:
        assert result["target_rows"][rid] == n
    # The acked prefix (x==0..7) ran exactly once; the crashed post-cut row (x==8)
    # was never acked so it recomputes once; every later row runs once on restore.
    counts = Counter(int(line) for line in trace.read_text().splitlines())
    assert counts[8] == 2
    for i in range(n):
        if i != 8:
            assert counts[i] == 1, (i, counts[i])

    records = list((tmp_path / "run-resources" / "units").glob("*/*/process.json"))
    assert len(records) == 8, "a new worker generation was not provisioned for restore"
    import json
    pids = {json.loads(path.read_text())["identity"]["pid"] for path in records}
    assert len(pids) == 8 and os.getpid() not in pids
    _release_crash_orphans(tmp_path / "run-resources")


def test_completed_graph_short_circuits_reentry(tmp_path):
    def shared_double(row):
        return {**row, "x": row["x"] * 2}

    spec, readers, n = _graph_spec(shared_double, n=12, cut_rows=6)
    executor = LocalDatasetExecutor(workers=1, unified_graph=True)
    limits = {"checkpoint_unconfirmed_rows": 6, "checkpoint_interval_ms": 60000}
    # The graph completes, then a later action fails so the run exits failed with
    # the graph result durable. Resuming re-enters the same completed action.
    with pytest.raises(RuntimeError, match="later action failed"):
        with RunContext.start(tmp_path / "run", limits=limits) as run:
            reference = run.ref
            with run.action({"kind": "materialize", "source": {"fixture": "u3-graph-complete"}}) as execution:
                first = executor.materialize_graph(spec)
                execution.complete(dict(kind="graph-materialized", egress=first["egress_rows"]))
            raise RuntimeError("later action failed")
    with RunContext.resume(reference, limits=limits) as run2:
        with run2.action({"kind": "materialize", "source": {"fixture": "u3-graph-complete"}}) as execution:
            result = executor.materialize_graph(spec)
            action_id = execution.action_id
            execution.complete(dict(kind="graph-materialized", egress=result["egress_rows"]))
        facts = run2.journal.execution_commits(action_id + "-graph-result")
        assert [f["sequence"] for f in facts] == [1, 2]
        assert all(f["status"] == "acked" for f in facts)
        for rid in readers:
            rows = _committed_rows(run2, action_id, rid, facts)
            assert [r["x"] for r in rows] == [i * 2 for i in range(n)]
    # terminal.complete short-circuits before any worker provisioning or duplicate
    # commit (the restart-after-completion exactly-once gap).
    assert result["workers"] == 0
    records = list((tmp_path / "run-resources" / "units").glob("*/*/process.json"))
    assert len(records) == 4, "completed graph re-entry provisioned a second generation"
    pool = LocalCapacityPool(tmp_path / "run-resources" / "capacity",
                             capacity={"cpu_millis": 16000, "memory_bytes": 16 * 1024 ** 3})
    assert pool.snapshot()["charged"] == {"cpu_millis": 0, "memory_bytes": 0}


@pytest.mark.parametrize("mode", ["memory_incremental", "durable_incremental", "finite_sealed"])
def test_graph_restore_across_exchange_modes(tmp_path, mode):
    # Exit gate 3: each exchange mode recovers from a producer fault with an equal
    # output multiset. The restore mechanism is mode-independent (the ancestor skips
    # the acked source prefix and re-sends post-cut data); only carriage differs
    # (inline records vs segment refs), so all three must restore identically.
    crash_marker = tmp_path / "crashed-once"

    def shared_double(row):
        if row["x"] == 8 and not crash_marker.exists():
            crash_marker.touch()
            os.kill(os.getpid(), signal.SIGKILL)
        return {**row, "x": row["x"] * 2}

    spec, readers, n = _graph_spec(shared_double, n=24, cut_rows=8, mode=mode)
    executor = LocalDatasetExecutor(workers=1, unified_graph=True)
    limits = {"checkpoint_rows": 8, "checkpoint_unconfirmed_rows": 8,
              "checkpoint_interval_ms": 60000}
    with pytest.raises((RecoveryContractError, OSError)):
        with RunContext.start(tmp_path / "run", limits=limits) as run:
            reference = run.ref
            with run.action({"kind": "materialize", "source": {"fixture": "u3-graph-mode-restore"}}) as execution:
                executor.materialize_graph(spec)
                execution.complete(dict(kind="graph-materialized"))
    assert crash_marker.exists(), mode
    with RunContext.resume(reference, limits=limits) as run2:
        with run2.action({"kind": "materialize", "source": {"fixture": "u3-graph-mode-restore"}}) as execution:
            result = executor.materialize_graph(spec)
            action_id = execution.action_id
            execution.complete(dict(kind="graph-materialized", egress=result["egress_rows"]))
        facts = run2.journal.execution_commits(action_id + "-graph-result")
        assert [f["sequence"] for f in facts] == [1, 2, 3], mode
        assert all(f["status"] == "acked" for f in facts)
        assert len({f["commit_id"] for f in facts}) == len(facts)
        for rid in readers:
            rows = _committed_rows(run2, action_id, rid, facts)
            assert [r["x"] for r in rows] == [i * 2 for i in range(n)], (mode, rid)
    assert result["egress_rows"] == n
    _release_crash_orphans(tmp_path / "run-resources")