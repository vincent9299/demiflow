"""U3 fault matrix + exit gates 2/3 Local (S4e, design §7 CUT/FLT / §9 S4).

Composes the frozen recovery contract through the real Local graph: a consumer
(target) crash and a stage failure never publish an unauthorized cut, the whole
graph restores from the latest acked cut with an equal output multiset, sealed
storage insufficiency fails early (BUDGET_EXCEEDED before workers) rather than
deadlocking, and a multi-input reader aligns a cut across channels including an
EOI channel that contributes an empty barrier. Producer-crash restore is the S4c
group; segment-corruption detection (verify-before-deserialize) is the S2 carriage
group; old-fence rejection and effect-unknown fail-closed are inherited frozen
ResourceManager/K6 contracts (U2/E2), not re-proven here.
"""
import json
import os
import signal
import sys
import threading
import time
from pathlib import Path

import pytest

from demiflow.data.plan import CallableSpec, LogicalPlan, MapOp, MapStreamOptions
from demiflow.data.sources import ItemsSource
from demiflow.execution.block_sink import BlockSink
from demiflow.execution.exchange.carriage_local import LocalSegmentStore, channel_pair, inline_records
from demiflow.execution.executors.local import LocalDatasetExecutor
from demiflow.execution.executors.local import LocalDatasetExecutor
from demiflow.execution.recovery_contract import RecoveryContractError
from demiflow.execution.contracts.dataflow_errors import DataflowError
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


def _spec(target_fns, *, ancestor_fn=None, n=24, cut_rows=8, mode="durable_incremental",
          storage_bytes=None):
    # Plan functions must be nested closures so cloudpickle carries them by value;
    # a module-level fn would pickle by reference and the worker cannot import the
    # test module. ``None`` selects the local passthrough.
    def double(row):
        return {**row, "x": row["x"] * 2}

    def passthrough(row):
        return row

    ancestor_fn = ancestor_fn or double
    items = tuple({"x": i} for i in range(n))
    readers = list(target_fns)
    targets = [{"plan": _stream_map(target_fns[rid] or passthrough), "logical_node_id": "node-" + rid,
                "occurrence_id": rid + "-p0", "reader_id": rid} for rid in readers]
    spec = {
        "source": ItemsSource(items), "source_id": "u3-graph-source",
        "ancestor": {"plan": _stream_map(ancestor_fn),
                     "logical_node_id": "shared", "occurrence_id": "shared-p0"},
        "targets": targets, "routing": "broadcast", "exchange_mode": mode,
        "credit_budget": {"rows": 8, "bytes": 8 * 1024 ** 2},
        "queue_depth": 4096, "cut_rows": cut_rows,
    }
    if storage_bytes is not None:
        spec["storage_bytes"] = storage_bytes
    return spec, readers, n


def _committed_rows(run, action_id, rid, facts):
    from runtime_v3_graph_history import committed_rows
    return committed_rows(run, action_id, rid, facts)


def _release_crash_orphans(root):
    # Historical callers supply the old suffix; observe the actual original
    # driver-owned unit roots and allocator. Never manufacture release evidence.
    from runtime_v3_graph_history import assert_closed
    root = Path(root)
    assert root.name.endswith('-resources')
    return assert_closed(root.with_name(root.name[:-len('-resources')]), allow_unprovisioned=True)


# ---------------- exit gate 3: consumer crash, whole-graph restore ----------------

def test_consumer_crash_recovers_whole_graph_exactly_once(tmp_path):
    from runtime_v3_owned_crash import kill_owned_worker, assert_worker_killed
    from runtime_v3_graph_history import capture_acks, assert_acks_retained
    driver_pid = os.getpid()
    crash_marker = tmp_path / "target-crashed-once"

    def crashing_target(row):
        # Crash exactly one target once, on the first POST-cut row (x==16 doubled).
        # O_EXCL makes the marker atomic so racing broadcast targets crash only one.
        if row["x"] == 16:
            try:
                kill_owned_worker(crash_marker, driver_pid)
            except FileExistsError:
                return row
        return row

    spec, readers, n = _spec({"c0": crashing_target, "c1": None},
                             n=24, cut_rows=8)
    executor = LocalDatasetExecutor(workers=1, unified_graph=True)
    limits = {"checkpoint_rows": 8, "checkpoint_unconfirmed_rows": 8,
              "checkpoint_interval_ms": 60000}
    with pytest.raises((RecoveryContractError, OSError, DataflowError)) as failure:
        with RunContext.start(tmp_path / "run", limits=limits) as run:
            reference = run.ref
            with run.action({"kind": "materialize", "source": {"fixture": "u3-graph-consumer-fault"}}) as execution:
                executor.materialize_graph(spec)
                execution.complete(dict(kind="graph-materialized"))
    from runtime_v3_owned_crash import assert_worker_loss
    assert_worker_loss(failure.value)
    assert_worker_killed(crash_marker, tmp_path / 'run')
    original_acks = capture_acks(run, execution.action_id)

    with RunContext.resume(reference, limits=limits) as run2:
        with run2.action({"kind": "materialize", "source": {"fixture": "u3-graph-consumer-fault"}}) as execution:
            result = executor.materialize_graph(spec)
            action_id = execution.action_id
            execution.complete(dict(kind="graph-materialized", egress=result["egress_rows"]))
        facts = run2.journal.execution_commits(action_id)
        assert [f["sequence"] for f in facts] == list(range(1, len(facts) + 1))
        assert all(f["status"] == "acked" for f in facts)
        assert len({f["commit_id"] for f in facts}) == len(facts)
        assert_acks_retained(run2, action_id, original_acks)
        for rid in readers:
            rows = _committed_rows(run2, action_id, rid, facts)
            assert [r["x"] for r in rows] == [i * 2 for i in range(n)], rid
        with run2.journal._connection() as db:
            owner = db.execute("SELECT attempt,fence FROM runtime_owner").fetchone()
        assert owner == (run2.attempt, run2.fence)
    assert result["egress_rows"] == n
    _release_crash_orphans(tmp_path / "run-resources")


# ---------------- exit gate 2: stage failure publishes nothing ----------------

def test_stage_failure_seals_nothing(tmp_path):
    from runtime_v3_graph_history import committed_rows
    def failing_target(row):
        if row["x"] == 4:
            raise RuntimeError("target stage failed")
        return row

    spec, readers, n = _spec({"c0": failing_target, "c1": None},
                             n=24, cut_rows=8)
    limits = {"checkpoint_rows": 8, "checkpoint_unconfirmed_rows": 8,
              "checkpoint_interval_ms": 60000}
    # The failed cut cannot publish. Earlier pressure-cut ACKs remain genuine
    # incremental prefixes; the original business exception must remain visible.
    with pytest.raises(RuntimeError, match='target stage failed'):
        with RunContext.start(tmp_path / "run", limits=limits) as run:
            with run.action({"kind": "materialize", "source": {"fixture": "u3-graph-stage-fail"}}) as execution:
                LocalDatasetExecutor().materialize_graph(spec, control_timeout_s=10)
                execution.complete(dict(kind="graph-materialized"))
    journal_facts = run.journal.execution_commits("action-1")
    acked = [fact for fact in journal_facts if fact['status'] == 'acked']
    for rid in readers:
        rows = committed_rows(run, 'action-1', rid, acked, require_final=False)
        # Every published payload is a completed pre-failure business row, in
        # order. Neither the failing row (4) nor any later row may be published.
        assert [row['x'] for row in rows] == [0, 2][:len(rows)]
        assert len(rows) <= 2
    assert run.journal.execution_action('action-1')['result'] is None
    assert run.journal.execution_status()['status'] != 'succeeded'
    _release_crash_orphans(tmp_path / "run-resources")


# ---------------- exit gate 2: sealed storage insufficiency fails early ----------------

def test_sealed_storage_exhaustion_fails_early_without_publication(tmp_path):
    from runtime_v3_graph_history import assert_rejected_before_workers
    business_marker = tmp_path / 'business-started'
    def double(row):
        business_marker.write_text('called')
        return {**row, 'x': row['x'] * 2}

    # The original 1-byte total cannot admit every declared channel even at the
    # minimum 1 byte per edge. Admission must reject before starting the graph.
    spec, readers, n = _spec({"c0": None}, n=24, cut_rows=8,
                             mode="finite_sealed", storage_bytes=1, ancestor_fn=double)
    limits = {"checkpoint_unconfirmed_rows": 64, "checkpoint_interval_ms": 60000}
    start = time.monotonic()
    with pytest.raises(DataflowError, match='^BUDGET_EXCEEDED: graph storage budget cannot admit every declared channel$') as failure:
        with RunContext.start(tmp_path / "run", limits=limits) as run:
            with run.action({"kind": "materialize", "source": {"fixture": "u3-graph-sealed-exhaust"}}) as execution:
                LocalDatasetExecutor().materialize_graph(spec, control_timeout_s=8)
                execution.complete(dict(kind="graph-materialized"))
    assert time.monotonic() - start < 30, "sealed exhaustion deadlocked instead of failing early"
    assert failure.value.code == 'BUDGET_EXCEEDED'
    assert_rejected_before_workers(run, 'action-1', business_marker)
    _release_crash_orphans(tmp_path / "run-resources")


# ---------------- CUT: multi-input fan-in alignment + EOI empty barrier ----------------

