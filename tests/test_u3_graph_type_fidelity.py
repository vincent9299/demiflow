"""#32 regression lock: graph DEFAULT memory_incremental preserves business-row types.

The default graph exchange once rode the strict-JSON inline carriage, silently
coercing tuple->list and hard-rejecting bytes (ancestor ``document()`` validation),
diverging from the durable/sealed pickle-4 segment carriage. Option B routes the
default through the SAME segment carriage (ephemeral durability per its declared
policy), so tuple/bytes survive end-to-end. This locks the fix (criterion-4
same-semantics-multi-carrier / F5 core-semantics) against regression: real Local
graph workers, default mode, committed egress rows read back through BlockSink at
both broadcast targets.

Env: run under the standard suite invocation (cwd=repo, PYTHONPATH=repo:repo/tests)
so spawned worker hosts (``-m demiflow.execution.worker.bootstrap``) inherit the
engine code — the #32 probe lesson. No internal env pin / manual wrapper, mirroring
the sibling graph tests (test_u3_graph_local.py).
"""
import os
import sys
from pathlib import Path

import pytest

from demiflow.data.plan import CallableSpec, LogicalPlan, MapOp, MapStreamOptions
from demiflow.data.sources import ItemsSource
from demiflow.execution.block_sink import BlockSink
from demiflow.execution.graph import run_local_graph
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


def _spec(n, cut_rows, special):
    # Local closures: cloudpickle serializes them by value into the independent
    # worker hosts, so the workers need not import this test module.
    def emit(row):
        out = {**row, "x": row["x"] * 2}
        if special == "tuple":
            out["t"] = (row["x"], row["x"] + 1)
        elif special == "bytes":
            out["b"] = bytes([row["x"] % 256])
        return out

    def passthrough(row):
        return row

    items = tuple({"x": i} for i in range(n))
    readers = ["c0", "c1"]
    targets = [{"plan": _stream_map(passthrough), "logical_node_id": "node-" + rid,
                "occurrence_id": rid + "-p0", "reader_id": rid} for rid in readers]
    spec = {"source": ItemsSource(items), "source_id": "type-fidelity-source",
            "ancestor": {"plan": _stream_map(emit),
                         "logical_node_id": "shared", "occurrence_id": "shared-p0"},
            "targets": targets, "routing": "broadcast",
            "exchange_mode": "memory_incremental",
            "credit_budget": {"rows": 8, "bytes": 8 * 1024 ** 2},
            "queue_depth": 4096, "cut_rows": cut_rows}
    return spec, readers


def _committed_rows(run, action_id, rid, facts):
    sink = BlockSink(RunAction(run, action_id + "-graph-" + rid, {"kind": "graph-node"}), rid)
    rows = []
    for fact in facts:
        rows.extend(sink.iter_rows(fact["receipts"][rid]["data"]))
    return rows


def _release(root):
    # Reclaim only this test's own graph workers; assert zero charged resources.
    root = Path(root)
    if not root.exists():
        return
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
            os.waitpid(evidence["identity"]["pid"], os.WNOHANG)
        except (ChildProcessError, OSError):
            pass
    assert pool.snapshot()["charged"] == {"cpu_millis": 0, "memory_bytes": 0}


def _run_default_graph(tmp_path, special, n=8, cut_rows=4):
    spec, readers = _spec(n, cut_rows, special)
    limits = {"checkpoint_rows": cut_rows, "checkpoint_unconfirmed_rows": cut_rows,
              "checkpoint_interval_ms": 60000}
    run_dir = tmp_path / "run"
    try:
        with RunContext.start(run_dir, limits=limits) as run:
            with run.action({"kind": "materialize",
                             "source": {"fixture": "type-fidelity"}}) as execution:
                action_id = execution.action_id
                result = run_local_graph(spec, control_timeout_s=15)
                execution.complete(dict(kind="graph-materialized"))
            facts = run.journal.execution_commits(action_id + "-graph-result")
            rows = {rid: _committed_rows(run, action_id, rid, facts) for rid in readers}
        return rows, result["egress_rows"], readers
    finally:
        _release(run_dir.with_name(run_dir.name + "-resources"))


@pytest.mark.parametrize("special", ["tuple", "bytes"])
def test_default_graph_preserves_business_row_types(tmp_path, special):
    n = 8
    # A bytes row was hard-rejected by the old strict-JSON default path; run_local_graph
    # raising here would fail the test, so success itself is part of the assertion.
    rows, egress, readers = _run_default_graph(tmp_path, special, n=n, cut_rows=4)
    assert egress == n
    field = "t" if special == "tuple" else "b"
    expected = ([(i, i + 1) for i in range(n)] if special == "tuple"
                else [bytes([i % 256]) for i in range(n)])
    for rid in readers:
        committed = rows[rid]
        assert len(committed) == n
        # Type preserved (tuple stays tuple, bytes stays bytes) — not coerced to list.
        assert all(type(r[field]) is type(expected[0]) for r in committed)
        # Value preserved end-to-end at both broadcast targets.
        assert [r[field] for r in committed] == expected