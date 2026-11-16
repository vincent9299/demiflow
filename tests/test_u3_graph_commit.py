"""U3 graph single-commit + full-coverage K6 acceptance (S4b, exit gates 2/3 Local).

The driver schedules global cuts; every worker contributes its K6 fragment and the
driver commits the whole group ONCE through a single CommitCoordinator on a graph
result child action, then acks. These tests prove: one durable acked commit per cut
(no worker owns commit authority), a full-coverage CheckpointManifest per cut
(source/channel/operator/sink participants all covered), committed staging blocks
read back verbatim with the shared expansion applied once, the final cut sealed,
and the empty-source / finite_sealed edges.
"""
import sys

import pytest

from demiflow.data.plan import CallableSpec, LogicalPlan, MapOp, MapStreamOptions
from demiflow.data.sources import ItemsSource
from demiflow.execution.block_sink import BlockSink
from demiflow.execution.contracts.checkpoint import validate_checkpoint_manifest
from demiflow.execution.executors.local import LocalDatasetExecutor
from demiflow.execution.run_context import RunAction, RunContext

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


def _graph_spec(n=24, cut_rows=8, mode="durable_incremental", readers=("c0", "c1", "c2", "c3", "c4", "c5")):
    def shared_double(row):
        return {**row, "x": row["x"] * 2}

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


def test_multi_cut_single_commit_full_k6(tmp_path):
    spec, readers, n = _graph_spec(n=24, cut_rows=8)
    executor = LocalDatasetExecutor(workers=16, unified_graph=True)
    with RunContext.start(tmp_path / "run",
                          limits={"memory_bytes": 16 * 1024 ** 3, "checkpoint_rows": 8, "checkpoint_unconfirmed_rows": 8,
                                  "checkpoint_interval_ms": 60000}) as run:
        with run.action({"kind": "materialize", "source": {"fixture": "u3-graph-commit"}}) as execution:
            result = executor.materialize_graph(spec)
            action_id = execution.action_id
            execution.complete(dict(kind="graph-materialized", egress=result["egress_rows"]))
        facts = run.journal.execution_commits(action_id + "-graph-result")
        # One durable commit per cut (8,16,24), each fully acked; workers never commit.
        assert [f["sequence"] for f in facts] == [1, 2, 3]
        assert all(f["status"] == "acked" for f in facts)
        assert all(set(f["intent"]["sinks"]) == set(readers) for f in facts)
        # Committed staging blocks read back verbatim; shared expansion applied once.
        for rid in readers:
            rows = _committed_rows(run, action_id, rid, facts)
            assert [r["x"] for r in rows] == [i * 2 for i in range(n)], rid
        with run.journal._connection() as db:
            owner = db.execute("SELECT attempt,fence FROM runtime_owner").fetchone()
        assert owner == (run.attempt, run.fence)

    assert result["egress_rows"] == n
    for rid in readers:
        assert result["target_rows"][rid] == n
    checkpoints = result["checkpoints"]
    assert len(checkpoints) == 3
    manifests = [c["manifest"] for c in checkpoints.values()]
    ordinals = sorted(m["source_progress"][0]["position"]["next_ordinal"] for m in manifests)
    assert ordinals == [8, 16, 24]
    sealed = [m for m in manifests if m["seal"]["sealed"]]
    assert len(sealed) == 1 and sealed[0]["source_progress"][0]["position"]["next_ordinal"] == n
    for m in manifests:
        validate_checkpoint_manifest(m)
        assert m["participants"]["sources"] == [{"source_id": "u3-graph-source", "partition_id": "p0"}]
        assert set(m["participants"]["sinks"]) == set(readers)
        assert len(m["participants"]["operators"]) == 7  # ancestor + six targets
        assert len(m["participants"]["channels"]) == 6
        assert {c["channel_id"] for c in m["channel_cuts"]} == set(m["participants"]["channels"])
        for cut in m["channel_cuts"]:
            assert set(cut["consumer_offsets"]) <= set(readers)
            assert cut["producer_cut_sequence"] >= 0
        assert {r["sink_id"] for r in m["sink_receipts"]} == set(readers)


def test_empty_source_sealed_cut(tmp_path):
    spec, readers, n = _graph_spec(n=0, cut_rows=8)
    executor = LocalDatasetExecutor(workers=16, unified_graph=True)
    with RunContext.start(tmp_path / "run",
                          limits={"memory_bytes": 16 * 1024 ** 3, "checkpoint_unconfirmed_rows": 8,
                                  "checkpoint_interval_ms": 60000}) as run:
        with run.action({"kind": "materialize", "source": {"fixture": "u3-graph-empty"}}) as execution:
            result = executor.materialize_graph(spec)
            action_id = execution.action_id
            execution.complete(dict(kind="graph-materialized", egress=result["egress_rows"]))
        facts = run.journal.execution_commits(action_id + "-graph-result")
        assert len(facts) == 1 and facts[0]["status"] == "acked"
        for rid in readers:
            assert _committed_rows(run, action_id, rid, facts) == []
    assert result["egress_rows"] == 0
    assert all(result["target_rows"][rid] == 0 for rid in readers)
    assert len(result["checkpoints"]) == 1
    manifest = next(iter(result["checkpoints"].values()))["manifest"]
    validate_checkpoint_manifest(manifest)
    assert manifest["seal"]["sealed"] is True
    assert manifest["source_progress"][0]["position"]["next_ordinal"] == 0


def test_finite_sealed_mode_commits(tmp_path):
    spec, readers, n = _graph_spec(n=12, cut_rows=12, mode="finite_sealed", readers=("c0", "c1"))
    # finite_sealed needs a storage budget on each output channel spec; the channel
    # plan derives it from the credit budget bytes when the orchestrator builds it.
    executor = LocalDatasetExecutor(workers=16, unified_graph=True)
    with RunContext.start(tmp_path / "run",
                          limits={"memory_bytes": 16 * 1024 ** 3, "checkpoint_unconfirmed_rows": 64,
                                  "checkpoint_interval_ms": 60000}) as run:
        with run.action({"kind": "materialize", "source": {"fixture": "u3-graph-sealed"}}) as execution:
            result = executor.materialize_graph(spec)
            action_id = execution.action_id
            execution.complete(dict(kind="graph-materialized", egress=result["egress_rows"]))
        facts = run.journal.execution_commits(action_id + "-graph-result")
        assert len(facts) == 1 and facts[0]["status"] == "acked"
        for rid in readers:
            rows = _committed_rows(run, action_id, rid, facts)
            assert [r["x"] for r in rows] == [i * 2 for i in range(n)]
    assert result["egress_rows"] == n
    manifest = next(iter(result["checkpoints"].values()))["manifest"]
    assert manifest["seal"]["sealed"] is True


def test_durable_graph_reclaims_channel_segments(tmp_path):
    import json
    spec, readers, n = _graph_spec(n=24, cut_rows=8, mode="durable_incremental", readers=("c0", "c1"))
    executor = LocalDatasetExecutor(workers=16, unified_graph=True)
    with RunContext.start(tmp_path / "run",
                          limits={"memory_bytes": 16 * 1024 ** 3, "checkpoint_rows": 8, "checkpoint_unconfirmed_rows": 8,
                                  "checkpoint_interval_ms": 60000}) as run:
        with run.action({"kind": "materialize", "source": {"fixture": "u3-graph-gc"}}) as execution:
            result = executor.materialize_graph(spec)
            action_id = execution.action_id
            execution.complete(dict(kind="graph-materialized", egress=result["egress_rows"]))
        facts = run.journal.execution_commits(action_id + "-graph-result")
        assert [f["sequence"] for f in facts] == [1, 2, 3]
        # Committed output (BlockSink) survives channel-segment GC verbatim.
        for rid in readers:
            rows = _committed_rows(run, action_id, rid, facts)
            assert [r["x"] for r in rows] == [i * 2 for i in range(n)]
        run_dir = run.directory
    # The ancestor wrote one channel segment per row (durable carriage, refs on the
    # wire) and reclaimed each once both readers consumed past it at a cut ack.
    evidence = run_dir / "gc-evidence.jsonl"
    assert evidence.exists(), "graph segment GC wrote no reclaim evidence"
    records = [json.loads(line) for line in evidence.read_text().splitlines()]
    assert len(records) == n
    assert len({r["segment_id"] for r in records}) == n
    assert all(r["reason"] == "safe-point" for r in records)
    # Each broadcast segment rode both reader channels; GC waited for both.
    assert all(len(r["channel_ranges"]) == 2 for r in records)
    # Terminate freed every channel segment; none linger after the run.
    assert not list((run_dir / "segments").rglob("*.pkl"))