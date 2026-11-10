"""U3 Ray Core graph carriage + SEM + fault matrix (S5, exit gate 3 Ray).

Single-host Ray Core (env-ray, ray 2.60.0 Core only) is semantic-adaptation
evidence only (design §8): no multi-host/GPU/performance claim. The whole graph
runs in independent named Ray actor hosts hosting the *same* WorkerRuntime +
GraphWorkerExecutor + GraphWorkerProgress as the Local process graph worker; the
driver owns the control plane, the fixed source capture and the single commit
authority (GraphCommitTerminal). The data plane is the Ray carriage: one async
channel actor per physical channel (actor-method frames), ray_object plasma for
large payloads — business rows flow worker->worker and never transit the driver.

SEM proves the same graph/source yields an identical output multiset, record
identity, commit sequence/position progression, K6 manifest shape and runtime_owner
across Local and Ray. The Ray fault matrix covers producer/consumer actor death
(whole-graph restore from the acked cut, exactly once) and sealed storage early
failure with no unauthorized publication.
"""
import json
import os
import signal
import sys
import time
from pathlib import Path

import pytest

ray = pytest.importorskip("ray")
from ray.exceptions import RayActorError

from demiflow.data.plan import CallableSpec, LogicalPlan, MapOp, MapStreamOptions
from demiflow.data.sources import ItemsSource
from demiflow.execution.block_sink import BlockSink
from demiflow.execution.contracts.checkpoint import validate_checkpoint_manifest
from demiflow.execution.executors.local import LocalDatasetExecutor
from demiflow.execution.executors.ray import RayDatasetExecutor
from demiflow.execution.recovery_contract import RecoveryContractError
from demiflow.execution.run_context import RunAction, RunContext

pytestmark = [
    pytest.mark.ray,
    pytest.mark.skipif(not sys.platform.startswith("linux"),
                       reason="managed units require Linux"),
]


@pytest.fixture(scope="module")
def ray_cluster():
    # Logical scheduling tokens only (single-host semantic evidence); channel actors
    # use num_cpus=0, each worker host reserves 1 CPU.
    ray.init(num_cpus=8, include_dashboard=False, ignore_reinit_error=True, logging_level="ERROR")
    try:
        yield
    finally:
        ray.shutdown()


def _stream_map(fn):
    return LogicalPlan((MapOp(CallableSpec.create(fn),
                                stream_options=MapStreamOptions(concurrency=1)),))


def _graph_spec(n=12, cut_rows=4, mode="memory_incremental", readers=("c0", "c1"),
                ancestor_fn=None, target_fns=None):
    # Nested closures: cloudpickle serializes by value so neither the Local worker
    # process nor the Ray actor need import this test module.
    def shared_double(row):
        return {**row, "x": row["x"] * 2}

    def passthrough(row):
        return row

    items = tuple({"x": i} for i in range(n))
    targets = []
    for rid in readers:
        fn = (target_fns or {}).get(rid, passthrough)
        targets.append({"plan": _stream_map(fn), "logical_node_id": "node-" + rid,
                        "occurrence_id": rid + "-p0", "reader_id": rid})
    spec = {
        "source": ItemsSource(items), "source_id": "u3-graph-source",
        "ancestor": {"plan": _stream_map(ancestor_fn or shared_double),
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


def _commit_shape(run, action_id):
    facts = run.journal.execution_commits(action_id + "-graph-result")
    return {"sequences": [f["sequence"] for f in facts],
            "positions": [f["intent"]["position"]["next_ordinal"] for f in facts],
            "statuses": [f["status"] for f in facts],
            "commit_ids": [f["commit_id"] for f in facts]}


def _k6_shape(result):
    shapes = []
    for entry in result["checkpoints"].values():
        m = entry["manifest"]
        shapes.append({"participants": {k: len(v) for k, v in m["participants"].items()},
                       "source_progress_components": len(m["source_progress"]),
                       "channel_cuts": len(m["channel_cuts"]),
                       "operator_states": len(m["operator_states"]),
                       "seal_keys": sorted(m["seal"].keys()), "sealed": m["seal"]["sealed"]})
    return sorted(shapes, key=lambda s: (s["sealed"], s["channel_cuts"]))


def _owner(run):
    with run.journal._connection() as db:
        return db.execute("SELECT attempt,fence FROM runtime_owner").fetchone()


# ---------------- basic Ray graph: real actor hosts, single commit ----------------

def test_ray_graph_two_targets_shared_egress_once(tmp_path, ray_cluster):
    spec, readers, n = _graph_spec(n=12, cut_rows=4)
    executor = RayDatasetExecutor(unified_graph=True)
    with RunContext.start(tmp_path / "run",
                          limits={"checkpoint_rows": 4, "checkpoint_unconfirmed_rows": 4,
                                  "checkpoint_interval_ms": 60000}) as run:
        with run.action({"kind": "materialize", "source": {"fixture": "u3-ray-graph"}}) as execution:
            result = executor.materialize_graph(spec)
            action_id = execution.action_id
            execution.complete(dict(kind="graph-materialized", workers=result["workers"],
                                    egress=result["egress_rows"]))
        facts = run.journal.execution_commits(action_id + "-graph-result")
        assert [f["sequence"] for f in facts] == [1, 2, 3]
        assert all(f["status"] == "acked" for f in facts)
        for rid in readers:
            rows = _committed_rows(run, action_id, rid, facts)
            assert [r["x"] for r in rows] == [i * 2 for i in range(n)], rid
        assert _owner(run) == (run.attempt, run.fence)
    assert result["workers"] == 3
    assert result["egress_rows"] == n  # shared expansion once, not 2n
    for rid in readers:
        assert result["target_rows"][rid] == n
    assert all(result["final_arrival"][rid] is not None for rid in ["ancestor", *readers])


# ---------------- SEM: same graph/source, Local vs Ray, identical semantics ----------------

def test_sem_local_ray_graph_equivalence(tmp_path, ray_cluster):
    spec, readers, n = _graph_spec(n=12, cut_rows=4)
    limits = {"checkpoint_rows": 4, "checkpoint_unconfirmed_rows": 4, "checkpoint_interval_ms": 60000}

    local_executor = LocalDatasetExecutor(workers=1, unified_graph=True)
    with RunContext.start(tmp_path / "local-run", limits=limits) as local_run:
        with local_run.action({"kind": "materialize", "source": {"fixture": "u3-sem"}}) as execution:
            local_result = local_executor.materialize_graph(spec)
            local_action = execution.action_id
            execution.complete(dict(kind="graph-materialized"))
        local_facts = local_run.journal.execution_commits(local_action + "-graph-result")
        local_rows = {rid: _committed_rows(local_run, local_action, rid, local_facts) for rid in readers}
        local_commits = _commit_shape(local_run, local_action)
        local_owner = _owner(local_run)
        assert local_owner == (local_run.attempt, local_run.fence)
    local_k6 = _k6_shape(local_result)

    ray_executor = RayDatasetExecutor(unified_graph=True)
    with RunContext.start(tmp_path / "ray-run", limits=limits) as ray_run:
        with ray_run.action({"kind": "materialize", "source": {"fixture": "u3-sem"}}) as execution:
            ray_result = ray_executor.materialize_graph(spec)
            ray_action = execution.action_id
            execution.complete(dict(kind="graph-materialized"))
        ray_facts = ray_run.journal.execution_commits(ray_action + "-graph-result")
        ray_rows = {rid: _committed_rows(ray_run, ray_action, rid, ray_facts) for rid in readers}
        ray_commits = _commit_shape(ray_run, ray_action)
        ray_owner = _owner(ray_run)
        assert ray_owner == (ray_run.attempt, ray_run.fence)
    ray_k6 = _k6_shape(ray_result)

    expected = [{"x": i * 2} for i in range(n)]
    for rid in readers:
        assert local_rows[rid] == expected
        assert ray_rows[rid] == expected
        # Output multiset + record identity (x value) identical across providers.
        assert sorted(local_rows[rid], key=lambda r: r["x"]) == sorted(ray_rows[rid], key=lambda r: r["x"])
    # Commit sequence/position progression identical (same checkpoint semantics).
    assert local_commits["sequences"] == ray_commits["sequences"] == [1, 2, 3]
    assert local_commits["positions"] == ray_commits["positions"] == [4, 8, 12]
    assert local_commits["statuses"] == ray_commits["statuses"] == ["acked"] * 3
    assert len(set(local_commits["commit_ids"])) == 3
    assert len(set(ray_commits["commit_ids"])) == 3
    # K6 manifest shape identical (participants/source-progress/channel cuts/seal).
    assert local_k6 == ray_k6
    assert local_k6
    assert sum(1 for s in local_k6 if s["sealed"]) == 1
    assert local_result["egress_rows"] == ray_result["egress_rows"] == n


# ---------------- ray_object large payload: ref on the wire, consumer ray.get direct ----------------

def test_ray_object_large_payload_direct_read_not_inline(tmp_path, ray_cluster):
    from demiflow.execution.exchange.carriage_ray import RayObjectStore, ref_records, expand_records
    store = RayObjectStore(tmp_path / "producer", host_id="h")
    big_rows = [{"v": i, "blob": "x" * 4096} for i in range(8)]
    descriptor, refs = store.write(big_rows, scope_ref="occ")
    assert refs[0]["kind"] == "ray_object" and refs[0]["durability"] == "ephemeral"
    records = ref_records(refs, occurrence_id="occ")
    # The wire record carries only the ref (payload is the DataRef, not the rows):
    # the large business payload never rides the channel frame / driver.
    assert records[0]["payload"] == refs[0]
    assert records[0]["size_bytes"] == refs[0]["bytes"]
    assert "blob" not in json.dumps(records[0]["payload"])
    # A fresh consumer store sharing the producer's read domain (same run directory,
    # as ancestor/targets share in the graph) but with an EMPTY pin dict: it must
    # reconstruct the ObjectRef from its hex location and ray.get the same-host plasma
    # directly, never relying on the producer's in-memory pins or the driver.
    consumer = RayObjectStore(tmp_path / "producer", host_id="h")
    assert consumer.domain == store.domain
    assert consumer._objects == {}
    assert expand_records(records, consumer) == big_rows
    # Tampered digest is rejected before unpickling.
    from demiflow.execution.contracts.errors import ProtocolError
    bad = dict(refs[0]); bad["sha256"] = "0" * 64
    bad["ref_id"] = __import__("demiflow.execution.contracts.exchange", fromlist=["data_ref_id"]).data_ref_id(
        bad["read_domain"], bad["location"], bad["sha256"])
    with pytest.raises(ProtocolError):
        consumer.read([bad])


# ---------------- Ray fault matrix: actor death -> whole-graph restore exactly once ----------------

def test_ray_graph_producer_death_recovers_acked_prefix_exactly_once(tmp_path, ray_cluster):
    from collections import Counter
    from demiflow.execution.graph import run_ray_graph
    crash_marker = tmp_path / "ancestor-crashed-once"
    trace = tmp_path / "ancestor-trace"

    def crashing_ancestor(row):
        # Trace each processed row, then kill the actor process once on the first
        # POST-cut row (x==8). unconfirmed_rows==8 blocks the feed at the cut-8 bound
        # until the driver acks, so the death lands strictly after cut 8 is durable.
        with open(trace, "a") as stream:
            stream.write(str(row["x"]) + "\n")
        if row["x"] == 8 and not crash_marker.exists():
            crash_marker.touch()
            os._exit(17)  # abrupt actor death; max_restarts=0 never auto-replays
        return {**row, "x": row["x"] * 2}

    spec, readers, n = _graph_spec(n=24, cut_rows=8, ancestor_fn=crashing_ancestor)
    limits = {"checkpoint_rows": 8, "checkpoint_unconfirmed_rows": 8, "checkpoint_interval_ms": 60000}
    with pytest.raises((RecoveryContractError, RayActorError, OSError)):
        with RunContext.start(tmp_path / "run", limits=limits) as run:
            reference = run.ref
            with run.action({"kind": "materialize", "source": {"fixture": "u3-ray-producer-fault"}}):
                run_ray_graph(spec)
    assert crash_marker.exists(), "the producer death window did not fire"

    executor = RayDatasetExecutor(unified_graph=True)
    with RunContext.resume(reference, limits=limits) as run2:
        assert run2.attempt == 2
        with run2.action({"kind": "materialize", "source": {"fixture": "u3-ray-producer-fault"}}) as execution:
            result = executor.materialize_graph(spec)
            action_id = execution.action_id
            execution.complete(dict(kind="graph-materialized", egress=result["egress_rows"]))
        facts = run2.journal.execution_commits(action_id + "-graph-result")
        assert [f["sequence"] for f in facts] == [1, 2, 3]  # contiguous across the fault
        assert all(f["status"] == "acked" for f in facts)
        assert len({f["commit_id"] for f in facts}) == len(facts)
        for rid in readers:
            rows = _committed_rows(run2, action_id, rid, facts)
            assert [r["x"] for r in rows] == [i * 2 for i in range(n)], rid
        assert _owner(run2) == (run2.attempt, run2.fence)
    assert result["egress_rows"] == n
    # The acked prefix (x==0..7) ran once; the crashed post-cut row (x==8) recomputes
    # once on restore; every later row runs once. No duplication of the acked prefix.
    counts = Counter(int(line) for line in trace.read_text().splitlines())
    assert counts[8] == 2
    for i in range(n):
        if i != 8:
            assert counts[i] == 1, (i, counts[i])


def test_ray_graph_consumer_death_recovers_whole_graph_exactly_once(tmp_path, ray_cluster):
    from demiflow.execution.graph import run_ray_graph
    crash_marker = tmp_path / "target-crashed-once"

    def crashing_target(row):
        # Kill exactly one target actor once, on the first POST-cut row (x==16 doubled
        # == input 8). O_EXCL makes the marker atomic so racing broadcast targets kill
        # only one; cut 8 is already durable when the death lands.
        if row["x"] == 16:
            try:
                fd = os.open(crash_marker, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600)
                os.close(fd)
            except FileExistsError:
                return row
            os._exit(17)
        return row

    spec, readers, n = _graph_spec(n=24, cut_rows=8, target_fns={"c0": crashing_target})
    limits = {"checkpoint_rows": 8, "checkpoint_unconfirmed_rows": 8, "checkpoint_interval_ms": 60000}
    with pytest.raises((RecoveryContractError, RayActorError, OSError)):
        with RunContext.start(tmp_path / "run", limits=limits) as run:
            reference = run.ref
            with run.action({"kind": "materialize", "source": {"fixture": "u3-ray-consumer-fault"}}):
                run_ray_graph(spec)
    assert crash_marker.exists(), "the consumer death window did not fire"

    executor = RayDatasetExecutor(unified_graph=True)
    with RunContext.resume(reference, limits=limits) as run2:
        with run2.action({"kind": "materialize", "source": {"fixture": "u3-ray-consumer-fault"}}) as execution:
            result = executor.materialize_graph(spec)
            action_id = execution.action_id
            execution.complete(dict(kind="graph-materialized", egress=result["egress_rows"]))
        facts = run2.journal.execution_commits(action_id + "-graph-result")
        assert [f["sequence"] for f in facts] == [1, 2, 3]
        assert all(f["status"] == "acked" for f in facts)
        assert len({f["commit_id"] for f in facts}) == len(facts)
        for rid in readers:
            rows = _committed_rows(run2, action_id, rid, facts)
            assert [r["x"] for r in rows] == [i * 2 for i in range(n)], rid
        assert _owner(run2) == (run2.attempt, run2.fence)
    assert result["egress_rows"] == n


# ---------------- Ray fault matrix: sealed storage insufficiency fails early ----------------

def test_ray_graph_sealed_storage_exhaustion_fails_early_without_publication(tmp_path, ray_cluster):
    from demiflow.execution.graph import run_ray_graph
    # finite_sealed charges a bounded storage reservation; a 1-byte budget cannot hold
    # even one segment, so the producer raises non-retriable QUOTA_EXCEEDED immediately
    # (no credit deadlock). The Ray graph run fails fast and publishes nothing.
    spec, readers, n = _graph_spec(n=24, cut_rows=8, mode="finite_sealed", readers=("c0",))
    spec["storage_bytes"] = 1
    limits = {"checkpoint_unconfirmed_rows": 64, "checkpoint_interval_ms": 60000}
    start = time.monotonic()
    with pytest.raises((RecoveryContractError, RayActorError, OSError)):
        with RunContext.start(tmp_path / "run", limits=limits) as run:
            with run.action({"kind": "materialize", "source": {"fixture": "u3-ray-sealed-exhaust"}}):
                run_ray_graph(spec, control_timeout_s=10)
    assert time.monotonic() - start < 40, "sealed exhaustion deadlocked instead of failing early"
    facts = run.journal.execution_commits("action-1-graph-result")
    assert all(f["status"] != "acked" for f in facts)