"""U4/S3 state-adapter (STA) conformance: one recovery mainline for operator state.

Proves the S3 closed loop end to end rather than re-proving frozen units: a
stateful graph actor's snapshot rides worker StateArtifacts -> the existing
cut_contribution operator_states field (K6 shape, state_schema_version 1) -> the
driver's single CommitCoordinator -> the CheckpointManifest state_ref -> restore
refs handed back to the replacement worker. After a SIGKILL the restored actor
state equals the acked-cut snapshot exactly (output continuity proves restore_state
ran with the acked value), the acked prefix is never recomputed, and commit
sequences stay contiguous. The caller-side K2 precheck fires STATE_ADAPTER_MISSING
before any worker is provisioned, and every native {} adapter round-trips without
serializing resource handles as semantic state.
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
from demiflow.execution.contracts.errors import ProtocolError
from demiflow.execution.executors.local import LocalDatasetExecutor
from demiflow.execution.operator_precheck import precheck_operator_state, precheck_plan_state
from demiflow.execution.recovery_contract import RecoveryContractError
from demiflow.execution.run_context import RunAction, RunContext
from demiflow.execution.state_artifacts import StateArtifacts
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


def _make_counter(trace=None, crash_marker=None, crash_on=None):
    # Defined dynamically so cloudpickle stages the actor BY VALUE into the worker
    # host (a module-level test class would serialize by reference and the worker
    # could not import the test module). seen is semantic state; paths are config.
    class GraphCounter:
        def __init__(self):
            self.seen = []
            self.trace = trace
            self.crash_marker = crash_marker
            self.crash_on = crash_on

        def __call__(self, row):
            if self.trace is not None:
                with open(self.trace, "a") as stream:
                    stream.write(str(row["x"]) + "\n")
            if (self.crash_on is not None and row["x"] == self.crash_on
                    and self.crash_marker is not None and not os.path.exists(self.crash_marker)):
                Path(self.crash_marker).touch()
                os.kill(os.getpid(), signal.SIGKILL)
            self.seen.append(row["x"])
            return {**row, "x": row["x"] * 2, "seen_count": len(self.seen)}

        def snapshot_state(self):
            return {"seen": list(self.seen)}

        def restore_state(self, state):
            self.seen = list(state["seen"])

    return GraphCounter()


def _stream_map(fn):
    return LogicalPlan((MapOp(CallableSpec.create(fn),
                              stream_options=MapStreamOptions(concurrency=1)),))


def _passthrough(row):
    return row


def _graph_spec(actor, n=24, cut_rows=8, readers=("c0", "c1")):
    # Local closure: cloudpickle serializes it by value so the independent worker
    # processes need not import this test module (same pattern as U3 graph tests).
    def passthrough(row):
        return row

    items = tuple({"x": i} for i in range(n))
    targets = [{"plan": _stream_map(passthrough), "logical_node_id": "node-" + rid,
                "occurrence_id": rid + "-p0", "reader_id": rid} for rid in readers]
    spec = {
        "source": ItemsSource(items), "source_id": "sta-graph-source",
        "ancestor": {"plan": _stream_map(actor),
                     "logical_node_id": "shared", "occurrence_id": "shared-p0"},
        "targets": targets, "routing": "broadcast", "exchange_mode": "durable_incremental",
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
            os.waitpid(evidence["identity"]["pid"], os.WNOHANG)
        except (ChildProcessError, OSError):
            pass
    assert pool.snapshot()["charged"] == {"cpu_millis": 0, "memory_bytes": 0}


def test_graph_stateful_actor_state_closed_loop(tmp_path):
    spec, readers, n = _graph_spec(_make_counter(), n=24, cut_rows=8)
    executor = LocalDatasetExecutor(workers=1, unified_graph=True)
    limits = {"checkpoint_rows": 8, "checkpoint_unconfirmed_rows": 8,
              "checkpoint_interval_ms": 60000}
    with RunContext.start(tmp_path / "run", limits=limits) as run:
        with run.action({"kind": "materialize", "source": {"fixture": "sta-graph-state"}}) as execution:
            result = executor.materialize_graph(spec)
            action_id = execution.action_id
            execution.complete(dict(kind="graph-materialized", egress=result["egress_rows"]))
        facts = run.journal.execution_commits(action_id + "-graph-result")
        assert [f["sequence"] for f in facts] == [1, 2, 3]
        assert all(f["status"] == "acked" for f in facts)
        # Each cut persists exactly the ancestor's acked snapshot under its own
        # occurrence key (K6 state_ref, schema 1); stateless targets contribute none.
        by_ordinal = {cp["manifest"]["source_progress"][0]["position"]["next_ordinal"]: cp
                      for cp in result["checkpoints"].values()}
        assert sorted(by_ordinal) == [8, 16, 24]
        for ordinal in (8, 16, 24):
            entries = by_ordinal[ordinal]["manifest"]["operator_states"]
            assert len(entries) == 1
            entry = entries[0]
            assert entry["logical_node_id"] == "shared"
            assert entry["occurrence_id"] == "shared-p0"
            assert entry["state_schema_version"] == 1
            assert set(entry["state_ref"]) == {"state_id", "path", "bytes", "sha256"}
            value = StateArtifacts(run).read(entry["state_ref"])
            assert value == {"node-0-worker-0": {"seen": list(range(ordinal))}}
        for rid in readers:
            rows = _committed_rows(run, action_id, rid, facts)
            assert [r["x"] for r in rows] == [i * 2 for i in range(n)], rid
            assert [r["seen_count"] for r in rows] == list(range(1, n + 1)), rid
    assert result["egress_rows"] == n


def test_graph_stateful_actor_kill_recovers_state_exactly(tmp_path):
    crash_marker = str(tmp_path / "crashed-once")
    trace = str(tmp_path / "ancestor-trace")
    actor = _make_counter(trace=trace, crash_marker=crash_marker, crash_on=8)
    spec, readers, n = _graph_spec(actor, n=24, cut_rows=8)
    executor = LocalDatasetExecutor(workers=1, unified_graph=True)
    limits = {"checkpoint_rows": 8, "checkpoint_unconfirmed_rows": 8,
              "checkpoint_interval_ms": 60000}
    # The ancestor SIGKILLs itself on the first post-cut row (x==8), strictly after
    # cut 8 is durable (unconfirmed_rows==8 blocks the feed until the driver acks).
    with pytest.raises((RecoveryContractError, OSError)):
        with RunContext.start(tmp_path / "run", limits=limits) as run:
            reference = run.ref
            with run.action({"kind": "materialize", "source": {"fixture": "sta-graph-kill"}}) as execution:
                executor.materialize_graph(spec)
                execution.complete(dict(kind="graph-materialized"))
    assert os.path.exists(crash_marker), "the ancestor crash window did not fire"

    with RunContext.resume(reference, limits=limits) as run2:
        assert run2.attempt == 2
        with run2.action({"kind": "materialize", "source": {"fixture": "sta-graph-kill"}}) as execution:
            result = executor.materialize_graph(spec)
            action_id = execution.action_id
            execution.complete(dict(kind="graph-materialized", egress=result["egress_rows"]))
        facts = run2.journal.execution_commits(action_id + "-graph-result")
        assert [f["sequence"] for f in facts] == [1, 2, 3]
        assert all(f["status"] == "acked" for f in facts)
        assert len({f["commit_id"] for f in facts}) == len(facts)
        # State full equality: the acked cut-8 artifact is the exact pre-crash snapshot.
        # Cut 8 was committed in attempt 1; the resumed terminal only schedules cuts
        # strictly after the acked ordinal, so read its committed state ref from the
        # recovered journal fact (the single commit authority), not this attempt's cuts.
        cut8 = facts[0]
        assert cut8["intent"]["position"]["next_ordinal"] == 8
        assert StateArtifacts(run2).read(cut8["prepared"]["state"]["shared-p0"]) == {
            "node-0-worker-0": {"seen": list(range(8))}}
        # Output continuity proves restore_state ran with the acked value: seen_count
        # resumes at 9 for x==8 (not 1), so the restored actor kept seen=[0..7].
        for rid in readers:
            rows = _committed_rows(run2, action_id, rid, facts)
            assert [r["x"] for r in rows] == [i * 2 for i in range(n)], rid
            assert [r["seen_count"] for r in rows] == list(range(1, n + 1)), rid
    assert result["egress_rows"] == n
    # The acked prefix (x==0..7) ran once; the crashed post-cut row (x==8) recomputes
    # once; every later row runs once on the restored generation.
    counts = Counter(int(line) for line in Path(trace).read_text().splitlines())
    assert counts[8] == 2
    for i in range(n):
        if i != 8:
            assert counts[i] == 1, (i, counts[i])
    _release_crash_orphans(tmp_path / "run-resources")


# --- Caller-side K2 precheck firing (authorization boundary) --------------------

class _StatefulNoAdapter:
    def __init__(self):
        self.value = 0

    def __call__(self, row):
        self.value += 1
        return row


class _AdapterActor:
    def __init__(self):
        self.value = 0

    def __call__(self, row):
        self.value += 1
        return row

    def snapshot_state(self):
        return {"value": self.value}

    def restore_state(self, state):
        self.value = state["value"]


def test_precheck_rejects_stateful_owner_without_adapter():
    spec = CallableSpec.create(_StatefulNoAdapter())
    with pytest.raises(ProtocolError) as error:
        precheck_operator_state(spec, {"state_ownership": "partitionable"})
    assert error.value.code == "STATE_ADAPTER_MISSING"


def test_precheck_rejects_ownership_none_instance_carrying_state():
    spec = CallableSpec.create(_StatefulNoAdapter())
    with pytest.raises(ProtocolError) as error:
        precheck_operator_state(spec, {"state_ownership": "none"})
    assert error.value.code == "STATE_ADAPTER_MISSING"


def test_precheck_accepts_stateless_function_and_adapter_actor():
    assert precheck_operator_state(CallableSpec.create(_passthrough),
                                   {"state_ownership": "none"}) is False
    assert precheck_operator_state(CallableSpec.create(_AdapterActor()),
                                   {"state_ownership": "partitionable"}) is True
    # An adapter-present instance is accepted even under ownership none.
    assert precheck_operator_state(CallableSpec.create(_AdapterActor()),
                                   {"state_ownership": "none"}) is True


def test_precheck_plan_fires_before_stream_materialization(tmp_path):
    from demiflow import data
    with pytest.raises(ProtocolError) as error:
        with RunContext.start(tmp_path / "run"):
            # Plain JSON state at concurrency=1 is now auto-adapted. Sharing one
            # mutable instance concurrently still needs an explicit adapter.
            data.from_items([{"id": 1}]).map_async(_StatefulNoAdapter(), concurrency=2).materialize()
    assert error.value.code == "STATE_ADAPTER_MISSING"


def test_precheck_plan_accepts_adapter_actor_stream(tmp_path):
    from demiflow import data
    with RunContext.start(tmp_path / "run", limits={"checkpoint_rows": 2,
                                                   "checkpoint_interval_ms": 60000}):
        result = data.from_items([{"id": i} for i in range(6)]).map_async(
            _AdapterActor(), concurrency=1).materialize().take_all()
    assert sorted(result, key=lambda r: r["id"]) == [{"id": i} for i in range(6)]


# --- Native {} adapter round-trips (no resource handle serialized as state) -----

def _bare(cls, **attrs):
    instance = object.__new__(cls)
    for key, value in attrs.items():
        setattr(instance, key, value)
    return instance


@pytest.mark.parametrize("factory", [
    lambda: _bare(__import__("demiflow.objects", fromlist=["ReadLocalObject"]).ReadLocalObject),
    lambda: _bare(__import__("demiflow.collect.wiki_syntax", fromlist=["ParseWikitext"]).ParseWikitext),
    lambda: _bare(__import__("demiflow.collect.documents", fromlist=["DecodeDocument"]).DecodeDocument),
    lambda: _bare(__import__("demiflow.collect.image_resize", fromlist=["EncodeImage"]).EncodeImage),
    lambda: _bare(__import__("demiflow.collect.operators", fromlist=["SearchWeb"]).SearchWeb),
    lambda: _bare(__import__("demiflow.execution.raw_fetch", fromlist=["RawFetch"]).RawFetch),
    lambda: _bare(__import__("demiflow.execution.queue_producer", fromlist=["QueueProducer"]).QueueProducer),
    lambda: _bare(__import__("demiflow.operator_llm.runtime", fromlist=["PromptActor"]).PromptActor),
    lambda: _bare(__import__("demiflow.embeddings.runtime", fromlist=["EmbeddingActor"]).EmbeddingActor),
    lambda: _bare(__import__("demiflow.execution.stream_lookup", fromlist=["LanceLookup"]).LanceLookup,
                  cache=__import__("collections").OrderedDict(), cache_bytes=0),
])
def test_native_adapter_snapshot_restore_round_trip(factory):
    actor = factory()
    snapshot = actor.snapshot_state()
    assert snapshot == {}
    actor.restore_state(snapshot)
    actor.restore_state({})
    with pytest.raises((ValueError, RecoveryContractError)):
        actor.restore_state({"semantic": "state"})


def test_embedding_adapter_declares_empty_semantic_state():
    from demiflow.embeddings.runtime import EmbeddingActor
    actor = _bare(EmbeddingActor)
    assert actor.snapshot_state() == {}
    actor.restore_state({})
    with pytest.raises(ValueError, match="no mutable semantic state"):
        actor.restore_state({"calls": 1})
