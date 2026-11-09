"""U2 CMT conformance: the driver commit terminal and CommitCut/CommitReceipt.

The driver is the single result-commit authority (D). A worker stages result
blocks under the granted fence and requests a durable commit; the driver re-reads
that descriptor (no re-write), commits through the existing CommitCoordinator,
records the K6 checkpoint manifest and replies. These tests simulate the worker
staging descriptor in-process and prove: exactly-once publication, idempotent
retry / differing-intent rejection, K6 participant coverage, the single-source
degenerate vector, fault-window recovery before any recompute, and that unknown
paid effects are never auto-replayed. No GPU/multi-host claims.
"""
import uuid
from types import SimpleNamespace

import pytest

from demiflow.execution import commit as commit_module
from demiflow.execution.block_sink import BlockSink
from demiflow.execution.chain import (
    DriverStreamTerminal, build_commit_cut, build_commit_receipt,
    validate_commit_cut, validate_commit_receipt,
)
from demiflow.execution.contracts.checkpoint import (
    legacy_checkpoint_fragment, replayable_effects,
)
from demiflow.execution.recovery_contract import RecoveryContractError, identity
from demiflow.execution.run_context import RunContext, RunAction


@pytest.fixture
def env(tmp_path):
    with RunContext.start(tmp_path / "run") as run:
        declaration = {"kind": "chain-materialize", "source": {"source_id": "fixed"}}
        run.journal.bind_execution_action(run.fence, "main-action", declaration)
        action = RunAction(run, "main-action", declaration)
        source_id = identity(["test-source"])
        channel_id = identity(["result-out-channel"])
        output_child = "main-action-chain"
        topology = {
            "run_id": run.ref.run_id, "action_id": output_child, "epoch": run.attempt,
            "topology_revision": 0, "route_epoch": 0, "plan_revision": 0,
            "artifact_manifest_ref": {"manifest_id": "a" * 64, "sha256": "b" * 64},
            "source_id": source_id, "channel_id": channel_id,
            "operators": [{"logical_node_id": identity(["node-0"]),
                           "occurrence_id": identity(["occ-0"])}],
            "sink_id": "output",
        }
        terminal = DriverStreamTerminal(action, {"source_id": source_id, "data": {}},
                                        output_child_id=output_child, topology=topology)
        try:
            yield SimpleNamespace(run=run, action=action, terminal=terminal,
                                  source_id=source_id, channel_id=channel_id,
                                  topology=topology, output_child=output_child)
        finally:
            for action_id in ("main-action", output_child):
                record = run.journal.execution_action(action_id)
                if record is not None and record["result"] is None:
                    run.journal.complete_execution_action(
                        run.fence, action_id, {"kind": "test-complete"})


def _stage(env, rows):
    """Simulate the worker writing one epoch of staging blocks (granted fence)."""
    sink = BlockSink(env.terminal.child, "output", staging_only=True)
    for row in rows:
        sink.append(row)
    return sink.descriptor()


def _cut(env, descriptor, *, sequence, next_ordinal, final=False, inflight=(),
         operation_id=None, checkpoint_id=None):
    return build_commit_cut(
        operation_id or uuid.uuid4().hex, checkpoint_id or identity(["ck", sequence]),
        source_id=env.source_id, next_ordinal=next_ordinal, sequence=sequence,
        channel_id=env.channel_id, channel_cut_sequence=sequence - 1,
        output_descriptor=descriptor,
        states={"stream-progress": {"rows": next_ordinal, "final": final,
                                    "inputs": next_ordinal}},
        inflight=inflight, final=final)


def test_cmt_commit_cut_publishes_exactly_once(env):
    descriptor = _stage(env, [{"v": 1}, {"v": 2}, {"v": 3}])
    cut = _cut(env, descriptor, sequence=1, next_ordinal=3, final=True)
    receipt = env.terminal.commit_cut(cut)
    validate_commit_receipt(receipt)
    assert receipt["status"] == "acked" and receipt["sequence"] == 1
    assert receipt["receipt_descriptor"] == descriptor
    assert env.terminal.rows == 3 and env.terminal.inputs == 3
    facts = env.run.journal.execution_commits(env.output_child)
    assert len(facts) == 1 and facts[0]["status"] == "acked"
    # The driver re-read the worker blocks; it never re-encoded them.
    assert list(env.terminal.sink.iter_rows(descriptor)) == [{"v": 1}, {"v": 2}, {"v": 3}]
    with env.run.journal._connection() as db:
        assert db.execute("SELECT attempt,fence FROM runtime_owner").fetchone() == (env.run.attempt, env.run.fence)
    env.terminal.finish(3)
    assert env.terminal.complete


def test_cmt_idempotent_retry_same_intent_dedups(env):
    descriptor = _stage(env, [{"v": i} for i in range(4)])
    cut = _cut(env, descriptor, sequence=1, next_ordinal=4, final=True)
    first = env.terminal.commit_cut(cut)
    second = env.terminal.commit_cut(cut)  # timeout retry, same operation_id/intent
    assert first == second
    facts = env.run.journal.execution_commits(env.output_child)
    assert len(facts) == 1 and facts[0]["commit_id"] == first["commit_id"]
    assert env.terminal.rows == 4 and env.terminal.sequence == 1


def test_cmt_differing_intent_same_sequence_rejected(env):
    descriptor = _stage(env, [{"v": 1}, {"v": 2}])
    env.terminal.commit_cut(_cut(env, descriptor, sequence=1, next_ordinal=2))
    # Same sequence/commit_id, different source position => different intent.
    conflicting = _cut(env, descriptor, sequence=1, next_ordinal=9)
    with pytest.raises(RecoveryContractError):
        env.terminal.commit_cut(conflicting)
    facts = env.run.journal.execution_commits(env.output_child)
    assert len(facts) == 1 and facts[0]["intent"]["position"]["next_ordinal"] == 2


def test_cmt_sequence_gap_rejected(env):
    descriptor = _stage(env, [{"v": 1}])
    with pytest.raises(RecoveryContractError):
        env.terminal.commit_cut(_cut(env, descriptor, sequence=2, next_ordinal=1))
    assert env.run.journal.execution_commits(env.output_child) == []


def test_cmt_k6_participants_coverage_and_degenerate_vector(env):
    descriptor = _stage(env, [{"v": 1}, {"v": 2}])
    cut = _cut(env, descriptor, sequence=1, next_ordinal=2, final=True)
    env.terminal.commit_cut(cut)
    manifest = env.terminal.checkpoints[cut["checkpoint_id"]]["manifest"]
    participants = manifest["participants"]
    assert participants["sources"] == [{"source_id": env.source_id, "partition_id": "p0"}]
    assert participants["channels"] == [env.channel_id]
    assert participants["sinks"] == ["output"]
    assert participants["operators"] == env.topology["operators"]
    # Single-source degenerate vector: exactly one source-progress component.
    assert manifest["source_progress"] == [
        {"source_id": env.source_id, "partition_id": "p0", "position": {"next_ordinal": 2}}]
    fragment = legacy_checkpoint_fragment(
        {"source_id": env.source_id, "next_ordinal": 2},
        source_ids=[env.source_id], channel_ids=(), partition_count=1)
    assert fragment["source_progress"] == manifest["source_progress"]
    assert manifest["channel_cuts"] == [
        {"channel_id": env.channel_id, "producer_cut_sequence": 0,
         "consumer_offsets": {"driver": 0}}]
    assert manifest["seal"]["sealed"] is True and "digest" in manifest["seal"]


def test_cmt_unknown_paid_effect_never_replayed(env):
    descriptor = _stage(env, [{"v": 1}])
    inflight = [{"request_key": identity(["paid-unknown"]), "status": "unknown"},
                {"request_key": identity(["paid-done"]), "status": "known_completed",
                 "effect_ref": identity(["effect-1"])}]
    cut = _cut(env, descriptor, sequence=1, next_ordinal=1, final=True, inflight=inflight)
    env.terminal.commit_cut(cut)
    manifest = env.terminal.checkpoints[cut["checkpoint_id"]]["manifest"]
    assert replayable_effects(manifest) == [identity(["effect-1"])]  # unknown excluded


def test_cmt_fault_after_prepared_recovers_before_recompute(env, monkeypatch):
    descriptor = _stage(env, [{"v": i} for i in range(5)])
    cut = _cut(env, descriptor, sequence=1, next_ordinal=5, final=True)

    def crash(name, commit_id):
        if name == "after_prepared":
            raise RuntimeError("controller restart after prepare")
    monkeypatch.setattr(commit_module, "fault_point", crash)
    with pytest.raises(RuntimeError):
        env.terminal.commit_cut(cut)
    facts = env.run.journal.execution_commits(env.output_child)
    assert len(facts) == 1 and facts[0]["status"] == "prepared"  # durable, unpublished

    # A fresh terminal (driver restart) reconciles the prepared unit before any
    # source/actor resume; the worker descriptor is committed, never re-written.
    revived = DriverStreamTerminal(env.action, {"source_id": env.source_id, "data": {}},
                                   output_child_id=env.output_child, topology=env.topology)
    assert revived.sequence == 1 and revived.rows == 5 and revived.inputs == 5
    facts = env.run.journal.execution_commits(env.output_child)
    assert len(facts) == 1 and facts[0]["status"] == "acked"
    assert list(revived.sink.iter_rows(descriptor)) == [{"v": i} for i in range(5)]
    # The retried cut dedups against the recovered commit.
    assert revived.commit_cut(cut)["commit_id"] == facts[0]["commit_id"]


def test_cmt_fault_after_sink_commit_before_receipt_recovers(env, monkeypatch):
    descriptor = _stage(env, [{"v": 9}])
    cut = _cut(env, descriptor, sequence=1, next_ordinal=1, final=True)

    def crash(name, commit_id):
        if name == "after_sink_commit_before_receipt":
            raise RuntimeError("restart after publication before receipt")
    monkeypatch.setattr(commit_module, "fault_point", crash)
    with pytest.raises(RuntimeError):
        env.terminal.commit_cut(cut)
    revived = DriverStreamTerminal(env.action, {"source_id": env.source_id, "data": {}},
                                   output_child_id=env.output_child, topology=env.topology)
    facts = env.run.journal.execution_commits(env.output_child)
    assert len(facts) == 1 and facts[0]["status"] == "acked"
    assert revived.rows == 1
    assert list(revived.sink.iter_rows(descriptor)) == [{"v": 9}]


def test_cmt_wire_validators_reject_bad_shapes(env):
    descriptor = _stage(env, [{"v": 1}])
    cut = _cut(env, descriptor, sequence=1, next_ordinal=1)
    with pytest.raises(RecoveryContractError):
        validate_commit_cut({k: v for k, v in cut.items() if k != "final"})
    with pytest.raises(RecoveryContractError):
        validate_commit_cut(dict(cut, source_id="not an identifier!"))
    with pytest.raises(RecoveryContractError):
        validate_commit_cut(dict(cut, inflight=[{"request_key": "x",
                                                "status": "known_completed"}]))  # missing effect_ref
    with pytest.raises(RecoveryContractError):
        build_commit_receipt(uuid.uuid4().hex, "c" * 64, 1, "pending", descriptor)
    receipt = build_commit_receipt(uuid.uuid4().hex, "c" * 64, 1, "acked", descriptor)
    with pytest.raises(RecoveryContractError):
        validate_commit_receipt(dict(receipt, sequence=0))


def test_cmt_compile_chain_topology_seals_and_builds_k6(env):
    import json

    from demiflow.data import plan as ops
    from demiflow.data.plan import CallableSpec, LogicalPlan
    from demiflow.data.sources import ItemsSource
    from demiflow.execution.chain import (
        build_commit_cut, chain_checkpoint_manifest, compile_chain_topology,
        stage_plan_artifact,
    )
    from demiflow.execution.contracts.artifact import verify_manifest

    def _double(row):
        return row

    source = ItemsSource(items=({"x": 1}, {"x": 2}))
    plan = LogicalPlan((ops.MapOp(callable=CallableSpec.create(_double)),
                        ops.FilterOp(callable=CallableSpec.create(_double))))
    plan_ref = stage_plan_artifact(env.action, plan)
    assert plan_ref["bytes"] > 0 and len(plan_ref["sha256"]) == 64
    assert (env.run.directory / plan_ref["path"]).is_file()
    source_reference = {"source_id": env.source_id, "data": {}}
    topology, logical_dag, physical = compile_chain_topology(
        env.action, source, plan, source_reference,
        output_child_id=env.output_child, host_id="host-1", plan_ref=plan_ref)
    # The two transforms are the operator participants; the terminal edge is the
    # result-out channel; worker counts never leak into logical ids.
    assert topology["operators"] == [
        {"logical_node_id": n["logical_node_id"], "occurrence_id": n["occurrence_id"]}
        for n in logical_dag["nodes"][1:-1]]
    assert len(topology["operators"]) == 2
    assert topology["channel_id"] == physical["channels"][-1]["channel_id"]
    assert topology["source_id"] == env.source_id and topology["sink_id"] == "output"
    assert physical["worker_groups"][0]["max_workers"] == 1
    # The K1 manifest seals, verifies and binds the platform + business digests.
    mref = topology["artifact_manifest_ref"]
    manifest = json.loads((env.run.directory / (mref["sha256"] + ".json")).read_text())
    assert manifest["sealed_at"] is not None
    assert verify_manifest(manifest, mref) is True
    config = env.run.journal.recovery_identity()
    assert manifest["platform"]["source_digest"] == config["platform_revision"]
    assert manifest["business"]["bundle_digest"] == "sha256:" + plan_ref["sha256"]
    # Deterministic across a driver restart: recompiling yields the same ref.
    again, _, _ = compile_chain_topology(
        env.action, source, plan, source_reference,
        output_child_id=env.output_child, host_id="host-1", plan_ref=plan_ref)
    assert again["artifact_manifest_ref"] == mref
    assert again["channel_id"] == topology["channel_id"]
    # A K6 manifest builds from the compiled topology and validates coverage.
    descriptor = _stage(env, [{"x": 1}, {"x": 2}])
    cut = build_commit_cut(
        uuid.uuid4().hex, identity(["ck-topo"]), source_id=env.source_id,
        next_ordinal=2, sequence=1, channel_id=topology["channel_id"],
        channel_cut_sequence=0, output_descriptor=descriptor,
        states={"stream-progress": {"rows": 2, "final": True, "inputs": 2}}, final=True)
    k6 = chain_checkpoint_manifest(topology, cut, state_refs={}, sink_receipts=[], sealed=True)
    assert k6["participants"]["channels"] == [topology["channel_id"]]
    assert k6["participants"]["operators"] == topology["operators"]
    assert k6["source_progress"] == [
        {"source_id": env.source_id, "partition_id": "p0", "position": {"next_ordinal": 2}}]
    assert k6["seal"]["sealed"] is True and "digest" in k6["seal"]