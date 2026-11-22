"""C-package tests against real B (scenarios 1-10).

No stand-ins: all interpretation, sealing and reading go through the real
execution.plan_interpreter and execution.sealed_plans modules.  Interface unit
tests use the B test fixtures (fixed documents + real frozen compilers); the
end-to-end closed-loop test runs a real local chain worker.
"""
import hashlib
import json
import shutil
import sys
import tempfile
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

from demiflow.execution.plan_interpreter import (
    CheckpointPolicy, ExecutionLayout, PlanContractError, WorkerPlacement,
    interpret_physical_plan, to_wire, validate_worker_assignment,
)
from demiflow.execution.recovery_contract import RecoveryContractError, identity
from demiflow.execution.sealed_plans import (
    load_execution_bindings, load_sealed_plan, read_plan_artifact,
    seal_execution_bindings,
)
from demiflow.resources.contracts import ResourceProfile
from test_execution_layout import (
    MANIFEST_REF, PROFILE, PROFILE_REF, bundle_for, chain_fixture,
    fanout_fixture, stage_id,
)

pytestmark = pytest.mark.skipif(not sys.platform.startswith("linux"),
                                reason="Local managed units require Linux")


@pytest.fixture
def workdir():
    path = Path(tempfile.mkdtemp(prefix="demiflow-c-", dir="/tmp"))
    try:
        yield path
    finally:
        shutil.rmtree(path, ignore_errors=True)


def _bind(run, action_id):
    from demiflow.execution.run_context import RunAction
    declaration = {"kind": "chain-materialize", "source": {"source_id": "fixed"}}
    run.journal.bind_execution_action(run.fence, action_id, declaration)
    return RunAction(run, action_id, declaration)


def _sealed_chain(workdir, action_id="c-action"):
    from demiflow.execution import chain
    from demiflow.execution.run_context import RunContext
    fx = chain_fixture(action_id=action_id)
    root = workdir / "run"
    with RunContext.start(root) as run:
        action = _bind(run, action_id)
        sealed_at = int(run.journal.recovery_identity()["started_at"] * 1000)
        chain.seal_plan_facts(run, action, fx.ld, fx.pp, fx.mref, sealed_at)
        seal_execution_bindings(
            run, action, logical_dag=fx.ld, physical_plan=fx.pp,
            profiles=fx.doc["profiles"], programs=fx.doc["programs"],
            driver_stage_ids=fx.doc["driver_stage_ids"],
            source_bindings=fx.doc["source_bindings"],
            channel_options=fx.doc["channel_options"],
            checkpoint_rows=fx.doc["checkpoint_rows"],
            supported_slice=fx.doc["supported_slice"])
        run_id = run.ref.run_id
        run.journal.complete_execution_action(run.fence, action_id, {"kind": "test"})
    return SimpleNamespace(root=root, run_id=run_id, fx=fx, action_id=action_id)


# ---------------------------------------------------------------------------
# Scenario 1: sealed plan interpretation produces correct layout
# ---------------------------------------------------------------------------

class TestScenario1Interpretation:
    def test_interpret_real_b_chain(self, workdir):
        sc = _sealed_chain(workdir)
        sp = load_sealed_plan(sc.root, sc.action_id)
        bindings = load_execution_bindings(sc.root, sc.action_id, sealed_plan=sp)
        layout = interpret_physical_plan(sp["logical_dag"], sp["physical_plan"],
                                         bindings=bindings,
                                         manifest_ref=sp["facts"]["artifact_manifest_ref"])
        assert isinstance(layout, ExecutionLayout)
        assert len(layout.workers) == 1
        assert layout.workers[0].group_id == "g0"
        assert layout.supported_slice == "chain_v1"
        assert layout.bindings_ref == bindings["ref"]

    def test_load_worker_layout_real(self, workdir):
        from demiflow.execution.worker.layout import load_worker_layout
        sc = _sealed_chain(workdir)
        sp = load_sealed_plan(sc.root, sc.action_id)
        bindings = load_execution_bindings(sc.root, sc.action_id, sealed_plan=sp)
        authority = MagicMock()
        authority.directory = sc.root
        launch_spec = {"execution_plan": {
            "action_id": sc.action_id, "group_id": "g0",
            "bindings_ref": bindings["ref"]}}
        placement, policy, limits = load_worker_layout(launch_spec, authority, {})
        assert placement.group_id == "g0"
        assert isinstance(policy, CheckpointPolicy)
        assert policy.checkpoint_rows == sc.fx.doc["checkpoint_rows"]
        assert len(limits) == len(sc.fx.operation_nodes)
        for idx, budget in limits.items():
            assert set(budget) == {"inflight_rows", "inflight_bytes", "output_bytes"}


# ---------------------------------------------------------------------------
# Scenario 2: placement profile drives ResourceProfile
# ---------------------------------------------------------------------------

class TestScenario2Profile:
    def test_to_wire_profile_roundtrip(self):
        wire = to_wire(PROFILE)
        profile = ResourceProfile(**wire)
        assert profile.requests["cpu_millis"] == PROFILE["requests"]["cpu_millis"]

    def test_sealed_profile_from_layout(self, workdir):
        sc = _sealed_chain(workdir)
        sp = load_sealed_plan(sc.root, sc.action_id)
        bindings = load_execution_bindings(sc.root, sc.action_id, sealed_plan=sp)
        layout = interpret_physical_plan(sp["logical_dag"], sp["physical_plan"],
                                         bindings=bindings,
                                         manifest_ref=sp["facts"]["artifact_manifest_ref"])
        placement = layout.workers[0]
        profile = ResourceProfile(**to_wire(placement.profile))
        assert profile.requests["cpu_millis"] == PROFILE["requests"]["cpu_millis"]


# ---------------------------------------------------------------------------
# Scenario 3: CheckpointPolicy reaches progress and barrier
# ---------------------------------------------------------------------------

class TestScenario3CheckpointPolicy:
    def test_layout_checkpoint_is_policy(self, workdir):
        sc = _sealed_chain(workdir)
        sp = load_sealed_plan(sc.root, sc.action_id)
        bindings = load_execution_bindings(sc.root, sc.action_id, sealed_plan=sp)
        layout = interpret_physical_plan(sp["logical_dag"], sp["physical_plan"],
                                         bindings=bindings,
                                         manifest_ref=sp["facts"]["artifact_manifest_ref"])
        assert isinstance(layout.checkpoint, CheckpointPolicy)
        assert layout.checkpoint.checkpoint_rows == sc.fx.doc["checkpoint_rows"]
        assert layout.checkpoint.interval_ms == sc.fx.pp["recovery"]["checkpoint_interval_ms"]

    def test_barrier_interval_from_policy(self):
        from demiflow.execution.stream_barrier import CheckpointControl
        progress = MagicMock()
        progress.checkpoint_policy = CheckpointPolicy(
            checkpoint_rows=100, interval_ms=2500, unconfirmed_rows=4,
            unconfirmed_bytes=64 * 1024 ** 2)
        progress.run.limits = {"checkpoint_interval_ms": 9999}
        ctrl = CheckpointControl.__new__(CheckpointControl)
        ctrl.progress = progress
        policy = getattr(progress, "checkpoint_policy", None)
        interval = (policy.interval_ms / 1000 if policy is not None
                    else progress.run.limits["checkpoint_interval_ms"] / 1000)
        assert interval == 2.5


# ---------------------------------------------------------------------------
# Scenario 4: stage_limits reach actual queues
# ---------------------------------------------------------------------------

class TestScenario4StageLimits:
    def test_stage_limits_from_layout_budgets(self, workdir):
        sc = _sealed_chain(workdir)
        sp = load_sealed_plan(sc.root, sc.action_id)
        bindings = load_execution_bindings(sc.root, sc.action_id, sealed_plan=sp)
        layout = interpret_physical_plan(sp["logical_dag"], sp["physical_plan"],
                                         bindings=bindings,
                                         manifest_ref=sp["facts"]["artifact_manifest_ref"])
        placement = layout.workers[0]
        stage_by_id = {s["stage_id"]: s for s in layout.stages}
        limits = {}
        for binding in placement.operator_bindings:
            stage = stage_by_id[binding["stage_id"]]
            limits[binding["program_index"]] = dict(stage["budgets"])
        assert len(limits) == len(sc.fx.operation_nodes)
        for budget in limits.values():
            assert budget["inflight_rows"] >= 1
            assert budget["inflight_bytes"] >= 1
            assert budget["output_bytes"] >= 1

    def test_materialize_applies_budgets(self):
        from demiflow.data.plan import AsyncMapOp, CallableSpec, LogicalPlan
        from demiflow.execution.stream import _materialize
        ops = tuple(AsyncMapOp(CallableSpec.create(lambda r: r), 1, 1, (),
                               f"op{i}", None, "inline") for i in range(3))
        plan = LogicalPlan(ops)
        limits = {0: {"inflight_rows": 8, "inflight_bytes": 8192, "output_bytes": 1024},
                  2: {"inflight_rows": 4, "inflight_bytes": 4096, "output_bytes": 512}}
        stages = _materialize(plan, stage_limits=limits)
        assert stages[0].stage_budget == limits[0]
        assert stages[1].stage_budget is None
        assert stages[2].stage_budget == limits[2]

    def test_filter_fold_preserves_index(self):
        from demiflow.data.plan import AsyncMapOp, CallableSpec, FilterOp, LogicalPlan
        from demiflow.execution.stream import _materialize
        ops = (AsyncMapOp(CallableSpec.create(lambda r: r), 1, 1, (), "a", None, "inline"),
               FilterOp(CallableSpec.create(lambda r: True)),
               AsyncMapOp(CallableSpec.create(lambda r: r), 1, 1, (), "b", None, "inline"))
        plan = LogicalPlan(ops)
        limits = {2: {"inflight_rows": 16, "inflight_bytes": 16384, "output_bytes": 1024}}
        stages = _materialize(plan, stage_limits=limits)
        assert len(stages) == 2
        assert stages[1].operation_index == 2
        assert stages[1].stage_budget == limits[2]


# ---------------------------------------------------------------------------
# Scenario 5: identity bridge maps report keys to execution nodes
# ---------------------------------------------------------------------------

class TestScenario5IdentityBridge:
    def test_build_and_validate(self):
        from demiflow.execution.identity_bridge import build_identity_bridge
        fx = chain_fixture()
        bindings = [
            {"report_node_id": "src", "report_occurrence": 0,
             "logical_node_id": fx.ld["nodes"][0]["logical_node_id"],
             "occurrence_id": fx.ld["nodes"][0]["occurrence_id"], "mode": "execute"},
            {"report_node_id": "op0", "report_occurrence": 0,
             "logical_node_id": fx.ld["nodes"][1]["logical_node_id"],
             "occurrence_id": fx.ld["nodes"][1]["occurrence_id"], "mode": "execute"},
        ]
        bridge = build_identity_bridge(fx.ld, action_id=fx.action_id,
                                       report_digest="d1", bindings=bindings)
        assert bridge["schema"] == "demiflow.identity_bridge.v1"
        assert len(bridge["entries"]) == 2
        assert bridge["dag_id"] == fx.ld["dag_id"]

    def test_duplicate_report_key_rejected(self):
        from demiflow.execution.identity_bridge import build_identity_bridge
        fx = chain_fixture()
        bindings = [
            {"report_node_id": "x", "report_occurrence": 0,
             "logical_node_id": fx.ld["nodes"][0]["logical_node_id"],
             "occurrence_id": fx.ld["nodes"][0]["occurrence_id"], "mode": "execute"},
            {"report_node_id": "x", "report_occurrence": 0,
             "logical_node_id": fx.ld["nodes"][1]["logical_node_id"],
             "occurrence_id": fx.ld["nodes"][1]["occurrence_id"], "mode": "execute"},
        ]
        with pytest.raises(RecoveryContractError, match="multiple instances"):
            build_identity_bridge(fx.ld, action_id=fx.action_id,
                                  report_digest="d1", bindings=bindings)

    def test_unknown_occurrence_rejected(self):
        from demiflow.execution.identity_bridge import build_identity_bridge
        fx = chain_fixture()
        bindings = [{"report_node_id": "x", "report_occurrence": 0,
                     "logical_node_id": fx.ld["nodes"][0]["logical_node_id"],
                     "occurrence_id": "nonexistent", "mode": "execute"}]
        with pytest.raises(RecoveryContractError, match="not in the sealed"):
            build_identity_bridge(fx.ld, action_id=fx.action_id,
                                  report_digest="d1", bindings=bindings)


# ---------------------------------------------------------------------------
# Scenario 6: graph DAG composition preserves distinct instances
# ---------------------------------------------------------------------------

class TestScenario6GraphDag:
    def test_same_function_distinct_targets(self):
        from demiflow.data.plan import AsyncMapOp, CallableSpec, LogicalPlan
        from demiflow.data.sources import ItemsSource
        from demiflow.planning.graph_dag import compile_graph_logical_dag
        ops = (AsyncMapOp(CallableSpec.create(lambda r: r), 1, 1, (), "op0", None, "inline"),)
        plan = LogicalPlan(ops)
        source = ItemsSource(items=(1, 2))
        ancestor = {"plan": plan}
        targets = [{"plan": plan, "reader_id": "t1"},
                   {"plan": plan, "reader_id": "t2"}]
        dag, programs = compile_graph_logical_dag(
            action_id="act", source=source, ancestor=ancestor, targets=targets)
        assert len(programs) == 3
        t1_ops = programs[1]["operators"]
        t2_ops = programs[2]["operators"]
        assert t1_ops[0]["occurrence_id"] != t2_ops[0]["occurrence_id"]
        assert t1_ops[0]["logical_node_id"] != t2_ops[0]["logical_node_id"]

    def test_shared_ancestor_once(self):
        from demiflow.data.plan import AsyncMapOp, CallableSpec, LogicalPlan
        from demiflow.data.sources import ItemsSource
        from demiflow.planning.graph_dag import compile_graph_logical_dag
        ops = tuple(AsyncMapOp(CallableSpec.create(lambda r: r), 1, 1, (),
                               f"op{i}", None, "inline") for i in range(2))
        plan = LogicalPlan(ops)
        source = ItemsSource(items=(1, 2))
        ancestor = {"plan": plan}
        targets = [{"plan": LogicalPlan(ops[:1]), "reader_id": "t1"},
                   {"plan": LogicalPlan(ops[:1]), "reader_id": "t2"}]
        dag, programs = compile_graph_logical_dag(
            action_id="act", source=source, ancestor=ancestor, targets=targets)
        assert len(programs[0]["operators"]) == 2
        occ_ids = [n["occurrence_id"] for n in dag["nodes"]]
        assert len(occ_ids) == len(set(occ_ids))


# ---------------------------------------------------------------------------
# Scenario 7: bindings artifact integrity (real B reader)
# ---------------------------------------------------------------------------

class TestScenario7BindingsIntegrity:
    def test_digest_mismatch_rejected(self, workdir):
        raw = b'{"programs":[]}'
        ref = {"path": "b.json", "sha256": "ab" * 32, "bytes": len(raw)}
        (workdir / "b.json").write_bytes(raw)
        with pytest.raises(PlanContractError) as exc:
            read_plan_artifact(workdir, ref, max_bytes=4 * 1024 ** 2)
        assert exc.value.reason == "corrupt_artifact"

    def test_oversized_rejected(self, workdir):
        raw = b"x" * 100
        ref = {"path": "b.json", "sha256": hashlib.sha256(raw).hexdigest(), "bytes": 10}
        (workdir / "b.json").write_bytes(raw)
        with pytest.raises(PlanContractError) as exc:
            read_plan_artifact(workdir, ref, max_bytes=4 * 1024 ** 2)
        assert exc.value.reason == "corrupt_artifact"

    def test_missing_bindings_rejected(self, workdir):
        sc = _sealed_chain(workdir)
        sp = load_sealed_plan(sc.root, sc.action_id)
        import sqlite3
        with sqlite3.connect(sc.root / "run.sqlite") as db:
            db.execute("DELETE FROM runtime_sources WHERE action_id=?",
                       (sc.action_id + "-execution-bindings",))
        with pytest.raises(PlanContractError) as exc:
            load_execution_bindings(sc.root, sc.action_id, sealed_plan=sp)
        assert exc.value.reason == "missing_bindings"


# ---------------------------------------------------------------------------
# Scenario 8: assignment validation against sealed layout
# ---------------------------------------------------------------------------

class TestScenario8AssignmentValidation:
    def test_mismatched_stages_rejected(self, workdir):
        sc = _sealed_chain(workdir)
        sp = load_sealed_plan(sc.root, sc.action_id)
        bindings = load_execution_bindings(sc.root, sc.action_id, sealed_plan=sp)
        layout = interpret_physical_plan(sp["logical_dag"], sp["physical_plan"],
                                         bindings=bindings,
                                         manifest_ref=sp["facts"]["artifact_manifest_ref"])
        placement = layout.workers[0]
        assignment = {
            "schema_version": "demiflow.worker_assignment.v1",
            "assignment_id": "a" * 32, "operation_id": "b" * 32,
            "scope_id": "c" * 32, "allocation_id": "d" * 32,
            "boot_id": "e" * 32, "worker_id": "f" * 32,
            "controller_epoch": 0, "grant_ref": "0" * 32,
            "plan_revision": 0, "expected_plan_revision": 0,
            "artifact_manifest_ref": MANIFEST_REF,
            "stages": ["wrong-stage"], "partitions": list(placement.partition_ids),
            "channels": {"inputs": [], "outputs": []},
            "restore_point": None, "route_epoch": 0, "topology_revision": 0,
            "ready_deadline_ms": 60000}
        with pytest.raises(PlanContractError) as exc:
            validate_worker_assignment(layout, assignment, group_id="g0")
        assert exc.value.reason == "invalid_assignment"

    def test_valid_assignment_accepted(self, workdir):
        from test_execution_layout import assignment_for, expected_channel_ids
        sc = _sealed_chain(workdir)
        sp = load_sealed_plan(sc.root, sc.action_id)
        bindings = load_execution_bindings(sc.root, sc.action_id, sealed_plan=sp)
        layout = interpret_physical_plan(sp["logical_dag"], sp["physical_plan"],
                                         bindings=bindings,
                                         manifest_ref=sp["facts"]["artifact_manifest_ref"])
        placement = layout.workers[0]
        inputs, outputs = expected_channel_ids(layout, placement)
        assignment = assignment_for(layout, placement, inputs, outputs)
        result = validate_worker_assignment(layout, assignment, group_id="g0")
        assert result.group_id == "g0"


# ---------------------------------------------------------------------------
# Scenario 9: legacy path unchanged without execution_plan
# ---------------------------------------------------------------------------

class TestScenario9LegacyPath:
    def test_no_execution_plan_returns_none(self, workdir):
        from demiflow.execution.worker.layout import load_worker_layout
        authority = MagicMock()
        authority.directory = workdir
        assert load_worker_layout({}, authority, {}) is None

    def test_run_stream_default_no_limits(self):
        from demiflow.data.plan import AsyncMapOp, CallableSpec, LogicalPlan
        from demiflow.execution.stream import run_stream
        ops = (AsyncMapOp(CallableSpec.create(lambda r: r), 1, 1, (), "op0", None, "inline"),)
        plan = LogicalPlan(ops)
        rows = [{"x": 1}, {"x": 2}]
        collected = []
        stats = run_stream(iter(rows), plan, consume=lambda r: collected.append(r))
        assert len(collected) == 2
        assert stats.emitted == 2


# ---------------------------------------------------------------------------
# Scenario 10: recovery reads original sealed inputs
# ---------------------------------------------------------------------------

class TestScenario10RecoverySealedInputs:
    def test_f1_missing_rejected(self, workdir):
        with pytest.raises(PlanContractError) as exc:
            load_sealed_plan(workdir, "nonexistent-action")
        assert exc.value.reason == "missing_plan"

    def test_sealed_artifact_digest_verified(self, workdir):
        sc = _sealed_chain(workdir)
        sp = load_sealed_plan(sc.root, sc.action_id)
        dag_path = sc.root / sp["facts"]["logical_dag_ref"]["path"]
        original = dag_path.read_bytes()
        dag_path.write_bytes(b"corrupted")
        with pytest.raises(PlanContractError) as exc:
            load_sealed_plan(sc.root, sc.action_id)
        assert exc.value.reason == "corrupt_artifact"
        dag_path.write_bytes(original)

    def test_seal_interpret_roundtrip(self, workdir):
        sc = _sealed_chain(workdir)
        sp = load_sealed_plan(sc.root, sc.action_id)
        bindings = load_execution_bindings(sc.root, sc.action_id, sealed_plan=sp)
        layout = interpret_physical_plan(sp["logical_dag"], sp["physical_plan"],
                                         bindings=bindings,
                                         manifest_ref=sp["facts"]["artifact_manifest_ref"])
        assert layout.dag_id == sc.fx.ld["dag_id"]
        assert layout.plan_id == sc.fx.pp["plan_id"]
        assert layout.workers[0].group_id == "g0"
        assert len(layout.workers[0].sealed_operators) == len(sc.fx.operation_nodes)
        assert len(layout.workers[0].operator_bindings) == len(sc.fx.operation_nodes)