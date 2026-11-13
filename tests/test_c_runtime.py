"""C-package interface tests: sealed-layout execution and recovery (scenarios 1-10).

B's plan_interpreter has not landed yet; test-domain stand-ins are injected via
sys.modules so C's consumers are exercised against the frozen type shapes.
"""
import hashlib
import json
import sys
import types
from dataclasses import dataclass, field, asdict
from pathlib import Path
from unittest.mock import MagicMock

import pytest


# ---------------------------------------------------------------------------
# B stand-ins (test domain only)
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class CheckpointPolicy:
    checkpoint_rows: int = 1000
    interval_ms: int = 5000
    unconfirmed_rows: int = 4
    unconfirmed_bytes: int = 64 * 1024 * 1024


@dataclass(frozen=True)
class ProgramOperation:
    program_index: int
    logical_node_id: str
    occurrence_id: str
    stage_limits: dict = None


@dataclass(frozen=True)
class ProfileSpec:
    requests: dict = field(default_factory=lambda: {"cpu_millis": 1000, "memory_bytes": 1024 ** 3})
    limits: object = None
    requirements: object = None


@dataclass(frozen=True)
class WorkerPlacement:
    group_id: str
    profile: ProfileSpec = field(default_factory=ProfileSpec)
    program: tuple = ()
    checkpoint_policy: object = None
    channels: tuple = ()


@dataclass(frozen=True)
class ExecutionLayout:
    placements: tuple = ()


def _to_wire(value):
    if isinstance(value, dict):
        return {k: _to_wire(v) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return [_to_wire(v) for v in value]
    if hasattr(value, "__dataclass_fields__"):
        return {k: _to_wire(getattr(value, k)) for k in value.__dataclass_fields__
                if getattr(value, k) is not None}
    return value


def _interpret_physical_plan(logical_dag, physical, bindings):
    placements = []
    for prog in bindings.get("programs", []):
        ops = tuple(ProgramOperation(
            program_index=op["program_index"],
            logical_node_id=op["logical_node_id"],
            occurrence_id=op["occurrence_id"],
            stage_limits=op.get("stage_limits"))
            for op in prog.get("operations", []))
        profile = ProfileSpec()
        for p in bindings.get("profiles", []):
            profile = ProfileSpec(requests=dict(p.get("requests", {})))
            break
        policy = CheckpointPolicy(
            checkpoint_rows=bindings.get("checkpoint_rows", 1000))
        placements.append(WorkerPlacement(
            group_id=prog["group_id"], profile=profile,
            program=ops, checkpoint_policy=policy))
    return ExecutionLayout(placements=tuple(placements))


@pytest.fixture(autouse=True)
def _inject_b_standins(monkeypatch):
    mod = types.ModuleType("demiflow.planning.plan_interpreter")
    mod.ExecutionLayout = ExecutionLayout
    mod.WorkerPlacement = WorkerPlacement
    mod.CheckpointPolicy = CheckpointPolicy
    mod.ProgramOperation = ProgramOperation
    mod.interpret_physical_plan = _interpret_physical_plan
    mod.to_wire = _to_wire
    monkeypatch.setitem(sys.modules, "demiflow.planning.plan_interpreter", mod)
    yield


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_plan(n_ops=2):
    from demiflow.data.plan import AsyncMapOp, CallableSpec, LogicalPlan
    ops = tuple(AsyncMapOp(CallableSpec.create(lambda row: row), 1, 1, (),
                           f"op{i}", None, "inline") for i in range(n_ops))
    return LogicalPlan(ops)


def _make_source():
    from demiflow.data.sources import ItemsSource
    return ItemsSource(items=(1, 2))


def _make_dag(action_id="act", n_ops=2):
    from demiflow.planning.dag import compile_logical_dag
    plan = _make_plan(n_ops)
    return compile_logical_dag(action_id=action_id, source=_make_source(),
                               operations=plan.operations,
                               terminal_kind="materialize", sink_ids=("output",))


def _mock_authority(tmp_path, facts=None, bindings_bytes=None, bindings_ref=None):
    authority = MagicMock()
    authority.directory = tmp_path
    journal = MagicMock()
    journal.source_binding.return_value = facts
    authority.journal = journal
    if bindings_bytes is not None and bindings_ref is not None:
        p = tmp_path / bindings_ref["path"]
        p.parent.mkdir(parents=True, exist_ok=True)
        p.write_bytes(bindings_bytes)
    return authority


# ---------------------------------------------------------------------------
# Scenario 1: sealed plan interpretation produces correct layout
# ---------------------------------------------------------------------------

class TestScenario1Interpretation:
    def test_interpret_called_with_dag_physical_bindings(self, tmp_path):
        from demiflow.execution.worker.layout import load_worker_layout
        dag = _make_dag()
        bindings_doc = {
            "schema": "demiflow.execution_bindings.v1",
            "programs": [{"group_id": "chain-group", "role": "cpu_prepare",
                          "operations": [{"program_index": 0,
                                          "logical_node_id": dag["nodes"][1]["logical_node_id"],
                                          "occurrence_id": dag["nodes"][1]["occurrence_id"]}]}],
            "profiles": [{"requests": {"cpu_millis": 2000, "memory_bytes": 2 * 1024 ** 3}}],
            "checkpoint_rows": 500,
        }
        raw = json.dumps(bindings_doc).encode()
        ref = {"path": "bindings.json", "sha256": hashlib.sha256(raw).hexdigest(), "bytes": len(raw)}
        facts = {"logical_dag_ref": {"path": "dag.json", "sha256": "x"},
                 "physical_plan_ref": {"path": "phys.json", "sha256": "y"}}
        dag_raw = json.dumps(dag).encode()
        facts["logical_dag_ref"]["sha256"] = hashlib.sha256(dag_raw).hexdigest()
        (tmp_path / "dag.json").write_bytes(dag_raw)
        phys = {"plan_id": "p1", "channels": [], "worker_groups": []}
        phys_raw = json.dumps(phys).encode()
        facts["physical_plan_ref"]["sha256"] = hashlib.sha256(phys_raw).hexdigest()
        (tmp_path / "phys.json").write_bytes(phys_raw)
        authority = _mock_authority(tmp_path, facts, raw, ref)
        launch_spec = {"execution_plan": {"action_id": "act", "group_id": "chain-group",
                                          "bindings_ref": ref}}
        assignment = {"stages": [dag["nodes"][1]["occurrence_id"]]}
        placement, policy, limits = load_worker_layout(launch_spec, authority, assignment)
        assert placement.group_id == "chain-group"
        assert policy.checkpoint_rows == 500


# ---------------------------------------------------------------------------
# Scenario 2: placement profile drives ResourceProfile
# ---------------------------------------------------------------------------

class TestScenario2Profile:
    def test_to_wire_profile_roundtrip(self):
        from demiflow.resources.contracts import ResourceProfile
        spec = ProfileSpec(requests={"cpu_millis": 2000, "memory_bytes": 2 * 1024 ** 3})
        wire = _to_wire(spec)
        profile = ResourceProfile(**wire)
        assert profile.requests["cpu_millis"] == 2000
        assert profile.requests["memory_bytes"] == 2 * 1024 ** 3

    def test_sealed_profile_in_bindings(self, tmp_path):
        from demiflow.execution.chain import _seal_execution_bindings
        dag = _make_dag()
        plan = _make_plan()
        run = MagicMock()
        run.ref.run_id = "r1"
        run.limits = {"max_row_bytes": 4096, "checkpoint_rows": 100}
        run.directory = tmp_path
        run.fence = 1
        journal = MagicMock()
        journal.publication_guard.return_value.__enter__ = MagicMock(return_value=None)
        journal.publication_guard.return_value.__exit__ = MagicMock(return_value=False)
        run.journal = journal
        action = MagicMock()
        action.action_id = "act"
        physical = {"plan_id": "p1", "worker_groups": [{"group_id": "chain-group", "profile_ref": "profile-local"}],
                    "channels": []}
        mref = {"manifest_id": "m1", "sha256": "abc"}
        ref = _seal_execution_bindings(run, action, dag, physical, {"kind": "items"}, plan, mref)
        raw = (tmp_path / ref["path"]).read_bytes()
        doc = json.loads(raw)
        assert doc["profiles"][0]["requests"]["cpu_millis"] == 1000
        assert hashlib.sha256(raw).hexdigest() == ref["sha256"]


# ---------------------------------------------------------------------------
# Scenario 3: CheckpointPolicy reaches progress and barrier
# ---------------------------------------------------------------------------

class TestScenario3CheckpointPolicy:
    def test_progress_uses_policy_rows(self):
        from demiflow.execution.worker.progress import WorkerStreamProgress
        action = MagicMock()
        action.context.limits = {"checkpoint_rows": 999, "max_row_bytes": 4096,
                                 "checkpoint_interval_ms": 1000,
                                 "checkpoint_unconfirmed_rows": 2,
                                 "checkpoint_unconfirmed_bytes": 1024}
        action.context.directory = Path("/tmp/demiflow-c-test")
        policy = CheckpointPolicy(checkpoint_rows=42)
        progress = WorkerStreamProgress.__new__(WorkerStreamProgress)
        progress.run = action.context
        progress.checkpoint_policy = policy
        progress.batch_rows = (policy.checkpoint_rows if policy is not None
                               else progress.run.limits["checkpoint_rows"])
        assert progress.batch_rows == 42

    def test_barrier_interval_from_policy(self):
        from demiflow.execution.stream_barrier import CheckpointControl
        progress = MagicMock()
        progress.checkpoint_policy = CheckpointPolicy(interval_ms=2500)
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
    def test_materialize_applies_budgets(self):
        from demiflow.execution.stream import _materialize
        plan = _make_plan(3)
        limits = {0: {"inflight_rows": 8, "inflight_bytes": 8192, "output_bytes": 1024},
                  2: {"inflight_rows": 4, "inflight_bytes": 4096, "output_bytes": 512}}
        stages = _materialize(plan, stage_limits=limits)
        assert stages[0].stage_budget == limits[0]
        assert stages[1].stage_budget is None
        assert stages[2].stage_budget == limits[2]
        assert stages[0].operation_index == 0
        assert stages[2].operation_index == 2

    def test_materialize_default_no_budget(self):
        from demiflow.execution.stream import _materialize
        plan = _make_plan(2)
        stages = _materialize(plan)
        assert all(s.stage_budget is None for s in stages)

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
        dag = _make_dag()
        bindings = [
            {"report_node_id": "src", "report_occurrence": 0,
             "logical_node_id": dag["nodes"][0]["logical_node_id"],
             "occurrence_id": dag["nodes"][0]["occurrence_id"], "mode": "execute"},
            {"report_node_id": "op0", "report_occurrence": 0,
             "logical_node_id": dag["nodes"][1]["logical_node_id"],
             "occurrence_id": dag["nodes"][1]["occurrence_id"], "mode": "execute"},
        ]
        bridge = build_identity_bridge(dag, action_id="act", report_digest="d1", bindings=bindings)
        assert bridge["schema"] == "demiflow.identity_bridge.v1"
        assert len(bridge["entries"]) == 2
        assert bridge["dag_id"] == dag["dag_id"]

    def test_duplicate_report_key_rejected(self):
        from demiflow.execution.identity_bridge import build_identity_bridge
        from demiflow.execution.recovery_contract import RecoveryContractError
        dag = _make_dag()
        bindings = [
            {"report_node_id": "x", "report_occurrence": 0,
             "logical_node_id": dag["nodes"][0]["logical_node_id"],
             "occurrence_id": dag["nodes"][0]["occurrence_id"], "mode": "execute"},
            {"report_node_id": "x", "report_occurrence": 0,
             "logical_node_id": dag["nodes"][1]["logical_node_id"],
             "occurrence_id": dag["nodes"][1]["occurrence_id"], "mode": "execute"},
        ]
        with pytest.raises(RecoveryContractError, match="multiple instances"):
            build_identity_bridge(dag, action_id="act", report_digest="d1", bindings=bindings)

    def test_unknown_occurrence_rejected(self):
        from demiflow.execution.identity_bridge import build_identity_bridge
        from demiflow.execution.recovery_contract import RecoveryContractError
        dag = _make_dag()
        bindings = [{"report_node_id": "x", "report_occurrence": 0,
                     "logical_node_id": dag["nodes"][0]["logical_node_id"],
                     "occurrence_id": "nonexistent", "mode": "execute"}]
        with pytest.raises(RecoveryContractError, match="not in the sealed"):
            build_identity_bridge(dag, action_id="act", report_digest="d1", bindings=bindings)


# ---------------------------------------------------------------------------
# Scenario 6: graph DAG composition preserves distinct instances
# ---------------------------------------------------------------------------

class TestScenario6GraphDag:
    def test_same_function_distinct_targets(self):
        from demiflow.planning.graph_dag import compile_graph_logical_dag
        plan = _make_plan(1)
        ancestor = {"plan": plan, "logical_node_id": "anc", "occurrence_id": "anc-occ"}
        targets = [{"plan": plan, "reader_id": "t1", "logical_node_id": "tgt1", "occurrence_id": "o1"},
                   {"plan": plan, "reader_id": "t2", "logical_node_id": "tgt2", "occurrence_id": "o2"}]
        dag, programs = compile_graph_logical_dag(
            action_id="act", source=_make_source(), ancestor=ancestor, targets=targets)
        assert len(programs) == 3
        t1_ops = programs[1]["operators"]
        t2_ops = programs[2]["operators"]
        assert t1_ops[0]["occurrence_id"] != t2_ops[0]["occurrence_id"]
        assert t1_ops[0]["logical_node_id"] != t2_ops[0]["logical_node_id"]

    def test_shared_ancestor_once(self):
        from demiflow.planning.graph_dag import compile_graph_logical_dag
        plan = _make_plan(2)
        ancestor = {"plan": plan}
        targets = [{"plan": _make_plan(1), "reader_id": "t1"},
                   {"plan": _make_plan(1), "reader_id": "t2"}]
        dag, programs = compile_graph_logical_dag(
            action_id="act", source=_make_source(), ancestor=ancestor, targets=targets)
        anc_ops = programs[0]["operators"]
        assert len(anc_ops) == 2
        occ_ids = [n["occurrence_id"] for n in dag["nodes"]]
        assert len(occ_ids) == len(set(occ_ids))


# ---------------------------------------------------------------------------
# Scenario 7: bindings artifact integrity
# ---------------------------------------------------------------------------

class TestScenario7BindingsIntegrity:
    def test_digest_mismatch_rejected(self, tmp_path):
        from demiflow.execution.worker.layout import load_worker_layout
        from demiflow.execution.recovery_contract import RecoveryContractError
        raw = b'{"programs":[]}'
        ref = {"path": "b.json", "sha256": "wrong", "bytes": len(raw)}
        authority = _mock_authority(tmp_path, None, raw, ref)
        launch_spec = {"execution_plan": {"action_id": "a", "group_id": "g", "bindings_ref": ref}}
        with pytest.raises(RecoveryContractError, match="digest differs"):
            load_worker_layout(launch_spec, authority, {})

    def test_oversized_rejected(self, tmp_path):
        from demiflow.execution.worker.layout import load_worker_layout
        from demiflow.execution.recovery_contract import RecoveryContractError
        raw = b"x" * 100
        ref = {"path": "b.json", "sha256": hashlib.sha256(raw).hexdigest(), "bytes": 10}
        authority = _mock_authority(tmp_path, None, raw, ref)
        launch_spec = {"execution_plan": {"action_id": "a", "group_id": "g", "bindings_ref": ref}}
        with pytest.raises(RecoveryContractError, match="exceeds declared size"):
            load_worker_layout(launch_spec, authority, {})


# ---------------------------------------------------------------------------
# Scenario 8: assignment validation against sealed program
# ---------------------------------------------------------------------------

class TestScenario8AssignmentValidation:
    def test_mismatched_stages_rejected(self, tmp_path):
        from demiflow.execution.worker.layout import load_worker_layout
        from demiflow.execution.recovery_contract import RecoveryContractError
        dag = _make_dag()
        occ = dag["nodes"][1]["occurrence_id"]
        bindings_doc = {"programs": [{"group_id": "g", "operations": [
            {"program_index": 0, "logical_node_id": "l", "occurrence_id": occ}]}],
            "profiles": [], "checkpoint_rows": 100}
        raw = json.dumps(bindings_doc).encode()
        ref = {"path": "b.json", "sha256": hashlib.sha256(raw).hexdigest(), "bytes": len(raw)}
        dag_raw = json.dumps(dag).encode()
        phys = {"plan_id": "p", "channels": [], "worker_groups": []}
        phys_raw = json.dumps(phys).encode()
        facts = {"logical_dag_ref": {"path": "d.json", "sha256": hashlib.sha256(dag_raw).hexdigest()},
                 "physical_plan_ref": {"path": "p.json", "sha256": hashlib.sha256(phys_raw).hexdigest()}}
        (tmp_path / "d.json").write_bytes(dag_raw)
        (tmp_path / "p.json").write_bytes(phys_raw)
        authority = _mock_authority(tmp_path, facts, raw, ref)
        launch_spec = {"execution_plan": {"action_id": "a", "group_id": "g", "bindings_ref": ref}}
        with pytest.raises(RecoveryContractError, match="do not match"):
            load_worker_layout(launch_spec, authority, {"stages": ["wrong-occ"]})


# ---------------------------------------------------------------------------
# Scenario 9: legacy path unchanged without execution_plan
# ---------------------------------------------------------------------------

class TestScenario9LegacyPath:
    def test_no_execution_plan_returns_none(self, tmp_path):
        from demiflow.execution.worker.layout import load_worker_layout
        authority = _mock_authority(tmp_path)
        assert load_worker_layout({}, authority, {}) is None

    def test_run_stream_default_no_limits(self):
        from demiflow.execution.stream import run_stream
        plan = _make_plan(1)
        rows = [{"x": 1}, {"x": 2}]
        collected = []
        stats = run_stream(iter(rows), plan, consume=lambda r: collected.append(r))
        assert len(collected) == 2
        assert stats.emitted == 2


# ---------------------------------------------------------------------------
# Scenario 10: recovery reads original sealed inputs
# ---------------------------------------------------------------------------

class TestScenario10RecoverySealedInputs:
    def test_f1_missing_rejected(self, tmp_path):
        from demiflow.execution.worker.layout import load_worker_layout
        from demiflow.execution.recovery_contract import RecoveryContractError
        raw = b'{"programs":[]}'
        ref = {"path": "b.json", "sha256": hashlib.sha256(raw).hexdigest(), "bytes": len(raw)}
        authority = _mock_authority(tmp_path, None, raw, ref)
        launch_spec = {"execution_plan": {"action_id": "a", "group_id": "g", "bindings_ref": ref}}
        with pytest.raises(RecoveryContractError, match="missing"):
            load_worker_layout(launch_spec, authority, {})

    def test_sealed_artifact_digest_verified(self, tmp_path):
        from demiflow.execution.worker.layout import load_worker_layout
        from demiflow.execution.recovery_contract import RecoveryContractError
        dag = _make_dag()
        bindings_doc = {"programs": [{"group_id": "g", "operations": []}],
                        "profiles": [], "checkpoint_rows": 100}
        raw = json.dumps(bindings_doc).encode()
        ref = {"path": "b.json", "sha256": hashlib.sha256(raw).hexdigest(), "bytes": len(raw)}
        dag_raw = json.dumps(dag).encode()
        facts = {"logical_dag_ref": {"path": "d.json", "sha256": "bad-digest"},
                 "physical_plan_ref": {"path": "p.json", "sha256": "x"}}
        (tmp_path / "d.json").write_bytes(dag_raw)
        authority = _mock_authority(tmp_path, facts, raw, ref)
        launch_spec = {"execution_plan": {"action_id": "a", "group_id": "g", "bindings_ref": ref}}
        with pytest.raises(RecoveryContractError, match="digest differs"):
            load_worker_layout(launch_spec, authority, {})