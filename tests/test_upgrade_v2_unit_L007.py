"""L007 unit: compile_native_topology seals a driver-only native layout (durable_write).

F004's Dataset-DAG compiler and L044's source/program_id-aware seal_write_plan_ref
are not yet merged, so they are injected at the boundary (sys.modules + monkeypatch)
as fixed-schema doubles; the injected compiler still builds the ld with the REAL
frozen compile_logical_dag. Everything else is the real durable path: the frozen
physical compiler, _build_sealed_manifest, seal_plan_facts (F1) and this group's
seal_execution_bindings (v2, which validates at seal time). The post-run roundtrip
load proves the sealed layout binds the decision's actual backend/recovery/route.
This is a wiring/unit test, NOT an integration pass (mocked callees).
"""
import contextlib
import dataclasses
import shutil
import sys
from types import ModuleType, SimpleNamespace

import pytest

from demiflow.execution import chain
from demiflow.execution.native_plan import compile_native_topology, load_native_execution
from demiflow.execution.plan_interpreter import PlanContractError
from demiflow.execution.recovery_contract import identity
from demiflow.execution.recovery_identity import describe
from demiflow.execution.run_context import RunAction, RunContext
from demiflow.execution.sealed_plans import load_sealed_plan
from demiflow.planning.dag import compile_logical_dag
from test_execution_layout import PROGRAM_REF, stage_id
import _v2_native_fixtures as fx

ACTION = "native-action"


@pytest.fixture
def workdir():
    path = fx.make_workdir()
    try:
        yield path
    finally:
        shutil.rmtree(path, ignore_errors=True)


def _decision(*, action_kind="materialize", backend="local_stream",
              recovery="native_checkpoint", route="native_relational"):
    return SimpleNamespace(action_kind=action_kind, backend=backend,
                           recovery=recovery, route=route)


@dataclasses.dataclass(frozen=True)
class _SliceDouble:
    """Fixed-schema double of F004's frozen ProgramSlice (unmerged). operator_bindings
    carry the FULL-GRAPH occurrence_id exactly as F004 delivers (dataset_dag.py:127);
    sealed_operators keep the LOCAL linear ids. L007 must resolve occurrence->stage."""
    program_id: str
    input_path: tuple
    source: object
    plan: object
    sealed_operators: tuple
    operator_bindings: tuple


def _program_slice(action_id, ld, source, plan):
    op_nodes = ld["nodes"][1:-1]
    return _SliceDouble(
        program_id=identity([action_id, []]), input_path=(), source=source, plan=plan,
        sealed_operators=tuple(fx.local_operators(plan)),
        operator_bindings=tuple({"program_index": i, "occurrence_id": node["occurrence_id"]}
                                for i, node in enumerate(op_nodes)))


def _install_f004(monkeypatch, record):
    """Inject the unmerged F004 compiler; it builds a real linear ld + one slice."""
    module = ModuleType("demiflow.planning.dataset_dag")

    def compile_dataset_logical_dag(*, action_id, source, plan, terminal_kind, sink_ids):
        record.append({"terminal_kind": terminal_kind, "sink_ids": sink_ids})
        ld = compile_logical_dag(action_id=action_id, source=source, operations=plan.operations,
                                 terminal_kind=terminal_kind, sink_ids=sink_ids)
        return ld, (_program_slice(action_id, ld, source, plan),)

    module.compile_dataset_logical_dag = compile_dataset_logical_dag
    monkeypatch.setitem(sys.modules, "demiflow.planning.dataset_dag", module)


def _install_l044_seal(monkeypatch):
    def fake_seal(action, plan, *, source=None, program_id=None):
        return {**PROGRAM_REF,
                "code_pin_digest": identity({"ops": len(plan.operations), "src": describe(source)})}
    monkeypatch.setattr(chain, "seal_write_plan_ref", fake_seal)


@contextlib.contextmanager
def _native_run(root, action_id):
    with RunContext.start(root) as run:
        declaration = {"kind": "chain-materialize", "source": {"source_id": "fixed"}}
        run.journal.bind_execution_action(run.fence, action_id, declaration)
        action = RunAction(run, action_id, declaration)
        yield run, action
        run.journal.complete_execution_action(run.fence, action_id, {"kind": "test-complete"})


def test_compile_seals_driver_only_native_layout(monkeypatch, workdir):
    record = []
    _install_f004(monkeypatch, record)
    _install_l044_seal(monkeypatch)
    source, plan = fx.native_source_plan()
    decision = _decision()
    with _native_run(workdir / "run", ACTION) as (run, action):
        limits = dict(run.limits)
        logical_dag, physical, bindings_ref = compile_native_topology(
            action, source, plan, decision=decision)
    assert record == [{"terminal_kind": "materialize", "sink_ids": ("output",)}]
    assert logical_dag["action_id"] == ACTION
    # driver-only: no worker group is fabricated for the native driver slice
    assert physical["worker_groups"] == []
    assert set(bindings_ref) == {"path", "sha256", "bytes"}
    # stage budgets come from the same run-limits authority as the linear seam
    row_bytes = limits["max_row_bytes"] + 256
    queue_bytes = max(row_bytes, limits["max_batch_bytes"])
    for node in logical_dag["nodes"][1:-1]:
        sid = identity([logical_dag["dag_id"], node["occurrence_id"]])
        stage = next(s for s in physical["stages"] if s["stage_id"] == sid)
        assert stage["budgets"] == {"inflight_rows": limits["batch_rows"],
                                    "inflight_bytes": queue_bytes, "output_bytes": row_bytes}
    # post-run roundtrip: the sealed layout binds the decision's actual authority
    layout, placement = load_native_execution(
        RunContext(workdir / "run"), ACTION,
        execution_id=identity([ACTION, "driver", decision.route]))
    assert layout.supported_slice == "native_driver_v1"
    assert placement.placement == "driver" and placement.group_id is None
    assert placement.backend == decision.backend
    assert placement.recovery == decision.recovery
    assert list(placement.stage_ids) == [stage_id(logical_dag, n)
                                         for n in logical_dag["nodes"][1:-1]]
    assert len(placement.programs) == 1
    assert placement.programs[0]["program_id"] == identity([ACTION, []])
    assert placement.programs[0]["source_identity_ref"] == identity(describe(source))
    # F004 delivers occurrence_id; L007 must resolve it to the physical stage_id
    assert [dict(ob) for ob in placement.programs[0]["operator_bindings"]] == [
        {"program_index": i, "stage_id": identity([logical_dag["dag_id"], node["occurrence_id"]])}
        for i, node in enumerate(logical_dag["nodes"][1:-1])]


def test_compile_write_lance_uses_lance_sink(monkeypatch, workdir):
    record = []
    _install_f004(monkeypatch, record)
    _install_l044_seal(monkeypatch)
    source, plan = fx.native_source_plan()
    with _native_run(workdir / "run", ACTION) as (run, action):
        logical_dag, _, _ = compile_native_topology(
            action, source, plan, decision=_decision(action_kind="write_lance"))
    assert record == [{"terminal_kind": "write_lance", "sink_ids": ("lance",)}]
    assert logical_dag["terminals"][0]["kind"] == "write_lance"
    assert logical_dag["terminals"][0]["sink_ids"] == ["lance"]


def test_compile_recovery_is_descriptive_verbatim(monkeypatch, workdir):
    _install_f004(monkeypatch, [])
    _install_l044_seal(monkeypatch)
    source, plan = fx.native_source_plan()
    # final_commit is a descriptive guarantee; it must NOT be converted to a
    # checkpoint policy or to the queue proof's final_commit field.
    with _native_run(workdir / "run", ACTION) as (run, action):
        compile_native_topology(action, source, plan,
                                decision=_decision(recovery="final_commit"))
    _, placement = load_native_execution(
        RunContext(workdir / "run"), ACTION,
        execution_id=identity([ACTION, "driver", "native_relational"]))
    assert placement.recovery == "final_commit"


def test_compile_refuses_invalid_backend_at_seal(monkeypatch, workdir):
    _install_f004(monkeypatch, [])
    _install_l044_seal(monkeypatch)
    source, plan = fx.native_source_plan()
    with _native_run(workdir / "run", ACTION) as (run, action):
        with pytest.raises(PlanContractError) as exc:
            compile_native_topology(action, source, plan, decision=_decision(backend="bogus"))
        assert exc.value.reason == "invalid_contract"


def test_compile_propagates_compiler_failure(monkeypatch, workdir):
    _install_l044_seal(monkeypatch)
    module = ModuleType("demiflow.planning.dataset_dag")

    def boom(**kwargs):
        raise PlanContractError("invalid_contract", "dataset dag compile failed")

    module.compile_dataset_logical_dag = boom
    monkeypatch.setitem(sys.modules, "demiflow.planning.dataset_dag", module)
    source, plan = fx.native_source_plan()
    with _native_run(workdir / "run", ACTION) as (run, action):
        with pytest.raises(PlanContractError) as exc:
            compile_native_topology(action, source, plan, decision=_decision())
        assert exc.value.reason == "invalid_contract"
    # the failure happened before any seal: no F1 was fabricated
    with pytest.raises(PlanContractError):
        load_sealed_plan(workdir / "run", ACTION)