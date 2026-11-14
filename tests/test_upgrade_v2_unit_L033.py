"""L033 · _WriteSeal.verify — v1 keeps verify_loaded_plan, v2 verifies every program.

Binds: a v1 seal still re-derives the code-pin and the sealed local operator order
through the original ``verify_loaded_plan`` and refuses an edited plan; a v2 native
seal dispatches to ``verify_native_programs`` with the *whole* sealed programs
tuple (root and every sub-program), the bound fixed source and the live plan under
the parent action id — never a root-only slice. A native refusal propagates
unchanged and the v1 verifier is not consulted on the v2 path.

The v1 cases use the real sealer, real journal and real carrier bytes. The v2
cases use a recording C08 contract sample for the absent ``native_plan`` module:
they prove this method's dispatch and argument coverage, not L031's verification
semantics, so they are interface-level evidence rather than an integration pass.
"""
import ast
import inspect
import sys
import textwrap
from types import SimpleNamespace

import pytest

import demiflow.data as data
from demiflow.data.plan import CallableSpec, LogicalPlan, MapOp
from demiflow.data.sources import ItemsSource
from demiflow.execution import chain
from demiflow.execution.chain import _WriteSeal, seal_write_topology
from demiflow.execution.plan_interpreter import CheckpointPolicy
from demiflow.execution.recovery_contract import RecoveryContractError, identity
from demiflow.execution.run_context import RunContext, RunAction

from _v2_write_seal_fixtures import (
    NativePlanRecorder, SampleExecution, SampleLayout, freeze_programs, program_binding,
)

CHECKPOINT = CheckpointPolicy(checkpoint_rows=8, interval_ms=1000,
                              unconfirmed_rows=4, unconfirmed_bytes=4096)


def _double(row):
    return {"k": row["k"], "v": row["v"] * 2}


def _plan():
    return LogicalPlan((MapOp(CallableSpec.create(_double)),))


@pytest.fixture
def sealed(tmp_path):
    with RunContext.start(tmp_path / "run") as run:
        declaration = {"kind": "write-lance", "source": {"source_id": "fixed"}}
        run.journal.bind_execution_action(run.fence, "write-action", declaration)
        action = RunAction(run, "write-action", declaration)
        with data.local_execution(workers=1, worker_mode="thread", partitions=1):
            try:
                yield SimpleNamespace(run=run, action=action)
            finally:
                record = run.journal.execution_action("write-action")
                if record is not None and record["result"] is None:
                    run.journal.complete_execution_action(
                        run.fence, "write-action", {"kind": "test-complete"})


@pytest.fixture
def source():
    return ItemsSource(items=({"k": "a", "v": 1},))


def _native_seal(action, source, programs, *, verify_error=None, execution_id="exec-1"):
    placement = SampleExecution(execution_id=execution_id, placement="driver",
                                programs=freeze_programs(programs))
    layout = SampleLayout(checkpoint=CHECKPOINT, executions=(placement,))
    recorder = NativePlanRecorder(layout=layout, placement=placement,
                                  verify_error=verify_error)
    seal = _WriteSeal(action, None, {"bindings_ref": {"path": "b", "sha256": "s"},
                                     "operators": []},
                      source=source, native_layout=layout,
                      native_execution_id=execution_id)
    return seal, recorder


def _programs(action_id):
    return [program_binding(program_id=identity([action_id, []]), input_path=[],
                            sealed_operators=[{"logical_node_id": "n0", "occurrence_id": "o0"}],
                            operator_bindings=[{"program_index": 0, "stage_id": "stage-0"}]),
            program_binding(program_id=identity([action_id, ["source", "left"]]),
                            input_path=["source", "left"],
                            sealed_operators=[{"logical_node_id": "n0", "occurrence_id": "o1"}],
                            operator_bindings=[{"program_index": 0, "stage_id": "stage-1"}]),
            program_binding(program_id=identity([action_id, ["source", "inputs", 0]]),
                            input_path=["source", "inputs", 0],
                            sealed_operators=[{"logical_node_id": "n0", "occurrence_id": "o2"}],
                            operator_bindings=[{"program_index": 0, "stage_id": "stage-2"}])]


# --- v1: the original verifier, real seal -------------------------------------

def test_v1_seal_verifies_the_executed_plan(sealed, source):
    plan = _plan()
    seal = seal_write_topology(sealed.action, source, plan)
    assert seal._native_placement is None
    assert seal.verify(plan) is None


def test_v1_seal_refuses_an_edited_plan(sealed, source):
    plan = _plan()
    seal = seal_write_topology(sealed.action, source, plan)

    def _triple(row):
        return {"k": row["k"], "v": row["v"] * 3}

    with pytest.raises(RecoveryContractError):
        seal.verify(LogicalPlan((MapOp(CallableSpec.create(_triple)),)))


def test_v1_verify_does_not_import_the_native_seam(sealed, source, monkeypatch):
    monkeypatch.delitem(sys.modules, "demiflow.execution.native_plan", raising=False)
    plan = _plan()
    seal = seal_write_topology(sealed.action, source, plan)
    seal.verify(plan)
    assert "demiflow.execution.native_plan" not in sys.modules


def test_v1_verify_uses_verify_loaded_plan(sealed, source, monkeypatch):
    plan = _plan()
    seal = seal_write_topology(sealed.action, source, plan)
    calls = []
    original = chain.verify_loaded_plan

    def spy(loaded, plan_ref, operators):
        calls.append((loaded, plan_ref, operators))
        return original(loaded, plan_ref, operators)

    monkeypatch.setattr(chain, "verify_loaded_plan", spy)
    seal.verify(plan)
    assert len(calls) == 1
    assert calls[0][0] is plan
    assert calls[0][2] == seal.topology["operators"]


# --- v2: every sealed sub-program, not just the root --------------------------

def test_v2_verify_passes_every_program(sealed, source, monkeypatch):
    programs = _programs(sealed.action.action_id)
    seal, recorder = _native_seal(sealed.action, source, programs)
    recorder.install(monkeypatch)
    plan = _plan()
    assert seal.verify(plan) is None
    assert len(recorder.verify_calls) == 1
    call = recorder.verify_calls[0]
    assert call["programs"] is seal._native_placement.programs
    assert len(call["programs"]) == 3
    assert [list(p["input_path"]) for p in call["programs"]] == [
        [], ["source", "left"], ["source", "inputs", 0]]
    assert call["source"] is source
    assert call["plan"] is plan
    assert call["action_id"] == sealed.action.action_id


def test_v2_verify_covers_a_sub_program_only_layout(sealed, source, monkeypatch):
    programs = _programs(sealed.action.action_id)[1:]
    seal, recorder = _native_seal(sealed.action, source, programs)
    recorder.install(monkeypatch)
    seal.verify(_plan())
    assert [list(p["input_path"]) for p in recorder.verify_calls[0]["programs"]] == [
        ["source", "left"], ["source", "inputs", 0]]


def test_v2_verify_does_not_call_the_v1_verifier(sealed, source, monkeypatch):
    seal, recorder = _native_seal(sealed.action, source,
                                  _programs(sealed.action.action_id))
    recorder.install(monkeypatch)

    def forbidden(*args, **kwargs):
        raise AssertionError("v2 verify must not consult verify_loaded_plan")

    monkeypatch.setattr(chain, "verify_loaded_plan", forbidden)
    seal.verify(_plan())
    assert len(recorder.verify_calls) == 1


def test_v2_verify_propagates_a_native_refusal(sealed, source, monkeypatch):
    refusal = RecoveryContractError("Native program code-pin differs")
    seal, recorder = _native_seal(sealed.action, source,
                                  _programs(sealed.action.action_id),
                                  verify_error=refusal)
    recorder.install(monkeypatch)
    with pytest.raises(RecoveryContractError) as excinfo:
        seal.verify(_plan())
    assert excinfo.value is refusal


def test_v2_verify_is_read_only(sealed, source, monkeypatch):
    seal, recorder = _native_seal(sealed.action, source,
                                  _programs(sealed.action.action_id))
    recorder.install(monkeypatch)
    journal = sealed.run.journal.path
    before = journal.read_bytes() if journal.is_file() else b""
    seal.verify(_plan())
    after = journal.read_bytes() if journal.is_file() else b""
    assert after == before
    assert source.items == ({"k": "a", "v": 1},)


def test_v2_verify_with_no_programs_still_dispatches(sealed, source, monkeypatch):
    seal, recorder = _native_seal(sealed.action, source, [])
    recorder.install(monkeypatch)
    seal.verify(_plan())
    assert recorder.verify_calls[0]["programs"] == ()


def test_v2_verify_argument_shape_is_the_whole_placement_programs():
    tree = ast.parse(textwrap.dedent(inspect.getsource(_WriteSeal.verify)))
    calls = [node for node in ast.walk(tree)
             if isinstance(node, ast.Call)
             and getattr(node.func, "id", None) == "verify_native_programs"]
    assert len(calls) == 1
    keywords = {kw.arg: ast.unparse(kw.value) for kw in calls[0].keywords}
    assert keywords["programs"] == "self._native_placement.programs"
    assert keywords["action_id"] == "self._action.action_id"
    assert [ast.unparse(a) for a in calls[0].args] == ["self._source", "plan"]