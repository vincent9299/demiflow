"""L031 unit: verify_native_programs locates sub-programs and fail-closes.

The structural path walk, program_id and source_identity_ref checks run against
the real PlanInput/UnionSource/KeyedSource model and real describe/identity. The
source-inclusive code-pin (L042, not yet merged) is injected at the chain boundary
as a fixed-schema double; this is a wiring/unit test, not an integration pass.
"""
import copy

import pytest

from demiflow.data.plan import CallableSpec, LogicalPlan, MapOp
from demiflow.data.sources import ItemsSource
from demiflow.execution import chain
from demiflow.execution.local_kernel import KeyedSource, PlanInput, UnionSource
from demiflow.execution.native_plan import verify_native_programs
from demiflow.execution.plan_interpreter import PlanContractError
from demiflow.execution.recovery_contract import RecoveryContractError, identity
from demiflow.execution.recovery_identity import describe, source_identity
from test_execution_layout import PROGRAM_REF
import _v2_native_fixtures as fx

ACTION = "native-action"


def _fake_pin(plan, *, source=None):
    return {"ops": len(plan.operations), "src": describe(source)}


def _binding_for(source, plan, input_path, *, pin_source=None):
    digest = identity(_fake_pin(plan, source=source if pin_source is None else pin_source))
    return {
        "program_id": identity([ACTION, input_path]),
        "input_path": list(input_path),
        "plan_ref": {**PROGRAM_REF, "code_pin_digest": digest},
        "source_identity_ref": identity(source_identity(source)),
        "sealed_operators": fx.local_operators(plan),
        "operator_bindings": [{"program_index": i, "stage_id": identity(["stage", i])}
                              for i in range(len(plan.operations))],
    }


def test_root_program_verifies(monkeypatch):
    source, plan, ld = fx.native_dag(ACTION)
    monkeypatch.setattr(chain, "plan_code_pin", _fake_pin)
    binding = _binding_for(source, plan, [])
    assert verify_native_programs(source, plan, programs=[binding], action_id=ACTION) is None


def test_nested_union_subprogram_verifies(monkeypatch):
    left = PlanInput(ItemsSource(items=({"x": 1},)), LogicalPlan((MapOp(CallableSpec.create(fx.first)),)))
    right = PlanInput(ItemsSource(items=({"x": 2},)), LogicalPlan((MapOp(CallableSpec.create(fx.second)),)))
    source = UnionSource(inputs=(left, right))
    plan = LogicalPlan()
    monkeypatch.setattr(chain, "plan_code_pin", _fake_pin)
    bindings = [_binding_for(left.source, left.plan, ["source", "inputs", 0]),
                _binding_for(right.source, right.plan, ["source", "inputs", 1])]
    assert verify_native_programs(source, plan, programs=bindings, action_id=ACTION) is None


def test_nested_keyed_subprogram_verifies(monkeypatch):
    left = PlanInput(ItemsSource(items=({"x": 1},)), LogicalPlan((MapOp(CallableSpec.create(fx.first)),)))
    source = KeyedSource("groups", left, ("x",))
    plan = LogicalPlan()
    monkeypatch.setattr(chain, "plan_code_pin", _fake_pin)
    binding = _binding_for(left.source, left.plan, ["source", "left"])
    assert verify_native_programs(source, plan, programs=[binding], action_id=ACTION) is None


def test_missing_source_rejected():
    source, plan, ld = fx.native_dag(ACTION)
    binding = _binding_for(source, plan, [])
    with pytest.raises(PlanContractError) as exc:
        verify_native_programs(None, plan, programs=[binding], action_id=ACTION)
    assert exc.value.reason == "invalid_contract"


def test_wrong_program_id_rejected():
    source, plan, ld = fx.native_dag(ACTION)
    binding = _binding_for(source, plan, [])
    binding["program_id"] = identity(["wrong-program"])
    with pytest.raises(PlanContractError) as exc:
        verify_native_programs(source, plan, programs=[binding], action_id=ACTION)
    assert exc.value.reason == "identity_mismatch"


def test_wrong_source_identity_rejected():
    source, plan, ld = fx.native_dag(ACTION)
    binding = _binding_for(source, plan, [])
    binding["source_identity_ref"] = identity(["wrong-source"])
    with pytest.raises(PlanContractError) as exc:
        verify_native_programs(source, plan, programs=[binding], action_id=ACTION)
    assert exc.value.reason == "identity_mismatch"


def test_missing_input_path_field_rejected():
    source, plan, ld = fx.native_dag(ACTION)
    binding = _binding_for(source, plan, ["source", "nonexistent"])
    with pytest.raises(PlanContractError) as exc:
        verify_native_programs(source, plan, programs=[binding], action_id=ACTION)
    assert exc.value.reason == "invalid_contract"


def test_input_path_non_program_leaf_rejected():
    source, plan, ld = fx.native_dag(ACTION)
    binding = _binding_for(source, plan, ["source"])  # lands on a SourcePlan, not a PlanInput
    with pytest.raises(PlanContractError) as exc:
        verify_native_programs(source, plan, programs=[binding], action_id=ACTION)
    assert exc.value.reason == "invalid_contract"


def test_input_path_out_of_range_rejected():
    left = PlanInput(ItemsSource(items=({"x": 1},)), LogicalPlan())
    source = UnionSource(inputs=(left,))
    binding = _binding_for(left.source, left.plan, ["source", "inputs", 5])
    with pytest.raises(PlanContractError) as exc:
        verify_native_programs(source, LogicalPlan(), programs=[binding], action_id=ACTION)
    assert exc.value.reason == "invalid_contract"


def test_input_path_negative_index_rejected():
    left = PlanInput(ItemsSource(items=({"x": 1},)), LogicalPlan())
    source = UnionSource(inputs=(left,))
    binding = _binding_for(left.source, left.plan, ["source", "inputs", -1])
    with pytest.raises(PlanContractError) as exc:
        verify_native_programs(source, LogicalPlan(), programs=[binding], action_id=ACTION)
    assert exc.value.reason == "invalid_contract"


def test_wrong_code_pin_rejected(monkeypatch):
    source, plan, ld = fx.native_dag(ACTION)
    monkeypatch.setattr(chain, "plan_code_pin", _fake_pin)
    binding = _binding_for(source, plan, [])
    binding["plan_ref"]["code_pin_digest"] = "ef" * 32  # does not match the recomputed pin
    with pytest.raises(RecoveryContractError):
        verify_native_programs(source, plan, programs=[binding], action_id=ACTION)


def test_wrong_sealed_operator_order_rejected(monkeypatch):
    source, plan, ld = fx.native_dag(ACTION)
    monkeypatch.setattr(chain, "plan_code_pin", _fake_pin)
    binding = _binding_for(source, plan, [])
    binding["sealed_operators"] = list(reversed(binding["sealed_operators"]))
    with pytest.raises(RecoveryContractError):
        verify_native_programs(source, plan, programs=[binding], action_id=ACTION)