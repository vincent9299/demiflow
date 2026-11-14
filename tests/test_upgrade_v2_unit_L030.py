"""L030 unit: stage_native_programs seals each slice as a code-pin ProgramBinding.

The source/program_id-aware seal_write_plan_ref (L044, not yet merged) is injected
at the chain boundary as a fixed-schema double that records its keywords. The
binding assembly, source_identity_ref and operator mapping are this group's real
logic. ProgramSlice is a test-domain double (F004's runtime dataclass is unmerged).
operator_bindings arrive stage_id-resolved: F004 emits full-graph occurrence_id and
L007 (compile_native_topology) resolves it to stage_id before this call, since
stage_native_programs has no dag authority. The pass-through below is intentional.
"""
from types import SimpleNamespace

import pytest

from demiflow.data.plan import CallableSpec, LogicalPlan, MapOp
from demiflow.data.sources import ItemsSource
from demiflow.execution import chain
from demiflow.execution.native_plan import stage_native_programs
from demiflow.execution.recovery_contract import identity
from demiflow.execution.recovery_identity import describe
from test_execution_layout import PROGRAM_REF
import _v2_native_fixtures as fx

ACTION = "native-action"


def _slice(input_path, plan, source):
    return SimpleNamespace(
        program_id=identity([ACTION, list(input_path)]), input_path=tuple(input_path),
        source=source, plan=plan, sealed_operators=tuple(fx.local_operators(plan)),
        operator_bindings=tuple({"program_index": i, "stage_id": identity(["stage", i])}
                                for i in range(len(plan.operations))))


def _fake_seal_factory(calls):
    def fake_seal(action, plan, *, source=None, program_id=None):
        ref = {**PROGRAM_REF, "code_pin_digest": identity({"ops": len(plan.operations),
                                                         "src": describe(source)})}
        calls.append({"action": action, "plan": plan, "source": source,
                      "program_id": program_id, "returned": ref})
        return ref
    return fake_seal


def test_stage_binds_each_slice(monkeypatch):
    calls = []
    monkeypatch.setattr(chain, "seal_write_plan_ref", _fake_seal_factory(calls))
    source, plan, ld = fx.native_dag(ACTION)
    slices = (_slice([], plan, source),)
    action = SimpleNamespace(action_id=ACTION)
    bindings = stage_native_programs(action, slices)
    assert isinstance(bindings, tuple) and len(bindings) == 1
    binding = bindings[0]
    assert set(binding) == {"program_id", "input_path", "plan_ref", "source_identity_ref",
                            "sealed_operators", "operator_bindings"}
    assert binding["program_id"] == slices[0].program_id
    assert binding["input_path"] == []
    assert binding["source_identity_ref"] == identity(describe(source))
    assert binding["sealed_operators"] == [dict(op) for op in slices[0].sealed_operators]
    assert binding["operator_bindings"] == [dict(ob) for ob in slices[0].operator_bindings]
    # plan_ref is the seal's return value verbatim (source-inclusive code-pin)
    assert binding["plan_ref"] == calls[0]["returned"]
    assert binding["plan_ref"]["code_pin_digest"] == identity({"ops": len(plan.operations),
                                                               "src": describe(source)})
    # the seal was called with the slice's own source and program_id
    assert calls[0]["source"] is source
    assert calls[0]["program_id"] == slices[0].program_id
    assert calls[0]["plan"] is plan


def test_stage_multiple_slices_unique_paths(monkeypatch):
    calls = []
    monkeypatch.setattr(chain, "seal_write_plan_ref", _fake_seal_factory(calls))
    left = _slice(["source", "inputs", 0], LogicalPlan((MapOp(CallableSpec.create(fx.first)),)),
                  ItemsSource(items=({"x": 1},)))
    right = _slice(["source", "inputs", 1], LogicalPlan((MapOp(CallableSpec.create(fx.second)),)),
                   ItemsSource(items=({"x": 2},)))
    bindings = stage_native_programs(SimpleNamespace(action_id=ACTION), (left, right))
    assert [b["input_path"] for b in bindings] == [["source", "inputs", 0], ["source", "inputs", 1]]
    assert len({b["program_id"] for b in bindings}) == 2
    assert [c["program_id"] for c in calls] == [left.program_id, right.program_id]


def test_stage_empty_slices(monkeypatch):
    monkeypatch.setattr(chain, "seal_write_plan_ref", _fake_seal_factory([]))
    assert stage_native_programs(SimpleNamespace(action_id=ACTION), ()) == ()