"""L002 unit: interpret_physical_plan derives v2 ExecutionPlacement read-only.

stage_limits come solely from physical.stages[].budgets (no second wire authority);
profile/programs are recursively frozen and detached; v1 derives no executions.
"""
import copy
from types import MappingProxyType

import pytest

from demiflow.execution.plan_interpreter import (
    ExecutionPlacement, interpret_physical_plan, to_wire,
)
from test_execution_layout import chain_fixture
import _v2_native_fixtures as fx


def _layout(n):
    return interpret_physical_plan(n.ld, n.pp, bindings=n.bundle, manifest_ref=n.mref)


def test_v2_executions_populated_item_by_item():
    n = fx.native_v2_doc(backend="datafusion", recovery="final_commit")
    layout = _layout(n)
    assert len(layout.executions) == 1
    entry = layout.executions[0]
    assert isinstance(entry, ExecutionPlacement)
    assert entry.execution_id == n.execution_id
    assert entry.placement == "driver"
    assert entry.group_id is None
    assert entry.backend == "datafusion"
    assert entry.recovery == "final_commit"
    assert list(entry.stage_ids) == n.op_stages


def test_stage_limits_derive_only_from_physical_budgets():
    n = fx.native_v2_doc()
    entry = _layout(n).executions[0]
    expected = {stage["stage_id"]: stage["budgets"] for stage in n.pp["stages"]
                if stage["stage_id"] in n.op_stages}
    assert to_wire(entry.stage_limits) == expected
    assert set(entry.stage_limits) == set(n.op_stages)


def test_profile_and_programs_are_frozen_and_detached():
    n = fx.native_v2_doc()
    entry = _layout(n).executions[0]
    assert isinstance(entry.profile, MappingProxyType)
    assert isinstance(entry.programs, tuple)
    assert isinstance(entry.programs[0], MappingProxyType)
    assert isinstance(entry.stage_limits, MappingProxyType)
    # mutating the source document cannot reach the interpreted placement
    n.doc["profiles"][next(iter(n.doc["profiles"]))]["requests"]["cpu_millis"] = 1
    assert entry.profile["requests"]["cpu_millis"] == 1000


def test_v1_derives_no_executions():
    v1 = chain_fixture()
    layout = interpret_physical_plan(v1.ld, v1.pp, bindings=v1.bundle, manifest_ref=v1.mref)
    assert layout.executions == ()
    assert len(layout.workers) == 1  # v1 workers unchanged


def test_repeated_interpretation_equal():
    n = fx.native_v2_doc()
    assert _layout(n) == _layout(n)


def test_interpretation_has_no_side_effects():
    n = fx.native_v2_doc()
    ld_before, pp_before, doc_before = copy.deepcopy(n.ld), copy.deepcopy(n.pp), copy.deepcopy(n.doc)
    _layout(n)
    assert n.ld == ld_before and n.pp == pp_before and n.doc == doc_before