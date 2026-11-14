"""L020 unit: load_worker_layout never returns a driver execution as a worker (C08).

execution_bindings.v2 is additive: an ExecutionPlacement may be a driver entry.
The guard added after the group lookup must reject a driver placement even when
its group_id collides with a worker group, while a v1 worker (no placement kind)
still loads unchanged. The upstream sealed-artifact loaders are unchanged baseline
and are faked here so the real load_worker_layout body -- including the new guard
-- executes against a controlled layout.
"""
import pathlib
import types

import pytest

import demiflow

assert pathlib.Path(demiflow.__file__).resolve().is_relative_to(
    pathlib.Path(__file__).resolve().parents[1]), demiflow.__file__

from demiflow.execution import plan_interpreter, sealed_plans
from demiflow.execution.plan_interpreter import PlanContractError
from demiflow.execution.worker.layout import load_worker_layout

_BUDGETS = {"inflight_rows": 4, "inflight_bytes": 100, "output_bytes": 200}


def _placement(group_id, *, kind="worker", with_kind=True):
    attrs = {"group_id": group_id, "stage_ids": ["s1"],
             "operator_bindings": [{"stage_id": "s1", "program_index": 0}]}
    if with_kind:
        attrs["placement"] = kind
    return types.SimpleNamespace(**attrs)


def _layout(placement):
    return types.SimpleNamespace(
        workers=[placement],
        stages=[{"stage_id": "s1", "budgets": dict(_BUDGETS)}],
        checkpoint="CK-POLICY")


@pytest.fixture
def wired(monkeypatch):
    holder = {}

    def install(placement):
        layout = _layout(placement)
        holder["layout"] = layout
        monkeypatch.setattr(sealed_plans, "load_sealed_plan",
                            lambda directory, action_id: {
                                "facts": {"artifact_manifest_ref": "M"},
                                "logical_dag": {}, "physical_plan": {}})
        monkeypatch.setattr(sealed_plans, "load_execution_bindings",
                            lambda directory, action_id, *, sealed_plan: {"ref": "ref-1"})
        monkeypatch.setattr(plan_interpreter, "interpret_physical_plan",
                            lambda logical_dag, physical_plan, *, bindings, manifest_ref: layout)
        return layout

    holder["install"] = install
    return holder


def _launch_spec(group_id="g1"):
    return {"execution_plan": {"action_id": "a1", "group_id": group_id,
                               "bindings_ref": "ref-1"}}


def _authority():
    return types.SimpleNamespace(directory="/tmp/demiflow-l020-fake")


def test_driver_placement_is_rejected_even_on_group_collision(wired):
    wired["install"](_placement("g1", kind="driver"))
    with pytest.raises(PlanContractError) as exc:
        load_worker_layout(_launch_spec("g1"), _authority(), None)
    assert exc.value.reason == "invalid_assignment"
    assert "driver execution is not a worker placement" in exc.value.detail


def test_worker_placement_loads_with_stage_limits(wired):
    placement = _placement("g1", kind="worker")
    wired["install"](placement)
    result = load_worker_layout(_launch_spec("g1"), _authority(), None)
    assert result is not None
    got_placement, checkpoint, stage_limits = result
    assert got_placement is placement
    assert checkpoint == "CK-POLICY"
    assert stage_limits == {0: _BUDGETS}


def test_v1_worker_without_placement_kind_still_loads(wired):
    placement = _placement("g1", with_kind=False)
    wired["install"](placement)
    got_placement, checkpoint, stage_limits = load_worker_layout(
        _launch_spec("g1"), _authority(), None)
    assert got_placement is placement
    assert stage_limits == {0: _BUDGETS}


def test_unknown_group_is_still_rejected(wired):
    wired["install"](_placement("g1", kind="worker"))
    with pytest.raises(PlanContractError) as exc:
        load_worker_layout(_launch_spec("missing"), _authority(), None)
    assert exc.value.reason == "invalid_assignment"
    assert "unknown worker group" in exc.value.detail


def test_no_execution_plan_returns_none(wired):
    assert load_worker_layout({}, _authority(), None) is None
