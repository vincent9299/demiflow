"""L003 unit: validate_worker_assignment refuses driver masquerading as worker.

A driver execution_id/group_id must never be accepted as a worker group, and a
None group_id is not a worker. Uses a v1 layout (real workers) with an injected
driver ExecutionPlacement to exercise the guard deterministically.
"""
import dataclasses
from types import MappingProxyType

import pytest

from demiflow.execution.seal.plan_interpreter import ExecutionPlacement, PlanContractError, interpret_physical_plan, validate_worker_assignment
from test_execution_layout import (
    assignment_for, chain_fixture, expected_channel_ids,
)


def _driver(execution_id):
    return ExecutionPlacement(
        execution_id=execution_id, placement="driver", group_id=None,
        backend="local_stream", stage_ids=(), profile=MappingProxyType({}),
        programs=(), stage_limits=MappingProxyType({}), recovery="native_checkpoint")


def _v1_layout():
    v1 = chain_fixture()
    return interpret_physical_plan(v1.ld, v1.pp, bindings=v1.bundle, manifest_ref=v1.mref)


def _assignment(layout, group_id):
    worker = next(w for w in layout.workers if w.group_id == group_id)
    inputs, outputs = expected_channel_ids(layout, worker)
    return assignment_for(layout, worker, sorted(inputs), sorted(outputs))


def _reason(exc):
    assert isinstance(exc.value, PlanContractError)
    return exc.value.reason


def test_driver_execution_id_cannot_be_a_worker_group():
    layout = dataclasses.replace(_v1_layout(), executions=(_driver("g0"),))
    with pytest.raises(PlanContractError) as exc:
        validate_worker_assignment(layout, _assignment(layout, "g0"), group_id="g0")
    assert _reason(exc) == "invalid_assignment"


def test_non_colliding_worker_group_accepted():
    layout = dataclasses.replace(_v1_layout(), executions=(_driver("driver-exec"),))
    placement = validate_worker_assignment(layout, _assignment(layout, "g0"), group_id="g0")
    assert placement.group_id == "g0"


def test_none_group_id_rejected():
    layout = _v1_layout()
    with pytest.raises(PlanContractError) as exc:
        validate_worker_assignment(layout, _assignment(layout, "g0"), group_id=None)
    assert _reason(exc) == "invalid_assignment"


def test_v1_without_executions_unaffected():
    layout = _v1_layout()
    assert layout.executions == ()
    placement = validate_worker_assignment(layout, _assignment(layout, "g0"), group_id="g0")
    assert placement.group_id == "g0"