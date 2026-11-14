"""S005 · precheck_plan_state — whole-plan startup gate via unified traits."""
import threading

import pytest

from demiflow.data.plan import (
    CallableSpec, LogicalPlan, MapOp, MapStreamOptions, LimitOp,
)
from demiflow.execution.operator_precheck import precheck_plan_state
from demiflow.execution.contracts.errors import ProtocolError


class Hooked:
    def __call__(self, row):
        return row

    def snapshot_state(self):
        return {}

    def restore_state(self, state):
        pass


class PlainData:
    def __init__(self):
        self.count = 0

    def __call__(self, row):
        return row


class EmptyHookless:
    def __call__(self, row):
        return row


class Locked:
    def __init__(self):
        self.lock = threading.Lock()

    def __call__(self, row):
        return row


def _fn(row):
    return row


def mapop(target, concurrency=1):
    return MapOp(callable=CallableSpec.create(target),
                 stream_options=MapStreamOptions(concurrency=concurrency))


def test_plan_of_functions_passes():
    precheck_plan_state(LogicalPlan((mapop(_fn), mapop(_fn))))


def test_plan_hooks_instance_passes_via_shared_colocated():
    precheck_plan_state(LogicalPlan((mapop(Hooked()),)))


def test_plan_class_factory_deferred_passes():
    precheck_plan_state(LogicalPlan((mapop(PlainData),)))


def test_plan_empty_hookless_instance_passes():
    precheck_plan_state(LogicalPlan((mapop(EmptyHookless()),)))


def test_plan_hookless_data_enforces_stateless_declaration_boundary():
    # unified traits keep a hookless instance at the caller's 'none'; the
    # precheck rejects undeclared cross-row state before any worker starts.
    with pytest.raises(ProtocolError) as e:
        precheck_plan_state(LogicalPlan((mapop(PlainData()),)))
    assert e.value.code == 'STATE_ADAPTER_MISSING'


def test_plan_locked_instance_rejected():
    with pytest.raises(ProtocolError):
        precheck_plan_state(LogicalPlan((mapop(Locked()),)))


def test_plan_non_callable_operations_skipped():
    precheck_plan_state(LogicalPlan((LimitOp(limit=3), mapop(_fn))))


def test_empty_plan_passes():
    precheck_plan_state(LogicalPlan(()))