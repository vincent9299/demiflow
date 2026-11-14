"""S004 · precheck_operator_state — same C04 policy as compile, before workers."""
import threading

import pytest

from demiflow.data.plan import CallableSpec
from demiflow.execution.operator_precheck import precheck_operator_state
from demiflow.execution.contracts.errors import ProtocolError


class Hooked:
    def __call__(self, row):
        return row

    def snapshot_state(self):
        return {}

    def restore_state(self, state):
        pass


class EmptyHookless:
    def __call__(self, row):
        return row


class PlainData:
    def __init__(self):
        self.count = 0

    def __call__(self, row):
        return row


class Locked:
    def __init__(self):
        self.lock = threading.Lock()

    def __call__(self, row):
        return row


class HalfHook:
    def __call__(self, row):
        return row

    def snapshot_state(self):
        return {}


def _fn(row):
    return row


def spec(target):
    return CallableSpec.create(target)


def test_function_and_empty_instance_admissible_under_none():
    assert precheck_operator_state(spec(_fn), {}) is False
    assert precheck_operator_state(spec(EmptyHookless()), {}) is False


def test_class_factory_deferred_adapter_present():
    # Q57: a hookless class factory passes declaration with deferred adaptation.
    assert precheck_operator_state(spec(PlainData), {'state_ownership': 'shared_colocated'}) is True


def test_hooks_instance_admits_any_ownership():
    assert precheck_operator_state(spec(Hooked()), {'state_ownership': 'shared_colocated'}) is True
    assert precheck_operator_state(spec(Hooked()), {'state_ownership': 'partitionable'}) is True


def test_hookless_data_under_none_rejected():
    with pytest.raises(ProtocolError) as e:
        precheck_operator_state(spec(PlainData()), {})
    assert e.value.code == 'STATE_ADAPTER_MISSING'


def test_hookless_data_auto_json_shared_colocated_concurrency_one():
    assert precheck_operator_state(
        spec(PlainData()), {'state_ownership': 'shared_colocated', 'concurrency': 1}) is True


def test_hookless_data_auto_json_higher_concurrency_rejected():
    with pytest.raises(ProtocolError, match='concurrency 1'):
        precheck_operator_state(
            spec(PlainData()), {'state_ownership': 'shared_colocated', 'concurrency': 2})


def test_locked_instance_rejected_under_stateful_ownership():
    with pytest.raises(ProtocolError) as e:
        precheck_operator_state(spec(Locked()), {'state_ownership': 'shared_colocated'})
    assert e.value.code == 'STATE_ADAPTER_MISSING'


def test_partitionable_hookless_data_needs_native_adapter():
    with pytest.raises(ProtocolError):
        precheck_operator_state(spec(PlainData()), {'state_ownership': 'partitionable'})


def test_half_hook_rejected():
    with pytest.raises(ProtocolError):
        precheck_operator_state(spec(HalfHook()), {'state_ownership': 'shared_colocated'})