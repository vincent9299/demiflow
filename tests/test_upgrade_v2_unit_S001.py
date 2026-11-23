"""S001 unit: classify_operator_state is the single C04 state policy.

A hookless instance is never stateless; a class is a deferred factory and is
never constructed; ownership defaults are shared between compile and instance.
"""
from pathlib import Path
import threading

import pytest
import demiflow

from demiflow.execution.compiler_support.operator_policy import classify_operator_state, StatePolicy
from demiflow.execution.recovery_contract import RecoveryContractError

assert Path(demiflow.__file__).resolve().is_relative_to(Path(__file__).resolve().parents[1])


def fn(row):
    return row


class Factory:
    def __init__(self, threshold):
        self.threshold = threshold

    def __call__(self, row):
        return row


class Hooks:
    def __init__(self):
        self.n = 0

    def __call__(self, row):
        self.n += 1
        return row

    def snapshot_state(self):
        return {"n": self.n}

    def restore_state(self, state):
        self.n = state["n"]


class Half:
    def snapshot_state(self):
        return {}


class Counter:
    def __init__(self):
        self.n = 0

    def __call__(self, row):
        self.n += 1
        return row


def test_function_is_stateless():
    p = classify_operator_state(fn, phase="declaration")
    assert p == StatePolicy("function", "none", False, None)
    assert classify_operator_state(len, phase="instance").mode == "function"


def test_class_is_deferred_factory_and_not_constructed():
    p = classify_operator_state(Factory, phase="declaration")
    assert p.mode == "factory" and p.adapter_present is True
    assert p.ownership == "shared_colocated"
    # A factory needing constructor args is still classified without building it.
    assert classify_operator_state(Factory, phase="declaration").reason is None


def test_hooks_instance_keeps_declared_ownership():
    assert classify_operator_state(Hooks(), phase="instance", ownership="partitionable").ownership == "partitionable"
    assert classify_operator_state(Hooks(), phase="instance", ownership="shared_colocated").ownership == "shared_colocated"
    p = classify_operator_state(Hooks(), phase="instance")
    assert p.mode == "hooks" and p.adapter_present is True and p.ownership == "shared_colocated"


def test_half_hook_is_unsupported():
    p = classify_operator_state(Half(), phase="instance")
    assert p.mode == "unsupported" and p.adapter_present is False
    assert "half-declared" in p.reason


def test_hookless_plain_instance_is_auto_json_not_stateless():
    p = classify_operator_state(Counter(), phase="instance")
    assert p.mode == "auto_json" and p.adapter_present is True
    assert p.ownership == "shared_colocated"


def test_auto_json_cannot_be_partitionable():
    p = classify_operator_state(Counter(), phase="instance", ownership="partitionable")
    assert p.mode == "unsupported" and p.adapter_present is False


def test_non_json_dep_is_unsupported():
    class WithDep:
        def __init__(self):
            self.dep = object()
            self.n = 0
    p = classify_operator_state(WithDep(), phase="instance")
    assert p.mode == "unsupported" and "strict JSON" in p.reason


def test_lock_field_is_unsupported():
    class Locky:
        def __init__(self):
            self.lock = threading.Lock()
    assert classify_operator_state(Locky(), phase="instance").mode == "unsupported"


def test_slots_and_property_and_custom_getattribute_unsupported():
    class Slots:
        __slots__ = ("n",)

        def __init__(self):
            self.n = 0

    class Prop:
        def __init__(self):
            self._n = 0

        @property
        def n(self):
            return self._n

    class Custom:
        def __getattribute__(self, name):
            return object.__getattribute__(self, name)

    assert classify_operator_state(Slots(), phase="instance").mode == "unsupported"
    assert classify_operator_state(Prop(), phase="instance").mode == "unsupported"
    assert classify_operator_state(Custom(), phase="instance").mode == "unsupported"


def test_method_target_unwraps_owner():
    h = Hooks()
    p = classify_operator_state(h.__call__, phase="instance")
    assert p.mode == "hooks" and p.adapter_present is True


def test_invalid_phase_and_ownership_rejected():
    with pytest.raises(RecoveryContractError):
        classify_operator_state(fn, phase="bogus")
    with pytest.raises(RecoveryContractError):
        classify_operator_state(fn, phase="instance", ownership="bogus")