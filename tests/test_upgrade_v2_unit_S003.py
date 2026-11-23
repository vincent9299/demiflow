"""S003 unit: restore_auto_state validates fully, then atomically replaces fields."""
from pathlib import Path

import pytest
import demiflow

from demiflow.execution.kernels.operator_state import snapshot_auto_state, restore_auto_state
from demiflow.execution.compiler_support.operator_policy import AUTO_STATE_SCHEMA
from demiflow.execution.recovery_contract import RecoveryContractError

assert Path(demiflow.__file__).resolve().is_relative_to(Path(__file__).resolve().parents[1])


class Counter:
    def __init__(self):
        self.n = 0

    def __call__(self, row):
        self.n += 1
        return row


def test_counter_roundtrip_restores_accumulated_value():
    src = Counter()
    src.n = 42
    state = snapshot_auto_state(src, max_bytes=4096)
    dst = Counter()
    restore_auto_state(dst, state, max_bytes=4096)
    assert dst.n == 42


def test_restore_replaces_field_set_exactly():
    dst = Counter()
    dst.extra = "stale"
    restore_auto_state(dst, {"schema": AUTO_STATE_SCHEMA, "fields": {"n": 3}}, max_bytes=4096)
    assert vars(dst) == {"n": 3}


def test_restore_is_detached():
    dst = Counter()
    state = {"schema": AUTO_STATE_SCHEMA, "fields": {"n": 1, "list": [1, 2]}}
    restore_auto_state(dst, state, max_bytes=4096)
    state["fields"]["list"].append(3)
    assert dst.list == [1, 2]


def test_invalid_state_leaves_owner_unchanged():
    dst = Counter()
    dst.n = 9
    for bad in [
        {"schema": "wrong", "fields": {"n": 1}},
        {"schema": AUTO_STATE_SCHEMA, "fields": {"n": (1, 2)}},
        {"schema": AUTO_STATE_SCHEMA, "fields": {"__class__": int}},
        {"schema": AUTO_STATE_SCHEMA, "fields": {"n": object()}},
        {"schema": AUTO_STATE_SCHEMA, "fields": [1]},
        {"fields": {"n": 1}},
    ]:
        with pytest.raises(RecoveryContractError):
            restore_auto_state(dst, bad, max_bytes=4096)
        assert dst.n == 9, bad


def test_restore_budget_enforced_before_mutation():
    dst = Counter()
    dst.n = 5
    big = {"schema": AUTO_STATE_SCHEMA, "fields": {"n": 1, "blob": "x" * 5000}}
    with pytest.raises(RecoveryContractError):
        restore_auto_state(dst, big, max_bytes=64)
    assert dst.n == 5


def test_restore_rejects_unsupported_owner_shape():
    class Slots:
        __slots__ = ("n",)

        def __init__(self):
            self.n = 0
    with pytest.raises(RecoveryContractError):
        restore_auto_state(Slots(), {"schema": AUTO_STATE_SCHEMA, "fields": {"n": 1}}, max_bytes=4096)


def test_restore_does_not_execute_setters():
    touched = []

    class Setter:
        def __init__(self):
            self._n = 0

        @property
        def n(self):
            return self._n

        @n.setter
        def n(self, value):
            touched.append(value)
    # A property owner is unsupported, so restore must refuse without calling setter.
    with pytest.raises(RecoveryContractError):
        restore_auto_state(Setter(), {"schema": AUTO_STATE_SCHEMA, "fields": {"n": 1}}, max_bytes=4096)
    assert touched == []