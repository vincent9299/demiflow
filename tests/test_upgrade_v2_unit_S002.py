"""S002 unit: snapshot_auto_state produces a bounded, detached strict-JSON copy."""
from pathlib import Path

import pytest
import demiflow

from demiflow.execution.kernels.operator_state import snapshot_auto_state
from demiflow.execution.compiler_support.operator_policy import AUTO_STATE_SCHEMA
from demiflow.execution.recovery_contract import RecoveryContractError

assert Path(demiflow.__file__).resolve().is_relative_to(Path(__file__).resolve().parents[1])


class Counter:
    def __init__(self):
        self.n = 0
        self.tags = ["a", "b"]
        self.meta = {"k": 1}


def test_snapshot_schema_and_fields():
    c = Counter()
    c.n = 7
    state = snapshot_auto_state(c, max_bytes=4096)
    assert state == {"schema": AUTO_STATE_SCHEMA,
                     "fields": {"n": 7, "tags": ["a", "b"], "meta": {"k": 1}}}


def test_snapshot_is_detached_no_shared_aliases():
    c = Counter()
    state = snapshot_auto_state(c, max_bytes=4096)
    state["fields"]["tags"].append("c")
    state["fields"]["meta"]["k"] = 99
    assert c.tags == ["a", "b"] and c.meta == {"k": 1}


def test_snapshot_preserves_field_set():
    c = Counter()
    del c.meta
    state = snapshot_auto_state(c, max_bytes=4096)
    assert set(state["fields"]) == {"n", "tags"}


def test_snapshot_budget_enforced():
    c = Counter()
    c.tags = ["x" * 5000]
    with pytest.raises(RecoveryContractError):
        snapshot_auto_state(c, max_bytes=64)


def test_snapshot_rejects_unsupported_owner():
    class WithDep:
        def __init__(self):
            self.dep = object()
    with pytest.raises(RecoveryContractError):
        snapshot_auto_state(WithDep(), max_bytes=4096)


def test_snapshot_rejects_invalid_budget():
    with pytest.raises(RecoveryContractError):
        snapshot_auto_state(Counter(), max_bytes=0)
    with pytest.raises(RecoveryContractError):
        snapshot_auto_state(Counter(), max_bytes=-1)


def test_snapshot_rejects_tuple_field_lossless():
    class Tup:
        def __init__(self):
            self.t = (1, 2)
    with pytest.raises(RecoveryContractError):
        snapshot_auto_state(Tup(), max_bytes=4096)