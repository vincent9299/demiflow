"""S010 · compile_operator_spec — C04 state admission in the durable protocol."""
import threading

import pytest

from demiflow.data.plan import CallableSpec
from demiflow.execution.contracts import operator
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


KW = dict(logical_node_id="n1", occurrence_id="o1",
          artifact_manifest_ref={"manifest_id": "m" * 64, "sha256": "a" * 64},
          module_key="p_" + "b" * 24)


def compile(target, ownership="none"):
    return operator.compile_operator_spec(
        CallableSpec.create(target), traits={"state_ownership": ownership}, **KW)


def test_function_compiles_stateless():
    spec = compile(_fn)
    assert spec["state"] == {"ownership": "none", "adapter_present": False}


def test_class_factory_deferred_compiles_shared_colocated():
    spec = compile(PlainData, "shared_colocated")
    assert spec["state"] == {"ownership": "shared_colocated", "adapter_present": True}


def test_hooks_instance_compiles_partitionable():
    spec = compile(Hooked(), "partitionable")
    assert spec["state"] == {"ownership": "partitionable", "adapter_present": True}


def test_hookless_data_auto_json_compiles_shared_colocated():
    spec = compile(PlainData(), "shared_colocated")
    assert spec["state"] == {"ownership": "shared_colocated", "adapter_present": True}


def test_empty_hookless_instance_compiles_under_none():
    spec = compile(EmptyHookless())
    assert spec["state"] == {"ownership": "none", "adapter_present": False}


def test_hookless_data_under_none_rejected():
    with pytest.raises(ProtocolError) as e:
        compile(PlainData())
    assert e.value.code == "STATE_ADAPTER_MISSING"


def test_locked_instance_stateful_rejected():
    with pytest.raises(ProtocolError) as e:
        compile(Locked(), "shared_colocated")
    assert e.value.code == "STATE_ADAPTER_MISSING"


def test_hookless_data_partitionable_needs_native_adapter():
    with pytest.raises(ProtocolError) as e:
        compile(PlainData(), "partitionable")
    assert e.value.code == "STATE_ADAPTER_MISSING"


def test_compiled_spec_validates_and_binds_spec_id():
    spec = compile(Hooked(), "shared_colocated")
    assert operator.validate_operator_spec(spec) is spec
    assert spec["spec_id"] == operator.spec_id(spec)