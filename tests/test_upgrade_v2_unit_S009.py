"""S009 · _adapter_present — frozen probe; class factory is deferred adaptation."""
import threading

from demiflow.execution.compiler_support.operator_policy import _adapter_present


class Hooked:
    def snapshot_state(self):
        return {}

    def restore_state(self, state):
        pass

    def method(self, row):
        return row


class HalfHook:
    def snapshot_state(self):
        return {}


class Hookless:
    def __init__(self):
        self.lock = threading.Lock()


def _fn(row):
    return row


def test_function_has_no_adapter():
    assert _adapter_present(_fn) is False
    assert _adapter_present(len) is False


def test_class_factory_deferred_adapter_present():
    # Q57: a class is a blueprint; True means deferred platform adaptation is
    # available, not that an instance was verified. Never constructed here.
    assert _adapter_present(Hooked) is True
    assert _adapter_present(Hookless) is True
    assert _adapter_present(HalfHook) is True


def test_instance_requires_both_hooks():
    assert _adapter_present(Hooked()) is True
    assert _adapter_present(HalfHook()) is False
    assert _adapter_present(Hookless()) is False


def test_bound_method_unwraps_owner():
    assert _adapter_present(Hooked().method) is True
    assert _adapter_present(Hookless().__init__) is False