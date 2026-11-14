"""S006 · OperatorStates.validate — only a half-declared adapter is always invalid."""
import threading
from types import SimpleNamespace

import pytest

from demiflow.execution.operator_state import OperatorStates
from demiflow.execution.recovery_contract import RecoveryContractError


def states():
    return OperatorStates(SimpleNamespace(limits={'max_state_bytes': 4096}))


class Hooked:
    def snapshot_state(self):
        return {}

    def restore_state(self, state):
        pass


class HalfHook:
    def snapshot_state(self):
        return {}


class HalfRestore:
    def restore_state(self, state):
        pass


class HooklessData:
    def __init__(self):
        self.count = 0


class HooklessLocked:
    def __init__(self):
        self.lock = threading.Lock()


def _fn(row):
    return row


def test_validate_accepts_function_builtin_class():
    s = states()
    s.validate(_fn)
    s.validate(len)
    s.validate(Hooked)          # factory blueprint, exempt until constructed
    s.validate(HooklessData)


def test_validate_accepts_full_hooks_and_hookless_instances():
    s = states()
    # A hookless instance is NOT presumed stateless here; recoverability is
    # decided by the single C04 policy at register, not by validate.
    s.validate(Hooked())
    s.validate(HooklessData())
    s.validate(HooklessLocked())


@pytest.mark.parametrize('owner', [HalfHook(), HalfRestore()])
def test_validate_rejects_half_declared_adapter(owner):
    with pytest.raises(RecoveryContractError, match='both snapshot_state/restore_state'):
        states().validate(owner)