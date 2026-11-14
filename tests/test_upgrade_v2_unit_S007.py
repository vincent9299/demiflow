"""S007 · OperatorStates.register — single C04 policy, hooks/auto_json, no drop of data."""
import asyncio
import threading
from types import SimpleNamespace

import pytest

from demiflow.execution.operator_state import OperatorStates
from demiflow.execution.recovery_contract import RecoveryContractError


def ctx():
    return SimpleNamespace(limits={'max_state_bytes': 65536})


class Hooked:
    def __init__(self):
        self.value = 0
        self.restored = None

    def __call__(self, row):
        self.value += 1
        return row

    def snapshot_state(self):
        return {'value': self.value}

    def restore_state(self, state):
        self.restored = state
        self.value = state['value']


class PlainData:
    def __init__(self):
        self.count = 5
        self.name = 'x'

    def __call__(self, row):
        return row


class Locked:
    def __init__(self):
        self.lock = threading.Lock()

    def __call__(self, row):
        return row


class HalfHook:
    def snapshot_state(self):
        return {}


def _fn(row):
    return row


def test_register_skips_function_builtin_class():
    s = OperatorStates(ctx())
    for owner in (_fn, len, Hooked):
        asyncio.run(s.register(owner, 'k'))
    assert s.owners == {} and s.policies == {}


def test_register_hooks_fresh_snapshots_initial_state():
    s = OperatorStates(ctx())
    actor = Hooked()
    asyncio.run(s.register(actor, 'stage'))
    assert s.owners['stage'] is actor
    assert s.policies['stage'] == 'hooks'
    assert s.aliases['stage'] == 'stage'
    assert s.identities[id(actor)] == 'stage'


def test_register_hooks_restored_calls_restore_state():
    actor = Hooked()
    s = OperatorStates(ctx(), restored={'stage': {'value': 9}})
    asyncio.run(s.register(actor, 'stage'))
    assert actor.restored == {'value': 9} and actor.value == 9
    assert s.policies['stage'] == 'hooks'


def test_register_auto_json_captures_plain_data():
    s = OperatorStates(ctx())
    actor = PlainData()
    asyncio.run(s.register(actor, 'stage'))
    assert s.policies['stage'] == 'auto_json'
    assert s.owners['stage'] is actor


def test_register_auto_json_restored_replaces_fields():
    actor = PlainData()
    saved = {'schema': 'demiflow.auto_state.v1', 'fields': {'count': 42, 'name': 'y'}}
    s = OperatorStates(ctx(), restored={'stage': saved})
    asyncio.run(s.register(actor, 'stage'))
    assert actor.count == 42 and actor.name == 'y'
    assert s.policies['stage'] == 'auto_json'


def test_register_drops_unsupported_resource_instance():
    s = OperatorStates(ctx())
    actor = Locked()
    asyncio.run(s.register(actor, 'stage'))
    # A locked hookless instance bears resources it cannot serialize: it is not
    # registered as a state participant (plain execution may still run it).
    assert s.owners == {} and s.policies == {}


def test_register_rejects_half_hook():
    with pytest.raises(RecoveryContractError):
        asyncio.run(OperatorStates(ctx()).register(HalfHook(), 'stage'))


def test_register_shared_owner_snapshots_once_via_alias():
    s = OperatorStates(ctx())
    actor = Hooked()
    asyncio.run(s.register(actor, 'stage-a'))
    asyncio.run(s.register(actor, 'stage-b'))
    assert len(s.owners) == 1
    assert s.aliases['stage-b'] == 'stage-a'
    assert s.identities[id(actor)] == 'stage-a'