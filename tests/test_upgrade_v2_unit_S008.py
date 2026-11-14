"""S008 · OperatorStates.snapshot — unified hooks/auto_json export, prefix + budget."""
import asyncio
from types import SimpleNamespace

from demiflow.execution.operator_state import OperatorStates


def ctx():
    return SimpleNamespace(limits={'max_state_bytes': 65536})


class Hooked:
    def __init__(self, value=0):
        self.value = value

    def snapshot_state(self):
        return {'value': self.value}

    def restore_state(self, state):
        self.value = state['value']


class PlainData:
    def __init__(self):
        self.count = 3


def test_snapshot_hooks_returns_detached_json():
    s = OperatorStates(ctx())
    actor = Hooked(7)
    asyncio.run(s.register(actor, 'stage'))
    out = asyncio.run(s.snapshot())
    assert out == {'stage': {'value': 7}}
    # Detached: mutating the live actor afterwards does not change the snapshot.
    actor.value = 99
    assert out['stage']['value'] == 7


def test_snapshot_auto_json_uses_schema_envelope():
    s = OperatorStates(ctx())
    asyncio.run(s.register(PlainData(), 'stage'))
    out = asyncio.run(s.snapshot())
    assert out['stage']['schema'] == 'demiflow.auto_state.v1'
    assert out['stage']['fields'] == {'count': 3}


def test_snapshot_prefix_filter_selects_only_matching_stages():
    s = OperatorStates(ctx())
    asyncio.run(s.register(Hooked(1), 'alpha'))
    asyncio.run(s.register(Hooked(2), 'beta'))
    out = asyncio.run(s.snapshot(prefixes={'alpha'}))
    assert set(out) == {'alpha'}


def test_snapshot_shared_owner_exported_once_under_canonical_key():
    s = OperatorStates(ctx())
    actor = Hooked(4)
    asyncio.run(s.register(actor, 'stage-a'))
    asyncio.run(s.register(actor, 'stage-b'))
    out = asyncio.run(s.snapshot())
    assert out == {'stage-a': {'value': 4}}


def test_snapshot_aggregates_into_supplied_budget():
    s = OperatorStates(ctx())
    asyncio.run(s.register(Hooked(1), 'a'))
    asyncio.run(s.register(Hooked(2), 'b'))
    budget = SimpleNamespace(snapshot_bytes=0)
    out = asyncio.run(s.snapshot(budget=budget))
    assert set(out) == {'a', 'b'}
    assert budget.snapshot_bytes > 0


def test_snapshot_empty_registries_returns_empty():
    assert asyncio.run(OperatorStates(ctx()).snapshot()) == {}