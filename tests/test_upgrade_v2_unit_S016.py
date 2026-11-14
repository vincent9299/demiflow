"""S016 · OperatorStates.__init__ — fresh registries, no second authority."""
from types import SimpleNamespace

from demiflow.execution.operator_state import OperatorStates


def test_init_starts_empty_with_restored_default():
    ctx = SimpleNamespace(limits={'max_state_bytes': 4096})
    states = OperatorStates(ctx)
    assert states.context is ctx
    assert states.restored == {}
    assert states.owners == {} and states.identities == {} and states.aliases == {}
    assert states.policies == {}


def test_init_keeps_supplied_restored_mapping():
    ctx = SimpleNamespace(limits={'max_state_bytes': 4096})
    restored = {'stage-a': {'queries': 1}}
    states = OperatorStates(ctx, restored)
    assert states.restored is restored
    # Each instance owns independent registries (no shared class-level state).
    other = OperatorStates(ctx)
    other.owners['x'] = object()
    assert states.owners == {}


def test_init_policies_is_per_instance_cache():
    ctx = SimpleNamespace(limits={'max_state_bytes': 4096})
    a, b = OperatorStates(ctx), OperatorStates(ctx)
    a.policies['k'] = 'hooks'
    assert b.policies == {}