"""W004 · overlay_sealed assigns binding_level per action, honestly.

C09: each action entry carries binding_level — 'sealed' for a valid engine-sealed
plan, 'declared' for the static-report projection fallback (F1 absent). An
'unavailable' entry (corrupt/oversize/identity-mismatch carrier) gets NO
binding_level: it is neither a static declared report nor a valid sealed plan, so
labelling it either way would be pseudo-evidence. view.basis stays 'executed' /
'declared' for compatibility (executed == run sealed source, NOT success).

All cases run on a REAL sealed run from the genuine engine seam reused from
test_workbench_sealed_facts; the corrupt case flips one carrier byte and restores
it. No mock journal/interpreter. Test data only under tmp_path (/tmp/demiflow-*).
"""
from test_workbench_sealed_facts import sealed, private_runtime_domain, _declared, _carrier  # noqa: F401

from demiflow.workbench.runs import facts


def test_sealed_and_declared_levels_coexist(sealed):
    declared, _, _ = _declared(('n3', 'n4'))
    view = facts.overlay_sealed(sealed.root, sealed.run_id, declared)
    by = {e['action_id']: e for e in view['actions']}
    assert by['n3']['binding_level'] == 'sealed'      # genuine F1 present
    assert by['n3']['status'] == 'sealed'
    assert by['n4']['binding_level'] == 'declared'    # F1 absent -> static report
    assert by['n4']['status'] == 'projected'
    assert by['n4']['fallback']['reason'] == facts.NO_FACTS
    # basis is the compat source-level field, not a success claim.
    assert view['basis'] == 'executed'
    assert view['run_sealed']['available'] is True


def test_all_declared_when_no_action_sealed(sealed):
    declared, _, _ = _declared(('n4',))  # n4 has no F1 binding
    view = facts.overlay_sealed(sealed.root, sealed.run_id, declared)
    assert view['basis'] == 'declared'
    assert view['run_sealed']['available'] is False
    assert view['run_sealed']['reason'] == facts.NO_FACTS
    entry = view['actions'][0]
    assert entry['binding_level'] == 'declared'
    assert entry['fallback']['reason'] == facts.NO_FACTS
    # Never impersonate: the declared dag_id is preserved unchanged.
    assert entry['dag']['dag_id'] == declared['actions'][0]['dag']['dag_id']


def test_unavailable_carrier_has_no_binding_level(sealed):
    declared, _, _ = _declared(('n3',))
    carrier = _carrier(sealed)
    original = carrier.read_bytes()
    carrier.write_bytes(original[:-1] + bytes([original[-1] ^ 0xFF]))
    try:
        view = facts.overlay_sealed(sealed.root, sealed.run_id, declared)
        entry = view['actions'][0]
        assert entry['status'] == 'unavailable'
        assert entry['reason'] == 'sealed_plan_carrier_corrupt'
        # Honest: neither a static declared report nor a valid sealed plan.
        assert 'binding_level' not in entry
        assert view['basis'] == 'declared'  # no sealed action remains
    finally:
        carrier.write_bytes(original)


def test_declared_fallback_isolated_from_sealed_corruption(sealed):
    declared, _, _ = _declared(('n3', 'n4'))
    carrier = _carrier(sealed)
    original = carrier.read_bytes()
    carrier.write_bytes(original[:-1] + bytes([original[-1] ^ 0xFF]))
    try:
        view = facts.overlay_sealed(sealed.root, sealed.run_id, declared)
        by = {e['action_id']: e for e in view['actions']}
        assert by['n3']['status'] == 'unavailable' and 'binding_level' not in by['n3']
        # n4's declared fallback is unaffected by n3's corruption.
        assert by['n4']['binding_level'] == 'declared'
        assert by['n4']['status'] == 'projected'
    finally:
        carrier.write_bytes(original)