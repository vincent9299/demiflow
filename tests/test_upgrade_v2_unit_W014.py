"""W014 · _sealed_entry labels a genuinely sealed plan binding_level='sealed'.

C09: binding_level is a per-action display field. A valid engine-sealed plan is
'sealed' (运行封存计划), distinct from a static 'declared' report and from execution
success. The label is asserted on a REAL sealed run produced by the genuine engine
seam (RunContext + compile_logical_dag + compile_physical_execution_plan +
chain.seal_plan_facts), reused from test_workbench_sealed_facts — no mock journal.

A missing identity bridge never invents a node map; the sealed label does not depend
on the bridge. Test data only under tmp_path (/tmp/demiflow-*).
"""
from test_workbench_sealed_facts import sealed, private_runtime_domain  # noqa: F401

from demiflow.workbench.runs import facts


def test_sealed_entry_binding_level_from_genuine_seal(sealed):
    result = facts.read_sealed_plan(sealed.target, sealed.action_id)
    assert result.get('available') is True
    entry = facts._sealed_entry(result)
    assert entry['binding_level'] == 'sealed'
    assert entry['status'] == 'sealed'
    assert entry['action_id'] == sealed.action_id
    assert entry['plan_id'] == sealed.physical['plan_id']
    assert entry['dag']['dag_id'] == sealed.logical_dag['dag_id']
    # The seal fixture carries no identity bridge: no A-space map is guessed.
    assert 'identity_bridge_ref' not in sealed.f1
    assert entry['node_map'] == {}
    assert entry['identity_bridge']['available'] is False
    assert entry['identity_bridge']['reason'] == facts.NO_BRIDGE


def test_sealed_label_independent_of_bridge(sealed):
    result = facts.read_sealed_plan(sealed.target, sealed.action_id)
    # An available bridge populates node_map but never changes the sealed label.
    bridge = dict(available=True, ref={'sha256': '0' * 64},
                  bridge=dict(entries=[dict(report_node_id='n3', report_occurrence=0,
                                            logical_node_id='ln0', occurrence_id='occ0',
                                            mode='execute')]))
    entry = facts._sealed_entry(result, bridge=bridge)
    assert entry['binding_level'] == 'sealed'
    assert entry['node_map'] == {'ln0': 'n3'}
    assert entry['identity_bridge'] is bridge


def test_overlay_sealed_action_carries_sealed_level(sealed):
    from test_workbench_sealed_facts import _declared
    declared, _, _ = _declared(('n3',))
    view = facts.overlay_sealed(sealed.root, sealed.run_id, declared)
    entry = view['actions'][0]
    assert entry['status'] == 'sealed'
    assert entry['binding_level'] == 'sealed'
    assert entry['dag']['dag_id'] == sealed.logical_dag['dag_id']