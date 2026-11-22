"""Read-only D1a sealed-plan facts (F1) consumed by the workbench run graph (gap⑤, B).

The workbench reads the engine's attempt-independent F1 binding and its
content-addressed carriers through the single RunJournal authority (mode=ro, zero
engine writes), validates them against the frozen dag contracts, and overlays the
executed C-space plan onto the declared projection. A missing F1 never re-projects
current code to impersonate a historical execution graph; it degrades to an
explicitly labelled declared projection. C-space physical facts join executed
logical nodes directly by logical_node_id; A-space observation joining awaits the
D1b identity bridge and is never guessed.

Eleven counterexamples (present / absent / corrupt / sha-mismatch / symlink /
oversize / identity-mismatch / multi-action / attempt-independent /
historical-immutable / bridge-missing) plus the model run-branch wiring. The seal
fixture uses the genuine engine seam (RunContext + compile_logical_dag +
compile_physical_execution_plan + chain.seal_plan_facts); no cloudpickle, managed
worker or GPU is required. Test data only under tmp_path (/tmp/demiflow-*).
"""
import json
import sqlite3
from pathlib import Path
from types import SimpleNamespace

import pytest

from demiflow.data.plan import CallableSpec, LogicalPlan, MapOp
from demiflow.data.sources import ItemsSource
from runtime_v3_history_scenarios import write_legacy_seal
from demiflow.execution.recovery_contract import identity
from demiflow.execution.run_journal import RunJournal
from demiflow.planning.dag import (
    compile_logical_dag, compile_physical_execution_plan,
    validate_logical_dag, validate_physical_execution_plan,
)
from demiflow.workbench.graph import logical, model
from demiflow.workbench.runs import facts, manager as runs

ENTRY = 'flows/questions.py:select'
PARAMS = 'flows/select_a.json'


@pytest.fixture(autouse=True)
def private_runtime_domain(tmp_path, monkeypatch):
    monkeypatch.setenv('DEMIFLOW_FORGE_DOMAIN', str(tmp_path / 'runtime-domain'))


def _identity_row(row):
    return row


def _increment(row):
    return {**row, 'y': row['x'] + 1}


def _executed_plan(action_id, run_id):
    """Genuine C-space executed plan, compiled purely (no cloudpickle/worker)."""
    plan = LogicalPlan((MapOp(CallableSpec.create(_identity_row)),
                        MapOp(CallableSpec.create(_increment))))
    source = ItemsSource(items=({'x': 1}, {'x': 2}))
    dag = compile_logical_dag(action_id=action_id, source=source, operations=plan.operations,
                              terminal_kind='materialize', sink_ids=('output',))
    manifest_ref = {'manifest_id': 'a' * 64, 'sha256': 'b' * 64}
    stage_refs = [identity([dag['dag_id'], n['occurrence_id']]) for n in dag['nodes']]
    physical = compile_physical_execution_plan(
        logical_dag=dag, plan_revision=0, artifact_manifest_ref=manifest_ref,
        worker_groups=[{'group_id': 'g', 'role': 'cpu_prepare', 'profile_ref': 'p',
                        'stage_refs': stage_refs, 'min_workers': 1, 'initial_workers': 1,
                        'max_workers': 1}],
        recovery={'checkpoint_interval_ms': 1000, 'unconfirmed_rows': 1024,
                  'unconfirmed_bytes': 8 * 1024 ** 2, 'terminals': []},
        feasibility_snapshot_ref=identity([run_id, 'feasibility']), read_domain='dom',
        durability_declared='durable', partition_count=1, scheme_kind='single')
    validate_logical_dag(dag)
    validate_physical_execution_plan(physical)
    return dag, physical, manifest_ref


@pytest.fixture
def sealed(tmp_path, legacy_seal_writer):
    """A finished run whose executed plan was sealed by the genuine engine seam.

    The run lives at the workbench-shaped path manager.directory resolves, so
    overlay_sealed reads it without monkeypatching. The engine run_id (uuid) is
    deliberately distinct from the workbench directory basename (r+24hex): the
    reader must bind F1 to the engine recovery identity, not the directory name.
    """
    root = tmp_path
    run_id = 'r' + '0' * 24
    target = root / '.forge' / 'runs' / run_id
    target.mkdir(parents=True)
    action_id = 'n3'
    dag, physical, manifest_ref = _executed_plan(action_id, run_id)
    f1 = write_legacy_seal(legacy_seal_writer, target, action_id=action_id,
                           logical_dag=dag, physical_plan=physical, manifest_ref=manifest_ref,
                           sealed_at=1700000000.0)['facts']
    return SimpleNamespace(root=root, run_id=run_id, target=target, action_id=action_id,
                           logical_dag=dag, physical=physical, f1=f1,
                           engine_run_id=f1['run_id'])


def _rebind(target, action_id, value):
    """Externally rewrite the F1 binding (the journal reader is read-only)."""
    with sqlite3.connect(target / 'run.sqlite') as db:
        db.execute('UPDATE runtime_sources SET document=? WHERE action_id=?',
                   (json.dumps(value), action_id + '-sealed-plan'))


def _carrier(sealed, ref=None):
    ref = ref or sealed.f1['logical_dag_ref']
    return sealed.target / ref['path']


def _declared(actions=('n3', 'n4')):
    def node(i, kind, role):
        return dict(id=i, kind=kind, role=role, api='demo.' + kind, operator=None,
                    parameters={'i': i}, source='flows/x.py', line=int(i[1:]))
    nodes = [node('n0', 'read_table', 'source'), node('n1', 'map', 'transform'),
             node('n2', 'join', 'bridge'), node('n3', 'save', 'action'),
             node('n4', 'save', 'action'), node('n5', 'map', 'transform')]
    edges = [{'from': 'n0', 'to': 'n1', 'port': 0}, {'from': 'n1', 'to': 'n2', 'port': 0},
             {'from': 'n0', 'to': 'n5', 'port': 0}, {'from': 'n2', 'to': 'n3', 'port': 0},
             {'from': 'n5', 'to': 'n3', 'port': 1}, {'from': 'n1', 'to': 'n4', 'port': 0}]
    return logical.project(nodes, edges, list(actions)), nodes, edges


# --- 1. present: the executed C-space plan is read and bound, not re-projected ---

def test_present_sealed_plan_is_executed_c_space(sealed):
    result = facts.read_sealed_plan(sealed.target, sealed.action_id)
    assert result.get('available') is True
    assert result['dag_id'] == sealed.logical_dag['dag_id']
    assert result['plan_id'] == sealed.physical['plan_id']
    assert result['sealed_at'] == sealed.f1['sealed_at']
    assert result['artifact_manifest_ref'] == sealed.f1['artifact_manifest_ref']
    dag = validate_logical_dag(result['logical_dag'])
    assert dag['action_id'] == sealed.action_id
    assert dag == sealed.logical_dag
    validate_physical_execution_plan(result['physical_plan'])
    # The executed C-space dag is NOT the workbench's B-space declared projection.
    declared, _, _ = _declared(('n3',))
    assert declared['actions'][0]['dag']['dag_id'] != result['dag_id']
    entry = facts._sealed_entry(result)
    assert entry['status'] == 'sealed' and entry['node_map'] == {}
    assert entry['plan_id'] == sealed.physical['plan_id']


# --- 2. absent: missing F1 degrades to an explicitly labelled declared projection ---

def test_absent_f1_degrades_to_labelled_declared(sealed):
    result = facts.read_sealed_plan(sealed.target, 'n4')
    assert result['status'] == 'unavailable' and result['reason'] == facts.NO_FACTS
    # The reason covers BOTH pre-D1a runs and non-chain terminal actions.
    assert 'pre-D1a' in result['detail'] and '非 chain' in result['detail']
    declared, _, _ = _declared(('n4',))
    view = facts.overlay_sealed(sealed.root, sealed.run_id, declared)
    assert view['basis'] == 'declared'
    assert view['run_sealed']['available'] is False
    assert view['run_sealed']['reason'] == facts.NO_FACTS
    entry = view['actions'][0]
    assert entry['status'] == 'projected'
    assert entry['fallback']['reason'] == facts.NO_FACTS
    # Never impersonate: the declared dag_id is preserved unchanged.
    assert entry['dag']['dag_id'] == declared['actions'][0]['dag']['dag_id']


# --- 3. corrupt: a tampered carrier is refused (sha gate) ---

def test_corrupt_carrier_refused(sealed):
    carrier = _carrier(sealed)
    original = carrier.read_bytes()
    carrier.write_bytes(original[:-1] + bytes([original[-1] ^ 0xFF]))
    try:
        result = facts.read_sealed_plan(sealed.target, sealed.action_id)
        assert result['status'] == 'unavailable'
        assert result['reason'] == 'sealed_plan_carrier_corrupt'
    finally:
        carrier.write_bytes(original)


# --- 4. sha-mismatch: F1 reference digest differs from the carrier bytes ---

def test_sha_mismatch_refused(sealed):
    bad = json.loads(json.dumps(sealed.f1))
    bad['logical_dag_ref']['sha256'] = '0' * 64  # bytes stay correct
    _rebind(sealed.target, sealed.action_id, bad)
    try:
        result = facts.read_sealed_plan(sealed.target, sealed.action_id)
        assert result['status'] == 'unavailable'
        assert result['reason'] == 'sealed_plan_carrier_corrupt'
    finally:
        _rebind(sealed.target, sealed.action_id, sealed.f1)


# --- 5. symlink: a symlinked carrier is refused (no-follow read) ---

def test_symlink_carrier_refused(sealed, tmp_path):
    carrier = _carrier(sealed)
    original = carrier.read_bytes()
    outside = tmp_path / 'outside.json'
    outside.write_bytes(original)
    carrier.unlink()
    carrier.symlink_to(outside)
    try:
        result = facts.read_sealed_plan(sealed.target, sealed.action_id)
        assert result['status'] == 'unavailable'
        assert result['reason'] == 'sealed_plan_carrier_unsafe'
    finally:
        carrier.unlink()
        carrier.write_bytes(original)


# --- 6. oversize: a carrier beyond the byte budget is refused, never truncated ---

def test_oversize_carrier_refused(sealed):
    carrier = _carrier(sealed)
    original = carrier.read_bytes()
    carrier.write_bytes(original + b' ' * (facts.MAX_SEALED_PLAN_BYTES + 1))
    try:
        result = facts.read_sealed_plan(sealed.target, sealed.action_id)
        assert result['status'] == 'unavailable'
        assert result['reason'] == 'sealed_plan_carrier_oversize'
    finally:
        carrier.write_bytes(original)


# --- 7. identity-mismatch: F1 dag_id/plan_id/action_id must bind the sealed docs ---

def test_identity_mismatch_refused(sealed):
    for field, value in (('dag_id', 'f' * 64), ('plan_id', 'e' * 64), ('action_id', 'n9')):
        bad = json.loads(json.dumps(sealed.f1))
        bad[field] = value
        _rebind(sealed.target, sealed.action_id, bad)
        try:
            result = facts.read_sealed_plan(sealed.target, sealed.action_id)
            assert result['status'] == 'unavailable', (field, result)
            assert result['reason'] in ('sealed_plan_identity_mismatch',
                                        'sealed_plan_facts_invalid'), (field, result)
        finally:
            _rebind(sealed.target, sealed.action_id, sealed.f1)


# --- 8. multi-action: per-action entries with degradation isolation ---

def test_multi_action_per_action_isolation(sealed):
    declared, _, _ = _declared(('n3', 'n4'))
    view = facts.overlay_sealed(sealed.root, sealed.run_id, declared)
    assert view['basis'] == 'executed' and view['run_sealed']['available'] is True
    by = {e['action_id']: e for e in view['actions']}
    assert by['n3']['status'] == 'sealed'
    assert by['n3']['dag']['dag_id'] == sealed.logical_dag['dag_id']
    assert by['n4']['status'] == 'projected' and by['n4']['fallback']['reason'] == facts.NO_FACTS
    # One action's corrupt carrier never fails the whole graph.
    carrier = _carrier(sealed)
    original = carrier.read_bytes()
    carrier.write_bytes(original[:-1] + bytes([original[-1] ^ 0xFF]))
    try:
        broken = facts.overlay_sealed(sealed.root, sealed.run_id, declared)
        byb = {e['action_id']: e for e in broken['actions']}
        assert byb['n3']['status'] == 'unavailable'
        assert byb['n3']['reason'] == 'sealed_plan_carrier_corrupt'
        assert byb['n4']['status'] == 'projected'  # isolated, unaffected
        assert broken['basis'] == 'declared'  # no sealed action remains
    finally:
        carrier.write_bytes(original)


# --- 9. attempt-independent: F1 omits attempt and reads identically across attempts ---

def test_attempt_independent_binding(sealed):
    assert 'attempt' not in sealed.f1
    key = sealed.action_id + '-sealed-plan'
    first = RunJournal(sealed.target, attempt=1).source_binding(key)
    second = RunJournal(sealed.target, attempt=2).source_binding(key)
    assert first == second == sealed.f1
    # The reader takes no attempt parameter; the binding is attempt-agnostic.
    result = facts.read_sealed_plan(sealed.target, sealed.action_id)
    assert result['available'] and result['dag_id'] == sealed.logical_dag['dag_id']


# --- 10. historical-immutable: current-code edits never rewrite the executed graph ---

def test_historical_graph_ignores_current_code_edits(sealed):
    declared, nodes, edges = _declared(('n3', 'n4'))
    original_declared_dag_id = declared['actions'][0]['dag']['dag_id']
    # Structurally edit the CURRENT code (an extra transform feeding n3).
    edited_nodes = nodes + [dict(id='n6', kind='map', role='transform', api='demo.map',
                                 operator=None, parameters={'i': 'n6'}, source='flows/x.py', line=6)]
    edited_edges = edges + [{'from': 'n0', 'to': 'n6', 'port': 0},
                            {'from': 'n6', 'to': 'n3', 'port': 2}]
    edited = logical.project(edited_nodes, edited_edges, ['n3', 'n4'])
    assert edited['actions'][0]['dag']['dag_id'] != original_declared_dag_id
    view = facts.overlay_sealed(sealed.root, sealed.run_id, edited)
    by = {e['action_id']: e for e in view['actions']}
    # The historical executed graph stays bound to the sealed plan, not the edit.
    assert by['n3']['status'] == 'sealed'
    assert by['n3']['dag']['dag_id'] == sealed.logical_dag['dag_id']
    assert by['n3']['dag']['dag_id'] != edited['actions'][0]['dag']['dag_id']


# --- 11. bridge-missing: C-space direct join, A-space observation never guessed ---

def test_bridge_missing_c_space_direct_join(sealed):
    assert 'identity_bridge_ref' not in sealed.f1
    result = facts.read_sealed_plan(sealed.target, sealed.action_id)
    entry = facts._sealed_entry(result)
    # No A-space node mapping is invented; the bridge is absent.
    assert entry['node_map'] == {}
    # C-space physical stages join executed logical nodes directly by id.
    assert set(entry['physical']['stages']) == {n['logical_node_id'] for n in sealed.logical_dag['nodes']}
    assert all(s['role'] == 'cpu_prepare' for s in entry['physical']['stages'].values())
    assert entry['physical']['plan_id'] == sealed.physical['plan_id']
    assert entry['physical']['channels'] and entry['physical']['worker_groups']


# --- model run-branch wiring: a prepared run (no F1) keeps the declared fallback ---

@pytest.fixture
def isolated(tmp_path):
    """Self-contained demo workspace; skips when lance/pyarrow are unavailable.

    Kept local (not imported from test_forge_workspace) so the eleven pure
    counterexamples above never depend on the lance-backed workspace harness.
    """
    pytest.importorskip('lance')
    pytest.importorskip('pyarrow')
    import importlib.util
    source = Path(__file__).parents[1] / 'examples/forge_workspace/make_demo.py'
    spec = importlib.util.spec_from_file_location('forge_demo_fixture', source)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    root = tmp_path / 'workspace'
    module.create(root)
    return root


def test_model_run_branch_wires_overlay_declared_fallback(isolated):
    run = runs.prepare(isolated, ENTRY, params=PARAMS)
    graph = model.get(isolated, basis_kind='run', run_id=run['run_id'], attempt=1)
    view = graph['logical']
    assert view['available']
    assert view['basis'] == 'declared'
    assert view['run_sealed']['available'] is False
    assert view['run_sealed']['reason'] == facts.NO_FACTS
    assert all(e['status'] == 'projected' for e in view['actions'])
    assert all(e.get('fallback', {}).get('reason') == facts.NO_FACTS for e in view['actions'])