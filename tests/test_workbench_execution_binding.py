"""D package: workbench consumption of the D1b identity bridge and node observations.

The bridge is C-produced and read through B's neutral bounded reader
(``read_plan_artifact``); the workbench only re-checks the action/dag/report triple
and joins observations by exact report observation key. A missing or invalid bridge
degrades honestly (never a guessed correspondence); a page absence is not evidence
that the run never observed a node. The seal fixture uses the genuine engine seam
(RunContext + compile_logical_dag + compile_physical_execution_plan +
chain.seal_plan_facts + journal.bind_source); no cloudpickle/worker/GPU is required.
Test data only under tmp_path (/tmp/demiflow-*).
"""
import hashlib
import json
import sqlite3
from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace

import pytest

from runtime_v3_history_scenarios import write_legacy_seal
from demiflow.execution.journal.run_journal import RunJournal
from demiflow.forge.schema import ForgeError
from demiflow.workbench.graph import model
from demiflow.workbench.runs import facts

from test_workbench_sealed_facts import _executed_plan, _declared


@pytest.fixture(autouse=True)
def private_runtime_domain(tmp_path, monkeypatch):
    monkeypatch.setenv('DEMIFLOW_FORGE_DOMAIN', str(tmp_path / 'runtime-domain'))


def _bridge_document(action_id, dag_id, report_digest, entries):
    return dict(schema=facts.BRIDGE_SCHEMA, action_id=action_id, dag_id=dag_id,
                report_digest=report_digest, entries=entries)


def _carrier_ref(raw):
    return dict(sha256=hashlib.sha256(raw).hexdigest(),
                path='scratch/identity-bridge.json', bytes=len(raw))


@pytest.fixture
def bridged(tmp_path, legacy_seal_writer):
    """A finished run with a sealed plan AND a C-shaped identity bridge bound."""
    root = tmp_path
    run_id = 'r' + '0' * 24
    target = root / '.forge' / 'runs' / run_id
    target.mkdir(parents=True)
    action_id = 'n3'
    dag, physical, manifest_ref = _executed_plan(action_id, run_id)
    report_digest = 'd' * 64
    entries = [dict(report_node_id='rn' + str(i), report_occurrence=i,
                    logical_node_id=n['logical_node_id'], occurrence_id=n['occurrence_id'],
                    mode='execute') for i, n in enumerate(dag['nodes'])]
    bridge = _bridge_document(action_id, dag['dag_id'], report_digest, entries)
    raw = json.dumps(bridge, ensure_ascii=False, sort_keys=True).encode()
    ref = _carrier_ref(raw)
    f1 = write_legacy_seal(legacy_seal_writer, target, action_id=action_id,
                           logical_dag=dag, physical_plan=physical, manifest_ref=manifest_ref,
                           sealed_at=1794527206071, bridge=(bridge, ref))['facts']
    return SimpleNamespace(root=root, run_id=run_id, target=target, action_id=action_id,
                           logical_dag=dag, physical=physical, f1=f1, bridge=bridge, ref=ref,
                           report_digest=report_digest, entries=entries)


@pytest.fixture
def sealed(tmp_path, legacy_seal_writer):
    """A finished run with a sealed plan but NO bridge (historical / pre-bridge)."""
    root = tmp_path
    run_id = 'r' + '1' * 24
    target = root / '.forge' / 'runs' / run_id
    target.mkdir(parents=True)
    action_id = 'n3'
    dag, physical, manifest_ref = _executed_plan(action_id, run_id)
    f1 = write_legacy_seal(legacy_seal_writer, target, action_id=action_id,
                           logical_dag=dag, physical_plan=physical, manifest_ref=manifest_ref,
                           sealed_at=1794527206071)['facts']
    return SimpleNamespace(root=root, run_id=run_id, target=target, action_id=action_id,
                           logical_dag=dag, physical=physical, f1=f1)


def _rebind_bridge(target, action_id, bridge):
    """Externally rewrite the bridge carrier + binding (journal reader is read-only)."""
    raw = json.dumps(bridge, ensure_ascii=False, sort_keys=True).encode()
    ref = _carrier_ref(raw)
    (target / ref['path']).write_bytes(raw)
    with sqlite3.connect(target / 'run.sqlite') as db:
        db.execute('UPDATE runtime_sources SET document=? WHERE action_id=?',
                   (json.dumps(ref), action_id + '-identity-bridge'))
    return ref


# --- read_identity_bridge: success through the real B reader ---

def test_read_identity_bridge_success(bridged):
    result = facts.read_identity_bridge(bridged.target, bridged.action_id,
                                        dag_id=bridged.logical_dag['dag_id'],
                                        report_digest=bridged.report_digest)
    assert result['available'] is True
    assert result['ref'] == bridged.ref
    assert result['bridge'] == bridged.bridge


def test_read_identity_bridge_missing(sealed):
    result = facts.read_identity_bridge(sealed.target, sealed.action_id,
                                        dag_id=sealed.logical_dag['dag_id'], report_digest='d' * 64)
    assert result['available'] is False
    assert result['reason'] == facts.NO_BRIDGE
    assert '不猜测' in result['detail']


def test_read_identity_bridge_triple_mismatch(bridged):
    for kwargs in (dict(dag_id='f' * 64, report_digest=bridged.report_digest),
                   dict(dag_id=bridged.logical_dag['dag_id'], report_digest='e' * 64)):
        result = facts.read_identity_bridge(bridged.target, bridged.action_id, **kwargs)
        assert result['available'] is False, kwargs
        assert result['reason'] == 'identity_bridge_mismatch', kwargs


def test_read_identity_bridge_action_mismatch(bridged):
    bad = dict(bridged.bridge, action_id='n9')
    _rebind_bridge(bridged.target, bridged.action_id, bad)
    result = facts.read_identity_bridge(bridged.target, bridged.action_id,
                                        dag_id=bridged.logical_dag['dag_id'],
                                        report_digest=bridged.report_digest)
    assert result['available'] is False and result['reason'] == 'identity_bridge_mismatch'


def test_read_identity_bridge_invalid_arguments(bridged):
    result = facts.read_identity_bridge(bridged.target, '', dag_id='d' * 64, report_digest='d' * 64)
    assert result['available'] is False and result['reason'] == 'identity_bridge_invalid'
    result = facts.read_identity_bridge(bridged.target, bridged.action_id,
                                        dag_id=bridged.logical_dag['dag_id'], report_digest='d' * 65)
    assert result['available'] is False and result['reason'] == 'identity_bridge_invalid'


@pytest.mark.parametrize('mutate,reason', [
    (lambda b: b.update(schema='wrong.schema'), 'identity_bridge_invalid'),
    (lambda b: b.update(entries={}), 'identity_bridge_invalid'),
    (lambda b: b['entries'][0].update(mode='guess'), 'identity_bridge_invalid'),
    (lambda b: b['entries'][0].update(report_occurrence=-1), 'identity_bridge_invalid'),
    (lambda b: b['entries'].append(dict(b['entries'][0])), 'identity_bridge_invalid'),
])
def test_read_identity_bridge_invalid_document(bridged, mutate, reason):
    bad = json.loads(json.dumps(bridged.bridge))
    mutate(bad)
    _rebind_bridge(bridged.target, bridged.action_id, bad)
    result = facts.read_identity_bridge(bridged.target, bridged.action_id,
                                        dag_id=bridged.logical_dag['dag_id'],
                                        report_digest=bridged.report_digest)
    assert result['available'] is False and result['reason'] == reason


def test_read_identity_bridge_corrupt_carrier(bridged):
    carrier = bridged.target / bridged.ref['path']
    raw = carrier.read_bytes()
    carrier.write_bytes(raw[:-1] + bytes([raw[-1] ^ 0xFF]))
    result = facts.read_identity_bridge(bridged.target, bridged.action_id,
                                        dag_id=bridged.logical_dag['dag_id'],
                                        report_digest=bridged.report_digest)
    assert result['available'] is False and result['reason'] == 'identity_bridge_corrupt'


def test_read_identity_bridge_oversize(bridged):
    big = dict(bridged.ref, bytes=facts.MAX_IDENTITY_BRIDGE_BYTES + 1)
    with sqlite3.connect(bridged.target / 'run.sqlite') as db:
        db.execute('UPDATE runtime_sources SET document=? WHERE action_id=?',
                   (json.dumps(big), bridged.action_id + '-identity-bridge'))
    result = facts.read_identity_bridge(bridged.target, bridged.action_id,
                                        dag_id=bridged.logical_dag['dag_id'],
                                        report_digest=bridged.report_digest)
    assert result['available'] is False and result['reason'] == 'identity_bridge_oversize'


# --- join_node_observations: exact key match, observed vs not_in_page ---

def _page(bridged, rows, *, attempt=1, action_id=None):
    return dict(format='demiflow_forge_node_observation_page_v1', run_id=bridged.run_id,
                attempt=attempt, node_id=None,
                action_id=bridged.action_id if action_id is None else action_id,
                observations=rows, has_more=False, cursor=None, observation_revision=1)


def _row(bridged, entry):
    return dict(action_id=bridged.action_id, node_id=entry['report_node_id'],
                occurrence=entry['report_occurrence'], sequence=1, status='observed')


def test_join_observed_and_not_in_page(bridged):
    first, second = bridged.entries[0], bridged.entries[1]
    page = _page(bridged, [_row(bridged, first)])
    joined = facts.join_node_observations(bridged.bridge, page,
                                          action_id=bridged.action_id, attempt=1)
    assert len(joined) == len(bridged.entries)
    by = {item['report_node_id']: item for item in joined}
    assert by[first['report_node_id']]['availability'] == 'observed'
    assert by[first['report_node_id']]['observation']['node_id'] == first['report_node_id']
    assert by[second['report_node_id']]['availability'] == 'not_in_page'
    assert by[second['report_node_id']]['observation'] is None


def test_join_empty_page_is_not_in_page_not_absence(bridged):
    joined = facts.join_node_observations(bridged.bridge, _page(bridged, []),
                                          action_id=bridged.action_id, attempt=1)
    assert all(item['availability'] == 'not_in_page' for item in joined)
    assert all(item['observation'] is None for item in joined)


def test_join_attempt_scope_conflict(bridged):
    with pytest.raises(ForgeError) as exc:
        facts.join_node_observations(bridged.bridge, _page(bridged, [], attempt=2),
                                     action_id=bridged.action_id, attempt=1)
    assert exc.value.code == 'conflict'


def test_join_action_scope_conflict(bridged):
    with pytest.raises(ForgeError) as exc:
        facts.join_node_observations(bridged.bridge, _page(bridged, [], action_id='other'),
                                     action_id=bridged.action_id, attempt=1)
    assert exc.value.code == 'conflict'


def test_join_rejects_wrong_bridge_action(bridged):
    with pytest.raises(ForgeError):
        facts.join_node_observations(bridged.bridge, _page(bridged, []),
                                     action_id='other', attempt=1)


def test_join_rejects_bad_format(bridged):
    page = _page(bridged, [])
    page['format'] = 'something_else'
    with pytest.raises(ForgeError):
        facts.join_node_observations(bridged.bridge, page, action_id=bridged.action_id, attempt=1)


# --- node_map derivation and sealed-entry wiring ---

def test_bridge_node_map_unambiguous(bridged):
    node_map = facts._bridge_node_map(bridged.entries)
    assert node_map == {e['logical_node_id']: e['report_node_id'] for e in bridged.entries}


def test_bridge_node_map_omits_multi_instance():
    entries = [dict(report_node_id='ra', report_occurrence=0, logical_node_id='L',
                    occurrence_id='o1', mode='execute'),
               dict(report_node_id='rb', report_occurrence=1, logical_node_id='L',
                    occurrence_id='o2', mode='reuse'),
               dict(report_node_id='rc', report_occurrence=0, logical_node_id='M',
                    occurrence_id='o3', mode='execute')]
    node_map = facts._bridge_node_map(entries)
    assert 'L' not in node_map  # ambiguous: two report nodes, never guessed
    assert node_map == {'M': 'rc'}


def test_sealed_entry_with_bridge_populates_node_map(bridged):
    result = facts.read_sealed_plan(bridged.target, bridged.action_id)
    bridge = facts.read_identity_bridge(bridged.target, bridged.action_id,
                                        dag_id=bridged.logical_dag['dag_id'],
                                        report_digest=bridged.report_digest)
    entry = facts._sealed_entry(result, bridge=bridge)
    assert entry['status'] == 'sealed'
    assert entry['node_map'] == {e['logical_node_id']: e['report_node_id'] for e in bridged.entries}
    assert entry['identity_bridge'] == bridge


def test_sealed_entry_without_bridge_is_honest(sealed):
    result = facts.read_sealed_plan(sealed.target, sealed.action_id)
    entry = facts._sealed_entry(result)
    assert entry['node_map'] == {}
    assert entry['identity_bridge']['available'] is False
    assert entry['identity_bridge']['reason'] == facts.NO_BRIDGE


def test_overlay_sealed_with_report_digest_binds_bridge(bridged):
    declared, _, _ = _declared(('n3',))
    view = facts.overlay_sealed(bridged.root, bridged.run_id, declared,
                                report_digest=bridged.report_digest)
    assert view['basis'] == 'executed'
    entry = view['actions'][0]
    assert entry['status'] == 'sealed'
    assert entry['identity_bridge']['available'] is True
    assert entry['node_map'] == {e['logical_node_id']: e['report_node_id'] for e in bridged.entries}


def test_overlay_sealed_without_report_digest_keeps_bridge_missing(bridged):
    declared, _, _ = _declared(('n3',))
    view = facts.overlay_sealed(bridged.root, bridged.run_id, declared)
    entry = view['actions'][0]
    assert entry['status'] == 'sealed'
    assert entry['identity_bridge']['available'] is False
    assert entry['node_map'] == {}


# --- bound_source bridge resolution (never guesses) ---

def test_resolve_bridge_node_unique(bridged):
    report = dict(actions=[bridged.action_id], digest=bridged.report_digest)
    entry = bridged.entries[0]
    resolved = model._resolve_bridge_node(bridged.target, report,
                                          entry['logical_node_id'], entry['occurrence_id'])
    assert resolved == entry['report_node_id']
    # occurrence omitted is fine when the logical node has a single bridged instance.
    assert model._resolve_bridge_node(bridged.target, report, entry['logical_node_id'], None) \
        == entry['report_node_id']


def test_resolve_bridge_node_multi_instance_requires_occurrence(bridged):
    entries = [dict(report_node_id='ra', report_occurrence=0, logical_node_id='L',
                    occurrence_id='o1', mode='execute'),
               dict(report_node_id='rb', report_occurrence=1, logical_node_id='L',
                    occurrence_id='o2', mode='execute')]
    bridge = _bridge_document(bridged.action_id, bridged.logical_dag['dag_id'],
                              bridged.report_digest, entries)
    _rebind_bridge(bridged.target, bridged.action_id, bridge)
    report = dict(actions=[bridged.action_id], digest=bridged.report_digest)
    with pytest.raises(ForgeError) as exc:
        model._resolve_bridge_node(bridged.target, report, 'L', None)
    assert exc.value.code == 'conflict'
    assert model._resolve_bridge_node(bridged.target, report, 'L', 'o2') == 'rb'


def test_resolve_bridge_node_unbridged_not_found(bridged):
    report = dict(actions=[bridged.action_id], digest=bridged.report_digest)
    with pytest.raises(ForgeError) as exc:
        model._resolve_bridge_node(bridged.target, report, 'no-such-logical-node', None)
    assert exc.value.code == 'not_found'


# --- F10: sealed_at is epoch milliseconds (frontend renders new Date(sealed_at)) ---

def test_f10_sealed_at_is_epoch_milliseconds():
    value = 1794527206071  # real D1a candidate value
    assert datetime.fromtimestamp(value / 1000, tz=timezone.utc).isoformat() \
        == '2026-11-12T23:46:46.071000+00:00'


def test_sealed_at_stored_as_integer_milliseconds(bridged):
    result = facts.read_sealed_plan(bridged.target, bridged.action_id)
    assert type(result['sealed_at']) is int
    assert result['sealed_at'] == 1794527206071
    assert datetime.fromtimestamp(result['sealed_at'] / 1000, tz=timezone.utc).isoformat() \
        == '2026-11-12T23:46:46.071000+00:00'