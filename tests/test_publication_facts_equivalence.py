"""``execution.publication_facts`` must answer exactly as the host copies it replaces.

The engine functions were moved from ``workbench/runs/manager.py`` and
``workbench/runs/native_stages.py`` without logic changes. Until the workbench
is switched over, both implementations exist. This module re-runs existing
real-journal scenarios with every host call shadowed by the engine function and
asserts identical results, so the later switch is a provable no-op.
"""
from pathlib import Path
from types import SimpleNamespace

import pytest

import demiflow
from demiflow.execution import publication_facts as facts
from demiflow.execution.recovery_contract import RecoveryContractError
from demiflow.forge.schema import ForgeError

# Existing scenarios, collected again here under the comparing fixture below.
from test_forge_runs import base, root  # noqa: F401  (fixtures used by the re-exported cases)
from test_execution_recovery_lance import (  # noqa: F401
    test_observer_projects_the_same_native_receipt_without_advancing_progress as test_original_lance_receipt_matches)
from test_gpt_write_recovery import (  # noqa: F401
    test_r_c_existing_native_commit_recovers_without_republication as test_crashed_original_commit_receipt_matches)
from test_runtime_v3_save_product import (  # noqa: F401
    test_native_save_prepare_execute_and_fixed_result as test_native_stage_results_match,
    test_managed_published_before_receipt_admits_only_original_native_recovery as test_prepared_stage_admission_matches)

CALLS = {'receipt': 0, 'receipt_value': 0, 'admit': 0, 'admit_value': 0}


def _outcome(call, translate=None):
    try:
        return ('value', call())
    except BaseException as exc:  # compared, then re-raised by the caller
        kind, text = type(exc), str(exc)
        if translate is not None:
            kind, text = translate(exc)
        return ('error', kind, text), exc


def _host_error(exc):
    """What the workbench wrapper will raise for an engine error after the switch."""
    if isinstance(exc, facts.PreparedPublicationUnresolved):
        return ForgeError, 'Native prepared publication is unresolved: ' + str(exc)
    if isinstance(exc, facts.PublicationMismatch):
        return ForgeError, str(exc)
    return type(exc), str(exc)


def _compare(old, new):
    old_key = old[0] if old[0][0] == 'error' else old
    new_key = new[0] if new[0][0] == 'error' else new
    assert new_key == old_key, f'engine projection differs from host copy: {new_key!r} != {old_key!r}'
    if old[0][0] == 'error':
        raise old[1]
    return old[1]


@pytest.fixture(autouse=True)
def shadow_host_copies(monkeypatch):
    assert Path(demiflow.__file__).resolve().is_relative_to(Path(__file__).resolve().parents[1]), \
        'test is not importing the tree it lives in'
    from demiflow.workbench.runs import manager, native_stages
    host_receipt, host_admit = manager._native_execution_receipt, native_stages.admit_prepared

    def receipt(journal, action_id):
        CALLS['receipt'] += 1
        old = _outcome(lambda: host_receipt(journal, action_id))
        new = _outcome(lambda: facts.inspect_write_receipt(journal, action_id))
        value = _compare(old, new)
        CALLS['receipt_value'] += value is not None
        return value

    def admit(target, journal, manifest, output):
        CALLS['admit'] += 1
        old = _outcome(lambda: host_admit(target, journal, manifest, output))
        new = _outcome(lambda: facts.admit_prepared_stage(
            journal, target, parents=manager._report(target, manifest)['actions'],
            report_node_id=output['action_id'], uri=output['uri'],
            schema_identity=output['schema_identity'], report_digest=manifest['report_digest']),
            translate=_host_error)
        value = _compare(old, new)
        CALLS['admit_value'] += value is not None
        return value

    host_reconcile = manager.reconcile

    def reconcile(root, run_id, *args, **kwargs):
        # The host admits prepared stages inside its worker process, where this
        # fixture is not active. Probe the same read-only admission here, with
        # the inputs ``validate_output_targets`` derives, before reconciling.
        from demiflow.execution.run_journal import RunJournal
        _, value = manager._load(root, run_id)
        manifest = value['manifest']
        target = manager.directory(manifest['root'], manifest['run_id'])
        for output in manager._describe(manifest['run_id'], value)['outputs']:
            if output.get('publication') == 'native_stage_v3':
                try:
                    admit(target, RunJournal(target), manifest, output)
                except AssertionError:
                    raise
                except Exception:
                    pass  # both implementations failed identically; already compared
        return host_reconcile(root, run_id, *args, **kwargs)

    monkeypatch.setattr(manager, '_native_execution_receipt', receipt)
    monkeypatch.setattr(native_stages, 'admit_prepared', admit)
    monkeypatch.setattr(manager, 'reconcile', reconcile)


def test_observation_context_is_the_shape_every_copy_built(tmp_path):
    binding = {'run_id': 'a' * 32, 'limits': {'batch_rows': 7}}
    journal = SimpleNamespace(directory=tmp_path, recovery_identity=lambda: binding,
                              execution_status=lambda: {'attempt': 3})
    context = facts.observation_context(journal)
    assert vars(context) == dict(directory=tmp_path, limits=binding['limits'], journal=journal,
                                 ref=context.ref, attempt=3, fence='observation-only')
    assert vars(context.ref) == {'run_id': binding['run_id']}
    other = tmp_path / 'other'
    assert facts.observation_context(journal, directory=other).directory == other
    journal.recovery_identity = lambda: None
    assert facts.observation_context(journal) is None
    assert facts.observation_action(journal, 'action-1', {'kind': 'write_table'}) is None


def test_observation_action_cannot_advance_a_real_journal(tmp_path):
    from demiflow.execution.run_context import RunContext
    with RunContext.start(tmp_path / 'run') as run:
        journal, native_fence = run.journal, run.fence
    action = facts.observation_action(journal, 'action-1', {'kind': 'write_table'})
    assert action.context.fence == 'observation-only' != native_fence
    assert action.context.ref.run_id == journal.recovery_identity()['run_id']
    with pytest.raises(Exception):
        action.complete({'status': 'forged'})
    assert journal.execution_action('action-1') is None


def test_object_namespace_matches_the_host_fabricated_context(tmp_path):
    from demiflow.execution.object_resources import namespace_record
    run_id = 'b' * 32
    for logical in (str(tmp_path / 'objects'), (tmp_path / 'objects').as_uri()):
        fabricated = SimpleNamespace(ref=SimpleNamespace(run_id=run_id))
        assert facts.object_namespace(run_id, logical) == namespace_record(fabricated, logical)
    with pytest.raises(RecoveryContractError):
        facts.object_namespace(run_id, 's3://bucket/objects')


def test_engine_module_does_not_load_hosts():
    import subprocess
    import sys
    root_dir = Path(__file__).resolve().parents[1]
    result = subprocess.run([sys.executable, '-I', '-B', '-c',
        'import sys;sys.path.insert(0,sys.argv[1]);import demiflow.execution.publication_facts;'
        'assert not any(n.startswith(("demiflow.workbench","demiflow.forge")) for n in sys.modules)',
        str(root_dir)], capture_output=True, text=True, timeout=60)
    assert result.returncode == 0, result.stderr


def test_zz_shadowed_scenarios_really_compared_both_paths():
    """Last in file order: the re-exported scenarios must have exercised the shadows."""
    print("shadow comparisons:", CALLS)
    assert CALLS["receipt_value"] >= 2, CALLS
    assert CALLS['admit'] >= 3 and CALLS['admit_value'] >= 2, CALLS
