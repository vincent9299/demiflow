"""P12 source publication and prior recovery survive derived-index failures."""
import json
import sqlite3
from pathlib import Path

import pytest

from demiflow.forge.schema import ForgeError
from demiflow.workbench import directory_operations, project_operations
from demiflow.workbench.projects import index, workspace as ws
from demiflow.workbench.runs import manager as runs
from demiflow.workbench.table_bindings import make_resolver
from test_forge_runs import base, root, prepared, execute
from test_forge_resume import stopped
from test_workbench_native_submission import native_flow
from _p12_bounded_test import bounded_test


def unreadable(*a, **k):
    raise PermissionError('owner summary unavailable')


@pytest.mark.parametrize('failure', ['summary', 'publish'])
def test_prepared_source_is_durable_when_cache_publication_fails(root, monkeypatch, failure):
    if bounded_test(test_prepared_source_is_durable_when_cache_publication_fails, root=root, failure=failure): return
    if failure == 'summary':
        monkeypatch.setattr(runs, 'run_index_entry', unreadable)
    else:
        monkeypatch.setattr(index, 'publish', unreadable)
    # The real source preparation runs in this private fixture process so the
    # failure is exactly after durable_tree, not a stubbed preparation outcome.
    value = runs._prepare(root, 'flows/questions.py:select', params='flows/select_a.json')
    target = runs.directory(root, value['run_id'])
    assert target.is_dir() and value['status'] == 'prepared' and value['attempt'] == 1
    assert runs.show(root, value['run_id'])['digest'] == value['digest']
    assert (target / 'inspection.json').is_file()
    assert index.status(root)['pending'] >= 1 and not index.status(root)['complete']


def test_report_bytes_and_digest_survive_summary_and_diagnostic_failure(root, monkeypatch):
    if bounded_test(test_report_bytes_and_digest_survive_summary_and_diagnostic_failure, root=root): return
    import logging
    parameter_path = root / 'flows/select_a.json'
    parameters = json.loads(parameter_path.read_text()); parameters['arguments']['minimum'] = .55
    parameter_path.write_text(json.dumps(parameters))
    previous_reports = set((root / '.forge/inspections').glob('*.json'))
    monkeypatch.setattr(ws, 'report_index_entry', unreadable)
    monkeypatch.setattr(logging.Logger, 'warning', unreadable)
    report = ws._inspect_flow(root, 'flows/questions.py:select', params='flows/select_a.json',
        table_resolver=make_resolver(ws.table_binding, runs.result_table))
    path = root / '.forge/inspections' / (report['digest'] + '.json')
    assert path not in previous_reports
    assert json.loads(path.read_text()) == report
    assert ws.load_report(root, report['digest']) == report
    assert index.status(root)['pending'] == 1


def test_prepare_begin_refusal_does_not_create_run_or_remove_prior_report(root):
    if bounded_test(test_prepare_begin_refusal_does_not_create_run_or_remove_prior_report, root=root): return
    identity = 'r' + 'b' * 24
    with ws.control_lock(root):
        index.begin(root, 'run', identity)
    reports = set((root / '.forge/inspections').iterdir())
    with pytest.raises(ForgeError, match='synchronize'):
        runs._prepare(root, 'flows/questions.py:select', params='flows/select_a.json', reserved_run_id=identity)
    assert not runs.directory(root, identity).exists()
    assert reports <= set((root / '.forge/inspections').iterdir())
    assert directory_operations.synchronize(root)['errors'] == []


def test_resume_pending_refusal_preserves_real_prior_recovery_without_new_attempt(root, monkeypatch):
    value = execute(root, native_flow(root))
    assert value['status'] == 'succeeded', value
    stopped(root, value['run_id'])
    identity = value['run_id']; target = runs.directory(root, identity)
    journal = runs.journal(root, identity)
    output = value['outputs'][0]
    # Simulate lost derived result registration after a real native commit.
    # The real receipt/publication proof and original manifest remain intact.
    with sqlite3.connect(journal.path) as db:
        db.execute("DELETE FROM action_events WHERE action_id=? AND kind IN ('result','recovered_result')", (output['action_id'],))
    journal.transition({'succeeded'}, 'interrupted', execution_stopped=True, cleanup='complete')
    assert runs.show(root, identity)['outputs'][0]['result'] is None
    before = journal.execution_status()
    with ws.control_lock(root):
        marker = index.begin(root, 'run', identity)
    bounded_test(_resume_after_real_commit, root=root, identity=identity, value=value,
                 output=output, before=before, marker=marker)


def _resume_after_real_commit(root, identity, value, output, before, marker, monkeypatch):
    journal = runs.journal(root, identity)
    # Real register_results restores the fixed ref before the pending refusal.
    # Keep a checkpoint immediately before begin to distinguish those earlier
    # facts from a forbidden new attempt or a fabricated launch failure.
    snapshots = []; original_begin = index.begin
    def observed_begin(*a, **k):
        snapshots.append(journal.execution_status())
        return original_begin(*a, **k)
    monkeypatch.setattr(index, 'begin', observed_begin)
    with pytest.raises(ForgeError, match='synchronize'):
        runs._resume(root, identity, expected=value['digest'], expected_attempt=1)
    assert snapshots and journal.execution_status() == snapshots[-1]
    after = journal.execution_status()
    assert after['attempt'] == 1 and after['status'] == 'interrupted'
    assert after['revision'] >= before['revision']
    assert ws.read_control(root / '.forge/index-pending' / ('run-' + identity + '.json')) == marker
    current = runs.show(root, identity)
    assert current['attempt'] == 1
    assert current['outputs'][0]['result']['ref'] == output['result']['ref']
