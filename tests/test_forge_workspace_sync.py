"""Read-only, bounded discovery of external workspace and execution changes."""
import json
import sqlite3
import time

import pytest

from demiflow.workbench.projects import workspace as ws
from demiflow.workbench import project_operations as project_ops
from demiflow.workbench.runs import manager as runs
from demiflow.workbench import project_operations as project_ops
from demiflow.forge.platform import content_digest
from demiflow.execution.run_journal import RunJournal
from test_forge_workspace import demo, isolated


def test_change_hints_cover_controls_configs_and_wal_without_content_reads(isolated, monkeypatch):
    root = isolated
    from demiflow.workbench.projects import index
    run_id = 'r' + 'a' * 24
    directory = root / '.forge/runs' / run_id
    directory.mkdir(parents=True)
    (directory / 'runtime').mkdir()
    (directory / 'runtime/ignored.py').write_text('never traversed')
    manifest = dict(format=runs.FORMAT, run_id=run_id, root=str(root), created_at=1, outputs={})
    manifest['digest'] = content_digest(manifest)
    run = RunJournal(directory)
    run.initialize(manifest)
    run.transition({None}, 'starting')
    with ws.control_lock(root):
        marker = index.begin(root, 'run', run_id)
        assert runs.publish_run_index(root, marker) is None
    baseline = project_ops.workspace_changes(root)
    def forbidden(*args, **kwargs):
        raise AssertionError('Change hints must not read report/journal contents')
    read_control = ws.read_control
    def bounded_control(path, *args, **kwargs):
        assert path == root / 'workspace.json'
        return read_control(path, *args, **kwargs)
    monkeypatch.setattr(ws, 'read_control', bounded_control)
    monkeypatch.setattr(ws, 'load_report', forbidden)
    monkeypatch.setattr(RunJournal, 'execution_view', forbidden)
    assert project_ops.workspace_changes(root) == baseline
    for path, group in [('flows/model.yaml', 'sources'), ('flows/model.config', 'sources'), ('operators/new.py', 'sources'),
                        ('README.md', 'workspace_stamp'), ('tables/new.json', 'tables')]:
        before = project_ops.workspace_changes(root)
        (root / path).write_text('changed')
        after = project_ops.workspace_changes(root)
        # README is also an input to original-project discovery. The unchanged
        # platform baseline already invalidates that metadata alongside its text.
        expected={group,'discovery'} if path=='README.md' else {group}
        assert {key for key in after if after[key] != before[key]} == expected
    before = project_ops.workspace_changes(root)
    run.transition({'starting'}, 'running')
    after = project_ops.workspace_changes(root)
    assert after['runs'][run_id] != before['runs'][run_id]
    (directory / 'runtime/ignored.py').write_text('another version')
    (root / 'data/ignored').write_bytes(b'data')
    assert project_ops.workspace_changes(root) == after


def test_change_inventory_reports_omissions_without_disabling_other_scopes(isolated):
    root = isolated
    link = root / 'flows/outside'
    link.symlink_to(root / 'data', target_is_directory=True)
    with pytest.raises(ValueError, match='Symlink'):
        ws.source_files(root, 'flows')
    assert 'Symlink' in project_ops.workspace_changes(root)['source_diagnostics'][0]
    link.unlink()
    for index in range(1025):
        (root / 'operators' / f'file{index}.py').touch()
    with pytest.raises(ValueError, match='budget'):
        ws.source_files(root, 'operators')
    before = project_ops.workspace_changes(root)
    assert any('operators' in item and 'budget' in item for item in before['source_diagnostics'])
    (root / 'flows/new.py').write_text('# discoverable source\n')
    after = project_ops.workspace_changes(root)
    assert after['sources'] != before['sources']
    assert after['source_diagnostics'] == before['source_diagnostics']


def test_run_page_at_capacity_reads_only_selected_outputs(isolated, monkeypatch):
    prepared = runs.prepare(isolated, 'flows/questions.py:select', params='flows/select_a.json')
    original = RunJournal(runs.directory(isolated, prepared['run_id'])).execution_view()['manifest']
    ids = [prepared['run_id']]
    for index in range(127):
        run_id = 'r' + f'{index:024x}'
        manifest = dict(original, run_id=run_id, created_at=index)
        manifest['digest'] = content_digest({k: v for k, v in manifest.items() if k != 'digest'})
        journal = RunJournal(runs.directory(isolated, run_id))
        journal.initialize(manifest, attempts=True)
        journal.transition({None}, 'prepared')
        ids.append(run_id)
    from demiflow.workbench import directory_operations
    directory_operations.rebuild(isolated)  # Explicit adoption of these native fixture records.
    sql = []
    connect = sqlite3.connect
    def traced(*args, **kwargs):
        db = connect(*args, **kwargs)
        db.set_trace_callback(sql.append)
        return db
    monkeypatch.setattr(sqlite3, 'connect', traced)
    started = time.monotonic()
    page = runs.list_runs(isolated, limit=20)
    elapsed = time.monotonic() - started
    assert page['total'] == 128 and len(page['runs']) == 20
    assert page['runs'][0]['run_id'] == prepared['run_id']
    assert not any('node_observations' in query or 'FROM completion' in query for query in sql)
    assert sum('SELECT sequence' in query for query in sql) == 20
    assert all('action_id IN' in query for query in sql if 'SELECT sequence' in query)
    assert not any(query.startswith(('INSERT', 'CREATE', 'UPDATE', 'DELETE')) for query in sql)
    print(f'128 runs: page=20 elapsed={elapsed:.3f}s queries={len(sql)}')
    before = project_ops.workspace_changes(isolated)
    runs.list_runs(isolated, offset=120, limit=20)
    assert project_ops.workspace_changes(isolated) == before
    # Page reads validate their original facts; off-page corruption is checked
    # when that page is requested or during an explicit directory rebuild.
    journal = RunJournal(runs.directory(isolated, ids[1]))
    with connect(journal.path) as db:
        bad = dict(original, run_id=ids[1], root='/wrong-workspace')
        db.execute('UPDATE manifest SET document=?', (json.dumps(bad),))
    assert runs.list_runs(isolated, limit=1)['runs'][0]['run_id'] == prepared['run_id']
    with pytest.raises(ValueError, match='relocated'):
        runs.list_runs(isolated, offset=127, limit=1)


def test_read_projections_preserve_committed_output_facts(isolated):
    prepared = runs.prepare(isolated, 'flows/questions.py:select', params='flows/select_a.json')
    run_id = prepared['run_id']
    journal = RunJournal(runs.directory(isolated, run_id))
    action = next(iter(journal.execution_view()['manifest']['outputs'].values()))['action_id']
    journal.action_event(action, 'intent', {'started': True})
    journal.action_event(action, 'receipt', {'status': 'committed', 'committed_version': 1})
    journal.action_event(action, 'result', {'location_id': 'local', 'ref': {'lance_version': 1, 'row_count': 2}})
    full = runs.show(isolated, run_id)
    listed = runs.list_runs(isolated)['runs'][0]
    assert listed['outputs'] == full['outputs']
    assert listed['status'] == 'prepared'  # A receipt never upgrades overall execution status.
    assert listed['outputs'][0]['status'] == 'committed'


def test_journal_projection_does_not_require_a_forge_manifest(tmp_path):
    journal = RunJournal(tmp_path / 'native-run')
    journal.initialize({'purpose': 'native execution'})
    journal.transition({None}, 'prepared')
    journal.action_event('a', 'intent', {'label': 'first'})
    journal.action_event('b', 'intent', {'label': 'second'})
    identity = journal.execution_view(detail='identity')
    assert identity['events'] == []
    summary = journal.execution_view(detail='summary', action_ids=('b',))
    assert [event['action_id'] for event in summary['events']] == ['b']
    assert journal.execution_view(detail='summary', action_ids=())['events'] == []
