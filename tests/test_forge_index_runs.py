"""Run membership is derived; the lease owner and source journal gate start."""
import json

import pytest

from demiflow.execution.run_journal import RunJournal
from demiflow.workbench.projects import index
from demiflow.workbench.runs import manager as runs
from demiflow.workbench.projects import workspace as ws
from demiflow.forge.platform import content_digest


def fixture_run(root, number, *, state='prepared'):
    identity = 'r' + format(number, '024x')
    directory = root / '.forge/runs' / identity
    directory.mkdir(parents=True)
    manifest = dict(format=runs.FORMAT, run_id=identity, root=str(root), created_at=number,
                    outputs={}, entry='flows/a.py:run', parameter_path=None, limits={}, report_digest='0' * 64)
    manifest['digest'] = content_digest(manifest)
    run = RunJournal(directory)
    run.initialize(manifest)
    run.transition({None}, state)
    with ws.control_lock(root):
        marker = index.begin(root, 'run', identity)
        assert runs.publish_run_index(root, marker) is None
    return identity, run


def test_run_pages_only_load_page_members_and_have_no_128_limit(tmp_path, monkeypatch):
    (tmp_path / '.forge').mkdir()
    # Explicitly initialize an empty directory before source publication.
    with ws.control_lock(tmp_path):
        index.initialize_empty(tmp_path)
    ids = [fixture_run(tmp_path, i)[0] for i in range(140)]
    original, calls = runs._load, []
    def counted(root, identity, **kwargs):
        calls.append(identity)
        return original(root, identity, **kwargs)
    monkeypatch.setattr(runs, '_load', counted)
    page = runs.list_runs(tmp_path, limit=7)
    assert page['total'] == 140
    assert [r['run_id'] for r in page['runs']] == list(reversed(ids))[:7]
    assert set(calls) == set(ids[-7:]) and len(calls) == 14
    assert page['index']['complete']


def test_lease_owner_adopts_legacy_once_and_does_not_trust_cached_activity(tmp_path, monkeypatch):
    (tmp_path / '.forge').mkdir()
    identity, journal = fixture_run(tmp_path, 1, state='running')
    with runs.execution_lock(tmp_path) as lease:
        assert runs._lease_owner(tmp_path, lease) == identity
    # A stale derived flag must not change the actual lease owner.
    with index._connect(index._path(tmp_path), write=True) as db:
        db.execute('UPDATE entries SET active=0')
        db.commit()
    assert not index.status(tmp_path)['active_runs']
    def forbidden(*args, **kwargs):
        raise AssertionError('Lease owner should not enumerate historical runs')
    monkeypatch.setattr(runs, '_run_ids', forbidden)
    with runs.execution_lock(tmp_path) as lease:
        assert runs._lease_owner(tmp_path, lease) == identity
        assert runs._load(tmp_path, identity, detail='identity')[1]['state']['status'] == 'running'
    journal.transition({'running'}, 'interrupted')
    assert runs.refresh_index(tmp_path, identity) is None


def test_new_run_admission_requires_entry_and_registered_parameter_path(tmp_path, monkeypatch):
    workspace = {'format': ws.FORMAT_V2, 'entries': {}, 'budgets': {}}
    monkeypatch.setattr(ws, 'load_workspace', lambda root: (tmp_path, workspace, {}))
    with pytest.raises(ValueError, match='registered Flow'):
        runs._prepare(tmp_path, 'flows/missing.py:run', params={})
    workspace['entries']['flows/missing.py:run'] = ['flows/known.json']
    with pytest.raises(ValueError, match='Parameter file.*registered'):
        runs._prepare(tmp_path, 'flows/missing.py:run', params='flows/other.json')
