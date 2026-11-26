"""P12 refresh composition does not confer run authority or hide unavailable indexes."""
from contextlib import contextmanager
from types import SimpleNamespace

import pytest

from demiflow.forge.platform import content_digest
from demiflow.workbench import project_operations
from demiflow.forge import index

from demiflow.workbench.projects import workspace as ws
from demiflow.forge.runs import manager as runs



def test_application_reads_status_once_and_passes_same_owned_object(tmp_path, monkeypatch):
    info = {'epoch': 'fixed', 'reports': 3, 'active_runs': []}
    calls = []
    monkeypatch.setattr(index, 'status', lambda root: calls.append(root) or info)
    project = dict(workspace=str(tmp_path), sources='source-hint')
    monkeypatch.setattr(ws, 'project_change_hints', lambda root: project)
    def run_hints(root, supplied):
        assert supplied is info and root == tmp_path
        return dict(runs={}, run_revision=content_digest(info))
    monkeypatch.setattr(runs, 'run_change_hints', run_hints)
    result = project_operations.workspace_changes(tmp_path)
    assert calls == [tmp_path] and result['index'] is info
    assert result == {**project, 'runs': {}, 'run_revision': content_digest(info),
        'index': info, 'reports': content_digest({'epoch': 'fixed', 'reports': 3})}


def test_unavailable_index_never_invokes_legacy_scan_and_missing_uses_same_hint(tmp_path, monkeypatch):
    monkeypatch.setattr(runs, '_run_ids', lambda *a, **k: pytest.fail('unavailable is not uninitialized'))
    assert runs.run_change_hints(tmp_path, {'reason': 'index_unavailable'})['runs'] == {}
    identity = 'r' + 'a' * 24
    assert runs.run_change_hints(tmp_path, {'active_runs': [identity]})['runs'] == {identity: content_digest(None)}


def test_uninitialized_fallback_is_bounded_and_observation_token_stays_opaque(tmp_path, monkeypatch):
    from demiflow.execution.host import inspection
    identity = 'r' + 'a' * 24
    target = tmp_path / '.forge/runs' / identity; target.mkdir(parents=True)
    token = object(); observed = []
    def fallback(root, *, maximum):
        assert maximum == 128 and root == tmp_path
        return [identity]
    @contextmanager
    def observation(path, *, deadline):
        observed.append((path, deadline))
        yield SimpleNamespace(change_token=token)
    monkeypatch.setattr(runs, '_run_ids', fallback)
    monkeypatch.setattr(inspection, 'observe_storage', observation)
    result = runs.run_change_hints(tmp_path, {'reason': 'index_not_initialized'})
    assert result['runs'][identity] is token and observed == [(target, None)]
