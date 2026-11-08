"""Explicit project inventory and request-local routing, with real controls."""
import json
from pathlib import Path
import shutil
import threading
import urllib.error
import urllib.request
import uuid

import pytest

from demiflow.workbench.projects import registry as console
from demiflow.workbench.projects import workspace as ws
from demiflow.forge.platform import content_digest
from demiflow.forge.schema import ForgeError
from demiflow.workbench.api import WorkspaceServer
from test_forge_workspace import demo, isolated


def test_existing_init_preserves_sources_and_rolls_back_only_owned_files(demo, tmp_path, monkeypatch):
    root = tmp_path / 'existing'
    (root / 'flows').mkdir(parents=True)
    (root / 'flows/user.py').write_text('raise RuntimeError("must never execute")')
    (root / 'README.md').write_text('User documentation')
    snapshot = ws.load_workspace(demo[0])[2]
    monkeypatch.setattr(ws, 'build_snapshot', lambda modules, **kwargs: snapshot)
    write = ws.write_control
    monkeypatch.setattr(ws, 'write_control', lambda *a, **kw: (_ for _ in ()).throw(OSError('fixture commit failure')))
    with pytest.raises(OSError, match='commit failure'):
        ws.init_workspace(root, existing=True)
    assert (root / 'README.md').read_text() == 'User documentation'
    assert (root / 'flows/user.py').read_text().startswith('raise')
    assert not (root / '.forge').exists()
    monkeypatch.setattr(ws, 'write_control', write)
    result = ws.init_workspace(root, existing=True)
    assert ws.load_workspace(root)[1]['id'] == result['id']
    assert (root / 'README.md').read_text() == 'User documentation'
    with pytest.raises(ForgeError, match='already exist'):
        ws.init_workspace(root, existing=True)


def test_nonempty_initialization_requires_explicit_mode_and_rejects_conflicts(tmp_path):
    (tmp_path / 'flows').write_text('existing user file')
    with pytest.raises(ForgeError, match='empty'):
        ws.init_workspace(tmp_path)
    ws.init_workspace(tmp_path, existing=True)
    assert (tmp_path / 'flows').read_text() == 'existing user file'


def test_registration_is_explicit_and_helper_is_not_a_flow(isolated):
    context = ws.workspace_context(isolated)
    assert [f['entry'] for f in context['flows']] == ['flows/questions.py:select']
    assert context['flows'][0]['parameters'] == ['flows/select_a.json', 'flows/select_b.json']
    with pytest.raises(ForgeError, match='missing'):
        ws.register_flow(isolated, 'flows/questions.py:nonexistent', params='flows/select_a.json')
    ws.register_flow(isolated, 'operators/question_rules.py:keep_selected', params='flows/select_a.json')
    assert ws.inspect_flow(isolated, 'operators/question_rules.py:keep_selected', params='flows/select_a.json')['status'] != 'checked'
    assert ws.register_flow(isolated, 'flows/questions.py:select', params='flows/select_a.json')['status'] == 'unchanged'
    path = isolated / 'flows/questions.py'
    path.write_text(path.read_text().replace('def select(', 'def renamed('))
    assert ws.workspace_context(isolated)['flows'][0]['diagnostic'] == 'Registered function is missing'


def test_report_binds_only_used_workspace_policy(isolated):
    result = ws.inspect_flow(isolated, 'flows/questions.py:select', params='flows/select_a.json')
    assert result['status'] == 'checked', result
    (isolated / 'flows/other.py').write_text('def independent():\n    pass\n')
    ws.register_flow(isolated, 'flows/other.py:independent', params='flows/select_b.json')
    control = ws.read_control(isolated / 'workspace.json')
    control['name'] = 'renamed'
    (isolated / 'workspace.json').write_text(json.dumps(control))
    assert ws.report_status(isolated, result['digest'])['status'] == 'checked'
    control['locations']['local']['writable'] = False
    (isolated / 'workspace.json').write_text(json.dumps(control))
    changed = ws.report_status(isolated, result['digest'])
    assert changed['status'] == 'stale' and 'workspace_changed' in changed['changes']


def test_registry_identity_conflicts_removal_and_broken_project_isolation(isolated, tmp_path):
    registry = tmp_path / 'console.json'
    row = console.add(registry, isolated)
    assert console.add(registry, isolated)['status'] == 'unchanged'
    with pytest.raises(ForgeError, match='different'):
        console.add(registry, isolated, allow_runs=True)
    clone = tmp_path / 'clone'
    shutil.copytree(isolated, clone)
    with pytest.raises(ForgeError, match='different'):
        console.add(registry, clone)
    value = ws.read_control(clone / 'workspace.json')
    value['id'] = 'w' + uuid.uuid4().hex
    (clone / 'workspace.json').write_text(json.dumps(value))
    second = console.add(registry, clone)
    (clone / 'workspace.json').unlink()
    listing = console.projects(console.load_registry(registry))
    assert {p['workspace_id']: p['status'] for p in listing['projects']} == {
        row['workspace_id']: 'available', second['workspace_id']: 'unavailable'}
    with pytest.raises(ForgeError, match='changed'):
        console.remove(registry, row['workspace_id'], expected='wrong')
    console.remove(registry, second['workspace_id'], expected=listing['digest'])
    assert clone.exists() and isolated.exists()


def test_legacy_workspace_registration_does_not_rewrite_control(isolated, tmp_path):
    path = isolated / 'workspace.json'
    value = ws.read_control(path)
    value.pop('id'); value.pop('entries'); value.pop('audit', None); value['format'] = ws.FORMAT
    value['locations']['local']['root'] = 'data'
    raw = json.dumps(value)
    path.write_text(raw)
    row = console.add(tmp_path / 'console.json', isolated)
    assert row['workspace_id'].startswith('l') and path.read_text() == raw
    assert len(ws.workspace_context(isolated)['flows']) == 3


def test_http_routes_same_alias_to_correct_project_and_checks_permissions(isolated, tmp_path):
    registry = tmp_path / 'console.json'
    first = console.add(registry, isolated, allow_runs=False)
    other = tmp_path / 'other'
    shutil.copytree(isolated, other)
    control = ws.read_control(other / 'workspace.json')
    control.update(id='w'+uuid.uuid4().hex, name='other-project')
    (other / 'workspace.json').write_text(json.dumps(control))
    second = console.add(registry, other, allow_runs=True)
    server = WorkspaceServer(registry=registry, enable_runs=True)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()

    def request(path, body=None, token=True):
        headers = {'X-Forge-Token': server.token} if token else {}
        req = urllib.request.Request(server.origin + path, data=json.dumps(body).encode() if body else None, headers=headers)
        with urllib.request.urlopen(req, timeout=30) as response:
            return json.load(response)

    try:
        assert len(request('/api/projects')['projects']) == 2
        for row, name in ((first, 'question-experiments'), (second, 'other-project')):
            context = request('/api/workspaces/'+row['workspace_id']+'/context')
            assert context['name'] == name and context['workspace'] == row['root']
            assert context['runs_enabled'] == row['allow_runs']
        assert server.root is None
        for path, body in [('/api/context', None),
                           ('/api/workspaces/w'+'0'*32+'/context', None),
                           ('/api/workspaces/'+first['workspace_id']+'/run-prepare', {'entry':'x','params':'x'})]:
            with pytest.raises(urllib.error.HTTPError):
                request(path, body)
        with pytest.raises(urllib.error.HTTPError) as exc:
            request('/api/projects', token=False)
        assert exc.value.code == 403
    finally:
        server.shutdown(); server.server_close(); thread.join(timeout=5)
