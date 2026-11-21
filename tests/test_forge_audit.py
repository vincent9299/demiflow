"""Change constraints preserve original bytes and never manufacture a pass."""

from demiflow.forge.project import initialize_project
import json
import os
from pathlib import Path
import time

import pytest

from demiflow.forge import audit
from demiflow.workbench.projects import workspace as ws
from demiflow.workbench import project_operations as project_ops
from demiflow.forge.platform import content_digest
from demiflow.forge.schema import ForgeError


def author_tree(root):
    (root / 'flows').mkdir()
    (root / 'operators').mkdir()
    (root / 'operators/a.py').write_text('# existing dirty content\n')
    return root


def baseline(root, limits=None):
    value = dict(format=audit.FORMAT, workspace_id='w'+'a'*32, root=str(root),
                 watch_roots=['flows', 'operators'], inventory_rules_version=1,
                 limits=audit.limits_with(limits), entries=audit.inventory(root, ['flows', 'operators'], limits),
                 workspace_policy_sha256='b'*64, created_at=time.time())
    value['digest'] = content_digest(value)
    return value


def compare(root, before, writes):
    return audit.compare(before, audit.inventory(root, before['watch_roots'], before['limits']), writes,
                         workspace_policy_sha256=before['workspace_policy_sha256'])


def test_baseline_preserves_dirty_and_compares_actual_bytes(tmp_path):
    root = author_tree(tmp_path)
    before = baseline(root)
    allowed = [dict(path='operators/a.py', kind='file')]
    assert compare(root, before, allowed)['change_count'] == 0
    path = root / 'operators/a.py'
    stamp = path.stat()
    path.write_text('# changed! dirty content\n')
    os.utime(path, ns=(stamp.st_atime_ns, stamp.st_mtime_ns))
    assert compare(root, before, allowed)['status'] == 'within_scope'
    (root / 'operators/b.py').write_text('# unexpected')
    result = compare(root, before, allowed)
    assert result['status'] == 'violated' and result['violation_count'] == 1
    assert result['changes'][0]['path'] == 'operators/b.py'


def test_new_parent_does_not_hide_unauthorized_descendant(tmp_path):
    root = author_tree(tmp_path)
    before = baseline(root)
    (root / 'flows/experiment').mkdir()
    (root / 'flows/experiment/a.py').write_text('a')
    allowed = [dict(path='flows/experiment/a.py', kind='file')]
    assert compare(root, before, allowed)['status'] == 'within_scope'
    (root / 'flows/experiment/b.py').write_text('b')
    assert compare(root, before, allowed)['violation_count'] == 2


def test_deletion_and_type_replacement_are_not_ignored(tmp_path):
    root = author_tree(tmp_path)
    before = baseline(root)
    (root / 'operators/a.py').unlink()
    assert compare(root, before, [])['changes'][0]['change'] == 'deleted'
    (root / 'operators/a.py').mkdir()
    assert compare(root, before, [dict(path='operators/a.py', kind='file')])['status'] == 'violated'


@pytest.mark.parametrize('kind', ['symlink', 'fifo', 'entries', 'bytes', 'depth'])
def test_inventory_rejects_unsafe_or_over_budget_tree(tmp_path, kind):
    root = author_tree(tmp_path)
    limits = {}
    if kind == 'symlink':
        (root / 'flows/link').symlink_to('/tmp')
    elif kind == 'fifo':
        os.mkfifo(root / 'flows/pipe')
    elif kind == 'entries':
        limits = {'entries': 2}
    elif kind == 'bytes':
        limits = {'file_bytes': 2}
    else:
        (root / 'flows/a/b').mkdir(parents=True)
        limits = {'depth': 1}
    with pytest.raises((ForgeError, ValueError)):
        audit.inventory(root, ['flows', 'operators'], limits)


def test_baseline_validation_and_control_projection(tmp_path):
    root = author_tree(tmp_path)
    value = baseline(root)
    audit.validate_baseline(value)
    value['entries'].pop()
    with pytest.raises(ForgeError):
        audit.validate_baseline(value)
    assert audit.workspace_digest({'a': 1, 'audit': {}}) == audit.workspace_digest({'a': 1, 'audit': {'changed': True}})
    assert audit.workspace_digest({'a': 2}) != audit.workspace_digest({'a': 1})


def test_complete_notebook_and_missing_root_are_inventoried(tmp_path):
    root = author_tree(tmp_path)
    (root / 'flows/cases.ipynb').write_text(json.dumps({'cells': [{'outputs': ['old output']}]}))
    first = audit.inventory(root, ['flows', 'operators', 'notes'])
    assert {'path': 'notes', 'kind': 'missing'} in first
    (root / 'flows/cases.ipynb').write_text(json.dumps({'cells': [{'outputs': ['new output']}]}))
    assert audit.inventory(root, ['flows', 'operators', 'notes']) != first


def test_scope_lifecycle_uses_original_baseline_and_cas(tmp_path):
    root = tmp_path / 'workspace'
    initialize_project(root)
    for directory in ('flows', 'operators', 'tables', 'data'):
        (root / directory).mkdir()
    ws.add_location(root, 'local', root / 'data', writable=True)
    (root / 'operators/a.py').write_text('# pre-existing dirty')
    def expected():
        return content_digest(ws.load_workspace(root)[1])
    first = project_ops.begin_change_scope(root, expected=expected(), writes=[dict(path='operators/a.py', kind='file')])
    original = first['audit']['change_scope']['baseline']
    (root / 'operators/b.py').write_text('# unexpected')
    assert ws._check_change_scope(root)['status'] == 'violated'
    with pytest.raises(ForgeError, match='outside'):
        audit.require_scope(ws._check_change_scope(root), expected_scope=first['audit']['change_scope'])
    with pytest.raises(ForgeError, match='active'):
        project_ops.begin_change_scope(root, expected=expected(), writes=[])
    amended = ws.amend_change_scope(root, expected=expected(), writes=[dict(path='operators', kind='tree')], reason='explicit extension')
    assert amended['audit']['change_scope']['baseline'] == original
    assert ws._check_change_scope(root)['status'] == 'within_scope'
    with pytest.raises(ForgeError, match='changed'):
        ws.end_change_scope(root, expected=first['workspace_digest'], reason='stale caller')
    ws.end_change_scope(root, expected=expected(), reason='finished')
    assert ws._check_change_scope(root)['status'] == 'unconfigured'
    ws.import_change_scope(root, root / '.forge/audit/baselines' / (original + '.json'),
                           writes=[], expected=expected())
    assert ws._check_change_scope(root)['status'] == 'violated'
    assert (root / 'operators/a.py').read_text() == '# pre-existing dirty'


def test_file_changed_between_stat_and_open_is_rejected(tmp_path, monkeypatch):
    root = author_tree(tmp_path)
    original = os.open
    def racing_open(path, flags, *args, **kwargs):
        fd = original(path, flags, *args, **kwargs)
        if Path(path) == root / 'operators/a.py':
            Path(path).write_text('# concurrent edit')
        return fd
    monkeypatch.setattr(audit.os, 'open', racing_open)
    with pytest.raises(ForgeError, match='changed'):
        audit.inventory(root, ['flows', 'operators'])


def test_non_scalar_contract_fields_are_rejected(tmp_path):
    before = baseline(author_tree(tmp_path))
    with pytest.raises(ForgeError):
        audit.writes([dict(path='flows/a.py', kind=[])])
    before['entries'][0]['kind'] = []
    with pytest.raises(ForgeError):
        audit.validate_baseline(before)


def test_data_roots_and_insufficient_baseline_storage_fail_closed(tmp_path, monkeypatch):
    import shutil
    from types import SimpleNamespace
    root = tmp_path / 'workspace'
    initialize_project(root)
    for directory in ('flows', 'operators', 'tables', 'data'):
        (root / directory).mkdir()
    ws.add_location(root, 'local', root / 'data', writable=True)
    value = ws.load_workspace(root)[1]
    expected = content_digest(value)
    with pytest.raises(ForgeError, match='overlap'):
        ws._capture_change_baseline(root, watch_roots=['flows', 'operators', 'data'], expected=expected)
    before = ws._capture_change_baseline(root, watch_roots=['flows', 'operators'], expected=expected)
    monkeypatch.setattr(shutil, 'disk_usage', lambda path: SimpleNamespace(free=0))
    with pytest.raises(ForgeError, match='storage budget'):
        ws._store_scope(root, value, before, [], expected)
    assert ws.load_workspace(root)[1]['audit']['change_scope'] is None
    assert not (root / '.forge/audit/baselines' / (before['digest'] + '.json')).exists()
