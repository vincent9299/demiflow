"""P12: cache failures cannot invent source failures or surrender uncertain owners."""
from contextlib import contextmanager
import json
from pathlib import Path
import time

import pytest

from demiflow.forge import operation_budget
from demiflow.forge.schema import ForgeError
from demiflow.workbench import directory_operations
from demiflow.workbench.projects import index, workspace as ws
from demiflow.workbench.runs import manager as runs, domain
from test_forge_runtime_review_fixes import control_fixture
from test_forge_index import report


@pytest.fixture
def prepared(tmp_path, monkeypatch):
    root = tmp_path / 'project'
    manifest, target, journal = control_fixture(root)
    monkeypatch.setenv('DEMIFLOW_FORGE_DOMAIN', str(tmp_path / 'domain'))
    monkeypatch.setattr('demiflow.workbench.worker.operation', lambda *a, **k: {'status': 'checked'})
    return root, manifest, target, journal


def marker_for(root, manifest):
    with ws.control_lock(root):
        return index.begin(root, 'run', manifest['run_id'])


@pytest.mark.parametrize('failure', ['pending', 'full', 'corrupt'])
def test_begin_refusal_precedes_fence_reserve_lease_spawn_and_source_mutation(prepared, monkeypatch, failure):
    root, manifest, target, journal = prepared
    if failure == 'pending':
        marker_for(root, manifest)
    elif failure == 'full':
        monkeypatch.setattr(index, 'MAX_PENDING', 1)
        with ws.control_lock(root):
            index.begin(root, 'run', 'r' + '2' * 24)
    else:
        (root / '.forge/index.sqlite').write_bytes(b'corrupt')
    before = journal.execution_status()
    def forbidden(*a, **k):
        pytest.fail('Index refusal must precede execution admission')
    for owner, name in ((runs, '_launch_unit'), (domain, 'reserve'), (runs, '_claim_lease'), (runs.subprocess, 'Popen')):
        monkeypatch.setattr(owner, name, forbidden)
    with pytest.raises((ForgeError, __import__('sqlite3').DatabaseError)):
        runs.start(root, manifest['run_id'], expected=manifest['digest'])
    assert journal.execution_status() == before
    assert not (target / 'unit-1.lock').exists()


@pytest.mark.parametrize('where', ['fence', 'reserve'])
def test_marker_without_reservation_finishes_without_launch_failure(prepared, monkeypatch, where):
    root, manifest, _, journal = prepared
    before = journal.execution_status()
    def rejected(*a, **k):
        raise OSError('original admission failure')
    monkeypatch.setattr(runs if where == 'fence' else domain, '_launch_unit' if where == 'fence' else 'reserve', rejected)
    with pytest.raises(OSError, match='original admission failure'):
        runs.start(root, manifest['run_id'], expected=manifest['digest'])
    assert journal.execution_status() == before
    assert index.status(root)['pending'] == 0
    assert index.page(root, 'run')['total'] == 1


def test_authority_guard_rejects_before_index_begin(prepared, monkeypatch):
    root, manifest, _, _ = prepared
    class Authority:
        def require(self, *a): pass
        def bootstrap(self): return {}
        @contextmanager
        def guard(self, *a):
            raise PermissionError('revoked')
            yield
    monkeypatch.setattr(index, 'begin', lambda *a: pytest.fail('guard must precede marker'))
    with pytest.raises(PermissionError, match='revoked'):
        runs.start(root, manifest['run_id'], expected=manifest['digest'], authority=Authority())


def test_begin_then_expired_deadline_keeps_pending_without_summary_read(prepared, monkeypatch):
    root, manifest, _, journal = prepared
    before = journal.execution_status()
    def expired(*a, **k):
        monkeypatch.setattr(operation_budget, 'current_deadline', lambda: time.monotonic() - 1)
        raise ForgeError('original resource refusal', code='resource_limit')
    monkeypatch.setattr(domain, 'reserve', expired)
    monkeypatch.setattr(runs, 'run_index_entry', lambda *a, **k: pytest.fail('expired cleanup must not read'))
    with pytest.raises(ForgeError, match='original resource refusal'):
        runs.start(root, manifest['run_id'], expected=manifest['digest'])
    assert journal.execution_status() == before
    assert index.status(root)['pending'] == 1


def test_reserve_post_replace_fsync_failure_preserves_owner_and_fence(prepared, monkeypatch):
    root, manifest, target, journal = prepared
    from demiflow.forge import io
    original = io.fsync_directory
    def fail_slot_directory(path):
        if Path(path) == domain.host_partition.location():
            raise OSError('slot directory fsync failed after replace')
        return original(path)
    monkeypatch.setattr(io, 'fsync_directory', fail_slot_directory)
    with pytest.raises(OSError, match='slot directory fsync'):
        runs.start(root, manifest['run_id'], expected=manifest['digest'])
    assert journal.execution_status()['status'] == 'prepared'
    assert (target / 'unit-1.lock').is_file()
    assert domain.reserved(root, manifest['run_id'], 1)
    assert index.status(root)['pending'] == 0
    monkeypatch.setattr(io, 'fsync_directory', original)
    value = runs._reconcile(root, manifest['run_id'], expected_attempt=1)
    assert value['status'] == 'failed' and value['state']['cleanup'] == 'complete'
    assert not domain.reserved(root, manifest['run_id'], 1)


def test_publish_exception_does_not_turn_start_into_launch_failed(prepared, monkeypatch):
    root, manifest, _, journal = prepared
    class SpawnInterrupted(BaseException): pass
    attempted = []
    monkeypatch.setattr(index, 'publish', lambda *a: (_ for _ in ()).throw(OSError('cache only')))
    def spawn(*a, **k):
        attempted.append(True)
        raise SpawnInterrupted()
    monkeypatch.setattr(runs.subprocess, 'Popen', spawn)
    with pytest.raises(SpawnInterrupted):
        runs.start(root, manifest['run_id'], expected=manifest['digest'])
    state = journal.execution_status()
    assert attempted == [True]
    assert state['status'] == 'starting' and state['launch_phase'] == 'spawning'
    assert state.get('reason') != 'launch_failed' and not state['usage_complete']
    assert domain.reserved(root, manifest['run_id'], 1)
    assert index.status(root)['pending'] == 1


@pytest.mark.parametrize('owner', ['run', 'report'])
def test_summary_failure_is_inside_owner_boundary_and_not_missing(prepared, monkeypatch, owner):
    root, manifest, _, _ = prepared
    if owner == 'run':
        module, reader, publish = runs, 'run_index_entry', runs.publish_run_index
        marker = marker_for(root, manifest)
    else:
        (root / '.forge/inspections').mkdir()
        _, marker = report(root, 1, publish=False)
        module, reader, publish = ws, 'report_index_entry', ws.publish_report_index
    def unreadable(*a, **k): raise PermissionError('cannot read owner facts')
    monkeypatch.setattr(module, reader, unreadable)
    monkeypatch.setattr(index, 'publish', lambda *a: pytest.fail('unreadable must not publish Missing'))
    with ws.control_lock(root):
        assert 'cannot read' in publish(root, marker)
    assert index.status(root)['pending'] == 1


def test_old_empty_root_requires_rebuild_while_fresh_root_can_attest_empty(tmp_path):
    root = tmp_path / 'old';(root / '.forge').mkdir(parents=True)
    assert index.status(root)['reason'] == 'index_not_initialized'
    assert not (root / '.forge/index.sqlite').exists()
    with ws.control_lock(root):index.begin(root, 'report', '0' * 64)
    assert not directory_operations.synchronize(root)['complete']
    assert directory_operations.rebuild(root)['complete']
    fresh = tmp_path / 'new';(fresh / '.forge').mkdir(parents=True)
    index.initialize_empty(fresh)
    assert index.status(fresh)['complete']


def test_generation_replacement_and_missing_do_not_delete_members(prepared):
    root, manifest, _, _ = prepared
    marker = marker_for(root, manifest)
    with ws.control_lock(root):
        assert runs.publish_run_index(root, marker) is None
        current = index.begin(root, 'run', manifest['run_id'])
        assert 'changed' in index.publish(root, marker, runs.run_index_entry(root, manifest['run_id']))
        assert index.publish(root, current, None) is None
    assert index.page(root, 'run')['total'] == 1


def test_rebuild_after_replace_cleanup_failure_reports_new_epoch_and_pending(tmp_path, monkeypatch):
    (tmp_path / '.forge/inspections').mkdir(parents=True)
    index.initialize_empty(tmp_path)
    report(tmp_path, 1)
    _, marker = report(tmp_path, 2, publish=False)
    before = index.status(tmp_path)
    original = Path.unlink
    def reject(path, *a, **k):
        if path.parent.name == 'index-pending': raise OSError('marker retained')
        return original(path, *a, **k)
    monkeypatch.setattr(Path, 'unlink', reject)
    with pytest.raises(OSError, match='marker retained'):
        directory_operations.rebuild(tmp_path)
    after = index.status(tmp_path)
    assert after['epoch'] != before['epoch'] and after['reports'] == 2
    assert after['pending'] == 1 and not after['complete']
    assert not list((tmp_path / '.forge').glob('.index-*'))
