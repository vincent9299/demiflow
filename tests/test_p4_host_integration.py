"""Host integration preserves stopped-run capacity and original attempt facts."""
import time

import pytest

from demiflow.forge.platform import content_digest
from demiflow.forge.schema import ForgeError
from demiflow.execution.host.control import RunJournal
from demiflow.workbench.runs import manager, storage


@pytest.fixture
def stopped_run(tmp_path, monkeypatch):
    root = tmp_path / 'workspace'
    identity = 'r' + '1' * 24
    target = root / '.forge/runs' / identity
    target.mkdir(parents=True)
    manifest = dict(format=manager.FORMAT, runtime_contract='demiflow_forge_runtime_v1',
        run_id=identity, root=str(root), entry='flow.py:run', report_digest='2' * 64,
        parameter_path='params.json', outputs={}, limits={}, created_at=1.0)
    manifest['digest'] = content_digest(manifest)
    journal = RunJournal(target)
    journal.initialize(manifest, attempts=True)
    journal.reserve_nodes([])
    journal.reserve_actions([])
    journal.transition({None}, 'failed', reason='launch_failed', recoverable=True,
                       execution_stopped=True, usage_complete=True)
    # Index refresh is derived, and separately exercised by its existing suite.
    monkeypatch.setattr(manager, 'refresh_index', lambda *args, **kwargs: None)
    return root, identity, target, journal


def test_completed_reconcile_preserves_state_revision_and_control_history(stopped_run):
    root, identity, target, journal = stopped_run
    journal.transition({'failed'}, 'failed', cleanup='complete')
    before = journal.execution_status()
    old_attempts = journal.list_attempts()
    result = manager._reconcile(root, identity, expected_attempt=1)
    assert result['state'] == before
    assert journal.execution_status() == before
    assert journal.list_attempts() == old_attempts
    assert result['reconciliation_errors'] == []
    assert not manager.attempt_directory(target, 'work', 1).exists()


@pytest.mark.parametrize('cleanup', [None, 'pending', 'unknown'])
def test_missing_or_unfinished_cleanup_reclaims_private_work_before_completion(stopped_run, cleanup):
    root, identity, target, journal = stopped_run
    if cleanup is not None:
        journal.transition({'failed'}, 'failed', cleanup=cleanup)
    work = manager.attempt_directory(target, 'work', 1)
    work.mkdir(parents=True)
    (work / 'scratch').write_bytes(b'private')
    result = manager._reconcile(root, identity, expected_attempt=1)
    assert result['state']['cleanup'] == 'complete'
    assert result['state']['attempt'] == 1
    assert not work.exists()
    assert result['state']['usage_complete'] is True


def test_completed_cleanup_with_private_work_refuses_without_reviving_capacity(stopped_run):
    root, identity, target, journal = stopped_run
    journal.transition({'failed'}, 'failed', cleanup='complete')
    before = journal.execution_status()
    work = manager.attempt_directory(target, 'work', 1)
    work.mkdir(parents=True)
    (work / 'scratch').write_bytes(b'keep')
    with pytest.raises(ForgeError, match='contradicts'):
        manager._reconcile(root, identity, expected_attempt=1)
    assert journal.execution_status() == before
    assert (work / 'scratch').read_bytes() == b'keep'


def test_expired_cleanup_keeps_pending_work_and_does_not_advance_attempt(stopped_run, monkeypatch):
    root, identity, target, journal = stopped_run
    work = manager.attempt_directory(target, 'work', 1)
    work.mkdir(parents=True)
    (work / 'scratch').write_bytes(b'keep')
    monkeypatch.setattr(storage, 'cleanup_deadline', lambda: time.monotonic() - 1)
    with pytest.raises(Exception, match='deadline|budget'):
        manager._reconcile(root, identity, expected_attempt=1)
    state = journal.execution_status()
    assert state['cleanup'] == 'pending'
    assert state['attempt'] == 1
    assert (work / 'scratch').read_bytes() == b'keep'


def test_stale_reconcile_cannot_clean_current_attempt(stopped_run):
    root, identity, target, journal = stopped_run
    before = journal.execution_status()
    with pytest.raises(ForgeError, match='attempt changed'):
        manager._reconcile(root, identity, expected_attempt=2)
    assert journal.execution_status() == before


def test_refresh_expiry_after_begin_keeps_recovery_marker(tmp_path, monkeypatch):
    from contextlib import nullcontext
    from demiflow.workbench.projects import index
    calls = []
    clock = iter([0.0, 0.0, 2.0])
    monkeypatch.setattr(manager.time, 'monotonic', lambda: next(clock))
    monkeypatch.setattr(manager.ws, 'control_lock', lambda _: nullcontext())
    monkeypatch.setattr(index, 'begin', lambda *args: calls.append('begin') or 'marker')
    monkeypatch.setattr(index, 'publish', lambda *args: calls.append('publish'))
    assert 'deferred' in manager.refresh_index(tmp_path, 'run', deadline=1.0)
    assert calls == ['begin']
