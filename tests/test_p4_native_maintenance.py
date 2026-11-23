"""Stopped maintenance keeps native ownership real and host lifecycle untouched."""
import fcntl
import hashlib
import os
import sqlite3
import time

import pytest

from demiflow.errors import ExecutionConflict, LanceResourceNotFound
from demiflow.execution.pipeline.run_context import RunContext, clean_stopped_execution, admit_absent_create
from demiflow.execution.run_journal import RunJournal
from test_p4_attempt_reservations import managed, resume


def native(directory):
    journal = managed(directory, actions=['a'])
    journal.transition({'interrupted'}, 'running')
    with RunContext.attach(journal):
        pass
    journal.transition({'running'}, 'interrupted', cleanup='complete', execution_stopped=True)
    return RunJournal(directory, attempt=1)


def cleanup(journal, **kwargs):
    state = journal.execution_status()
    args = dict(expected_attempt=state.get('attempt'), expected_revision=state['revision'],
                confirm_stopped=lambda snapshot: True, deadline=time.monotonic() + 10)
    args.update(kwargs)
    return clean_stopped_execution(journal, **args)


def owners(journal):
    with sqlite3.connect(journal.path) as db:
        return db.execute('SELECT * FROM runtime_owner').fetchall()


def insert_gc(journal, status):
    with sqlite3.connect(journal.path) as db:
        db.execute('INSERT INTO runtime_dataflow_gc VALUES (?,?,?,?,?,?,?,?)',
                   ('g', 's', 'd', 0, 'c', 1, 2, status))


def test_cleanup_has_no_lifecycle_or_capacity_side_effects(tmp_path):
    journal = native(tmp_path)
    before = journal.execution_status()
    assert cleanup(journal) == dict(attempt=1, revision=before['revision'], native_work=True)
    assert journal.execution_status() == before and not owners(journal)
    assert resume(journal)['attempt'] == 2


def test_identity_free_cleanup_does_not_create_native_facts(tmp_path):
    journal = managed(tmp_path)
    before = hashlib.sha256(journal.path.read_bytes()).digest()
    assert cleanup(journal)['native_work'] is False
    assert hashlib.sha256(journal.path.read_bytes()).digest() == before
    insert_gc(journal, 'done')
    with pytest.raises(ExecutionConflict, match='no execution identity'):
        cleanup(journal)


def test_native_done_gc_is_retained_and_does_not_block_new_attempt(tmp_path):
    journal = native(tmp_path)
    insert_gc(journal, 'done')
    cleanup(journal)
    assert resume(journal)['attempt'] == 2
    with sqlite3.connect(journal.path) as db:
        assert db.execute('SELECT status FROM runtime_dataflow_gc').fetchall() == [('done',)]


@pytest.mark.parametrize('fact', ['owner', 'prepared', 'quarantined'])
def test_managed_attempt_guard_covers_all_native_unsettled_facts(tmp_path, fact):
    journal = native(tmp_path)
    with sqlite3.connect(journal.path) as db:
        if fact == 'owner':
            db.execute('INSERT INTO runtime_owner VALUES (1,99,?)', ('old-owner',))
        else:
            db.execute('INSERT INTO runtime_dataflow_gc VALUES (?,?,?,?,?,?,?,?)', ('g','s','d',0,'c',1,2,fact))
    with pytest.raises(ExecutionConflict, match='not settled'):
        resume(journal)
    assert journal.execution_status()['attempt'] == 1


def test_revision_attempt_stop_and_live_lease_are_required(tmp_path):
    journal = native(tmp_path)
    for kwargs in [dict(expected_attempt=2), dict(expected_revision=None),
                   dict(expected_revision=0), dict(confirm_stopped=lambda _: 1)]:
        with pytest.raises(ExecutionConflict):
            cleanup(journal, **kwargs)
    fd = os.open(tmp_path / '.runtime.lock', os.O_RDWR)
    try:
        fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        with pytest.raises(ExecutionConflict, match='lease is busy'):
            cleanup(journal)
    finally:
        os.close(fd)
    assert not owners(journal)


def test_active_revision_can_advance_for_cancel_and_snapshot_is_detached(tmp_path):
    journal = native(tmp_path)
    journal.transition({'interrupted'}, 'running')
    def confirm(snapshot):
        assert set(snapshot) == {'attempt','revision','status','unit_identity','supervisor','worker','execution_stopped','reason'}
        snapshot['status'] = 'tampered'
        journal.transition({'running'}, 'stopping')
        return True
    assert cleanup(journal, expected_revision=None, confirm_stopped=confirm)['native_work']
    assert journal.execution_status()['status'] == 'stopping'


def test_partial_claim_failure_releases_only_its_real_owner(tmp_path, monkeypatch):
    journal = native(tmp_path)
    original = RunJournal._reconcile_published_gc
    calls = [0]
    def fail_once(self, fence):
        calls[0] += 1
        if calls[0] == 1:
            raise OSError('claim GC failed')
        return original(self, fence)
    monkeypatch.setattr(RunJournal, '_reconcile_published_gc', fail_once)
    with pytest.raises(OSError, match='claim GC failed'):
        cleanup(journal)
    assert not owners(journal)
    assert resume(journal)['attempt'] == 2


def test_release_failure_retains_owner_and_blocks_resume(tmp_path, monkeypatch):
    journal = native(tmp_path)
    def fail(self, fence):
        raise OSError('release failed')
    monkeypatch.setattr(RunJournal, 'release_execution', fail)
    with pytest.raises(OSError, match='release failed'):
        cleanup(journal)
    assert owners(journal)
    with pytest.raises(ExecutionConflict, match='not settled'):
        resume(journal)


def test_expired_cleanup_does_not_start_and_expiry_after_claim_retains_owner(tmp_path, monkeypatch):
    journal = native(tmp_path)
    with pytest.raises(ExecutionConflict, match='expired'):
        cleanup(journal, deadline=time.monotonic() - 1)
    assert not owners(journal)
    def expire(self, fence):
        monkeypatch.setattr('demiflow.execution.run_journal.time.monotonic', lambda: 10**12)
    monkeypatch.setattr(RunJournal, 'clean_orphans', expire)
    with pytest.raises(ExecutionConflict, match='expired'):
        cleanup(journal)
    assert owners(journal)
    with pytest.raises(ExecutionConflict, match='not settled'):
        resume(journal)


def create_action(journal, uri):
    from demiflow.lance.model import LanceWriteSpec
    from demiflow.execution.recovery_identity import plan_identity
    from demiflow.execution.recovery_contract import identity
    spec = LanceWriteSpec(str(uri), mode='create')
    configuration = journal.recovery_identity()
    journal.claim_execution(configuration, 'setup')
    journal.bind_execution_action('setup', 'a', plan_identity('write_table', None, None, spec))
    journal.action_event('a', 'intent', dict(kind='write_table', spec=spec.to_dict(),
        request_hash=spec.content_hash, operation_id=identity([configuration['run_id'], 'a', 1])))
    journal.release_execution('setup')
    return spec


def admission(journal, uri, **kwargs):
    args = dict(action_id='a', expected_attempt=journal.attempt,
        expected_revision=journal.execution_status()['revision'], expected_uri=str(uri),
        confirm_stopped=lambda _: True, deadline=time.monotonic()+10)
    args.update(kwargs)
    return admit_absent_create(journal, **args)


def test_absence_storage_is_once_before_claim_and_repeated_admission_rechecks(tmp_path, monkeypatch):
    journal = native(tmp_path)
    uri = tmp_path / 'absent.lance'
    create_action(journal, uri)
    calls = []
    def absent(*args, **kwargs):
        assert not owners(journal)
        calls.append('inspect')
        raise LanceResourceNotFound('absent')
    monkeypatch.setattr('demiflow.lance.reconcile.reconcile_lance_create', lambda *a, **kw: calls.append('reconcile'))
    monkeypatch.setattr('demiflow.lance.storage.inspect_lance', absent)
    assert admission(journal, uri)['admitted']
    assert admission(journal, uri)['admitted']
    assert calls == ['reconcile','inspect','reconcile','inspect']
    assert not owners(journal) and journal.source_binding('a-create-target-absent')
    monkeypatch.setattr('demiflow.lance.storage.inspect_lance', lambda *a, **kw: object())
    assert admission(journal, uri)['admitted'] is False


def test_storage_failure_and_revision_change_leave_no_owner_or_binding(tmp_path, monkeypatch):
    journal = native(tmp_path)
    uri = tmp_path / 'out.lance'
    create_action(journal, uri)
    def denied(*args, **kwargs):
        raise PermissionError('unreadable')
    monkeypatch.setattr('demiflow.lance.reconcile.reconcile_lance_create', denied)
    with pytest.raises(PermissionError):
        admission(journal, uri)
    assert not owners(journal) and journal.source_binding('a-create-target-absent') is None
    monkeypatch.setattr('demiflow.lance.reconcile.reconcile_lance_create', lambda *a, **kw: None)
    def changed(*args, **kwargs):
        journal.transition({'interrupted'}, 'interrupted', reason='changed')
        raise LanceResourceNotFound('absent')
    monkeypatch.setattr('demiflow.lance.storage.inspect_lance', changed)
    with pytest.raises(ExecutionConflict, match='revision'):
        admission(journal, uri)
    assert not owners(journal) and journal.source_binding('a-create-target-absent') is None


def test_current_attempt_never_borrows_old_intent(tmp_path, monkeypatch):
    journal = native(tmp_path)
    uri = tmp_path / 'out.lance'
    create_action(journal, uri)
    resume(journal, actions=['a'])
    current = RunJournal(tmp_path, attempt=2)
    current.transition({'prepared'}, 'interrupted')
    def forbidden(*args, **kwargs):
        raise AssertionError('storage must not be called without selected-attempt intent')
    monkeypatch.setattr('demiflow.lance.reconcile.reconcile_lance_create', forbidden)
    assert admission(current, uri)['admitted'] is False


def test_publication_contention_respects_maintenance_deadline(tmp_path):
    journal = native(tmp_path)
    descriptor = os.open(tmp_path / '.publication.lock', os.O_RDWR)
    before = time.monotonic()
    try:
        fcntl.flock(descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
        with pytest.raises(ExecutionConflict, match='expired'):
            cleanup(journal, deadline=before + .04)
    finally:
        os.close(descriptor)
    assert time.monotonic() - before < 1 and not owners(journal)


def test_body_failure_is_not_hidden_by_successful_release(tmp_path, monkeypatch):
    journal = native(tmp_path)
    def fail(self, fence):
        raise OSError('orphan cleanup failed')
    monkeypatch.setattr(RunJournal, 'clean_orphans', fail)
    with pytest.raises(OSError, match='orphan cleanup failed'):
        cleanup(journal)
    assert not owners(journal)
    assert journal.execution_status()['cleanup'] == 'complete'


def test_legacy_no_attempt_no_identity_is_readonly_noop(tmp_path):
    journal = RunJournal(tmp_path)
    journal.initialize({'legacy': True})
    journal.transition({None}, 'interrupted')
    before = hashlib.sha256(journal.path.read_bytes()).digest()
    result = cleanup(journal)
    assert result['attempt'] is None and result['native_work'] is False
    assert hashlib.sha256(journal.path.read_bytes()).digest() == before


def test_gc_deadline_reaches_original_reconcile_and_stops_next_intent(tmp_path, monkeypatch):
    from contextlib import nullcontext
    from demiflow.execution.storage import gc
    journal = native(tmp_path)
    monkeypatch.setattr(gc, '_exclusive', lambda *args: nullcontext())
    monkeypatch.setattr(journal, 'dataflow_gc_intents', lambda: [{'gc_id': 'first'}, {'gc_id': 'next'}])
    observed = []
    def reconcile(journal, fence, item, *, deadline):
        observed.append((item['gc_id'], deadline))
        monkeypatch.setattr(gc.time, 'monotonic', lambda: 200)
        return 7
    monkeypatch.setattr(gc, '_reconcile', reconcile)
    monkeypatch.setattr(gc.time, 'monotonic', lambda: 100)
    result = gc.recover_published_gc(journal, 'fence', deadline=150)
    assert observed == [('first', 150)]
    assert result['removed'] == 1 and result['bytes'] == 7 and 'exhausted' in result['blocked']


@pytest.mark.parametrize('attempt', [None, True])
def test_malformed_modern_attempt_cannot_use_legacy_noop(tmp_path, attempt):
    import json
    journal = managed(tmp_path)
    state = journal.execution_status()
    state['attempt'] = attempt
    with sqlite3.connect(journal.path) as db:
        db.execute('UPDATE execution_state SET document=?', (json.dumps(state),))
    with pytest.raises(ExecutionConflict, match='attempt differs'):
        cleanup(journal, expected_attempt=None if attempt is None else 1)
