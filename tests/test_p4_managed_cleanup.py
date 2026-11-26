"""Private work deletion observes one deadline without following replacement paths."""
import os
import time

import pytest

from demiflow.forge.schema import ForgeError
from demiflow.forge.runs.storage import cleanup_deadline, remove_attempt_work


def test_cleanup_deadline_inherits_the_smaller_control_bound():
    from demiflow.forge.operation_budget import scope
    before = time.monotonic()
    assert before + 59 <= cleanup_deadline() <= time.monotonic() + 60
    with scope(deadline=before + 5):
        assert cleanup_deadline() == before + 5


def test_only_private_work_is_removed(tmp_path):
    work = tmp_path / 'work' / 'attempt-1'
    (work / 'sub').mkdir(parents=True)
    (work / 'sub' / 'tmp.bin').write_bytes(b'bytes')
    retained = tmp_path / 'materializations' / 'committed.bin'
    retained.parent.mkdir()
    retained.write_bytes(b'protected')
    remove_attempt_work(work, deadline=time.monotonic()+5)
    assert not work.exists() and retained.read_bytes() == b'protected'
    remove_attempt_work(work, deadline=time.monotonic()+5)


@pytest.mark.parametrize('kind', ['symlink', 'fifo'])
def test_symlink_and_special_entries_fail_without_touching_external_data(tmp_path, kind):
    work = tmp_path / 'work'
    work.mkdir()
    target = tmp_path / 'external'
    target.write_bytes(b'untouched')
    if kind == 'symlink':
        (work / 'entry').symlink_to(target)
    else:
        os.mkfifo(work / 'entry')
    with pytest.raises(ForgeError, match='symlink or special'):
        remove_attempt_work(work, deadline=time.monotonic()+5)
    assert target.read_bytes() == b'untouched' and work.exists()


def test_mid_tree_deadline_keeps_remaining_files_for_reconcile(tmp_path, monkeypatch):
    work = tmp_path / 'work'
    work.mkdir()
    for number in range(3):
        (work / str(number)).write_bytes(b'owned')
    original = os.unlink
    def expire_after_unlink(*args, **kwargs):
        original(*args, **kwargs)
        monkeypatch.setattr('demiflow.forge.operation_budget.time.monotonic', lambda: 10**12)
    with monkeypatch.context() as scope:
        scope.setattr(os, 'unlink', expire_after_unlink)
        with pytest.raises(ForgeError, match='deadline'):
            remove_attempt_work(work, deadline=time.monotonic()+5)
    assert work.exists() and len(list(work.iterdir())) == 2
    # Expiry is a partial deletion, not a success marker; the same directory can finish later.
    monkeypatch.undo()
    remove_attempt_work(work, deadline=time.monotonic()+5)
    assert not work.exists()


def test_directory_replacement_is_rejected_and_replacement_preserved(tmp_path, monkeypatch):
    work = tmp_path / 'work'
    work.mkdir()
    (work / 'entry').mkdir()
    original = os.open
    replaced = [False]
    def replace_before_open(path, flags, *args, **kwargs):
        if path == 'entry' and not replaced[0]:
            replaced[0] = True
            (work / 'entry').rename(work / 'old')
            (work / 'entry').mkdir()
            (work / 'entry' / 'foreign').write_bytes(b'preserve')
        return original(path, flags, *args, **kwargs)
    monkeypatch.setattr(os, 'open', replace_before_open)
    with pytest.raises(ForgeError, match='identity changed'):
        remove_attempt_work(work, deadline=time.monotonic()+5)
    assert (work / 'entry' / 'foreign').read_bytes() == b'preserve'


def test_supervisor_lease_closes_even_if_initialization_or_finalization_raises(tmp_path, monkeypatch):
    from demiflow.forge.runs import worker

    fd = os.open(tmp_path / 'execution.lock', os.O_RDWR | os.O_CREAT, 0o600)
    def fail(*args, **kwargs):
        raise RuntimeError('finalization failed')
    monkeypatch.setattr(worker, '_supervise', fail)
    with pytest.raises(RuntimeError, match='finalization'):
        worker.supervise(tmp_path, 'r' + '1' * 24, fd, 1)
    with pytest.raises(OSError):
        os.fstat(fd)


@pytest.mark.parametrize('cleanup_failure', [False, True])
def test_supervisor_publishes_one_final_state_after_cancel_race(tmp_path, monkeypatch, cleanup_failure):
    from demiflow.execution.journal.run_journal import RunJournal
    from demiflow.forge.runs import worker, manager

    from demiflow.forge.runs import storage

    from test_p4_attempt_reservations import managed
    run_id = 'r' + '2' * 24
    journal = managed(manager.directory(tmp_path, run_id))
    journal.transition({'interrupted'}, 'starting')
    state = journal.execution_status()
    manifest = dict(limits={}, object_locations=[])
    monkeypatch.setattr(manager, '_load', lambda *args: (journal.directory, dict(manifest=manifest, state=state)))
    monkeypatch.setattr(manager, 'remaining_limits', lambda *args: {})
    def launch_failure(*args, **kwargs):
        raise OSError('injected pre-spawn failure')
    monkeypatch.setattr(manager, 'verify_seal', launch_failure)
    indexed = []
    monkeypatch.setattr(manager, 'refresh_index', lambda *args, **kwargs: indexed.append(kwargs['deadline']))
    original_remove = storage.remove_attempt_work
    def race_cancel(work, *, deadline):
        journal.transition({'starting'}, 'stopping')
        if cleanup_failure:
            raise ForgeError('cleanup deadline expired', code='resource_limit')
        return original_remove(work, deadline=deadline)
    monkeypatch.setattr(storage, 'remove_attempt_work', race_cancel)
    transitions = []
    original_transition = RunJournal.transition
    def record(self, expected, status, **kwargs):
        if status in {'failed', 'succeeded', 'interrupted', 'cancelled'}:
            transitions.append((expected, status, kwargs))
        return original_transition(self, expected, status, **kwargs)
    monkeypatch.setattr(RunJournal, 'transition', record)
    fd = os.open(tmp_path / 'lease', os.O_RDWR | os.O_CREAT, 0o600)
    worker.supervise(tmp_path, run_id, fd, 1)
    final = journal.execution_status()
    assert len(transitions) == 1 and final['status'] == 'failed'
    assert final['execution_stopped'] is True and final['stop_phase'] == 'confirmed'
    assert final['cleanup'] == ('pending' if cleanup_failure else 'complete')
    assert ('cleanup_error' in final) is cleanup_failure
    assert len(indexed) == 1 and indexed[0] is not None
    with pytest.raises(OSError):
        os.fstat(fd)


def test_supervisor_invalid_identity_still_closes_inherited_lease(tmp_path):
    from demiflow.forge.runs import worker

    fd = os.open(tmp_path / 'lease', os.O_RDWR | os.O_CREAT, 0o600)
    with pytest.raises(ForgeError, match='Invalid run identity'):
        worker.supervise(tmp_path, 'run', fd, 1)
    with pytest.raises(OSError):
        os.fstat(fd)
    assert not (tmp_path / '.forge').exists()


def test_replaced_ancestor_cannot_redirect_cleanup_and_closes_descriptors(tmp_path, monkeypatch):
    run = tmp_path / 'run'
    work = run / 'work' / 'attempt-1'
    work.mkdir(parents=True)
    (work / 'original').write_bytes(b'owned')
    external = tmp_path / 'external'
    foreign = external / 'work' / 'attempt-1' / 'protected'
    foreign.parent.mkdir(parents=True)
    foreign.write_bytes(b'do not delete')
    original_open = os.open
    replaced, descriptors = [], set()
    before = len(os.listdir('/proc/self/fd'))

    def replace_during_component_open(path, flags, *args, **kwargs):
        if path == 'run' and 'dir_fd' in kwargs and not replaced:
            replaced.append(True)
            run.rename(tmp_path / 'saved-run')
            run.symlink_to(external, target_is_directory=True)
        fd = original_open(path, flags, *args, **kwargs)
        descriptors.add(fd)
        return fd

    monkeypatch.setattr(os, 'open', replace_during_component_open)
    with pytest.raises(OSError):
        remove_attempt_work(work, deadline=time.monotonic() + 5)
    assert replaced and foreign.read_bytes() == b'do not delete'
    assert (tmp_path / 'saved-run' / 'work' / 'attempt-1' / 'original').read_bytes() == b'owned'
    assert len(os.listdir('/proc/self/fd')) == before
    for descriptor in descriptors:
        with pytest.raises(OSError):
            os.fstat(descriptor)
