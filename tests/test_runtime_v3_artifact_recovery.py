"""Crash recovery uses native journal facts and an exited physical writer lease."""
from dataclasses import replace
import hashlib
from pathlib import Path
import subprocess
import sys

import pytest

from demiflow import data
from demiflow.execution.contracts.dataflow import ArtifactCommand, ArtifactRef
from demiflow.execution.contracts.dataflow_errors import DataflowError
from demiflow.execution.dataflow_api import make_action_request
from demiflow.execution.dataflow_authority import DriverArtifactPort
from demiflow.execution.dataflow_staging import StagingLease, recover_staging, staging_paths
from demiflow.execution.dataflow_store import RunArtifactStore
from demiflow.execution.recovery_contract import RecoveryContractError, identity
from demiflow.execution.run_context import RunContext
from demiflow.execution.run_journal import RunJournal
from demiflow.execution.recovery_contract import RunRef
from demiflow.planning.dataflow_policy import resolve_runtime_policy
from test_runtime_v3_artifact_accounting import store_fixture, consumed
from test_runtime_v3_real_seams import PreparationOnly


_CHILD = r'''
import os, sys
from pathlib import Path
from test_runtime_v3_artifact_recovery import _open, _store
root, fault = Path(sys.argv[1]), sys.argv[2]
import demiflow
assert Path(demiflow.__file__).resolve().parent.parent == Path.cwd().resolve()
with _open(root) as run:
    store, port, bounds = _store(run)
    artifact = port.artifact
    def crash_command(command):
        if fault == 'before_settle' and command.kind == 'settle_staging':
            os._exit(73)
        result = artifact(command)
        if (fault == 'after_begin' and command.kind == 'begin_staging'
                or fault == 'after_prepare' and command.kind == 'prepare_staging'):
            os._exit(73)
        return result
    port.artifact = crash_command
    if fault == 'after_publish':
        link = os.link
        def crash_link(*args, **kwargs):
            link(*args, **kwargs)
            os._exit(73)
        os.link = crash_link
        import demiflow.execution.dataflow_store as store_module
        sync = store_module._sync_directory
        def crash_deduplicated_publish(path):
            sync(path)
            if path.name == 'objects':
                os._exit(73)
        store_module._sync_directory = crash_deduplicated_publish
    def chunks():
        yield b'crash-safe-content'
        if fault == 'during_write':
            os._exit(73)
    store.put(chunks(), max_bytes=1024, media_type='application/octet-stream')
    raise AssertionError('crash seam was not reached')
'''


def _open(directory):
    journal = RunJournal(directory)
    # No filesystem-presence inference grants recovery: use the native identity.
    binding = journal.recovery_identity() if directory.exists() else None
    return (RunContext.resume(RunRef(str(directory), binding['run_id'])) if binding
            else RunContext.start(directory))


def _store(run, *, ceiling=2048):
    dataset = data.from_items([])
    request = make_action_request(dataset._executor, dataset._source, dataset._plan,
                                  kind='take', arguments={'limit': 1})
    policy, bounds = resolve_runtime_policy({}, run.limits, provider='local')
    bounds = replace(bounds, max_spill_bytes=ceiling)
    port = DriverArtifactPort(request.action, bounds=bounds)
    return (RunArtifactStore(root=run.directory / '.dataflow-store', store_id=policy.store_id,
                             journal=port, bounds=bounds), port, bounds)


def _crash(directory, fault):
    result = subprocess.run([sys.executable, '-B', '-c', _CHILD, str(directory), fault],
                            capture_output=True, text=True, timeout=45)
    assert result.returncode == 73, result.stdout + result.stderr


@pytest.mark.parametrize('fault,published', [
    ('after_begin', False), ('during_write', False), ('after_prepare', False),
    ('after_publish', True), ('before_settle', True),
])
def test_real_driver_crash_resume_does_not_accumulate_reservations(tmp_path, fault, published):
    directory = tmp_path / 'run'
    for _ in range(4):
        _crash(directory, fault)
        with pytest.raises(PreparationOnly):
            with _open(directory) as run:
                store, port, bounds = _store(run)
                assert port.recovery['reconciled'] == 1
                assert consumed(run) == (len(b'crash-safe-content') if published else 0)
                assert recover_staging(port._action, bounds=bounds)['reconciled'] == 0
                assert not list((store.root / store.store_id / 'staging').iterdir())
                if published:
                    digest = hashlib.sha256(b'crash-safe-content').hexdigest()
                    assert store.inspect(ArtifactRef(digest, len(b'crash-safe-content'),
                        store.store_id, digest, 'application/octet-stream'))
                raise PreparationOnly()
    with pytest.raises(PreparationOnly):
        with _open(directory) as run:
            store, port, _ = _store(run)
            ref = store.put(iter([b'crash-safe-content']), max_bytes=1024, media_type='application/octet-stream')
            assert consumed(run) == ref.size_bytes
            assert b''.join(store.read(ref, max_bytes=1024)) == b'crash-safe-content'
            raise PreparationOnly()


def _begin(store, port, reservation):
    owner, pending = staging_paths(store.root, store.store_id, reservation)
    lease = StagingLease.create(owner)
    port.artifact(ArtifactCommand(reservation, 'begin_staging', None, store.store_id, 1024))
    return lease, pending


def test_live_writer_and_open_fd_survive_new_driver_fence(tmp_path):
    # Independent worker retains its lease and open data FD after the old driver
    # scope releases runtime.lock. New attempt/fence is not death evidence.
    directory, worker = tmp_path / 'run', None
    try:
        with pytest.raises(PreparationOnly):
            with _open(directory) as run:
                store, port, bounds = _store(run)
                reservation = identity('live-child-writer')
                owner, pending = staging_paths(store.root, store.store_id, reservation)
                worker = subprocess.Popen([sys.executable, '-B', '-u', '-c', r'''
import sys
from pathlib import Path
from demiflow.execution.dataflow_staging import StagingLease
lease = StagingLease.create(Path(sys.argv[1]))
stream = open(sys.argv[2], 'wb')
stream.write(b'live'); stream.flush()
print('locked', flush=True)
sys.stdin.readline()
stream.write(b'-finished'); stream.flush(); stream.close()
lease.close()
''', str(owner), str(pending)], stdin=subprocess.PIPE, stdout=subprocess.PIPE, text=True)
                assert worker.stdout.readline().strip() == 'locked'
                port.artifact(ArtifactCommand(reservation, 'begin_staging', None, store.store_id, 1024))
                old_fence = run.fence
                raise PreparationOnly()
        with pytest.raises(PreparationOnly):
            with _open(directory) as run:
                store, port, bounds = _store(run)
                assert run.fence != old_fence
                assert port.recovery['unknown'] == 1 and consumed(run) == 1024
                assert pending.read_bytes() == b'live'
                worker.communicate('\n', timeout=15)
                assert worker.returncode == 0 and pending.read_bytes() == b'live-finished'
                assert recover_staging(port._action, bounds=bounds)['reconciled'] == 1
                assert consumed(run) == 0 and not pending.exists()
                raise PreparationOnly()
    finally:
        if worker is not None and worker.poll() is None:
            worker.kill()
            worker.communicate(timeout=15)


def test_changed_or_missing_lock_cannot_prove_owner_exit(tmp_path):
    with store_fixture(tmp_path, ceiling=4096) as (run, store, port):
        reservation = identity('replaced-owner')
        lease, pending = _begin(store, port, reservation)
        pending.write_bytes(b'owned')
        lease.path.unlink()
        replacement = StagingLease.create(lease.path)
        replacement.close()
        assert recover_staging(port._action, bounds=port._bounds)['unknown'] == 1
        assert pending.read_bytes() == b'owned' and consumed(run) == 1024
        lease.path.unlink()
        lease.close()
        assert recover_staging(port._action, bounds=port._bounds)['unknown'] == 1
        assert pending.exists() and consumed(run) == 1024


def test_historical_pins_and_published_bytes_are_preserved(tmp_path, monkeypatch):
    with store_fixture(tmp_path, ceiling=4096) as (run, store, port):
        old = store.put(iter([b'historical']), max_bytes=1024, media_type='application/octet-stream')
        store.retain(old, owner='committed-history')
        artifact = port.artifact
        def unknown(command):
            if command.kind == 'settle_staging':
                raise RuntimeError('lost reply')
            return artifact(command)
        with monkeypatch.context() as patch:
            patch.setattr(port, 'artifact', unknown)
            with pytest.raises(RuntimeError, match='lost reply'):
                store.put(iter([b'historical']), max_bytes=1024, media_type='application/octet-stream')
        assert consumed(run) == 1024 + old.size_bytes
        assert recover_staging(port._action, bounds=port._bounds)['reconciled'] == 1
        assert consumed(run) == old.size_bytes
        assert store.inspect(old)
        assert run.journal.artifact_owners(old, limit=10)[0] == ('committed-history',)


def test_unregistered_legacy_reservation_and_corrupt_publication_are_not_refunded(tmp_path):
    with store_fixture(tmp_path, ceiling=4096) as (run, store, port):
        port.reserve(identity('legacy-without-proof'), {'artifact_staging_bytes': 1024})
        reservation = identity('corrupt-publication')
        lease, pending = _begin(store, port, reservation)
        payload = b'correct'
        digest = hashlib.sha256(payload).hexdigest()
        ref = ArtifactRef(digest, len(payload), store.store_id, digest, 'application/octet-stream')
        port.artifact(ArtifactCommand(identity('prepare-corrupt'), 'prepare_staging', ref, reservation, len(payload)))
        (store.root / store.store_id / 'objects' / digest).write_bytes(b'changed')
        lease.close()
        with pytest.raises(DataflowError, match='reference'):
            recover_staging(port._action, bounds=port._bounds)
        assert consumed(run) == 2048
        assert (store.root / store.store_id / 'objects' / digest).read_bytes() == b'changed'


def test_begin_requires_locked_original_inode_and_intent_is_immutable(tmp_path):
    with store_fixture(tmp_path, ceiling=4096) as (run, store, port):
        reservation = identity('protocol')
        owner, pending = staging_paths(store.root, store.store_id, reservation)
        owner.touch()
        with pytest.raises(DataflowError, match='held writer lease'):
            port.artifact(ArtifactCommand(reservation, 'begin_staging', None, store.store_id, 1024))
        owner.unlink()
        lease, _ = _begin(store, port, reservation)
        try:
            command = ArtifactCommand(reservation, 'begin_staging', None, store.store_id, 1024)
            port.artifact(command)
            assert consumed(run) == 1024
            with pytest.raises(RecoveryContractError, match='ownership changed'):
                port.artifact(replace(command, size_bytes=512))
            digest = hashlib.sha256(b'one').hexdigest()
            ref = ArtifactRef(digest, 3, store.store_id, digest, 'application/octet-stream')
            prepare = ArtifactCommand(identity('prepare-protocol'), 'prepare_staging', ref, reservation, 3)
            port.artifact(prepare)
            port.artifact(prepare)
            with pytest.raises(RecoveryContractError, match='publication changed'):
                port.artifact(replace(prepare, ref=replace(ref, media_type='text/plain')))
        finally:
            lease.close()


def test_bounded_scan_advances_past_live_owners_without_directory_inference(tmp_path):
    with store_fixture(tmp_path, ceiling=4096) as (run, store, port):
        first, second = '1' * 64, '2' * 64
        live, pending_live = _begin(store, port, first)
        dead, pending_dead = _begin(store, port, second)
        pending_live.write_bytes(b'live')
        pending_dead.write_bytes(b'orphan')
        foreign = pending_dead.parent / '.pending-not-registered-data'
        foreign.write_bytes(b'foreign')
        dead.close()
        try:
            assert recover_staging(port._action, bounds=port._bounds, max_items=1)['unknown'] == 1
            second_page = recover_staging(port._action, bounds=port._bounds, max_items=1)
            assert second_page['scanned'] == second_page['reconciled'] == 1
            assert consumed(run) == 1024 and pending_live.exists()
            assert not pending_dead.exists() and foreign.read_bytes() == b'foreign'
        finally:
            live.close()


def test_live_temporary_or_published_object_cannot_be_refunded_by_release(tmp_path):
    with store_fixture(tmp_path, ceiling=4096) as (run, store, port):
        reservation = identity('no-premature-refund')
        lease, pending = _begin(store, port, reservation)
        pending.write_bytes(b'content')
        digest = hashlib.sha256(b'content').hexdigest()
        ref = ArtifactRef(digest, 7, store.store_id, digest, 'application/octet-stream')
        prepare = ArtifactCommand(identity('prepare-held'), 'prepare_staging', ref, reservation, 7)
        release = ArtifactCommand(identity('release-held'), 'release_staging', None, reservation, 0)
        settle = ArtifactCommand(identity('settle-held'), 'settle_staging', ref, reservation, 7)
        try:
            port.artifact(prepare)
            (store.root / store.store_id / 'objects' / digest).write_bytes(b'content')
            with pytest.raises(DataflowError, match='deletion'):
                port.artifact(settle)
            pending.unlink()
            with pytest.raises(DataflowError, match='Published staging'):
                port.artifact(release)
            assert consumed(run) == 1024
            port.artifact(settle)
            assert consumed(run) == 7
        finally:
            lease.close()


def test_definite_initial_quota_rejection_does_not_leak_empty_owner_files(tmp_path):
    with store_fixture(tmp_path, ceiling=2048) as (run, store, port):
        ref = store.put(iter([b'x' * 1500]), max_bytes=2048, media_type='application/octet-stream')
        for _ in range(3):
            with pytest.raises(RecoveryContractError, match='budget exhausted'):
                store.put(iter([b'new']), max_bytes=1024, media_type='application/octet-stream')
        assert consumed(run) == 1500 and store.inspect(ref)
        assert not list((store.root / store.store_id / 'staging').iterdir())


def test_sealed_store_limits_apply_to_other_views_and_the_original_reservation(tmp_path):
    with store_fixture(tmp_path, ceiling=4096) as (run, store, port):
        _, bounds = resolve_runtime_policy({}, run.limits, provider='local')
        view = RunArtifactStore(root=store.root, store_id=store.store_id, journal=None,
                                bounds=bounds, readonly=True)
        try:
            effective = replace(bounds, max_open_files=6, max_spill_bytes=32)
            used = store.open_file_slots
            store.bind_driver_bounds(effective)
            assert view.available_file_slots() == 6 - used
            with pytest.raises(RecoveryContractError):
                port.reserve('over-sealed-budget', {'artifact_staging_bytes': 33})
            ref = store.put(iter([b'original']), max_bytes=8, media_type='application/octet-stream')
            assert b''.join(view.read(ref, max_bytes=8)) == b'original'
        finally:
            view.close()
            store.close()
