"""Physical staging leases; durable ownership lives exclusively in RunJournal.

A writer locks before registering its inode and holds the lock until every data
FD is closed and settlement finishes. Recovery never infers writer death from a
fence, PID or timestamp. Missing/replaced/held leases are unknown, not refunds.
The shared artifact filesystem must support coherent POSIX flock/hardlink/fsync.
"""
from __future__ import annotations

import fcntl
import os
import stat
from pathlib import Path

from .contracts.dataflow_errors import DataflowError
from .recovery_contract import identifier


def staging_paths(root, store_id, reservation_id):
    from .dataflow_store import _no_symlink
    identifier(store_id)
    identifier(reservation_id)
    directory = Path(root) / store_id / 'staging'
    _no_symlink(directory)
    return directory / ('.owner-' + reservation_id), directory / ('.pending-' + reservation_id + '-data')


def _same_inode(path, fd):
    current, held = path.lstat(), os.fstat(fd)
    if (not stat.S_ISREG(current.st_mode) or current.st_nlink != 1
            or (current.st_dev, current.st_ino) != (held.st_dev, held.st_ino)
            or held.st_size != 0):
        raise DataflowError('INVALID_CONTRACT', 'Staging owner lease was replaced or malformed')
    return held.st_dev, held.st_ino


class StagingLease:
    def __init__(self, path, fd):
        self.path, self.fd = path, fd
        self.inode = _same_inode(path, fd)

    @classmethod
    def create(cls, path):
        from .dataflow_store import _sync_directory
        fd = os.open(path, os.O_RDWR | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW | os.O_CLOEXEC, 0o600)
        try:
            fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
            lease = cls(path, fd)
            _sync_directory(path.parent)
            return lease
        except BaseException:
            os.close(fd)
            raise

    @classmethod
    def claim(cls, path, inode):
        """None means the original owner is alive or its exit is unprovable."""
        try:
            fd = os.open(path, os.O_RDWR | os.O_NOFOLLOW | os.O_CLOEXEC)
        except OSError:
            return None
        try:
            fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
            lease = cls(path, fd)
            if lease.inode != inode:
                os.close(fd)
                return None
            return lease
        except (OSError, DataflowError):
            os.close(fd)
            return None

    def remove(self):
        from .dataflow_store import _sync_directory
        _same_inode(self.path, self.fd)
        self.path.unlink()
        _sync_directory(self.path.parent)

    def close(self):
        os.close(self.fd)


def inspect_held_owner(path):
    """Bind begin to the already locked original inode, never a future lock."""
    fd = os.open(path, os.O_RDWR | os.O_NOFOLLOW | os.O_CLOEXEC)
    try:
        inode = _same_inode(path, fd)
        try:
            fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            _same_inode(path, fd)
            return inode
        raise DataflowError('INVALID_CONTRACT', 'Staging registration requires a held writer lease')
    finally:
        os.close(fd)


def recover_staging(action, *, bounds, max_items=1024):
    """Bounded journal-driven reconciliation; immutable objects are preserved.

    Unpublished files are removed before refund. Published bytes are verified and
    uniquely charged, including objects with historical/committed references.
    Legacy reservations lacking ownership evidence remain conservatively charged.
    """
    from .contracts.dataflow import ArtifactRef
    from .dataflow_store import RunArtifactStore, _no_symlink, _sync_directory
    if type(max_items) is not int or not 0 < max_items <= 4096:
        raise ValueError('Recovery item limit must be in 1..4096')
    journal, fence = action.context.journal, action.context.fence
    root = action.context.directory / '.dataflow-store'
    cursor = journal.dataflow_recovery_cursor(fence)
    scanned, reclaimed, unknown, inspected_bytes = 0, 0, 0, 0
    wrapped = not bool(cursor)
    while scanned < max_items:
        rows = journal.pending_dataflow_staging(fence, after=cursor, limit=min(128, max_items - scanned))
        if not rows:
            cursor = ''
            if not wrapped and scanned < max_items:
                wrapped = True
                continue
            break
        for row in rows:
            reservation, store_id, device, inode, publication = row
            cursor, scanned = reservation, scanned + 1
            try:
                owner, pending = staging_paths(root, store_id, reservation)
            except (OSError, DataflowError):
                unknown += 1
                continue
            lease = StagingLease.claim(owner, (device, inode))
            if lease is None:
                unknown += 1
                continue
            try:
                ref = ArtifactRef(**publication) if publication else None
                if ref is not None:
                    if inspected_bytes + ref.size_bytes > bounds.max_spill_bytes:
                        unknown += 1
                        continue
                    store = RunArtifactStore(root=root, store_id=store_id, journal=None, bounds=bounds, readonly=True)
                    inspected_bytes += ref.size_bytes
                    if not store.inspect(ref):
                        ref = None
                # Only the registered reservation's fixed path is eligible.
                # No glob, directory-age scan, or immutable object deletion.
                _no_symlink(pending)
                pending.unlink(missing_ok=True)
                _sync_directory(pending.parent)
                journal.settle_dataflow_staging(fence, reservation, ref)
                lease.remove()
                reclaimed += 1
            finally:
                lease.close()
    journal.dataflow_recovery_cursor(fence, value=cursor if scanned == max_items else '')
    return {'scanned': scanned, 'reconciled': reclaimed, 'unknown': unknown,
            'inspected_bytes': inspected_bytes, 'limit_reached': scanned == max_items}
