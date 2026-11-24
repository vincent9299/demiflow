"""Physical staging leases; durable ownership lives exclusively in RunJournal.

A writer locks before registering its inode and holds the lock until every data
FD is closed and settlement finishes. Recovery never infers writer death from a
fence, PID or timestamp. Missing/replaced/held leases are unknown, not refunds.
The shared artifact filesystem must support coherent POSIX flock/hardlink/fsync.
"""
from __future__ import annotations

from typing import Mapping
from demiflow.execution.contracts.dataflow import ArtifactReply

from demiflow.execution.contracts.dataflow import ArtifactCommand
from demiflow.execution.contracts.dataflow_errors import DataflowError
from demiflow.execution.contracts.recovery_contract import identity
from demiflow.execution.contracts.errors import ExecutionBudgetExceeded

import fcntl
import os
import stat
from pathlib import Path


from demiflow.execution.contracts.recovery_contract import identifier


def staging_paths(root, store_id, reservation_id):
    from demiflow.execution.storage.objects import _no_symlink
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
        from demiflow.execution.storage.objects import _sync_directory
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
        from demiflow.execution.storage.objects import _sync_directory
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
    from demiflow.execution.contracts.dataflow import ArtifactRef
    from demiflow.execution.storage.objects import RunArtifactStore, _no_symlink, _sync_directory
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


def grow_staging(journal, operation, *, reserved, required, maximum, spill_budget=None):
    # Disk reservation only: this does not allocate a resident byte buffer.
    # Start small and grow geometrically. Large disk grants amortize control
    # round-trips in at most 8 MiB steps, also capped at 1/64 of the task grant
    # (with the original 1 MiB step retained for small grants).
    # Every unused byte is charged to the SAME original journal until settlement.
    step = 1 << 20 if spill_budget is None else max(1 << 20, min(8 << 20, spill_budget // 64))
    extent = min(maximum, max(required, reserved + min(max(reserved, 65536), step)))

    def admit(size):
        reply = journal.artifact(ArtifactCommand(identity([operation, 'grow', size]),
            'grow_staging', None, operation, size))
        if type(reply.charged_bytes) is not int or not size <= reply.charged_bytes <= maximum:
            raise DataflowError('INVALID_CONTRACT', 'staging growth reply differs from its byte grant')
        return reply.charged_bytes

    try:
        return admit(extent)
    except ExecutionBudgetExceeded:
        if extent == required:
            raise
        # Only an atomic, definite budget rejection can take this path. The
        # failed transaction changed no charge. A smaller exact request avoids
        # denying a write merely because optional growth headroom did not fit.
        # Unknown/transport/fence failures propagate; they are never retried.
        return admit(required)


_ARTIFACT_KINDS = frozenset({"pin", "unpin", "inspect", "reserve_staging", "begin_staging", "prepare_staging", "grow_staging", "settle_staging", "release_staging"})

def apply_artifact_command(action: "RunAction", command: ArtifactCommand, *, bounds: "Bounds") -> ArtifactReply:
    """Single pin/unpin/inspect/reserve_staging transaction shared by the driver
    port and the verified worker service (H074)."""
    from demiflow.execution.storage.objects import ARTIFACT_STAGING_BUDGET
    if command.kind not in _ARTIFACT_KINDS:
        raise DataflowError("INVALID_CONTRACT", "unknown artifact command kind")
    identifier(command.operation_id)
    identifier(command.owner)
    journal = action.context.journal
    fence = action.context.fence
    if command.kind == 'begin_staging':
        # operation_id is the reservation; owner is its store_id. The physical
        # lease must already be held before either ownership or quota is recorded.
        if command.ref is not None or command.size_bytes <= 0:
            raise DataflowError('INVALID_CONTRACT', 'Begin requires a positive size without a ref')

        owner, _ = staging_paths(action.context.directory / '.dataflow-store', command.owner, command.operation_id)
        inode = inspect_held_owner(owner)
        charged = journal.begin_dataflow_staging(fence, command.operation_id, command.owner,
            inode, command.size_bytes, ceiling=bounds.max_spill_bytes)
        return ArtifactReply(command.operation_id, None, charged)
    if command.kind == 'prepare_staging':
        # owner is the original reservation; ref is the exact upcoming object.
        if command.ref is None or command.size_bytes != command.ref.size_bytes:
            raise DataflowError('INVALID_CONTRACT', 'Prepare requires its exact publication ref')
        journal.prepare_dataflow_staging(fence, command.owner, command.ref)
        return ArtifactReply(command.operation_id, None, command.size_bytes)
    if command.kind == 'grow_staging':
        if command.ref is not None or command.size_bytes <= 0:
            raise DataflowError('INVALID_CONTRACT', 'Growth requires a positive staging size without a ref')
        charged = journal.grow_dataflow_staging(fence, command.owner, command.size_bytes,
                                                ceiling=bounds.max_spill_bytes)
        return ArtifactReply(operation_id=command.operation_id, present=None, charged_bytes=charged)
    if command.kind in {'settle_staging', 'release_staging'}:
        ref = command.ref
        owned = journal.dataflow_staging_owner(fence, command.owner)
        if owned is not None:

            from demiflow.execution.storage.objects import _no_symlink
            _, pending = staging_paths(action.context.directory / '.dataflow-store', owned[0], command.owner)
            _no_symlink(pending)
            if pending.exists():
                raise DataflowError('INVALID_CONTRACT', 'Settlement requires deletion of owned staging files')
            if command.kind == 'release_staging' and owned[3] is not None and not owned[4]:
                from demiflow.execution.contracts.dataflow import ArtifactRef
                from demiflow.execution.storage.objects import RunArtifactStore
                proposed = ArtifactRef(**owned[3])
                store = RunArtifactStore(root=action.context.directory / '.dataflow-store',
                    store_id=proposed.store_id, journal=None, bounds=bounds, readonly=True)
                if store.inspect(proposed):
                    raise DataflowError('INVALID_CONTRACT', 'Published staging requires artifact settlement')
        if command.kind == 'release_staging' and ref is not None:
            raise DataflowError('INVALID_CONTRACT', 'Release cannot carry an artifact')
        if command.kind == 'release_staging':
            root = action.context.directory / '.dataflow-store'
            if command.size_bytes != 0 or (owned is None and next(root.glob('*/staging/.pending-' + command.owner + '-*'), None) is not None):
                raise DataflowError('INVALID_CONTRACT', 'Release requires deletion of owned staging files')
        if command.kind == 'settle_staging':
            if ref is None:
                raise DataflowError('INVALID_CONTRACT', 'Settlement requires a durable artifact')
            if command.size_bytes != ref.size_bytes:
                raise DataflowError('INVALID_CONTRACT', 'Settlement size differs from its artifact')
            from demiflow.execution.storage.objects import RunArtifactStore
            store = RunArtifactStore(root=action.context.directory / '.dataflow-store',
                store_id=ref.store_id, journal=None, bounds=bounds, readonly=True)
            if not store.inspect(ref):
                raise DataflowError('CORRUPT_ARTIFACT', 'Settlement object is not durable')
        charged = journal.settle_dataflow_staging(fence, command.owner, ref)
        return ArtifactReply(operation_id=command.operation_id, present=ref is not None, charged_bytes=charged)
    if command.kind == "reserve_staging":
        if type(command.size_bytes) is not int or command.size_bytes <= 0:
            raise DataflowError("INVALID_CONTRACT", "reserve_staging needs positive size_bytes")
        if command.size_bytes > bounds.max_spill_bytes:
            raise DataflowError("BUDGET_EXCEEDED", "staging reservation exceeds ceiling")
        journal.reserve_budgets(fence, command.operation_id,
                                {ARTIFACT_STAGING_BUDGET: (command.size_bytes, bounds.max_spill_bytes)})
        return ArtifactReply(operation_id=command.operation_id, present=None, charged_bytes=command.size_bytes)
    if command.ref is None:
        raise DataflowError("INVALID_CONTRACT", "artifact command needs a ref")
    ref = command.ref
    if command.kind == "pin":
        journal.bind_artifact_owner(fence, ref, owner=command.owner, operation_id=command.operation_id)
        return ArtifactReply(operation_id=command.operation_id, present=True, charged_bytes=0)
    if command.kind == "unpin":
        journal.release_artifact_owner(fence, ref, owner=command.owner, operation_id=command.operation_id)
        return ArtifactReply(operation_id=command.operation_id, present=None, charged_bytes=0)
    owners, _ = journal.artifact_owners(ref, limit=1)
    return ArtifactReply(operation_id=command.operation_id, present=bool(owners), charged_bytes=0)

class DriverArtifactPort:
    """Narrow driver artifact port bound to the original action journal (H071-H073).

    Constructed before the store; it never depends on the store or service.
    """

    def __init__(self, action: "RunAction", *, bounds: "Bounds"):
        self._action = action
        self._bounds = bounds
        if action.context.journal.pending_dataflow_gc():
            from demiflow.execution.storage.gc import recover_published_gc
            result = recover_published_gc(action.context.journal, action.context.fence)
            if result['blocked']:
                raise DataflowError('UNKNOWN_EFFECT', result['blocked'])

        self.recovery = recover_staging(action, bounds=bounds)

    def bind_bounds(self, bounds: "Bounds") -> None:
        self._bounds = bounds

    def bind_store_lease(self, inode, *, fresh):
        return self._action.context.journal.bind_dataflow_gc_protocol(
            self._action.context.fence, inode, fresh=fresh)

    def store_protocol(self):
        return self._action.context.journal.dataflow_gc_protocol()

    def reserve(self, operation_id: str, amounts: Mapping[str, int]) -> None:
        from demiflow.execution.storage.objects import ARTIFACT_STAGING_BUDGET
        identifier(operation_id)
        if type(amounts) is not dict or set(amounts) != {ARTIFACT_STAGING_BUDGET}:
            raise DataflowError("INVALID_CONTRACT", "driver port only admits artifact staging budget")
        amount = amounts[ARTIFACT_STAGING_BUDGET]
        if type(amount) is not int or amount <= 0:
            raise DataflowError("INVALID_CONTRACT", "staging amount must be a positive int")
        self._action.context.journal.reserve_budgets(
            self._action.context.fence, operation_id,
            {ARTIFACT_STAGING_BUDGET: (amount, self._bounds.max_spill_bytes)})

    def artifact(self, command: ArtifactCommand) -> ArtifactReply:
        return apply_artifact_command(self._action, command, bounds=self._bounds)

    def release_materialized(self, ref, *, owner):
        # This method is deliberately absent from the worker RPC vocabulary.
        # Only a driver-created handle retains this port and its random owner.
        import hashlib
        from demiflow.execution.storage.objects import _pin_identity
        prefix = self._action.action_id + '-'
        suffix = owner[len(prefix):] if isinstance(owner, str) and owner.startswith(prefix) else ''
        if len(suffix) != 32 or any(char not in '0123456789abcdef' for char in suffix):
            raise DataflowError('FENCED', 'Only this action\'s materialized handle may release its pin')
        operation = hashlib.sha256(('pin:' + _pin_identity(ref, owner)).encode()).hexdigest()
        self._action.context.journal.release_materialized_owner(ref, owner=owner, pin_operation_id=operation)
