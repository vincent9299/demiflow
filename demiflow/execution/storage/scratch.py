"""Random-access kernel scratch charged to the original artifact authority.

The registered staging inode and held lease are identical to immutable artifact
staging. A killed worker leaves a journal-owned pending file that the existing
reconciler can remove; it never leaves an uncharged native temporary directory.
"""
import os

from demiflow.execution.contracts.dataflow import ArtifactCommand
from demiflow.execution.contracts.dataflow_errors import DataflowError
from demiflow.execution.storage.staging import StagingLease, staging_paths
from demiflow.execution.storage.objects import _sync_directory
from demiflow.execution.recovery_contract import identity, RecoveryContractError


class KernelScratch:
    def __init__(self, store, *, max_bytes, max_io_bytes):
        if type(max_bytes) is not int or max_bytes <= 0 or type(max_io_bytes) is not int or max_io_bytes <= 0:
            raise ValueError('Scratch limits must be positive integers')
        if store._readonly:
            raise DataflowError('FENCED', 'Historical artifact store is read-only')
        self.store = store
        self.maximum = min(max_bytes, store._bounds.max_spill_bytes)
        self.max_io_bytes = max_io_bytes
        self.size = self.reserved = 0
        self.fd = None
        self.lease = None
        self.begun = False
        self._file_slots = None
        self.operation = store._staging_operation()
        self.owner, self.path = staging_paths(store.root, store.store_id, self.operation)

    def __enter__(self):
        self._file_slots = self.store.file_slots(self.store.SCRATCH_FILES)
        self._file_slots.__enter__()
        try:
            return self._open()
        except BaseException:
            self._release_file_slots()
            raise

    def _release_file_slots(self):
        slots,self._file_slots = self._file_slots,None
        if slots is not None:
            slots.__exit__(None,None,None)

    def _open(self):
        self.lease = StagingLease.create(self.owner)
        try:
            self.reserved = min(65536, self.maximum)
            self.store._journal.artifact(ArtifactCommand(self.operation, 'begin_staging',
                None, self.store.store_id, self.reserved))
            self.begun = True
            self.fd = os.open(self.path, os.O_RDWR | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW | os.O_CLOEXEC, 0o600)
            return self
        except BaseException as exc:
            # A definitive rejection owns no reservation. An unknown reply
            # retains its locked-inode evidence for the original reconciler.
            if isinstance(exc, RecoveryContractError) and not self.begun:
                self.lease.remove()
            if self.begun:
                self.__exit__(type(exc), exc, exc.__traceback__)
            else:
                self.lease.close()
            raise

    def _check(self, offset, count):
        if self.fd is None:
            raise ValueError('Scratch file is closed')
        if type(offset) is not int or type(count) is not int or offset < 0 or count < 0:
            raise ValueError('Scratch offset and length must be nonnegative integers')
        if count > self.max_io_bytes:
            raise DataflowError('BUDGET_EXCEEDED', 'Scratch I/O exceeds the admitted resident window')

    def read_at(self, offset, count):
        self._check(offset, count)
        if offset + count > self.size:
            raise DataflowError('CORRUPT_ARTIFACT', 'Scratch reference exceeds its owned extent')
        raw = os.pread(self.fd, count, offset)
        if len(raw) != count:
            raise DataflowError('CORRUPT_ARTIFACT', 'Scratch reference is truncated')
        return raw

    def write_at(self, offset, raw):
        if type(raw) is not bytes:
            raise TypeError('Scratch writes require bytes')
        self._check(offset, len(raw))
        end = offset + len(raw)
        if offset > self.size:
            raise DataflowError('INVALID_CONTRACT', 'Scratch writes cannot create sparse holes')
        if end > self.maximum:
            raise DataflowError('BUDGET_EXCEEDED', 'Kernel scratch exceeds max_spill_bytes')
        if end > self.reserved:
            from demiflow.execution.storage.staging import grow_staging
            self.reserved = grow_staging(self.store._journal, self.operation,
                reserved=self.reserved, required=end, maximum=self.maximum,
                spill_budget=self.store._bounds.max_spill_bytes)
        view = memoryview(raw)
        written = 0
        while written < len(view):
            count = os.pwrite(self.fd, view[written:], offset + written)
            if count <= 0:
                raise OSError('Scratch write made no progress')
            written += count
        self.size = max(self.size, end)

    def append(self, raw):
        start = self.size
        self.write_at(start, raw)
        return start

    def close_local(self):
        """Close physical ownership after control shutdown; driver reconciles.

        No refund is claimed here. The original journal row and owner inode stay
        available for the driver's bounded lease-proven reconciliation.
        """
        try:
            if self.fd is not None:
                os.close(self.fd)
                self.fd = None
            self.path.unlink(missing_ok=True)
            _sync_directory(self.path.parent)
        finally:
            if self.lease is not None:
                self.lease.close()
                self.lease = None
            self._release_file_slots()

    def __exit__(self, kind, value, traceback):
        try:
            if self.fd is not None:
                os.close(self.fd)
                self.fd = None
            self.path.unlink(missing_ok=True)
            _sync_directory(self.path.parent)
            if self.begun:
                self.store._journal.artifact(ArtifactCommand(identity([self.operation, 'release']),
                    'release_staging', None, self.operation, 0))
                self.lease.remove()
        finally:
            if self.lease is not None:
                self.lease.close()
                self.lease = None
            self._release_file_slots()


class ScratchStream:
    """Lazy bounded file interface for worker finite/terminal buffers.

    Construction performs no control RPC (worker load may precede its service
    loop). Reads and writes use one admitted extent; close leaves reconciliation
    to the driver because the worker control mailbox may already be cancelled.
    """
    def __init__(self, store, *, max_bytes, max_io_bytes):
        self._store, self._maximum, self._io = store, max_bytes, max_io_bytes
        self._file = None
        self._position = 0
        self.closed = False

    def _check(self):
        if self.closed:
            raise ValueError('Scratch stream is closed')

    def tell(self):
        self._check()
        return self._position

    def seek(self, offset, whence=0):
        self._check()
        if type(offset) is not int or whence not in (0, 1, 2):
            raise ValueError('Invalid scratch seek')
        size = self._file.size if self._file is not None else 0
        position = offset + (self._position if whence == 1 else size if whence == 2 else 0)
        if not 0 <= position <= size:
            raise ValueError('Scratch seek cannot create holes')
        self._position = position
        return position

    def write(self, raw):
        self._check()
        if type(raw) is not bytes:
            raise TypeError('Scratch stream writes require bytes')
        if len(raw) > self._io or self._position + len(raw) > self._maximum:
            raise DataflowError('BUDGET_EXCEEDED', 'Scratch stream exceeds admitted extent or I/O window')
        if not raw:
            return 0
        if self._file is None:
            file = self._store.scratch(max_bytes=self._maximum, max_io_bytes=self._io)
            file.__enter__()
            self._file = file
        self._file.write_at(self._position, raw)
        self._position += len(raw)
        return len(raw)

    def read(self, count=-1):
        self._check()
        remaining = (self._file.size if self._file is not None else 0) - self._position
        count = remaining if count is None or count < 0 else min(count, remaining)
        if count > self._io:
            raise DataflowError('BUDGET_EXCEEDED', 'Scratch read exceeds admitted I/O window')
        if not count:
            return b''
        raw = self._file.read_at(self._position, count)
        self._position += count
        return raw

    def readline(self, limit=-1):
        self._check()
        limit = self._io if limit is None or limit < 0 else min(limit, self._io)
        parts, total = [], 0
        while total < limit:
            raw = self.read(min(65536, limit - total))
            if not raw:
                break
            end = raw.find(b'\n')
            if end >= 0:
                self._position -= len(raw) - end - 1
                parts.append(raw[:end + 1])
                break
            parts.append(raw)
            total += len(raw)
        return b''.join(parts)

    def flush(self):
        self._check()

    def close(self):
        if self.closed:
            return
        if self._file is not None:
            self._file.close_local()
        self.closed = True

