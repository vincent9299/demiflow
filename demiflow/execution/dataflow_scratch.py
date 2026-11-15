"""Random-access kernel scratch charged to the original artifact authority.

The registered staging inode and held lease are identical to immutable artifact
staging. A killed worker leaves a journal-owned pending file that the existing
reconciler can remove; it never leaves an uncharged native temporary directory.
"""
import os

from .contracts.dataflow import ArtifactCommand
from .contracts.dataflow_errors import DataflowError
from .dataflow_staging import StagingLease, staging_paths
from .dataflow_store import _sync_directory
from .recovery_contract import identity, RecoveryContractError


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
        self.operation = store._staging_operation()
        self.owner, self.path = staging_paths(store.root, store.store_id, self.operation)

    def __enter__(self):
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
            extent = min(self.maximum, ((end + 65535) // 65536) * 65536)
            self.store._journal.artifact(ArtifactCommand(identity([self.operation, 'grow', extent]),
                'grow_staging', None, self.operation, extent))
            self.reserved = extent
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

