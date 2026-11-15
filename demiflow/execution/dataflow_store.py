"""Content-addressed immutable artifact store (H030-H035, C10).

The store owns only immutable data files under an authenticated shared root.
Pins live in the original RunJournal through the injected ArtifactAuthorityPort;
this module never opens a database and never creates a parallel authority. The
persistent key is the content digest, never an absolute path.
"""
from __future__ import annotations

import hashlib
import os
import secrets
import stat
import threading
import weakref
from contextlib import contextmanager
from pathlib import Path
from typing import TYPE_CHECKING, Iterator

from .contracts.dataflow import ArtifactRef, ArtifactCommand
from .contracts.dataflow_errors import DataflowError
from .recovery_contract import identifier, identity, RecoveryContractError

if TYPE_CHECKING:
    from .contracts.dataflow import ArtifactAuthorityPort, Bounds

ARTIFACT_STAGING_BUDGET = "artifact_staging_bytes"
_MAX_CHUNK_BYTES = 1 << 20
_OBJECTS = "objects"
_STAGING = "staging"


class _OpenFiles:
    """Live handles of this physical store, shared by its local view objects."""
    def __init__(self):
        self.lock = threading.Lock()
        self.used = 0
        self.limits = {}


_FILE_VIEWS = weakref.WeakValueDictionary()
_FILE_VIEWS_LOCK = threading.Lock()


def _open_files(root):
    # Weak values expire after all views and outstanding leases are gone; no
    # run registry, durable reservation or second resource allocator is created.
    key = (os.getpid(), str(Path(root).resolve()))
    with _FILE_VIEWS_LOCK:
        state = _FILE_VIEWS.get(key)
        if state is None:
            state = _OpenFiles()
            _FILE_VIEWS[key] = state
        return state


def _no_symlink(path: Path) -> None:
    for item in (path, *path.parents):
        if item.is_symlink():
            raise DataflowError("INVALID_CONTRACT", "artifact root contains a symlink: " + str(item))


def _create_staging(path):
    return os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW | os.O_CLOEXEC, 0o600)


def _sync_directory(path):
    descriptor = os.open(path, os.O_RDONLY | os.O_DIRECTORY)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


class RunArtifactStore:
    def __init__(self, *, root: Path, store_id: str, journal: "ArtifactAuthorityPort", bounds: "Bounds", readonly: bool = False):
        self._bounds = bounds
        self._file_state = _open_files(root)
        self._writer_slot = False
        self._writer_lease = None
        self._writer_closed = False
        # Initial writer lease plus header/directory fsync; readonly bootstrap
        # only authenticates the header. Transfer the surviving writer slot.
        initial_slots = 2 if not readonly and journal is not None else 1
        initial_limit = self._acquire_files(initial_slots)
        try:
            self._initialize(root=root, store_id=store_id, journal=journal,
                             bounds=bounds, readonly=readonly)
        except BaseException:
            _release_files(self._file_state,initial_slots,initial_limit)
            raise
        self._writer_slot = self._writer_lease is not None
        self._writer_limit = initial_limit
        _release_files(self._file_state,initial_slots - int(self._writer_slot),initial_limit,final=not self._writer_slot)

    def _initialize(self, *, root, store_id, journal, bounds, readonly):
        identifier(store_id)
        root = Path(root)
        if not root.is_absolute():
            raise DataflowError("INVALID_CONTRACT", "artifact store root must be absolute")
        _no_symlink(root)
        fresh = not root.exists()
        if not readonly:
            root.mkdir(parents=True, exist_ok=True)
        if not root.is_dir():
            raise DataflowError("INVALID_CONTRACT", "artifact store root is not a directory")
        _no_symlink(root)
        self._root = root
        self._readonly = readonly
        self.store_id = store_id
        self._journal = journal
        self._bounds = bounds
        self._writer_lease = None
        self._writer_closed = False
        from .dataflow_object_lease import protocol_identity
        self._lease_identity = protocol_identity(root)
        self._protocol_required = self._lease_identity is not None
        original_protocol = getattr(journal, 'store_protocol', lambda: None)()
        if original_protocol is not None:
            original = tuple(original_protocol[:2])
            if original_protocol[2] and self._lease_identity != original:
                raise DataflowError('FENCED', 'Published object header differs from original journal')
            self._lease_identity = original
            self._protocol_required = bool(original_protocol[2])
        if not readonly and journal is not None:
            from .dataflow_object_lease import ObjectLease, held_exclusive, record_protocol
            bind = getattr(journal, 'bind_store_lease', None)
            if bind is not None:
                import fcntl
                created = False
                if self._lease_identity is not None:
                    self._writer_lease = ObjectLease(root, expected=self._lease_identity)
                else:
                    try:
                        self._writer_lease = ObjectLease(root, create=True, exclusive=True)
                        created = True
                    except FileExistsError:
                        self._writer_lease = ObjectLease(root)
                try:
                    if created:
                        with held_exclusive(self._writer_lease):
                            if fresh:
                                record_protocol(root, self._writer_lease.identity)
                            eligible = bind(self._writer_lease.identity, fresh=fresh)
                        fcntl.flock(self._writer_lease.fd, fcntl.LOCK_SH)
                    else:
                        eligible = bind(self._writer_lease.identity, fresh=fresh)
                    if eligible and protocol_identity(root) != self._writer_lease.identity:
                        raise DataflowError('FENCED', 'Registered store protocol header is missing or changed')
                    self._protocol_required = eligible or self._protocol_required
                except BaseException:
                    self.close()
                    raise
            else:
                try:
                    self._writer_lease = ObjectLease(root, expected=self._lease_identity)
                except FileNotFoundError:
                    if self._lease_identity is not None:
                        raise DataflowError('FENCED', 'Registered published object lease is missing')
                    pass  # Historical/non-native test stores are not GC eligible.
            if self._writer_lease is not None:
                if (self._lease_identity is not None and
                        self._lease_identity != self._writer_lease.identity):
                    self.close()
                    raise DataflowError('FENCED', 'Published object lease identity changed')
                self._lease_identity = self._writer_lease.identity
        self._objects = root / store_id / _OBJECTS
        self._staging = root / store_id / _STAGING
        try:
            if not readonly:
                self._objects.mkdir(parents=True, exist_ok=True)
                self._staging.mkdir(parents=True, exist_ok=True)
            _no_symlink(self._objects)
            _no_symlink(self._staging)
        except BaseException:
            self.close()
            raise

    # Conservative simultaneous artifact FDs, including physical lease and
    # native-journal metadata access while staging is open. Not process RLIMIT:
    # resource-provider sockets/SQLite connections retain their original owner.
    READ_FILES = 2
    PUT_FILES = 5
    SCRATCH_FILES = 3

    @property
    def open_file_slots(self):
        with self._file_state.lock:
            return self._file_state.used

    def require_file_slots(self, count, *, maximum=None):
        maximum = self._bounds.max_open_files if maximum is None else min(maximum,self._bounds.max_open_files)
        with self._file_state.lock:
            maximum = min(maximum,*self._file_state.limits) if self._file_state.limits else maximum
            if self._file_state.used + count > maximum:
                raise DataflowError('BUDGET_EXCEEDED', 'Artifact open file budget is insufficient')

    def _acquire_files(self, count):
        with self._file_state.lock:
            limit = self._bounds.max_open_files
            maximum = min(limit,*self._file_state.limits) if self._file_state.limits else limit
            if self._file_state.used + count > maximum:
                raise DataflowError('BUDGET_EXCEEDED', 'Artifact open file budget is insufficient')
            self._file_state.used += count
            self._file_state.limits[limit] = self._file_state.limits.get(limit,0)+1
            return limit

    @contextmanager
    def file_slots(self, count):
        limit = self._acquire_files(count)
        try:
            yield
        finally:
            _release_files(self._file_state,count,limit)

    def native_owner_lease(self):
        from .dataflow_object_lease import ObjectLease
        limit = self._acquire_files(1)
        state = self._file_state
        try:
            return ObjectLease(self.root,expected=self._lease_identity,
                               on_close=lambda: _release_files(state,1,limit))
        except BaseException:
            _release_files(state,1,limit)
            raise

    @property
    def root(self) -> Path:
        return self._root

    def _object_path(self, ref: ArtifactRef) -> Path:
        identifier(ref.digest)
        identifier(ref.store_id)
        identifier(ref.key)
        if ref.store_id != self.store_id:
            raise DataflowError("INVALID_CONTRACT", "artifact belongs to another store domain")
        if ref.key != ref.digest:
            raise DataflowError("INVALID_CONTRACT", "artifact key must be its content digest")
        path = self._objects / ref.key
        _no_symlink(path)
        return path

    def _staging_operation(self) -> str:
        return secrets.token_hex(32)

    def scratch(self, *, max_bytes, max_io_bytes):
        """Borrow bounded random-access scratch from this store's authority."""
        from .dataflow_scratch import KernelScratch
        return KernelScratch(self, max_bytes=max_bytes, max_io_bytes=max_io_bytes)

    def put(self, chunks: Iterator[bytes], *, max_bytes: int, media_type: str) -> ArtifactRef:
        from .dataflow_object_lease import shared_access
        if self._writer_closed:
            raise DataflowError('FENCED', 'Artifact writer scope is closed')
        self.require_file_slots(self.PUT_FILES)
        with self.file_slots(1), shared_access(self._root, self._lease_identity, require_protocol=self._protocol_required):
            return self._put(chunks, max_bytes=max_bytes, media_type=media_type)

    def _put(self, chunks, *, max_bytes, media_type):
        # Owner plus fsync/journal inspection headroom. The data FD is counted
        # only while open, so same-thread settlement inspection is not double
        # charged against a closed staging data descriptor.
        with self.file_slots(2):
            return self._put_owned(chunks,max_bytes=max_bytes,media_type=media_type)

    def _put_owned(self, chunks, *, max_bytes, media_type):
        if self._readonly:
            raise DataflowError("FENCED", "historical artifact store is read-only")
        if type(max_bytes) is not int or max_bytes <= 0:
            raise DataflowError("INVALID_CONTRACT", "max_bytes must be a positive int")
        # A caller's upper bound is not a request to allocate that entire
        # amount. Enforce both ceilings on actual bytes before each write.
        max_bytes = min(max_bytes, self._bounds.max_spill_bytes)
        if type(media_type) is not str or not media_type:
            raise DataflowError("INVALID_CONTRACT", "media_type must be a nonempty string")
        operation_id = self._staging_operation()
        reserved = min(max_bytes, 64 * 1024)
        from .dataflow_staging import StagingLease, staging_paths
        owner, pending = staging_paths(self._root, self.store_id, operation_id)
        lease = StagingLease.create(owner)
        staging = None
        begun = settled = prepared = False
        digest = hashlib.sha256()
        size = 0
        published = False
        try:
            try:
                self._journal.artifact(ArtifactCommand(operation_id, 'begin_staging', None, self.store_id, reserved))
            except RecoveryContractError:
                # A native transaction rejection is a definite non-admission.
                # Transport/unknown failures retain the inode for reconciliation.
                lease.remove()
                raise
            begun = True
            with self.file_slots(1):
                descriptor = _create_staging(pending)
                staging = pending
                with os.fdopen(descriptor, "wb") as stream:
                    for chunk in chunks:
                        if type(chunk) is not bytes:
                            raise DataflowError("INVALID_CONTRACT", "artifact chunks must be bytes")
                        size += len(chunk)
                        if size > max_bytes:
                            raise DataflowError("BUDGET_EXCEEDED", "artifact exceeds max_bytes")
                        if size > reserved:
                            # Admission precedes every write beyond the owned extent.
                            # Do not reserve an entire source/spill ceiling up front.
                            from .dataflow_staging_growth import grow_staging
                            reserved = grow_staging(self._journal, operation_id,
                                reserved=reserved, required=size, maximum=max_bytes,
                                spill_budget=self._bounds.max_spill_bytes)
                        view = memoryview(chunk)
                        for offset in range(0, len(view), _MAX_CHUNK_BYTES):
                            part = view[offset:offset + _MAX_CHUNK_BYTES]
                            digest.update(part)
                            stream.write(part)
                    stream.flush()
                    os.fsync(stream.fileno())
            hex_digest = digest.hexdigest()
            ref = ArtifactRef(digest=hex_digest, size_bytes=size, store_id=self.store_id,
                              key=hex_digest, media_type=media_type)
            prepared = True  # An unknown prepare/publication reply must keep its charge.
            self._journal.artifact(ArtifactCommand(identity([operation_id, 'prepare']),
                'prepare_staging', ref, operation_id, size))
            target = self._objects / hex_digest
            _no_symlink(target)
            if target.exists():
                if target.is_symlink():
                    raise DataflowError("INVALID_CONTRACT", "artifact target is a symlink")
                if target.stat().st_size != size or _file_digest(target, max_bytes=size) != hex_digest:
                    raise DataflowError("INVALID_CONTRACT", "content digest collision on publish")
            else:
                try:
                    os.link(staging, target)
                except FileExistsError:
                    if target.stat().st_size != size or _file_digest(target, max_bytes=size) != hex_digest:
                        raise DataflowError("INVALID_CONTRACT", "content digest collision on publish")
            published = True
            _sync_directory(self._objects)
            # A deduplicated temporary still occupies a separate inode until
            # unlink. Release that physical ownership before refunding budget.
            staging.unlink()
            _sync_directory(self._staging)
            self._journal.artifact(ArtifactCommand(identity([operation_id, 'settle']),
                'settle_staging', ref, operation_id, size))
            settled = True
            return ref
        finally:
            try:
                if staging is not None:
                    staging.unlink(missing_ok=True)
                    _sync_directory(self._staging)
                if begun and not published and not prepared:
                    try:
                        self._journal.artifact(ArtifactCommand(identity([operation_id, 'release']),
                            'release_staging', None, operation_id, 0))
                        settled = True
                    except BaseException:
                        # Unknown authority retains its original charge and lease
                        # inode evidence for a later driver reconciliation.
                        pass
                if settled:
                    lease.remove()
            finally:
                lease.close()

    def read(self, ref: ArtifactRef, *, max_bytes: int) -> Iterator[bytes]:
        from .dataflow_object_lease import shared_access
        # This lease covers the generator's actual FD lifetime, including yield.
        with self.file_slots(self.READ_FILES), shared_access(self._root, self._lease_identity, require_protocol=self._protocol_required):
            yield from self._read(ref, max_bytes=max_bytes)

    def _read(self, ref, *, max_bytes):
        if type(max_bytes) is not int or max_bytes <= 0:
            raise DataflowError("INVALID_CONTRACT", "max_bytes must be a positive int")
        if ref.size_bytes > max_bytes:
            raise DataflowError("BUDGET_EXCEEDED", "artifact is larger than max_bytes")
        path = self._object_path(ref)
        chunk_bytes = min(_MAX_CHUNK_BYTES, max(1, self._bounds.max_row_bytes))
        try:
            stat = path.stat()
        except FileNotFoundError:
            raise DataflowError("UNKNOWN_EFFECT", "artifact is definitely absent")
        except OSError as exc:
            raise DataflowError("UNKNOWN_EFFECT", "artifact cannot be inspected: " + type(exc).__name__)
        if stat.st_size != ref.size_bytes:
            raise DataflowError("INVALID_CONTRACT", "artifact size differs from its reference")
        stream = open(path, "rb")
        try:
            digest = hashlib.sha256()
            total = 0
            while True:
                chunk = stream.read(chunk_bytes)
                if not chunk:
                    break
                total += len(chunk)
                if total > ref.size_bytes:
                    raise DataflowError("INVALID_CONTRACT", "artifact grew beyond its reference")
                digest.update(chunk)
                yield chunk
            if total != ref.size_bytes or digest.hexdigest() != ref.digest:
                raise DataflowError("INVALID_CONTRACT", "artifact bytes do not match its reference")
        finally:
            stream.close()

    def inspect(self, ref: ArtifactRef) -> bool:
        from .dataflow_object_lease import shared_access
        with self.file_slots(self.READ_FILES), shared_access(self._root, self._lease_identity, require_protocol=self._protocol_required):
            return self._inspect(ref)

    def _inspect(self, ref):
        path = self._object_path(ref)
        try:
            stat = path.stat()
        except FileNotFoundError:
            return False
        except OSError as exc:
            raise DataflowError("UNKNOWN_EFFECT", "artifact cannot be inspected: " + type(exc).__name__)
        if stat.st_size != ref.size_bytes:
            raise DataflowError("INVALID_CONTRACT", "artifact size differs from its reference")
        if _file_digest(path, max_bytes=ref.size_bytes) != ref.digest:
            raise DataflowError("INVALID_CONTRACT", "artifact bytes do not match its reference")
        return True

    def retain(self, ref: ArtifactRef, *, owner: str) -> None:
        from .dataflow_object_lease import shared_access
        with self.file_slots(self.READ_FILES), shared_access(self._root, self._lease_identity, require_protocol=self._protocol_required):
            if not self._inspect(ref):
                raise DataflowError('UNKNOWN_EFFECT', 'Cannot pin an absent artifact')
            self._retain(ref, owner=owner)

    def _retain(self, ref, *, owner):
        if self._readonly:
            raise DataflowError("FENCED", "historical artifact store is read-only")
        from .contracts.dataflow import ArtifactCommand
        identifier(owner)
        self._object_path(ref)
        operation_id = hashlib.sha256(("pin:" + _pin_identity(ref, owner)).encode()).hexdigest()
        self._journal.artifact(ArtifactCommand(operation_id=operation_id, kind="pin", ref=ref,
                                               owner=owner, size_bytes=ref.size_bytes))

    def release(self, ref: ArtifactRef, *, owner: str) -> None:
        if self._readonly:
            raise DataflowError("FENCED", "historical artifact store is read-only")
        from .contracts.dataflow import ArtifactCommand
        identifier(owner)
        self._object_path(ref)
        operation_id = hashlib.sha256(("unpin:" + _pin_identity(ref, owner)).encode()).hexdigest()
        self._journal.artifact(ArtifactCommand(operation_id=operation_id, kind="unpin", ref=ref,
                                               owner=owner, size_bytes=ref.size_bytes))

    def release_materialized(self, ref, *, owner):
        if self._readonly:
            raise DataflowError('FENCED', 'Historical artifact store is read-only')
        self._object_path(ref)
        self._journal.release_materialized(ref, owner=owner)

    def close(self):
        # Read-only/result handles may still read explicitly rooted history.
        # New publications require a new active store owner.
        self._writer_closed = True
        lease, self._writer_lease = self._writer_lease, None
        if lease is not None:
            try:
                lease.close()
            finally:
                if self._writer_slot:
                    self._writer_slot = False
                    _release_files(self._file_state,1,self._writer_limit)

    def __del__(self):
        if getattr(self, '_writer_lease', None) is not None:
            self.close()


def _release_files(state, count, limit, *, final=True):
    with state.lock:
        state.used -= count
        if final:
            state.limits[limit] -= 1
            if not state.limits[limit]:
                del state.limits[limit]
        if state.used < 0:
            raise RuntimeError('Artifact file window released twice')


def _pin_identity(ref: ArtifactRef, owner: str) -> str:
    from .recovery_contract import identity
    return identity({"digest": ref.digest, "store_id": ref.store_id, "key": ref.key, "owner": owner})


def _file_digest(path: Path, *, max_bytes: int) -> str:
    digest, total = hashlib.sha256(), 0
    fd = os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK | os.O_CLOEXEC)
    try:
        if not stat.S_ISREG(os.fstat(fd).st_mode):
            raise DataflowError('INVALID_CONTRACT', 'Artifact object is not a regular file')
        with os.fdopen(fd, 'rb', closefd=False) as stream:
            while True:
                chunk = stream.read(min(_MAX_CHUNK_BYTES, max_bytes - total + 1))
                if not chunk:
                    break
                total += len(chunk)
                if total > max_bytes:
                    raise DataflowError('INVALID_CONTRACT', 'Artifact grew beyond its reference')
                digest.update(chunk)
    finally:
        os.close(fd)
    return digest.hexdigest()
