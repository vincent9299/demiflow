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
import tempfile
from pathlib import Path
from typing import TYPE_CHECKING, Iterator

from .contracts.dataflow import ArtifactRef, ArtifactCommand
from .contracts.dataflow_errors import DataflowError
from .recovery_contract import identifier, identity

if TYPE_CHECKING:
    from .contracts.dataflow import ArtifactAuthorityPort, Bounds

ARTIFACT_STAGING_BUDGET = "artifact_staging_bytes"
_MAX_CHUNK_BYTES = 1 << 20
_OBJECTS = "objects"
_STAGING = "staging"


def _no_symlink(path: Path) -> None:
    for item in (path, *path.parents):
        if item.is_symlink():
            raise DataflowError("INVALID_CONTRACT", "artifact root contains a symlink: " + str(item))


def _sync_directory(path):
    descriptor = os.open(path, os.O_RDONLY | os.O_DIRECTORY)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


class RunArtifactStore:
    def __init__(self, *, root: Path, store_id: str, journal: "ArtifactAuthorityPort", bounds: "Bounds", readonly: bool = False):
        identifier(store_id)
        root = Path(root)
        if not root.is_absolute():
            raise DataflowError("INVALID_CONTRACT", "artifact store root must be absolute")
        _no_symlink(root)
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
        self._objects = root / store_id / _OBJECTS
        self._staging = root / store_id / _STAGING
        if not readonly:
            self._objects.mkdir(parents=True, exist_ok=True)
            self._staging.mkdir(parents=True, exist_ok=True)
        _no_symlink(self._objects)
        _no_symlink(self._staging)

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

    def put(self, chunks: Iterator[bytes], *, max_bytes: int, media_type: str) -> ArtifactRef:
        if self._readonly:
            raise DataflowError("FENCED", "historical artifact store is read-only")
        if type(max_bytes) is not int or max_bytes <= 0:
            raise DataflowError("INVALID_CONTRACT", "max_bytes must be a positive int")
        if max_bytes > self._bounds.max_spill_bytes:
            raise DataflowError("BUDGET_EXCEEDED", "artifact exceeds the staging ceiling")
        if type(media_type) is not str or not media_type:
            raise DataflowError("INVALID_CONTRACT", "media_type must be a nonempty string")
        operation_id = self._staging_operation()
        self._journal.reserve(operation_id, {ARTIFACT_STAGING_BUDGET: max_bytes})
        staging = None
        digest = hashlib.sha256()
        size = 0
        published = False
        try:
            descriptor, staging_name = tempfile.mkstemp(prefix='.pending-' + operation_id + '-', dir=self._staging)
            staging = Path(staging_name)
            with os.fdopen(descriptor, "wb") as stream:
                for chunk in chunks:
                    if type(chunk) is not bytes:
                        raise DataflowError("INVALID_CONTRACT", "artifact chunks must be bytes")
                    size += len(chunk)
                    if size > max_bytes:
                        raise DataflowError("BUDGET_EXCEEDED", "artifact exceeds max_bytes")
                    view = memoryview(chunk)
                    for offset in range(0, len(view), _MAX_CHUNK_BYTES):
                        part = view[offset:offset + _MAX_CHUNK_BYTES]
                        digest.update(part)
                        stream.write(part)
                stream.flush()
                os.fsync(stream.fileno())
            hex_digest = digest.hexdigest()
            target = self._objects / hex_digest
            _no_symlink(target)
            if target.exists():
                if target.is_symlink():
                    raise DataflowError("INVALID_CONTRACT", "artifact target is a symlink")
                if target.stat().st_size != size or _file_digest(target) != hex_digest:
                    raise DataflowError("INVALID_CONTRACT", "content digest collision on publish")
            else:
                try:
                    os.link(staging, target)
                except FileExistsError:
                    if target.stat().st_size != size or _file_digest(target) != hex_digest:
                        raise DataflowError("INVALID_CONTRACT", "content digest collision on publish")
            published = True
            _sync_directory(self._objects)
            ref = ArtifactRef(digest=hex_digest, size_bytes=size, store_id=self.store_id,
                              key=hex_digest, media_type=media_type)
            # A deduplicated temporary still occupies a separate inode until
            # unlink. Release that physical ownership before refunding budget.
            staging.unlink()
            _sync_directory(self._staging)
            self._journal.artifact(ArtifactCommand(identity([operation_id, 'settle']),
                'settle_staging', ref, operation_id, size))
            return ref
        finally:
            if staging is not None:
                staging.unlink(missing_ok=True)
                _sync_directory(self._staging)
            if not published:
                try:
                    self._journal.artifact(ArtifactCommand(identity([operation_id, 'release']),
                        'release_staging', None, operation_id, 0))
                except BaseException:
                    # Keep conservative accounting if the authority is unknown;
                    # never hide the original write/iterator failure.
                    pass

    def read(self, ref: ArtifactRef, *, max_bytes: int) -> Iterator[bytes]:
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
        path = self._object_path(ref)
        try:
            stat = path.stat()
        except FileNotFoundError:
            return False
        except OSError as exc:
            raise DataflowError("UNKNOWN_EFFECT", "artifact cannot be inspected: " + type(exc).__name__)
        if stat.st_size != ref.size_bytes:
            raise DataflowError("INVALID_CONTRACT", "artifact size differs from its reference")
        if _file_digest(path) != ref.digest:
            raise DataflowError("INVALID_CONTRACT", "artifact bytes do not match its reference")
        return True

    def retain(self, ref: ArtifactRef, *, owner: str) -> None:
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


def _pin_identity(ref: ArtifactRef, owner: str) -> str:
    from .recovery_contract import identity
    return identity({"digest": ref.digest, "store_id": ref.store_id, "key": ref.key, "owner": owner})


def _file_digest(path: Path) -> str:
    digest = hashlib.sha256()
    with open(path, "rb") as stream:
        while True:
            chunk = stream.read(_MAX_CHUNK_BYTES)
            if not chunk:
                break
            digest.update(chunk)
    return digest.hexdigest()
