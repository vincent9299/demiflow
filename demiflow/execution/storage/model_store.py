"""Verified, immutable project-owned model objects, outside the run artifact store."""
from __future__ import annotations
from demiflow.execution.contracts.training import validate_result

from contextlib import contextmanager
from dataclasses import replace
import hashlib
import fcntl
import io
import json
import os
from pathlib import Path
import stat
import time
from urllib.parse import urlsplit, unquote

from demiflow.execution.contracts.training import FitResult, ProjectObject
from demiflow.execution.contracts.recovery_contract import MAX_DOCUMENT_BYTES, RecoveryContractError, document
from demiflow.execution.journal.training_index import FitConflict


def local_path(uri):
    parsed = urlsplit(uri)
    if parsed.scheme:
        if parsed.scheme != 'file' or parsed.netloc or parsed.query or parsed.fragment:
            raise RecoveryContractError("Model store requires an authorized local file URI")
        uri = unquote(parsed.path)
    path = Path(uri)
    if not path.is_absolute() or '..' in path.parts:
        raise RecoveryContractError("Model store requires an absolute local path")
    return path


@contextmanager
def open_regular_file(path):
    # Traverse by descriptor so a staged symlink or renamed ancestor cannot
    # redirect an input into credentials or another project's file.
    flags = os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW | os.O_CLOEXEC
    parent = os.open('/', flags)
    try:
        for component in path.parts[1:-1]:
            child = os.open(component, flags, dir_fd=parent)
            os.close(parent)
            parent = child
        fd = os.open(path.name, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK | os.O_CLOEXEC,
                     dir_fd=parent)
        with os.fdopen(fd, 'rb') as stream:
            if not stat.S_ISREG(os.fstat(fd).st_mode):
                raise RecoveryContractError("Model member must be a regular file")
            yield stream
    finally:
        os.close(parent)


def _transfer(source, expected, size, output=None, *, check_stop):
    digest = hashlib.sha256()
    length = 0
    while True:
        if check_stop is not None:
            check_stop()
        data = source.read(min(1024 * 1024, size - length + 1))
        if not data:
            break
        length += len(data)
        if length > size:
            raise RecoveryContractError("Model object exceeds its declared size")
        digest.update(data)
        if output is not None:
            output.write(data)
    if length != size or digest.hexdigest() != expected:
        raise RecoveryContractError("Model object content differs from its immutable identity")


class ModelStore:
    def __init__(self, index, *, check_stop=None):
        self.check_stop = check_stop
        self.index = index
        self.root = local_path(index.binding.model_store_uri)
        if any(p.is_symlink() for p in (self.root, *self.root.parents)):
            raise RecoveryContractError("Symlink in model store root")
        if not index.readonly:
            self.root.mkdir(mode=0o700, parents=True, exist_ok=True)
        self.objects = self.root / 'objects'
        if self.objects.is_symlink():
            raise RecoveryContractError("Symlink in project model objects")
        if not index.readonly:
            self.objects.mkdir(mode=0o700, exist_ok=True)

    def _path(self, digest):
        return self.objects / digest[:2] / digest

    def verify(self, ref):
        path = self._path(ref.sha256)
        if local_path(ref.uri) != path or ref.size_bytes > self.index.model_byte_limit():
            raise RecoveryContractError("Object is outside this project's immutable model store")
        with open_regular_file(path) as stream:
            _transfer(stream, ref.sha256, ref.size_bytes, check_stop=self.check_stop)

    def _reap_imports(self, current_digest, current_fd):
        """Recover only unfinalized reservations from demonstrably dead importers."""
        cursor = ''
        while pending := self.index.pending_objects(str(self.root),cursor):
            for digest in pending:
                if self.check_stop is not None:
                    self.check_stop()
                cursor = digest
                own_partition = digest[:2] == current_digest[:2]
                directory = current_fd if own_partition else os.open(
                    self._path(digest).parent, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
                try:
                    if not own_partition:
                        try:
                            fcntl.flock(directory,fcntl.LOCK_EX | fcntl.LOCK_NB)
                        except BlockingIOError:
                            continue  # An actual writer owns this partition.
                    try:
                        os.stat(digest, dir_fd=directory, follow_symlinks=False)
                    except FileNotFoundError:
                        pass
                    else:
                        continue  # A linked/promoted inode is never reclaimed.
                    try:
                        os.unlink('.'+digest+'.importing',dir_fd=directory)
                        os.fsync(directory)
                    except FileNotFoundError:
                        pass
                    self.index.release_unwritten_object(str(self.root),digest)
                finally:
                    if not own_partition:
                        os.close(directory)

    def _put(self, stream, digest, size):
        target = self._path(digest)
        target.parent.mkdir(mode=0o700, exist_ok=True)
        directory = os.open(target.parent, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
        try:
            # Same-prefix imports serialize. The deterministic staging file can
            # only be removed by a writer holding this lock; process death
            # releases it, while a confirmed immutable inode is never replaced.
            while True:
                if self.check_stop is not None:
                    self.check_stop()
                try:
                    fcntl.flock(directory, fcntl.LOCK_EX | fcntl.LOCK_NB)
                    break
                except BlockingIOError:
                    if self.check_stop is None:
                        raise FitConflict("Model import is busy")
                    time.sleep(.02)
            ref = ProjectObject(str(target), digest, size)
            name = '.' + digest + '.importing'
            try:
                os.unlink(name, dir_fd=directory)
            except FileNotFoundError:
                pass
            if not self.index.reserve_object(str(self.root), digest, size):
                self._reap_imports(digest,directory)
                if not self.index.reserve_object(str(self.root),digest,size):
                    raise FitConflict("Project model store exceeds file or byte quota")
            if os.path.lexists(target):
                _transfer(stream, digest, size, check_stop=self.check_stop)
                self.verify(ref)
                self.index.complete_object(str(self.root),digest)
                return ref
            flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW | os.O_CLOEXEC
            try:
                with os.fdopen(os.open(name, flags, 0o600, dir_fd=directory), 'wb') as tmp:
                    _transfer(stream, digest, size, tmp, check_stop=self.check_stop)
                    tmp.flush()
                    os.fchmod(tmp.fileno(), 0o400)
                    os.fsync(tmp.fileno())
                os.link(name, digest, src_dir_fd=directory, dst_dir_fd=directory, follow_symlinks=False)
                os.fsync(directory)
                self.index.complete_object(str(self.root),digest)
            except BaseException:
                if not os.path.lexists(target):
                    self.index.release_unwritten_object(str(self.root), digest)
                raise
            finally:
                try:
                    os.unlink(name, dir_fd=directory)
                except FileNotFoundError:
                    pass
            return ref
        finally:
            os.close(directory)

    def _import(self, ref, staging):
        path = local_path(ref.uri)
        if path == self._path(ref.sha256):
            self.verify(ref)
            return ref
        if not path.is_relative_to(staging):
            raise RecoveryContractError("Returned model file is outside this launch's authorized staging")
        with open_regular_file(path) as stream:
            if os.fstat(stream.fileno()).st_size != ref.size_bytes:
                raise RecoveryContractError("Staged model file size differs")
            return self._put(stream, ref.sha256, ref.size_bytes)

    def snapshot(self, snapshot, instance, key):
        staging = self.root / 'staging' / instance / key
        return replace(snapshot, **{group: {k: self._import(ref, staging)
                   for k, ref in getattr(snapshot, group).items()}
                   for group in ('files','resume_files')})

    def save_manifest(self, result):
        data = document(result.to_dict())
        return self._put(io.BytesIO(data), hashlib.sha256(data).hexdigest(), len(data))

    def load(self, ref, request):
        if local_path(ref.uri) != self._path(ref.sha256) or ref.size_bytes > MAX_DOCUMENT_BYTES:
            raise RecoveryContractError("Invalid project fit manifest")
        output = io.BytesIO()
        with open_regular_file(local_path(ref.uri)) as stream:
            _transfer(stream, ref.sha256, ref.size_bytes, output, check_stop=self.check_stop)
        result = FitResult.from_dict(json.loads(output.getvalue()))
        validate_result(result, request)
        seen = set()
        for snapshot in result.snapshots:
            for obj in (*snapshot.files.values(), *snapshot.resume_files.values()):
                if obj not in seen:
                    self.verify(obj)
                    seen.add(obj)
        return result


def read_metadata(path, limit):
    if path.resolve(strict=True) != path:
        raise ValueError("Job metadata must be a direct file")
    with path.open('rb') as stream:
        raw = stream.read(limit + 1)
    if len(raw) > limit:
        raise ValueError("Job metadata exceeds its admitted byte limit")
    return raw
