"""Safe local JSON and durable file writes for Forge control files."""
from __future__ import annotations

from contextlib import contextmanager
import errno
import json
import os
from pathlib import Path
import uuid

from .schema import ForgeError


@contextmanager
def directory_handle(path, *, create=False):
    """Anchor every component of the publication directory without following links."""
    path = Path(path).absolute()
    if '..' in path.parts:
        raise ForgeError('Non-normalized directory', code='invalid_request')
    flags = os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW
    fd = os.open(path.anchor, flags)
    try:
        for component in path.parts[1:]:
            try:
                child = os.open(component, flags, dir_fd=fd)
            except FileNotFoundError:
                if not create:
                    raise
                try:
                    os.mkdir(component, mode=0o700, dir_fd=fd)
                except FileExistsError:
                    pass
                else:
                    os.fsync(fd)
                child = os.open(component, flags, dir_fd=fd)
            os.close(fd)
            fd = child
        yield fd
        if not os.path.samestat(os.fstat(fd), os.stat(path, follow_symlinks=False)):
            raise ForgeError('Project directory changed during publication', code='conflict')
    except OSError as exc:
        if exc.errno in (errno.ENOTDIR, errno.ELOOP):
            raise ForgeError('Invalid project directory or symlink: ' + str(path), code='conflict') from exc
        raise
    finally:
        os.close(fd)


def ensure_directory(path):
    with directory_handle(path, create=True):
        pass


def publish_bytes(path, raw, *, create=True):
    """Publish a complete file using relative names on one open directory."""
    path = Path(path)
    with directory_handle(path.parent) as parent:
        temp = '.forge-write-' + uuid.uuid4().hex
        handle = os.open(temp, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600, dir_fd=parent)
        try:
            with os.fdopen(handle, 'wb') as out:
                out.write(raw)
                out.flush()
                os.fsync(out.fileno())
            if create:
                try:
                    os.link(temp, path.name, src_dir_fd=parent, dst_dir_fd=parent, follow_symlinks=False)
                except FileExistsError as exc:
                    raise ForgeError('File already exists: ' + path.name, code='conflict') from exc
            else:
                os.replace(temp, path.name, src_dir_fd=parent, dst_dir_fd=parent)
            os.fsync(parent)
        finally:
            try:
                os.unlink(temp, dir_fd=parent)
            except FileNotFoundError:
                pass


def no_symlinks(path: Path) -> Path:
    for part in (path, *path.parents):
        if part.is_symlink():
            raise ForgeError(f"Symlink in Forge path: {part}")
    return path


def read_json(path: Path, *, maximum: int = 2 * 1024 * 1024):
    no_symlinks(path)
    if not path.is_file():
        raise ForgeError(f"Expected regular JSON file: {path}")
    if path.stat().st_size > maximum:
        raise ForgeError(f"JSON file exceeds {maximum} bytes: {path}")
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (UnicodeError, json.JSONDecodeError) as exc:
        raise ForgeError(f"Invalid JSON: {path}: {exc}") from exc


def atomic_write_text(path: Path, value: str) -> None:
    no_symlinks(path.parent)
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.{os.getpid()}.{uuid.uuid4().hex}.tmp")
    try:
        with temporary.open("x", encoding="utf-8") as handle:
            handle.write(value)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, path)
        fsync_directory(path.parent)
    finally:
        temporary.unlink(missing_ok=True)


def atomic_write_json(path: Path, value) -> None:
    atomic_write_text(path, json.dumps(value, ensure_ascii=False, indent=2, sort_keys=True, allow_nan=False) + "\n")


def write_exclusive(path: Path, value: str) -> None:
    no_symlinks(path.parent)
    path.parent.mkdir(parents=True, exist_ok=True)
    data = value.encode("utf-8")
    fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    try:
        with os.fdopen(fd, "wb") as handle:
            handle.write(data)
            handle.flush()
            os.fsync(handle.fileno())
    except Exception:
        path.unlink(missing_ok=True)
        raise


def fsync_directory(path: Path) -> None:
    descriptor = os.open(path, os.O_RDONLY)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


__all__ = ["atomic_write_json", "atomic_write_text", "directory_handle", "ensure_directory", "no_symlinks", "publish_bytes", "read_json", "write_exclusive"]
