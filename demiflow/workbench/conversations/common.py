"""Bounded data and stable files for development chat."""
from __future__ import annotations

import hashlib
import json
import os
from pathlib import Path
import re
import stat
import time

from demiflow.forge.schema import ForgeError
from demiflow.agent_transport import encode
from demiflow.agent_transport import strict_json


def reject(message, code='operation_rejected'):
    raise ForgeError(message, code=code)


def wire(value, maximum=1024 * 1024):
    try:
        return encode(value, maximum)
    except ValueError as exc:
        raise ForgeError(str(exc), code='resource_limit') from exc


def digest(value):
    return hashlib.sha256(wire(value)).hexdigest()


def identifier(value):
    if type(value) is not str or not re.fullmatch(r'[A-Za-z0-9_-]{1,128}', value):
        reject('Invalid development identity', 'invalid_request')
    return value


def text(value, maximum=32768):
    if type(value) is not str or not value.strip() or len(value.encode()) > maximum:
        reject('Text is empty or exceeds byte budget', 'invalid_request')
    return value


def integer(value, low, high):
    if type(value) is not int or not low <= value <= high:
        reject('Integer outside development budget', 'invalid_request')
    return value


def plain_path(value):
    if type(value) is not str or not value or len(value) > 2048:
        reject('Invalid scoped path')
    if value.startswith('/') or '\\' in value or any(p in {'', '.', '..'} for p in value.split('/')):
        reject('Scoped paths must be normalized relative paths')
    return value


def safe_path(root, relative, *, missing=False):
    path = Path(root)
    for component in plain_path(relative).split('/'):
        path = path / component
        try:
            info = path.lstat()
        except FileNotFoundError:
            if missing:
                continue
            raise
        if stat.S_ISLNK(info.st_mode) or not (stat.S_ISREG(info.st_mode) or stat.S_ISDIR(info.st_mode)):
            reject('Symbolic links and special files are outside the development scope')
        if stat.S_ISREG(info.st_mode) and info.st_nlink != 1:
            reject('Hard-linked files are outside the development scope')
    return path


def root_identity(root):
    path = Path(root).absolute()
    if path != path.resolve(strict=True) or not path.is_dir():
        reject('Development root must be a physical directory', 'conflict')
    info = path.stat()
    return dict(path=str(path), device=info.st_dev, inode=info.st_ino)


def read_file(path, maximum=1024 * 1024):
    fd = os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
    with os.fdopen(fd, 'rb') as stream:
        before = os.fstat(fd)
        if not stat.S_ISREG(before.st_mode) or before.st_size > maximum:
            reject('Context file exceeds budget or is not regular', 'resource_limit')
        raw = stream.read(maximum + 1)
        after = os.fstat(fd)
        keys = ('st_dev', 'st_ino', 'st_size', 'st_mtime_ns', 'st_ctime_ns')
        if len(raw) > maximum or any(getattr(before, k) != getattr(after, k) for k in keys):
            reject('File changed during bounded reading', 'conflict')
        return raw


def snapshot(root, paths, *, max_files=4096, max_bytes=64 * 1024**2):
    """Hash exact watched bytes, including notebook output; never alter the source."""
    result, total, visited = {}, 0, 0
    deadline = time.monotonic() + 10
    pending = [plain_path(p) for p in paths]
    while pending:
        rel = pending.pop()
        if rel in result:
            continue
        visited += 1
        if visited > max_files or time.monotonic() > deadline:
            reject('Source snapshot count/time budget exceeded', 'resource_limit')
        path = safe_path(root, rel, missing=True)
        if not path.exists():
            result[rel] = None
        elif path.is_dir():
            with os.scandir(path) as entries:
                for entry in entries:
                    if len(pending) + visited >= max_files:
                        reject('Source snapshot entry budget exceeded', 'resource_limit')
                    pending.append(rel + '/' + entry.name)
        else:
            size = path.stat().st_size
            if total + size > max_bytes:
                reject('Source snapshot byte budget exceeded', 'resource_limit')
            hashed = hashlib.sha256()
            fd = os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
            with os.fdopen(fd, 'rb') as stream:
                before = os.fstat(fd)
                if not stat.S_ISREG(before.st_mode) or before.st_size != size:
                    reject('Source replaced during snapshot', 'conflict')
                remaining = size
                while remaining:
                    chunk = stream.read(min(65536, remaining))
                    if not chunk:
                        reject('Source truncated during snapshot', 'conflict')
                    hashed.update(chunk); remaining -= len(chunk)
                after = os.fstat(fd)
                if (before.st_size, before.st_mtime_ns, before.st_ctime_ns) != (after.st_size, after.st_mtime_ns, after.st_ctime_ns):
                    reject('Source changed during snapshot', 'conflict')
            total += size
            result[rel] = dict(bytes=size, sha256=hashed.hexdigest())
    return result


def directory_bytes(root, *, entries=16384, deadline=None):
    # Include directory blocks and small-file allocation overhead. Logical size
    # alone undercounts retained provider state; sparse files still reserve their
    # logical size here because later writes can allocate their holes.
    try:
        info = Path(root).stat()
    except FileNotFoundError:
        return 0  # A finished owned profile may be removed during sampling.
    total, count, pending = max(info.st_size, info.st_blocks * 512), 0, [Path(root)]
    deadline = min(deadline, time.monotonic() + 2) if deadline is not None else time.monotonic() + 2
    while pending:
        try:
            rows = os.scandir(pending.pop())
        except FileNotFoundError:
            continue
        with rows as rows:
            for row in rows:
                count += 1
                if count > entries or time.monotonic() > deadline:
                    reject('Private state scan exceeded entry/time budget', 'resource_limit')
                if row.is_symlink():
                    reject('Unexpected link in private provider state')
                try:
                    info = row.stat(follow_symlinks=False)
                except FileNotFoundError:
                    continue
                total += max(info.st_size, info.st_blocks * 512)
                if row.is_dir(follow_symlinks=False):
                    pending.append(Path(row.path))
    return total


def private_directory_bytes(root, *, entries):
    """One shared inventory for retained provider homes and Forge evidence.

    Top-level database/control files use their own reservations. Count every
    physical child directory, including its allocated blocks; retain the
    existing per-directory entry limit within one total scan deadline.
    """
    sizes = {}
    deadline = time.monotonic() + 3
    with os.scandir(root) as children:
        for count, child in enumerate(children):
            if count > entries or time.monotonic() > deadline:
                reject('State inventory exceeds entry/time budget', 'resource_limit')
            if child.is_dir(follow_symlinks=False):
                sizes[child.name] = directory_bytes(child.path, deadline=deadline)
    return sizes
