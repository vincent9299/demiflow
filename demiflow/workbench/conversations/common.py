"""Bounded data and stable files for development chat (shared primitives owned by Forge bridge)."""
from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
import hashlib
import os
from pathlib import Path
import re
import stat
import time

from demiflow.agent_transport import strict_json
from demiflow.forge.bridge.common import (
    digest,
    integer,
    plain_path,
    read_file,
    reject,
    safe_path,
    text,
    wire,
)


def identifier(value):
    if type(value) is not str or not re.fullmatch(r'[A-Za-z0-9_-]{1,128}', value):
        reject('Invalid development identity', 'invalid_request')
    return value


def root_identity(root):
    path = Path(root).absolute()
    if path != path.resolve(strict=True) or not path.is_dir():
        reject('Development root must be a physical directory', 'conflict')
    info = path.stat()
    return dict(path=str(path), device=info.st_dev, inode=info.st_ino)


def snapshot(root, paths, *, max_files=4096, max_bytes=64 * 1024**2, exclude=()):
    """Hash exact watched bytes, including notebook output; never alter the source."""
    result, total, visited = {}, 0, 0
    deadline = time.monotonic() + 10
    pending = [plain_path(p) for p in paths]
    excluded = set(exclude)
    while pending:
        rel = pending.pop()
        if rel in result or rel in excluded:
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
                    child = rel + '/' + entry.name
                    if child in excluded:
                        continue
                    if len(pending) + visited >= max_files:
                        reject('Source snapshot entry budget exceeded', 'resource_limit')
                    pending.append(child)
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


def directory_bytes(root, *, entries=65536, deadline=None, codex_executable=None):
    # Include directory blocks and small-file allocation overhead. Logical size
    # alone undercounts retained provider state; sparse files still reserve their
    # logical size here because later writes can allocate their holes.
    now = time.monotonic()
    deadline = deadline if deadline is not None else now + 10
    if now > deadline:
        reject('Private state scan exceeded entry/time budget', 'resource_limit')
    try:
        info = Path(root).lstat()
    except FileNotFoundError:
        return 0  # A finished owned profile may be removed during sampling.
    if not stat.S_ISDIR(info.st_mode):
        reject('Unexpected non-directory in private provider state')
    total, count, pending = max(info.st_size, info.st_blocks * 512), 0, [Path(root)]
    while pending:
        if time.monotonic() > deadline:
            reject('Private state scan exceeded entry/time budget', 'resource_limit')
        try:
            rows = os.scandir(pending.pop())
        except FileNotFoundError:
            continue
        with rows as rows:
            for row in rows:
                count += 1
                if count > entries or time.monotonic() > deadline:
                    reject('Private state scan exceeded entry/time budget', 'resource_limit')
                try:
                    info = row.stat(follow_symlinks=False)
                except FileNotFoundError:
                    continue
                if stat.S_ISLNK(info.st_mode):
                    from .codex import native_helper_link
                    link = Path(row.path)
                    try:
                        target = link.readlink()
                    except FileNotFoundError:
                        continue
                    if not native_helper_link(link.relative_to(root), target, codex_executable):
                        reject('Unexpected link in private provider state: ' + str(link.relative_to(root))[:256])
                total += max(info.st_size, info.st_blocks * 512)
                if stat.S_ISDIR(info.st_mode):
                    pending.append(Path(row.path))
    if time.monotonic() > deadline:
        reject('Private state scan exceeded entry/time budget', 'resource_limit')
    return total


def private_directory_bytes(root, *, entries, codex_executable=None):
    """One shared inventory for retained provider homes and Forge evidence.

    Top-level database/control files use their own reservations. Count every
    physical child directory, including its allocated blocks. Scan independent
    homes concurrently so storage metadata latency does not add up across
    histories. Every worker shares the same overall deadline.
    """
    directories = []
    deadline = time.monotonic() + 15
    with os.scandir(root) as children:
        for count, child in enumerate(children):
            if count > entries or time.monotonic() > deadline:
                reject('State inventory exceeds entry/time budget', 'resource_limit')
            if child.is_dir(follow_symlinks=False):
                directories.append(Path(child.path))
    if time.monotonic() > deadline:
        reject('State inventory exceeds entry/time budget', 'resource_limit')

    def scan(path):
        return path.name, directory_bytes(path, deadline=deadline, codex_executable=codex_executable)

    if len(directories) < 2:
        return dict(map(scan, directories))
    with ThreadPoolExecutor(max_workers=min(8, len(directories))) as workers:
        return dict(workers.map(scan, directories))
