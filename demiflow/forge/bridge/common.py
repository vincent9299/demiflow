"""Bounded framing, strict JSON decoding, and safe relative path helpers for Forge bridge."""
from __future__ import annotations

import hashlib
import os
from pathlib import Path
import stat

from demiflow.agent_transport import encode, strict_json
from demiflow.forge.schema import ForgeError


def reject(message, code='operation_rejected'):
    raise ForgeError(message, code=code)


def wire(value, maximum=1024 * 1024):
    try:
        return encode(value, maximum)
    except ValueError as exc:
        raise ForgeError(str(exc), code='resource_limit') from exc


def digest(value):
    return hashlib.sha256(wire(value)).hexdigest()


def decode(raw):
    """Bound lexical nesting/container count before strict JSON parsing."""
    depth = count = 0
    quoted = escaped = False
    for char in raw:
        if quoted:
            if escaped:
                escaped = False
            elif char == 92:
                escaped = True
            elif char == 34:
                quoted = False
        elif char == 34:
            quoted = True
        elif char in (91, 123):
            depth += 1
            count += 1
            if depth > 64:
                reject('Provider JSON nesting exceeds budget', 'resource_limit')
        elif char in (93, 125):
            depth -= 1
        elif char in (44, 58):
            count += 1
        if count > 50000:
            reject('Provider JSON structure exceeds budget', 'resource_limit')
    value = strict_json(raw)
    if type(value) is not dict:
        reject('Provider frame is not an object')
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
