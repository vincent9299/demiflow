"""Project control I/O and filesystem path definitions shared by Forge domains."""
from contextlib import contextmanager
import fcntl
import json
import os
from pathlib import Path, PurePosixPath
import re
import math
import stat
from demiflow.forge.io import directory_handle, no_symlinks, publish_bytes
from demiflow.forge.platform import bounded_json
from demiflow.forge.platform import content_digest
from demiflow.forge.platform_environment import read_bounded
from demiflow.forge.schema import ForgeError

SYSTEM_PRIVATE_PATHS = tuple(Path(path) for path in (
    '/etc/shadow', '/etc/gshadow', '/etc/ssh', '/etc/ssl/private', '/etc/sudoers', '/etc/sudoers.d',
))

def identifier(value):
    if not isinstance(value, str) or not re.fullmatch(
        r"[A-Za-z][A-Za-z0-9_-]{0,63}", value
    ):
        raise ForgeError("Name must use 1..64 letters, digits, underscores or hyphens")
    return value


def relative_path(root, value):
    if not isinstance(value, str):
        raise ForgeError("Expected a relative path", code='invalid_request')
    path = PurePosixPath(value)
    if (
        not path.parts
        or path.is_absolute()
        or str(path) != value
        or any(p in {".", ".."} for p in path.parts)
        or "\\" in value
    ):
        raise ForgeError("Path must stay within its declared root", code='invalid_request')
    result = Path(root).absolute().joinpath(*path.parts)
    no_symlinks(result)
    return result


def decode_control(raw):
    def unique_fields(pairs):
        value = {}
        for key, item in pairs:
            if key in value:
                raise ForgeError('Duplicate control field: ' + key[:128], code='invalid_request')
            value[key] = item
        return value
    def finite(value):
        number = float(value)
        if not math.isfinite(number):
            raise ValueError('Nonfinite JSON number')
        return number
    try:
        return json.loads(raw, object_pairs_hook=unique_fields, parse_float=finite, parse_constant=finite)
    except ForgeError:
        raise
    except (ValueError, RecursionError) as exc:
        raise ForgeError(f'Invalid control JSON: {exc}', code='invalid_request') from exc


def read_control(path, maximum=1024**2):
    try:
        return decode_control(read_bounded(Path(path), maximum))
    except FileNotFoundError as exc:
        raise ForgeError(f'Control file not found: {Path(path).name}', code='not_found') from exc


@contextmanager
def control_lock(root):
    with directory_handle(Path(root) / '.forge') as parent:
        fd = os.open('control.lock', os.O_CREAT | os.O_RDWR | os.O_NOFOLLOW | os.O_NONBLOCK, 0o600, dir_fd=parent)
        try:
            if not stat.S_ISREG(os.fstat(fd).st_mode):
                raise ForgeError('Project lock must be a regular file', code='conflict')
            try:
                fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
            except BlockingIOError as exc:
                raise ForgeError('Workspace is being updated; retry', code='busy') from exc
            yield
        finally:
            os.close(fd)


def write_control(path, value, *, expected=None, create=False):
    path = no_symlinks(Path(path))
    raw = bounded_json(value, 8 * 1024**2)
    if expected is None:
        publish_bytes(path, raw)
        return
    if create:
        raise ForgeError('Cannot create with a previous control digest', code='conflict')
    try:
        current = read_control(path, 8 * 1024**2)
    except ForgeError as exc:
        if exc.code == 'not_found':
            raise ForgeError('Expected control no longer exists', code='conflict') from exc
        raise
    if content_digest(current) != expected:
        raise ForgeError('Control changed; refresh before replacing', code='conflict')
    publish_bytes(path, raw, create=False)
