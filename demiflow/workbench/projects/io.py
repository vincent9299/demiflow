"""Project control I/O shared by domain owners; no engineering policy."""
from contextlib import contextmanager
import fcntl
import hashlib
import json
import os
from pathlib import Path
from pathlib import PurePosixPath
import re
import tempfile
from demiflow.forge.io import no_symlinks
from demiflow.forge.platform import bounded_json
from demiflow.forge.platform import content_digest
from demiflow.forge.platform_environment import read_bounded
from demiflow.forge.schema import ForgeError

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


def read_control(path, maximum=1024**2):
    def unique_fields(pairs):
        value = {}
        for key, item in pairs:
            if key in value:
                raise ForgeError('Duplicate control field: ' + key[:128], code='invalid_request')
            value[key] = item
        return value
    try:
        return json.loads(read_bounded(Path(path), maximum), object_pairs_hook=unique_fields)
    except ForgeError:
        raise
    except FileNotFoundError as exc:
        raise ForgeError(f'Control file not found: {Path(path).name}', code='not_found') from exc
    except (ValueError, OSError, RecursionError) as exc:
        raise ForgeError(f"Invalid control file {Path(path).name}: {exc}") from exc


@contextmanager
def control_lock(root):
    path = Path(root) / ".forge" / "control.lock"
    no_symlinks(path)
    fd = os.open(path, os.O_CREAT | os.O_RDWR | os.O_NOFOLLOW, 0o600)
    try:
        fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        yield
    except BlockingIOError as exc:
        raise ForgeError("Workspace is being updated; retry", code='busy') from exc
    finally:
        os.close(fd)


def write_control(path, value, *, expected=None, create=False):
    path = Path(path)
    no_symlinks(path)
    raw = bounded_json(value, 8 * 1024**2)
    if path.exists():
        if create:
            raise ForgeError("Control already exists", code='conflict')
        if (
            expected is None
            or content_digest(read_control(path, 8 * 1024**2)) != expected
        ):
            raise ForgeError("Control changed; refresh before replacing", code='conflict')
    elif expected is not None:
        raise ForgeError("Expected control no longer exists", code='conflict')
    fd, temp = tempfile.mkstemp(prefix=".forge-write-", dir=path.parent)
    try:
        with os.fdopen(fd, "wb") as out:
            out.write(raw)
            out.flush()
            os.fsync(out.fileno())
        no_symlinks(path)
        if create:
            try:
                os.link(temp, path)
            except FileExistsError as exc:
                raise ForgeError('Control already exists', code='conflict') from exc
        else:
            os.replace(temp, path)
    finally:
        Path(temp).unlink(missing_ok=True)
        from demiflow.forge.io import fsync_directory
        fsync_directory(path.parent)



def workspace_id(root, value):
    # Legacy IDs are derived without modifying historic workspace controls.
    return value.get('id', 'l' + hashlib.sha256(str(Path(root).absolute()).encode()).hexdigest()[:32])
