"""Durable engineering receipts and cooperative writer admission shared by CLI and hosts.

This is an author-interval fence, not a Pipeline resource or run registry. The
owner supplies its existing unit identity and retires only after durable cleanup.
"""
from contextlib import contextmanager
import fcntl
import hashlib
import os
from pathlib import Path
import stat

from .io import ensure_directory, no_symlinks, publish_bytes
from .platform import content_digest, bounded_json
from .project_io import read_control, write_control
from .schema import ForgeError


def physical_root(root):
    root = no_symlinks(Path(root).absolute())
    if root != root.resolve(strict=True) or not root.is_dir():
        raise ForgeError('Development needs a physical project root', code='conflict')
    info = root.stat()
    return dict(path=str(root), device=info.st_dev, inode=info.st_ino)


def _private(directory):
    ensure_directory(directory)
    info = directory.stat()
    if info.st_uid != os.geteuid() or stat.S_IMODE(info.st_mode) & 0o077:
        raise ForgeError('Engineering state must be private to the current UID', code='conflict')
    return directory


def writer_domain():
    """Fixed same-UID domain, deliberately independent of TMPDIR and task state."""
    return _private(Path('/tmp') / ('demiflow-forge-writers-' + str(os.geteuid())))


@contextmanager
def _locked(directory, name):
    fd = os.open(directory / name, os.O_CREAT | os.O_RDWR | os.O_NOFOLLOW | os.O_NONBLOCK, 0o600)
    try:
        info = os.fstat(fd)
        if not stat.S_ISREG(info.st_mode) or info.st_nlink != 1 or info.st_uid != os.geteuid():
            raise ForgeError('Invalid engineering lock', code='conflict')
        try:
            fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as exc:
            raise ForgeError('Engineering operation is busy', code='busy') from exc
        yield
    finally:
        os.close(fd)


@contextmanager
def _writers():
    directory = writer_domain()
    with _locked(directory, 'admission.lock'):
        index = directory / 'writers.json'
        rows = read_control(index, 2 * 1024**2) if index.exists() else {}
        if not isinstance(rows, dict) or len(rows) > 512:
            raise ForgeError('Writer admission state exceeds its bound', code='resource_limit')
        yield rows
        publish_bytes(index, bounded_json(rows, 2 * 1024**2), create=not index.exists())


def writer_key(owner):
    """Stable owner key, saved by the host before publishing admission."""
    if not isinstance(owner, str) or not 1 <= len(owner.encode()) <= 512:
        raise ForgeError('Invalid engineering owner', code='invalid_request')
    return hashlib.sha256(owner.encode()).hexdigest()


def claim_writer(root, owner, unit_path, unit_identity):
    """Reserve a physical root before spawn or a warm-connection handoff."""
    identity = physical_root(root)
    key = writer_key(owner)
    unit = no_symlinks(Path(unit_path).absolute())
    info = unit.stat()
    if (not stat.S_ISREG(info.st_mode) or info.st_nlink != 1
            or unit_identity != dict(device=info.st_dev, inode=info.st_ino)):
        raise ForgeError('Writer unit identity changed', code='conflict')
    with _writers() as rows:
        for saved_key, row in rows.items():
            try:
                unchanged = physical_root(row['root']['path']) == row['root']
            except (OSError, ValueError):
                unchanged = False
            if not unchanged:
                raise ForgeError('An active project identity changed; its owner must finish recovery', code='busy')
            if saved_key == key:
                if (row['root'], row['unit'], row['unit_identity']) != (identity, str(unit), unit_identity):
                    raise ForgeError('Writer owner was rebound', code='conflict')
                return key
            other = Path(row['root']['path'])
            path = Path(identity['path'])
            if ((row['root']['device'], row['root']['inode']) == (identity['device'], identity['inode'])
                    or path == other or path in other.parents or other in path.parents):
                raise ForgeError('This physical project has an active or unresolved author', code='busy')
        if len(rows) >= 512:
            raise ForgeError('Writer admission records require cleanup', code='resource_limit')
        rows[key] = dict(root=identity, unit=str(unit), unit_identity=unit_identity, phase='admitted')
    return key


def start_writer(key):
    with _writers() as rows:
        if key not in rows:
            raise ForgeError('Writer admission is missing', code='conflict')
        if physical_root(rows[key]['root']['path']) != rows[key]['root']:
            raise ForgeError('Project identity changed before author dispatch', code='conflict')
        rows[key]['phase'] = 'spawning'


def retire_writer(key, *, terminal, cleanup):
    """The original host calls this only after committing its terminal/drain receipt."""
    if not terminal or cleanup != 'complete':
        return False
    with _writers() as rows:
        rows.pop(key, None)
    return True


class DevelopmentState:
    """One private CLI operation with atomic receipts and a stable original session."""

    def __init__(self, directory, root):
        self.directory = _private(no_symlinks(Path(directory).absolute()))
        self.root = physical_root(root)
        if self.directory == Path(root) or Path(root) in self.directory.parents:
            raise ForgeError('Engineering receipts must stay outside author source', code='invalid_request')
        self.path = self.directory / 'receipt.json'

    @contextmanager
    def lock(self):
        with _locked(self.directory, 'operation.lock'):
            yield self

    def load(self):
        value = read_control(self.path, 8 * 1024**2)
        if value.get('root') != self.root or value.get('format') != 'demiflow_forge_development_state_v1':
            raise ForgeError('Engineering receipt belongs to another project or format', code='conflict')
        return value

    def save(self, value):
        if value.get('root') != self.root:
            raise ForgeError('Engineering project identity changed', code='conflict')
        expected = content_digest(read_control(self.path, 8 * 1024**2)) if self.path.exists() else None
        write_control(self.path, value, expected=expected, create=expected is None)
