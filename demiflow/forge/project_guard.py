"""Forge protected-file restoration, with explicit trusted control-write provenance."""
import os
import base64
from contextlib import contextmanager
from contextvars import ContextVar
from pathlib import Path
import stat

from .io import no_symlinks, publish_bytes
from .platform import bounded_json
from .platform_environment import read_bounded
from .project_io import read_control
from .schema import ForgeError

CONTROL_ENV = 'DEMIFLOW_FORGE_CONTROL_WRITES'
_CONTROL_FILES = ('workspace.json', '.forge/state.json')


_control_publisher = ContextVar('forge_control_publisher', default=None)


@contextmanager
def guarded_control_writes(root, snapshot, authorize):
    """Bind a trusted caller's protected snapshot to exact publications in this operation."""
    root = Path(root)
    def verify():
        authorize()
        for name in _CONTROL_FILES:
            path = no_symlinks(root / name)
            current = read_bounded(path, 8 * 1024**2) if path.exists() else None
            previous = snapshot.get(name)
            if current != (previous[0] if previous else None):
                raise ForgeError('Protected control changed outside the authorized operation', code='inputs_changed')

    def publish(path, raw, *, create):
        name = Path(path).relative_to(root).as_posix()
        if name not in _CONTROL_FILES:
            raise ForgeError('Unexpected control publication', code='conflict')
        verify()
        publish_bytes(path, raw, create=create)
        snapshot[name] = (raw, 0o600)

    verify()
    token = _control_publisher.set(publish)
    try:
        yield
    finally:
        _control_publisher.reset(token)


def prepare_control_writes(root, path, nonce):
    """Record the original control bytes in the current private author interval."""
    from .development_state import physical_root
    files = {}
    for name in _CONTROL_FILES:
        target = no_symlinks(Path(root) / name)
        raw = read_bounded(target, 1024**2) if target.exists() else None
        files[name] = dict(source=base64.b64encode(raw).decode() if raw is not None else None,
                           mode=stat.S_IMODE(target.stat().st_mode) if raw is not None else 0o600)
    record = dict(root=physical_root(root), nonce=nonce, files=files, authorized_paths=[])
    publish_bytes(path, bounded_json(record, 4 * 1024**2), create=not path.exists())


def record_authorized_tree_write(root, rel_path: str) -> None:
    """Record an authorized tree mutation (e.g. `forge dep vendor/remove` on `packages/<name>`) for trusted local CLI."""
    location = os.environ.get(CONTROL_ENV)
    if not location:
        return
    from .development_state import physical_root
    record_path = no_symlinks(Path(location))
    if not record_path.is_file():
        return
    record = read_control(record_path, 4 * 1024**2)
    if physical_root(root) != record['root']:
        return
    rel = str(rel_path).strip('/')
    if not rel:
        return
    authorized = list(record.get('authorized_paths') or ())
    if rel not in authorized:
        authorized.append(rel)
    record['authorized_paths'] = authorized
    publish_bytes(record_path, bounded_json(record, 4 * 1024**2), create=False)


def publish_project_control(path, raw, *, create=False):
    """Persist exact authorized bytes before a native Forge control publication.

    This is cooperative provenance for trusted local CLIs, not a same-UID
    security boundary. Restricted hosts retain their own trusted control bridge.
    """
    publisher = _control_publisher.get()
    if publisher is not None:
        return publisher(path, raw, create=create)
    location = os.environ.get(CONTROL_ENV)
    if not location:
        return publish_bytes(path, raw, create=create)
    from .development_state import physical_root
    record_path = no_symlinks(Path(location))
    record = read_control(record_path, 4 * 1024**2)
    root = Path(record['root']['path'])
    if physical_root(root) != record['root']:
        raise ForgeError('Control publication root changed', code='conflict')
    name = Path(path).relative_to(root).as_posix()
    if name not in _CONTROL_FILES:
        raise ForgeError('Unexpected protected control publication', code='conflict')
    previous = record['files'][name]
    if previous.get('pending'):
        raise ForgeError('Previous control publication needs recovery', code='conflict')
    expected = base64.b64decode(previous['source']) if previous['source'] is not None else None
    current = read_bounded(path, 1024**2) if Path(path).exists() else None
    if current != expected:
        raise ForgeError('Protected control changed outside a Forge operation', code='inputs_changed')
    planned = dict(source=base64.b64encode(raw).decode(), mode=previous['mode'], pending=previous)
    record['files'][name] = planned
    publish_bytes(record_path, bounded_json(record, 4 * 1024**2), create=False)
    publish_bytes(path, raw, create=create)
    planned.pop('pending')
    publish_bytes(record_path, bounded_json(record, 4 * 1024**2), create=False)


def apply_control_writes(root, path, nonce, snapshot):
    """Preserve the last authorized version, including a published pending write."""
    from .development_state import physical_root
    record = read_control(path, 4 * 1024**2)
    if record['root'] != physical_root(root) or record['nonce'] != nonce:
        raise ForgeError('Control provenance belongs to another author interval', code='conflict')
    for name, item in record['files'].items():
        if name not in _CONTROL_FILES:
            raise ForgeError('Unexpected protected control evidence', code='conflict')
        raw = base64.b64decode(item['source']) if item['source'] is not None else None
        if item.get('pending'):
            target = Path(root) / name
            current = read_bounded(target, 1024**2) if target.exists() else None
            previous = item['pending']
            before = base64.b64decode(previous['source']) if previous['source'] is not None else None
            if current == before:
                raw, item = before, previous  # Planned publication did not happen.
            elif current != raw:
                raise ForgeError('Control publication outcome is unknown; automatic continuation stopped', code='inspection_incomplete')
        if raw is not None:
            snapshot[name] = (raw, item['mode'])
    return list(record.get('authorized_paths') or ())

_PROTECTED_GUARD_STATIC = ('.env', '.env.local', '.forge/state.json', 'workspace.json', 'AGENTS.md', 'PIPELINE_SPEC.md')
_PROTECTED_GUARD_TREES = ('data_tools', 'packages')
_PROTECTED_GUARD_MAX_BYTES = 512 * 1024
_PROTECTED_TREE_MAX_ENTRIES = 2048
_PROTECTED_TREE_MAX_BYTES = 16 * 1024**2


class _ProtectedSnapshot(dict):
    """Pre-turn bytes of guarded files plus the exact file/dir listing of guarded trees."""

    def __init__(self):
        super().__init__()
        self.trees: dict[str, tuple[frozenset, frozenset]] = {}


def _tree_listing(root: Path, top: str):
    """Return (files, dirs) relative paths under a guarded tree, or None when it exceeds budget."""
    base = root / top
    files, dirs = set(), set()
    try:
        if base.is_symlink() or not base.is_dir():
            return frozenset(), frozenset()
    except OSError:
        return frozenset(), frozenset()
    pending = [base]
    while pending:
        current = pending.pop()
        try:
            with os.scandir(current) as entries:
                for entry in entries:
                    rel = Path(entry.path).relative_to(root).as_posix()
                    if entry.name == '__pycache__':
                        continue
                    if len(files) + len(dirs) >= _PROTECTED_TREE_MAX_ENTRIES:
                        return None
                    if entry.is_dir(follow_symlinks=False):
                        dirs.add(rel)
                        pending.append(Path(entry.path))
                    else:
                        files.add(rel)
        except OSError:
            continue
    return frozenset(files), frozenset(dirs)


def _authorized_by_host(rel: str, authorized) -> bool:
    """Host manage_project_layout writes are legitimate; single-segment role dirs authorize only themselves."""
    for item in authorized or ():
        if not isinstance(item, str) or not item:
            continue
        item = item.strip('/')
        if rel == item:
            return True
        if len(Path(item).parts) >= 2 and rel.startswith(item + '/'):
            return True
    return False


def _protected_guard_paths(root: Path) -> list[str]:
    """Return relative paths of project-internal secret/governance files guarded by Forge hooks."""
    paths = list(_PROTECTED_GUARD_STATIC)
    try:
        with os.scandir(root) as entries:
            for entry in entries:
                if (entry.name.startswith('.env.')
                        and entry.name not in {'.env.example', '.env.sample'}
                        and entry.name not in paths):
                    paths.append(entry.name)
    except OSError:
        pass
    return paths


def capture_protected_files(project_root) -> dict[str, tuple[bytes, int]]:
    """Snapshot protected files and guarded trees (data_tools/) before a developer turn (OS-independent Forge hook)."""
    root = Path(project_root)
    snap = _ProtectedSnapshot()

    def remember(rel):
        path = root / rel
        try:
            info = path.lstat()
        except OSError:
            return
        if not stat.S_ISREG(info.st_mode) or info.st_size > (8 * 1024**2 if rel in _CONTROL_FILES else _PROTECTED_GUARD_MAX_BYTES):
            return
        try:
            snap[rel] = (path.read_bytes(), info.st_mode & 0o777)
        except OSError:
            return

    for rel in _protected_guard_paths(root):
        remember(rel)
    for top in _PROTECTED_GUARD_TREES:
        listing = _tree_listing(root, top)
        if listing is None:
            continue
        files, dirs = listing
        total = 0
        for rel in sorted(files):
            try:
                total += (root / rel).lstat().st_size
            except OSError:
                continue
            if total > _PROTECTED_TREE_MAX_BYTES:
                break
            remember(rel)
        snap.trees[top] = (files, dirs, (root / top).is_dir() and not (root / top).is_symlink())
    return snap


def enforce_protected_files(project_root, before_snapshot: dict[str, tuple[bytes, int]], authorized=()) -> list[str]:
    """Detect and restore unauthorized mutation/deletion/creation of protected files after a developer turn.

    Paths written by host ``manage_project_layout`` actions (``authorized``) are legitimate and kept.
    """
    trees = getattr(before_snapshot, 'trees', {}) or {}
    if not before_snapshot and not trees:
        return []
    root = Path(project_root)
    violations: list[str] = []
    for top, (old_files, old_dirs, existed) in sorted(trees.items()):
        listing = _tree_listing(root, top)
        if listing is None:
            continue
        files, dirs = listing
        for rel in sorted(files - old_files):
            if _authorized_by_host(rel, authorized):
                continue
            violations.append(rel)
            try:
                (root / rel).unlink()
            except OSError:
                pass
        for rel in sorted(dirs - old_dirs, key=lambda item: -len(Path(item).parts)):
            if _authorized_by_host(rel, authorized):
                continue
            try:
                (root / rel).rmdir()
            except OSError:
                pass
        if not existed and not _authorized_by_host(top, authorized):
            try:
                (root / top).rmdir()
            except OSError:
                pass
    for rel, (orig_bytes, orig_mode) in sorted(before_snapshot.items()):
        if _authorized_by_host(rel, authorized):
            continue
        path = root / rel
        mutated = False
        try:
            info = path.lstat()
            if not stat.S_ISREG(info.st_mode):
                mutated = True
            elif path.read_bytes() != orig_bytes:
                mutated = True
        except OSError:
            mutated = True
        if not mutated:
            continue
        violations.append(rel)
        try:
            if path.is_symlink():
                path.unlink()
            path.parent.mkdir(parents=True, exist_ok=True)
            tmp = path.with_name(f'.{path.name}.forge-restore-{os.getpid()}')
            tmp.write_bytes(orig_bytes)
            try:
                os.chmod(tmp, orig_mode)
            except OSError:
                pass
            os.replace(tmp, path)
        except OSError:
            pass
    return violations
