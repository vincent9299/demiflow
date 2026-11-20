"""Rebuildable run/report directory, never execution or data-write authority.

Control writers create a bounded pending marker before publishing source facts,
then update this derived index and clear the exact marker. Failed cache writes
leave the source facts and marker intact. Reads do not repair or migrate state.
"""
from contextlib import contextmanager
import base64
import json
import math
import os
from pathlib import Path
import re
import sqlite3
import time
import uuid

from demiflow.workbench.projects import io as ws
from demiflow.forge.schema import ForgeError

FORMAT = 'demiflow_forge_index_v1'
MAX_ENTRIES = 1000000
MAX_PENDING = 256
MAX_BYTES = 256 * 1024**2


def _identity(kind, identity):
    pattern = r'r[0-9a-f]{24}' if kind == 'run' else r'[0-9a-f]{64}' if kind == 'report' else None
    if pattern is None or type(identity) is not str or not re.fullmatch(pattern, identity):
        raise ForgeError('Invalid directory index identity')


def _path(root):
    return ws.relative_path(root, '.forge/index.sqlite')


def _pending(root):
    path = ws.relative_path(root, '.forge/index-pending')
    if not path.exists():
        return []
    result = []
    with os.scandir(path) as entries:
        for entry in entries:
            if len(result) >= MAX_PENDING or entry.is_symlink() or not entry.is_file(follow_symlinks=False):
                raise ForgeError('Directory index pending store exceeds its bound or contains an invalid file')
            result.append(Path(entry.path))
    return sorted(result)


@contextmanager
def _connect(path, *, write=False, create=False):
    ws.no_symlinks(path)
    for suffix in ('-journal', '-wal', '-shm'):
        ws.no_symlinks(path.with_name(path.name + suffix))
    if path.exists() and path.stat().st_size > MAX_BYTES:
        raise ForgeError('Directory index exceeds disk budget')
    mode = 'rwc' if create else 'rw' if write else 'ro'
    db = sqlite3.connect(path.as_uri() + '?mode=' + mode, uri=True, timeout=1)
    try:
        db.execute('PRAGMA cache_size=-2048')
        db.execute('PRAGMA mmap_size=0')
        db.execute('PRAGMA temp_store=FILE')
        if write or create:
            db.execute('PRAGMA journal_mode=DELETE')
            db.execute('PRAGMA synchronous=FULL')
            page = db.execute('PRAGMA page_size').fetchone()[0]
            db.execute('PRAGMA max_page_count=' + str(MAX_BYTES // page))
        yield db
    finally:
        db.close()


def _create(path, *, complete):
    with _connect(path, create=True) as db:
        db.executescript('''CREATE TABLE metadata (id INTEGER PRIMARY KEY CHECK(id=1), format TEXT NOT NULL,
            epoch TEXT NOT NULL, revision INTEGER NOT NULL, complete INTEGER NOT NULL,
            runs INTEGER NOT NULL, reports INTEGER NOT NULL);
            CREATE TABLE entries (kind TEXT NOT NULL, identity TEXT NOT NULL, created REAL NOT NULL,
                digest TEXT NOT NULL, active INTEGER NOT NULL, PRIMARY KEY(kind,identity)) WITHOUT ROWID;
            CREATE INDEX entries_page ON entries(kind,created DESC,identity DESC);
            CREATE INDEX entries_active ON entries(active,kind,identity);''')
        db.execute('INSERT INTO metadata VALUES(1,?,?,0,?,0,0)', (FORMAT, uuid.uuid4().hex, int(complete)))
        db.commit()


def _metadata(db):
    row = db.execute('SELECT format,epoch,revision,complete,runs,reports FROM metadata WHERE id=1').fetchone()
    if not row or row[0] != FORMAT or not re.fullmatch('[0-9a-f]{32}', row[1]) or any(type(v) is not int or v < 0 for v in row[2:]):
        raise ForgeError('Invalid directory index metadata')
    if row[4] + row[5] > MAX_ENTRIES or row[3] not in (0, 1):
        raise ForgeError('Directory index count exceeds budget')
    return dict(format=row[0], epoch=row[1], revision=row[2], complete=bool(row[3]), runs=row[4], reports=row[5])


def _ensure(root, *, complete=False):
    """Writers only. Only a new workspace can attest an empty source set."""
    path = _path(root)
    if path.exists():
        return
    temp = path.with_name('.index-' + uuid.uuid4().hex + '.sqlite')
    try:
        _create(temp, complete=complete)
        os.replace(temp, path)
    finally:
        temp.unlink(missing_ok=True)
        temp.with_name(temp.name + '-journal').unlink(missing_ok=True)


def initialize_empty(root):
    """The caller exclusively created the workspace control directory."""
    _ensure(root, complete=True)


def begin(root, kind, identity):
    """Before source mutation, under the caller's workspace control lock."""
    _identity(kind, identity)
    _ensure(root)
    markers = _pending(root)
    path = ws.relative_path(root, '.forge/index-pending/' + kind + '-' + identity + '.json')
    if path in markers:
        raise ForgeError('This directory entry has a pending update; run demiflow workbench index ROOT --synchronize first')
    if len(markers) >= MAX_PENDING:
        raise ForgeError('Directory index pending budget is full; run demiflow workbench index ROOT --synchronize before more control writes')
    with _connect(_path(root)) as db:
        metadata = _metadata(db)
        exists = db.execute('SELECT 1 FROM entries WHERE kind=? AND identity=?', (kind, identity)).fetchone()
        if not exists and metadata['runs'] + metadata['reports'] >= MAX_ENTRIES:
            raise ForgeError('Directory index entry budget exceeded')
    path.parent.mkdir(exist_ok=True)
    marker = dict(format=FORMAT, kind=kind, identity=identity, generation=uuid.uuid4().hex)
    ws.write_control(path, marker, create=True)
    return marker


def _entry(value, marker=None):
    """Validate owner-supplied data, without reading or interpreting its source."""
    if type(value) is not dict or set(value) != {'kind', 'identity', 'created_at', 'digest', 'active'}:
        raise ForgeError('Invalid directory entry')
    _identity(value['kind'], value['identity'])
    created = value['created_at']
    if type(created) not in (int, float) or not math.isfinite(created) or created < 0:
        raise ForgeError('Directory source has an invalid creation timestamp')
    if type(value['digest']) is not str or not re.fullmatch('[0-9a-f]{64}', value['digest']) or type(value['active']) is not bool:
        raise ForgeError('Invalid directory source digest or activity')
    if marker is not None and (value['kind'], value['identity']) != (marker['kind'], marker['identity']):
        raise ForgeError('Directory entry does not match its marker')
    return value['kind'], value['identity'], float(created), value['digest'], int(value['active'])


def _marker(marker):
    if (type(marker) is not dict or set(marker) != {'format', 'kind', 'identity', 'generation'}
            or marker['format'] != FORMAT or type(marker['generation']) is not str
            or not re.fullmatch('[0-9a-f]{32}', marker['generation'])):
        raise ForgeError('Invalid pending marker')
    _identity(marker['kind'], marker['identity'])


def pending_markers(root):
    """Bounded storage records/errors for explicit synchronization under control_lock."""
    for path in _pending(root):
        try:
            marker = ws.read_control(path, 4096)
            _marker(marker)
            if path.name != marker['kind'] + '-' + marker['identity'] + '.json':
                raise ForgeError('Pending marker filename differs')
        except Exception as exc:
            yield path.name, None, str(exc)[:1024]
        else:
            yield path.name, marker, None


def _insert(db, value):
    previous = db.execute('SELECT created,digest,active FROM entries WHERE kind=? AND identity=?', value[:2]).fetchone()
    if previous and previous[1:] == value[3:]:
        return
    if previous and previous[1] != value[3]:
        raise ForgeError('Immutable directory source identity changed')
    db.execute('INSERT INTO entries VALUES(?,?,?,?,?) ON CONFLICT(kind,identity) DO UPDATE SET active=excluded.active', value)
    column = 'runs' if value[0] == 'run' else 'reports'
    db.execute('UPDATE metadata SET revision=revision+1,' + column + '=' + column + '+? WHERE id=1', (int(previous is None),))
    _metadata(db)


def publish(root, marker, entry):
    """After source publication, under control_lock. Cache failure is reported.

    The writer must not roll back an already published source because this
    returns an error. The exact marker remains for explicit recovery.
    """
    try:
        _marker(marker)
        value = None if entry is None else _entry(entry, marker)
        path = ws.relative_path(root, '.forge/index-pending/' + marker['kind'] + '-' + marker['identity'] + '.json')
        if ws.read_control(path, 4096) != marker:
            raise ForgeError('Directory update marker changed')
        with _connect(_path(root), write=True) as db:
            if value is not None:
                _insert(db, value)
                # State/results can change without changing membership or the
                # cached active flag. This revision is only a refresh hint.
                db.execute('UPDATE metadata SET revision=revision+1 WHERE id=1')
            db.commit()
        # control_lock serializes marker writers; compare again before removal.
        if ws.read_control(path, 4096) != marker:
            raise ForgeError('Directory update marker changed before removal')
        path.unlink()
        return None
    except Exception as error:
        return str(error)[:1024]





def status(root):
    try:
        pending = _pending(root)
        if not _path(root).exists():
            return dict(format=FORMAT, complete=False, revision=None, pending=len(pending), reason='index_not_initialized')
        with _connect(_path(root)) as db:
            result = _metadata(db)
            active = db.execute('SELECT identity FROM entries WHERE active=1 AND kind=? LIMIT ?', ('run', MAX_PENDING + 1)).fetchall()
            if len(active) > MAX_PENDING:
                raise ForgeError('Active directory set exceeds budget; rebuild after reconciliation')
        return {**result, 'complete': result['complete'] and not pending, 'pending': len(pending),
                'active_runs': [row[0] for row in active]}
    except (ValueError, OSError, sqlite3.Error) as error:
        return dict(format=FORMAT, complete=False, revision=None, reason='index_unavailable', detail=str(error)[:1024])


def page(root, kind, *, limit=20, offset=0, cursor=None):
    if kind not in {'run', 'report'} or type(limit) is not int or not 1 <= limit <= 100 or type(offset) is not int or not 0 <= offset <= MAX_ENTRIES:
        raise ForgeError('Invalid directory page')
    details = status(root)
    if details.get('revision') is None:
        raise ForgeError('Directory index is unavailable; explicitly rebuild it')
    last = None
    if cursor is not None:
        if offset or type(cursor) is not str or len(cursor) > 1024:
            raise ForgeError('Invalid directory cursor')
        try:
            value = json.loads(base64.urlsafe_b64decode(cursor))
            if set(value) != {'epoch', 'kind', 'created', 'identity'} or value['epoch'] != details['epoch'] or value['kind'] != kind:
                raise ValueError('Cursor belongs to a different directory generation')
            _identity(kind, value['identity'])
            if type(value['created']) not in (int, float) or not math.isfinite(value['created']):
                raise ValueError('Invalid cursor timestamp')
            last = (value['created'], value['identity'])
        except (ValueError, TypeError, KeyError) as error:
            raise ForgeError('Invalid directory cursor: ' + str(error)) from error
    with _connect(_path(root)) as db:
        db.execute('BEGIN')
        current = _metadata(db)
        if current['epoch'] != details['epoch']:
            raise ForgeError('Directory index was rebuilt while paging; restart from its first page')
        details.update(current)
        details['complete'] = current['complete'] and not details.get('pending')
        condition, args = 'kind=?', [kind]
        if last:
            condition += ' AND (created<? OR created=? AND identity<?)'
            args.extend([last[0], last[0], last[1]])
        rows = db.execute('SELECT identity,created,digest FROM entries WHERE ' + condition + ' ORDER BY created DESC,identity DESC LIMIT ? OFFSET ?', (*args, limit + 1, offset)).fetchall()
    next_cursor = None
    if len(rows) > limit:
        identity, created, _ = rows[limit - 1]
        next_cursor = base64.urlsafe_b64encode(json.dumps(dict(epoch=details['epoch'],kind=kind,created=created,identity=identity), separators=(',', ':')).encode()).decode()
    return dict(entries=[dict(identity=r[0],created_at=r[1],digest=r[2]) for r in rows[:limit]],
                total=details['runs' if kind == 'run' else 'reports'], next_cursor=next_cursor, index=details)


def rebuild_from_entries(root, entries, *, max_entries, deadline):
    """Stream owner entries into a replacement; caller holds control_lock.

    Failure after replace may leave the new database with pending markers.
    Neither the old nor new database is an execution authority.
    """
    if type(max_entries) is not int or not 1 <= max_entries <= MAX_ENTRIES:
        raise ForgeError('Invalid index rebuild budget')
    target = _path(root)
    temp = target.with_name('.index-' + uuid.uuid4().hex + '.sqlite')
    def check():
        if time.monotonic() >= deadline:
            raise ForgeError('Index rebuild exceeded its entry/time budget')
    try:
        check()
        _create(temp, complete=True)
        with _connect(temp, write=True) as db:
            for count, value in enumerate(entries, 1):
                check()
                if count > max_entries:
                    raise ForgeError('Index rebuild exceeded its entry/time budget')
                if value is None:
                    raise ForgeError('Directory source is incomplete')
                _insert(db, _entry(value))
                if count % 100 == 0:
                    db.commit()
            check()
            db.commit()
        check()
        os.replace(temp, target)
        for path in _pending(root):
            path.unlink()
    finally:
        temp.unlink(missing_ok=True)
        temp.with_name(temp.name + '-journal').unlink(missing_ok=True)
    return status(root)
