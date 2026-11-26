"""Bounded SQLite ledger, manifest storage, and kernel-lock liveness for workflow runs."""
from __future__ import annotations

from contextlib import contextmanager
import fcntl
import json
import os
from pathlib import Path
import re
import sqlite3
import time
import uuid

from demiflow.forge.io import atomic_write_json, ensure_directory, no_symlinks
from demiflow.forge.platform import bounded_json, content_digest
from demiflow.forge.platform_environment import read_bounded
from demiflow.forge.schema import ForgeError

MANIFEST_FORMAT = 'demiflow_workflow_run_v1'
WORKFLOW_RUN_ID_RE = re.compile(r'^wf[0-9a-f]{24}$')
MAX_MANIFEST_BYTES = 2 * 1024 * 1024
MAX_WORKFLOW_RUNS_SCAN = 256
MAX_WORKFLOW_ATTEMPTS = 32
MAX_EVENTS_PAGE = 200

ACTIVE_WORKFLOW_STATUSES = frozenset({'starting', 'running', 'stopping'})
TERMINAL_WORKFLOW_STATUSES = frozenset({'succeeded', 'failed', 'cancelled', 'interrupted'})
ACTIVE_NODE_STATUSES = frozenset({'preparing', 'running', 'waiting_project', 'pending_resume'})
TERMINAL_NODE_STATUSES = frozenset({'succeeded', 'reused', 'failed', 'skipped', 'cancelled', 'interrupted'})

_SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS workflow_state (
    id INTEGER PRIMARY KEY CHECK (id = 1),
    wf_run_id TEXT NOT NULL,
    current_attempt INTEGER NOT NULL,
    status TEXT NOT NULL,
    stop_requested INTEGER NOT NULL DEFAULT 0,
    created_at REAL NOT NULL,
    updated_at REAL NOT NULL,
    finished_at REAL,
    error TEXT
);
CREATE TABLE IF NOT EXISTS wf_attempts (
    attempt INTEGER PRIMARY KEY,
    status TEXT NOT NULL,
    from_node TEXT,
    orchestrator_json TEXT,
    started_at REAL NOT NULL,
    updated_at REAL NOT NULL,
    finished_at REAL,
    error TEXT
);
CREATE TABLE IF NOT EXISTS node_runs (
    attempt INTEGER NOT NULL,
    node_id TEXT NOT NULL,
    status TEXT NOT NULL,
    project_alias TEXT NOT NULL,
    project_root TEXT NOT NULL,
    project_workspace_id TEXT,
    entry TEXT NOT NULL,
    params TEXT,
    child_run_id TEXT,
    child_attempt INTEGER,
    resolved_overrides_json TEXT,
    bindings_json TEXT,
    artifacts_json TEXT,
    error TEXT,
    started_at REAL,
    finished_at REAL,
    updated_at REAL NOT NULL,
    PRIMARY KEY (attempt, node_id)
);
CREATE TABLE IF NOT EXISTS workflow_events (
    seq INTEGER PRIMARY KEY AUTOINCREMENT,
    attempt INTEGER NOT NULL,
    ts REAL NOT NULL,
    kind TEXT NOT NULL,
    node_id TEXT,
    detail_json TEXT
);
CREATE INDEX IF NOT EXISTS idx_wf_events_attempt_seq ON workflow_events(attempt, seq);
"""


def new_workflow_run_id() -> str:
    return 'wf' + uuid.uuid4().hex[:24]


def validate_workflow_run_id(wf_run_id: str) -> str:
    if type(wf_run_id) is not str or not WORKFLOW_RUN_ID_RE.fullmatch(wf_run_id):
        raise ForgeError('Invalid workflow run ID', code='invalid_request')
    return wf_run_id


def runs_root(host_root: Path | str) -> Path:
    root = Path(host_root).absolute()
    no_symlinks(root)
    return root / '.forge' / 'workflow-runs'


def run_directory(host_root: Path | str, wf_run_id: str) -> Path:
    validate_workflow_run_id(wf_run_id)
    base = runs_root(host_root)
    target = base / wf_run_id
    no_symlinks(target)
    return target


def manifest_path(run_dir: Path) -> Path:
    return run_dir / 'manifest.json'


def ledger_path(run_dir: Path) -> Path:
    return run_dir / 'workflow.sqlite'


def lock_path(run_dir: Path) -> Path:
    return run_dir / 'orchestrator.lock'


def log_path(run_dir: Path, attempt: int) -> Path:
    return run_dir / f'attempt-{attempt}.log'


@contextmanager
def hold_orchestrator_lock(run_dir: Path):
    """Acquire non-blocking exclusive kernel flock for the orchestrator process lifetime."""
    path = no_symlinks(lock_path(run_dir))
    fd = os.open(path, os.O_CREAT | os.O_RDWR | os.O_NOFOLLOW | os.O_CLOEXEC, 0o600)
    try:
        try:
            fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as exc:
            raise ForgeError('Workflow orchestrator is already running', code='busy') from exc
        yield fd
    finally:
        try:
            fcntl.flock(fd, fcntl.LOCK_UN)
        except OSError:
            pass
        os.close(fd)


def is_orchestrator_alive(run_dir: Path) -> bool:
    """Kernel-authoritative liveness check via non-blocking flock probe (immune to PID reuse)."""
    path = lock_path(run_dir)
    if not path.exists() or path.is_symlink():
        return False
    try:
        fd = os.open(path, os.O_RDWR | os.O_NOFOLLOW | os.O_CLOEXEC)
    except OSError:
        return False
    try:
        try:
            fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            return True
        fcntl.flock(fd, fcntl.LOCK_UN)
        return False
    finally:
        os.close(fd)


def _connect(run_dir: Path, *, readonly: bool = False) -> sqlite3.Connection:
    db_file = ledger_path(run_dir)
    no_symlinks(db_file)
    if readonly:
        if not db_file.is_file():
            raise ForgeError('Workflow ledger not found', code='not_found')
        conn = sqlite3.connect(db_file.resolve().as_uri() + '?mode=ro', uri=True, timeout=2.0)
    else:
        conn = sqlite3.connect(str(db_file), timeout=5.0)
        conn.execute('PRAGMA journal_mode=WAL')
        conn.execute('PRAGMA synchronous=NORMAL')
    conn.row_factory = sqlite3.Row
    conn.execute('PRAGMA busy_timeout=5000')
    return conn


def write_manifest(run_dir: Path, manifest: dict) -> dict:
    ensure_directory(run_dir)
    body = {k: v for k, v in manifest.items() if k != 'digest'}
    body['digest'] = content_digest(body)
    bounded_json(body, MAX_MANIFEST_BYTES)
    atomic_write_json(manifest_path(run_dir), body)
    return body


def read_manifest(run_dir: Path) -> dict:
    path = manifest_path(run_dir)
    no_symlinks(path)
    if not path.is_file():
        raise ForgeError('Workflow run manifest not found', code='not_found')
    raw = read_bounded(path, MAX_MANIFEST_BYTES)
    data = json.loads(raw.decode('utf-8'))
    if type(data) is not dict or data.get('format') != MANIFEST_FORMAT:
        raise ForgeError('Invalid workflow run manifest', code='conflict')
    expected = content_digest({k: v for k, v in data.items() if k != 'digest'})
    if data.get('digest') != expected:
        raise ForgeError('Workflow run manifest digest mismatch', code='conflict')
    return data


def initialize_ledger(run_dir: Path, *, wf_run_id: str, nodes_init: list[dict],
                      created_at: float | None = None) -> None:
    now = time.time() if created_at is None else created_at
    ensure_directory(run_dir)
    conn = _connect(run_dir, readonly=False)
    try:
        with conn:
            conn.executescript(_SCHEMA_SQL)
            conn.execute(
                'INSERT INTO workflow_state (id, wf_run_id, current_attempt, status, stop_requested, created_at, updated_at, finished_at, error) '
                'VALUES (1, ?, 1, ?, 0, ?, ?, NULL, NULL)',
                (wf_run_id, 'starting', now, now),
            )
            conn.execute(
                'INSERT INTO wf_attempts (attempt, status, from_node, orchestrator_json, started_at, updated_at, finished_at, error) '
                'VALUES (1, ?, NULL, NULL, ?, ?, NULL, NULL)',
                ('starting', now, now),
            )
            for node in nodes_init:
                conn.execute(
                    'INSERT INTO node_runs (attempt, node_id, status, project_alias, project_root, project_workspace_id, '
                    'entry, params, child_run_id, child_attempt, resolved_overrides_json, bindings_json, artifacts_json, '
                    'error, started_at, finished_at, updated_at) '
                    'VALUES (1, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)',
                    (
                        node['node_id'],
                        node['status'],
                        node['project_alias'],
                        node['project_root'],
                        node.get('project_workspace_id'),
                        node['entry'],
                        node.get('params'),
                        node.get('child_run_id'),
                        node.get('child_attempt'),
                        _dump_opt(node.get('resolved_overrides')),
                        _dump_opt(node.get('bindings')),
                        _dump_opt(node.get('artifacts')),
                        node.get('error'),
                        node.get('started_at'),
                        node.get('finished_at'),
                        now,
                    ),
                )
            conn.execute(
                'INSERT INTO workflow_events (attempt, ts, kind, node_id, detail_json) VALUES (1, ?, ?, NULL, ?)',
                (now, 'workflow.created', _dump_opt({'node_count': len(nodes_init)})),
            )
    finally:
        conn.close()


def _dump_opt(value) -> str | None:
    if value is None:
        return None
    return json.dumps(value, ensure_ascii=False, sort_keys=True)


def _load_opt(raw):
    if raw is None:
        return None
    try:
        return json.loads(raw)
    except (TypeError, json.JSONDecodeError):
        return None


def create_attempt(run_dir: Path, *, attempt: int, from_node: str | None,
                   nodes_init: list[dict]) -> None:
    if attempt < 2 or attempt > MAX_WORKFLOW_ATTEMPTS:
        raise ForgeError(f'Workflow attempt count exceeds limit ({MAX_WORKFLOW_ATTEMPTS})', code='resource_limit')
    now = time.time()
    conn = _connect(run_dir, readonly=False)
    try:
        with conn:
            conn.execute(
                'UPDATE workflow_state SET current_attempt=?, status=?, stop_requested=0, updated_at=?, finished_at=NULL, error=NULL WHERE id=1',
                (attempt, 'starting', now),
            )
            conn.execute(
                'INSERT INTO wf_attempts (attempt, status, from_node, orchestrator_json, started_at, updated_at, finished_at, error) '
                'VALUES (?, ?, ?, NULL, ?, ?, NULL, NULL)',
                (attempt, 'starting', from_node, now, now),
            )
            for node in nodes_init:
                conn.execute(
                    'INSERT INTO node_runs (attempt, node_id, status, project_alias, project_root, project_workspace_id, '
                    'entry, params, child_run_id, child_attempt, resolved_overrides_json, bindings_json, artifacts_json, '
                    'error, started_at, finished_at, updated_at) '
                    'VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)',
                    (
                        attempt,
                        node['node_id'],
                        node['status'],
                        node['project_alias'],
                        node['project_root'],
                        node.get('project_workspace_id'),
                        node['entry'],
                        node.get('params'),
                        node.get('child_run_id'),
                        node.get('child_attempt'),
                        _dump_opt(node.get('resolved_overrides')),
                        _dump_opt(node.get('bindings')),
                        _dump_opt(node.get('artifacts')),
                        node.get('error'),
                        node.get('started_at'),
                        node.get('finished_at'),
                        now,
                    ),
                )
            conn.execute(
                'INSERT INTO workflow_events (attempt, ts, kind, node_id, detail_json) VALUES (?, ?, ?, ?, ?)',
                (attempt, now, 'workflow.resumed', from_node, _dump_opt({'attempt': attempt, 'from_node': from_node})),
            )
    finally:
        conn.close()


def update_workflow_status(run_dir: Path, attempt: int, status: str, *,
                           orchestrator: dict | None = None, error: str | None = None,
                           finished: bool = False) -> None:
    now = time.time()
    fin = now if finished else None
    conn = _connect(run_dir, readonly=False)
    try:
        with conn:
            conn.execute(
                'UPDATE wf_attempts SET status=?, orchestrator_json=COALESCE(?, orchestrator_json), '
                'updated_at=?, finished_at=COALESCE(?, finished_at), error=? WHERE attempt=?',
                (status, _dump_opt(orchestrator), now, fin, error, attempt),
            )
            conn.execute(
                'UPDATE workflow_state SET status=?, updated_at=?, finished_at=COALESCE(?, finished_at), error=? '
                'WHERE id=1 AND current_attempt=?',
                (status, now, fin, error, attempt),
            )
    finally:
        conn.close()


def request_stop(run_dir: Path, *, expected_attempt: int | None = None) -> tuple[int, str]:
    now = time.time()
    conn = _connect(run_dir, readonly=False)
    try:
        with conn:
            row = conn.execute('SELECT current_attempt, status FROM workflow_state WHERE id=1').fetchone()
            if row is None:
                raise ForgeError('Workflow state missing', code='not_found')
            cur_attempt, cur_status = row['current_attempt'], row['status']
            if expected_attempt is not None and expected_attempt != cur_attempt:
                raise ForgeError('Workflow attempt changed; refresh before stopping', code='conflict')
            if cur_status in TERMINAL_WORKFLOW_STATUSES:
                return cur_attempt, cur_status
            conn.execute(
                'UPDATE workflow_state SET status=?, stop_requested=1, updated_at=? WHERE id=1',
                ('stopping', now),
            )
            conn.execute(
                'UPDATE wf_attempts SET status=?, updated_at=? WHERE attempt=?',
                ('stopping', now, cur_attempt),
            )
            conn.execute(
                'INSERT INTO workflow_events (attempt, ts, kind, node_id, detail_json) VALUES (?, ?, ?, NULL, ?)',
                (cur_attempt, now, 'workflow.stop_requested', None),
            )
            return cur_attempt, 'stopping'
    finally:
        conn.close()


def is_stop_requested(run_dir: Path, attempt: int) -> bool:
    conn = _connect(run_dir, readonly=True)
    try:
        row = conn.execute('SELECT current_attempt, stop_requested, status FROM workflow_state WHERE id=1').fetchone()
        if row is None:
            return True
        if row['current_attempt'] != attempt:
            return True
        return bool(row['stop_requested']) or row['status'] == 'stopping'
    finally:
        conn.close()


def update_node(run_dir: Path, attempt: int, node_id: str, **fields) -> None:
    allowed = {
        'status', 'child_run_id', 'child_attempt', 'resolved_overrides',
        'bindings', 'artifacts', 'error', 'started_at', 'finished_at',
    }
    bad = set(fields) - allowed
    if bad:
        raise ValueError('Unknown node update fields: ' + ', '.join(sorted(bad)))
    now = time.time()
    sets = ['updated_at=?']
    vals: list = [now]
    for key, val in fields.items():
        if key in {'resolved_overrides', 'bindings', 'artifacts'}:
            sets.append(f'{key}_json=?')
            vals.append(_dump_opt(val))
        else:
            sets.append(f'{key}=?')
            vals.append(val)
    vals.extend([attempt, node_id])
    conn = _connect(run_dir, readonly=False)
    try:
        with conn:
            conn.execute(f'UPDATE node_runs SET {", ".join(sets)} WHERE attempt=? AND node_id=?', vals)
    finally:
        conn.close()


def append_event(run_dir: Path, attempt: int, kind: str, *, node_id: str | None = None,
                 detail: dict | None = None) -> None:
    now = time.time()
    conn = _connect(run_dir, readonly=False)
    try:
        with conn:
            conn.execute(
                'INSERT INTO workflow_events (attempt, ts, kind, node_id, detail_json) VALUES (?, ?, ?, ?, ?)',
                (attempt, now, kind, node_id, _dump_opt(detail)),
            )
    finally:
        conn.close()


def read_run_snapshot(run_dir: Path, *, attempt: int | None = None, event_limit: int = 80) -> dict:
    """Read-only bounded snapshot of a workflow run's state, attempts, node states, and recent events."""
    event_limit = max(1, min(MAX_EVENTS_PAGE, int(event_limit)))
    conn = _connect(run_dir, readonly=True)
    try:
        wf = conn.execute('SELECT * FROM workflow_state WHERE id=1').fetchone()
        if wf is None:
            raise ForgeError('Workflow ledger is empty', code='not_found')
        current_attempt = wf['current_attempt']
        selected_attempt = current_attempt if attempt is None else attempt
        if type(selected_attempt) is not int or selected_attempt < 1:
            raise ForgeError('Invalid workflow attempt', code='invalid_request')
        att_rows = conn.execute('SELECT * FROM wf_attempts ORDER BY attempt ASC LIMIT ?', (MAX_WORKFLOW_ATTEMPTS,)).fetchall()
        selected_row = next((r for r in att_rows if r['attempt'] == selected_attempt), None)
        if selected_row is None:
            raise ForgeError(f'Workflow attempt {selected_attempt} not found', code='not_found')
        node_rows = conn.execute(
            'SELECT * FROM node_runs WHERE attempt=? ORDER BY rowid ASC',
            (selected_attempt,),
        ).fetchall()
        ev_rows = conn.execute(
            'SELECT * FROM workflow_events WHERE attempt=? ORDER BY seq DESC LIMIT ?',
            (selected_attempt, event_limit),
        ).fetchall()
    finally:
        conn.close()

    attempts = [
        {
            'attempt': r['attempt'],
            'status': r['status'],
            'from_node': r['from_node'],
            'orchestrator': _load_opt(r['orchestrator_json']),
            'started_at': r['started_at'],
            'updated_at': r['updated_at'],
            'finished_at': r['finished_at'],
            'error': r['error'],
        }
        for r in att_rows
    ]
    nodes = {}
    for r in node_rows:
        nodes[r['node_id']] = {
            'node_id': r['node_id'],
            'status': r['status'],
            'project_alias': r['project_alias'],
            'project_root': r['project_root'],
            'project_workspace_id': r['project_workspace_id'],
            'entry': r['entry'],
            'params': r['params'],
            'child_run_id': r['child_run_id'],
            'child_attempt': r['child_attempt'],
            'resolved_overrides': _load_opt(r['resolved_overrides_json']),
            'bindings': _load_opt(r['bindings_json']),
            'artifacts': _load_opt(r['artifacts_json']),
            'error': r['error'],
            'started_at': r['started_at'],
            'finished_at': r['finished_at'],
            'updated_at': r['updated_at'],
        }
    events = [
        {
            'seq': r['seq'],
            'attempt': r['attempt'],
            'ts': r['ts'],
            'kind': r['kind'],
            'node_id': r['node_id'],
            'detail': _load_opt(r['detail_json']),
        }
        for r in reversed(ev_rows)
    ]
    return {
        'wf_run_id': wf['wf_run_id'],
        'current_attempt': current_attempt,
        'attempt': selected_attempt,
        'status': selected_row['status'],
        'current_status': wf['status'],
        'stop_requested': bool(wf['stop_requested']),
        'created_at': wf['created_at'],
        'started_at': selected_row['started_at'],
        'updated_at': selected_row['updated_at'],
        'finished_at': selected_row['finished_at'],
        'error': selected_row['error'],
        'attempts': attempts,
        'nodes': nodes,
        'events': events,
    }


def scan_run_ids(host_root: Path | str) -> list[str]:
    base = runs_root(host_root)
    if not base.is_dir() or base.is_symlink():
        return []
    found = []
    with os.scandir(base) as entries:
        for entry in entries:
            if len(found) >= MAX_WORKFLOW_RUNS_SCAN:
                break
            if entry.is_symlink() or not entry.is_dir(follow_symlinks=False):
                continue
            if WORKFLOW_RUN_ID_RE.fullmatch(entry.name):
                found.append(entry.name)
    return found
