"""Single-host finite task queue on LOCAL storage, with atomic batch claims.

Use SQLiteQueue(path).add(tasks), register_worker(pool), claim(worker, limit=8),
then complete/fail. Task rows declare task_id, payload, pool, budget_class, cost.
A verified durable artifact must exist before complete; retries are at least once.
"""
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path
import json
import math
import os
import socket
import sqlite3
import time
import uuid

_NETWORK_FS = {'nfs', 'nfs4', 'cifs', 'smb3', 'ceph', 'lustre', 'dpc', 'glusterfs', 'fuse.glusterfs', 'fuse.sshfs', '9p'}


def require_local_storage(path, *, mountinfo='/proc/self/mountinfo'):
    path = Path(path).resolve()
    if not Path(mountinfo).exists():
        raise ValueError('Cannot establish local filesystem; use COSQueue on this host')
    matches = []
    for line in Path(mountinfo).read_text().splitlines():
        left, right = line.split(' - ', 1)
        mount = Path(left.split()[4].replace('\\040', ' ').replace('\\134', '\\'))
        if path == mount or mount in path.parents:
            matches.append((len(mount.parts), right.split()[0]))
    if not matches or max(matches)[1] in _NETWORK_FS or max(matches)[1].startswith('fuse.'):
        raise ValueError('SQLite WAL queue requires local storage; use local disk + backup or COSQueue')
    return path


def process_identity(pid):
    try:
        # /proc stat comm can contain spaces and parentheses.
        fields = Path(f'/proc/{pid}/stat').read_text().rsplit(')', 1)[1].split()
        return None if fields[0] == 'Z' else fields[19]
    except FileNotFoundError:
        return None


@dataclass(frozen=True)
class TaskHandle:
    task_id: str
    payload: object
    pool: str
    budget_class: str
    cost: float
    attempt: int
    worker: str
    token: str


class SQLiteQueue:
    def __init__(self, path, *, timeout_s=30, clock=time.time):
        self.path = require_local_storage(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.timeout_s, self.clock = timeout_s, clock
        with self._connection() as db:
            db.execute('PRAGMA journal_mode=WAL')
            db.executescript('''
              CREATE TABLE IF NOT EXISTS tasks (
                task_id TEXT PRIMARY KEY, payload_json TEXT NOT NULL, pool TEXT NOT NULL,
                budget_class TEXT NOT NULL, cost REAL NOT NULL CHECK(cost>=0),
                state TEXT NOT NULL DEFAULT 'pending', attempts INTEGER NOT NULL DEFAULT 0,
                max_attempts INTEGER NOT NULL, next_run_at REAL NOT NULL DEFAULT 0,
                worker TEXT, token TEXT, claimed_at REAL, result_json TEXT, error TEXT);
              CREATE INDEX IF NOT EXISTS claim_tasks ON tasks(pool, state, next_run_at, task_id);
              CREATE TABLE IF NOT EXISTS workers (
                worker TEXT PRIMARY KEY, pool TEXT NOT NULL, host TEXT NOT NULL,
                pid INTEGER NOT NULL, process_start TEXT NOT NULL, heartbeat REAL NOT NULL);
              CREATE TABLE IF NOT EXISTS budgets (
                class TEXT PRIMARY KEY, cap REAL NOT NULL, spent REAL NOT NULL DEFAULT 0,
                reserved REAL NOT NULL DEFAULT 0);
              CREATE TABLE IF NOT EXISTS completions (
                task_id TEXT PRIMARY KEY, worker TEXT NOT NULL, token TEXT NOT NULL,
                completed_at REAL NOT NULL, cost REAL NOT NULL, result_json TEXT NOT NULL);
              CREATE TABLE IF NOT EXISTS claim_budgets (
                task_id TEXT PRIMARY KEY, class TEXT NOT NULL, cost REAL NOT NULL);
              CREATE TABLE IF NOT EXISTS completion_budgets (
                task_id TEXT PRIMARY KEY, class TEXT NOT NULL, cost REAL NOT NULL);
              INSERT OR IGNORE INTO claim_budgets
                SELECT task_id,budget_class,cost FROM tasks WHERE state='running';
            ''')

    @contextmanager
    def _connection(self, *, write=False):
        db = sqlite3.connect(self.path, timeout=self.timeout_s)
        db.row_factory = sqlite3.Row
        db.execute('PRAGMA synchronous=NORMAL')
        try:
            with db:
                if write:
                    db.execute('BEGIN IMMEDIATE')
                yield db
        finally:
            db.close()

    def set_budget(self, budget_class, cap):
        if not math.isfinite(cap) or cap < 0:
            raise ValueError('budget must be nonnegative')
        with self._connection(write=True) as db:
            db.execute('''INSERT INTO budgets(class, cap, spent, reserved) VALUES (?, ?,
                (SELECT COALESCE(sum(cost),0) FROM completion_budgets WHERE class=?),
                (SELECT COALESCE(sum(cost),0) FROM claim_budgets WHERE class=?))
                ON CONFLICT(class) DO UPDATE SET cap=excluded.cap''',
                       (budget_class, cap, budget_class, budget_class))

    def add(self, tasks, *, max_attempts=3):
        if type(max_attempts) is not int or max_attempts < 1:
            raise ValueError('max_attempts must be positive')
        count = 0
        # Caller can stream millions of tasks; commits stay bounded.
        iterator = iter(tasks)
        import itertools
        while batch := list(itertools.islice(iterator, 1000)):
            with self._connection(write=True) as db:
                for task in batch:
                    key = task['task_id']
                    if not isinstance(key, str) or not key:
                        raise ValueError('task_id must be nonempty text')
                    values = (json.dumps(task['payload'], sort_keys=True, ensure_ascii=False),
                              task.get('pool', 'default'), task.get('budget_class', 'default'), float(task.get('cost', 0)))
                    if not math.isfinite(values[-1]) or values[-1] < 0:
                        raise ValueError('task cost must be nonnegative')
                    old = db.execute('SELECT payload_json, pool, budget_class, cost FROM tasks WHERE task_id=?', (key,)).fetchone()
                    if old is not None:
                        if tuple(old) != values:
                            raise ValueError('Task identity differs: ' + key)
                        continue
                    db.execute('INSERT INTO tasks(task_id, payload_json, pool, budget_class, cost, max_attempts) VALUES (?, ?, ?, ?, ?, ?)',
                               (key, *values, max_attempts))
                    count += 1
        return count

    def register_worker(self, pool='default'):
        worker = uuid.uuid4().hex
        pid = os.getpid()
        with self._connection(write=True) as db:
            hosts = {row[0] for row in db.execute('SELECT DISTINCT host FROM workers')}
            if hosts - {socket.gethostname()}:
                raise ValueError('Queue contains workers from another host; restore only an inactive snapshot')
            db.execute('INSERT INTO workers VALUES (?, ?, ?, ?, ?, ?)',
                       (worker, pool, socket.gethostname(), pid, process_identity(pid), self.clock()))
        return worker

    def heartbeat(self, worker):
        with self._connection(write=True) as db:
            if db.execute('UPDATE workers SET heartbeat=? WHERE worker=?', (self.clock(), worker)).rowcount != 1:
                raise ValueError('Unknown worker')

    def claim(self, worker, *, limit=8):
        if type(limit) is not int or not 1 <= limit <= 1024:
            raise ValueError('claim limit must be 1..1024')
        handles = []
        with self._connection(write=True) as db:
            owner = db.execute('SELECT pool FROM workers WHERE worker=?', (worker,)).fetchone()
            if owner is None:
                raise ValueError('Worker must register before claiming')
            now = self.clock()
            candidates = db.execute('''SELECT t.* FROM tasks t LEFT JOIN budgets b ON t.budget_class=b.class
                WHERE t.pool=? AND t.state='pending' AND t.next_run_at<=?
                AND (b.class IS NULL OR b.spent+b.reserved+t.cost<=b.cap)
                ORDER BY t.next_run_at,t.task_id LIMIT ?''', (owner['pool'], now, limit)).fetchall()
            for row in candidates:
                budget = db.execute('SELECT * FROM budgets WHERE class=?', (row['budget_class'],)).fetchone()
                if budget and budget['spent'] + budget['reserved'] + row['cost'] > budget['cap']:
                    continue
                token = uuid.uuid4().hex
                db.execute("UPDATE tasks SET state='running', attempts=attempts+1, worker=?, token=?, claimed_at=? WHERE task_id=?",
                           (worker, token, now, row['task_id']))
                db.execute('UPDATE budgets SET reserved=reserved+? WHERE class=?', (row['cost'], row['budget_class']))
                db.execute('INSERT INTO claim_budgets VALUES (?, ?, ?)', (row['task_id'], row['budget_class'], row['cost']))
                handles.append(TaskHandle(row['task_id'], json.loads(row['payload_json']), row['pool'], row['budget_class'],
                                          row['cost'], row['attempts'] + 1, worker, token))
            db.execute('UPDATE workers SET heartbeat=? WHERE worker=?', (now, worker))
        return handles

    def _owned(self, db, handle):
        row = db.execute('SELECT * FROM tasks WHERE task_id=?', (handle.task_id,)).fetchone()
        if row is None or row['state'] != 'running' or row['worker'] != handle.worker or row['token'] != handle.token:
            raise ValueError('Task is no longer owned by this claim')
        return row

    def complete(self, handle, result, *, actual_cost=None):
        cost = handle.cost if actual_cost is None else actual_cost
        if not math.isfinite(cost) or cost < 0:
            raise ValueError('actual_cost must be nonnegative')
        value = json.dumps(result, ensure_ascii=False, sort_keys=True)
        with self._connection(write=True) as db:
            old = db.execute('SELECT token, result_json, cost FROM completions WHERE task_id=?', (handle.task_id,)).fetchone()
            if old and tuple(old) == (handle.token, value, cost):
                return
            self._owned(db, handle)
            allocation = db.execute('SELECT class,cost FROM claim_budgets WHERE task_id=?', (handle.task_id,)).fetchone()
            db.execute('INSERT INTO completions VALUES (?, ?, ?, ?, ?, ?)',
                       (handle.task_id, handle.worker, handle.token, self.clock(), cost, value))
            db.execute('INSERT INTO completion_budgets VALUES (?, ?, ?)', (handle.task_id, allocation['class'], cost))
            db.execute("UPDATE tasks SET state='done', result_json=? WHERE task_id=?", (value, handle.task_id))
            db.execute('UPDATE budgets SET reserved=reserved-?, spent=spent+? WHERE class=?',
                       (allocation['cost'], cost, allocation['class']))
            db.execute('DELETE FROM claim_budgets WHERE task_id=?', (handle.task_id,))

    def transfer_budget(self, handle, budget_class, cost):
        """Reserve a fallback source's budget BEFORE consumption, atomically.

        Leaves the original task definition unchanged. Failed transfers retain
        the previous reservation; retries and crash recovery release exactly the
        current allocation. Successful bytes are charged on complete.
        """
        if not math.isfinite(cost) or cost < 0:
            raise ValueError('cost must be nonnegative and finite')
        with self._connection(write=True) as db:
            self._owned(db, handle)
            old = db.execute('SELECT class,cost FROM claim_budgets WHERE task_id=?', (handle.task_id,)).fetchone()
            db.execute('UPDATE budgets SET reserved=reserved-? WHERE class=?', (old['cost'], old['class']))
            new = db.execute('SELECT * FROM budgets WHERE class=?', (budget_class,)).fetchone()
            if new and new['spent'] + new['reserved'] + cost > new['cap']:
                raise ValueError('Fallback budget exhausted: ' + budget_class)
            db.execute('UPDATE budgets SET reserved=reserved+? WHERE class=?', (cost, budget_class))
            db.execute('UPDATE claim_budgets SET class=?,cost=? WHERE task_id=?', (budget_class, cost, handle.task_id))

    @staticmethod
    def _release_budget(db, task_id):
        allocation = db.execute('SELECT class,cost FROM claim_budgets WHERE task_id=?', (task_id,)).fetchone()
        if allocation:
            db.execute('UPDATE budgets SET reserved=reserved-? WHERE class=?', (allocation['cost'], allocation['class']))
            db.execute('DELETE FROM claim_budgets WHERE task_id=?', (task_id,))

    def fail(self, handle, error, *, backoff_s=60):
        if backoff_s < 0:
            raise ValueError('backoff_s must be nonnegative')
        with self._connection(write=True) as db:
            row = self._owned(db, handle)
            state = 'failed' if row['attempts'] >= row['max_attempts'] else 'pending'
            db.execute('UPDATE tasks SET state=?, error=?, next_run_at=?, worker=NULL, token=NULL WHERE task_id=?',
                       (state, str(error), self.clock() + backoff_s, handle.task_id))
            self._release_budget(db, handle.task_id)
        return state

    def reclaim(self, *, stale_s=300):
        if stale_s <= 0:
            raise ValueError('stale_s must be positive')
        reclaimed = []
        with self._connection(write=True) as db:
            for owner in db.execute('SELECT * FROM workers WHERE heartbeat<?', (self.clock() - stale_s,)).fetchall():
                if owner['host'] != socket.gethostname():
                    continue
                if process_identity(owner['pid']) == owner['process_start']:
                    continue  # Heartbeat alone is never proof a live worker died.
                for row in db.execute("SELECT * FROM tasks WHERE worker=? AND state='running'", (owner['worker'],)).fetchall():
                    state = 'failed' if row['attempts'] >= row['max_attempts'] else 'pending'
                    db.execute('UPDATE tasks SET state=?, worker=NULL, token=NULL, next_run_at=? WHERE task_id=?',
                               (state, self.clock(), row['task_id']))
                    self._release_budget(db, row['task_id'])
                    reclaimed.append(row['task_id'])
                db.execute('DELETE FROM workers WHERE worker=?', (owner['worker'],))
        return reclaimed

    def unregister_worker(self, worker):
        with self._connection(write=True) as db:
            if db.execute("SELECT 1 FROM tasks WHERE worker=? AND state='running'", (worker,)).fetchone():
                raise ValueError('Worker still owns tasks')
            db.execute('DELETE FROM workers WHERE worker=?', (worker,))

    def snapshot(self):
        with self._connection() as db:
            return {'states': [dict(row) for row in db.execute('SELECT pool, state, count(*) AS count FROM tasks GROUP BY pool, state')],
                    'workers': [dict(row) for row in db.execute('SELECT pool, count(*) AS count FROM workers GROUP BY pool')],
                    'budgets': [dict(row) for row in db.execute('SELECT * FROM budgets')]}

    def backup(self, target):
        """Consistent backup API; atomically replace the network-disk snapshot."""
        target = Path(target)
        if target.resolve() == self.path:
            raise ValueError('Snapshot must differ from live queue')
        target.parent.mkdir(parents=True, exist_ok=True)
        temporary = target.with_name(target.name + '.' + uuid.uuid4().hex + '.tmp')
        try:
            with self._connection() as db:
                destination = sqlite3.connect(temporary)
                try:
                    db.backup(destination)
                finally:
                    destination.close()
            with temporary.open('rb') as stream:
                os.fsync(stream.fileno())
            os.replace(temporary, target)
        finally:
            temporary.unlink(missing_ok=True)
        return str(target)

    def reconcile(self, verify):
        """Compare committed tasks, completion ledger and actual artifacts."""
        mismatches = []
        with self._connection() as db:
            for row in db.execute('''SELECT t.task_id,t.state,t.result_json,c.result_json AS ledger
                FROM tasks t LEFT JOIN completions c ON t.task_id=c.task_id
                WHERE t.state='done' OR c.task_id IS NOT NULL'''):
                if row['state'] != 'done' or row['result_json'] != row['ledger']:
                    mismatches.append({'task_id': row['task_id'], 'reason': 'ledger_state_mismatch'})
                elif not verify(json.loads(row['result_json'])):
                    mismatches.append({'task_id': row['task_id'], 'reason': 'artifact_missing_or_invalid'})
        return mismatches
