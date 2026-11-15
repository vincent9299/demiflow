"""Run lifecycle journal: manifest, committed stages and explicit empty reset.

SQLite stores control-plane records; DatasetRef points at business data in Lance.
All mutations share the run's nonblocking writer lock. No arbitrary key/value API.
"""
from pathlib import Path
import json
import sqlite3
import time
import math
import os
import threading
from contextlib import contextmanager, ExitStack

from .artifacts import run_lock, encoded
from ..errors import ExecutionConflict
from .recovery_contract import RecoveryContractError


class ExecutionBudgetExceeded(RecoveryContractError):
    """A valid reservation exceeded its named durable ceiling."""

    def __init__(self, budget, message=None):
        self.budget = budget
        super().__init__(message or 'Execution budget exhausted: ' + budget)

OBSERVATION_BYTES = 8192
OBSERVATION_TOTAL_BYTES = 8 * 1024**2
OBSERVATION_COUNT = min(4096, OBSERVATION_TOTAL_BYTES // OBSERVATION_BYTES)
_MAX_ARTIFACT_OWNERS = 16384
_NODE_STATES = {'pending', 'running', 'succeeded', 'failed', 'incomplete', 'reused'}
_TERMINAL_NODE_STATES = {'succeeded', 'failed', 'incomplete', 'reused'}


def _node_key(action_id, node_id, occurrence):
    if any(type(value) is not str or not 1 <= len(value) <= 64 for value in (action_id, node_id)):
        raise ValueError('Invalid node observation identity')
    if type(occurrence) is not int or not 0 <= occurrence < OBSERVATION_COUNT:
        raise ValueError('Invalid node occurrence')
    return action_id, node_id, occurrence


def _node_document(document):
    keys = {'sequence', 'state', 'rows_input', 'rows_output', 'calls', 'dropped', 'errors',
            'elapsed_s', 'timing_scope', 'group_members', 'evidence', 'detail', 'lifecycle'}
    if type(document) is not dict or set(document) - keys or not {'sequence', 'state'} <= document.keys():
        raise ValueError('Invalid node observation fields')
    if type(document['sequence']) is not int or not 0 <= document['sequence'] < 2**63 or document['state'] not in _NODE_STATES:
        raise ValueError('Invalid node sequence/state')
    for name in ('rows_input', 'rows_output', 'calls', 'dropped', 'errors'):
        value = document.get(name)
        if value is not None and (type(value) is not int or not 0 <= value < 2**63):
            raise ValueError('Invalid node counter')
    elapsed = document.get('elapsed_s')
    if elapsed is not None and (type(elapsed) not in (int, float) or not math.isfinite(elapsed) or elapsed < 0):
        raise ValueError('Invalid node duration')
    if document.get('timing_scope', 'unknown') not in {'node', 'group', 'unknown'}:
        raise ValueError('Invalid timing scope')
    detail = document.get('detail', '')
    if type(detail) is not str or len(detail) > 1024:
        raise ValueError('Node detail exceeds budget')
    members = document.get('group_members', [])
    if type(members) is not list or len(members) > 128 or any(type(v) is not str or len(v) > 64 for v in members):
        raise ValueError('Invalid execution group members')
    lifecycle = document.get('lifecycle', {})
    if type(lifecycle) is not dict or set(lifecycle) - {'created', 'started', 'stopped', 'closed'} or any(v is not None and type(v) is not bool for v in lifecycle.values()):
        raise ValueError('Invalid lifecycle observation')
    evidence = document.get('evidence', [])
    if type(evidence) is not list or len(evidence) > 16:
        raise ValueError('Evidence reference budget exceeded')
    for item in evidence:
        if type(item) is not dict or set(item) != {'kind', 'resource', 'id'} or any(type(v) is not str or len(v) > 256 for v in item.values()):
            raise ValueError('Invalid native evidence reference')
    raw = encoded(document)
    if len(raw) > OBSERVATION_BYTES:
        raise ValueError('Node observation exceeds byte budget')
    return raw.decode()


class RunJournal:
    def __init__(self, directory, *, attempt=None):
        if attempt is not None and (type(attempt) is not int or not 1 <= attempt <= 32):
            raise ValueError('Attempt must be 1..32')
        self.directory = Path(directory)
        self.path = self.directory / 'run.sqlite'
        self.attempt = attempt
        self._pid = os.getpid()
        self._connection_lock = threading.RLock()
        self._reader_lock = threading.RLock()
        self._connections = None

    @contextmanager
    def connection_scope(self):
        """Reuse at most one writer and one read-only connection for this owner.

        Each operation still ends its own transaction. In particular, readers
        never share the writer's uncommitted state or retain a polling snapshot.
        The driver must drain its callbacks before leaving this scope.
        """
        self._check_process()
        with self._connection_lock, self._reader_lock:
            if self._connections is not None:
                raise ExecutionConflict('Journal connection scope is already active')
            self._connections = {}
        try:
            yield self
        finally:
            self._check_process()
            with self._connection_lock, self._reader_lock:
                connections, self._connections = self._connections, None
                with ExitStack() as closing:
                    for db in connections.values():
                        closing.callback(db.close)

    def _check_process(self):
        # Check before acquiring a lock which another thread held at fork.
        if os.getpid() != self._pid:
            raise ExecutionConflict('Create a new RunJournal after fork')

    @contextmanager
    def _connection_access(self, write):
        # Native sinks can pull Arrow batches on a callback thread while the
        # publishing thread holds the writer transaction. Its read-only stop
        # polls must remain independent of the writer's Python lock as well.
        lock = self._connection_lock if write else self._reader_lock
        if not lock.acquire(timeout=1 if write else 5):
            raise sqlite3.OperationalError('Timed out waiting for journal connection')
        try:
            yield
        finally:
            lock.release()

    @contextmanager
    def _connection(self, *, write=False):
        self._check_process()
        with self._connection_access(write):
            scoped = self._connections is not None
            db = self._connections.get(write) if scoped else None
            if db is None:
                db = (self._connect() if write else sqlite3.connect(
                    self.path.resolve().as_uri() + '?mode=ro', uri=True,
                    timeout=5, check_same_thread=False, cached_statements=64))
                if not write:
                    try:
                        db.execute('PRAGMA cache_size=-2048')
                    except BaseException:
                        db.close()
                        raise
                if scoped:
                    self._connections[write] = db
            if db.in_transaction:
                raise ExecutionConflict('Nested journal connection transaction')
            try:
                yield db
                # Multi-query reads use BEGIN for a coherent view. End that
                # snapshot before the next poll, also when the connection lives.
                if db.in_transaction:
                    db.rollback()
            except BaseException:
                if scoped:
                    self._connections.pop(write, None)
                db.close()
                raise
            finally:
                if not scoped:
                    db.close()

    @staticmethod
    def _modern(db):
        return db.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='attempts'").fetchone() is not None

    def _scope(self, db):
        if not self._modern(db):
            if self.attempt is not None:
                raise ValueError('Historical journal has no attempt contract')
            return '', ()
        state = json.loads(db.execute('SELECT document FROM execution_state').fetchone()[0])
        return ' WHERE attempt=?', (self.attempt or state['attempt'],)

    def _connect(self):
        self.directory.mkdir(parents=True, exist_ok=True)
        db = sqlite3.connect(self.path, timeout=1, check_same_thread=False,
                             cached_statements=64)
        try:
            self._initialize_connection(db)
        except BaseException:
            db.close()
            raise
        return db

    @staticmethod
    def _initialize_connection(db):
        db.execute('PRAGMA synchronous=FULL')
        db.execute('PRAGMA cache_size=-2048')
        db.execute('PRAGMA temp_store=MEMORY')
        if db.execute("SELECT 1 FROM sqlite_master WHERE name='forge_action_reservations'").fetchone():
            db.execute('PRAGMA max_page_count=16384')
        db.executescript('''
          CREATE TABLE IF NOT EXISTS manifest (singleton INTEGER PRIMARY KEY CHECK(singleton=1), document TEXT NOT NULL);
          CREATE TABLE IF NOT EXISTS stages (name TEXT PRIMARY KEY, fingerprint TEXT NOT NULL, dataset_ref TEXT NOT NULL);
          CREATE TABLE IF NOT EXISTS completion (singleton INTEGER PRIMARY KEY CHECK(singleton=1), document TEXT NOT NULL);
          CREATE TABLE IF NOT EXISTS activities (kind TEXT NOT NULL, identity TEXT NOT NULL, PRIMARY KEY(kind, identity));
          CREATE TABLE IF NOT EXISTS resets (id INTEGER PRIMARY KEY, actor TEXT NOT NULL, reason TEXT NOT NULL, time REAL NOT NULL, old_manifest TEXT NOT NULL);
          CREATE TABLE IF NOT EXISTS execution_state (singleton INTEGER PRIMARY KEY CHECK(singleton=1), document TEXT NOT NULL);
          CREATE TABLE IF NOT EXISTS action_events (sequence INTEGER PRIMARY KEY, action_id TEXT NOT NULL, kind TEXT NOT NULL, document TEXT NOT NULL, UNIQUE(action_id, kind));
          CREATE TABLE IF NOT EXISTS node_observations (action_id TEXT NOT NULL, node_id TEXT NOT NULL, occurrence INTEGER NOT NULL, sequence INTEGER NOT NULL, terminal INTEGER NOT NULL, document TEXT NOT NULL, PRIMARY KEY(action_id,node_id,occurrence));
          CREATE TABLE IF NOT EXISTS runtime_identity (singleton INTEGER PRIMARY KEY CHECK(singleton=1), document TEXT NOT NULL);
          CREATE TABLE IF NOT EXISTS runtime_owner (singleton INTEGER PRIMARY KEY CHECK(singleton=1), attempt INTEGER NOT NULL, fence TEXT NOT NULL);
          CREATE TABLE IF NOT EXISTS runtime_actions (action_id TEXT PRIMARY KEY, declaration TEXT NOT NULL, snapshot TEXT, result TEXT);
          CREATE TABLE IF NOT EXISTS runtime_effects (action_id TEXT NOT NULL, effect_id TEXT NOT NULL, document TEXT NOT NULL, PRIMARY KEY(action_id,effect_id));
          CREATE TABLE IF NOT EXISTS runtime_dataflow_recovery (singleton INTEGER PRIMARY KEY CHECK(singleton=1), cursor TEXT NOT NULL);
          CREATE TABLE IF NOT EXISTS runtime_dataflow_staging (reservation_id TEXT PRIMARY KEY, store_id TEXT NOT NULL, device INTEGER NOT NULL, inode INTEGER NOT NULL, initial_bytes INTEGER NOT NULL, publication TEXT);
          CREATE TABLE IF NOT EXISTS runtime_dataflow_objects (store_id TEXT NOT NULL, digest TEXT NOT NULL, bytes INTEGER NOT NULL, PRIMARY KEY(store_id,digest));
          CREATE TABLE IF NOT EXISTS runtime_dataflow_gc_protocol (singleton INTEGER PRIMARY KEY CHECK(singleton=1), device INTEGER NOT NULL, inode INTEGER NOT NULL, eligible INTEGER NOT NULL);
          CREATE TABLE IF NOT EXISTS runtime_dataflow_gc (gc_id TEXT PRIMARY KEY, store_id TEXT NOT NULL, digest TEXT NOT NULL, bytes INTEGER NOT NULL, charge_id TEXT NOT NULL, device INTEGER NOT NULL, inode INTEGER NOT NULL, status TEXT NOT NULL);
          CREATE UNIQUE INDEX IF NOT EXISTS runtime_dataflow_gc_pending ON runtime_dataflow_gc(store_id,digest) WHERE status!='done';
          CREATE TABLE IF NOT EXISTS runtime_dataflow_settlements (reservation_id TEXT PRIMARY KEY, store_id TEXT, digest TEXT, bytes INTEGER NOT NULL, charged INTEGER NOT NULL);
          CREATE TABLE IF NOT EXISTS runtime_artifacts (path TEXT PRIMARY KEY, commit_id TEXT, protected INTEGER NOT NULL DEFAULT 0, bytes INTEGER NOT NULL DEFAULT 0);
          CREATE TABLE IF NOT EXISTS runtime_object_roots (root_id TEXT PRIMARY KEY, logical_root TEXT UNIQUE NOT NULL, physical_root TEXT NOT NULL, run_id TEXT NOT NULL);
          CREATE TABLE IF NOT EXISTS runtime_sources (action_id TEXT PRIMARY KEY, document TEXT NOT NULL);
          CREATE TABLE IF NOT EXISTS runtime_commits (commit_id TEXT PRIMARY KEY, action_id TEXT NOT NULL, sequence INTEGER NOT NULL, status TEXT NOT NULL, intent TEXT NOT NULL, prepared TEXT, UNIQUE(action_id,sequence));
          CREATE TABLE IF NOT EXISTS runtime_receipts (commit_id TEXT NOT NULL, sink_id TEXT NOT NULL, document TEXT NOT NULL, PRIMARY KEY(commit_id,sink_id));
          CREATE TABLE IF NOT EXISTS runtime_budgets (name TEXT PRIMARY KEY, consumed INTEGER NOT NULL, ceiling INTEGER NOT NULL);
          CREATE TABLE IF NOT EXISTS runtime_reservations (name TEXT NOT NULL, reservation_id TEXT NOT NULL, amount INTEGER NOT NULL, PRIMARY KEY(name,reservation_id));
          CREATE TABLE IF NOT EXISTS artifact_owner (digest TEXT NOT NULL, store_id TEXT NOT NULL, key TEXT NOT NULL, owner TEXT NOT NULL, operation_id TEXT NOT NULL, PRIMARY KEY(digest,store_id,key,owner));
        ''')

        # Additive ownership metadata; historical execution identity is unchanged.
        columns = {r[1] for r in db.execute('PRAGMA table_info(runtime_artifacts)')}
        for name, definition in (('protected', 'INTEGER NOT NULL DEFAULT 0'), ('bytes', 'INTEGER NOT NULL DEFAULT 0'),
                                 ('reference_commit_id', 'TEXT')):
            if name not in columns:
                db.execute('ALTER TABLE runtime_artifacts ADD COLUMN ' + name + ' ' + definition)
        object_columns = {r[1] for r in db.execute('PRAGMA table_info(runtime_dataflow_objects)')}
        if 'charge_id' not in object_columns:
            db.execute('ALTER TABLE runtime_dataflow_objects ADD COLUMN charge_id TEXT')
        db.executescript("""
          CREATE TABLE IF NOT EXISTS runtime_storage (singleton INTEGER PRIMARY KEY CHECK(singleton=1), items INTEGER NOT NULL, bytes INTEGER NOT NULL, object_bytes INTEGER NOT NULL);
          CREATE TABLE IF NOT EXISTS runtime_reservation_count (singleton INTEGER PRIMARY KEY CHECK(singleton=1), items INTEGER NOT NULL);
          CREATE TRIGGER IF NOT EXISTS runtime_reservation_added AFTER INSERT ON runtime_reservations BEGIN
            UPDATE runtime_reservation_count SET items=items+1 WHERE singleton=1;
          END;
          CREATE TRIGGER IF NOT EXISTS runtime_reservation_removed AFTER DELETE ON runtime_reservations BEGIN
            UPDATE runtime_reservation_count SET items=items-1 WHERE singleton=1;
          END;
          CREATE INDEX IF NOT EXISTS runtime_artifacts_protected ON runtime_artifacts(protected,path);
          CREATE INDEX IF NOT EXISTS runtime_artifacts_reference_commit ON runtime_artifacts(reference_commit_id);
          CREATE INDEX IF NOT EXISTS artifact_owner_operation ON artifact_owner(operation_id);
          CREATE INDEX IF NOT EXISTS artifact_owner_owner ON artifact_owner(owner);
          CREATE TRIGGER IF NOT EXISTS runtime_artifact_added AFTER INSERT ON runtime_artifacts BEGIN
            UPDATE runtime_storage SET items=items+1,bytes=bytes+NEW.bytes,object_bytes=object_bytes+CASE WHEN NEW.path LIKE 'materializations/@objects-%' THEN NEW.bytes ELSE 0 END WHERE singleton=1;
          END;
          CREATE TRIGGER IF NOT EXISTS runtime_artifact_removed AFTER DELETE ON runtime_artifacts BEGIN
            UPDATE runtime_storage SET items=items-1,bytes=bytes-OLD.bytes,object_bytes=object_bytes-CASE WHEN OLD.path LIKE 'materializations/@objects-%' THEN OLD.bytes ELSE 0 END WHERE singleton=1;
          END;
          CREATE TRIGGER IF NOT EXISTS runtime_artifact_resized AFTER UPDATE OF bytes ON runtime_artifacts BEGIN
            UPDATE runtime_storage SET bytes=bytes+NEW.bytes-OLD.bytes,object_bytes=object_bytes+CASE WHEN NEW.path LIKE 'materializations/@objects-%' THEN NEW.bytes-OLD.bytes ELSE 0 END WHERE singleton=1;
          END;
        """)

        if db.execute('SELECT 1 FROM runtime_reservation_count WHERE singleton=1').fetchone() is None:
            db.execute('INSERT INTO runtime_reservation_count SELECT 1,count(*) FROM runtime_reservations')
            db.commit()
        if db.execute('SELECT 1 FROM runtime_storage WHERE singleton=1').fetchone() is None:
            db.execute("INSERT INTO runtime_storage SELECT 1,count(*),COALESCE(sum(bytes),0),COALESCE(sum(CASE WHEN path LIKE 'materializations/@objects-%' THEN bytes ELSE 0 END),0) FROM runtime_artifacts")
            db.commit()

    @contextmanager
    def _transaction(self):
        from .dataflow_object_lease import shared_access
        protocol = self.dataflow_gc_protocol() if self.path.exists() else None
        # GC is optional maintenance, not loss of this supervisor's attempt.
        # Wait before taking any DB connection/transaction; the original inode
        # and the attempt below are still checked after acquisition. The finite
        # wait matches SQLite's existing 5 s contention budget, not run time.
        with shared_access(self.directory / '.dataflow-store', protocol[:2] if protocol else None,
                           require_protocol=bool(protocol and protocol[2]), wait_ms=5000), self._connection(write=True) as db:
            with db:
                db.execute('BEGIN IMMEDIATE')
                if self.attempt is not None:
                    if not self._modern(db):
                        raise ValueError('Historical journal has no attempt contract')
                    row = db.execute('SELECT document FROM execution_state').fetchone()
                    if row is None or json.loads(row[0])['attempt'] != self.attempt:
                        raise ExecutionConflict('Stale execution attempt cannot mutate this run')
                yield db

    def initialize(self, manifest, *, attempts=False):
        value = encoded(manifest).decode()
        with run_lock(self.directory), self._transaction() as db:
            if len(value.encode()) > 8*1024**2:
                raise ValueError('Run manifest exceeds budget')
            if manifest.get('runtime_contract') == 'demiflow_forge_runtime_v1':
                db.execute('PRAGMA max_page_count=16384')
                db.execute('CREATE TABLE IF NOT EXISTS forge_action_reservations (attempt INTEGER, action_id TEXT, PRIMARY KEY(attempt,action_id))')
                db.execute('CREATE TABLE IF NOT EXISTS control_events (sequence INTEGER PRIMARY KEY, attempt INTEGER NOT NULL, document TEXT NOT NULL)')
                db.execute('CREATE TABLE IF NOT EXISTS runtime_summary (singleton INTEGER PRIMARY KEY CHECK(singleton=1),document TEXT)')
                summary={k:manifest[k] for k in ('run_id','root','entry','digest','report_digest','parameter_path','outputs','limits','created_at')}
                summary_raw=encoded(summary).decode()
                if len(summary_raw.encode())>65536:
                    raise ValueError('Runtime summary exceeds budget')
                db.execute('INSERT OR IGNORE INTO runtime_summary VALUES(1,?)',(summary_raw,))
            old = db.execute('SELECT document FROM manifest').fetchone()
            if old and old[0] != value:
                raise ValueError('Immutable manifest differs; use a new run or reset an empty run')
            db.execute('INSERT OR IGNORE INTO manifest VALUES (1, ?)', (value,))
            if attempts and not self._modern(db):
                if old or any(db.execute('SELECT 1 FROM ' + table + ' LIMIT 1').fetchone()
                              for table in ('completion', 'execution_state', 'action_events', 'node_observations', 'stages', 'activities')):
                    raise ValueError('Attempt format is only created for a new empty run')
                for table in ('completion', 'action_events', 'node_observations'):
                    db.execute('DROP TABLE ' + table)
                db.execute('CREATE TABLE attempts (attempt INTEGER PRIMARY KEY, document TEXT NOT NULL)')
                db.execute('CREATE TABLE completion (attempt INTEGER PRIMARY KEY, document TEXT NOT NULL)')
                db.execute('CREATE TABLE action_events (sequence INTEGER PRIMARY KEY, attempt INTEGER NOT NULL, action_id TEXT NOT NULL, kind TEXT NOT NULL, document TEXT NOT NULL, UNIQUE(attempt,action_id,kind))')
                db.execute('CREATE TABLE node_observations (attempt INTEGER NOT NULL, action_id TEXT NOT NULL, node_id TEXT NOT NULL, occurrence INTEGER NOT NULL, sequence INTEGER NOT NULL, terminal INTEGER NOT NULL, document TEXT NOT NULL, PRIMARY KEY(attempt,action_id,node_id,occurrence))')
                state = encoded({'status': None, 'revision': 0, 'attempt': 1}).decode()
                db.execute('INSERT INTO execution_state VALUES (1,?)', (state,))
                db.execute('INSERT INTO attempts VALUES (1,?)', (state,))

    @staticmethod
    def _managed(db):
        return db.execute("SELECT 1 FROM sqlite_master WHERE name='forge_action_reservations'").fetchone() is not None

    @classmethod
    def _reserve_capacity(cls, db, *, nodes=0, actions=0):
        if not cls._managed(db):
            return
        node_count = db.execute('SELECT count(*) FROM node_observations').fetchone()[0] + nodes
        action_count = db.execute('SELECT count(*) FROM forge_action_reservations').fetchone()[0] + actions
        manifest = db.execute('SELECT length(CAST(document AS BLOB)) FROM manifest').fetchone()[0]
        # Eight immutable fact slots per action, full node documents, B-tree
        # overhead, state/attempt/completion and sparse control-event reserve.
        action_bytes = action_count * 8 * 256*1024
        if action_bytes > 16*1024**2 or node_count * OBSERVATION_BYTES > 32*1024**2:
            raise ValueError('Cumulative reliable fact reservation exceeds budget')
        if manifest + node_count*(OBSERVATION_BYTES+1024) + action_bytes + 8*1024**2 > 60*1024**2:
            raise ValueError('Reliable journal reservation exceeds capacity')

    def reserve_actions(self, identities):
        if type(identities) is not list or len(identities) > 128 or any(type(i) is not str or len(i)>64 for i in identities):
            raise ValueError('Invalid action reservation')
        with self._transaction() as db:
            if not self._managed(db):
                return
            _, scope = self._scope(db)
            attempt = scope[0]
            old = {r[0] for r in db.execute('SELECT action_id FROM forge_action_reservations WHERE attempt=?', (attempt,))}
            new = set(identities)-old
            self._reserve_capacity(db, actions=len(new))
            db.executemany('INSERT INTO forge_action_reservations VALUES (?,?)', [(attempt, i) for i in new])

    def stage(self, name, fingerprint, dataset_ref):
        value = encoded(dataset_ref).decode()
        with run_lock(self.directory), self._transaction() as db:
            old = db.execute('SELECT fingerprint, dataset_ref FROM stages WHERE name=?', (name,)).fetchone()
            if old and old != (fingerprint, value):
                raise ValueError('Committed stage differs: ' + name)
            db.execute('INSERT OR IGNORE INTO stages VALUES (?, ?, ?)', (name, fingerprint, value))

    def activity(self, kind, identity):
        """Mark a nonempty run before a model call or external publication."""
        with run_lock(self.directory), self._transaction() as db:
            db.execute('INSERT OR IGNORE INTO activities VALUES (?, ?)', (kind, identity))

    def finish(self, state):
        with run_lock(self.directory), self._transaction() as db:
            _, parameters = self._scope(db)
            db.execute('INSERT OR REPLACE INTO completion VALUES (?, ?)', (parameters[0] if parameters else 1, encoded(state).decode()))

    def transition(self, expected, status, **details):
        """CAS execution state; control facts are separate from legacy completion."""
        allowed = {'prepared', 'starting', 'running', 'stopping', 'succeeded',
                   'failed', 'cancelled', 'interrupted'}
        if status not in allowed:
            raise ValueError('Unsupported execution status')
        if {'attempt', 'revision'} & details.keys():
            raise ValueError('Attempt and revision are journal-owned control fields')
        with self._transaction() as db:
            old = db.execute('SELECT document FROM execution_state').fetchone()
            value = json.loads(old[0]) if old else {'status': None, 'revision': 0}
            if value['status'] not in expected:
                return False
            previous = value['status']
            value.update(details, status=status, revision=value['revision'] + 1, updated_at=time.time())
            if self._managed(db):
                event = dict(revision=value['revision'], previous=previous, status=status,
                             stop_phase=value.get('stop_phase'), time=value['updated_at'])
                count = db.execute('SELECT count(*) FROM control_events').fetchone()[0]
                if count >= 1024:
                    raise ValueError('Control event capacity exhausted')
                db.execute('INSERT INTO control_events(attempt,document) VALUES (?,?)',
                           (value['attempt'], encoded(event).decode()))
            raw = encoded(value).decode()
            if len(raw.encode()) > 65536:
                raise ValueError('Execution state exceeds budget')
            db.execute('INSERT OR REPLACE INTO execution_state VALUES (1, ?)', (raw,))
            if self._modern(db):
                db.execute('UPDATE attempts SET document=? WHERE attempt=?', (raw, value['attempt']))
            return True

    def begin_attempt(self, *, expected_attempt, expected_revision):
        """CAS a new prepared attempt; callers additionally own the execution lease.

        This does not authorize a particular failure for retry, change the
        immutable manifest, reset usage, or reconcile external writes.
        """
        if type(expected_attempt) is not int or type(expected_revision) is not int:
            raise ValueError('Attempt and revision must be exact integers')
        with self._transaction() as db:
            if not self._modern(db):
                raise ValueError('Historical journal cannot resume under a new contract')
            old = json.loads(db.execute('SELECT document FROM execution_state').fetchone()[0])
            if (old['attempt'], old['revision']) != (expected_attempt, expected_revision):
                raise ExecutionConflict('Attempt or execution revision changed')
            if old['status'] not in {'cancelled', 'interrupted', 'failed'} or expected_attempt >= 32:
                raise ValueError('Execution status or attempt budget forbids resume')
            value = {'attempt': expected_attempt + 1, 'status': 'prepared', 'revision': expected_revision + 1,
                     'updated_at': time.time(), 'usage': old.get('usage', {}),
                     'usage_complete': old.get('usage_complete', False),
                     'preparation_ready': old.get('preparation_ready', False)}
            raw = encoded(value).decode()
            db.execute('INSERT INTO attempts VALUES (?,?)', (value['attempt'], raw))
            db.execute('UPDATE execution_state SET document=? WHERE singleton=1', (raw,))
            return value

    def record_usage(self, *, elapsed_ms, output_bytes, complete=False):
        """Persist cumulative supervisor accounting, fenced by execution attempt.

        An open interval is not proof of its final usage. A supervisor must close
        it after the worker stops and all available notifications are consumed;
        a lost accounting interval must never grant fresh budget on resume.
        """
        counters = {'elapsed_ms': elapsed_ms, 'output_bytes': output_bytes}
        if (type(complete) is not bool or any(type(v) is not int or not 0 <= v < 2**63 for v in counters.values())):
            raise ValueError('Invalid cumulative execution usage')
        with self._transaction() as db:
            old = db.execute('SELECT document FROM execution_state').fetchone()
            if old is None:
                raise ValueError('Execution has not been prepared')
            value = json.loads(old[0])
            if any(v < value.get('usage', {}).get(k, 0) for k, v in counters.items()):
                raise ValueError('Cumulative execution usage cannot decrease')
            value.update(usage=counters, usage_complete=complete, usage_revision=value.get('usage_revision', 0)+1)
            raw = encoded(value).decode()
            db.execute('UPDATE execution_state SET document=? WHERE singleton=1', (raw,))
            if self._modern(db):
                db.execute('UPDATE attempts SET document=? WHERE attempt=?', (raw, value['attempt']))

    def list_attempts(self):
        with self._connection() as db:
            if not self._modern(db):
                return []
            rows = db.execute('SELECT document FROM attempts ORDER BY attempt LIMIT 33').fetchall()
            if len(rows) > 32 or any(len(row[0].encode()) > 65536 for row in rows):
                raise ValueError('Attempt history exceeds budget')
            return [json.loads(row[0]) for row in rows]

    def action_history(self, action_ids, *, before_attempt, kinds):
        """Bounded read-only projection of prior action facts, without manifests."""
        if (type(action_ids) not in (list, tuple) or len(action_ids) > 32
                or any(type(a) is not str or not 1 <= len(a) <= 64 for a in action_ids)
                or type(before_attempt) is not int or not 1 <= before_attempt <= 32
                or type(kinds) not in (list, tuple) or not 1 <= len(kinds) <= 16
                or any(type(k) is not str or len(k) > 64 for k in kinds)):
            raise ValueError('Invalid bounded action history projection')
        if not self.path.is_file() or self.path.stat().st_size > 64 * 1024**2:
            raise ValueError('Run journal unavailable or exceeds budget')
        with self._connection() as db:
            db.execute('BEGIN')
            if not self._modern(db) or not action_ids:
                return []
            where = (' WHERE attempt<? AND action_id IN (' + ','.join('?' for _ in action_ids)
                     + ') AND kind IN (' + ','.join('?' for _ in kinds) + ')')
            parameters = (before_attempt, *action_ids, *kinds)
            count, size = db.execute('SELECT count(*),coalesce(sum(length(CAST(document AS BLOB))),0) FROM action_events' + where, parameters).fetchone()
            if count > 10000 or size > 16 * 1024**2:
                raise ValueError('Action history exceeds budget')
            return [dict(attempt=a, sequence=s, action_id=i, kind=k, value=json.loads(raw))
                    for a, s, i, k, raw in db.execute('SELECT attempt,sequence,action_id,kind,document FROM action_events' + where + ' ORDER BY attempt,sequence', parameters)]

    def action_event(self, action_id, kind, value):
        """Durable, bounded, idempotent action facts; never infer commits from logs."""
        if kind not in {'intent', 'receipt', 'failed', 'materialized', 'consumed', 'result', 'recovered', 'value',
                        'checkpoint', 'checkpoint_read', 'checkpoint_commits', 'stage_commit',
                        'stage_recovered', 'recovered_result'}:
            raise ValueError('Unsupported action event')
        if not isinstance(action_id, str) or not 1 <= len(action_id) <= 64:
            raise ValueError('Invalid action identity')
        raw = encoded(value).decode()
        if len(raw.encode()) > 256 * 1024:
            raise ValueError('Action event exceeds budget')
        with self._transaction() as db:
            scope, parameters = self._scope(db)
            old = db.execute('SELECT document FROM action_events' + scope + (' AND ' if scope else ' WHERE ') + 'action_id=? AND kind=?',
                             (*parameters, action_id, kind)).fetchone()
            if old:
                if old[0] != raw:
                    raise ValueError('Immutable action fact differs')
                return
            count, size = db.execute('SELECT count(*), coalesce(sum(length(CAST(document AS BLOB))), 0) FROM action_events').fetchone()
            if count >= 10000 or size + len(raw.encode()) > 16 * 1024**2:
                raise ValueError('Action journal exceeds budget')
            if self._managed(db):
                if not db.execute('SELECT 1 FROM forge_action_reservations WHERE attempt=? AND action_id=?', (*parameters, action_id)).fetchone():
                    raise ValueError('Action facts were not reserved before execution')
                used = db.execute('SELECT count(*) FROM action_events WHERE attempt=? AND action_id=?', (*parameters, action_id)).fetchone()[0]
                if used >= 8:
                    raise ValueError('Action fact reservation exhausted')
            columns = 'attempt,' if scope else ''
            placeholders = '?,' if scope else ''
            db.execute('INSERT INTO action_events(' + columns + 'action_id,kind,document) VALUES (' + placeholders + '?,?,?)',
                       (*parameters, action_id, kind, raw))

    def execution_view(self, *, detail='full', action_ids=None):
        """Read one snapshot; optional control/event projection never interprets a manifest."""
        if detail not in {'full', 'summary', 'identity'}:
            raise ValueError('Invalid execution read detail')
        if action_ids is not None and (type(action_ids) not in (tuple, list) or len(action_ids) > 10000
                or any(type(a) is not str or not 1 <= len(a) <= 64 for a in action_ids)):
            raise ValueError('Invalid action read projection')
        if not self.path.is_file() or self.path.stat().st_size > 64 * 1024**2:
            raise ValueError('Run journal unavailable or exceeds budget')
        with self._connection() as db:
            db.execute('BEGIN')
            for table, maximum in (('manifest', 8 * 1024**2), ('execution_state', 65536)):
                size = db.execute(f'SELECT length(CAST(document AS BLOB)) FROM {table}').fetchone()
                if size and size[0] > maximum:
                    raise ValueError('Execution control exceeds byte budget')
            manifest = db.execute('SELECT document FROM manifest').fetchone()
            manifest = json.loads(manifest[0]) if manifest else None
            state = db.execute('SELECT document FROM execution_state').fetchone()
            scope, scoped = self._scope(db)
            if scope:
                state = db.execute('SELECT document FROM attempts' + scope, scoped).fetchone()
                if state is None:
                    raise ValueError('Unknown execution attempt')
            completion = db.execute('SELECT document FROM completion' + scope, scoped).fetchone() if detail == 'full' else None
            rows = []
            if detail != 'identity':
                where, parameters = scope, scoped
                if action_ids is not None:
                    parameters = (*scoped, *action_ids)
                    where += (' AND ' if scope else ' WHERE ') + 'action_id IN (' + ','.join('?' for _ in action_ids) + ')'
                count, size = db.execute('SELECT count(*), coalesce(sum(length(CAST(document AS BLOB))),0) FROM action_events' + where, parameters).fetchone()
                if count > 10000 or size > 16 * 1024**2:
                    raise ValueError('Action journal exceeds budget')
                rows = db.execute('SELECT sequence, action_id, kind, document FROM action_events' + where + ' ORDER BY sequence LIMIT 10001', parameters).fetchall()
            observations = []
            if detail == 'full' and db.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='node_observations'").fetchone():
                count, size = db.execute('SELECT count(*), coalesce(sum(length(CAST(document AS BLOB))),0) FROM node_observations' + scope, scoped).fetchone()
                if count > OBSERVATION_COUNT or size > OBSERVATION_TOTAL_BYTES:
                    raise ValueError('Node observation store exceeds budget')
                observations = [dict(action_id=action, node_id=node, occurrence=occurrence, **json.loads(raw))
                                for action, node, occurrence, raw in db.execute('SELECT action_id,node_id,occurrence,document FROM node_observations' + scope + ' ORDER BY action_id,node_id,occurrence', scoped)]
            return {'manifest': manifest,
                    'state': json.loads(state[0]) if state else None,
                    'completion': json.loads(completion[0]) if completion else None,
                    'observations': observations,
                    'events': [dict(sequence=seq, action_id=aid, kind=kind, value=json.loads(raw))
                               for seq, aid, kind, raw in rows]}

    def reserve_nodes(self, identities):
        """Reserve every terminal document before any execution or side effect."""
        if type(identities) is not list or len(identities) > OBSERVATION_COUNT:
            raise ValueError('Node observation reservation exceeds count/byte budget')
        keys = [_node_key(*identity) for identity in identities]
        if len(set(keys)) != len(keys):
            raise ValueError('Duplicate node execution identity')
        with self._transaction() as db:
            scope, parameters = self._scope(db)
            existing = set(db.execute('SELECT action_id,node_id,occurrence FROM node_observations' + scope + ' LIMIT ?', (*parameters, OBSERVATION_COUNT + 1)))
            if len(existing | set(keys)) > OBSERVATION_COUNT:
                raise ValueError('Node observation reservation exceeds total byte budget')
            self._reserve_capacity(db, nodes=len(set(keys)-existing))
            total = db.execute('SELECT count(*) FROM node_observations').fetchone()[0]
            if total + len(set(keys) - existing) > OBSERVATION_COUNT * 4:
                raise ValueError('Cumulative node reservation exceeds 32 MiB')
            raw = _node_document({'sequence': 0, 'state': 'pending'})
            db.executemany('INSERT OR IGNORE INTO node_observations VALUES (' + ('?,' if scope else '') + '?,?,?,0,0,?)', [(*parameters, *key, raw) for key in keys])

    def observe_nodes(self, updates):
        """Batch latest snapshots; stale progress can never overwrite a terminal."""
        if type(updates) is not list or len(updates) > OBSERVATION_COUNT:
            raise ValueError('Node update batch exceeds budget')
        values = []
        for update in updates:
            if type(update) is not dict or set(update) != {'action_id', 'node_id', 'occurrence', 'document'}:
                raise ValueError('Invalid node update')
            key = _node_key(update['action_id'], update['node_id'], update['occurrence'])
            raw = _node_document(update['document'])
            values.append((key, update['document']['sequence'], update['document']['state'] in _TERMINAL_NODE_STATES, raw))
        with self._transaction() as db:
            scope, parameters = self._scope(db)
            where = scope + (' AND ' if scope else ' WHERE ') + 'action_id=? AND node_id=? AND occurrence=?'
            for key, sequence, terminal, raw in values:
                old = db.execute('SELECT sequence,terminal FROM node_observations' + where, (*parameters, *key)).fetchone()
                if old is None:
                    raise ValueError('Node observation was not reserved before execution')
                if old[1] or sequence <= old[0]:
                    continue
                db.execute('UPDATE node_observations SET sequence=?,terminal=?,document=?' + where,
                           (sequence, int(terminal), raw, *parameters, *key))

    def execution_status(self):
        """Small read-only control poll; do not reload source identities/events."""
        with self._connection() as db:
            row = db.execute('SELECT document FROM execution_state').fetchone()
            if row is None or len(row[0]) > 65536:
                raise ValueError('Invalid execution state')
            state = json.loads(row[0])
            if self.attempt is not None and state.get('attempt') != self.attempt:
                raise ValueError('Stale execution attempt')
            return state

    def reset_empty(self, *, actor, reason):
        """Remove only a manifest with zero stages, activity, completion or files.

        Unknown files are evidence of possible work and block reset. Audit history
        survives; callers initialize the replacement manifest explicitly afterward.
        """
        if not actor.strip() or not reason.strip():
            raise ValueError('actor and reason are required')
        with run_lock(self.directory), self._transaction() as db:
            for table in ('stages', 'activities', 'completion', 'execution_state', 'action_events', 'node_observations'):
                if db.execute(f'SELECT 1 FROM {table} LIMIT 1').fetchone():
                    raise ValueError('Run is not empty: ' + table)
            extras = [p for p in self.directory.rglob('*') if p.is_file()
                      and p.name not in {'.lock', 'run.sqlite', 'run.sqlite-journal'}]
            if extras:
                raise ValueError('Run has unaccounted artifacts: ' + str(extras[0]))
            old = db.execute('SELECT document FROM manifest').fetchone()
            if old is None:
                raise ValueError('Run has no manifest')
            cursor = db.execute('INSERT INTO resets(actor, reason, time, old_manifest) VALUES (?, ?, ?, ?)',
                                (actor, reason, time.time(), old[0]))
            db.execute('DELETE FROM manifest')
            receipt = {'reset_id': cursor.lastrowid, 'actor': actor, 'reason': reason}
        return receipt

    def inspect(self):
        with self._connection(write=True) as db:
            manifest = db.execute('SELECT document FROM manifest').fetchone()
            scope, parameters = self._scope(db)
            completion = db.execute('SELECT document FROM completion' + scope, parameters).fetchone()
            return {'manifest': json.loads(manifest[0]) if manifest else None,
                    'stages': {name: {'fingerprint': fingerprint, 'dataset_ref': json.loads(ref)}
                               for name, fingerprint, ref in db.execute('SELECT * FROM stages')},
                    'completion': json.loads(completion[0]) if completion else None,
                    'resets': [dict(zip(('id', 'actor', 'reason', 'time', 'old_manifest'), row))
                               for row in db.execute('SELECT * FROM resets')]}

    # One execution authority. These typed tables replace per-operator progress
    # stores; they contain no model request/reply bodies or arbitrary user KV.
    def recovery_identity(self):
        if not self.path.exists():
            return None
        with self._connection() as db:
            if not db.execute("SELECT 1 FROM sqlite_master WHERE name='runtime_identity'").fetchone():
                return None
            row = db.execute('SELECT document FROM runtime_identity').fetchone()
            return json.loads(row[0]) if row else None

    def incomplete_execution(self):
        with self._connection() as db:
            return (db.execute('SELECT 1 FROM runtime_actions WHERE result IS NULL LIMIT 1').fetchone() is not None
                    or db.execute("SELECT 1 FROM runtime_commits WHERE status!='acked' LIMIT 1").fetchone() is not None)

    @contextmanager
    def _publication_lease(self, timeout=30):
        """Serialize fence replacement with publication, without holding SQLite."""
        import fcntl
        path = self.directory / '.publication.lock'
        descriptor = os.open(path, os.O_RDWR | os.O_CREAT | os.O_CLOEXEC | os.O_NOFOLLOW, 0o600)
        deadline = time.monotonic() + timeout
        try:
            while True:
                try:
                    fcntl.flock(descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
                    break
                except BlockingIOError:
                    if time.monotonic() >= deadline:
                        raise ExecutionConflict('Publication ownership wait expired')
                    time.sleep(.01)
            yield
        finally:
            os.close(descriptor)

    @contextmanager
    def publication_guard(self, fence):
        self._reconcile_published_gc(fence)
        with self._publication_lease():
            with self.execution_guard(fence):
                pass
            yield None

    def claim_execution(self, configuration, fence):
        with self._publication_lease(configuration['limits'].get('admission_timeout_s', 30)):
            attempt = self._claim_execution(configuration, fence)
        self._reconcile_published_gc(fence)
        return attempt

    def _reconcile_published_gc(self, fence):
        from .dataflow_object_lease import exclusive_here
        if not exclusive_here(self.directory / '.dataflow-store') and self.pending_dataflow_gc():
            from .dataflow_gc import recover_published_gc
            result = recover_published_gc(self, fence)
            if result.get('blocked'):
                raise ExecutionConflict('Published artifact reconciliation is still owned or unknown: ' + result['blocked'])

    def _claim_execution(self, configuration, fence):
        from .recovery_contract import document, identifier, RecoveryContractError
        raw = document(configuration).decode()
        identifier(fence)
        with self._transaction() as db:
            old = db.execute('SELECT document FROM runtime_identity').fetchone()
            if old is not None and old[0] != raw:
                raise RecoveryContractError('Immutable execution identity/limits differ')
            if old is None:
                if db.execute('SELECT 1 FROM action_events LIMIT 1').fetchone():
                    raise RecoveryContractError('Historical execution requires read-only migration')
                db.execute('INSERT INTO runtime_identity VALUES (1,?)', (raw,))
            state = json.loads(db.execute('SELECT document FROM execution_state').fetchone()[0])
            attempt = state['attempt']
            db.execute('INSERT OR REPLACE INTO runtime_owner VALUES (1,?,?)', (attempt, fence))
        return attempt

    @contextmanager
    def execution_guard(self, fence):
        """Fence metadata and publication together, not a check-then-write.

        Only the existing driver publishes durable effects. Holding this SQLite
        transaction across publication prevents a replacement owner from being
        admitted between the ownership check and the external write.
        """
        from .recovery_contract import RecoveryContractError
        from .dataflow_object_lease import exclusive_here
        self._reconcile_published_gc(fence)
        with self._transaction() as db:
            if (not exclusive_here(self.directory / '.dataflow-store') and
                    db.execute("SELECT 1 FROM runtime_dataflow_gc WHERE status!='done' LIMIT 1").fetchone()):
                raise ExecutionConflict('Published artifact reconciliation must finish before publication')
            row = db.execute('SELECT attempt,fence FROM runtime_owner').fetchone()
            state = json.loads(db.execute('SELECT document FROM execution_state').fetchone()[0])
            if row is None or row != (state['attempt'], fence):
                raise ExecutionConflict('Stale execution owner cannot publish or mutate progress')
            configuration = json.loads(db.execute('SELECT document FROM runtime_identity').fetchone()[0])
            page_size = db.execute('PRAGMA page_size').fetchone()[0]
            maximum = configuration['limits']['max_control_bytes'] // page_size
            if maximum < db.execute('PRAGMA page_count').fetchone()[0]:
                raise RecoveryContractError('Execution control store exceeds disk budget')
            db.execute('PRAGMA max_page_count=' + str(maximum))
            yield db

    def release_execution(self, fence):
        with self.execution_guard(fence) as db:
            db.execute('DELETE FROM runtime_owner WHERE fence=?', (fence,))

    @staticmethod
    def _artifact_ref_key(ref):
        from .recovery_contract import identifier, RecoveryContractError
        try:
            return identifier(ref.digest), identifier(ref.store_id), identifier(ref.key)
        except (AttributeError, RecoveryContractError):
            raise RecoveryContractError('Invalid artifact reference identity')

    def bind_artifact_owner(self, fence, ref, *, owner, operation_id):
        """Idempotent owner pin in the original journal; fence and quota checked."""
        from .recovery_contract import identifier, RecoveryContractError
        digest, store_id, key = self._artifact_ref_key(ref)
        identifier(owner)
        identifier(operation_id)
        with self.execution_guard(fence) as db:
            for row in db.execute('SELECT digest,store_id,key,owner FROM artifact_owner WHERE operation_id=?',
                                  (operation_id,)):
                if tuple(row) != (digest, store_id, key, owner):
                    raise RecoveryContractError('Artifact operation_id reused with a different pin')
            if db.execute('SELECT 1 FROM artifact_owner WHERE digest=? AND store_id=? AND key=? AND owner=?',
                          (digest, store_id, key, owner)).fetchone() is not None:
                return
            if db.execute('SELECT count(*) FROM artifact_owner').fetchone()[0] >= _MAX_ARTIFACT_OWNERS:
                raise RecoveryContractError('Artifact owner pin count exceeds budget')
            db.execute('INSERT INTO artifact_owner VALUES (?,?,?,?,?)',
                       (digest, store_id, key, owner, operation_id))

    def release_artifact_owner(self, fence, ref, *, owner, operation_id):
        """Idempotent unpin of one owner; never a whole-object GC delete decision."""
        from .recovery_contract import identifier, RecoveryContractError
        digest, store_id, key = self._artifact_ref_key(ref)
        identifier(owner)
        identifier(operation_id)
        with self.execution_guard(fence) as db:
            for row in db.execute('SELECT digest,store_id,key,owner FROM artifact_owner WHERE operation_id=?',
                                  (operation_id,)):
                if tuple(row) != (digest, store_id, key, owner):
                    raise RecoveryContractError('Artifact operation_id reused with a different pin')
            db.execute('DELETE FROM artifact_owner WHERE digest=? AND store_id=? AND key=? AND owner=?',
                       (digest, store_id, key, owner))

    def release_materialized_owner(self, ref, *, owner, pin_operation_id):
        """Drop one driver's handle pin after its execution scope has ended.

        This consumes only the exact original pin capability. It cannot publish,
        refund storage, delete objects or mutate another owner's/historical pin.
        Worker artifact commands continue to require the live execution fence.
        """
        from .recovery_contract import identifier, RecoveryContractError
        digest, store_id, key = self._artifact_ref_key(ref)
        identifier(owner)
        identifier(pin_operation_id)
        with self._transaction() as db:
            row = db.execute('SELECT operation_id FROM artifact_owner WHERE digest=? AND store_id=? AND key=? AND owner=?',
                (digest, store_id, key, owner)).fetchone()
            if row is None:
                return
            if row[0] != pin_operation_id:
                raise RecoveryContractError('Materialized release does not own the original pin')
            db.execute('DELETE FROM artifact_owner WHERE digest=? AND store_id=? AND key=? AND owner=? AND operation_id=?',
                (digest, store_id, key, owner, pin_operation_id))

    def artifact_owners(self, ref, *, limit, cursor=None):
        """Bounded keyset page of pins for one ref; a failed page raises, never empty."""
        from .recovery_contract import identifier, RecoveryContractError
        digest, store_id, key = self._artifact_ref_key(ref)
        if type(limit) is not int or not 1 <= limit <= 1024:
            raise RecoveryContractError('Artifact owner page limit is invalid')
        if cursor is not None:
            identifier(cursor)
        with self._connection() as db:
            if cursor is None:
                rows = db.execute('SELECT owner FROM artifact_owner WHERE digest=? AND store_id=? AND key=? '
                                  'ORDER BY owner LIMIT ?', (digest, store_id, key, limit + 1)).fetchall()
            else:
                rows = db.execute('SELECT owner FROM artifact_owner WHERE digest=? AND store_id=? AND key=? '
                                  'AND owner>? ORDER BY owner LIMIT ?',
                                  (digest, store_id, key, cursor, limit + 1)).fetchall()
        owners = tuple(row[0] for row in rows[:limit])
        return owners, owners[-1] if len(rows) > limit else None

    def bind_dataflow_gc_protocol(self, fence, inode, *, fresh):
        """Only a namespace born under the physical lease protocol is eligible."""
        from .recovery_contract import RecoveryContractError
        with self.execution_guard(fence) as db:
            old = db.execute('SELECT device,inode,eligible FROM runtime_dataflow_gc_protocol').fetchone()
            if old is not None:
                if old[:2] != tuple(inode):
                    raise RecoveryContractError('Published object lease was replaced')
                return bool(old[2])
            legacy = db.execute('SELECT 1 FROM runtime_dataflow_objects LIMIT 1').fetchone()
            legacy = legacy or db.execute('SELECT 1 FROM runtime_dataflow_staging LIMIT 1').fetchone()
            eligible = bool(fresh and not legacy)
            db.execute('INSERT INTO runtime_dataflow_gc_protocol VALUES (1,?,?,?)', (*inode, int(eligible)))
            return eligible

    def dataflow_gc_protocol(self):
        with self._connection() as db:
            if not db.execute("SELECT 1 FROM sqlite_master WHERE name='runtime_dataflow_gc_protocol'").fetchone():
                return None
            return db.execute('SELECT device,inode,eligible FROM runtime_dataflow_gc_protocol').fetchone()

    def pending_dataflow_gc(self):
        with self._connection() as db:
            if not db.execute("SELECT 1 FROM sqlite_master WHERE name='runtime_dataflow_gc'").fetchone():
                return False
            return db.execute("SELECT 1 FROM runtime_dataflow_gc WHERE status!='done' LIMIT 1").fetchone() is not None

    def dataflow_gc_intents(self, *, limit=64):
        if type(limit) is not int or not 1 <= limit <= 64:
            raise ValueError('GC intent page exceeds budget')
        with self._connection() as db:
            rows = db.execute("SELECT gc_id,store_id,digest,bytes,charge_id,device,inode,status FROM runtime_dataflow_gc WHERE status!='done' ORDER BY gc_id LIMIT ?", (limit,)).fetchall()
        names = ('gc_id','store_id','digest','bytes','charge_id','device','inode','status')
        return [dict(zip(names, row)) for row in rows]

    def prepare_dataflow_gc(self, fence, *, store_id, digest, size, charge_id, inode):
        from .recovery_contract import identity, RecoveryContractError
        from .dataflow_object_lease import exclusive_here
        if not exclusive_here(self.directory / '.dataflow-store'):
            raise RecoveryContractError('GC requires the original exclusive physical lease')
        gc_id = identity(['dataflow-published-gc-v1', store_id, digest, charge_id])
        with self.execution_guard(fence) as db:
            obj = db.execute('SELECT bytes,charge_id FROM runtime_dataflow_objects WHERE store_id=? AND digest=?', (store_id,digest)).fetchone()
            if obj != (size, charge_id):
                raise RecoveryContractError('GC does not own this object allocation')
            if db.execute('SELECT 1 FROM artifact_owner WHERE store_id=? AND digest=? LIMIT 1', (store_id,digest)).fetchone():
                raise RecoveryContractError('GC object still has an owner')
            if db.execute('SELECT count(*) FROM runtime_dataflow_gc').fetchone()[0] >= 100000:
                raise RecoveryContractError('GC journal count exceeds budget')
            db.execute('INSERT OR IGNORE INTO runtime_dataflow_gc VALUES (?,?,?,?,?,?,?,?)',
                (gc_id,store_id,digest,size,charge_id,*inode,'prepared'))
        return gc_id

    def confirm_dataflow_gc_quarantine(self, fence, gc_id):
        with self.execution_guard(fence) as db:
            db.execute("UPDATE runtime_dataflow_gc SET status='quarantined' WHERE gc_id=? AND status='prepared'", (gc_id,))

    def finish_dataflow_gc(self, fence, gc_id):
        """Debit exactly the original allocation once; old RPC receipts stay immutable."""
        from .recovery_contract import RecoveryContractError
        from .dataflow_object_lease import exclusive_here
        if not exclusive_here(self.directory / '.dataflow-store'):
            raise RecoveryContractError('GC refund requires exclusive physical reconciliation')
        with self.execution_guard(fence) as db:
            row = db.execute('SELECT store_id,digest,bytes,charge_id,status FROM runtime_dataflow_gc WHERE gc_id=?', (gc_id,)).fetchone()
            if row is None or row[4] not in ('quarantined','done'):
                raise RecoveryContractError('GC lacks verified physical quarantine')
            if row[4] == 'done':
                return 0
            from .dataflow_gc import gc_paths
            target, quarantine = gc_paths(self, dict(gc_id=gc_id, store_id=row[0], digest=row[1]))
            if target.exists() or quarantine.exists():
                raise RecoveryContractError('GC physical removal is not confirmed')
            store_id,digest,size,charge_id,_ = row
            obj = db.execute('SELECT bytes,charge_id FROM runtime_dataflow_objects WHERE store_id=? AND digest=?', (store_id,digest)).fetchone()
            charge = db.execute("SELECT amount FROM runtime_reservations WHERE name='artifact_staging_bytes' AND reservation_id=?", (charge_id,)).fetchone()
            if obj != (size,charge_id) or charge != (size,):
                raise RecoveryContractError('GC allocation charge changed')
            db.execute("UPDATE runtime_reservations SET amount=0 WHERE name='artifact_staging_bytes' AND reservation_id=?", (charge_id,))
            db.execute("UPDATE runtime_budgets SET consumed=consumed-? WHERE name='artifact_staging_bytes'", (size,))
            db.execute('DELETE FROM runtime_dataflow_objects WHERE store_id=? AND digest=?', (store_id,digest))
            db.execute("UPDATE runtime_dataflow_gc SET status='done' WHERE gc_id=?", (gc_id,))
            return size

    def bind_execution_action(self, fence, action_id, declaration):
        from .recovery_contract import document, identifier, RecoveryContractError
        identifier(action_id)
        raw = document(declaration).decode()
        with self.execution_guard(fence) as db:
            old = db.execute('SELECT declaration FROM runtime_actions WHERE action_id=?', (action_id,)).fetchone()
            if old is not None and old[0] != raw:
                raise RecoveryContractError('Action semantic/source identity differs on resume')
            if old is None:
                if db.execute('SELECT count(*) FROM runtime_actions').fetchone()[0] >= 1024:
                    raise RecoveryContractError('Execution action count exceeds budget')
                db.execute('INSERT INTO runtime_actions VALUES (?,?,NULL,NULL)', (action_id, raw))
        return self.execution_action(action_id)

    def execution_action(self, action_id):
        with self._connection() as db:
            row = db.execute('SELECT declaration,snapshot,result FROM runtime_actions WHERE action_id=?', (action_id,)).fetchone()
            if row is None:
                return None
            return dict(zip(('declaration', 'snapshot', 'result'),
                            (json.loads(v) if v is not None else None for v in row)))

    def set_action_snapshot(self, fence, action_id, snapshot):
        from .recovery_contract import document, RecoveryContractError
        raw = document(snapshot).decode()
        with self.execution_guard(fence) as db:
            row = db.execute('SELECT snapshot FROM runtime_actions WHERE action_id=?', (action_id,)).fetchone()
            if row is None or row[0] is not None and row[0] != raw:
                raise RecoveryContractError('Fixed source snapshot differs')
            db.execute('UPDATE runtime_actions SET snapshot=? WHERE action_id=?', (raw, action_id))

    def source_binding(self, action_id):
        with self._connection() as db:
            row = db.execute('SELECT document FROM runtime_sources WHERE action_id=?', (action_id,)).fetchone()
            return json.loads(row[0]) if row else None

    def bind_source(self, fence, action_id, binding):
        from .recovery_contract import document, RecoveryContractError
        raw = document(binding).decode()
        with self.execution_guard(fence) as db:
            old = db.execute('SELECT document FROM runtime_sources WHERE action_id=?', (action_id,)).fetchone()
            if old is not None and old[0] != raw:
                raise RecoveryContractError('Fixed source binding differs')
            db.execute('INSERT OR IGNORE INTO runtime_sources VALUES (?,?)', (action_id, raw))

    def complete_execution_action(self, fence, action_id, result):
        from .recovery_contract import document, RecoveryContractError
        raw = document(result).decode()
        with self.execution_guard(fence) as db:
            if db.execute("SELECT 1 FROM runtime_commits WHERE action_id=? AND status!='acked'", (action_id,)).fetchone():
                raise RecoveryContractError('Action still has unconfirmed commits or ACKs')
            row = db.execute('SELECT result FROM runtime_actions WHERE action_id=?', (action_id,)).fetchone()
            if row is None or row[0] is not None and row[0] != raw:
                raise RecoveryContractError('Action completion differs')
            db.execute('UPDATE runtime_actions SET result=? WHERE action_id=?', (raw, action_id))

    def bind_commit_scope(self, fence, scope_id, parent_action_id, declaration):
        """Bounded child commit identity; it is not a nested Dataset action."""
        from .recovery_contract import document, identifier, RecoveryContractError
        identifier(scope_id)
        raw = document(declaration, 65536).decode()
        with self.execution_guard(fence) as db:
            db.execute('CREATE TABLE IF NOT EXISTS runtime_commit_scopes (scope_id TEXT PRIMARY KEY, parent_action_id TEXT NOT NULL, declaration TEXT NOT NULL)')
            if db.execute('SELECT 1 FROM runtime_actions WHERE action_id=?',(parent_action_id,)).fetchone() is None:
                raise RecoveryContractError('Commit scope has no native parent action')
            old = db.execute('SELECT parent_action_id,declaration FROM runtime_commit_scopes WHERE scope_id=?',(scope_id,)).fetchone()
            if old is not None:
                if old != (parent_action_id,raw):
                    raise RecoveryContractError('Native commit scope identity changed')
                return
            limit = json.loads(db.execute('SELECT document FROM runtime_identity').fetchone()[0])['limits']['max_commits']
            if db.execute('SELECT count(*) FROM runtime_commit_scopes').fetchone()[0] >= limit:
                raise RecoveryContractError('Native commit scope count exceeds budget')
            db.execute('INSERT INTO runtime_commit_scopes VALUES (?,?,?)',(scope_id,parent_action_id,raw))

    def begin_commit(self, fence, action_id, sequence, intent):
        from .recovery_contract import document, identity, RecoveryContractError
        if type(sequence) is not int or not 1 <= sequence <= 16384:
            raise RecoveryContractError('Invalid commit sequence')
        raw = document(intent).decode()
        with self.execution_guard(fence) as db:
            configuration = json.loads(db.execute('SELECT document FROM runtime_identity').fetchone()[0])
            commit_id = identity([configuration['run_id'], action_id, sequence])
            old = db.execute('SELECT intent FROM runtime_commits WHERE commit_id=?', (commit_id,)).fetchone()
            if old is not None and old[0] != raw:
                raise RecoveryContractError('Commit input/state/sink identity differs')
            if old is None:
                config = json.loads(db.execute('SELECT document FROM runtime_identity').fetchone()[0])
                if db.execute('SELECT count(*) FROM runtime_commits').fetchone()[0] >= config['limits']['max_commits']:
                    raise RecoveryContractError('Run commit count exceeds budget')
                bound = db.execute('SELECT 1 FROM runtime_actions WHERE action_id=?', (action_id,)).fetchone()
                if not bound and db.execute("SELECT 1 FROM sqlite_master WHERE name='runtime_commit_scopes'").fetchone():
                    bound = db.execute('SELECT 1 FROM runtime_commit_scopes WHERE scope_id=?',(action_id,)).fetchone()
                if not bound:
                    raise RecoveryContractError('Commit has no bound action or native scope')
                previous = db.execute('SELECT sequence,status,intent FROM runtime_commits WHERE action_id=? ORDER BY sequence DESC LIMIT 1', (action_id,)).fetchone()
                if (previous is None and sequence != 1 or previous is not None and
                        (sequence != previous[0] + 1 or previous[1] != 'acked')):
                    raise RecoveryContractError('Previous commit has not completed')
                if previous is not None:
                    position = json.loads(previous[2])['position']
                    if (intent['position']['source_id'] != position['source_id'] or
                            intent['position']['next_ordinal'] < position['next_ordinal']):
                        raise RecoveryContractError('Source position cannot regress or change lineage')
                db.execute('INSERT INTO runtime_commits VALUES (?,?,?,?,?,NULL)',
                           (commit_id, action_id, sequence, 'pending', raw))
        return self.execution_commit(commit_id)

    def execution_commit(self, commit_id):
        with self._connection() as db:
            db.execute('BEGIN')
            return self._execution_commit(db, commit_id)

    @staticmethod
    def _execution_commit(db, commit_id):
        row = db.execute('SELECT action_id,sequence,status,intent,prepared FROM runtime_commits WHERE commit_id=?', (commit_id,)).fetchone()
        if row is None:
            return None
        receipts = {key: json.loads(raw) for key, raw in db.execute(
            'SELECT sink_id,document FROM runtime_receipts WHERE commit_id=?', (commit_id,))}
        return dict(commit_id=commit_id, action_id=row[0], sequence=row[1], status=row[2],
                    intent=json.loads(row[3]), prepared=json.loads(row[4]) if row[4] else None,
                    receipts=receipts)

    def latest_execution_commit(self, action_id, *, confirmed=False):
        with self._connection() as db:
            sql = 'SELECT commit_id FROM runtime_commits WHERE action_id=?'
            if confirmed:
                sql += " AND status!='pending'"
            row = db.execute(sql + ' ORDER BY sequence DESC LIMIT 1', (action_id,)).fetchone()
            return self._execution_commit(db, row[0]) if row else None

    def iter_execution_commits(self, action_id, *, through_sequence=None):
        """Indexed bounded pages; callers retain at most one decoded commit."""
        after = 0
        while True:
            with self._connection() as db:
                ids = db.execute('SELECT sequence,commit_id FROM runtime_commits WHERE action_id=? AND sequence>? AND sequence<=? ORDER BY sequence LIMIT 16',
                    (action_id, after, through_sequence if through_sequence is not None else 16384)).fetchall()
            if not ids:
                return
            for after, commit_id in ids:
                yield self.execution_commit(commit_id)

    def execution_commits(self, action_id):
        with self._connection() as db:
            db.execute('BEGIN')
            rows = db.execute('SELECT commit_id FROM runtime_commits WHERE action_id=? ORDER BY sequence LIMIT 16385', (action_id,)).fetchall()
            if len(rows) > 16384:
                raise ValueError('Commit history exceeds budget')
            return [self._execution_commit(db, row[0]) for row in rows]

    def prepare_commit(self, fence, commit_id, prepared):
        """Validate pages outside write transactions and bind references in batches.

        Provisional reference ownership is separate from allocation ownership:
        equal content-addressed state can be allocated again by a later cut.
        Only the final prepared transition protects these references. A failed
        traversal therefore leaves its unpublished files eligible for cleanup.
        """
        from .recovery_contract import document, RecoveryContractError
        from itertools import islice
        raw = document(prepared).decode()
        limits = self.recovery_identity()['limits']
        state_limit, remaining = limits['max_state_bytes'], limits['max_artifacts'] * 4

        def references(value, depth=0):
            nonlocal remaining
            if depth > 32:
                raise RecoveryContractError('Prepared reference nesting exceeds budget')
            if type(value) is dict:
                path = value.get('path')
                if type(path) is str:
                    remaining -= 1
                    if remaining < 0:
                        raise RecoveryContractError('Prepared artifact count exceeds budget')
                    yield path
                    if value.get('format') == 'paged-artifacts-v1':
                        from .artifact_manifest import manifest_pages
                        for page_path, page in manifest_pages(self.directory, value,
                                maximum=limits['max_artifacts']):
                            remaining -= 1
                            if remaining < 0:
                                raise RecoveryContractError('Prepared artifact count exceeds budget')
                            yield page_path
                            yield from references(page, depth + 1)
                    if 'state_id' in value:
                        from .artifact_manifest import _read
                        relative = Path(path)
                        if (relative.is_absolute() or '..' in relative.parts
                                or not relative.parts or relative.parts[0] != 'state'):
                            raise RecoveryContractError('State artifact escapes its owner')
                        raw_state = _read(self.directory / relative, state_limit,
                                          size=value['bytes'], digest=value['sha256'])
                        yield from references(json.loads(raw_state), depth + 1)
                for child in value.values():
                    yield from references(child, depth + 1)
            elif type(value) is list:
                for child in value:
                    yield from references(child, depth + 1)

        iterator = references(prepared)
        while batch := tuple(set(islice(iterator, 64))):
            # Already durable references need no new write. Decode and hash the
            # next bounded page before entering either journal transaction.
            with self._connection() as db:
                pending = [path for path in batch if db.execute(
                    'SELECT 1 FROM runtime_artifacts WHERE path=? AND protected=0',
                    (path,)).fetchone()]
            if pending:
                with self.execution_guard(fence) as db:
                    for path in pending:
                        row = db.execute('SELECT protected,reference_commit_id FROM runtime_artifacts WHERE path=?',
                                         (path,)).fetchone()
                        if row is None or row[0]:
                            continue
                        if row[1] not in (None, commit_id):
                            raise RecoveryContractError('Artifact has another pending reference owner')
                        db.execute('UPDATE runtime_artifacts SET reference_commit_id=? WHERE path=?',
                                   (commit_id, path))
        with self.execution_guard(fence) as db:
            row = db.execute('SELECT status,prepared FROM runtime_commits WHERE commit_id=?', (commit_id,)).fetchone()
            if row is None or row[0] != 'pending':
                if row is not None and row[1] == raw:
                    return
                raise RecoveryContractError('Commit cannot be prepared in its current state')
            db.execute('UPDATE runtime_artifacts SET protected=1 WHERE reference_commit_id=?', (commit_id,))
            db.execute("UPDATE runtime_commits SET status='prepared',prepared=? WHERE commit_id=?", (raw, commit_id))

    def confirm_commit(self, fence, commit_id, *, expected, status):
        from .recovery_contract import COMMIT_STATES, RecoveryContractError
        if status not in COMMIT_STATES or expected not in COMMIT_STATES or COMMIT_STATES.index(status) != COMMIT_STATES.index(expected) + 1:
            raise RecoveryContractError('Invalid commit state transition')
        with self.execution_guard(fence) as db:
            row = db.execute('SELECT status,intent FROM runtime_commits WHERE commit_id=?', (commit_id,)).fetchone()
            if row is None:
                raise RecoveryContractError('Unknown commit')
            if COMMIT_STATES.index(row[0]) >= COMMIT_STATES.index(status):
                return
            if row[0] != expected:
                raise RecoveryContractError('Commit state changed')
            intent = json.loads(row[1])
            if status == 'data_committed':
                actual = {r[0] for r in db.execute('SELECT sink_id FROM runtime_receipts WHERE commit_id=?', (commit_id,))}
                if actual != set(intent['sinks']):
                    raise RecoveryContractError('Not all sink receipts are durable')
            db.execute('UPDATE runtime_commits SET status=? WHERE commit_id=?', (status, commit_id))

    def publish_receipt(self, db, commit_id, sink_id, receipt):
        """Called inside execution_guard, including the external publication."""
        from .recovery_contract import document, RecoveryContractError
        raw = document(receipt).decode()
        old = db.execute('SELECT document FROM runtime_receipts WHERE commit_id=? AND sink_id=?', (commit_id, sink_id)).fetchone()
        if old is not None and old[0] != raw:
            raise RecoveryContractError('Durable receipt changed')
        db.execute('INSERT OR IGNORE INTO runtime_receipts VALUES (?,?,?)', (commit_id, sink_id, raw))

    def begin_dataflow_staging(self, fence, reservation_id, store_id, inode, size, *, ceiling):
        """Atomically bind byte admission to an already locked physical owner."""
        from .recovery_contract import identifier, RecoveryContractError
        identifier(reservation_id)
        identifier(store_id)
        if (type(inode) is not tuple or len(inode) != 2
                or any(type(v) is not int or not 0 <= v < 2**63 for v in inode)
                or type(size) is not int or size <= 0):
            raise RecoveryContractError('Invalid staging owner or size')
        with self.execution_guard(fence) as db:
            old = db.execute('SELECT store_id,device,inode,initial_bytes FROM runtime_dataflow_staging WHERE reservation_id=?', (reservation_id,)).fetchone()
            expected = (store_id, *inode, size)
            if old is not None:
                if old != expected:
                    raise RecoveryContractError('Staging ownership changed')
                return size
            if db.execute("SELECT 1 FROM runtime_reservations WHERE name='artifact_staging_bytes' AND reservation_id=?", (reservation_id,)).fetchone():
                raise RecoveryContractError('Cannot attach new ownership to a legacy reservation')
            self.reserve_budgets_in_transaction(db, reservation_id, {'artifact_staging_bytes': (size, ceiling)})
            db.execute('INSERT INTO runtime_dataflow_staging VALUES (?,?,?,?,?,NULL)', (reservation_id, *expected))
            return size

    def prepare_dataflow_staging(self, fence, reservation_id, ref):
        """Record the exact future object before the worker may publish it."""
        from dataclasses import asdict
        from .recovery_contract import document, RecoveryContractError
        raw = document(asdict(ref)).decode()
        with self.execution_guard(fence) as db:
            row = db.execute('SELECT store_id,publication FROM runtime_dataflow_staging WHERE reservation_id=?', (reservation_id,)).fetchone()
            amount = db.execute("SELECT amount FROM runtime_reservations WHERE name='artifact_staging_bytes' AND reservation_id=?", (reservation_id,)).fetchone()
            if row is None or row[0] != ref.store_id or amount is None or ref.size_bytes > amount[0]:
                raise RecoveryContractError('Publication lacks its original staging admission')
            if row[1] is not None and row[1] != raw:
                raise RecoveryContractError('Staging publication changed')
            if db.execute('SELECT 1 FROM runtime_dataflow_settlements WHERE reservation_id=?', (reservation_id,)).fetchone():
                raise RecoveryContractError('Settled staging cannot prepare publication')
            db.execute('UPDATE runtime_dataflow_staging SET publication=? WHERE reservation_id=?', (raw, reservation_id))

    def dataflow_staging_owner(self, fence, reservation_id):
        from .recovery_contract import identifier
        identifier(reservation_id)
        with self.execution_guard(fence) as db:
            row = db.execute('SELECT s.store_id,s.device,s.inode,s.publication,d.reservation_id IS NOT NULL FROM runtime_dataflow_staging s LEFT JOIN runtime_dataflow_settlements d ON d.reservation_id=s.reservation_id WHERE s.reservation_id=?', (reservation_id,)).fetchone()
        return (row[0], row[1], row[2], json.loads(row[3]) if row[3] else None, bool(row[4])) if row else None

    def dataflow_recovery_cursor(self, fence, *, value=None):
        # Bounded recovery rotates through unknown/live owners instead of forever
        # starving later reservations behind the first page. This is scan progress,
        # never ownership or completion evidence.
        from .recovery_contract import identifier
        if value:
            identifier(value)
        with self.execution_guard(fence) as db:
            if value is not None:
                db.execute('INSERT OR REPLACE INTO runtime_dataflow_recovery VALUES (1,?)', (value,))
                return value
            row = db.execute('SELECT cursor FROM runtime_dataflow_recovery WHERE singleton=1').fetchone()
            return row[0] if row else ''

    def pending_dataflow_staging(self, fence, *, after='', limit=128):
        from .recovery_contract import identifier
        if after:
            identifier(after)
        if type(limit) is not int or not 1 <= limit <= 128:
            raise ValueError('Invalid staging page size')
        with self.execution_guard(fence) as db:
            rows = db.execute('SELECT s.reservation_id,s.store_id,s.device,s.inode,s.publication FROM runtime_dataflow_staging s LEFT JOIN runtime_dataflow_settlements d ON d.reservation_id=s.reservation_id WHERE d.reservation_id IS NULL AND s.reservation_id>? ORDER BY s.reservation_id LIMIT ?', (after, limit)).fetchall()
        return [(r, s, d, i, json.loads(p) if p else None) for r, s, d, i, p in rows]

    def grow_dataflow_staging(self, fence, reservation_id, size, *, ceiling):
        """Admit an increase before writing, in the same fenced byte ledger.

        Retransmission and stale smaller requests do not charge twice. A settled
        reservation cannot be reopened, including after an unknown RPC reply.
        """
        from .recovery_contract import identifier, RecoveryContractError
        identifier(reservation_id)
        if any(type(v) is not int or not 0 < v < 2**63 for v in (size, ceiling)):
            raise RecoveryContractError('Invalid staging growth')
        with self.execution_guard(fence) as db:
            if db.execute('SELECT 1 FROM runtime_dataflow_settlements WHERE reservation_id=?',
                          (reservation_id,)).fetchone():
                raise RecoveryContractError('Settled staging cannot grow')
            old = db.execute("SELECT amount FROM runtime_reservations WHERE name='artifact_staging_bytes' AND reservation_id=?",
                             (reservation_id,)).fetchone()
            budget = db.execute("SELECT consumed,ceiling FROM runtime_budgets WHERE name='artifact_staging_bytes'").fetchone()
            if old is None or budget is None or budget[1] != ceiling:
                raise RecoveryContractError('Unknown staging reservation or changed ceiling')
            delta = max(0, size - old[0])
            if budget[0] + delta > ceiling:
                raise ExecutionBudgetExceeded('artifact_staging_bytes')
            db.execute("UPDATE runtime_budgets SET consumed=consumed+? WHERE name='artifact_staging_bytes'", (delta,))
            db.execute("UPDATE runtime_reservations SET amount=amount+? WHERE name='artifact_staging_bytes' AND reservation_id=?",
                       (delta, reservation_id))
            self._storage_usage(db)
            return old[0] + delta

    def settle_dataflow_staging(self, fence, reservation_id, ref):
        """Replace the staging maximum with unique durable bytes, atomically.

        No refund for an unknown publication: callers release only after deleting
        their temporary file, or settle a verified immutable object. Repeat RPCs
        bind the same settlement and cannot refund twice.
        """
        from .recovery_contract import identifier, RecoveryContractError
        identifier(reservation_id)
        store_id, digest, size = (ref.store_id, ref.digest, ref.size_bytes) if ref is not None else (None, None, 0)
        with self.execution_guard(fence) as db:
            old = db.execute('SELECT store_id,digest,bytes,charged FROM runtime_dataflow_settlements WHERE reservation_id=?',
                             (reservation_id,)).fetchone()
            if old:
                if old[:3] != (store_id, digest, size):
                    raise RecoveryContractError('Artifact settlement changed')
                return old[3]
            owner = db.execute('SELECT publication FROM runtime_dataflow_staging WHERE reservation_id=?', (reservation_id,)).fetchone()
            if owner is not None and ref is not None:
                from dataclasses import asdict
                if owner[0] is None or json.loads(owner[0]) != asdict(ref):
                    raise RecoveryContractError('Artifact settlement differs from publication intent')
            reservation = db.execute("SELECT amount FROM runtime_reservations WHERE name='artifact_staging_bytes' AND reservation_id=?",
                                     (reservation_id,)).fetchone()
            if reservation is None or size > reservation[0]:
                raise RecoveryContractError('Artifact settlement exceeds its staging reservation')
            prior = db.execute('SELECT bytes FROM runtime_dataflow_objects WHERE store_id=? AND digest=?', (store_id,digest)).fetchone() if digest else None
            if prior and prior[0] != size:
                raise RecoveryContractError('Immutable artifact size changed')
            charged = 0 if prior else size
            if digest:
                db.execute('INSERT OR IGNORE INTO runtime_dataflow_objects(store_id,digest,bytes,charge_id) VALUES (?,?,?,?)', (store_id, digest, size, reservation_id))
            db.execute("UPDATE runtime_budgets SET consumed=consumed-? WHERE name='artifact_staging_bytes'",
                       (reservation[0] - charged,))
            db.execute("UPDATE runtime_reservations SET amount=? WHERE name='artifact_staging_bytes' AND reservation_id=?",
                       (charged, reservation_id))
            db.execute('INSERT INTO runtime_dataflow_settlements VALUES (?,?,?,?,?)', (reservation_id, store_id, digest, size, charged))
            return charged

    def transition_effect(self, fence, action_id, command, *, ceilings):
        """Native generic effect intent and budget share one fenced transaction.

        A reserved record proves no send yet; a sent/unknown record never grants
        another send. Lost replies may repeat the same transition idempotently.
        """
        from .contracts.dataflow_errors import DataflowError
        from .recovery_contract import document, identifier, identity
        from dataclasses import asdict
        for value in (action_id, command.effect_id, command.actor_id, command.request_digest):
            identifier(value)
        with self.execution_guard(fence) as db:
            row = db.execute('SELECT document FROM runtime_effects WHERE action_id=? AND effect_id=?',
                             (action_id, command.effect_id)).fetchone()
            record = json.loads(row[0]) if row else None
            if record and (record['request_digest'] != command.request_digest or record['actor_id'] != command.actor_id):
                raise DataflowError('INVALID_CONTRACT', 'Effect identity reused for a different actor/request')
            state = record['state'] if record else 'absent'
            if command.kind == 'lookup':
                return record or {'state': 'absent'}
            if command.kind == 'reserve':
                if state == 'absent':
                    if db.execute('SELECT count(*) FROM runtime_effects').fetchone()[0] >= 100000:
                        raise DataflowError('BUDGET_EXCEEDED', 'Native effect record count exceeds budget')
                    charges = {name: (amount, ceilings[name]) for name, amount in command.budget.items()}
                    if charges:
                        reservation = identity(['native-effect', action_id, command.effect_id])
                        self.reserve_budgets_in_transaction(db, reservation, charges)
                    record = {'state': 'reserved', 'actor_id': command.actor_id,
                              'request_digest': command.request_digest,
                              'budget': dict(command.budget), 'result_ref': None, 'receipt_ref': None}
                elif state != 'reserved' or record.get('budget') != dict(command.budget):
                    raise DataflowError('INVALID_CONTRACT', 'Effect cannot acquire a new reservation')
            else:
                transitions = {'mark_sent': ('reserved', 'sent'), 'confirm': ('sent', 'confirmed'),
                               'unknown': ('sent', 'unknown'), 'release_unsent': ('reserved', 'unsent')}
                if command.kind not in transitions:
                    raise DataflowError('INVALID_CONTRACT', 'Unknown native effect transition')
                before, after = transitions[command.kind]
                if state not in (before, after):
                    raise DataflowError('UNKNOWN_EFFECT', 'Effect state does not permit this transition')
                record = dict(record, state=after)
                if command.kind == 'confirm':
                    if command.payload_ref is None:
                        raise DataflowError('INVALID_CONTRACT', 'Confirm requires a durable result reference')
                    result_ref = asdict(command.payload_ref)
                    if state == after and record['result_ref'] != result_ref:
                        raise DataflowError('INVALID_CONTRACT', 'Confirmed effect result changed')
                    record['result_ref'] = result_ref
            db.execute('INSERT OR REPLACE INTO runtime_effects VALUES (?,?,?)',
                       (action_id, command.effect_id, document(record).decode()))
            return record

    def reserve_budget(self, fence, name, reservation_id, amount, ceiling):
        """Cumulative admission. Unknown consumed work is never refunded."""
        return self.reserve_budgets(fence, reservation_id, {name: (amount, ceiling)})[name]

    def reserve_budgets(self, fence, reservation_id, charges):
        """Atomically admit components and their existing aggregate budget.

        Payload and metadata remain separate counters in this same control DB;
        no component can consume capacity while the aggregate rejects it.
        """
        with self.execution_guard(fence) as db:
            return self.reserve_budgets_in_transaction(db, reservation_id, charges)

    @staticmethod
    def reserve_budgets_in_transaction(db, reservation_id, charges):
        from .recovery_contract import identifier, RecoveryContractError, identity
        identifier(reservation_id)
        if type(charges) is not dict or not 1 <= len(charges) <= 4:
            raise RecoveryContractError('Invalid bounded budget component set')
        for name, pair in charges.items():
            identifier(name)
            if type(pair) not in (tuple, list) or len(pair) != 2 or any(
                    type(v) is not int or not 0 <= v < 2**63 for v in pair):
                raise RecoveryContractError('Invalid execution budget reservation')
        if len(charges) > 1:
            # A different component set is a different reservation, so adding
            # a component cannot reuse the already charged aggregate entry.
            reservation_id = identity({'reservation': reservation_id, 'components': sorted(charges)})
        pending, totals = {}, {}
        for name, (amount, ceiling) in charges.items():
            old = db.execute('SELECT amount FROM runtime_reservations WHERE name=? AND reservation_id=?', (name, reservation_id)).fetchone()
            budget = db.execute('SELECT consumed,ceiling FROM runtime_budgets WHERE name=?', (name,)).fetchone()
            if budget is not None and budget[1] != ceiling:
                raise RecoveryContractError('Budget ceiling differs across attempts')
            if old is not None:
                if old[0] != amount or budget is None:
                    raise RecoveryContractError('Budget reservation changed')
                totals[name] = budget[0]
                continue
            consumed = (budget[0] if budget else 0) + amount
            if consumed > ceiling:
                raise ExecutionBudgetExceeded(name)
            pending[name] = (consumed, ceiling, amount)
            totals[name] = consumed
        count = db.execute('SELECT items FROM runtime_reservation_count WHERE singleton=1').fetchone()[0]
        if count + len(pending) > 100000:
            raise RecoveryContractError('Reservation count exceeds budget')
        for name, (consumed, ceiling, amount) in pending.items():
            db.execute('INSERT OR REPLACE INTO runtime_budgets VALUES (?,?,?)', (name, consumed, ceiling))
            db.execute('INSERT INTO runtime_reservations VALUES (?,?,?)', (name, reservation_id, amount))
        if 'artifact_staging_bytes' in charges:
            RunJournal._storage_usage(db)
        return totals

    def allocate_artifact(self, fence, path, *, commit_id=None, size=0, atomic=True):
        """Record ownership before allocation, including the atomic staging file."""
        from .recovery_contract import RecoveryContractError
        relative = Path(path)
        if (relative.is_absolute() or '..' in relative.parts or not relative.parts
                or relative.parts[0] not in {'state', 'materializations', 'scratch'}):
            raise RecoveryContractError('Artifact allocation escapes run storage')
        with self.execution_guard(fence) as db:
            self._artifact_path(db, str(relative))
            limit = json.loads(db.execute('SELECT document FROM runtime_identity').fetchone()[0])['limits']['max_artifacts']
            allocations = [(str(relative), commit_id)]
            if atomic:
                allocations.append((str(relative) + '.staging', None))
            for name, owner in allocations:
                previous = db.execute('SELECT commit_id FROM runtime_artifacts WHERE path=?', (name,)).fetchone()
                if previous is not None:
                    if previous[0] != owner:
                        raise RecoveryContractError('Artifact owner differs')
                    continue
                if db.execute('SELECT items FROM runtime_storage WHERE singleton=1').fetchone()[0] >= limit:
                    raise RecoveryContractError('Artifact allocation count exceeds budget')
                db.execute('INSERT INTO runtime_artifacts(path,commit_id) VALUES (?,?)', (name, owner))
            if size:
                if type(size) is not int or size < 0:
                    raise RecoveryContractError('Invalid artifact byte reservation')
                db.executemany('UPDATE runtime_artifacts SET bytes=? WHERE path=?', [(size, name) for name, _ in allocations])
                self._storage_usage(db)

    def resize_artifact(self, fence, path, size, *, atomic=False):
        """Reserve a known file's next size before writing, then settle actual bytes."""
        from .recovery_contract import RecoveryContractError
        if type(size) is not int or size < 0:
            raise RecoveryContractError('Invalid artifact byte reservation')
        with self.execution_guard(fence) as db:
            for name in (path, path + '.staging') if atomic else (path,):
                if db.execute('SELECT path FROM runtime_artifacts WHERE path=?', (name,)).fetchone() is None:
                    raise RecoveryContractError('Artifact byte reservation has no registered owner')
                db.execute('UPDATE runtime_artifacts SET bytes=? WHERE path=?', (size, name))
            self._storage_usage(db)

    @staticmethod
    def _storage_usage(db):
        from .recovery_contract import RecoveryContractError
        total, objects = db.execute('SELECT bytes,object_bytes FROM runtime_storage WHERE singleton=1').fetchone()
        admission = db.execute("SELECT consumed FROM runtime_budgets WHERE name='image_scratch_admission_bytes'").fetchone()
        total = max(total, total - objects + (admission[0] if admission else 0))
        metadata = db.execute("SELECT consumed FROM runtime_budgets WHERE name='object_namespace_metadata_bytes'").fetchone()
        total += metadata[0] if metadata else 0
        limit = json.loads(db.execute('SELECT document FROM runtime_identity').fetchone()[0])['limits']['max_scratch_bytes']
        # Both are original journal-owned byte ledgers. Dataflow staging and
        # immutable objects may coexist with native Lance/legacy temporaries;
        # checking either component alone admits up to twice the shared budget.
        dataflow = db.execute("SELECT consumed,ceiling FROM runtime_budgets WHERE name='artifact_staging_bytes'").fetchone()
        if dataflow is not None:
            total += dataflow[0]
            limit = min(limit, dataflow[1])
        if total > limit:
            raise ExecutionBudgetExceeded('max_scratch_bytes', 'Runtime storage exceeds scratch admission budget')
        return total

    def storage_usage(self):
        with self._connection() as db:
            return self._storage_usage(db)

    def release_temporaries(self, fence, names):
        """Forget only absent private files after their producer has joined.

        This never deletes files or scans unrelated storage. Durable artifact
        registrations and any prepared reference remain owned by the journal.
        """
        from .recovery_contract import RecoveryContractError
        if type(names) not in (list, tuple) or len(names) > 128:
            raise RecoveryContractError('Temporary release batch exceeds budget')
        with self.execution_guard(fence) as db:
            selected = set(names)
            for name in selected:
                relative = Path(name)
                if not (relative.parts[0] == 'scratch' or name.endswith('.staging')
                        or relative.name.startswith('.pending-')):
                    raise RecoveryContractError('Release requires a private temporary artifact')
                path = self._artifact_path(db, name)
                if any(p.is_symlink() for p in (path, *path.parents)) or path.exists():
                    raise RecoveryContractError('Temporary producer has not removed its file')
                owner = db.execute('SELECT commit_id FROM runtime_artifacts WHERE path=?', (name,)).fetchone()
                if owner is not None and owner[0] is not None:
                    raise RecoveryContractError('Cannot release a commit-owned artifact')
            for name in selected:
                protected = db.execute('SELECT protected FROM runtime_artifacts WHERE path=?', (name,)).fetchone()
                if protected is not None and protected[0]:
                    raise RecoveryContractError('Prepared commit still references temporary artifact')
            db.executemany('DELETE FROM runtime_artifacts WHERE path=?', [(n,) for n in selected])

    def object_roots(self):
        """Only this run's registered namespaces; never scan the logical store."""
        if not self.path.exists():
            return []
        with self._connection() as db:
            if not db.execute("SELECT 1 FROM sqlite_master WHERE name='runtime_object_roots'").fetchone():
                return []
            rows = db.execute('SELECT root_id,logical_root,physical_root,run_id FROM runtime_object_roots LIMIT 65').fetchall()
        from .recovery_contract import RecoveryContractError
        if len(rows)>64:
            raise RecoveryContractError('Object resource count exceeds ceiling')
        return [dict(zip(('root_id','logical_root','physical_root','run_id'), row)) for row in rows]

    def budget_consumed(self,name):
        if not self.path.exists():return 0
        with self._connection() as db:
            row=db.execute('SELECT consumed FROM runtime_budgets WHERE name=?',(name,)).fetchone()
            return row[0] if row else 0

    def register_object_root(self, fence, record):
        from .recovery_contract import RecoveryContractError, document
        document(record,8192)
        with self.execution_guard(fence) as db:
            from .recovery_contract import identity
            binding=json.loads(db.execute('SELECT document FROM runtime_identity').fetchone()[0])
            if (set(record)!={'root_id','logical_root','physical_root','run_id'}
                    or record['run_id']!=binding['run_id']
                    or record['root_id']!=identity({'logical_root':record['logical_root']})
                    or record['physical_root']!=str(Path(record['logical_root'])/'.demiflow-objects'/record['run_id'])):
                raise RecoveryContractError('Object resource escapes its run namespace')
            old=db.execute('SELECT logical_root,physical_root,run_id FROM runtime_object_roots WHERE root_id=?',
                           (record['root_id'],)).fetchone()
            expected=tuple(record[k] for k in ('logical_root','physical_root','run_id'))
            if old is not None:
                if old!=expected:
                    raise RecoveryContractError('Object resource identity differs')
                return False
            if Path(record['physical_root']).exists():
                raise RecoveryContractError('Object namespace is not owned by this run')
            if db.execute('SELECT count(*) FROM runtime_object_roots').fetchone()[0]>=64:
                raise RecoveryContractError('Object resource count exceeds ceiling')
            db.execute('INSERT INTO runtime_object_roots VALUES (?,?,?,?)',
                       (record['root_id'],*expected))
            return True

    def artifact_name(self, path):
        from .recovery_contract import RecoveryContractError
        path=Path(path).absolute()
        matches=[r for r in self.object_roots() if path.is_relative_to(Path(r['physical_root']))]
        if len(matches)>1:
            raise RecoveryContractError('Ambiguous artifact resource ownership')
        if matches:
            root=matches[0]
            return str(Path('materializations')/('@objects-'+root['root_id'])/path.relative_to(root['physical_root']))
        try:
            return str(path.relative_to(self.directory))
        except ValueError as exc:
            raise RecoveryContractError('Artifact has no registered owner') from exc

    def _artifact_path(self, db, name):
        from .recovery_contract import RecoveryContractError
        relative=Path(name)
        if relative.is_absolute() or '..' in relative.parts:
            raise RecoveryContractError('Artifact path escapes its owner')
        if len(relative.parts)>1 and relative.parts[0]=='materializations' and relative.parts[1].startswith('@objects-'):
            root_id=relative.parts[1][len('@objects-'):]
            row=db.execute('SELECT logical_root,physical_root,run_id FROM runtime_object_roots WHERE root_id=?',(root_id,)).fetchone()
            if row is None:
                raise RecoveryContractError('Artifact object resource is not registered')
            from .object_resources import inspect_namespace
            inspect_namespace(dict(root_id=root_id,logical_root=row[0],physical_root=row[1],run_id=row[2]))
            return Path(row[1]).joinpath(*relative.parts[2:])
        return self.directory/relative

    def clean_orphans(self, fence):
        """Bounded ownership-index audit; file I/O never holds the control lock.

        prepare_commit and native result persistence atomically protect their
        references. Cleanup consults those indexed facts, never every old state
        document. The publication lease prevents an owner switch during unlink.
        """
        from .recovery_contract import RecoveryContractError
        after, removed = '', 0
        while True:
            with self._connection() as db:
                rows = db.execute("SELECT path FROM runtime_artifacts AS a WHERE protected=0 AND path>? AND NOT EXISTS (SELECT 1 FROM runtime_commits AS c WHERE c.commit_id=a.commit_id AND c.prepared IS NOT NULL) ORDER BY path LIMIT 64", (after,)).fetchall()
                paths = [(name, self._artifact_path(db, name)) for (name,) in rows]
            if not paths:
                return removed
            with self.publication_guard(fence):
                for name, path in paths:
                    if any(p.is_symlink() for p in (path, *path.parents)):
                        raise RecoveryContractError('Orphan path contains a symlink')
                    if path.exists():
                        path.unlink()
                        removed += 1
                with self.execution_guard(fence) as db:
                    db.executemany('DELETE FROM runtime_artifacts WHERE path=? AND protected=0', rows)
            after = rows[-1][0]

    def finish_execution(self, fence, status):
        if status not in {'succeeded', 'failed', 'cancelled'}:
            raise ValueError('Invalid execution outcome')
        with self.execution_guard(fence) as db:
            value = json.loads(db.execute('SELECT document FROM execution_state').fetchone()[0])
            if value['status'] == 'stopping':
                status = 'cancelled'
            elif value['status'] != 'running':
                raise ExecutionConflict('Execution lifecycle changed')
            value.update(status=status, revision=value['revision'] + 1, updated_at=time.time())
            raw = encoded(value).decode()
            db.execute('UPDATE execution_state SET document=? WHERE singleton=1', (raw,))
            db.execute('UPDATE attempts SET document=? WHERE attempt=?', (raw, value['attempt']))
            db.execute('DELETE FROM runtime_owner WHERE fence=?', (fence,))
