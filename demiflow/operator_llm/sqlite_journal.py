"""Compact, durable HTTP call results for resume; business rows stay in Dataset.

Entry: SQLitePromptJournal(path). lookup/reserve read a request key; response
commits the original provider result. Images remain in the input object store:
request metadata records their digest, never a second copy of inline base64.
The key still hashes the exact HTTP request, so existing Lance calls can be
imported without changing model inputs or triggering another provider call.

Two storage modes share this one contract. ``throughput=None`` keeps the
original single file, per-request file lease and per-request FULL commit.
``throughput={}`` routes the same keys over one or more native SQLite shards,
replaces file leases with rows in ``writer_leases`` and groups reserve/response
operations into one bounded, fully synchronous transaction per shard.
"""
from __future__ import annotations

import hashlib
import json
import os
import queue
import sqlite3
import threading
import fcntl
import time
import uuid
from concurrent.futures import Future
from contextlib import ExitStack
from pathlib import Path

from .errors import PromptBudgetExceededError, PromptError
from .journal import UncertainPromptCall, canonical, request_key


_LEASE_INSERT = ('INSERT INTO writer_leases(request_key,owner_token,owner_pid,owner_start) '
                 'VALUES (?,?,?,?)')
_MAX_PAGE_CANDIDATES = 1024
_THROUGHPUT_FIELDS = {'batch_size', 'flush_interval_s', 'queue_size', 'wal',
                      'max_record_bytes', 'max_queue_bytes'}


def _payload_bytes(text):
    """Encoded size of one serialized record; exact for ASCII with no copy."""
    return len(text) if text.isascii() else len(text.encode('utf-8'))


def _metadata(value):
    """Keep text/options; replace inline binary data with a content digest."""
    if isinstance(value, dict):
        return {key: _metadata(item) for key, item in value.items()}
    if isinstance(value, list):
        return [_metadata(item) for item in value]
    if isinstance(value, str) and value.startswith('data:') and ';base64,' in value:
        return {'data_uri_sha256': hashlib.sha256(value.encode()).hexdigest(),
                'media_type': value[5:value.index(';')], 'data_uri_chars': len(value)}
    return value


def _process_start(pid):
    """Linux start-time field of pid: None when gone, '' when unreadable."""
    try:
        return Path(f'/proc/{pid}/stat').read_text().rsplit(') ', 1)[1].split()[19]
    except FileNotFoundError:
        return None
    except (OSError, IndexError, ValueError):
        return ''


def _process_alive(pid, start):
    """Conservative owner liveness for a persisted database lease.

    Only an unreadable ``/proc`` entry (hidepid, another host namespace) is
    treated as alive without evidence. A legacy row without owner identity has
    no verifiable owner: no writer of this format predates those columns, so it
    is treated as stale and left to the explicit recovery path.
    """
    if type(pid) is not int or not isinstance(start, str) or not start:
        return False
    current = _process_start(pid)
    return True if current == '' else current == start


class _DatabaseLease:
    """Owner identity for one reservation; the row is written by the commit."""
    def __init__(self, token):
        self.token = token
        self.pid = os.getpid()
        self.start = _process_start(self.pid)

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.close()

    def close(self):
        pass


class _BatchItem:
    """One queued journal operation and its bounded serialized payload."""
    __slots__ = ('operation', 'payload', 'future', 'enqueued_at', 'size', 'released')

    def __init__(self, operation, payload, size):
        self.operation = operation
        self.payload = payload
        self.future = Future()
        self.enqueued_at = time.monotonic()
        self.size = size
        self.released = False


class _PayloadBudget:
    """Bounded accounting for serialized payloads queued or awaiting commit.

    Covers the queue, the batch being assembled and the in-flight transaction.
    A caller blocked here is backpressure, not a silent allocation; one record
    larger than the whole budget is refused before it is queued.
    """
    def __init__(self, limit):
        self.limit = limit
        self.used = 0
        self.peak = 0
        self._condition = threading.Condition()

    def reserve(self, size, stopped):
        if size > self.limit:
            raise PromptBudgetExceededError(
                f'Journal payload of {size} bytes exceeds the {self.limit}-byte record bound')
        with self._condition:
            while self.used + size > self.limit:
                if stopped():
                    raise PromptError('Journal writer stopped while waiting for payload capacity')
                self._condition.wait(.05)
            self.used += size
            self.peak = max(self.peak, self.used)

    def release(self, size):
        with self._condition:
            self.used -= size
            self._condition.notify_all()

    def snapshot(self):
        with self._condition:
            return {'queued_bytes': self.used, 'queued_bytes_max': self.peak,
                    'queue_bytes_limit': self.limit}


class _Metrics:
    """Bounded group-commit counters; no per-request history is retained."""
    def __init__(self):
        self._lock = threading.Lock()
        self.values = {
            'reserve_queued': 0, 'reserve_committed': 0, 'reserve_reused': 0,
            'save_queued': 0, 'save_committed': 0, 'failed_operations': 0,
            'batches': 0, 'batch_rows': 0, 'batch_rows_max': 0,
            'batch_seconds_total': 0.0, 'batch_seconds_max': 0.0,
            'queue_wait_seconds_total': 0.0, 'queue_wait_seconds_max': 0.0,
            'batch_bytes_max': 0,
        }

    def add(self, name, delta=1):
        with self._lock:
            self.values[name] += delta

    def batch(self, rows, seconds, size, waits):
        with self._lock:
            values = self.values
            values['batches'] += 1
            values['batch_rows'] += rows
            values['batch_rows_max'] = max(values['batch_rows_max'], rows)
            values['batch_bytes_max'] = max(values['batch_bytes_max'], size)
            values['batch_seconds_total'] += seconds
            values['batch_seconds_max'] = max(values['batch_seconds_max'], seconds)
            if waits:
                values['queue_wait_seconds_total'] += sum(waits)
                values['queue_wait_seconds_max'] = max(values['queue_wait_seconds_max'], max(waits))

    def snapshot(self):
        with self._lock:
            return dict(self.values)


class _SQLiteJournalShard:
    """One explicit calls table, atomic reservation, no per-row full-table scan.

    timeout_s controls lock waits. DELETE journaling + FULL synchronous commits
    avoid WAL's shared-memory requirement on shared project filesystems. Each
    connection is serialized locally; SQLite coordinates other processes.
    Opening is lazy: async clients do all journal I/O in their I/O executor.
    """
    def __init__(self, path, max_requests=None, *, timeout_s=30, legacy_source=None,
                 read_only=False, throughput=None):
        if type(read_only) is not bool:
            raise ValueError('read_only must be a boolean')
        if read_only and legacy_source:
            raise ValueError('A read-only journal cannot import legacy records')
        self.read_only = read_only
        from ..execution.artifacts import resolve_local_artifact
        self.path = resolve_local_artifact(path)
        self.limit = max_requests
        self.timeout_s = timeout_s
        self._lock = threading.RLock()
        self._db = None
        self._leases = {}
        self.legacy_source = legacy_source
        self.throughput = None if throughput is None else dict(throughput)
        config = self.throughput or {}
        if set(config) - _THROUGHPUT_FIELDS:
            raise ValueError('Unsupported journal throughput fields')
        self.batch_size = config.get('batch_size', 64)
        self.flush_interval_s = config.get('flush_interval_s', .01)
        self.queue_size = config.get('queue_size', 256)
        self.wal = config.get('wal', False)
        self.max_record_bytes = config.get('max_record_bytes', 8 * 1024 * 1024)
        self.max_queue_bytes = config.get('max_queue_bytes', 64 * 1024 * 1024)
        for name, value in (('batch_size', self.batch_size), ('queue_size', self.queue_size)):
            if type(value) is not int or not 1 <= value <= 4096:
                raise ValueError('Invalid journal throughput ' + name)
        if (not isinstance(self.flush_interval_s, (int, float))
                or not 0 < self.flush_interval_s <= 60 or type(self.wal) is not bool):
            raise ValueError('Invalid journal throughput flush interval or WAL flag')
        if (type(self.max_record_bytes) is not int
                or not 1024 <= self.max_record_bytes <= 64 * 1024 * 1024):
            raise ValueError('Invalid journal throughput max_record_bytes')
        if (type(self.max_queue_bytes) is not int
                or not self.max_record_bytes <= self.max_queue_bytes <= 512 * 1024 * 1024):
            raise ValueError('Invalid journal throughput max_queue_bytes')
        self._write_lock = threading.RLock()
        self._submit_lock = threading.Lock()
        self._closed = False
        self._batch_error = None
        self._batch_queue = None
        self._batch_thread = None
        self._bytes = _PayloadBudget(self.max_queue_bytes)
        self._metrics = _Metrics()
        self._ordinal_column = None

    def _require_writable(self):
        if self.read_only:
            raise PermissionError('The prompt journal is read-only: ' + str(self.path))

    def _connect(self):
        if self._db is None:
            if self.read_only:
                db = sqlite3.connect(self.path.as_uri() + '?mode=ro', uri=True,
                                     timeout=self.timeout_s, check_same_thread=False)
                try:
                    db.execute('PRAGMA query_only=ON')
                    db.execute('SELECT request_key, response_json FROM calls LIMIT 0')
                except BaseException:
                    db.close()
                    raise
                self._db = db
                return db
            self.path.parent.mkdir(parents=True, exist_ok=True)
            db = sqlite3.connect(self.path, timeout=self.timeout_s, check_same_thread=False)
            db.execute('PRAGMA journal_mode=' + ('WAL' if self.wal else 'DELETE'))
            db.execute('PRAGMA synchronous=FULL')
            db.executescript('''
                CREATE TABLE IF NOT EXISTS calls (
                    request_key TEXT PRIMARY KEY,
                    request_json TEXT,
                    response_json TEXT,
                    error_json TEXT,
                    legacy_refs_json TEXT
                );
                CREATE TABLE IF NOT EXISTS journal_state (
                    name TEXT PRIMARY KEY, value TEXT NOT NULL
                );
                INSERT OR IGNORE INTO journal_state VALUES ('request_count', '0');
                CREATE TABLE IF NOT EXISTS recovery_events (
                    operation_id TEXT NOT NULL, request_key TEXT NOT NULL,
                    actor TEXT NOT NULL, reason TEXT NOT NULL, recovered_at REAL NOT NULL,
                    request_json TEXT, error_json TEXT, attempt INTEGER NOT NULL,
                    PRIMARY KEY(operation_id, request_key)
                );
                CREATE INDEX IF NOT EXISTS recovery_request_attempt ON recovery_events(request_key,attempt);
            ''')
            if self.throughput is not None:
                db.execute('''CREATE TABLE IF NOT EXISTS writer_leases (
                    request_key TEXT PRIMARY KEY, owner_token TEXT NOT NULL,
                    owner_pid INTEGER, owner_start TEXT)''')
            # Serialize the additive migration across processes opening an old
            # journal at once. Existing archived attempts and refs stay valid.
            with db:
                db.execute('BEGIN IMMEDIATE')
                if 'response_json' not in {r[1] for r in db.execute('PRAGMA table_info(recovery_events)')}:
                    db.execute('ALTER TABLE recovery_events ADD COLUMN response_json TEXT')
                if self.throughput is not None:
                    lease_columns = {row[1] for row in db.execute('PRAGMA table_info(writer_leases)')}
                    for name, definition in (('owner_pid', 'INTEGER'), ('owner_start', 'TEXT')):
                        if name not in lease_columns:
                            db.execute(f'ALTER TABLE writer_leases ADD COLUMN {name} {definition}')
                    if 'ordinal' not in {row[1] for row in db.execute('PRAGMA table_info(calls)')}:
                        db.execute('ALTER TABLE calls ADD COLUMN ordinal INTEGER')
            self._db = db
            if self.legacy_source:
                try:
                    self.import_lance(**self.legacy_source)
                except BaseException:
                    db.close()
                    self._db = None
                    raise
        return self._db

    def _references_at(self, key, attempt):
        from .call_ref import PromptRecordRef
        return {'request_id': key, 'journal_path': str(self.path),
                **{kind + '_ref': PromptRecordRef(str(self.path), key, kind, attempt).to_dict()
                   for kind in ('request', 'response')}}

    def references(self, request):
        # The ID and location are known before writing; never re-read payloads
        # merely to attach call metadata. Imported fixed refs travel with lookup.
        key = request_key(request)
        with self._lock:
            attempt = self._connect().execute('SELECT COALESCE(MAX(attempt),0)+1 FROM recovery_events '
                'WHERE request_key=?',(key,)).fetchone()[0]
        return self._references_at(key, attempt)

    def _lease(self, key):
        if len(key) != 64 or any(c not in '0123456789abcdef' for c in key):
            raise ValueError('Expected a SHA256 request key')
        if self.throughput is not None:
            if key in self._leases:
                raise UncertainPromptCall('Request still has an active writer: ' + key)
            return _DatabaseLease(uuid.uuid4().hex)
        directory = self.path.with_name(self.path.name + '.leases')
        directory.mkdir(parents=True, exist_ok=True)
        stream = (directory / key).open('a')
        try:
            fcntl.flock(stream, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            stream.close()
            raise UncertainPromptCall('Request still has an active writer: ' + key) from None
        return stream

    def _insert_lease(self, db, statement, parameters):
        """Insert one lease row, replacing only a verifiably dead owner."""
        try:
            db.execute(statement, parameters)
        except sqlite3.IntegrityError:
            key = parameters[0]
            row = db.execute('SELECT owner_pid, owner_start FROM writer_leases WHERE request_key=?',
                             (key,)).fetchone()
            if row is None or _process_alive(row[0], row[1]):
                raise UncertainPromptCall('Request still has an active writer: ' + key) from None
            # Only an explicit recovery/reservation re-establishes ownership;
            # an unanswered row is never retried by a plain lookup.
            db.execute('DELETE FROM writer_leases WHERE request_key=?', (key,))
            db.execute(statement, parameters)

    def _stopped(self):
        return self._closed or self._batch_error is not None

    def _enqueue(self, operation, payload, size):
        budget = self._bytes
        budget.reserve(size, self._stopped)
        try:
            with self._submit_lock:
                if self._batch_error is not None:
                    raise PromptError('Journal writer failed: ' + repr(self._batch_error)) from self._batch_error
                if self._closed:
                    if self._batch_thread is not None:
                        raise PromptError('The prompt journal is being closed')
                    # A cleanly closed shard reopens lazily, like the historic
                    # single-file journal; only a failed writer stays terminal.
                    self._closed = False
                if self._batch_thread is None:
                    self._batch_queue = queue.Queue(self.queue_size)
                    self._batch_thread = threading.Thread(
                        target=self._batch_run, name='demiflow-journal-writer', daemon=True)
                    self._batch_thread.start()
                item = _BatchItem(operation, payload, size)
                self._metrics.add(operation + '_queued')
                while True:
                    try:
                        self._batch_queue.put(item, timeout=.05)
                        break
                    except queue.Full:
                        if self._stopped():
                            raise PromptError('Journal writer stopped before accepting the operation')
        except BaseException:
            budget.release(size)
            raise
        return item.future.result()

    def _finish_item(self, item, *, result=None, error=None):
        if not item.released:
            item.released = True
            self._bytes.release(item.size)
        if item.future.done():
            return
        try:
            if error is not None:
                item.future.set_exception(error)
            else:
                item.future.set_result(result)
        except Exception:  # a concurrent cancellation already finalized it
            pass

    def _batch_run(self):
        pending = []
        try:
            while True:
                try:
                    item = self._batch_queue.get(timeout=self.flush_interval_s)
                except queue.Empty:
                    if pending:
                        self._commit_batch(pending)
                        pending = []
                    continue
                if item is None:
                    if pending:
                        self._commit_batch(pending)
                    return
                pending.append(item)
                if len(pending) >= self.batch_size:
                    self._commit_batch(pending)
                    pending = []
        except BaseException as exc:
            # A dead writer must fail every accepted operation, never leave a
            # caller waiting on a future that no thread can complete.
            self._batch_error = exc
            for item in pending:
                self._finish_item(item, error=exc)
            while True:
                try:
                    item = self._batch_queue.get_nowait()
                except queue.Empty:
                    break
                if item is not None:
                    self._finish_item(item, error=exc)

    def _apply_operation(self, db, item, effects):
        """Apply one operation inside its own savepoint; return its result."""
        if item.operation == 'reserve':
            key, metadata, lease, ordinal = item.payload
            row = db.execute('SELECT response_json IS NOT NULL FROM calls WHERE request_key=?',
                             (key,)).fetchone()
            if row is not None and row[0]:
                return False
            if row is not None:
                raise UncertainPromptCall('Request reserved without complete response: ' + key)
            count = int(db.execute(
                "SELECT value FROM journal_state WHERE name='request_count'").fetchone()[0])
            if self.limit is not None and count >= self.limit:
                raise PromptBudgetExceededError('Persistent prompt request budget exhausted')
            if ordinal is None:
                db.execute('INSERT INTO calls(request_key,request_json) VALUES (?,?)',
                           (key, metadata))
            else:
                db.execute('INSERT INTO calls(request_key,request_json,ordinal) VALUES (?,?,?)',
                           (key, metadata, ordinal))
            self._insert_lease(db, _LEASE_INSERT, (key, lease.token, lease.pid, lease.start))
            db.execute("UPDATE journal_state SET value=? WHERE name='request_count'",
                       (str(count + 1),))
            effects[key] = lease
            return True
        key, column, payload_text = item.payload
        attempt = db.execute('SELECT COALESCE(MAX(attempt),0)+1 FROM recovery_events '
                             'WHERE request_key=?', (key,)).fetchone()[0]
        row = db.execute(f'SELECT {column} FROM calls WHERE request_key=?', (key,)).fetchone()
        if row is None:
            raise ValueError('Response/error has no reserved request: ' + key)
        if row[0] is not None and row[0] != payload_text:
            raise ValueError('Immutable prompt record differs: ' + key)
        lease = effects[key] if key in effects else self._leases.get(key)
        if row[0] is None:
            if lease is None:
                raise UncertainPromptCall('Writer no longer owns this reservation: ' + key)
            db.execute(f'UPDATE calls SET {column}=? WHERE request_key=?', (payload_text, key))
        if lease is not None:
            # An idempotent duplicate save releases the lease row it still owns;
            # otherwise close() would delete only an already-forgotten token.
            db.execute('DELETE FROM writer_leases WHERE request_key=? AND owner_token=?',
                       (key, lease.token))
        # Only drop the in-memory owner after every statement of this savepoint
        # succeeded: a failure below must leave the caller able to retry.
        result = self._references_at(key, attempt)
        effects[key] = None
        return result

    def _commit_batch(self, pending):
        pending = [item for item in pending if not item.future.done()]
        if not pending:
            return
        started = time.monotonic()
        total_bytes = sum(item.size for item in pending)
        completed, effects = [], {}
        try:
            with self._write_lock, self._lock:
                db = self._connect()
                with db:
                    db.execute('BEGIN IMMEDIATE')
                    for item in pending:
                        db.execute('SAVEPOINT batch_operation')
                        try:
                            result = self._apply_operation(db, item, effects)
                        except BaseException as exc:
                            try:
                                db.execute('ROLLBACK TO batch_operation')
                                db.execute('RELEASE batch_operation')
                            except sqlite3.Error:
                                pass
                            self._metrics.add('failed_operations')
                            self._finish_item(item, error=exc)
                        else:
                            db.execute('RELEASE batch_operation')
                            completed.append((item, result))
        except BaseException as exc:
            # Nothing was committed: in-memory leases must stay untouched and
            # every accepted future must reach one terminal state.
            for item in pending:
                self._finish_item(item, error=exc)
            return
        for key, lease in effects.items():
            if lease is None:
                self._leases.pop(key, None)
            else:
                self._leases[key] = lease
        self._metrics.batch(len(pending), time.monotonic() - started, total_bytes,
                            [started - item.enqueued_at for item in pending])
        for item, result in completed:
            if item.operation == 'reserve':
                self._metrics.add('reserve_committed' if result else 'reserve_reused')
            else:
                self._metrics.add('save_committed')
            self._finish_item(item, result=result)

    def lookup(self, request):
        with self._lock:
            db = self._connect()
            row = db.execute(
                'SELECT response_json, legacy_refs_json, request_json IS NULL, error_json FROM calls WHERE request_key=?',
                (request_key(request),)).fetchone()
            if row is not None and row[2] and not self.read_only:
                # A legacy import deliberately skipped full request payloads.
                # The caller now supplies this exact identity; store its compact
                # metadata so a newly returned native request_ref is readable.
                with db:
                    db.execute('UPDATE calls SET request_json=? WHERE request_key=? AND request_json IS NULL',
                               (canonical(_metadata(request)), request_key(request)))
        if row is None:
            return None
        if row[0] is None:
            from .call_ref import PromptRecordRef
            error = UncertainPromptCall('Request reserved without complete response: ' + request_key(request))
            saved = json.loads(row[3]) if row[3] else {}
            error.call = {key: value for key, value in (saved.get('call') or {}).items()
                          if key not in {'partial_response', 'partial_body', 'pending_event', 'provider_error', 'last_event'}}
            refs = self.references(request)
            error.call.update(refs, reused=True,
                error_ref={**refs['request_ref'],'kind':'error'})
            raise error
        result = json.loads(row[0])
        if row[1]:
            result = {**result, '_legacy_refs': json.loads(row[1])}
        return result

    def registered_keys(self, *, max_keys=100000):
        """Bounded snapshot of all reservations, including errors/unknowns.

        Read only identity keys, never payloads or response bodies. Consumers
        needing a stable pre-run history must call before admitting new work
        and rebuild after restart. This is not a live absence proof.
        The hard ceiling bounds Python key/set storage to tens of MiB.
        """
        if type(max_keys) is not int or not 0<=max_keys<=100000:
            raise ValueError('Registered request snapshot permits at most 100000 keys')
        with self._lock:
            if self.read_only and not self.path.exists():
                return frozenset()
            cursor=self._connect().execute('SELECT request_key FROM calls')
            def keys():
                for index,(key,) in enumerate(cursor):
                    if index>=max_keys:
                        raise ValueError('Registered request snapshot exceeds key budget')
                    if (not isinstance(key,str) or len(key)!=64
                            or any(c not in '0123456789abcdef' for c in key)):
                        raise ValueError('Expected a SHA256 request key')
                    yield key
            try:
                return frozenset(keys())
            finally:
                cursor.close()

    def reserve(self, request, *, ordinal=None):
        self._require_writable()
        key = request_key(request)
        # Encoding is outside the transaction; the lock covers only small rows.
        metadata = canonical(_metadata(request))
        if self.throughput is not None:
            lease = self._lease(key)
            return self._enqueue('reserve', (key, metadata, lease, ordinal),
                                 _payload_bytes(metadata) + len(key))
        with self._lock, ExitStack() as stack:
            lease = stack.enter_context(self._lease(key))
            db = self._connect()
            with db:
                db.execute('BEGIN IMMEDIATE')
                row = db.execute('SELECT response_json IS NOT NULL FROM calls WHERE request_key=?', (key,)).fetchone()
                if row is not None:
                    if row[0]:
                        return False
                    raise UncertainPromptCall('Request reserved without complete response: ' + key)
                count = int(db.execute("SELECT value FROM journal_state WHERE name='request_count'").fetchone()[0])
                if self.limit is not None and count >= self.limit:
                    raise PromptBudgetExceededError('Persistent prompt request budget exhausted')
                db.execute('INSERT INTO calls(request_key, request_json) VALUES (?, ?)', (key, metadata))
                db.execute("UPDATE journal_state SET value=? WHERE name='request_count'", (str(count + 1),))
            self._leases[key] = lease
            stack.pop_all()
        return True

    def reserve_http_retry(self, request, *, expected_attempt, expected_status, max_attempts):
        """Archive a complete error and reserve its next attempt in one transaction.

        A stale observer, exhausted budget, active writer, success, or uncertain
        response leaves the prior record untouched. All earlier attempt refs
        remain readable. The per-key attempt ceiling survives process restarts.
        """
        self._require_writable()
        if (type(expected_attempt) is not int or expected_attempt < 1
                or type(max_attempts) is not int or not 2 <= max_attempts <= 6
                or type(expected_status) is not int or not 400 <= expected_status <= 599):
            raise ValueError('Invalid bounded HTTP retry reservation')
        if type(self.limit) is not int or self.limit < 0:
            raise ValueError('HTTP retry requires a finite persistent request budget')
        key = request_key(request)
        metadata = canonical(_metadata(request))
        with self._write_lock, self._lock, ExitStack() as stack:
            lease = stack.enter_context(self._lease(key))
            db = self._connect()
            with db:
                db.execute('BEGIN IMMEDIATE')
                row = db.execute('SELECT request_json,error_json,response_json FROM calls WHERE request_key=?', (key,)).fetchone()
                attempt = db.execute('SELECT COALESCE(MAX(attempt),0)+1 FROM recovery_events WHERE request_key=?', (key,)).fetchone()[0]
                if (attempt != expected_attempt or row is None or row[2] is None
                        or json.loads(row[2]).get('status_code') != expected_status):
                    raise ValueError('HTTP retry no longer matches the saved complete error')
                if attempt >= max_attempts:
                    raise PromptBudgetExceededError('Persistent per-request HTTP attempt limit exhausted')
                count = int(db.execute("SELECT value FROM journal_state WHERE name='request_count'").fetchone()[0])
                if count >= self.limit:
                    raise PromptBudgetExceededError('Persistent prompt request budget exhausted')
                db.execute('INSERT INTO recovery_events (operation_id,request_key,actor,reason,recovered_at,request_json,error_json,attempt,response_json) VALUES (?,?,?,?,?,?,?,?,?)',
                    (f'http-retry:{key}:{attempt}', key, 'native-http-retry',
                     f'Explicit HTTP {expected_status} retry policy; max_attempts={max_attempts}',
                     time.time(), row[0], row[1], attempt, row[2]))
                if self.throughput is not None:
                    self._insert_lease(db, _LEASE_INSERT,
                                       (key, lease.token, lease.pid, lease.start))
                db.execute('UPDATE calls SET request_json=?,response_json=NULL,error_json=NULL,legacy_refs_json=NULL WHERE request_key=?', (metadata, key))
                db.execute("UPDATE journal_state SET value=? WHERE name='request_count'", (str(count+1),))
            self._leases[key] = lease
            stack.pop_all()
        return True

    def _save(self, request, column, value):
        self._require_writable()
        key, payload = request_key(request), canonical(value)
        if self.throughput is not None:
            # Queue only the serialized record: retaining the caller's request
            # (which may hold a large data URI) would escape max_queue_bytes.
            return self._enqueue('save', (key, column, payload),
                                 _payload_bytes(payload) + len(key))
        with self._lock:
            db = self._connect()
            with db:
                db.execute('BEGIN IMMEDIATE')
                row = db.execute(f'SELECT {column} FROM calls WHERE request_key=?', (key,)).fetchone()
                if row is None:
                    raise ValueError('Response/error has no reserved request: ' + key)
                if row[0] is not None and row[0] != payload:
                    raise ValueError('Immutable prompt record differs: ' + key)
                if row[0] is None:
                    if key not in self._leases:
                        raise UncertainPromptCall('Writer no longer owns this reservation: ' + key)
                    db.execute(f'UPDATE calls SET {column}=? WHERE request_key=?', (payload, key))
            lease = self._leases.pop(key, None)
            if lease is not None:
                lease.close()
        return self.references(request)

    def response(self, request, record):
        return self._save(request, 'response_json', record)

    def failed(self, request, error, elapsed):
        return self._save(request, 'error_json',
                          {'type': type(error).__name__, 'detail': str(error), 'elapsed_s': elapsed,
                           **({'call': error.call} if getattr(error, 'call', None) else {})})

    def import_lance(self, *, root, relative_uri, version, batch_size=256, log=None):
        """Read one fixed legacy snapshot → copy keys/responses → commit once.

        Never reads request payloads or mutates/cleans Lance. Old RecordRefs keep
        their written_version. Unanswered reservations remain uncertain. The
        import is atomic and idempotent; an interrupted import rolls back.
        """
        self._require_writable()
        import lance
        from ..lance.storage import resolve_local_uri
        source = {'root': str(Path(root).absolute()), 'relative_uri': str(relative_uri), 'version': version}
        marker = canonical(source)
        with self._write_lock, self._lock:
            db = self._connect()
            with db:
                db.execute('BEGIN IMMEDIATE')
                prior = db.execute("SELECT value FROM journal_state WHERE name='legacy_source'").fetchone()
                if prior:
                    if prior[0] != marker:
                        raise ValueError('Journal already imported a different legacy snapshot')
                    return self.stats()
                if db.execute('SELECT 1 FROM calls LIMIT 1').fetchone():
                    raise ValueError('Import legacy calls before submitting new requests')
                ds = lance.dataset(str(resolve_local_uri(Path(root) / relative_uri)), version=version)
                # Only key/version columns for requests: no base64 is scanned.
                count = 0
                for batch in ds.to_batches(columns=['key', 'written_version'],
                                           filter="starts_with(key, 'request/')", batch_size=batch_size):
                    for row in batch.to_pylist():
                        ref = {'relative_uri': str(relative_uri), 'version': row['written_version'], 'key': row['key']}
                        db.execute('INSERT INTO calls(request_key, legacy_refs_json) VALUES (?, ?)',
                                   (row['key'].split('/', 1)[1], canonical({'request_ref': ref})))
                        count += 1
                if log:
                    log(f'旧调用导入：{count:,} 个请求键；正在读取响应（不复制图片字节）')
                completed = 0
                for batch in ds.to_batches(columns=['key', 'payload', 'written_version'],
                                           filter="starts_with(key, 'response/')", batch_size=batch_size):
                    for row in batch.to_pylist():
                        key = row['key'].split('/', 1)[1]
                        found = db.execute('SELECT legacy_refs_json FROM calls WHERE request_key=?', (key,)).fetchone()
                        if found is None:
                            raise ValueError('Legacy response has no request: ' + key)
                        refs = json.loads(found[0])
                        refs['response_ref'] = {'relative_uri': str(relative_uri), 'version': row['written_version'], 'key': row['key']}
                        db.execute('UPDATE calls SET response_json=?, legacy_refs_json=? WHERE request_key=?',
                                   (row['payload'], canonical(refs), key))
                        completed += 1
                    if log:
                        log(f'旧调用导入：已复制 {completed:,} 条完整响应')
                db.execute("UPDATE journal_state SET value=? WHERE name='request_count'", (str(count),))
                db.execute("INSERT INTO journal_state VALUES ('legacy_source', ?)", (marker,))
            return self.stats()

    def stats(self):
        """Explicit monitoring read; never called in the per-request hot path."""
        with self._lock:
            count, completed = self._connect().execute(
                'SELECT count(*), count(response_json) FROM calls').fetchone()
        return {'requests': count, 'responses': completed, 'uncertain': count - completed}

    def read(self, key, kind='response'):
        """Read a native call component, not a generic key/value record."""
        column = {'request': 'request_json', 'response': 'response_json', 'error': 'error_json'}[kind]
        with self._lock:
            row = self._connect().execute(f'SELECT {column} FROM calls WHERE request_key=?', (key,)).fetchone()
        from .sqlite_offline import restore
        return restore(json.loads(row[0])) if row and row[0] is not None else None

    def request_ids(self):
        with self._lock:
            return {row[0] for row in self._connect().execute('SELECT request_key FROM calls')}

    def records(self, kind='response'):
        """Explicit component inspection; request metadata omits HTTP image bytes."""
        column = {'request': 'request_json', 'response': 'response_json', 'error': 'error_json'}[kind]
        with self._lock:
            rows = self._connect().execute(f'SELECT request_key, {column} FROM calls WHERE {column} IS NOT NULL').fetchall()
        from .sqlite_offline import restore
        return {key: restore(json.loads(value)) for key, value in rows}

    def calls(self, *, pending_only=False):
        """Explicit operator inspection, outside the per-request hot path."""
        with self._lock:
            rows = self._connect().execute('SELECT request_key, request_json, response_json, error_json FROM calls'
                + (' WHERE response_json IS NULL' if pending_only else '') + ' ORDER BY request_key').fetchall()
        return [{'request_id': key, 'request': json.loads(req) if req else None,
                 'response': json.loads(resp) if resp else None, 'error': json.loads(err) if err else None}
                for key, req, resp, err in rows]

    def _has_ordinal(self, db):
        if self._ordinal_column is None:
            self._ordinal_column = 'ordinal' in {row[1] for row in db.execute('PRAGMA table_info(calls)')}
        return self._ordinal_column

    def _page_rows(self, *, offset=0, limit=3, responses_only=True,
                   max_record_bytes=2 * 1024 * 1024, max_page_bytes=8 * 1024 * 1024):
        """Bounded inspection page with one recency sort key per row.

        Check stored UTF-8 byte lengths before transferring JSON to Python.
        Oversized records remain visible as omitted entries, never silently
        counted as decoded successes. Bounds cover serialized payloads, not RSS
        or SQLite's internal page cache. This does not reserve/recover calls.
        """
        if type(offset) is not int or offset < 0 or type(limit) is not int or not 1 <= limit <= 64:
            raise ValueError('offset >= 0 and limit in 1..64 required')
        if (type(max_record_bytes) is not int or type(max_page_bytes) is not int
                or not 1 <= max_record_bytes <= max_page_bytes <= 32 * 1024 * 1024):
            raise ValueError('Require 0 < record bytes <= page bytes <= 32 MiB')
        where = ' WHERE response_json IS NOT NULL' if responses_only else ''
        with self._lock:
            db = self._connect()
            # A read-only throughput view of a classic file has no ordinal
            # column; fall back to the historic rowid order instead of failing.
            ordered = self.throughput is not None and self._has_ordinal(db)
            columns = 'request_key, ordinal, rowid' if ordered else 'request_key, rowid'
            order = (' ORDER BY ordinal IS NULL, ordinal DESC, rowid DESC'
                     if ordered else ' ORDER BY rowid DESC')
            headers = db.execute('SELECT ' + columns + ', '
                'coalesce(length(cast(request_json AS BLOB)),0) + '
                'coalesce(length(cast(response_json AS BLOB)),0) + '
                'coalesce(length(cast(error_json AS BLOB)),0) FROM calls'
                + where + order + ' LIMIT ? OFFSET ?', (limit, offset)).fetchall()
            result, used = [], 0
            for header in headers:
                if ordered:
                    key, ordinal, rowid, size = header
                    sort_key = (0 if ordinal is not None else 1, -(ordinal or 0), -rowid)
                else:
                    key, rowid, size = header
                    sort_key = (2, 0, -rowid)
                if size > max_record_bytes or used + size > max_page_bytes:
                    result.append((sort_key, {'request_id': key,
                                              'omitted': 'inspection_byte_limit',
                                              'stored_bytes': size}, size))
                    continue
                # Recheck in the SELECT: a writer may have filled the response
                # since the header query; it cannot bypass this byte admission.
                row = db.execute('SELECT request_json, response_json, error_json FROM calls WHERE request_key=? '
                    'AND coalesce(length(cast(request_json AS BLOB)),0) + '
                    'coalesce(length(cast(response_json AS BLOB)),0) + '
                    'coalesce(length(cast(error_json AS BLOB)),0) <= ?',
                    (key, min(max_record_bytes, max_page_bytes - used))).fetchone()
                if row is None:
                    result.append((sort_key, {'request_id': key,
                                              'omitted': 'record_changed_or_exceeds_limit'}, size))
                    continue
                used += sum(len(value.encode('utf-8')) for value in row if value)
                result.append((sort_key, {'request_id': key,
                    **{kind: json.loads(value) if value else None
                       for kind, value in zip(('request', 'response', 'error'), row)}}, size))
        return result

    def call_page(self, **kwargs):
        return [row for _, row, _ in self._page_rows(**kwargs)]

    def inspection_stats(self):
        """Scalar counts only; unanswered errors differ from active/unknown calls."""
        with self._lock:
            row = self._connect().execute('SELECT count(*), count(response_json), '
                'coalesce(sum(response_json IS NULL AND error_json IS NOT NULL),0) FROM calls').fetchone()
        return {'requests': row[0], 'responses': row[1], 'saved_errors': row[2],
                'without_response_or_error': row[0] - row[1] - row[2]}

    def requeue_uncertain(self, keys, *, actor, reason, operation_id=None):
        """Explicitly authorize another attempt after all old writers exit.

        Per-request leases exclude live writers (including other processes).
        The transaction archives the old attempt before removing its reservation;
        complete responses are never modified and the budget is never refunded.
        """
        self._require_writable()
        keys = sorted(set(keys))
        if not keys or not actor.strip() or not reason.strip():
            raise ValueError('Explicit request keys, actor and reason are required')
        operation_id = operation_id or uuid.uuid4().hex
        with self._write_lock, self._lock, ExitStack() as stack:
            leases = {key: stack.enter_context(self._lease(key)) for key in keys}
            db = self._connect()
            with db:
                db.execute('BEGIN IMMEDIATE')
                prior = db.execute('SELECT request_key, actor, reason FROM recovery_events WHERE operation_id=?',
                                   (operation_id,)).fetchall()
                if prior:
                    if sorted(prior) != [(key, actor, reason) for key in keys]:
                        raise ValueError('Recovery operation differs')
                    return {'operation_id': operation_id, 'keys': keys, 'reused': True}
                if self.throughput is not None:
                    for key, lease in leases.items():
                        self._insert_lease(db, _LEASE_INSERT,
                                           (key, lease.token, lease.pid, lease.start))
                for key in keys:
                    row = db.execute('SELECT request_json, error_json, response_json FROM calls WHERE request_key=?', (key,)).fetchone()
                    if row is None or row[2] is not None:
                        raise ValueError('Only existing unanswered requests can be requeued: ' + key)
                    attempt = db.execute('SELECT count(*)+1 FROM recovery_events WHERE request_key=?', (key,)).fetchone()[0]
                    db.execute('INSERT INTO recovery_events (operation_id,request_key,actor,reason,recovered_at,request_json,error_json,attempt) VALUES (?, ?, ?, ?, ?, ?, ?, ?)',
                               (operation_id, key, actor, reason, time.time(), row[0], row[1], attempt))
                    db.execute('DELETE FROM calls WHERE request_key=?', (key,))
                if self.throughput is not None:
                    for key, lease in leases.items():
                        db.execute('DELETE FROM writer_leases WHERE request_key=? AND owner_token=?',
                                   (key, lease.token))
        return {'operation_id': operation_id, 'keys': keys, 'reused': False}

    def requeue_http_errors(self, keys, *, expected_statuses, actor, reason, operation_id=None):
        """Authorize a bounded retry of exact, saved HTTP error responses.

        This is an explicit operator action after the external failure changes,
        never an automatic retry. Archive the complete response under its old
        attempt before releasing the key. Successful responses are ineligible;
        every future reservation still consumes the persistent request budget.
        """
        self._require_writable()
        keys, statuses = sorted(set(keys)), set(expected_statuses)
        if not keys or not actor.strip() or not reason.strip():
            raise ValueError('Explicit request keys, actor and reason are required')
        if not statuses or any(type(s) is not int or not 400 <= s <= 599 for s in statuses):
            raise ValueError('Explicit HTTP error statuses in 400..599 are required')
        operation_id = operation_id or uuid.uuid4().hex
        with self._write_lock, self._lock, ExitStack() as stack:
            leases = {key: stack.enter_context(self._lease(key)) for key in keys}
            db = self._connect()
            with db:
                db.execute('BEGIN IMMEDIATE')
                prior = db.execute('SELECT request_key,actor,reason,response_json FROM recovery_events WHERE operation_id=?',
                                   (operation_id,)).fetchall()
                if prior:
                    if (sorted((k,a,r) for k,a,r,_ in prior) != [(key,actor,reason) for key in keys]
                            or any(resp is None or json.loads(resp).get('status_code') not in statuses for _,_,_,resp in prior)):
                        raise ValueError('Recovery operation differs')
                    return {'operation_id': operation_id, 'keys': keys, 'reused': True}
                if self.throughput is not None:
                    for key, lease in leases.items():
                        self._insert_lease(db, _LEASE_INSERT,
                                           (key, lease.token, lease.pid, lease.start))
                for key in keys:
                    row = db.execute('SELECT request_json,error_json,response_json FROM calls WHERE request_key=?', (key,)).fetchone()
                    if row is None or row[2] is None or json.loads(row[2]).get('status_code') not in statuses:
                        raise ValueError('Only matching saved HTTP errors can be requeued: ' + key)
                    attempt = db.execute('SELECT COALESCE(MAX(attempt),0)+1 FROM recovery_events WHERE request_key=?', (key,)).fetchone()[0]
                    db.execute('INSERT INTO recovery_events (operation_id,request_key,actor,reason,recovered_at,request_json,error_json,attempt,response_json) VALUES (?,?,?,?,?,?,?,?,?)',
                               (operation_id,key,actor,reason,time.time(),row[0],row[1],attempt,row[2]))
                    db.execute('DELETE FROM calls WHERE request_key=?', (key,))
                if self.throughput is not None:
                    for key, lease in leases.items():
                        db.execute('DELETE FROM writer_leases WHERE request_key=? AND owner_token=?',
                                   (key, lease.token))
        return {'operation_id': operation_id, 'keys': keys, 'reused': False}

    def precheck_recovery(self, keys, *, operation_id, expected_statuses=None):
        """Read-only admission for an explicit recovery spanning other shards.

        A shard that already recorded the operation is skipped; otherwise the
        same conditions as the transaction are checked so a key that can never
        be recovered fails before any shard is mutated.
        """
        with self._lock:
            db = self._connect()
            done = {row[0] for row in db.execute(
                'SELECT request_key FROM recovery_events WHERE operation_id=?', (operation_id,))}
            for key in keys:
                if key in done:
                    continue
                row = db.execute('SELECT response_json FROM calls WHERE request_key=?', (key,)).fetchone()
                if expected_statuses is None:
                    if row is None or row[0] is not None:
                        raise ValueError('Only existing unanswered requests can be requeued: ' + key)
                elif (row is None or row[0] is None
                      or json.loads(row[0]).get('status_code') not in expected_statuses):
                    raise ValueError('Only matching saved HTTP errors can be requeued: ' + key)
                lease = db.execute('SELECT owner_pid, owner_start FROM writer_leases '
                                   'WHERE request_key=?', (key,)).fetchone()
                if lease is not None and _process_alive(lease[0], lease[1]):
                    raise UncertainPromptCall('Request still has an active writer: ' + key)

    def recovery_history(self):
        with self._lock:
            cursor = self._connect().execute('SELECT * FROM recovery_events ORDER BY recovered_at, request_key')
            return [dict(zip([c[0] for c in cursor.description], row)) for row in cursor]

    def throughput_metrics(self):
        values = self._metrics.snapshot()
        values.update(self._bytes.snapshot())
        return values

    def close(self):
        if self._batch_queue is not None:
            with self._submit_lock:
                self._closed = True
                while True:
                    try:
                        self._batch_queue.put(None, timeout=.05)
                        break
                    except queue.Full:
                        if self._batch_thread is None or not self._batch_thread.is_alive():
                            break
            if self._batch_thread is not None:
                self._batch_thread.join()
            self._batch_thread = self._batch_queue = None
        with self._write_lock, self._lock:
            if self.throughput is not None and self._db is not None:
                with self._db:
                    for lease in self._leases.values():
                        self._db.execute('DELETE FROM writer_leases WHERE owner_token=?', (lease.token,))
            for lease in self._leases.values():
                lease.close()
            self._leases.clear()
            if self._db is not None:
                self._db.close()
                self._db = None


class SQLitePromptJournal:
    """Route one journal contract over one or more native SQLite shards.

    ``throughput=None`` keeps the historic single file. A configured journal
    keeps one writer thread, queue and connection per shard; keys never migrate
    between shards, so a request can only be reserved, answered and replayed by
    the shard that owns its key. Cross-shard limits are process-local.
    """

    def __init__(self, path, max_requests=None, *, timeout_s=30, legacy_source=None,
                 read_only=False, throughput=None):
        if type(read_only) is not bool:
            raise ValueError('read_only must be a boolean')
        if read_only and legacy_source:
            raise ValueError('A read-only journal cannot import legacy records')
        from ..execution.artifacts import resolve_local_artifact
        self.path = resolve_local_artifact(path)
        self.limit = max_requests
        self.timeout_s = timeout_s
        self.read_only = read_only
        config = None if throughput is None else dict(throughput)
        self.shard_count = 1 if config is None else config.pop('shards', 1)
        if type(self.shard_count) is not int or not 1 <= self.shard_count <= 8:
            raise ValueError('Journal throughput shards must be in 1..8')
        if self.shard_count > 1 and legacy_source:
            raise ValueError('Legacy import requires a single journal shard')
        if self.shard_count == 1:
            paths = [self.path]
        else:
            paths = [self._shard_directory() / f'{index:03d}.sqlite'
                     for index in range(self.shard_count)]
        self._shards = [_SQLiteJournalShard(
            shard_path, max_requests if self.shard_count == 1 else None,
            timeout_s=timeout_s, legacy_source=legacy_source,
            read_only=read_only, throughput=config) for shard_path in paths]
        self._lock = self._shards[0]._lock if self.shard_count == 1 else None
        self._manifest_lock = threading.Lock()
        self._manifest_ready = self.shard_count == 1
        self._budget_lock = threading.Lock()
        self._budget_used = None
        self._ordinal = 0
        self._budget_active = set()

    def _shard_directory(self):
        return self.path.with_name(self.path.name + '.d')

    def _ensure_manifest(self):
        if self._manifest_ready:
            return
        with self._manifest_lock:
            if self._manifest_ready:
                return
            directory = self._shard_directory()
            manifest = directory / 'manifest.json'
            expected = {'format': 'demiflow-sqlite-journal-v1', 'shards': self.shard_count,
                        'paths': [f'{index:03d}.sqlite' for index in range(self.shard_count)]}
            if self.read_only:
                if not manifest.exists():
                    raise FileNotFoundError(manifest)
                if json.loads(manifest.read_text()) != expected:
                    raise ValueError('Journal shard manifest differs from the requested layout')
            else:
                directory.mkdir(parents=True, exist_ok=True)
                if not manifest.exists():
                    # First creation must be atomic: two processes may race
                    # with different shard counts. Write the complete file
                    # first and link it into place, so a visible manifest is
                    # never partial and exactly one layout can win.
                    temporary = directory / ('manifest.' + uuid.uuid4().hex + '.tmp')
                    try:
                        with temporary.open('x') as stream:
                            stream.write(json.dumps(expected, sort_keys=True))
                            stream.flush()
                            os.fsync(stream.fileno())
                        try:
                            os.link(temporary, manifest)
                        except FileExistsError:
                            pass
                    finally:
                        temporary.unlink(missing_ok=True)
                if json.loads(manifest.read_text()) != expected:
                    raise ValueError('Journal shard manifest differs from the requested layout')
            self._manifest_ready = True

    def _for_key(self, key):
        self._ensure_manifest()
        return self._shards[int(key[:16], 16) % self.shard_count]

    def _for_request(self, request):
        return self._for_key(request_key(request))

    def _all(self):
        self._ensure_manifest()
        return self._shards

    def _connect(self):
        if self.shard_count != 1:
            raise ValueError('A direct journal connection requires a single shard')
        return self._shards[0]._connect()

    def _initialize_budget(self):
        used = ordinal = 0
        for item in self._shards:
            with item._lock:
                db = item._connect()
                used += int(db.execute(
                    "SELECT value FROM journal_state WHERE name='request_count'").fetchone()[0])
                ordinal = max(ordinal, db.execute('SELECT MAX(ordinal) FROM calls').fetchone()[0] or 0)
        self._budget_used, self._ordinal = used, ordinal

    def _admit(self, key, shard, *, replay):
        """Return 'admitted', 'existing' or raise; never over-issues the budget."""
        with self._budget_lock:
            if self._budget_used is None:
                self._initialize_budget()
            if key in self._budget_active:
                return 'active'
            if self.limit is None or self._budget_used < self.limit:
                self._budget_used += 1
                return 'admitted'
        if replay:
            with shard._lock:
                row = shard._connect().execute(
                    'SELECT response_json FROM calls WHERE request_key=?', (key,)).fetchone()
            if row is not None:
                return 'existing'
        raise PromptBudgetExceededError('Persistent prompt request budget exhausted')

    def _next_ordinal(self):
        with self._budget_lock:
            if self._budget_used is None:
                self._initialize_budget()
            self._ordinal += 1
            return self._ordinal

    def _count_late(self, key):
        with self._budget_lock:
            self._budget_used += 1
            self._budget_active.add(key)

    def _reserved(self, key, admitted, result):
        with self._budget_lock:
            if result:
                self._budget_active.add(key)
            elif admitted:
                self._budget_used -= 1

    def _reserve_failed(self, key, admitted):
        with self._budget_lock:
            if admitted and key not in self._budget_active:
                self._budget_used -= 1

    def _saved(self, key):
        with self._budget_lock:
            self._budget_active.discard(key)

    def lookup(self, request):
        return self._for_request(request).lookup(request)

    def reserve(self, request):
        key = request_key(request)
        shard = self._for_key(key)
        if self.shard_count == 1:
            shard.limit = self.limit
            return shard.reserve(request)
        state = self._admit(key, shard, replay=True)
        if state == 'active':
            return shard.reserve(request)
        admitted = state == 'admitted'
        ordinal = self._next_ordinal() if admitted else None
        try:
            result = shard.reserve(request, ordinal=ordinal)
        except BaseException:
            self._reserve_failed(key, admitted)
            raise
        if admitted:
            self._reserved(key, admitted, result)
        elif result:
            self._count_late(key)
        return result

    def reserve_http_retry(self, request, **kwargs):
        key = request_key(request)
        shard = self._for_key(key)
        if self.shard_count == 1:
            shard.limit = self.limit
            return shard.reserve_http_retry(request, **kwargs)
        state = self._admit(key, shard, replay=False)
        if state == 'active':
            return shard.reserve_http_retry(request, **kwargs)
        try:
            result = shard.reserve_http_retry(request, **kwargs)
        except BaseException:
            self._reserve_failed(key, state == 'admitted')
            raise
        self._reserved(key, state == 'admitted', result)
        return result

    def response(self, request, record):
        key = request_key(request)
        try:
            return self._for_key(key).response(request, record)
        finally:
            self._saved(key)

    def failed(self, request, error, elapsed):
        key = request_key(request)
        try:
            return self._for_key(key).failed(request, error, elapsed)
        finally:
            self._saved(key)

    def references(self, request):
        return self._for_request(request).references(request)

    def registered_keys(self, *, max_keys=100000):
        if type(max_keys) is not int or not 0 <= max_keys <= 100000:
            raise ValueError('Registered request snapshot permits at most 100000 keys')
        result = set()
        for shard in self._all():
            remaining = max_keys - len(result)
            if remaining < 0:
                raise ValueError('Registered request snapshot exceeds key budget')
            result.update(shard.registered_keys(max_keys=min(100000, remaining + 1)))
            if len(result) > max_keys:
                raise ValueError('Registered request snapshot exceeds key budget')
        return frozenset(result)

    def read(self, key, kind='response'):
        return self._for_key(key).read(key, kind)

    def request_ids(self):
        return set().union(*(shard.request_ids() for shard in self._all()))

    def records(self, kind='response'):
        result = {}
        for shard in self._all():
            result.update(shard.records(kind))
        return result

    def calls(self, *, pending_only=False):
        return sorted((row for shard in self._all() for row in shard.calls(pending_only=pending_only)),
                      key=lambda row: row['request_id'])

    def call_page(self, **kwargs):
        """One bounded inspection page, newest reservation first.

        With several shards the merge uses the reservation ordinal stored by
        this mode, so the page keeps global recency order. ``offset+limit`` is
        capped; each shard contributes at most that many candidates.
        """
        if self.shard_count == 1:
            return self._shards[0].call_page(**kwargs)
        offset, limit = kwargs.get('offset', 0), kwargs.get('limit', 3)
        if type(offset) is not int or offset < 0 or type(limit) is not int or not 1 <= limit <= 64:
            raise ValueError('offset >= 0 and limit in 1..64 required')
        needed = offset + limit
        if needed > _MAX_PAGE_CANDIDATES:
            raise ValueError(f'A multi-shard inspection page supports offset+limit <= {_MAX_PAGE_CANDIDATES}')
        candidates = []
        for shard in self._all():
            for start in range(0, needed, 64):
                chunk = min(64, needed - start)
                page = shard._page_rows(**{**kwargs, 'offset': start, 'limit': chunk})
                candidates.extend(page)
                if len(page) < chunk:
                    break
            candidates.sort(key=lambda item: item[0])
            del candidates[needed:]
        max_record = kwargs.get('max_record_bytes', 2 * 1024 * 1024)
        max_page = kwargs.get('max_page_bytes', 8 * 1024 * 1024)
        result, used = [], 0
        for _, row, size in candidates[offset:offset + limit]:
            if size > max_record or used + size > max_page:
                result.append({'request_id': row['request_id'],
                               'omitted': 'inspection_byte_limit', 'stored_bytes': size})
                continue
            used += size
            result.append(row)
        return result

    def stats(self):
        values = [shard.stats() for shard in self._all()]
        return {key: sum(value[key] for value in values) for key in ('requests', 'responses', 'uncertain')}

    def inspection_stats(self):
        values = [shard.inspection_stats() for shard in self._all()]
        return {key: sum(value[key] for value in values)
                for key in ('requests', 'responses', 'saved_errors', 'without_response_or_error')}

    def throughput_metrics(self):
        """Bounded group-commit counters for this journal mode.

        Totals and maxima are O(1) per shard; no per-request history is kept.
        A cross-shard requeue that fails after another shard committed is not a
        global transaction: retry with the same ``operation_id`` to finish it.
        """
        values = [shard.throughput_metrics() for shard in self._all()]
        summed = ('reserve_queued', 'reserve_committed', 'reserve_reused', 'save_queued',
                  'save_committed', 'failed_operations', 'batches', 'batch_rows',
                  'batch_seconds_total', 'queue_wait_seconds_total')
        maxima = ('batch_rows_max', 'batch_seconds_max', 'queue_wait_seconds_max',
                  'batch_bytes_max', 'queued_bytes_max', 'queue_bytes_limit')
        result = {name: sum(value[name] for value in values) for name in summed}
        result.update({name: max(value[name] for value in values) for name in maxima})
        result['queued_bytes'] = sum(value['queued_bytes'] for value in values)
        result['shards'] = len(values)
        result['batch_seconds_mean'] = (result['batch_seconds_total'] / result['batches']
                                        if result['batches'] else None)
        result['queue_wait_seconds_mean'] = (result['queue_wait_seconds_total'] / result['batch_rows']
                                             if result['batch_rows'] else None)
        return result

    def _precheck_recovery(self, keys, operation_id, statuses):
        """Reject keys that no shard could recover before mutating any shard."""
        groups = {}
        for key in keys:
            shard = self._for_key(key)
            groups.setdefault(id(shard), (shard, []))[1].append(key)
        for shard, group in groups.values():
            shard.precheck_recovery(group, operation_id=operation_id, expected_statuses=statuses)

    def _requeue(self, method, keys, kwargs, statuses):
        keys = sorted(set(keys))
        if not keys:
            raise ValueError('Explicit request keys, actor and reason are required')
        operation_id = kwargs.get('operation_id') or uuid.uuid4().hex
        if self.shard_count > 1:
            self._precheck_recovery(keys, operation_id, statuses)
        groups = {}
        for key in keys:
            shard = self._for_key(key)
            groups.setdefault(id(shard), (shard, []))[1].append(key)
        results, done = [], []
        try:
            for shard, group in groups.values():
                result = getattr(shard, method)(group, **{**kwargs, 'operation_id': operation_id})
                results.append(result)
                done.extend(result['keys'])
        finally:
            with self._budget_lock:
                self._budget_active.difference_update(done)
        return {'operation_id': operation_id, 'keys': keys,
                'reused': bool(results) and all(result['reused'] for result in results)}

    def requeue_uncertain(self, keys, **kwargs):
        return self._requeue('requeue_uncertain', keys, kwargs, None)

    def requeue_http_errors(self, keys, **kwargs):
        return self._requeue('requeue_http_errors', keys, kwargs,
                             kwargs.get('expected_statuses'))

    def recovery_history(self):
        return sorted((row for shard in self._all() for row in shard.recovery_history()),
                      key=lambda row: (row['recovered_at'], row['request_key']))

    def import_lance(self, **kwargs):
        if self.shard_count != 1:
            raise ValueError('Legacy import requires a single journal shard')
        return self._shards[0].import_lance(**kwargs)

    def close(self):
        failure = None
        for shard in self._shards:
            try:
                shard.close()
            except BaseException as exc:
                failure = failure or exc
        if failure is not None:
            raise failure
