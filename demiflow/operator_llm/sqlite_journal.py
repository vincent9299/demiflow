"""Compact, durable HTTP call results for resume; business rows stay in Dataset.

Entry: SQLitePromptJournal(path). lookup/reserve read a request key; response
commits the original provider result. Images remain in the input object store:
request metadata records their digest, never a second copy of inline base64.
The key still hashes the exact HTTP request, so existing Lance calls can be
imported without changing model inputs or triggering another provider call.
"""
from __future__ import annotations

import hashlib
import json
import sqlite3
import threading
import fcntl
import time
import uuid
from contextlib import ExitStack
from pathlib import Path

from .errors import PromptBudgetExceededError
from .journal import UncertainPromptCall, canonical, request_key


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


class SQLitePromptJournal:
    """One explicit calls table, atomic reservation, no per-row full-table scan.

    timeout_s controls lock waits. DELETE journaling + FULL synchronous commits
    avoid WAL's shared-memory requirement on shared project filesystems. Each
    connection is serialized locally; SQLite coordinates other processes.
    Opening is lazy: async clients do all journal I/O in their I/O executor.
    """
    def __init__(self, path, max_requests=None, *, timeout_s=30, legacy_source=None,
                 read_only=False):
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
            db.execute('PRAGMA journal_mode=DELETE')
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
            # Serialize the additive migration across processes opening an old
            # journal at once. Existing archived attempts and refs stay valid.
            with db:
                db.execute('BEGIN IMMEDIATE')
                if 'response_json' not in {r[1] for r in db.execute('PRAGMA table_info(recovery_events)')}:
                    db.execute('ALTER TABLE recovery_events ADD COLUMN response_json TEXT')
            self._db = db
            if self.legacy_source:
                try:
                    self.import_lance(**self.legacy_source)
                except BaseException:
                    db.close()
                    self._db = None
                    raise
        return self._db

    def references(self, request):
        # The ID and location are known before writing; never re-read payloads
        # merely to attach call metadata. Imported fixed refs travel with lookup.
        key = request_key(request)
        from .call_ref import PromptRecordRef
        with self._lock:
            attempt = self._connect().execute('SELECT COALESCE(MAX(attempt),0)+1 FROM recovery_events '
                'WHERE request_key=?',(key,)).fetchone()[0]
        return {'request_id': key, 'journal_path': str(self.path),
                **{kind + '_ref': PromptRecordRef(str(self.path), key, kind, attempt).to_dict()
                   for kind in ('request', 'response')}}

    def _lease(self, key):
        if len(key) != 64 or any(c not in '0123456789abcdef' for c in key):
            raise ValueError('Expected a SHA256 request key')
        directory = self.path.with_name(self.path.name + '.leases')
        directory.mkdir(parents=True, exist_ok=True)
        stream = (directory / key).open('a')
        try:
            fcntl.flock(stream, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            stream.close()
            raise UncertainPromptCall('Request still has an active writer: ' + key) from None
        return stream

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

    def reserve(self, request):
        self._require_writable()
        key = request_key(request)
        # Encoding is outside the transaction; the lock covers only small rows.
        metadata = canonical(_metadata(request))
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
        with self._lock, ExitStack() as stack:
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
                db.execute('UPDATE calls SET request_json=?,response_json=NULL,error_json=NULL,legacy_refs_json=NULL WHERE request_key=?', (metadata, key))
                db.execute("UPDATE journal_state SET value=? WHERE name='request_count'", (str(count+1),))
            self._leases[key] = lease
            stack.pop_all()
        return True

    def _save(self, request, column, value):
        self._require_writable()
        key, payload = request_key(request), canonical(value)
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
        with self._lock:
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

    def call_page(self, *, offset=0, limit=3, responses_only=True,
                  max_record_bytes=2 * 1024 * 1024, max_page_bytes=8 * 1024 * 1024):
        """Read a bounded inspection page, newest reservations first.

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
            headers = db.execute('SELECT request_key, '
                'coalesce(length(cast(request_json AS BLOB)),0) + '
                'coalesce(length(cast(response_json AS BLOB)),0) + '
                'coalesce(length(cast(error_json AS BLOB)),0) FROM calls'
                + where + ' ORDER BY rowid DESC LIMIT ? OFFSET ?', (limit, offset)).fetchall()
            result, used = [], 0
            for key, size in headers:
                if size > max_record_bytes or used + size > max_page_bytes:
                    result.append({'request_id': key, 'omitted': 'inspection_byte_limit', 'stored_bytes': size})
                    continue
                # Recheck in the SELECT: a writer may have filled the response
                # since the header query; it cannot bypass this byte admission.
                row = db.execute('SELECT request_json, response_json, error_json FROM calls WHERE request_key=? '
                    'AND coalesce(length(cast(request_json AS BLOB)),0) + '
                    'coalesce(length(cast(response_json AS BLOB)),0) + '
                    'coalesce(length(cast(error_json AS BLOB)),0) <= ?',
                    (key, min(max_record_bytes, max_page_bytes - used))).fetchone()
                if row is None:
                    result.append({'request_id': key, 'omitted': 'record_changed_or_exceeds_limit'})
                    continue
                used += sum(len(value.encode('utf-8')) for value in row if value)
                result.append({'request_id': key, **{kind: json.loads(value) if value else None
                    for kind, value in zip(('request', 'response', 'error'), row)}})
        return result

    def inspection_stats(self):
        """Scalar counts only; unanswered errors differ from active/unknown calls."""
        with self._lock:
            row = self._connect().execute('SELECT count(*), count(response_json), '
                'coalesce(sum(response_json IS NULL AND error_json IS NOT NULL),0) FROM calls').fetchone()
        return {'requests': row[0], 'responses': row[1], 'saved_errors': row[2],
                'without_response_or_error': row[0] - row[1] - row[2]}

    def requeue_uncertain(self, keys, *, actor, reason, operation_id=None):
        """Explicitly authorize another attempt after all old writers exit.

        Per-request OS leases exclude live writers (including other processes).
        The transaction archives the old attempt before removing its reservation;
        complete responses are never modified and the budget is never refunded.
        """
        self._require_writable()
        keys = sorted(set(keys))
        if not keys or not actor.strip() or not reason.strip():
            raise ValueError('Explicit request keys, actor and reason are required')
        operation_id = operation_id or uuid.uuid4().hex
        with self._lock, ExitStack() as stack:
            for key in keys:
                stack.enter_context(self._lease(key))
            db = self._connect()
            with db:
                db.execute('BEGIN IMMEDIATE')
                prior = db.execute('SELECT request_key, actor, reason FROM recovery_events WHERE operation_id=?',
                                   (operation_id,)).fetchall()
                if prior:
                    if sorted(prior) != [(key, actor, reason) for key in keys]:
                        raise ValueError('Recovery operation differs')
                    return {'operation_id': operation_id, 'keys': keys, 'reused': True}
                for key in keys:
                    row = db.execute('SELECT request_json, error_json, response_json FROM calls WHERE request_key=?', (key,)).fetchone()
                    if row is None or row[2] is not None:
                        raise ValueError('Only existing unanswered requests can be requeued: ' + key)
                    attempt = db.execute('SELECT count(*)+1 FROM recovery_events WHERE request_key=?', (key,)).fetchone()[0]
                    db.execute('INSERT INTO recovery_events (operation_id,request_key,actor,reason,recovered_at,request_json,error_json,attempt) VALUES (?, ?, ?, ?, ?, ?, ?, ?)',
                               (operation_id, key, actor, reason, time.time(), row[0], row[1], attempt))
                    db.execute('DELETE FROM calls WHERE request_key=?', (key,))
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
        with self._lock, ExitStack() as stack:
            for key in keys:
                stack.enter_context(self._lease(key))
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
                for key in keys:
                    row = db.execute('SELECT request_json,error_json,response_json FROM calls WHERE request_key=?', (key,)).fetchone()
                    if row is None or row[2] is None or json.loads(row[2]).get('status_code') not in statuses:
                        raise ValueError('Only matching saved HTTP errors can be requeued: ' + key)
                    attempt = db.execute('SELECT COALESCE(MAX(attempt),0)+1 FROM recovery_events WHERE request_key=?', (key,)).fetchone()[0]
                    db.execute('INSERT INTO recovery_events (operation_id,request_key,actor,reason,recovered_at,request_json,error_json,attempt,response_json) VALUES (?,?,?,?,?,?,?,?,?)',
                               (operation_id,key,actor,reason,time.time(),row[0],row[1],attempt,row[2]))
                    db.execute('DELETE FROM calls WHERE request_key=?', (key,))
        return {'operation_id': operation_id, 'keys': keys, 'reused': False}

    def recovery_history(self):
        with self._lock:
            cursor = self._connect().execute('SELECT * FROM recovery_events ORDER BY recovered_at, request_key')
            return [dict(zip([c[0] for c in cursor.description], row)) for row in cursor]

    def close(self):
        with self._lock:
            for lease in self._leases.values():
                lease.close()
            self._leases.clear()
            if self._db is not None:
                self._db.close()
                self._db = None
