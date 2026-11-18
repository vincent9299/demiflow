"""Bounded read-only access to a run journal for hosts.

``RunJournal`` is the writer's object: it may initialize schema, hold a writer
connection and wait five seconds for a lock. A host answering a GET over a
possibly old, large or hostile run directory needs the opposite, so hosts used
to open ``run.sqlite`` themselves and query engine tables directly. This module
owns those queries instead, next to the schema they depend on.

Guarantees of every entry here:
  * the file is opened ``mode=ro``; no schema is created and nothing is written;
  * a symlink anywhere in the journal path, a non-regular file or a journal
    above ``JOURNAL_BYTES`` is refused before SQLite sees it;
  * the default lock wait is one second;
  * documents are bounded in SQL before they reach Python.

Paging, cursors, response budgets, permissions and error-code presentation stay
with the host. Failures raise ``RunReadError``; its ``code`` is one of
``invalid_request``, ``not_found``, ``resource_limit`` or ``None`` (rejected).

The SQL was moved from ``workbench/runs/view.py``, ``storage.py``, ``domain.py``
and ``requests.py`` without changing what it selects or how it is bounded.
"""
from contextlib import closing, contextmanager
import hashlib
import json
from pathlib import Path
import sqlite3
import stat
import time

from .recovery_contract import MAX_DOCUMENT_BYTES, RecoveryContractError
from .recovery_contract import document as _checked_document
from .run_journal import OBSERVATION_COUNT, _node_document, _node_key

__all__ = ['JOURNAL_BYTES', 'RunReadError', 'RunReader', 'open_journal', 'read_run',
           'storage_snapshot']

JOURNAL_BYTES = 64 * 1024**2
_MANIFEST_BYTES = 8 * 1024**2
_STATE_BYTES = 65536
_OBSERVATION_RECORD_BYTES = 8192
_OUTPUT_KINDS = ('intent', 'receipt', 'recovered', 'stage_commit', 'stage_recovered',
                 'result', 'recovered_result')
_STATE_PROJECTION = ('attempt', 'revision', 'status', 'execution_stopped', 'cleanup',
                     'usage_complete', 'launch_phase', 'stop_phase')
_SUMMARY_FIELDS = ('state', 'rows_input', 'rows_output', 'calls', 'errors', 'elapsed_s',
                   'timing_scope')
_SUMMARY_SCOPE = ('plan_id', 'dag_id', 'plan_revision', 'occurrence_id')
_PAGE_SCOPE = ('plan_id', 'dag_id', 'plan_revision')


class RunReadError(RecoveryContractError):
    """A bounded journal read was refused. ``code`` tells the host how to present it."""

    def __init__(self, message, *, code=None):
        super().__init__(message)
        self.code = code


def _compact(value, maximum):
    """Canonical compact JSON, bounded while encoding (same bytes hosts hashed before)."""
    output = bytearray()
    encoder = json.JSONEncoder(ensure_ascii=False, sort_keys=True, separators=(',', ':'),
                               allow_nan=False)
    for part in encoder.iterencode(value):
        encoded = part.encode('utf-8')
        if len(output) + len(encoded) > maximum:
            raise RunReadError('Journal projection exceeds byte budget', code='resource_limit')
        output.extend(encoded)
    return bytes(output)


def _check_attempt(attempt):
    if attempt is not None and (type(attempt) is not int or not 1 <= attempt <= 32):
        raise RunReadError('Invalid attempt', code='invalid_request')


def open_journal(directory, *, timeout=1.0, max_bytes=JOURNAL_BYTES, cached_statements=None):
    """Open ``<directory>/run.sqlite`` read-only under the host read policy.

    The caller owns the connection and must close it. Prefer :func:`read_run`.
    """
    path = Path(directory) / 'run.sqlite'
    for part in (path, *path.parents):
        if part.is_symlink():
            raise RunReadError('Symlink in run journal path: ' + str(part))
    if not path.is_file() or path.stat().st_size > max_bytes:
        raise RunReadError('Run journal unavailable or exceeds budget')
    options = {} if cached_statements is None else {'cached_statements': cached_statements}
    db = sqlite3.connect(path.resolve().as_uri() + '?mode=ro', uri=True, timeout=timeout, **options)
    try:
        db.execute('PRAGMA cache_size=-2048')
    except BaseException:
        db.close()
        raise
    return db


@contextmanager
def read_run(directory, *, timeout=1.0, deadline=None, transaction=True):
    """One short read transaction over a journal, as a :class:`RunReader`.

    ``deadline`` (``time.monotonic()`` value) interrupts a long statement.
    """
    with closing(open_journal(directory, timeout=timeout)) as db:
        if deadline is not None:
            db.set_progress_handler(lambda: int(time.monotonic() >= deadline), 1000)
        if transaction:
            db.execute('BEGIN')
        yield RunReader(db)


class RunReader:
    """Typed reads over an open read-only journal connection.

    Methods never commit and never write. A table missing from an old journal
    is reported as absence (``None`` / ``False``), not as an error.
    """

    def __init__(self, db):
        self.db = db

    # -- shape -------------------------------------------------------------
    def tables(self, limit=33):
        return {row[0] for row in self.db.execute(
            "SELECT name FROM sqlite_master WHERE type='table' LIMIT ?", (limit,))}

    def has_table(self, name):
        return self.db.execute('SELECT 1 FROM sqlite_master WHERE name=?', (name,)).fetchone() is not None

    def has_rows(self, table):
        if table not in ('action_events', 'completion', 'stages', 'activities'):
            raise ValueError('Unsupported table')
        return self.has_table(table) and self.db.execute(f'SELECT 1 FROM {table} LIMIT 1').fetchone() is not None

    def document(self, table, maximum):
        """The single JSON object document of a control table, or ``None`` if absent."""
        if table not in ('execution_state', 'manifest'):
            raise ValueError('Unsupported table')
        if not self.has_table(table):
            return None
        row = self.db.execute(f'SELECT substr(document,1,?) FROM {table} LIMIT 1', (maximum + 1,)).fetchone()
        if not row:
            return None
        if type(row[0]) is not str or len(row[0].encode()) > maximum:
            raise RunReadError('Journal control document exceeds budget', code='resource_limit')
        value = json.loads(row[0])
        if type(value) is not dict:
            raise RunReadError('Invalid journal control document')
        return value

    # -- identity and control state ----------------------------------------
    def manifest_identity(self):
        """Bounded sealed identity: ``root``, ``run_id``, ``digest``, ``format``.

        ``digest``/``format`` are ``None`` unless they are JSON strings of at
        most 64 bytes. The manifest itself is never loaded into Python.
        """
        size = self.db.execute('SELECT length(CAST(document AS BLOB)) FROM manifest LIMIT 1').fetchone()
        if not size or size[0] > _MANIFEST_BYTES:
            raise RunReadError('Run manifest exceeds native budget', code='resource_limit')
        row = self.db.execute(
            "SELECT json_extract(document,'$.root'),json_extract(document,'$.run_id'),"
            "json_type(document,'$.digest'),substr(CAST(json_extract(document,'$.digest') AS BLOB),1,65),"
            "json_type(document,'$.format'),substr(CAST(json_extract(document,'$.format') AS BLOB),1,65) "
            'FROM manifest LIMIT 1').fetchone()

        def text(kind, raw):
            if kind != 'text' or not isinstance(raw, bytes) or len(raw) > 64:
                return None
            return raw.decode('utf-8', 'replace')

        return dict(root=row[0], run_id=row[1], digest=text(row[2], row[3]), format=text(row[4], row[5]))

    def state(self, attempt=None):
        """``(state document, current attempt)``; a historical attempt reads ``attempts``."""
        _check_attempt(attempt)
        raw = self.db.execute('SELECT document FROM execution_state').fetchone()
        if not raw or len(raw[0].encode()) > _STATE_BYTES:
            raise RunReadError('Invalid run state')
        current = json.loads(raw[0])
        if attempt is not None and attempt != current.get('attempt'):
            raw = self.db.execute('SELECT document FROM attempts WHERE attempt=?', (attempt,)).fetchone()
            if not raw or len(raw[0].encode()) > _STATE_BYTES:
                raise RunReadError('Unknown attempt', code='not_found')
            return json.loads(raw[0]), current.get('attempt')
        return current, current.get('attempt')

    def attempt_state(self, attempt):
        """The recorded document of one attempt, or ``None`` when absent or oversized."""
        row = self.db.execute('SELECT document FROM attempts WHERE attempt=?', (attempt,)).fetchone()
        if not row or len(row[0]) > _STATE_BYTES:
            return None
        return json.loads(row[0])

    def state_projection(self, attempt=None):
        """``(selected, current)`` scalar control fields only; each at most 4 KiB."""
        _check_attempt(attempt)
        projection = 'json_object(' + ','.join(
            "'" + key + "',json_extract(document,'$." + key + "')" for key in _STATE_PROJECTION) + ')'

        def state(table, where='', parameters=()):
            # Table names and SQL come only from these two fixed branches.
            row = self.db.execute('SELECT length(CAST(document AS BLOB)), substr(CAST(' + projection +
                                  ' AS BLOB),1,4097) FROM ' + table + where + ' LIMIT 1', parameters).fetchone()
            if not row:
                raise RunReadError('Unknown attempt', code='not_found')
            if row[0] > _STATE_BYTES or len(row[1]) > 4096:
                raise RunReadError('Run context state exceeds budget', code='resource_limit')
            return json.loads(row[1])

        current = state('execution_state')
        selected = (current if attempt is None or attempt == current['attempt']
                    else state('attempts', ' WHERE attempt=?', (attempt,)))
        return selected, current

    def runtime_summary(self):
        """The managed-run summary document, or ``None`` for a pre-summary journal."""
        if not self.has_table('runtime_summary'):
            return None
        raw = self.db.execute('SELECT document FROM runtime_summary').fetchone()[0]
        if len(raw.encode()) > _STATE_BYTES:
            raise RunReadError('Runtime projection exceeds budget')
        return json.loads(raw)

    # -- node observations ---------------------------------------------------
    def observations_supported(self):
        """False for the historical observation table without an ``attempt`` column."""
        return 'attempt' in {row[1] for row in self.db.execute('PRAGMA table_info(node_observations)')}

    def observation_revision(self, attempt):
        """``(revision, count, sequence_total)`` over identity/sequence rows.

        reserve_nodes adds sequence=0 membership. observe_nodes only updates when
        sequence increases, and terminal records cannot change. Hashing membership
        as well as every sequence detects additions, updates, removals and restored
        older metadata; it does not rely on a control-event or execution revision.
        """
        digest = hashlib.sha256()
        count = total = sequence_total = 0
        for row in self.db.execute(
                'SELECT action_id,node_id,occurrence,sequence,terminal FROM node_observations '
                'WHERE attempt=? ORDER BY action_id,node_id,occurrence', (attempt,)):
            count += 1
            if count > OBSERVATION_COUNT:
                raise RunReadError('Observation metadata count exceeds native budget', code='resource_limit')
            _node_key(*row[:3])
            if type(row[3]) is not int or not 0 <= row[3] < 2**63 or row[4] not in (0, 1):
                raise RunReadError('Invalid native observation version')
            sequence_total += row[3]
            raw = _compact(list(row), 1024)
            total += len(raw)
            if total > 1024**2:
                raise RunReadError('Observation version scan exceeds 1MiB', code='resource_limit')
            digest.update(raw)
            digest.update(b'\n')
        return 'o' + digest.hexdigest(), count, sequence_total

    @staticmethod
    def _selection(attempt, node_id, action_id):
        clauses, values = ['attempt=?'], [attempt]
        for key, value in (('node_id', node_id), ('action_id', action_id)):
            if value is not None:
                clauses.append(key + '=?')
                values.append(value)
        return clauses, values

    def observation_scope(self, attempt, *, node_id=None, action_id=None):
        """The single native plan scope of the selected rows, or ``None``.

        Comes only from persisted producer observations; mixed or partial scopes
        are not a scope.
        """
        clauses, values = self._selection(attempt, node_id, action_id)
        query = ','.join("json_extract(document,'$." + name + "')" for name in _PAGE_SCOPE)
        scopes = self.db.execute('SELECT DISTINCT ' + query + ' FROM node_observations WHERE ' +
                                 ' AND '.join(clauses) + ' LIMIT 2', values).fetchall()
        if len(scopes) == 1 and all(value is not None for value in scopes[0]):
            return dict(zip(_PAGE_SCOPE, scopes[0]))
        return None

    def observation_rows(self, attempt, *, node_id=None, action_id=None, after=None, limit):
        """Validated full observation rows in key order; yields at most ``limit``.

        ``after`` is an exclusive ``(action_id, node_id, occurrence)`` position.
        Ask for one more than a page holds to learn whether more rows exist.
        """
        clauses, values = self._selection(attempt, node_id, action_id)
        if after is not None:
            clauses.append('(action_id,node_id,occurrence)>(?,?,?)')
            values.extend(after)
        sql = ('SELECT action_id,node_id,occurrence,sequence,length(CAST(document AS BLOB)),'
               'CASE WHEN length(CAST(document AS BLOB))<=8192 THEN document END FROM node_observations WHERE ' +
               ' AND '.join(clauses) + ' ORDER BY action_id,node_id,occurrence LIMIT ?')
        values.append(limit)
        for action, node, occurrence, sequence, length, raw in self.db.execute(sql, values):
            if raw is None or length > _OBSERVATION_RECORD_BYTES:
                raise RunReadError('Observation exceeds native record budget', code='resource_limit')
            document = json.loads(raw)
            _node_document(document)
            if document['sequence'] != sequence:
                raise RunReadError('Observation document differs from its native sequence')
            yield dict(document, action_id=action, node_id=node, occurrence=occurrence, sequence=sequence)

    def observation_summaries(self, attempt, limit=65):
        """Scalar projection of observations, non-terminal first; no full documents."""
        query = 'SELECT action_id,node_id,occurrence,' + ','.join(
            "json_extract(document,'$." + key + "')" for key in _SUMMARY_FIELDS + _SUMMARY_SCOPE)
        query += ' FROM node_observations WHERE attempt=? ORDER BY terminal,action_id,node_id,occurrence LIMIT ?'
        summaries = []
        for row in self.db.execute(query, (attempt, limit)):
            scope = row[3 + len(_SUMMARY_FIELDS):]
            summaries.append(dict(
                action_id=row[0], node_id=row[1], occurrence=row[2],
                **dict(zip(_SUMMARY_FIELDS, row[3:3 + len(_SUMMARY_FIELDS)])),
                **(dict(zip(_SUMMARY_SCOPE, scope)) if all(value is not None for value in scope) else {})))
        return summaries

    # -- action and control events -------------------------------------------
    def action_sequence(self, attempt):
        return self.db.execute('SELECT coalesce(max(sequence),0) FROM action_events WHERE attempt=?',
                               (attempt,)).fetchone()[0]

    def output_facts(self, attempt, action_id):
        """``{attempt: {kind: (status, reference)}}`` newest first, up to ``attempt``.

        Only publication kinds are read, and only their status and result
        reference; arbitrary action values never leave SQLite.
        """
        query = ("SELECT attempt,kind,json_extract(document,'$.status'),json_extract(document,'$.ref') "
                 'FROM action_events WHERE attempt<=? AND action_id=? AND kind IN (' +
                 ','.join('?' * len(_OUTPUT_KINDS)) + ') ORDER BY attempt DESC,sequence DESC LIMIT 256')
        candidates = {}
        for found, kind, status, reference in self.db.execute(query, (attempt, action_id, *_OUTPUT_KINDS)):
            candidates.setdefault(found, {})[kind] = (status, reference)
        return candidates

    def control_sequence(self, attempt):
        """Highest control event sequence of the attempt, or ``None`` without the table."""
        if not self.has_table('control_events'):
            return None
        return self.db.execute('SELECT coalesce(max(sequence),0) FROM control_events WHERE attempt=?',
                               (attempt,)).fetchone()[0]

    def control_events(self, attempt, *, after=0, limit=100):
        """``(sequence, raw JSON text)`` in order; the host applies its page byte budget."""
        return self.db.execute(
            'SELECT sequence,document FROM control_events WHERE attempt=? AND sequence>? ORDER BY sequence LIMIT ?',
            (attempt, after, limit))


def _bounded_object(raw, *, characters=None):
    """Bound decoded metadata before allocating JSON container objects."""
    if raw is None:
        return None
    depth = tokens = 0
    quoted = escaped = False
    for byte in raw:
        if quoted:
            if escaped:
                escaped = False
            elif byte == 92:
                escaped = True
            elif byte == 34:
                quoted = False
        elif byte == 34:
            quoted = True
            tokens += 1
        elif byte in (91, 123):
            depth += 1
            tokens += 1
        elif byte in (93, 125):
            depth -= 1
        elif byte in (44, 58):
            tokens += 1
        if depth > 64 or tokens > 524288:
            raise RecoveryContractError('Historical JSON structural budget exceeded')
    text = raw.decode('utf-8')
    if characters is not None and len(text) > characters:
        raise RecoveryContractError('Historical state exceeds budget')

    def invalid_constant(value):
        raise RecoveryContractError('Invalid historical JSON constant: ' + value)
    value = json.loads(text, parse_constant=invalid_constant)
    if type(value) is not dict:
        raise RecoveryContractError('Invalid historical metadata document')
    return value


def storage_snapshot(directory):
    """Bounded native ownership/control facts in a short read transaction.

    No schema initialization or writer connection. Close the read transaction
    before decoding or traversing storage. Optional runtime tables are absent
    in old journals.
    This is not a filesystem snapshot or a retained usage summary.
    """
    path = Path(directory) / 'run.sqlite'

    def file_identity():
        info = path.stat(follow_symlinks=False)
        if not stat.S_ISREG(info.st_mode) or info.st_size > JOURNAL_BYTES:
            raise RecoveryContractError('Historical journal type/size exceeds budget')
        return info.st_dev, info.st_ino

    identity = file_identity()
    with closing(sqlite3.connect(path.resolve().as_uri() + '?mode=ro',
                                 uri=True, timeout=0.1, cached_statements=8)) as db:
        db.execute('PRAGMA cache_size=-2048')
        try:
            db.execute('BEGIN')
            tables = {r[0] for r in db.execute(
                "SELECT name FROM sqlite_master WHERE type='table' AND name IN "
                "('manifest','execution_state','runtime_identity','runtime_object_roots') LIMIT 5")}
            if 'execution_state' not in tables:
                raise RecoveryContractError('Historical journal has no execution state table')

            def document(table, maximum):
                if table not in tables:
                    return None
                # Literal table names above; bound each BLOB before it is
                # returned to Python, including corrupt oversized records.
                rows = db.execute(
                    f"SELECT CASE WHEN typeof(document)='text' THEN "
                    f'substr(CAST(document AS BLOB),1,?) END FROM {table} LIMIT 2',
                    (maximum + 1,)).fetchall()
                if len(rows) > 1:
                    raise RecoveryContractError('Ambiguous historical ' + table)
                if not rows:
                    return None
                raw = rows[0][0]
                if not isinstance(raw, bytes) or len(raw) > maximum:
                    raise RecoveryContractError('Historical ' + table + ' exceeds byte budget')
                return raw

            state = document('execution_state', 4 * 65536)
            records = []
            if 'runtime_object_roots' in tables:
                fields = ('root_id', 'logical_root', 'physical_root', 'run_id')
                bounds = (64, 4096, 4096, 32)
                projection = ','.join(
                    f"CASE WHEN typeof({field})='text' THEN "
                    f'substr(CAST({field} AS BLOB),1,{maximum + 1}) END'
                    for field, maximum in zip(fields, bounds))
                rows = db.execute('SELECT ' + projection + ' FROM runtime_object_roots LIMIT 65').fetchall()
                if len(rows) > 64:
                    raise RecoveryContractError('Object resource count exceeds ceiling')
                for row in rows:
                    if any(not isinstance(v, bytes) or len(v) > maximum
                           for v, maximum in zip(row, bounds)):
                        raise RecoveryContractError('Historical object root field exceeds byte budget')
                    record = dict(zip(fields, (v.decode('utf-8') for v in row)))
                    # Same aggregate record limit used by register_object_root.
                    _checked_document(record, 8192)
                    records.append(record)
                # Sort only the bounded result, never an untrusted entire
                # table (legacy/corrupt schemas may lack the native index).
                records.sort(key=lambda record: record['root_id'])
            # Validate/hash complete documents, then retain only storage
            # fields. No large source inventories live across traversal.
            manifest = document('manifest', _MANIFEST_BYTES)
            binding = document('runtime_identity', MAX_DOCUMENT_BYTES)
        finally:
            if db.in_transaction:
                db.rollback()
    if file_identity() != identity:
        raise RecoveryContractError('Historical journal identity changed')
    # Decode after ending the short SQL read transaction. Even a bounded
    # large document must not retain a reader lock during Python parsing.

    def projected(raw, keys, *, characters=None):
        value = _bounded_object(raw, characters=characters)
        if value is None:
            return None, None
        result = {key: value[key] for key in keys if key in value}
        return result, hashlib.sha256(raw).hexdigest()

    state, _ = projected(state, ('status', 'attempt'), characters=65536)
    manifest, manifest_digest = projected(manifest, ('run_id', 'object_locations', 'report_digest'))
    binding, identity_digest = projected(binding, ('run_id',))
    return dict(state=state, manifest=manifest, identity=binding, object_roots=records,
                manifest_digest=manifest_digest, identity_digest=identity_digest)
