"""Bounded read-only access to a run journal for hosts.

``RunJournal`` is the writer's object: it may initialize schema, hold a writer
connection and wait five seconds for a lock. A host answering a GET over a
possibly old, large or hostile run directory needs the opposite, so hosts used
to open ``run.sqlite`` themselves and query engine tables directly. This module
owns those queries instead, next to the schema they depend on.

The open_journal / read_run path entries guarantee:
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

from demiflow.execution.contracts.errors import PublicationMismatch
_STREAM_LANCE_SCHEMA = 'demiflow.stream_lance.v3'
from contextlib import closing, contextmanager
import hashlib
import json
import math
from pathlib import Path
import sqlite3
import stat
import time

from demiflow.execution.contracts.recovery_contract import MAX_DOCUMENT_BYTES, RecoveryContractError
from demiflow.execution.contracts.recovery_contract import document as _checked_document
from demiflow.execution.journal.run_journal import OBSERVATION_COUNT, _node_document, _node_key

__all__ = ['JOURNAL_BYTES', 'RunReadError', 'RunReader', 'open_journal', 'read_run',
           'storage_snapshot', 'observe_storage']

JOURNAL_BYTES = 64 * 1024**2
_MANIFEST_BYTES = 8 * 1024**2
_STATE_BYTES = 65536
_OBSERVATION_RECORD_BYTES = 8192
_OUTPUT_KINDS = ('intent', 'receipt', 'recovered', 'stage_commit', 'stage_recovered',
                 'result', 'recovered_result', 'failed')
_STATE_PROJECTION = ('attempt', 'revision', 'status', 'execution_stopped', 'cleanup',
                     'usage_complete', 'launch_phase', 'stop_phase')
_SUMMARY_FIELDS = ('state', 'rows_input', 'rows_output', 'calls', 'errors', 'elapsed_s',
                   'timing_scope')
_SUMMARY_SCOPE = ('plan_id', 'dag_id', 'plan_revision', 'occurrence_id')
_PAGE_SCOPE = ('plan_id', 'dag_id', 'plan_revision')


# A control document is stored as TEXT. Casting to BLOB makes substr count bytes, so the
# budget holds before the value reaches Python; the CASE keeps the storage-class check that
# the cast alone would erase. Anything that is not TEXT comes back as NULL and is refused.
_TEXT_BYTES = "CASE WHEN typeof(document)='text' THEN substr(CAST(document AS BLOB),1,?) END"


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
    before = path.stat(follow_symlinks=False)
    options = {} if cached_statements is None else {'cached_statements': cached_statements}
    db = sqlite3.connect(path.resolve().as_uri() + '?mode=ro', uri=True, timeout=timeout, **options)
    try:
        db.execute('PRAGMA cache_size=-2048')
        after = path.stat(follow_symlinks=False)
        if (not stat.S_ISREG(after.st_mode) or after.st_size > max_bytes
                or (before.st_dev, before.st_ino) != (after.st_dev, after.st_ino)
                or any(part.is_symlink() for part in (path, *path.parents))):
            raise RunReadError('Run journal identity changed while opening')
    except BaseException:
        db.close()
        raise
    return db


@contextmanager
def read_run(directory, *, timeout=1.0, deadline=None, transaction=True):
    """One short read transaction over a journal, as a :class:`RunReader`.

    ``deadline`` (``time.monotonic()`` value) interrupts a long statement.
    """
    if deadline is not None and (type(deadline) not in (int, float) or not math.isfinite(deadline)):
        raise RunReadError('Invalid read deadline', code='invalid_request')
    path = Path(directory) / 'run.sqlite'
    try:
        identity = path.stat(follow_symlinks=False)
    except OSError as exc:
        raise RunReadError('Run journal unavailable') from exc
    with closing(open_journal(directory, timeout=timeout)) as db:
        opened = path.stat(follow_symlinks=False)
        if (opened.st_dev, opened.st_ino) != (identity.st_dev, identity.st_ino):
            raise RunReadError('Run journal identity changed while opening')
        if deadline is not None:
            db.set_progress_handler(lambda: int(time.monotonic() >= deadline), 1000)
        if transaction:
            db.execute('BEGIN')
        yield RunReader(db)
        after = path.stat(follow_symlinks=False)
        if (not stat.S_ISREG(after.st_mode) or after.st_size > JOURNAL_BYTES
                or (after.st_dev, after.st_ino) != (identity.st_dev, identity.st_ino)
                or any(part.is_symlink() for part in (path, *path.parents))):
            raise RunReadError('Run journal identity changed while reading')


def fit_table_prefix(attempt, action_id):
    from demiflow.execution.contracts.recovery_contract import identity
    return 'fit-table-' + identity([attempt, action_id]) + '-'


class RunReader:
    """Typed reads over an open read-only journal connection.

    Methods never commit and never write. A table missing from an old journal
    is reported as absence (``None`` / ``False``), not as an error.
    """

    def __init__(self, db):
        self.db = db

    def fit_routes(self):
        """Original producer routes for an authorized stopped-run control operation."""
        if not self.has_table('runtime_sources'):
            return []
        rows = self.db.execute('SELECT ' + _TEXT_BYTES + """ FROM runtime_sources
            WHERE action_id >= 'fit-route-' AND action_id < 'fit-route-~'
            ORDER BY action_id LIMIT 1025""",(8193,)).fetchall()
        if len(rows) > 1024 or any(type(raw) is not bytes or len(raw)>8192 for raw, in rows):
            raise RunReadError("Fit maintenance routing exceeds budget")
        return [_bounded_object(raw) for raw, in rows]

    def fit_publication(self, run_id, parent, scope, commit_id):
        """Read the original model publication in this one journal snapshot."""
        if not self.has_table('runtime_commit_scopes'):
            return None
        row = self.db.execute('SELECT ' + _TEXT_BYTES + ' FROM runtime_identity LIMIT 1',
                              (MAX_DOCUMENT_BYTES + 1,)).fetchone()
        if (not row or type(row[0]) is not bytes or len(row[0]) > MAX_DOCUMENT_BYTES
                or _bounded_object(row[0]).get('run_id') != run_id):
            raise RunReadError('Training producer journal identity differs')
        row = self.db.execute('SELECT parent_action_id FROM runtime_commit_scopes WHERE scope_id=?',
                              (scope,)).fetchone()
        if not row or row[0] != parent:
            raise RunReadError('Training publication has no original parent scope')
        fact = _ReadonlyJournal._commit(self.db, commit_id)
        if not fact or fact['action_id'] != scope or fact['sequence'] != 1:
            raise RunReadError('Training publication commit identity differs')
        if fact['status'] != 'acked':
            return None
        if set(fact['receipts']) != {'model_manifest'}:
            raise RunReadError('Training publication receipt set differs')
        return fact['receipts']['model_manifest']

    def training_outputs(self, attempt, action_id):
        """Small attempt-scoped scoring-table facts, with no project/provider access."""
        _check_attempt(attempt)
        if not self.has_table('runtime_sources'):
            return None
        prefix = fit_table_prefix(attempt, action_id)
        rows = self.db.execute('SELECT ' + _TEXT_BYTES + """
            FROM runtime_sources WHERE action_id >= ? AND action_id < ?
            ORDER BY action_id LIMIT 9""", (8193, prefix, prefix + '~')).fetchall()
        if not rows:
            return None
        items = []
        for raw, in rows[:8]:
            if type(raw) is not bytes or len(raw) > 8192:
                raise RunReadError("Scoring provenance record exceeds budget")
            value = _bounded_object(raw)
            if value.get('attempt') != attempt or value.get('action_id') != action_id:
                raise RunReadError("Scoring provenance belongs to another action or attempt")
            items.append(value)
        return dict(models=items, has_more=len(rows)>len(items))

    def training_provenance(self, attempt, action_id, occurrence):
        """One bounded original version reference. Never opens weights or another project's index."""
        _check_attempt(attempt)
        if not self.has_table('runtime_sources'):
            return None
        from demiflow.execution.contracts.recovery_contract import identifier
        identifier(action_id)
        identifier(occurrence)
        def source(key):
            row = self.db.execute('SELECT ' + _TEXT_BYTES + ' FROM runtime_sources WHERE action_id=?',
                                  (MAX_DOCUMENT_BYTES + 1,key)).fetchone()
            if row is None:
                return None
            if type(row[0]) is not bytes or len(row[0]) > MAX_DOCUMENT_BYTES:
                raise RunReadError('Training provenance exceeds history read budget')
            return _bounded_object(row[0])
        selected = source(fit_table_prefix(attempt, action_id)+occurrence)
        if not selected:
            return None
        if (selected['attempt'],selected['action_id'],selected['occurrence']) != (attempt,action_id,occurrence):
            raise RunReadError('Training provenance scope differs')
        original = source(selected['provenance_id'])
        if not original or (original['instance'],original['manifest']) != (
                selected['instance'], selected['manifest_digest']):
            raise RunReadError('Original scoring-table ownership differs')
        result = dict(table=selected, provenance=original['provenance'])
        _compact(result, MAX_DOCUMENT_BYTES)
        return result

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
        row = self.db.execute(f'SELECT {_TEXT_BYTES} FROM {table} LIMIT 1', (maximum + 1,)).fetchone()
        if not row:
            return None
        if not isinstance(row[0], bytes) or len(row[0]) > maximum:
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
        raw = self.db.execute(f'SELECT {_TEXT_BYTES} FROM execution_state LIMIT 1', (_STATE_BYTES + 1,)).fetchone()
        if not raw or not isinstance(raw[0], bytes) or len(raw[0]) > _STATE_BYTES:
            raise RunReadError('Invalid run state')
        current = json.loads(raw[0])
        if attempt is not None and current.get('attempt') is None:
            raise RunReadError('Historical journal has no attempt contract', code='invalid_request')
        if attempt is not None and attempt != current.get('attempt'):
            raw = self.db.execute(f'SELECT {_TEXT_BYTES} FROM attempts WHERE attempt=? LIMIT 1',
                                  (_STATE_BYTES + 1, attempt)).fetchone()
            if not raw or not isinstance(raw[0], bytes) or len(raw[0]) > _STATE_BYTES:
                raise RunReadError('Unknown attempt', code='not_found')
            return json.loads(raw[0]), current.get('attempt')
        return current, current.get('attempt')

    def attempt_state(self, attempt):
        """The recorded document of one attempt, or ``None`` when absent or oversized."""
        row = self.db.execute('SELECT substr(CAST(document AS BLOB),1,262145) FROM attempts WHERE attempt=? LIMIT 1', (attempt,)).fetchone()
        if not row or len(row[0]) > 262144:
            return None
        try:
            return _bounded_object(row[0], characters=65536)
        except (ValueError, RecoveryContractError, UnicodeError):
            return None

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
        row = self.db.execute(f'SELECT {_TEXT_BYTES} FROM runtime_summary LIMIT 1', (_STATE_BYTES + 1,)).fetchone()
        if not row or not isinstance(row[0], bytes) or len(row[0]) > _STATE_BYTES:
            raise RunReadError('Runtime projection exceeds budget')
        return json.loads(row[0])

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
        scope, args = (' WHERE attempt=?', (attempt,)) if self._modern_events() else ('', ())
        return self.db.execute('SELECT coalesce(max(sequence),0) FROM action_events' + scope, args).fetchone()[0]

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

    def _modern_events(self):
        return 'attempt' in {row[1] for row in self.db.execute('PRAGMA table_info(action_events)')}

    def _events(self, *, attempt=None, action_id=None, kinds=None, through=False):
        clauses, args = [], []
        modern = self._modern_events()
        if modern and attempt is not None:
            clauses.append('attempt' + ('<=?' if through else '=?'))
            args.append(attempt)
        if action_id is not None:
            clauses.append('action_id=?'); args.append(action_id)
        if kinds:
            clauses.append('kind IN (' + ','.join('?' for _ in kinds) + ')'); args.extend(kinds)
        where = ' WHERE ' + ' AND '.join(clauses) if clauses else ''
        count, size = self.db.execute('SELECT count(*),coalesce(sum(length(CAST(document AS BLOB))),0) FROM action_events' + where, args).fetchone()
        limit = 256 if through else 10000
        if count > limit or size > 16 * 1024**2:
            raise RunReadError('Action history exceeds budget', code='resource_limit')
        found, seen = [], set()
        selector = 'attempt' if modern else '1'
        rows = self.db.execute('SELECT ' + selector + ',sequence,action_id,kind,document FROM action_events' + where + ' ORDER BY ' + ('attempt,' if modern else '') + 'sequence LIMIT ?', (*args, limit + 1))
        for a, seq, aid, kind, raw in rows:
            if type(a) is not int or not 1 <= a <= 32 or (a, aid, kind) in seen:
                raise RunReadError('Ambiguous action history')
            seen.add((a, aid, kind))
            value = _bounded_object(raw.encode() if isinstance(raw, str) else raw, _object=False)
            if kind in _OUTPUT_KINDS and kind != 'failed' and type(value) is not dict:
                raise RunReadError('Invalid output fact document')
            found.append(dict(attempt=a, sequence=seq, action_id=aid, kind=kind, value=value))
        return found

    def _output_selection(self, selected_attempt, action_id):
        _check_attempt(selected_attempt)
        if type(action_id) is not str or not 1 <= len(action_id) <= 64:
            raise RunReadError('Invalid output action', code='invalid_request')
        candidates = {}
        for event in self._events(attempt=selected_attempt, action_id=action_id, kinds=_OUTPUT_KINDS, through=True):
            candidates.setdefault(event['attempt'], {})[event['kind']] = event['value']
        current = candidates.get(selected_attempt, {})
        selected, retained = current, None
        if not any(k in current for k in ('receipt', 'recovered', 'stage_commit', 'stage_recovered')):
            for a in sorted(candidates, reverse=True):
                old = candidates[a]
                receipt = old.get('recovered') or old.get('receipt')
                if a < selected_attempt and (old.get('stage_recovered') or old.get('stage_commit') or receipt and receipt.get('status') == 'committed'):
                    selected, retained = old, a
                    break
        return current, selected, retained

    def output_projection(self, selected_attempt, action_id, *, diagnostics=False):
        """Interpret publication history once, without consulting target storage."""
        if not self._modern_events():
            if selected_attempt not in (None, 1):
                raise RunReadError('Historical journal has no attempt contract', code='invalid_request')
            state, current_attempt = self.state()
            selected_attempt = 1
        else:
            state, current_attempt = self.state(selected_attempt)
        current, selected, retained = self._output_selection(selected_attempt, action_id)
        receipt = selected.get('recovered') or selected.get('receipt')
        commit = selected.get('stage_recovered') or selected.get('stage_commit')
        evidence = None
        if commit:
            ref = commit.get('reference')
            if (type(ref) is not dict or type(ref.get('uri')) is not str
                    or type(ref.get('version')) is not int or ref['version'] < 1):
                raise RunReadError('Invalid committed stage reference')
            evidence = dict(kind='stage_recovered' if selected.get('stage_recovered') else 'stage_commit',
                            attempt=retained or selected_attempt, uri=ref['uri'], version=ref['version'],
                            schema_hash=None, row_count=None, schema_identity=commit.get('schema_identity'),
                            checkpoint_node=commit.get('checkpoint_node'))
        elif receipt and receipt.get('status') == 'committed':
            from demiflow.lance.model import LanceWriteReceipt
            try:
                parsed = LanceWriteReceipt.from_dict(receipt)
            except (ValueError, TypeError, KeyError) as exc:
                raise RunReadError('Invalid committed write receipt') from exc
            evidence = dict(kind='recovered' if selected.get('recovered') else 'receipt',
                            attempt=retained or selected_attempt, uri=parsed.uri, version=parsed.committed_version,
                            schema_hash=parsed.schema_hash, row_count=parsed.written_rows,
                            schema_identity=None, checkpoint_node=None)
        result = selected.get('recovered_result') or selected.get('result')
        if result is not None and (type(result) is not dict or type(result.get('ref')) is not dict):
            raise RunReadError('Invalid recorded result reference')
        current_state = self.document('execution_state', _STATE_BYTES) or {}
        value = dict(action_id=action_id, attempt=selected_attempt, current_attempt=current_attempt,
                     revision=current_state.get('revision', 0), action_sequence=self.action_sequence(selected_attempt),
                     has_intent='intent' in current, failed='failed' in current, error=current.get('failed'),
                     committed=evidence is not None, evidence=evidence, result=result,
                     result_ref=result.get('ref') if result else None, retained_from_attempt=retained)
        if diagnostics:
            value.update(receipt=receipt, commit=commit)
        return value

    def list_attempts(self):
        if not self.has_table('attempts'):
            return []
        values = []
        for raw, in self.db.execute('SELECT substr(CAST(document AS BLOB),1,65537) FROM attempts ORDER BY attempt LIMIT 33'):
            if len(values) == 32 or len(raw) > _STATE_BYTES:
                raise RunReadError('Attempt history exceeds budget', code='resource_limit')
            values.append(_bounded_object(raw))
        return values

    def execution_view(self, *, detail='full', action_ids=None, attempt=None):
        if detail not in {'full', 'summary', 'identity'} or (action_ids is not None and
                (type(action_ids) not in (list, tuple) or len(action_ids) > 10000
                 or any(type(a) is not str or not 1 <= len(a) <= 64 for a in action_ids))):
            raise RunReadError('Invalid execution view', code='invalid_request')
        manifest = self.document('manifest', _MANIFEST_BYTES)
        state, current = self.state(attempt)
        selected = state.get('attempt', 1)
        events = [] if detail == 'identity' else self._events(attempt=selected)
        if action_ids is not None:
            events = [e for e in events if e['action_id'] in action_ids]
        actions = {}
        for event in events:
            actions.setdefault(event['action_id'], {})[event['kind']] = event['value']
        completion, observations = None, []
        modern = self._modern_events()
        scope, args = (' WHERE attempt=?', (selected,)) if modern else ('', ())
        if detail == 'full':
            if self.has_table('completion'):
                row = self.db.execute('SELECT substr(CAST(document AS BLOB),1,?) FROM completion' + scope + ' LIMIT 1', (MAX_DOCUMENT_BYTES + 1, *args)).fetchone()
                if row:
                    if len(row[0]) > MAX_DOCUMENT_BYTES:
                        raise RunReadError('Completion exceeds budget', code='resource_limit')
                    completion = _bounded_object(row[0], _object=False)
            if self.has_table('node_observations'):
                count, size = self.db.execute('SELECT count(*),coalesce(sum(length(CAST(document AS BLOB))),0) FROM node_observations' + scope, args).fetchone()
                if count > OBSERVATION_COUNT or size > 8 * 1024**2:
                    raise RunReadError('Node observation store exceeds budget', code='resource_limit')
                observations = [dict(action_id=a, node_id=n, occurrence=o, **_bounded_object(raw.encode()))
                                for a, n, o, raw in self.db.execute('SELECT action_id,node_id,occurrence,document FROM node_observations' + scope + ' ORDER BY action_id,node_id,occurrence', args)]
        outputs = {}
        if detail != 'identity':
            for spec in (manifest or {}).get('outputs', {}).values():
                aid = spec['action_id']
                if action_ids is None or aid in action_ids:
                    outputs[aid] = self.output_projection(selected, aid, diagnostics=True)
        return dict(manifest=manifest, state=state, completion=completion, observations=observations,
                    events=[{k:v for k,v in e.items() if k != 'attempt'} for e in events],
                    actions=actions, current_attempt=current, revision=(self.document('execution_state', _STATE_BYTES) or {}).get('revision', 0),
                    output_projections=outputs)

    def preparation_evidence(self):
        """Conservative evidence only; directory ownership remains the host's."""
        if len(self.tables()) > 32:
            raise RunReadError('Preparation journal shape exceeds budget')
        state = self.document('execution_state', _STATE_BYTES)
        empty = dict(prepare_request_id=None, run_id=None, root=None)
        if state is not None:
            if state.get('status') is not None:
                return dict(status='not_proven', manifest_present=False, **empty)
            if state != {'status': None, 'revision': 0, 'attempt': 1}:
                raise RunReadError('Unknown preparation state')
        manifest = self.document('manifest', _MANIFEST_BYTES)
        if manifest is not None:
            empty = {key: manifest.get(key) for key in empty}
        if any(self.has_rows(t) for t in ('action_events', 'completion', 'stages', 'activities')):
            raise RunReadError('Preparation contains execution facts')
        return dict(status='never_started', manifest_present=manifest is not None, **empty)

    def native_maintenance_state(self):
        present = self.has_table('runtime_identity')
        identity = self.db.execute('SELECT 1 FROM runtime_identity LIMIT 1').fetchone() if present else None
        owner = self.has_table('runtime_owner')
        gc = self.has_table('runtime_dataflow_gc')
        if identity and not (owner and gc):
            raise RunReadError('Incomplete native maintenance schema')
        owned = owner and self.db.execute('SELECT 1 FROM runtime_owner LIMIT 1').fetchone() is not None
        pending = gc and self.db.execute("SELECT 1 FROM runtime_dataflow_gc WHERE status!='done' LIMIT 1").fetchone() is not None
        if not identity and (owned or gc and self.db.execute('SELECT 1 FROM runtime_dataflow_gc LIMIT 1').fetchone()):
            raise RunReadError('Native maintenance facts have no recovery identity')
        return dict(owner_released=not owned, gc_settled=not pending)

    def control_sequence(self, attempt):
        """Highest control event sequence of the attempt, or ``None`` without the table."""
        if not self.has_table('control_events'):
            return None
        return self.db.execute('SELECT coalesce(max(sequence),0) FROM control_events WHERE attempt=?',
                               (attempt,)).fetchone()[0]

    def control_events(self, attempt, *, after=0, limit=100):
        """``(sequence, raw JSON text)`` in order; the host applies its page byte budget."""
        return self.db.execute(
            'SELECT sequence,substr(document,1,122881) FROM control_events WHERE attempt=? AND sequence>? ORDER BY sequence LIMIT ?',
            (attempt, after, limit))


def _bounded_object(raw, *, characters=None, _object=True):
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
    if _object and type(value) is not dict:
        raise RecoveryContractError('Invalid historical metadata document')
    return value


def _read_object_roots(db):
    """Bound native namespace records before they leave SQLite."""
    records = []
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
    return records


def storage_snapshot(directory, *, _file_fd=None, _deadline=None, _footprint_only=False):
    """Bounded native ownership/control facts in a short read transaction.

    No schema initialization or writer connection. Close the read transaction
    before decoding or traversing storage. Optional runtime tables are absent
    in old journals.
    ``directory`` may be a host-pinned /proc/self/fd directory. The host owns
    directory traversal; this entry rejects a symlink journal and checks its
    inode before and after the read. Do not resolve away that pinned directory.
    This is not a filesystem snapshot or a retained usage summary.
    """
    path = Path(directory) / 'run.sqlite'

    def file_identity():
        info = path.stat(follow_symlinks=False)
        if not stat.S_ISREG(info.st_mode) or info.st_size > JOURNAL_BYTES:
            raise RecoveryContractError('Historical journal type/size exceeds budget')
        return info.st_dev, info.st_ino

    identity = file_identity()
    uri = (path.resolve().as_uri() if _file_fd is None else 'file:/proc/self/fd/' + str(_file_fd))
    with closing(sqlite3.connect(uri + '?mode=ro',
                                 uri=True, timeout=0.1, cached_statements=8)) as db:
        if _deadline is not None:
            db.set_progress_handler(lambda: int(time.monotonic() >= _deadline), 1000)
        db.execute('PRAGMA cache_size=-2048')
        try:
            db.execute('BEGIN')
            tables = {r[0] for r in db.execute(
                "SELECT name FROM sqlite_master WHERE type='table' AND name IN "
                "('manifest','execution_state','runtime_identity','runtime_object_roots') LIMIT 5")}
            if not _footprint_only and 'execution_state' not in tables:
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

            state = None if _footprint_only else document('execution_state', 4 * 65536)
            records = []
            if 'runtime_object_roots' in tables:
                records = _read_object_roots(db)
            # Validate/hash complete documents, then retain only storage
            # fields. No large source inventories live across traversal.
            manifest = None if _footprint_only else document('manifest', _MANIFEST_BYTES)
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


class _ReadonlyJournal:
    """Private native reader protocol; deliberately has no writer methods.

    Each connection is a short, bounded read. Native publication helpers can
    inspect their own receipts without borrowing a live writer or its leases.
    """
    def __init__(self, directory, *, attempt=None, deadline=None, connection=None):
        self.directory = Path(directory)
        self.path = self.directory / 'run.sqlite'
        self.attempt, self.deadline = attempt, deadline
        self._borrowed_connection = connection

    def check_read(self):
        if self.deadline is not None and time.monotonic() >= self.deadline:
            raise RunReadError('Native read deadline exceeded', code='resource_limit')

    @contextmanager
    def _connection(self):
        if self._borrowed_connection is not None:
            yield self._borrowed_connection
        else:
            self.check_read()
            with read_run(self.directory, deadline=self.deadline) as reader:
                yield reader.db
            self.check_read()

    def _one(self, table, column='document', *, where='', args=(), maximum=MAX_DOCUMENT_BYTES):
        # This helper is private; its fixed callers own all SQL identifiers.
        with self._connection() as db:
            if not db.execute("SELECT 1 FROM sqlite_master WHERE name=?", (table,)).fetchone():
                return None
            row = db.execute('SELECT substr(CAST(' + column + ' AS BLOB),1,?) FROM ' + table + where + ' LIMIT 2', (maximum + 1, *args)).fetchall()
            if len(row) > 1:
                raise RunReadError('Ambiguous native document')
            if not row or row[0][0] is None:
                return None
            raw = row[0][0]
            if len(raw) > maximum:
                raise RunReadError('Native document exceeds budget', code='resource_limit')
            return _bounded_object(raw)

    def object_roots(self):
        """Registered namespaces and their owner from one bounded read snapshot."""
        with self._connection() as db:
            if not db.execute("SELECT 1 FROM sqlite_master WHERE name='runtime_object_roots'").fetchone():
                return []
            records = _read_object_roots(db)
            binding = None
            if db.execute("SELECT 1 FROM sqlite_master WHERE name='runtime_identity'").fetchone():
                rows = db.execute('SELECT ' + _TEXT_BYTES + ' FROM runtime_identity LIMIT 2',
                                  (MAX_DOCUMENT_BYTES + 1,)).fetchall()
                if len(rows) > 1:
                    raise RunReadError('Ambiguous native identity')
                if rows:
                    raw = rows[0][0]
                    if not isinstance(raw, bytes) or len(raw) > MAX_DOCUMENT_BYTES:
                        raise RunReadError('Native identity exceeds budget', code='resource_limit')
                    binding = _bounded_object(raw)
            _checked_native_roots(dict(identity=binding, object_roots=records))
        return records

    def recovery_identity(self):
        return self._one('runtime_identity')

    def source_binding(self, action_id):
        return self._one('runtime_sources', where=' WHERE action_id=?', args=(action_id,))

    def execution_status(self):
        with read_run(self.directory, deadline=self.deadline) as reader:
            return reader.state(self.attempt if reader._modern_events() else None)[0]

    def execution_action(self, action_id):
        with self._connection() as db:
            if not db.execute("SELECT 1 FROM sqlite_master WHERE name='runtime_actions'").fetchone():
                return None
            row = db.execute('SELECT ' + ','.join('substr(CAST(' + c + ' AS BLOB),1,1048577)' for c in ('declaration','snapshot','result')) + ' FROM runtime_actions WHERE action_id=?', (action_id,)).fetchone()
            if row is None:
                return None
            if any(raw is not None and len(raw) > MAX_DOCUMENT_BYTES for raw in row):
                raise RunReadError('Native action exceeds budget', code='resource_limit')
            return dict(zip(('declaration','snapshot','result'), (_bounded_object(raw, _object=index == 0) if raw is not None else None for index, raw in enumerate(row))))

    @staticmethod
    def _commit(db, commit_id):
        row = db.execute('SELECT action_id,sequence,status,substr(CAST(intent AS BLOB),1,1048577),substr(CAST(prepared AS BLOB),1,1048577) FROM runtime_commits WHERE commit_id=?', (commit_id,)).fetchone()
        if row is None:
            return None
        raw_receipts = db.execute('SELECT sink_id,substr(CAST(document AS BLOB),1,1048577) FROM runtime_receipts WHERE commit_id=? LIMIT 65', (commit_id,)).fetchall()
        if len(raw_receipts) > 64 or any(raw is not None and len(raw) > MAX_DOCUMENT_BYTES for raw in (*row[3:], *(r[1] for r in raw_receipts))):
            raise RunReadError('Native commit exceeds budget', code='resource_limit')
        receipts = {k: _bounded_object(raw) for k, raw in raw_receipts}
        return dict(commit_id=commit_id, action_id=row[0], sequence=row[1], status=row[2],
                    intent=_bounded_object(row[3]), prepared=_bounded_object(row[4]) if row[4] else None,
                    receipts=receipts)

    def execution_commit(self, commit_id):
        with self._connection() as db:
            return self._commit(db, commit_id)

    def latest_execution_commit(self, action_id, *, confirmed=False):
        with self._connection() as db:
            if not db.execute("SELECT 1 FROM sqlite_master WHERE name='runtime_commits'").fetchone():
                return None
            row = db.execute('SELECT commit_id FROM runtime_commits WHERE action_id=?' + (" AND status!='pending'" if confirmed else '') + ' ORDER BY sequence DESC LIMIT 1', (action_id,)).fetchone()
            return self._commit(db, row[0]) if row else None

    def execution_commits(self, action_id):
        with self._connection() as db:
            if not db.execute("SELECT 1 FROM sqlite_master WHERE name='runtime_commits'").fetchone():
                return []
            rows = db.execute('SELECT commit_id FROM runtime_commits WHERE action_id=? ORDER BY sequence LIMIT 16385', (action_id,)).fetchall()
            if len(rows) > 16384:
                raise RunReadError('Native commit history exceeds budget', code='resource_limit')
            # All callers currently need the last commit. Bound aggregate bytes
            # nevertheless so this compatibility method cannot amplify history.
            total = 0; result = []
            for key, in rows:
                item = self._commit(db, key)
                total += len(_compact(item, JOURNAL_BYTES))
                if total > JOURNAL_BYTES:
                    raise RunReadError('Native commit history exceeds byte budget', code='resource_limit')
                result.append(item)
            return result


class _StorageObservation:
    """Linux namespace proof around SQLite's path-based readonly opens.

    The held main fd is not a SQLite fd. Inotify proves that path resolution
    cannot be redirected unnoticed between the checked chain and SQLite open.
    In-place writer transactions and SQLite SHM coordination remain legal.
    """
    _MUTATION = 0x40 | 0x80 | 0x100 | 0x200
    _SELF = 0x400 | 0x800 | 0x2000 | 0x8000
    _OVERFLOW = 0x4000

    def __init__(self, directory, directory_fd, deadline):
        import ctypes
        import os
        if deadline is not None and (type(deadline) not in (int, float) or not math.isfinite(deadline)):
            raise RunReadError('Invalid storage deadline', code='invalid_request')
        self.deadline = deadline
        self._fds, self._chain, self._watches = [], [], {}
        self._watch_fd = self._main_fd = None
        self._directory_fd = None
        self._identity = None
        try:
            if (directory is None) == (directory_fd is None):
                raise RunReadError('Provide exactly one storage directory', code='invalid_request')
            if directory_fd is not None:
                if type(directory_fd) is not int:
                    raise RunReadError('Invalid directory descriptor', code='invalid_request')
                borrowed = os.dup(directory_fd); self._fds.append(borrowed)
                info = os.fstat(borrowed)
                if not stat.S_ISDIR(info.st_mode):
                    raise RunReadError('Storage descriptor is not a directory')
                name = os.readlink('/proc/self/fd/' + str(borrowed))
                if name.endswith(' (deleted)'):
                    raise RunReadError('Storage directory is unlinked')
                directory = name
                self._borrowed_identity = info.st_dev, info.st_ino
            else:
                self._borrowed_identity = None
            path = Path(directory).absolute()
            if '..' in path.parts or len(path.parts) > 64:
                raise RunReadError('Storage directory ancestry exceeds budget')
            libc = ctypes.CDLL(None, use_errno=True)
            init = libc.inotify_init1
            init.argtypes = [ctypes.c_int]; init.restype = ctypes.c_int
            self._add = libc.inotify_add_watch
            self._add.argtypes = [ctypes.c_int, ctypes.c_char_p, ctypes.c_uint32]
            self._add.restype = ctypes.c_int
            self._watch_fd = init(os.O_NONBLOCK | os.O_CLOEXEC)
            if self._watch_fd < 0:
                self._watch_fd = None
                raise RunReadError('Storage namespace observation is unavailable')
            parent = os.open('/', os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW | os.O_CLOEXEC)
            self._fds.append(parent)
            for part in path.parts[1:]:
                self._watch(parent, part)
                child = os.open(part, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW | os.O_CLOEXEC, dir_fd=parent)
                self._fds.append(child)
                info = os.fstat(child)
                self._chain.append((parent, part, (info.st_dev, info.st_ino)))
                parent = child
                self._events()
            self._directory_fd = parent
            info = os.fstat(parent)
            if self._borrowed_identity is not None and self._borrowed_identity != (info.st_dev, info.st_ino):
                raise RunReadError('Storage directory identity changed')
            self._watch(parent, 'run.sqlite')
            try:
                self._main_fd = os.open('run.sqlite', os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK | os.O_CLOEXEC, dir_fd=parent)
            except FileNotFoundError:
                pass
            self.exists = self._main_fd is not None
            if self.exists:
                info = os.fstat(self._main_fd)
                self._regular(info)
                self._identity = info.st_dev, info.st_ino
            self._check()
            self.change_token = self._token()
        except BaseException:
            self.close()
            raise

    def _watch(self, fd, name):
        import os
        if len(self._watches) >= 64:
            raise RunReadError('Storage watch budget exceeded', code='resource_limit')
        wd = self._add(self._watch_fd, os.fsencode('/proc/self/fd/' + str(fd)), self._MUTATION | self._SELF)
        if wd < 0:
            raise RunReadError('Storage namespace watch is unavailable')
        self._watches.setdefault(wd, set()).add(name)

    def _events(self):
        import os
        import struct
        used = 0
        while True:
            try:
                raw = os.read(self._watch_fd, 65536)
            except BlockingIOError:
                break
            if not raw:
                raise RunReadError('Storage namespace watch closed')
            used += len(raw)
            if used > 1024 * 1024:
                raise RunReadError('Storage namespace event budget exceeded', code='resource_limit')
            offset = 0
            while offset < len(raw):
                if len(raw) - offset < 16:
                    raise RunReadError('Invalid storage namespace event')
                wd, mask, _, size = struct.unpack_from('iIII', raw, offset)
                end = offset + 16 + size
                if end > len(raw):
                    raise RunReadError('Invalid storage namespace event')
                name = os.fsdecode(raw[offset + 16:end].split(b'\0', 1)[0])
                if mask & (self._SELF | self._OVERFLOW) or wd not in self._watches or (mask & self._MUTATION and name in self._watches[wd]):
                    raise RunReadError('Storage namespace identity changed')
                offset = end

    @staticmethod
    def _regular(info):
        if not stat.S_ISREG(info.st_mode) or info.st_size > JOURNAL_BYTES:
            raise RunReadError('Historical journal type/size exceeds budget', code='resource_limit')

    def _check(self):
        import os
        if self.deadline is not None and time.monotonic() >= self.deadline:
            raise RunReadError('Storage observation deadline exceeded', code='resource_limit')
        self._events()
        for parent, name, expected in self._chain:
            current = os.stat(name, dir_fd=parent, follow_symlinks=False)
            if not stat.S_ISDIR(current.st_mode) or (current.st_dev, current.st_ino) != expected:
                raise RunReadError('Storage directory identity changed')
        try:
            linked = os.stat('run.sqlite', dir_fd=self._directory_fd, follow_symlinks=False)
        except FileNotFoundError:
            if self.exists:
                raise RunReadError('Storage journal identity changed')
        else:
            if not self.exists:
                raise RunReadError('Storage journal appeared during observation')
            self._regular(linked)
            pinned = os.fstat(self._main_fd); self._regular(pinned)
            if (linked.st_dev, linked.st_ino) != self._identity or (pinned.st_dev,pinned.st_ino) != self._identity:
                raise RunReadError('Storage journal identity changed')
        self._sidecars()
        self._events()

    def _sidecars(self):
        import os
        for name in ('run.sqlite-wal', 'run.sqlite-journal', 'run.sqlite-shm'):
            try:
                info = os.stat(name, dir_fd=self._directory_fd, follow_symlinks=False)
            except FileNotFoundError:
                continue
            if not stat.S_ISREG(info.st_mode):
                raise RunReadError('Storage sidecar is not a regular file')

    def _token(self):
        import os
        parts = []
        for name in ('run.sqlite', 'run.sqlite-wal', 'run.sqlite-journal'):
            try:
                st = os.stat(name, dir_fd=self._directory_fd, follow_symlinks=False)
                if not stat.S_ISREG(st.st_mode):
                    raise RunReadError('Storage sidecar is not a regular file')
                parts.append((name,st.st_dev,st.st_ino,st.st_size,st.st_mtime_ns,st.st_ctime_ns))
            except FileNotFoundError:
                parts.append((name,None))
        return hashlib.sha256(_compact(parts, 8192)).hexdigest()

    def snapshot(self):
        self._check()
        if not self.exists:
            value = dict(state=None, manifest=None, identity=None, object_roots=[], manifest_digest=None, identity_digest=None)
        else:
            try:
                value = storage_snapshot(Path('/proc/self/fd') / str(self._directory_fd),
                                         _file_fd=self._main_fd, _deadline=self.deadline)
            except RecoveryContractError as exc:
                raise RunReadError(str(exc)) from exc
        self._check()
        _checked_native_roots(value)
        return value

    def native_footprint(self):
        self._check()
        value = (storage_snapshot(Path('/proc/self/fd') / str(self._directory_fd),
                                  _file_fd=self._main_fd, _deadline=self.deadline, _footprint_only=True)
                 if self.exists else dict(identity=None, object_roots=[]))
        self._check()
        _checked_native_roots(value)
        return dict(relative_roots=('state','materializations','scratch'),
                    object_roots=tuple(value['object_roots'] or ()),
                    native_run_id=(value['identity'] or {}).get('run_id'))

    def close(self):
        import os
        if self._main_fd is not None:
            os.close(self._main_fd); self._main_fd = None
        if self._watch_fd is not None:
            os.close(self._watch_fd); self._watch_fd = None
        for fd in reversed(self._fds):
            os.close(fd)
        self._fds = []


def _checked_native_roots(value):
    """Original namespace ownership algorithm, kept inside its native domain."""
    from demiflow.execution.storage.object_resources import object_namespace
    identity = value['identity']
    run_id = identity.get('run_id') if identity else None
    if identity is not None and (type(run_id) is not str or len(run_id) != 32 or any(c not in '0123456789abcdef' for c in run_id)):
        raise RunReadError('Owned objects have invalid native run identity')
    for record in value['object_roots'] or ():
        if run_id is None or record != object_namespace(run_id, record['logical_root']):
            raise RunReadError('Owned object namespace differs from native identity')


@contextmanager
def observe_storage(directory=None, *, directory_fd=None, deadline=None):
    """Observe one native inode across bounded, independent readonly snapshots."""
    try:
        observation = _StorageObservation(directory, directory_fd, deadline)
    except (OSError, AttributeError) as exc:
        raise RunReadError('Storage namespace observation unavailable: ' + str(exc)) from exc
    try:
        try:
            yield observation
        except BaseException as original:
            try:
                observation._check()
            except BaseException as changed:
                original.add_note('Storage observation also failed: ' + str(changed))
            raise
        else:
            observation._check()
    finally:
        observation.close()


def _check_deadline(deadline):
    import time
    import math
    if deadline is not None and (type(deadline) not in (int, float) or not math.isfinite(deadline)):
        raise RecoveryContractError('Invalid publication read deadline')
    if deadline is not None and time.monotonic() >= deadline:
        raise RecoveryContractError('Publication read deadline exceeded')

def _publication_snapshot(directory, action_id, attempt, deadline):
    _check_deadline(deadline)
    with read_run(directory, deadline=deadline) as reader:
        state, _ = reader.state(attempt)
        selected = state.get('attempt', 1)
        projection = reader.output_projection(selected, action_id)
        current, effective, _ = reader._output_selection(selected, action_id)
        return projection, current, effective

def _latest_stream_lance_scope(journal, parent, node):
    """Newest ``stream_lance.v3`` commit scope of one occurrence, or ``None``.

    Old journals have no ``runtime_commit_scopes`` table; that is absence, not
    an error.
    """
    with journal._connection() as db:
        if not db.execute("SELECT 1 FROM sqlite_master WHERE name='runtime_commit_scopes'").fetchone():
            return None
        return db.execute(
            "SELECT scope_id,substr(declaration,1,65537) FROM runtime_commit_scopes WHERE parent_action_id=? "
            "AND json_extract(declaration,'$.node_id')=? AND json_extract(declaration,'$.schema')=? "
            "ORDER BY CAST(json_extract(declaration,'$.request.ordinal') AS INTEGER) DESC,scope_id DESC LIMIT 1",
            (parent, node, _STREAM_LANCE_SCHEMA)).fetchone()

def action_recovery_facts(directory, action_id, *, attempt=None, deadline=None):
    """Journal-only recovery eligibility; never proof that a target is absent."""
    from demiflow.execution.outputs.lance_sink import create_target_absent_binding
    _check_deadline(deadline)
    # The readonly protocol shares this single transaction for the entire fact.
    with read_run(directory, deadline=deadline) as reader:
        journal = _ReadonlyJournal(directory, attempt=attempt, deadline=deadline,
                                   connection=reader.db)
        state, current = reader.state(attempt)
        selected = state.get('attempt', 1)
        facts, _, _ = reader._output_selection(selected, action_id)
        native = journal.execution_action(action_id)
        identity = journal.recovery_identity()
        binding = (create_target_absent_binding(identity['run_id'], action_id, native['declaration'], facts.get('intent'))
                   if native is not None and identity is not None else None)
        absence = journal.source_binding(action_id + '-create-target-absent')
        if absence is not None:
            from demiflow.execution.outputs.lance_sink import spec_from_identity
            if native is None or identity is None or type(absence) is not dict:
                raise PublicationMismatch('Create absence has no original native action')
            spec = spec_from_identity(native['declaration']['sink'])
            recorded = create_target_absent_binding(identity['run_id'], action_id, native['declaration'],
                dict(kind='write_table', spec=spec.to_dict(), request_hash=absence.get('request_hash'),
                     operation_id=absence.get('operation_id')))
            if recorded is None or absence != recorded:
                raise PublicationMismatch('Create absence binding differs from original action')
        restricted = journal.source_binding(action_id + '-state-recovery-restriction')
        return dict(action_id=action_id, attempt=selected, current_attempt=current,
                    revision=(reader.document('execution_state', 65536) or {}).get('revision', 0),
                    native_action_present=native is not None,
                    native_completed=native is not None and native['result'] is not None,
                    recovery_restricted=native is not None and native['result'] is None and restricted is not None,
                    absence_eligible=binding is not None and journal.latest_execution_commit(action_id) is None,
                    absence_recorded=absence is not None)


def read_maintenance_snapshot(journal, expected_attempt, expected_revision, *, terminal=False, action_id=None):
    """Read one original maintenance snapshot without claiming or proving a stop."""
    import json
    from demiflow.errors import ExecutionConflict
    from demiflow.execution.contracts.recovery_contract import MAX_DOCUMENT_BYTES
    from demiflow.execution.outputs.lance_sink import create_target_absent_binding
    with read_run(journal.directory, deadline=journal._maintenance_deadline) as reader:
        state = reader.document('execution_state', 65536)
        if (state is None or state.get('attempt') != expected_attempt
                or expected_attempt is not None and type(state.get('attempt')) is not int
                or expected_attempt is None and reader.has_table('attempts')
                or journal.attempt is not None and journal.attempt != expected_attempt):
            raise ExecutionConflict('Stopped execution attempt differs')
        revision = state.get('revision')
        if type(revision) is not int or revision < 0:
            raise ExecutionConflict('Invalid stopped execution revision')
        if (expected_revision is None and (terminal or state.get('status') not in
                {'starting', 'running', 'stopping'}) or expected_revision is not None
                and expected_revision != revision):
            raise ExecutionConflict('Stopped execution revision differs or is required')
        if terminal and state.get('status') not in {'succeeded', 'failed', 'cancelled', 'interrupted'}:
            raise ExecutionConflict('Create absence admission requires a terminal attempt')

        def document(table, column, where='', parameters=()):
            if not reader.has_table(table):
                return None
            row = reader.db.execute('SELECT substr(CAST(' + column + ' AS BLOB),1,?) FROM ' +
                table + where + ' LIMIT 1', (MAX_DOCUMENT_BYTES + 1, *parameters)).fetchone()
            if row is None or row[0] is None:
                return None
            if not isinstance(row[0], bytes) or len(row[0]) > MAX_DOCUMENT_BYTES:
                raise RecoveryContractError('Native maintenance input exceeds document budget')
            value = json.loads(row[0])
            if type(value) is not dict:
                raise RecoveryContractError('Invalid native maintenance document')
            return value

        configuration = document('runtime_identity', 'document')
        if configuration is None:
            for table in ('runtime_owner', 'runtime_dataflow_gc'):
                if reader.has_table(table) and reader.db.execute('SELECT 1 FROM ' + table + ' LIMIT 1').fetchone():
                    raise ExecutionConflict('Native ownership or GC has no execution identity')
        elif expected_attempt is None:
            raise ExecutionConflict('Native maintenance requires an identified attempt')
        binding = None
        if action_id is not None and configuration is not None:
            declaration = document('runtime_actions', 'declaration', ' WHERE action_id=?', (action_id,))
            columns = {row[1] for row in reader.db.execute('PRAGMA table_info(action_events)')}
            intent = (document('action_events', 'document',
                " WHERE attempt=? AND action_id=? AND kind='intent'", (expected_attempt, action_id))
                if 'attempt' in columns else None)
            committed = reader.has_table('runtime_commits') and reader.db.execute(
                'SELECT 1 FROM runtime_commits WHERE action_id=? LIMIT 1', (action_id,)).fetchone()
            if not committed:
                binding = create_target_absent_binding(configuration['run_id'], action_id, declaration, intent)
            recorded = document('runtime_sources', 'document', ' WHERE action_id=?',
                                (action_id + '-create-target-absent',))
            if binding is not None and recorded is not None and recorded != binding:
                raise RecoveryContractError('Fixed source binding differs')
        else:
            declaration = None
    return state, configuration, binding, declaration


def read_maintenance_owner(journal, fence):
    """Return the existing fence owner fact, without claiming an owner."""
    with journal._connection() as db:
        return db.execute('SELECT 1 FROM runtime_owner WHERE fence=?', (fence,)).fetchone()


def read_runtime_owner(journal):
    """Read the current attempt/fence fact; no claim, maintenance or GC."""
    with journal._connection() as db:
        return db.execute("SELECT attempt,fence FROM runtime_owner").fetchone()


def require_gc_resource_cleanup(journal):
    """Require confirmation of the original physical resource allocations."""
    from demiflow.execution.contracts.errors import UnprovenGC
    with journal._connection() as db:
        if db.execute("SELECT 1 FROM sqlite_master WHERE name='resource_allocations'").fetchone():
            if db.execute("SELECT 1 FROM resource_allocations WHERE cleanup!='CONFIRMED' LIMIT 1").fetchone():
                raise UnprovenGC('Original resource cleanup is not confirmed for every allocation')


def gc_root_documents(journal, *, max_document_bytes):
    """Read one bounded original root row per connection, before decoding it."""
    from demiflow.execution.contracts.errors import UnprovenGC
    columns = {'runtime_actions': ('declaration','snapshot','result'),
        'runtime_sources': ('document',), 'runtime_commits': ('intent','prepared'),
        'runtime_receipts': ('document',), 'runtime_effects': ('document',),
        'runtime_commit_scopes': ('declaration',),
        'calls': ('request_json','response_json','error_json')}
    for table, fields in columns.items():
        after = 0
        with journal._connection() as db:
            existing = {row[1] for row in db.execute('PRAGMA table_info(' + table + ')')}
        if not existing:
            continue
        if not set(fields) <= existing:
            raise UnprovenGC('Unrecognized journal root schema: ' + table)
        while True:
            with journal._connection() as db:
                sizes = db.execute('SELECT rowid,' + ','.join('length(CAST('+f+' AS BLOB))' for f in fields) +
                    ' FROM ' + table + ' WHERE rowid>? ORDER BY rowid LIMIT 1', (after,)).fetchone()
                if sizes is None:
                    break
                if any(size is not None and size > max_document_bytes for size in sizes[1:]):
                    raise UnprovenGC('Journal root exceeds collector decode bound')
                rows = db.execute('SELECT rowid,' + ','.join(fields) + ' FROM ' + table +
                    ' WHERE rowid=?', (sizes[0],)).fetchall()
            if not rows:
                break
            for row in rows:
                after = row[0]
                for raw in row[1:]:
                    if raw is not None:
                        yield raw


def gc_owner_publications(journal, *, maximum):
    """Read the original typed publication proofs of bounded live owners."""
    from demiflow.execution.contracts.errors import UnprovenGC
    with journal._connection() as db:
        owners = db.execute('SELECT DISTINCT store_id,digest,key FROM artifact_owner LIMIT ?',
                            (maximum + 1,)).fetchall()
    if len(owners) > maximum:
        raise UnprovenGC('Owner roots exceed trace budget')
    for store, digest, key in owners:
        after, found = '', False
        while True:
            with journal._connection() as db:
                rows = db.execute('SELECT s.reservation_id,s.publication FROM runtime_dataflow_staging s '
                    'JOIN runtime_dataflow_settlements d ON s.reservation_id=d.reservation_id '
                    'WHERE d.store_id=? AND d.digest=? AND s.publication IS NOT NULL '
                    'AND s.reservation_id>? ORDER BY s.reservation_id LIMIT 16', (store,digest,after)).fetchall()
            if not rows:
                break
            for after, raw in rows:
                found = True
                yield raw
        if not found or key != digest:
            raise UnprovenGC('Owner lacks original typed publication proof')


def has_unowned_staging_reservation(journal):
    """Whether an old staging reservation lacks both publication owners."""
    with journal._connection() as db:
        return db.execute("SELECT 1 FROM runtime_reservations r LEFT JOIN runtime_dataflow_settlements s ON s.reservation_id=r.reservation_id LEFT JOIN runtime_dataflow_staging t ON t.reservation_id=r.reservation_id WHERE r.name='artifact_staging_bytes' AND s.reservation_id IS NULL AND t.reservation_id IS NULL LIMIT 1").fetchone()


def gc_object_page(journal, *, after):
    """Read the next original bounded object-ledger page in key order."""
    with journal._connection() as db:
        return db.execute('SELECT store_id,digest,bytes,charge_id FROM runtime_dataflow_objects '
            'WHERE (store_id,digest)>(?,?) ORDER BY store_id,digest LIMIT 64', after).fetchall()
