"""Native call references and read-only access to historical Lance responses."""
from dataclasses import asdict, dataclass
from pathlib import Path
import json
import sqlite3
from ..execution.artifacts import resolve_local_artifact


@dataclass(frozen=True)
class PromptRecordRef:
    journal_path: str
    request_id: str
    kind: str
    attempt: int = 1

    def to_dict(self):
        value = asdict(self)
        if self.attempt == 1:
            value.pop('attempt')
        return value

    @classmethod
    def from_dict(cls, value):
        return cls(**value)

    def read(self, root=None):
        column = {'request': 'request_json', 'response': 'response_json', 'error': 'error_json'}[self.kind]
        path = Path(self.journal_path)
        if not path.is_absolute():
            path = Path(root or '.') / path
        db = sqlite3.connect(resolve_local_artifact(path).as_uri() + '?mode=ro', uri=True)
        try:
            if type(self.attempt) is not int or self.attempt < 1:
                raise ValueError('A call reference attempt must be positive')
            archives = db.execute("SELECT 1 FROM sqlite_master WHERE name='recovery_events'").fetchone()
            response_column = _archived_response_column(db) if archives else 'NULL'
            archived = (db.execute(f'SELECT request_json,error_json,{response_column} FROM recovery_events '
                'WHERE request_key=? AND attempt=?', (self.request_id,self.attempt)).fetchone()
                if archives else None)
            if archived is not None:
                # An HTTP rejection is a complete failed response. Old refs
                # remain bound to that attempt even after a later success.
                row = (archived[{'request':0,'error':1,'response':2}[self.kind]],)
            else:
                current = (db.execute('SELECT COALESCE(MAX(attempt),0)+1 FROM recovery_events '
                    'WHERE request_key=?',(self.request_id,)).fetchone()[0] if archives else 1)
                row = (db.execute(f'SELECT {column} FROM calls WHERE request_key=?', (self.request_id,)).fetchone()
                       if current == self.attempt else None)
            if row is None or row[0] is None:
                raise KeyError(self.request_id + '/' + self.kind)
            from .sqlite_offline import restore
            return restore(json.loads(row[0]))
        finally:
            db.close()


def read_call(ref, root=None):
    if 'journal_path' in ref:
        return PromptRecordRef.from_dict(ref).read(root)
    # Historical business results may still point to a fixed Lance snapshot.
    # This reader cannot write records or reserve calls.
    import lance
    from ..lance.storage import resolve_local_uri
    path = resolve_local_uri(Path(root or '.') / ref['relative_uri'])
    key = ref['key'].replace("'", "''")
    rows = lance.dataset(str(path), version=ref['version']).to_table(
        columns=['payload'], filter=f"key = '{key}'").to_pylist()
    if len(rows) != 1:
        raise KeyError(ref['key'])
    return json.loads(rows[0]['payload'])


def journal_options(root=None, relative_uri=None, *, path=None, timeout_s=30, legacy_source=None):
    """Bind the new journal and, if present, one fixed legacy source snapshot.

    The first journal access atomically imports existing calls before any request
    can be sent. This function only inspects metadata; it never starts a model.
    """
    if path is not None:
        if root is not None or relative_uri is not None:
            raise ValueError('Use path or a legacy source locator')
        return {'path': str(path), 'timeout_s': timeout_s, **({'legacy_source': legacy_source} if legacy_source else {})}
    path = Path(root).resolve() / relative_uri
    options = {'path': str(path.with_suffix('.sqlite'))}
    if path.suffix == '.lance' and path.exists():
        import lance
        options['legacy_source'] = {'root': str(Path(root).resolve()),
            'relative_uri': str(relative_uri), 'version': lance.dataset(str(path)).version}
    return options


def call_snapshot(*, path=None, root=None, relative_uri=None):
    """Read one consistent monitoring snapshot without opening a writer/importer.

    HTTP requests contain compact metadata. Offline requests are returned with
    external binary references, so monitoring does not load all input images.
    A legacy locator is used only when its replacement SQLite file is absent.
    """
    legacy = Path(root or '.') / relative_uri if relative_uri else None
    native = resolve_local_artifact(Path(path) if path is not None else legacy.with_suffix('.sqlite'))
    result = {'requests': {}, 'responses': {}, 'errors': {}}
    if native.exists():
        db = sqlite3.connect(native.resolve().as_uri() + '?mode=ro', uri=True)
        try:
            for key, request, response, error in db.execute(
                    'SELECT request_key, request_json, response_json, error_json FROM calls'):
                for kind, value in zip(result, (request, response, error)):
                    if value is not None or kind == 'requests':
                        result[kind][key] = json.loads(value) if value is not None else {}
        finally:
            db.close()
    elif legacy is not None and legacy.exists():
        import lance
        ds = lance.dataset(str(legacy))
        rows = ds.to_table(columns=['key', 'payload', 'written_version']).to_pylist()
        for row in sorted(rows, key=lambda row: row['written_version']):
            kind, _, key = row['key'].partition('/')
            target = {'request': 'requests', 'response': 'responses', 'transport_error': 'errors'}.get(kind)
            if target:
                result[target][key] = json.loads(row['payload'])
    return result


def _archived_response_column(db):
    """Read both pre-migration and current journals without opening a writer."""
    columns = {r[1] for r in db.execute('PRAGMA table_info(recovery_events)')}
    return 'response_json' if 'response_json' in columns else 'NULL'


def journal_totals(path):
    """Read-only SQL aggregation, without loading request/response bodies.

    Counts cover the entire native journal, including earlier invocations.
    Missing provider usage remains unknown rather than an invented zero bill.
    """
    path = resolve_local_artifact(path)
    if not path.exists():
        return {'requests':0,'responses':0,'transport_errors':0,'provider_responses':0,
                'usage_records':0,'input_tokens':0,'output_tokens':0,
                'http_successes':0,'http_errors':0}
    db=sqlite3.connect(path.resolve().as_uri()+'?mode=ro',uri=True,timeout=30)
    try:
        archived = db.execute("SELECT 1 FROM sqlite_master WHERE name='recovery_events'").fetchone()
        records = 'SELECT response_json,error_json FROM calls'
        if archived:
            records += f' UNION ALL SELECT {_archived_response_column(db)} AS response_json,error_json FROM recovery_events'
        row=db.execute('WITH attempts AS ('+records+''') SELECT COUNT(*), COALESCE(SUM(response_json IS NOT NULL),0),
            COALESCE(SUM(error_json IS NOT NULL),0),
            COALESCE(SUM(json_extract(response_json,'$.status_code') IS NOT NULL),0),
            COALESCE(SUM(json_type(response_json,'$.body.usage')='object'
                OR json_extract(error_json,'$.call.usage.prompt_tokens') IS NOT NULL
                OR json_extract(error_json,'$.call.usage.input_tokens') IS NOT NULL),0),
            COALESCE(SUM(COALESCE(json_extract(response_json,'$.body.usage.prompt_tokens'),
                                 json_extract(response_json,'$.body.usage.input_tokens'),
                                 json_extract(error_json,'$.call.usage.prompt_tokens'),
                                 json_extract(error_json,'$.call.usage.input_tokens'),0)),0),
            COALESCE(SUM(COALESCE(json_extract(response_json,'$.body.usage.completion_tokens'),
                                 json_extract(response_json,'$.body.usage.output_tokens'),
                                 json_extract(error_json,'$.call.usage.completion_tokens'),
                                 json_extract(error_json,'$.call.usage.output_tokens'),0)),0)
            , COALESCE(SUM(json_extract(response_json,'$.status_code') BETWEEN 200 AND 299),0)
            , COALESCE(SUM(json_extract(response_json,'$.status_code') >= 400),0)
            FROM attempts''').fetchone()
        # A saved response includes HTTP quota/authentication errors. Keep that
        # audit count distinct from successful HTTP exchanges and transport loss.
        return dict(zip(('requests','responses','transport_errors','provider_responses','usage_records','input_tokens','output_tokens','http_successes','http_errors'),row))
    finally:
        db.close()


def _provider_cache_observation(rows):
    successful = [r for r in rows if isinstance(r[5], int) and 200 <= r[5] < 300]
    known = [r for r in successful if type(r[6]) is int and type(r[7]) is int
             and 0 <= r[7] <= r[6]]
    inputs = sum(r[6] for r in successful if type(r[6]) is int and r[6] >= 0)
    reported_inputs = sum(r[6] for r in known)
    cached = sum(r[7] for r in known)
    return {'successful_responses':len(successful), 'reported_responses':len(known),
            'missing_or_invalid_responses':len(successful)-len(known),
            'input_tokens':inputs, 'cached_tokens_reported':cached,
            'input_tokens_with_cache_reports':reported_inputs,
            'token_hit_rate_on_reported_inputs':cached/reported_inputs if reported_inputs else None,
            'reported_cached_share_all_inputs':cached/inputs if inputs else None}


def journal_observation(path, *, since=None):
    """Read compact timing/accounting metadata; never load prompts or retry calls.

    ``since`` filters completed calls by the provider's creation timestamp.
    Pending reservations have no timestamp and are reported separately for the
    whole journal. They are not a measurement of active HTTP connections.
    """
    import statistics
    path = resolve_local_artifact(path)
    if not path.exists():
        return {'totals': journal_totals(path), 'pending_reservations': 0,
                'completed_in_window': 0, 'errors_in_window': 0,
                'response_mean_s': None, 'response_p50_s': None,
                'response_p95_s': None, 'window_timestamp': 'provider_created',
                'provider_cache_total':_provider_cache_observation([]),
                'provider_cache_window':_provider_cache_observation([])}
    with sqlite3.connect(path.resolve().as_uri() + '?mode=ro', uri=True) as db:
        records = 'SELECT response_json,error_json,1 AS current_attempt FROM calls'
        if db.execute("SELECT 1 FROM sqlite_master WHERE name='recovery_events'").fetchone():
            records += f' UNION ALL SELECT {_archived_response_column(db)} AS response_json,error_json,0 AS current_attempt FROM recovery_events'
        rows = db.execute('WITH attempts AS ('+records+''') SELECT response_json IS NOT NULL, error_json IS NOT NULL,
            json_extract(response_json,'$.elapsed_s'),
            COALESCE(json_extract(response_json,'$.body.created'),
                     json_extract(error_json,'$.call.partial_response.created')),current_attempt,
            json_extract(response_json,'$.status_code'),
            COALESCE(json_extract(response_json,'$.body.usage.prompt_tokens'),
                     json_extract(response_json,'$.body.usage.input_tokens')),
            COALESCE(json_extract(response_json,'$.body.usage.prompt_tokens_details.cached_tokens'),
                     json_extract(response_json,'$.body.usage.input_tokens_details.cached_tokens'),
                     json_extract(response_json,'$.body.usage.cached_input_tokens'),
                     json_extract(response_json,'$.body.usage.cache_read_input_tokens'))
            FROM attempts''').fetchall()
    pending = sum(r[4] and not r[0] and not r[1] for r in rows)
    window = [r for r in rows if since is None or (isinstance(r[3], (float, int)) and r[3] >= since)]
    elapsed = sorted(r[2] for r in window if r[0] and isinstance(r[2], (float, int)))
    import math
    return {'totals': journal_totals(path), 'pending_reservations': pending,
            'completed_in_window': sum(bool(r[0]) for r in window),
            'errors_in_window': sum(bool(r[1]) for r in window),
            'response_mean_s': statistics.mean(elapsed) if elapsed else None,
            'response_p50_s': statistics.median(elapsed) if elapsed else None,
            'response_p95_s': elapsed[math.ceil(len(elapsed)*.95)-1] if elapsed else None,
            'window_timestamp': 'provider_created',
            'provider_cache_total':_provider_cache_observation(rows),
            'provider_cache_window':_provider_cache_observation(window)}
