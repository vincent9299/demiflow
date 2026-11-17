"""Explicit, bounded local key/value storage for lookup and datasinks.

Reads never create a database. ``put_many`` writes one bounded transaction;
the native Dataset sink stages its input, then atomically publishes the original
commit's rows and SQLite receipt. Values are strict JSON, not Python code.
"""
from contextlib import closing
from dataclasses import dataclass
import json
import math
import os
from pathlib import Path
import sqlite3
import tempfile

from .data.datasink import Datasink, WriteResult
from ._kv_sink import DurableKVSink


def encode(value, limit):
    from .execution.stream_grouping import row_size
    try:
        size = row_size(value, limit)
    except ValueError as exc:
        raise ValueError('KV value exceeds max_value_bytes') from exc
    if size > limit:
        raise ValueError('KV value exceeds max_value_bytes')
    payload = json.dumps(value, ensure_ascii=False, sort_keys=True,
                         separators=(',', ':'), allow_nan=False).encode()
    if len(payload) > limit:
        raise ValueError('KV value exceeds max_value_bytes')
    return payload


def preserve_cache_origin(previous, proposed, *, same_value):
    """Keep completed stateful-cache provenance until its owner changes it.

    Ordinary KV entries remain replaceable. An unrelated writer cannot erase
    the only fact distinguishing an unacknowledged callback from a cache hit.
    Identical plain writes keep that fact; a new version/namespace is required
    to replace a result belonging to another state owner.
    """
    if previous is None:
        return proposed
    if proposed is None:
        if same_value:
            return previous
        raise ValueError('Stateful cache provenance forbids replacing this result; use a new version or namespace')
    try:
        old, new = json.loads(previous), json.loads(proposed)
        owner = old.get('owner') if type(old) is dict else None
        same_owner = (type(owner) is str and bool(owner) and type(new) is dict
                      and new.get('owner') == owner)
    except (ValueError, TypeError):
        same_owner = False
    if not same_owner:
        raise ValueError('Stateful cache provenance cannot be transferred to another owner')
    return proposed


@dataclass(frozen=True)
class SQLiteKV:
    path: str
    namespace: str
    max_value_bytes: int = 8 * 1024**2
    max_entries: int = 2_000_000
    max_bytes: int = 8 * 1024**3
    timeout_s: float = 30.
    missing: str = 'error'

    def __post_init__(self):
        if not isinstance(self.path, (str, Path)) or not str(self.path) or '://' in str(self.path):
            raise ValueError('SQLiteKV requires a local path')
        object.__setattr__(self, 'path', str(Path(self.path).expanduser().resolve()))
        if not isinstance(self.namespace, str) or not 1 <= len(self.namespace) <= 256:
            raise ValueError('SQLiteKV requires a bounded nonempty namespace')
        for name in ('max_value_bytes', 'max_entries', 'max_bytes'):
            if type(getattr(self, name)) is not int or getattr(self, name) < 1:
                raise ValueError('Invalid KV bound: ' + name)
        if self.max_value_bytes > 64 * 1024**2 or self.max_bytes < self.max_value_bytes:
            raise ValueError('Invalid KV byte bounds')
        if type(self.timeout_s) not in (int, float) or not math.isfinite(self.timeout_s) or not 0 < self.timeout_s <= 3600:
            raise ValueError('Invalid KV timeout')
        if self.missing not in {'error', 'empty'}:
            raise ValueError('KV missing must be error or empty')

    @property
    def identity(self):
        return {'backend': 'sqlite', 'path': self.path, 'namespace': self.namespace}

    @staticmethod
    def key(value):
        if not isinstance(value, str) or not value or len(value.encode()) > 65536:
            raise ValueError('KV key requires a nonempty string of at most 65536 bytes')
        return value

    def get(self, key, *, _with_origin=False):
        key = self.key(key)
        path = Path(self.path)
        if not path.exists() and self.missing == 'empty':
            return {'hit': False, 'value': None}
        with closing(sqlite3.connect(path.as_uri() + '?mode=ro', uri=True, timeout=self.timeout_s)) as db:
            db.execute('BEGIN')
            size = db.execute('SELECT length(CAST(value AS BLOB)) FROM kv WHERE namespace=? AND key=?',
                              (self.namespace, key)).fetchone()
            if size is None:
                return {'hit': False, 'value': None}
            if size[0] > self.max_value_bytes:
                raise ValueError('KV result exceeds max_value_bytes')
            value = db.execute('SELECT value FROM kv WHERE namespace=? AND key=?',
                               (self.namespace, key)).fetchone()[0]
            origin = None
            if _with_origin and db.execute("SELECT 1 FROM sqlite_master WHERE name='kv_origins'").fetchone():
                size = db.execute('SELECT length(CAST(origin AS BLOB)) FROM kv_origins WHERE namespace=? AND key=?',
                                  (self.namespace, key)).fetchone()
                if size is not None:
                    if type(size[0]) is not int or size[0] > 512:
                        raise ValueError('Invalid KV origin')
                    origin = db.execute('SELECT origin FROM kv_origins WHERE namespace=? AND key=?',
                                        (self.namespace, key)).fetchone()[0]
                    if type(origin) is not str:
                        raise ValueError('Invalid KV origin')
        result = {'hit': True, 'value': json.loads(value)}
        if _with_origin:
            result['origin'] = origin
        return result

    def _create_database(self):
        """Create only an absent database; never mutate a concurrent winner."""
        Path(self.path).parent.mkdir(parents=True, exist_ok=True)
        if not Path(self.path).exists():
            descriptor, temporary = tempfile.mkstemp(prefix='.kv-', dir=Path(self.path).parent)
            os.close(descriptor)
            try:
                with closing(sqlite3.connect(temporary)) as initial:
                    initial.execute('PRAGMA synchronous=FULL')
                    initial.execute('PRAGMA page_size=4096')
                    initial.execute('CREATE TABLE kv (namespace TEXT, key TEXT, value BLOB NOT NULL, PRIMARY KEY(namespace,key))')
                    initial.execute('CREATE TABLE kv_usage (namespace TEXT PRIMARY KEY, entries INTEGER NOT NULL, bytes INTEGER NOT NULL)')
                try:
                    os.link(temporary, self.path)
                except FileExistsError:
                    pass
            finally:
                os.unlink(temporary)

    def put_many(self, items, *, _origin=None, _origin_only=False):
        # Optional bounded provenance is not a result or commit authority. It
        # shares the result transaction, with at most one row per live KV key.
        if _origin is not None and (type(_origin) is not str or len(_origin.encode()) > 512):
            raise ValueError('Invalid KV origin')
        if _origin_only and _origin is None:
            raise ValueError('Origin-only write needs provenance')
        # Caller batches both count and bytes before entering this transaction.
        origin_bytes = len(_origin.encode()) if _origin is not None else 0
        encoded = []
        total = 0
        for key, value in items:
            payload = encode(value, self.max_value_bytes)
            total += len(payload) + origin_bytes
            if len(encoded) >= 256 or total > 32 * 1024**2:
                raise ValueError('KV write batch exceeds 256 rows or 32 MiB')
            encoded.append((self.namespace, self.key(key), payload))
        self._create_database()
        with closing(sqlite3.connect(self.path, timeout=self.timeout_s)) as db, db:
            db.execute('PRAGMA synchronous=FULL')
            db.execute('BEGIN IMMEDIATE')
            if _origin is not None:
                db.execute('CREATE TABLE IF NOT EXISTS kv_origins (namespace TEXT, key TEXT, origin TEXT NOT NULL, PRIMARY KEY(namespace,key))')
            has_origins = _origin is not None or db.execute("SELECT 1 FROM sqlite_master WHERE name='kv_origins'").fetchone() is not None
            usage = db.execute('SELECT entries,bytes FROM kv_usage WHERE namespace=?', (self.namespace,)).fetchone() or (0, 0)
            count, size = usage
            for namespace, key, payload in encoded:
                old = db.execute('SELECT length(CAST(value AS BLOB)) FROM kv WHERE namespace=? AND key=?', (namespace, key)).fetchone()
                count += int(old is None)
                old_origin = (db.execute('SELECT length(CAST(origin AS BLOB)) FROM kv_origins WHERE namespace=? AND key=?',
                                         (namespace, key)).fetchone() if has_origins else None)
                if old_origin is not None and (type(old_origin[0]) is not int or old_origin[0] > 512):
                    raise ValueError('Invalid KV origin')
                previous = (db.execute('SELECT origin FROM kv_origins WHERE namespace=? AND key=?',
                                      (namespace, key)).fetchone()[0] if old_origin else None)
                if previous is not None and type(previous) is not str:
                    raise ValueError('Invalid KV origin')
                current = (db.execute('SELECT CAST(value AS BLOB)=? FROM kv WHERE namespace=? AND key=?',
                                      (payload, namespace, key)).fetchone() if previous is not None or _origin_only else None)
                effective = preserve_cache_origin(previous, _origin,
                    same_value=current is not None and bool(current[0]))
                effective_bytes = len(effective.encode()) if effective is not None else 0
                # Provenance is real retained cache data, charged to the same
                # byte allowance. Plain historical databases have no overhead.
                size += len(payload) - (old[0] if old else 0) + effective_bytes - (len(previous.encode()) if previous else 0)
                if count > self.max_entries or size > self.max_bytes:
                    raise ValueError('KV capacity exceeded')
                if _origin_only:
                    if current is None or not current[0]:
                        raise ValueError('Completed cache result changed during state replay')
                else:
                    db.execute('INSERT INTO kv VALUES (?,?,?) ON CONFLICT(namespace,key) DO UPDATE SET value=excluded.value',
                               (namespace, key, payload))
                if effective is not None:
                    db.execute('INSERT INTO kv_origins VALUES (?,?,?) ON CONFLICT(namespace,key) DO UPDATE SET origin=excluded.origin',
                               (namespace, key, effective))
                elif has_origins:
                    db.execute('DELETE FROM kv_origins WHERE namespace=? AND key=?', (namespace, key))
            db.execute('INSERT INTO kv_usage VALUES (?,?,?) ON CONFLICT(namespace) DO UPDATE SET entries=excluded.entries,bytes=excluded.bytes',
                       (self.namespace, count, size))
        return len(encoded)

    def datasink(self, *, key='key', value='value', batch_rows=128, batch_bytes=16*1024**2):
        return KVDatasink(self, key, value, batch_rows, batch_bytes)


class KVDatasink(DurableKVSink, Datasink):
    def __init__(self, store, key, value, batch_rows, batch_bytes):
        if type(batch_rows) is not int or not 1 <= batch_rows <= 256:
            raise ValueError('Invalid KV sink batch_rows')
        if type(batch_bytes) is not int or not 1 <= batch_bytes <= 32*1024**2:
            raise ValueError('Invalid KV sink batch_bytes')
        self.store, self.key, self.value = store, key, value
        self.batch_rows, self.batch_bytes = batch_rows, batch_bytes
        self.receipt = None

    def on_write_start(self, schema=None):
        self.receipt = None

    def write(self, blocks, context):
        count = commits = size = 0
        pending = []
        for block in blocks:
            # Arrow rows are decoded one at a time, including wide values.
            for i in range(block.num_rows):
                row = block.slice(i, 1).to_pylist()[0]
                weight = len(encode(row[self.value], self.store.max_value_bytes))
                if weight > self.batch_bytes:
                    raise ValueError('KV sink single value exceeds batch_bytes')
                if pending and (len(pending) >= self.batch_rows or size + weight > self.batch_bytes):
                    count += self.store.put_many(pending); commits += 1
                    pending, size = [], 0
                pending.append((row[self.key], row[self.value])); size += weight
        if pending:
            count += self.store.put_many(pending); commits += 1
        return WriteResult(count, blocks_written=commits, target=self.store.path,
                           metadata={'namespace': self.store.namespace, 'atomicity': 'batch'})

    def on_write_complete(self, result):
        self.receipt = result


class Lookup:
    concurrency = 1
    queue_depth = 1
    catch = ()
    label = 'lookup'

    def __init__(self, source, key, output, when):
        if not callable(getattr(source, 'get', None)):
            raise TypeError('lookup source must provide get(key) -> {hit,value}')
        if not isinstance(key, str) or not key or not isinstance(output, str) or not output or key == output:
            raise ValueError('lookup requires distinct key and output fields')
        self.source, self.key, self.output, self.when = source, key, output, when

    def __call__(self, row):
        if self.when is not None and not self.when(row):
            return row
        result = self.source.get(row[self.key])
        if not isinstance(result, dict) or set(result) != {'hit', 'value'} or type(result['hit']) is not bool:
            raise TypeError('lookup source returned an invalid receipt')
        return {**row, self.output: result}
