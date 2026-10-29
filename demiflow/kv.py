"""Explicit, bounded local key/value storage for lookup and datasinks.

Reads never create a database. Each write batch is atomic; separate batches
and other sinks are separate commits. Values are strict JSON, not Python code.
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

    def get(self, key):
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
        return {'hit': True, 'value': json.loads(value)}

    def put_many(self, items):
        # Caller batches both count and bytes before entering this transaction.
        encoded = []
        total = 0
        for key, value in items:
            payload = encode(value, self.max_value_bytes)
            total += len(payload)
            if len(encoded) >= 256 or total > 32 * 1024**2:
                raise ValueError('KV write batch exceeds 256 rows or 32 MiB')
            encoded.append((self.namespace, self.key(key), payload))
        Path(self.path).parent.mkdir(parents=True, exist_ok=True)
        if not Path(self.path).exists():
            descriptor, temporary = tempfile.mkstemp(prefix='.kv-', dir=Path(self.path).parent)
            os.close(descriptor)
            try:
                with closing(sqlite3.connect(temporary)) as initial:
                    initial.execute('PRAGMA synchronous=FULL')
                    initial.execute('CREATE TABLE kv (namespace TEXT, key TEXT, value BLOB NOT NULL, PRIMARY KEY(namespace,key))')
                    initial.execute('CREATE TABLE kv_usage (namespace TEXT PRIMARY KEY, entries INTEGER NOT NULL, bytes INTEGER NOT NULL)')
                try:
                    os.link(temporary, self.path)
                except FileExistsError:
                    pass
            finally:
                os.unlink(temporary)
        with closing(sqlite3.connect(self.path, timeout=self.timeout_s)) as db, db:
            db.execute('PRAGMA synchronous=FULL')
            db.execute('BEGIN IMMEDIATE')
            usage = db.execute('SELECT entries,bytes FROM kv_usage WHERE namespace=?', (self.namespace,)).fetchone() or (0, 0)
            count, size = usage
            for namespace, key, payload in encoded:
                old = db.execute('SELECT length(CAST(value AS BLOB)) FROM kv WHERE namespace=? AND key=?', (namespace, key)).fetchone()
                count += int(old is None)
                size += len(payload) - (old[0] if old else 0)
                if count > self.max_entries or size > self.max_bytes:
                    raise ValueError('KV capacity exceeded')
                db.execute('INSERT INTO kv VALUES (?,?,?) ON CONFLICT(namespace,key) DO UPDATE SET value=excluded.value',
                           (namespace, key, payload))
            db.execute('INSERT INTO kv_usage VALUES (?,?,?) ON CONFLICT(namespace) DO UPDATE SET entries=excluded.entries,bytes=excluded.bytes',
                       (self.namespace, count, size))
        return len(encoded)

    def datasink(self, *, key='key', value='value', batch_rows=128, batch_bytes=16*1024**2):
        return KVDatasink(self, key, value, batch_rows, batch_bytes)


class KVDatasink(Datasink):
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
