"""Versioned keyed records for run metadata and durable request journals.

All payloads live in Lance. A returned reference pins a table version and key;
locks are coordination files, never another authoritative data store.
"""
from __future__ import annotations
import fcntl
import json
from .storage import resolve_local_uri
from pathlib import Path
from .control import control_directory, table_lock_path
from dataclasses import dataclass


def canonical(value):
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(',', ':'), allow_nan=False)


@dataclass(frozen=True)
class RecordRef:
    relative_uri: str
    version: int
    key: str

    def to_dict(self):
        return {'relative_uri': self.relative_uri, 'version': self.version, 'key': self.key}

    @classmethod
    def from_dict(cls, value): return cls(**value)

    def read(self, root):
        return LanceRecordStore(root, self.relative_uri).get(self.key, version=self.version)


class LanceRecordStore:
    def __init__(self, root, relative_uri):
        self.root = Path(root)
        relative = Path(relative_uri)
        if relative.is_absolute() or '..' in relative.parts:
            raise ValueError('record store URI must be relative to root')
        self.relative_uri = str(relative)
        self.path = resolve_local_uri(self.root / relative)

    def get(self, key, *, version=None):
        if not self.path.exists():
            if version is not None: raise FileNotFoundError(self.path)
            return None
        import lance
        ds = lance.dataset(str(self.path), version=version)
        escaped = key.replace("'", "''")
        table = ds.scanner(filter=f"key = '{escaped}'", scan_in_order=True).to_table()
        return json.loads(table['payload'][-1].as_py()) if table.num_rows else None

    def items(self, *, version=None, prefix=None):
        if not self.path.exists(): return {}
        import lance
        predicate = "starts_with(key, '" + prefix.replace("'", "''") + "')" if prefix else None
        rows = lance.dataset(str(self.path), version=version).scanner(
            columns=['key','payload'], filter=predicate, scan_in_order=True).to_table().to_pylist()
        return {r['key']: json.loads(r['payload']) for r in rows}

    def keys(self, *, prefix=None):
        if not self.path.exists(): return set()
        import lance
        predicate = "starts_with(key, '" + prefix.replace("'", "''") + "')" if prefix else None
        return set(lance.dataset(str(self.path)).to_table(columns=['key'], filter=predicate)['key'].to_pylist())

    def reference(self, key):
        import lance
        ds = lance.dataset(str(self.path))
        escaped = key.replace("'", "''")
        rows = ds.scanner(columns=['written_version'], filter=f"key = '{escaped}'", scan_in_order=True).to_table().to_pylist()
        if not rows: raise KeyError(key)
        return RecordRef(self.relative_uri, rows[-1]['written_version'], key)

    def put(self, key, value, *, immutable=True):
        import lance
        import pyarrow as pa
        self.path.parent.mkdir(parents=True, exist_ok=True)
        with table_lock_path(self.path).open('a') as lock:
            fcntl.flock(lock, fcntl.LOCK_EX)
            prior = None
            if self.path.exists():
                try: prior = self.reference(key)
                except KeyError: pass
            existing = self.get(key) if prior is not None else None
            if prior is not None and existing != value and immutable:
                raise ValueError(f'Immutable record differs; use a new run: {key}')
            if prior is not None and existing == value:
                return prior
            schema = pa.schema([pa.field('key', pa.string(), nullable=False),
                                pa.field('payload', pa.large_string(), nullable=False),
                                pa.field('written_version', pa.int64(), nullable=False)])
            version = lance.dataset(str(self.path)).version + 1 if self.path.exists() else 1
            table = pa.Table.from_pylist([{'key': key, 'payload': canonical(value),
                                         'written_version': version}], schema=schema)
            ds = lance.write_dataset(table, str(self.path), mode='append' if self.path.exists() else 'create')
            return RecordRef(self.relative_uri, ds.version, key)
