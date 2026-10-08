"""Shared exact-URL / SHA image reuse. Declaration performs no I/O.

Only successfully decoded original bytes enter the URL index. The object store
is content addressed; a URL is a retrieval alias, never an identity assertion.
SQLite rows are bounded metadata, not image payloads. ImageClient verifies the
actual file on reuse; no growing in-memory inventory or whole-library scan is required.
"""
from dataclasses import dataclass
from contextlib import asynccontextmanager
import asyncio
import fcntl
import time
from pathlib import Path
import hashlib
import json
import sqlite3

from demiflow.objects import LocalObjectStore
from .image_decode import local_path, verify_file


@dataclass(frozen=True)
class ImageLibrary:
    object_directory: str
    index_path: str
    lock_timeout_s: float = 30

    def __post_init__(self):
        import math
        for name in ('object_directory', 'index_path'):
            path = getattr(self, name)
            if not isinstance(path, str) or not Path(path).is_absolute():
                raise ValueError(name + ' must be an absolute local path')
        if self.object_directory == self.index_path:
            raise ValueError('Image index and object directory must differ')
        if not isinstance(self.lock_timeout_s, (int, float)) or not math.isfinite(self.lock_timeout_s) or not 0 < self.lock_timeout_s <= 300:
            raise ValueError('Invalid image library lock timeout')

    @property
    def identity(self):
        return ['image-library-v1', self.object_directory, self.index_path]

    @asynccontextmanager
    async def claim(self, key):
        """A bounded cross-run claim, released on success, error or cancellation."""
        directory = Path(self.index_path + '.locks')
        directory.mkdir(parents=True, exist_ok=True)
        # At most 4096 persistent inodes, one open descriptor per active fetch.
        shard = hashlib.sha256(key.encode()).hexdigest()[:3]
        with (directory / shard).open('a') as handle:
            deadline = time.monotonic() + self.lock_timeout_s
            try:
                while True:
                    try:
                        fcntl.flock(handle, fcntl.LOCK_EX | fcntl.LOCK_NB)
                        break
                    except BlockingIOError:
                        if time.monotonic() >= deadline:
                            raise TimeoutError('shared_image_lock_timeout')
                        await asyncio.sleep(min(.05, max(0, deadline-time.monotonic())))
                yield
            finally:
                fcntl.flock(handle, fcntl.LOCK_UN)

    def _db(self):
        Path(self.index_path).parent.mkdir(parents=True, exist_ok=True)
        db = sqlite3.connect(self.index_path, timeout=self.lock_timeout_s)
        try:
            db.execute('PRAGMA synchronous=FULL')
            db.execute('PRAGMA cache_size=-8192')
            tables = {row[0] for row in db.execute("SELECT name FROM sqlite_master WHERE type='table'")}
            if not {'images','image_library_meta'} <= tables:
                with db:
                    db.execute('BEGIN IMMEDIATE')
                    db.execute('CREATE TABLE IF NOT EXISTS images (url TEXT PRIMARY KEY, receipt TEXT NOT NULL)')
                    db.execute('CREATE TABLE IF NOT EXISTS image_library_meta (key TEXT PRIMARY KEY, value TEXT NOT NULL)')
                    db.execute('INSERT OR IGNORE INTO image_library_meta VALUES (?,?)', ('object_directory', self.object_directory))
                    db.execute('INSERT OR IGNORE INTO image_library_meta VALUES (?,?)', ('schema_version', '1'))
            meta = dict(db.execute('SELECT key,value FROM image_library_meta'))
            from demiflow.execution.artifacts import resolve_local_artifact
            if (meta.get('schema_version') != '1' or
                resolve_local_artifact(meta['object_directory']) != resolve_local_artifact(self.object_directory)):
                raise ValueError('Image index is bound to a different library/schema')
        except BaseException:
            db.close()
            raise
        return db

    def lookup(self, url):
        # A new shared library does not need initialization for a cache miss.
        if not Path(self.index_path).exists():
            return None
        db = self._db()
        try:
            row = db.execute('SELECT CASE WHEN length(CAST(receipt AS BLOB))<=65536 THEN receipt END '
                             'FROM images WHERE url=?', (url,)).fetchone()
            if row is None:
                return None
            if row[0] is None:
                raise ValueError('image_index_record_exceeds_budget')
            return json.loads(row[0])
        finally:
            db.close()

    def reference(self, sha256):
        return LocalObjectStore(self.object_directory).reference(sha256)

    def publish(self, receipt):
        if receipt['status'] != 'ok':
            raise ValueError('Only decoded images can enter the shared URL index')
        value = json.dumps(receipt, ensure_ascii=False, sort_keys=True)
        if len(value.encode()) > 64 * 1024:
            raise ValueError('image_index_record_exceeds_budget')
        urls = list(dict.fromkeys(u for u in (receipt.get('url'), receipt.get('final_url')) if u))
        if not urls:
            return
        db = self._db()
        try:
            with db:
                db.executemany('INSERT INTO images VALUES (?,?) ON CONFLICT(url) DO UPDATE SET receipt=excluded.receipt',
                               [(url, value) for url in urls])
        finally:
            db.close()
