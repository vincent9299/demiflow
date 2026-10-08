"""Read a pinned snapshot of native fetch receipts, without opening a client."""
import hashlib
import json
from pathlib import Path
import re
import sqlite3
from contextlib import closing

from .contracts import DOCUMENT_RESULT


def document_receipt_factory(path, *, sha256, max_journal_bytes, max_receipt_bytes, max_rows):
    path = Path(path).expanduser().resolve()
    if not isinstance(sha256, str) or not re.fullmatch('[0-9a-f]{64}', sha256):
        raise ValueError('Document receipts require a pinned snapshot SHA256')
    for value in (max_journal_bytes,max_receipt_bytes,max_rows):
        if type(value) is not int or value < 1:
            raise ValueError('Receipt source budgets must be positive integers')

    def read():
        before = path.stat()
        if before.st_size > max_journal_bytes:
            raise ValueError('Document receipt journal exceeds max_journal_bytes')
        if any(Path(str(path)+suffix).exists() for suffix in ('-wal','-journal')):
            raise ValueError('Use a closed SQLite backup snapshot, not a live journal')
        digest = hashlib.sha256()
        with path.open('rb') as stream:
            while block := stream.read(1024*1024):
                digest.update(block)
        if digest.hexdigest() != sha256:
            raise ValueError('Document receipt snapshot SHA256 differs')
        # The only accepted input is an immutable, closed native cache snapshot.
        # Bound a value before asking SQLite to parse its JSON or Python to decode it.
        # Arrow may resume the same serialized source iterator on another thread.
        # Each iterator owns its connection; never share it between iterators.
        with closing(sqlite3.connect(path.as_uri()+'?mode=ro&immutable=1',uri=True,check_same_thread=False)) as db:
            db.setlimit(sqlite3.SQLITE_LIMIT_LENGTH, max_receipt_bytes+4096)
            db.execute('PRAGMA query_only=ON')
            db.execute('PRAGMA cache_size=-8192')
            db.execute('PRAGMA mmap_size=0')
            if db.execute('SELECT 1 FROM cache WHERE length(CAST(value AS BLOB))>? OR length(key)>4096 LIMIT 1',
                          (max_receipt_bytes,)).fetchone():
                raise ValueError('Journal value exceeds max_receipt_bytes')
            # Missing document_ref denotes search/other native cache entries;
            # JSON null is a failed document receipt and is deliberately retained.
            cursor = db.execute("SELECT key,value FROM cache WHERE json_type(value,'$.document_ref') IS NOT NULL ORDER BY rowid")
            count = 0
            for key, value in cursor:
                count += 1
                if count > max_rows:
                    raise ValueError('Document receipt source exceeds max_rows')
                receipt = json.loads(value)
                yield {'receipt_id': key, 'receipt': {f.name:receipt.get(f.name) for f in DOCUMENT_RESULT}}
        after = path.stat()
        if (before.st_size,before.st_mtime_ns,before.st_ino) != (after.st_size,after.st_mtime_ns,after.st_ino):
            raise ValueError('Document receipt snapshot changed while reading')
    return read
