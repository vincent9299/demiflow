"""Durable, first-arrival admission for a stream, without grouping its payloads.

Only accepted identities, unique keys and counters are retained. Replay delivers
the original owners once per action, even when input completion order changes.
Rejected rows retain their receipt downstream and never consume capacity.
"""
import asyncio
import hashlib
import json
from pathlib import Path
import sqlite3


def canonical(value):
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(',', ':'), allow_nan=False)


def quota_policy(quotas):
    if not isinstance(quotas, (list, tuple)) or len(quotas) > 16:
        raise ValueError('Admission allows at most 16 quotas')
    result = []
    for q in quotas:
        if not isinstance(q, dict) or set(q) != {'on', 'limit'}:
            raise ValueError('Each admission quota requires on and limit')
        on = q['on']
        if (not isinstance(on, (list, tuple)) or len(on) > 32 or len(set(on)) != len(on)
                or any(not isinstance(k, str) or not k for k in on)
                or q['limit'] is not None and (type(q['limit']) is not int or q['limit'] < 0)):
            raise ValueError('Invalid admission quota')
        result.append({'on': list(on), 'limit': q['limit']})
    return result


def relaxed_quotas(previous, current):
    """Only widen limits; retain the exact ordered grouping keys and counters."""
    return len(previous) == len(current) and all(
        a['on'] == b['on'] and (b['limit'] is None or
            a['limit'] is not None and b['limit'] >= a['limit'])
        for a, b in zip(previous, current))


def relaxed_policy(previous, current):
    fixed = ('key', 'unique_on', 'max_key_bytes')
    return (set(previous) == set(current) and all(previous[k] == current[k] for k in fixed)
        and relaxed_quotas(previous['quotas'], current['quotas'])
        and all(current[k] >= previous[k] for k in ('max_entries', 'max_disk_bytes')))


def key_digest(values, max_key_bytes):
    size = 0
    for value in values:
        if not isinstance(value, (str, int, bool)) or value == '' or value is None:
            raise ValueError('Admission keys must be nonempty scalar strings/integers')
        if isinstance(value, str) and len(value) > max_key_bytes:
            raise ValueError('Admission key exceeds max_key_bytes')
        if isinstance(value, int) and value.bit_length() > max_key_bytes*4:
            raise ValueError('Admission key exceeds max_key_bytes')
        size += len(value.encode()) if isinstance(value, str) else len(str(value))
        if size > max_key_bytes:
            raise ValueError('Admission key exceeds max_key_bytes')
    return hashlib.sha256(canonical(values).encode()).digest()




class AdmissionQuotaReader:
    """Read monotone admission counters without reserving or renewing capacity.

    One caller at a time; close after use. At most 16 indexed lookups, 64 KiB
    policy and 2 MiB SQLite cache. A positive answer is advisory: only the
    downstream admission writer can grant capacity. A missing journal is empty.
    """
    def __init__(self, path, *, quotas, max_key_bytes=16384):
        self.path, self.quotas = Path(path).resolve(), quota_policy(quotas)
        if type(max_key_bytes) is not int or not 1 <= max_key_bytes <= 65536:
            raise ValueError('Invalid admission bound: max_key_bytes')
        self.max_key_bytes, self.db = max_key_bytes, None

    def remaining(self, row):
        groups = [key_digest([row[k] for k in q['on']], self.max_key_bytes) for q in self.quotas]
        if self.db is None:
            if not self.path.exists():
                return [q['limit'] for q in self.quotas]
            db = sqlite3.connect(self.path.as_uri()+'?mode=ro', uri=True, timeout=5, check_same_thread=False)
            try:
                db.execute('PRAGMA cache_size=-2048')
                saved = db.execute('SELECT CASE WHEN length(CAST(value AS BLOB))<=65536 THEN value END FROM config LIMIT 1').fetchone()
                if saved is None or saved[0] is None:
                    raise ValueError('Admission policy missing or exceeds reader budget')
                policy = json.loads(saved[0])
                if not relaxed_quotas(policy['quotas'], self.quotas) or policy['max_key_bytes'] != self.max_key_bytes:
                    raise ValueError('Admission reader policy changed')
            except BaseException:
                db.close()
                raise
            self.db = db
        self.db.execute('BEGIN')
        try:
            counts = [self.db.execute('SELECT n FROM counts WHERE q=? AND k=?', (i, g)).fetchone()
                      for i, g in enumerate(groups)]
        finally:
            self.db.rollback()
        return [None if q['limit'] is None else max(0, q['limit']-(n[0] if n else 0))
                for q, n in zip(self.quotas, counts)]

    def close(self):
        if self.db is not None:
            self.db.close()
            self.db = None


class StreamAdmission:
    concurrency = 1
    queue_depth = 1
    label = 'admit_rows'
    catch = ()

    def __init__(self, *, path, key, unique_on, quotas, output, when, max_entries,
                 max_key_bytes, max_disk_bytes):
        if not isinstance(unique_on, (tuple, list)):
            raise TypeError('Admission unique_on must be a list of fields')
        self.path = Path(path).resolve()
        self.key, self.unique_on, self.output, self.when = key, tuple(unique_on), output, when
        if not all(isinstance(k, str) and k for k in (key, output, *self.unique_on)):
            raise ValueError('Admission fields must be nonempty strings')
        if len(set(self.unique_on)) != len(self.unique_on) or len(self.unique_on) > 32:
            raise ValueError('Admission unique_on must contain at most 32 distinct fields')
        if when is not None and not callable(when):
            raise TypeError('Admission when must be callable')
        self.quotas = quota_policy(quotas)
        for name, value, ceiling in [('max_entries', max_entries, 10_000_000),
                                     ('max_key_bytes', max_key_bytes, 65536),
                                     ('max_disk_bytes', max_disk_bytes, 16*1024**3)]:
            if type(value) is not int or not 1 <= value <= ceiling:
                raise ValueError('Invalid admission bound: ' + name)
        self.max_entries, self.max_key_bytes, self.max_disk_bytes = max_entries, max_key_bytes, max_disk_bytes
        self.config = canonical(dict(key=key, unique_on=list(self.unique_on), quotas=self.quotas,
                                     max_entries=max_entries, max_key_bytes=max_key_bytes,
                                     max_disk_bytes=max_disk_bytes))
        self.db = None
        self.lock = None
        self.delivered = set()  # fixed 32-byte digests, at most max_entries

    def digest(self, values):
        # Validate scalars and combined length before serializing key metadata.
        return key_digest(values, self.max_key_bytes)

    def start(self):
        from .artifacts import run_lock
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.lock = run_lock(self.path.with_suffix(self.path.suffix + '.owner'))
        self.lock.__enter__()
        self.db = sqlite3.connect(self.path, timeout=5, check_same_thread=False)
        self.db.execute('PRAGMA journal_mode=DELETE')
        self.db.execute('PRAGMA synchronous=FULL')
        self.db.execute('PRAGMA cache_size=-2048')
        page = self.db.execute('PRAGMA page_size').fetchone()[0]
        if self.db.execute('PRAGMA page_count').fetchone()[0]*page > self.max_disk_bytes:
            raise ValueError('Admission database exceeds max_disk_bytes')
        self.db.execute('PRAGMA max_page_count=' + str(max(1, self.max_disk_bytes//page)))
        self.db.executescript('''
            CREATE TABLE IF NOT EXISTS config(value TEXT NOT NULL);
            CREATE TABLE IF NOT EXISTS accepted(id BLOB PRIMARY KEY, signature BLOB NOT NULL, u BLOB UNIQUE NOT NULL);
            CREATE TABLE IF NOT EXISTS counts(q INTEGER, k BLOB, n INTEGER NOT NULL, PRIMARY KEY(q,k));
        ''')
        with self.db:
            previous = self.db.execute('SELECT value FROM config').fetchone()
            if previous is None:
                self.db.execute('INSERT INTO config VALUES(?)', (self.config,))
            elif previous[0] != self.config:
                if not relaxed_policy(json.loads(previous[0]), json.loads(self.config)):
                    raise ValueError('Admission policy changed; only explicit monotone limit increases are supported')
                self.db.execute('UPDATE config SET value=?', (self.config,))
        self.total = self.db.execute('SELECT count(*) FROM accepted').fetchone()[0]
        if self.total > self.max_entries:
            raise ValueError('Admission journal exceeds max_entries')
        self.delivered.clear()

    async def astart(self):
        await self.thread(self.start)

    @staticmethod
    async def thread(fn, *args):
        task = asyncio.create_task(asyncio.to_thread(fn, *args))
        try:
            return await asyncio.shield(task)
        except asyncio.CancelledError:
            await task
            raise

    def admit(self, row):
        if self.output in row:
            raise ValueError('Admission output would overwrite an input field')
        if self.when is not None and not self.when(row):
            return {**row, self.output: {'status': 'skipped', 'reason': ''}}
        ident = self.digest([row[self.key]])
        unique = self.digest([row[k] for k in self.unique_on]) if self.unique_on else ident
        groups = [self.digest([row[k] for k in q['on']]) for q in self.quotas]
        signature = hashlib.sha256(unique+b''.join(groups)).digest()
        with self.db:
            previous = self.db.execute('SELECT signature FROM accepted WHERE id=?', (ident,)).fetchone()
            if previous is not None:
                if previous[0] != signature:
                    raise ValueError('Admission identity was reused for different keys')
                status, reason = ('duplicate', 'already_delivered') if ident in self.delivered else ('admitted', 'reused')
            elif self.db.execute('SELECT 1 FROM accepted WHERE u=?', (unique,)).fetchone():
                status, reason = 'duplicate', 'unique_key_already_admitted'
            else:
                counts = [self.db.execute('SELECT n FROM counts WHERE q=? AND k=?', (i, g)).fetchone()
                          for i, g in enumerate(groups)]
                exhausted = next((i for i, (q, n) in enumerate(zip(self.quotas, counts))
                                  if q['limit'] is not None and (n[0] if n else 0) >= q['limit']), None)
                if exhausted is not None:
                    status, reason = 'limited', 'quota:' + str(exhausted)
                else:
                    if self.total >= self.max_entries:
                        raise ValueError('Admission state exceeds max_entries')
                    self.db.execute('INSERT INTO accepted VALUES(?,?,?)', (ident, signature, unique))
                    for i, g in enumerate(groups):
                        self.db.execute('INSERT INTO counts VALUES(?,?,1) ON CONFLICT(q,k) DO UPDATE SET n=n+1', (i, g))
                    status, reason = 'admitted', ''
        if status == 'admitted':
            if previous is None:
                self.total += 1
            self.delivered.add(ident)
        return {**row, self.output: {'status': status, 'reason': reason}}

    async def __call__(self, row):
        return await self.thread(self.admit, row)

    async def aclose(self):
        if self.db is not None:
            self.db.close()
            self.db = None
        if self.lock is not None:
            self.lock.__exit__(None, None, None)
            self.lock = None
