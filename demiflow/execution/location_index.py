"""Bounded, disposable indexes for large exact-path relocation manifests.

The JSON remains authoritative. Index identity includes its inode, size, mtime
and ctime; publishing/rebuilding never writes the manifest or mapped artifacts.
"""
from contextlib import contextmanager, closing
import errno
import fcntl
from functools import lru_cache
import hashlib
import json
import os
from pathlib import Path
import re
import sqlite3
import stat
import time
import uuid

MAX_SOURCE = 1024**3
MAX_INDEX = 2*1024**3
MAX_CACHE = 4*1024**3
MAX_RECORDS = 5_000_000
MAX_TOKEN = 16384  # Unicode characters; at most 64 KiB encoded per JSON value.
CHUNK = 8192
FORMAT = 'demiflow-location-index-v1'


class _JSON:
    """Incremental JSON values; containers are only materialized within MAX_TOKEN."""
    def __init__(self, stream):
        self.stream=stream; self.text=''; self.pos=0; self.eof=False
        self.decoder=json.JSONDecoder()

    def fill(self):
        rest=self.text[self.pos:]
        if len(rest)>=MAX_TOKEN: raise ValueError('Location manifest token exceeds 16384 characters')
        extra=self.stream.read(min(CHUNK,MAX_TOKEN-len(rest)))
        self.text=rest+extra; self.pos=0; self.eof=not extra

    def char(self, optional=False):
        while True:
            while self.pos<len(self.text) and self.text[self.pos] in ' \r\n\t': self.pos+=1
            if self.pos<len(self.text): return self.text[self.pos]
            if self.eof:
                if optional: return None
                raise ValueError('Incomplete location manifest')
            self.fill()

    def take(self, expected):
        if self.char()!=expected: raise ValueError('Invalid location manifest JSON')
        self.pos+=1

    def value(self):
        self.char()
        while True:
            try: value,end=self.decoder.raw_decode(self.text,self.pos)
            except json.JSONDecodeError:
                if self.eof: raise
                self.fill(); continue
            # A number at the buffer boundary may have more digits to come.
            if end==len(self.text) and not self.eof:
                self.fill(); continue
            if end<len(self.text) and self.text[end] not in ' \r\n\t,:}]':
                raise ValueError('Invalid location manifest value delimiter')
            self.pos=end
            return value


def _records(stream, field):
    parser=_JSON(stream); parser.take('{'); version=None; found=False
    while parser.char()!='}':
        key=parser.value()
        if not isinstance(key,str): raise ValueError('Invalid location manifest key')
        parser.take(':')
        if key==field:
            if parser.char()!='{':
                parser.value(); found=False
                yield None
                if parser.char()=='}': break
                parser.take(',')
                if parser.char()=='}': raise ValueError('Trailing comma in manifest')
                continue
            parser.take('{'); found=True
            yield None  # A repeated mapping field replaces the previous object.
            while parser.char()!='}':
                source=parser.value()
                if not isinstance(source,str): raise ValueError('Invalid location key')
                parser.take(':'); target=parser.value()
                yield source,json.dumps(target,ensure_ascii=False,separators=(',',':'))
                if parser.char()=='}': break
                parser.take(',')
                if parser.char()=='}': raise ValueError('Trailing comma in locations')
            parser.take('}')
        else:
            value=parser.value()
            if key=='version': version=value
        if parser.char()=='}': break
        parser.take(',')
        if parser.char()=='}': raise ValueError('Trailing comma in manifest')
    parser.take('}')
    if parser.char(optional=True) is not None: raise ValueError('Trailing location manifest data')
    if version!=1 or not found: raise ValueError('Invalid location manifest schema')


def identity(info):
    return (info.st_dev,info.st_ino,info.st_size,info.st_mtime_ns,info.st_ctime_ns)


def source_digest(filename, source_identity):
    """Bounded content proof when filesystem timestamp ticks can coincide."""
    if source_identity[2] > MAX_SOURCE:
        raise ValueError('Location manifest exceeds 1 GiB')
    fd = os.open(filename, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
    digest, size, started = hashlib.sha256(), 0, time.monotonic()
    with os.fdopen(fd, 'rb') as stream:
        info = os.fstat(stream.fileno())
        if not stat.S_ISREG(info.st_mode) or identity(info) != tuple(source_identity):
            raise ValueError('Location manifest changed before fingerprinting')
        while chunk := stream.read(64 * 1024):
            size += len(chunk)
            if size > source_identity[2] or time.monotonic() - started > 110:
                raise ValueError('Location manifest fingerprint exceeds size/time budget')
            digest.update(chunk)
        if (size != source_identity[2] or identity(os.fstat(stream.fileno())) != tuple(source_identity)
                or identity(Path(filename).stat()) != tuple(source_identity)):
            raise ValueError('Location manifest changed while fingerprinting')
    return digest.hexdigest()


def recent_digest(filename, info):
    # A same-size rewrite and restored mtime may share the preceding ctime tick.
    # Old unchanged files retain the cheap metadata cache; fresh files must
    # distinguish bytes before looking up either an in-memory or disk index.
    return source_digest(filename, identity(info)) if time.time_ns() - info.st_ctime_ns < 2_000_000_000 else None


@contextmanager
def _lock(directory, exclusive):
    directory.mkdir(parents=True,exist_ok=True,mode=0o700)
    with (directory/'index.lock').open('a') as handle:
        deadline=time.monotonic()+120
        while True:
            try:
                fcntl.flock(handle, (fcntl.LOCK_EX if exclusive else fcntl.LOCK_SH)|fcntl.LOCK_NB)
                break
            except BlockingIOError:
                if time.monotonic()>=deadline: raise TimeoutError('Location index lock timed out')
                time.sleep(.02)
        try: yield
        finally: fcntl.flock(handle,fcntl.LOCK_UN)


class IndexedLocations:
    def __init__(self, filename, source_identity, field='files', *, content_digest=None):
        if field not in {'files', 'tables'}: raise ValueError('Unknown location mapping field')
        if source_identity[2]>MAX_SOURCE: raise ValueError('Location manifest exceeds 1 GiB')
        self.filename=filename; self.source_identity=tuple(source_identity); self.field=field
        if content_digest is None and time.time_ns() - self.source_identity[4] < 2_000_000_000:
            content_digest = source_digest(filename, self.source_identity)
        self.content_digest = content_digest
        self._content_verified_after_tick = False
        key = [FORMAT,filename,self.source_identity,field]
        if content_digest is not None:
            key.append(content_digest)
        self.key=json.dumps(key,separators=(',',':'))
        directory=Path(os.environ.get('DEMIFLOW_LOCATION_CACHE',Path.home()/'.cache/demiflow/location-index')).expanduser().absolute()
        self.path=directory/(hashlib.sha256(self.key.encode()).hexdigest()+'.sqlite')

    def _ready(self):
        if not self.path.is_file(): return False
        try:
            with closing(sqlite3.connect(self.path.as_uri()+'?mode=ro&immutable=1',uri=True)) as db:
                row=db.execute('SELECT identity FROM source').fetchone()
            return row==(self.key,)
        except sqlite3.DatabaseError:
            return False  # A damaged derivative is rebuilt under the write lock.

    def _ensure(self):
        directory=self.path.parent
        with _lock(directory,False):
            if self._ready(): return
        with _lock(directory,True):
            if self._ready(): return
            # Only this derivative cache is pruned, under the global build lock.
            files=[]; partials=[]
            for index,path in enumerate(directory.iterdir()):
                if index>=128: raise ValueError('Location cache file count exceeded')
                if re.fullmatch(r'[0-9a-f]{64}\.sqlite',path.name): files.append(path)
                elif re.fullmatch(r'[0-9a-f]{64}\.sqlite\.[0-9a-f]{32}\.partial',path.name): partials.append(path)
            for path in partials: path.unlink()
            sizes={path:path.stat() for path in files}
            used=sum(info.st_size for info in sizes.values())
            for path in sorted(files,key=lambda p:sizes[p].st_mtime_ns):
                if used+MAX_INDEX<=MAX_CACHE and len(sizes)<16: break
                path.unlink(); used-=sizes.pop(path).st_size
            free=os.statvfs(directory)
            if free.f_bavail*free.f_frsize<MAX_INDEX: raise ValueError('Location index requires 2 GiB free disk')
            temporary=self.path.with_name(self.path.name+'.'+uuid.uuid4().hex+'.partial')
            try:
                fd = os.open(self.filename, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
                with os.fdopen(fd, 'r', encoding='utf-8') as stream:
                    if identity(os.fstat(stream.fileno()))!=self.source_identity:
                        raise ValueError('Location manifest changed before indexing')
                    db=sqlite3.connect(temporary)
                    try:
                        db.execute('PRAGMA journal_mode=OFF');db.execute('PRAGMA synchronous=OFF')
                        db.execute('PRAGMA cache_size=-8192');db.execute('PRAGMA mmap_size=0')
                        page=db.execute('PRAGMA page_size').fetchone()[0]
                        db.execute('PRAGMA max_page_count='+str(MAX_INDEX//page))
                        db.execute('CREATE TABLE locations (key TEXT PRIMARY KEY,value TEXT NOT NULL) WITHOUT ROWID')
                        db.execute('CREATE TABLE source (identity TEXT NOT NULL)')
                        batch=[]; count=0; started=time.monotonic()
                        def flush():
                            db.executemany('INSERT OR REPLACE INTO locations VALUES (?,?)',batch);batch.clear()
                        for item in _records(stream,self.field):
                            count+=1
                            if count>MAX_RECORDS or time.monotonic()-started>110:
                                raise ValueError('Location index row/time budget exceeded')
                            if item is None:
                                flush();db.execute('DELETE FROM locations')
                            else: batch.append(item)
                            if len(batch)>=128: flush()
                        flush()
                        if (identity(os.fstat(stream.fileno()))!=self.source_identity
                                or identity(Path(self.filename).stat())!=self.source_identity):
                            raise ValueError('Location manifest changed while indexing')
                        if (self.content_digest is not None
                                and source_digest(self.filename, self.source_identity) != self.content_digest):
                            raise ValueError('Location manifest content changed while indexing')
                        db.execute('INSERT INTO source VALUES (?)',(self.key,));db.commit()
                    finally: db.close()
                # Publish only a durable complete derivative; a crash must not
                # leave a valid-looking metadata row with missing mapping pages.
                with temporary.open('rb') as completed: os.fsync(completed.fileno())
                os.replace(temporary,self.path)
                directory_fd=os.open(directory,os.O_DIRECTORY)
                try: os.fsync(directory_fd)
                finally: os.close(directory_fd)
            finally: temporary.unlink(missing_ok=True)

    def get(self,key):
        from .plan_capture import current_capture
        if current_capture() is not None:
            return self.get_readonly(key)
        if type(key) is not str or len(key)>MAX_TOKEN: raise ValueError('Location lookup key exceeds token budget')
        self._check_source()
        try:
            value = self._get(key)
        except OSError as exc:
            if exc.errno not in (errno.EACCES, errno.EPERM, errno.EROFS):
                raise
            return self.get_readonly(key)
        self._check_source()
        if self.field == 'tables' and value is not None and not isinstance(value, str):
            raise ValueError('Invalid relocated table target')
        return value

    def get_readonly(self, key):
        """Resolve the authoritative manifest without creating a derivative.

        Construction-only plan inspection has no write permission, even to a
        disposable index/lock. Reuse the same streaming parser and duplicate-key
        semantics as indexing, retaining at most the requested bounded value.
        Always consume/validate the entire manifest: an early match is not proof
        that a later field does not replace it or make the document invalid.
        """
        if type(key) is not str or len(key)>MAX_TOKEN:
            raise ValueError('Location lookup key exceeds token budget')
        self._check_source()
        fd = os.open(self.filename, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
        value, count, started = None, 0, time.monotonic()
        with os.fdopen(fd, 'r', encoding='utf-8') as stream:
            if identity(os.fstat(stream.fileno())) != self.source_identity:
                raise ValueError('Location manifest changed before read-only lookup')
            for item in _records(stream, self.field):
                count += 1
                if count > MAX_RECORDS or time.monotonic() - started > 110:
                    raise ValueError('Location lookup row/time budget exceeded')
                if item is None:
                    value = None
                elif item[0] == key:
                    value = json.loads(item[1])
            if identity(os.fstat(stream.fileno())) != self.source_identity:
                raise ValueError('Location manifest changed during read-only lookup')
        self._check_source()
        if self.field == 'tables' and value is not None and not isinstance(value, str):
            raise ValueError('Invalid relocated table target')
        return value

    def _check_source(self):
        path = Path(self.filename)
        if any(p.is_symlink() for p in (path, *path.parents)):
            raise ValueError('Symlink in location manifest')
        info = path.stat()
        if not stat.S_ISREG(info.st_mode) or identity(info) != self.source_identity:
            raise ValueError('Location manifest changed before indexing or lookup')
        if self.content_digest is not None and not self._content_verified_after_tick:
            if source_digest(self.filename, self.source_identity) != self.content_digest:
                raise ValueError('Location manifest content changed before indexing or lookup')
            # After a full proof beyond the coarse timestamp window, any future
            # rewrite changes ctime and the metadata check above rejects it.
            self._content_verified_after_tick = time.time_ns() - info.st_ctime_ns >= 2_000_000_000

    @lru_cache(maxsize=1024)
    def _get(self,key):
        for _ in range(3):
            self._ensure()
            with _lock(self.path.parent,False):
                if not self.path.is_file(): continue  # Evicted before our read lock.
                with closing(sqlite3.connect(self.path.as_uri()+'?mode=ro&immutable=1',uri=True)) as db:
                    row=db.execute('SELECT value FROM locations WHERE key=?',(key,)).fetchone()
                return None if row is None else json.loads(row[0])
        raise RuntimeError('Location index repeatedly evicted during lookup')
