"""Configurable successful-document index, separate from execution journals.

Dataset.fetch_documents and Dataset.register_documents own the public entry
points. This resource contains no source credibility or consumer field policy.
"""
from __future__ import annotations

import asyncio
from contextlib import asynccontextmanager
from dataclasses import dataclass
from datetime import datetime, timezone
import fcntl
import hashlib
import json
import math
from pathlib import Path
import sqlite3
import time
from urllib.parse import urlsplit, urlunsplit

from demiflow.collect.documents import DocumentError, PARSER_VERSION, canonical, read_document
from demiflow.collect.document_formats import WIKI_PARSER_VERSION, normalize_material
from demiflow.objects import LocalObjectStore, ObjectRef, open_object


def source_url(value):
    """Remove fragments; preserve case-sensitive paths, query and scheme."""
    if not isinstance(value, str) or any(ord(c) < 32 or ord(c) == 127 for c in value):
        raise DocumentError('invalid_document_url')
    try:
        p = urlsplit(value)
        if p.scheme not in {'http', 'https'} or not p.hostname or p.username or p.password:
            raise ValueError()
        p.port
        return urlunsplit((p.scheme.lower(), p.netloc.lower(), p.path or '/', p.query, ''))
    except ValueError as exc:
        raise DocumentError('invalid_document_url') from exc


def timestamp(value):
    try:
        parsed = datetime.fromisoformat(value.replace('Z', '+00:00'))
        if parsed.tzinfo is None:
            raise ValueError()
        return parsed.timestamp()
    except (AttributeError, TypeError, ValueError, OverflowError) as exc:
        raise DocumentError('retrieved_at_requires_timezone_aware_iso_timestamp') from exc


def verified_bytes(ref, limit):
    reference = ObjectRef(**ref)
    with open_object(reference.uri) as stream:
        body = stream.read(limit + 1)
    if len(body) > limit:
        raise DocumentError('raw_snapshot_too_large')
    if hashlib.sha256(body).hexdigest() != reference.sha256:
        raise DocumentError('raw_snapshot_integrity_failure')
    return body


class DocumentLibraryBusy(TimeoutError):
    """Known local admission failure: no new source request was sent."""


@dataclass(frozen=True)
class DocumentLibrary:
    """Lazy declaration; paths are explicit and construction performs no I/O.

    reuse: accept a verified matching snapshot, optionally within max_age_s.
    refresh: bypass shared lookups; the run journal still freezes completed work.
    A shared POSIX filesystem must support SQLite and flock. Distributed object
    stores/locks and fuzzy URL equivalence are deliberately not implied.
    """
    index_path: str
    object_directory: str
    policy: str = 'reuse'
    max_age_s: float | None = None
    lock_timeout_s: float = 120.
    parser_versions: tuple[str, ...] = (PARSER_VERSION, WIKI_PARSER_VERSION)
    max_alias_hops: int = 8

    def __post_init__(self):
        for key in ('index_path', 'object_directory'):
            value = getattr(self, key)
            if not isinstance(value, (str, Path)) or not str(value).strip() or '://' in str(value):
                raise ValueError(key + ' must be a nonempty local filesystem path')
            object.__setattr__(self, key, str(Path(value).expanduser().resolve()))
        if self.index_path == self.object_directory:
            raise ValueError('Document index and object directory must differ')
        if self.policy not in {'reuse', 'refresh'}:
            raise ValueError('Document library policy must be reuse or refresh')
        if type(self.max_alias_hops) is not int or self.max_alias_hops < 1:
            raise ValueError('max_alias_hops must be a positive integer')
        for key in ('max_age_s', 'lock_timeout_s'):
            value = getattr(self, key)
            if key == 'max_age_s' and value is None:
                continue
            if type(value) not in (int, float) or not math.isfinite(value) or value <= 0:
                raise ValueError(key + ' must be positive and finite')
        if not isinstance(self.parser_versions, (tuple, list)) or not self.parser_versions or not all(
                isinstance(v, str) and v for v in self.parser_versions):
            raise ValueError('parser_versions must list accepted parser versions')
        object.__setattr__(self, 'parser_versions', tuple(sorted(set(self.parser_versions))))

    @property
    def identity(self):
        return [self.index_path, self.object_directory, self.policy, self.max_age_s, self.parser_versions, self.max_alias_hops]

    def _db(self):
        from demiflow.execution.artifacts import resolve_local_artifact
        index = resolve_local_artifact(self.index_path)
        index.parent.mkdir(parents=True, exist_ok=True)
        db = sqlite3.connect(str(index), timeout=self.lock_timeout_s)
        try:
            db.execute('PRAGMA synchronous=FULL')
            # A shared-library lookup must not become a writer. Repeating even
            # INSERT OR IGNORE for existing metadata competes with bulk imports
            # and can starve live consumers despite doing no useful write.
            tables = {r[0] for r in db.execute("SELECT name FROM sqlite_master WHERE type='table'")}
            meta = (dict(db.execute('SELECT key,value FROM library_meta'))
                    if 'library_meta' in tables else {})
            if (not {'library_meta', 'documents', 'urls', 'redirects'} <= tables
                    or not {'schema_version', 'object_directory'} <= meta.keys()):
                with db:
                    db.execute('BEGIN IMMEDIATE')
                    db.execute('CREATE TABLE IF NOT EXISTS library_meta (key TEXT PRIMARY KEY, value TEXT NOT NULL)')
                    db.execute('INSERT OR IGNORE INTO library_meta VALUES (?, ?)', ('schema_version', '1'))
                    db.execute('INSERT OR IGNORE INTO library_meta VALUES (?, ?)', ('object_directory', self.object_directory))
                    db.execute('CREATE TABLE IF NOT EXISTS documents (id TEXT PRIMARY KEY, observed REAL NOT NULL, parser TEXT NOT NULL, receipt TEXT NOT NULL)')
                    db.execute('CREATE TABLE IF NOT EXISTS urls (url TEXT NOT NULL, document_id TEXT NOT NULL, PRIMARY KEY(url, document_id))')
                    db.execute('CREATE TABLE IF NOT EXISTS redirects (url TEXT PRIMARY KEY, target_url TEXT NOT NULL, receipt TEXT NOT NULL)')
                meta = dict(db.execute('SELECT key,value FROM library_meta'))
            version = meta['schema_version']
            if version != '1':
                raise ValueError('Unsupported document library schema version')
            actual = meta['object_directory']
            if resolve_local_artifact(actual) != resolve_local_artifact(self.object_directory):
                raise ValueError('Shared index is bound to a different object_directory')
            return db
        except BaseException:
            db.close()
            raise

    @asynccontextmanager
    async def claim(self, url):
        """Coalesce misses across processes without holding a SQLite transaction."""
        key = hashlib.sha256(source_url(url).encode()).hexdigest()
        from demiflow.execution.artifacts import resolve_local_artifact
        directory = Path(str(resolve_local_artifact(self.index_path)) + '.locks')
        directory.mkdir(parents=True, exist_ok=True)
        # Bound directory growth to 4096 lock inodes, even for millions of URLs.
        # Hash collisions serialize unrelated fetches but cannot mix their data.
        handle = (directory / key[:3]).open('a+b')
        deadline = time.monotonic() + self.lock_timeout_s
        try:
            while True:
                try:
                    fcntl.flock(handle, fcntl.LOCK_EX | fcntl.LOCK_NB)
                    break
                except BlockingIOError:
                    if time.monotonic() >= deadline:
                        raise DocumentLibraryBusy('shared_document_lock_timeout')
                    await asyncio.sleep(min(.05, max(0., deadline-time.monotonic())))
            yield
        finally:
            # Do not unlink lock files: another process may already hold the inode.
            handle.close()

    def lookup(self, url, *, max_bytes, max_document_bytes):
        if self.policy == 'refresh':
            return None
        db = self._db()
        try:
            marks = ','.join('?' for _ in self.parser_versions)
            current=source_url(url); visited=set(); row=None
            for _ in range(self.max_alias_hops+1):
                if current in visited:break
                visited.add(current)
                row = db.execute(
                    'SELECT d.receipt FROM urls u JOIN documents d ON d.id=u.document_id '
                    f'WHERE u.url=? AND d.parser IN ({marks}) AND d.observed>=? '
                    'ORDER BY d.observed DESC, d.id DESC LIMIT 1',
                    (current, *self.parser_versions,
                     time.time()-self.max_age_s if self.max_age_s is not None else -1e100)).fetchone()
                if row is not None:break
                redirect=db.execute('SELECT target_url FROM redirects WHERE url=?',(current,)).fetchone()
                if redirect is None:break
                current=redirect[0]
        finally:
            db.close()
        if row is None:
            return None
        receipt = json.loads(row[0])
        doc = read_document(receipt['document_ref'], max_bytes=max_document_bytes)
        if (doc['raw_ref'] != receipt['raw_ref'] or doc['parser']['version'] not in self.parser_versions
                or any(doc['source'][key] != receipt[key]
                       for key in ('url','final_url','content_type','retrieved_at'))):
            raise DocumentError('shared_document_index_mismatch')
        verified_bytes(receipt['raw_ref'], max_bytes)
        return {**receipt, 'url':url, 'attempts':[],
                'acquisition':{**receipt['acquisition'], 'kind':'shared_library'}}

    def register(self, request, *, max_bytes, max_document_bytes, origin='import'):
        return self.publish([self.prepare(request,max_bytes=max_bytes,
                            max_document_bytes=max_document_bytes,origin=origin)])[0]

    def prepare(self, request, *, max_bytes, max_document_bytes, origin='import'):
        """Verify and copy a normalized document and its raw snapshot, then publish.

        request: document_ref, optional exact aliases, source_id and revision.
        The original source timestamp is retained; import time never means source
        freshness. Old imports do not supersede snapshots with a later timestamp.
        """
        if origin not in {'download', 'import'}:
            raise ValueError('Invalid document origin')
        if isinstance(request,dict) and request.get('format')=='redirect':
            return self._prepare_redirect(request)
        try:
            if not isinstance(request, dict):
                raise TypeError('request must be a mapping')
            if 'document_ref' in request:
                doc = read_document(request['document_ref'], max_bytes=max_document_bytes)
                body = verified_bytes(doc['raw_ref'], max_bytes)
            else:
                doc,body=normalize_material(request,max_bytes=max_bytes)
        except (KeyError, TypeError, ValueError) as exc:
            # Invalid input is a row receipt. Storage/IO errors remain fatal;
            # in particular, never treat a disk failure during publication as
            # an unverified or semantically rejected source.
            raise DocumentError('invalid_import_document: ' + str(exc)) from exc
        if doc['parser']['version'] not in self.parser_versions:
            raise DocumentError('unaccepted_document_parser')
        source = doc['source']
        # Unknown historical acquisition time remains blank in the receipt.
        # Such entries are eligible only when no max_age_s was requested, and
        # never outrank a dated snapshot merely because they were imported now.
        observed = timestamp(source['retrieved_at']) if source['retrieved_at'] else -1e100
        aliases = request.get('aliases', [])
        if not isinstance(aliases, list):
            raise DocumentError('aliases_must_be_url_list')
        urls = sorted({source_url(value) for value in [source['url'], source['final_url'], *aliases]})
        source_id, revision = request.get('source_id', ''), request.get('revision', '')
        if not all(isinstance(value, str) for value in (source_id, revision)):
            raise DocumentError('source_id_and_revision_must_be_strings')
        store = LocalObjectStore(self.object_directory)
        doc['raw_ref'] = store.put(body).to_dict()
        payload = canonical(doc).encode('utf-8')
        if len(payload) > max_document_bytes:
            raise DocumentError('normalized_document_too_large')
        ref = store.put(payload).to_dict()
        receipt = {key:source[key] for key in ('url','final_url','content_type','retrieved_at')}
        receipt.update(document_ref=ref, raw_ref=doc['raw_ref'], status='ok', reason='', attempts=[],
            acquisition={'kind':origin, 'origin':origin, 'source_id':source_id, 'revision':revision,
                         'registered_at':datetime.now(timezone.utc).isoformat()})
        identity = hashlib.sha256(canonical([ref['sha256'], origin, source_id, revision]).encode()).hexdigest()
        return {'kind':'document','identity':identity,'observed':observed,
                'parser':doc['parser']['version'],'receipt':receipt,'urls':urls}

    def _prepare_redirect(self, request):
        url=source_url(request.get('url')); target=source_url(request.get('target_url'))
        if not all(isinstance(request.get(k,''),str) for k in ('source_id','revision')):
            raise DocumentError('source_id_and_revision_must_be_strings')
        aliases=request.get('aliases',[])
        if not isinstance(aliases,list):raise DocumentError('aliases_must_be_url_list')
        urls=sorted({url,*(source_url(v) for v in aliases)})
        if target in urls:raise DocumentError('self_referencing_redirect')
        receipt={'url':url,'final_url':target,'status':'redirect_registered','reason':'','attempts':[],
            'acquisition':{'kind':'import','origin':'import','source_id':request.get('source_id',''),
                           'revision':request.get('revision',''),
                           'registered_at':datetime.now(timezone.utc).isoformat()}}
        return {'kind':'redirect','target':target,'urls':urls,'receipt':receipt}

    def publish(self, prepared, *, _connection=None):
        """Publish a bounded batch in one transaction, after object preparation.

        No source parsing or object IO occurs while this write transaction is
        open. Failed rows remain receipts and never become indexed documents.
        """
        # The batch actor may retain a connection on its sole publisher thread;
        # ordinary callers continue to own and close a connection per call.
        from .document_index import publish_prepared
        db=self._db() if _connection is None else _connection
        try:
            with db:
                # Wait before reading a redirect conflict or changing a row,
                # avoiding an un-waitable deferred read-to-write lock upgrade.
                db.execute('BEGIN IMMEDIATE')
                output=publish_prepared(db,prepared,self._publish_one)
        finally:
            if _connection is None:db.close()
        return output

    @staticmethod
    def _publish_one(db,item):
        """Ordinary path without a batch lookup cache for unusually wide metadata."""
        receipt=item['receipt']
        if item['kind']=='failure':return receipt
        if item['kind']=='document':
            identity=item['identity']
            db.execute('INSERT OR IGNORE INTO documents VALUES (?, ?, ?, ?)',
                (identity,item['observed'],item['parser'],canonical(receipt)))
            db.executemany('INSERT OR IGNORE INTO urls VALUES (?, ?)',((url,identity) for url in item['urls']))
            saved=db.execute('SELECT receipt FROM documents WHERE id=?',(identity,)).fetchone()
        else:
            conflicting=any((old:=db.execute('SELECT target_url FROM redirects WHERE url=?',(url,)).fetchone())
                and old[0]!=item['target'] for url in item['urls'])
            if conflicting:
                return {'url':receipt['url'],'status':'invalid_document',
                    'reason':'conflicting_redirect_target','attempts':[]}
            encoded=canonical(receipt)
            db.executemany('INSERT OR IGNORE INTO redirects VALUES (?, ?, ?)',
                ((url,item['target'],encoded) for url in item['urls']))
            saved=db.execute('SELECT receipt FROM redirects WHERE url=?',(receipt['url'],)).fetchone()
        return json.loads(saved[0])


class RegisterDocuments:
    """Native Dataset actor; one existing document per input row."""
    def __init__(self, request, output, library, when, max_bytes, max_document_bytes):
        self.request, self.output, self.library, self.when = request, output, library, when
        self.max_bytes, self.max_document_bytes = max_bytes, max_document_bytes

    async def __call__(self, row):
        if self.when is not None and not self.when(row):
            return row
        # Publication is one bounded synchronous operation. Drain it on cancel,
        # so no detached worker can publish after the Dataset action has closed.
        task = asyncio.create_task(asyncio.to_thread(self.library.register, row[self.request],
            max_bytes=self.max_bytes, max_document_bytes=self.max_document_bytes))
        try:
            result = await asyncio.shield(task)
        except asyncio.CancelledError:
            await task
            raise
        except DocumentError as exc:
            result = {'status':'invalid_document', 'reason':str(exc), 'attempts':[]}
        return {**row, self.output:result}


def _prepare_import(library,request,max_bytes,max_document_bytes):
    try:return library.prepare(request,max_bytes=max_bytes,max_document_bytes=max_document_bytes)
    except DocumentError as exc:
        return {'kind':'failure','receipt':{'status':'invalid_document','reason':str(exc),'attempts':[]}}


class BatchRegisterDocuments(RegisterDocuments):
    """Bounded source preparation in workers, then a short atomic index commit."""
    def __init__(self,*args,prepare_workers=4,index_cache_mb=64,publish_pause_s=0.,**kwargs):
        super().__init__(*args,**kwargs)
        self.prepare_workers=prepare_workers;self.pool=None;self.publish_lock=None
        self.publisher_pool=None;self.index_connection=None
        self.index_cache_mb=index_cache_mb
        self.publish_pause_s=publish_pause_s

    def _publish(self,prepared):
        # Only the dedicated publisher thread touches this connection. Reusing
        # its bounded page cache avoids rereading index pages for every batch;
        # every publish still commits its own FULL-synchronous transaction.
        if self.index_connection is None:
            self.index_connection=self.library._db()
            self.index_connection.execute(f'PRAGMA cache_size=-{self.index_cache_mb*1024}')
        return self.library.publish(prepared,_connection=self.index_connection)

    def _close_index(self):
        if self.index_connection is not None:
            self.index_connection.close();self.index_connection=None

    async def aclose(self):
        if self.pool is not None:
            await asyncio.to_thread(self.pool.shutdown,wait=True,cancel_futures=True)
            self.pool=None
        if self.publisher_pool is not None:
            await asyncio.get_running_loop().run_in_executor(self.publisher_pool,self._close_index)
            await asyncio.to_thread(self.publisher_pool.shutdown,wait=True,cancel_futures=True)
            self.publisher_pool=None
        self.publish_lock=None

    async def __call__(self,rows):
        selected=[(i,row) for i,row in enumerate(rows) if self.when is None or self.when(row)]
        if not selected:return rows
        if self.pool is None and any(not isinstance(row[self.request],dict) or row[self.request].get('format')!='redirect' for _,row in selected):
            from concurrent.futures import ProcessPoolExecutor
            import multiprocessing
            self.pool=ProcessPoolExecutor(max_workers=self.prepare_workers,
                                          mp_context=multiprocessing.get_context('spawn'))
        loop=asyncio.get_running_loop()
        async def prepare(row):
            request=row[self.request]
            if isinstance(request,dict) and request.get('format')=='redirect':
                return _prepare_import(self.library,request,self.max_bytes,self.max_document_bytes)
            return await loop.run_in_executor(self.pool,_prepare_import,self.library,request,
                                             self.max_bytes,self.max_document_bytes)
        futures=[prepare(row) for _,row in selected]
        pending=asyncio.gather(*futures)
        try:prepared=await asyncio.shield(pending)
        except asyncio.CancelledError:
            await pending
            raise
        # Preparation stays concurrent; one actor admits one SQLite publisher
        # at a time. Competing worker threads only lengthen lock holds/waits on
        # the single-writer index and can starve unrelated shared consumers.
        if self.publish_lock is None:self.publish_lock=asyncio.Lock()
        async with self.publish_lock:
            if self.publisher_pool is None:
                from concurrent.futures import ThreadPoolExecutor
                self.publisher_pool=ThreadPoolExecutor(max_workers=1,thread_name_prefix='document-index')
            task=loop.run_in_executor(self.publisher_pool,self._publish,prepared)
            try:receipts=await asyncio.shield(task)
            except asyncio.CancelledError:
                await task
                raise
            # The transaction has committed. Keep our actor's next publisher
            # queued while other processes can acquire SQLite's writer lock.
            if self.publish_pause_s:await asyncio.sleep(self.publish_pause_s)
        output=list(rows)
        for (i,row),receipt in zip(selected,receipts):output[i]={**row,self.output:receipt}
        return output
