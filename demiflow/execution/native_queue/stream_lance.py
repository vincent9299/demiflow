"""Single-writer streaming snapshots with verified microbatch delivery.

Overwrite starts a new snapshot; append keeps prior keys and verifies repeated
content. A StreamCheckpoint can coordinate all sinks before downstream delivery.
Used by batch_map (concurrency=1), without materializing the whole stream.
"""
import asyncio
import hashlib
import json
import sqlite3
import tempfile
from pathlib import Path
import lance
import pyarrow as pa
from demiflow.lance.model import LanceWriteSpec
from demiflow.lance.write import write_lance
from demiflow.lance.read import scalar_equal


def _canonical(row):
    return json.dumps(row, ensure_ascii=False, sort_keys=True, separators=(',',':'))


class StreamLanceWriter:
    def __init__(self, uri, schema, *, key, transient_columns=(), output_ref=None,
                 mode='overwrite', when=None, max_rows=1000000,
                 max_row_bytes=16*1024**2, max_key_bytes=4096, max_index_bytes=512*1024**2):
        self.uri, self.schema, self.key = str(uri), schema, tuple(key) if isinstance(key, (list, tuple)) else key
        self.transient_columns = tuple(transient_columns)
        self.version = None
        self._index = None
        self._index_directory = None
        self._index_failed = False
        self.rows = 0
        if output_ref is not None and (not isinstance(output_ref,str) or not 1<=len(output_ref)<=128
                                       or output_ref in schema.names):
            raise ValueError('output_ref must be a distinct 1..128 character field outside the saved schema')
        self.output_ref=output_ref
        if mode not in {'overwrite', 'append'} or when is not None and not callable(when):
            raise ValueError('Invalid stream save mode/when')
        for name, value, ceiling in [('max_rows', max_rows, 100000000),
                ('max_row_bytes', max_row_bytes, 64*1024**2), ('max_key_bytes', max_key_bytes, 65536),
                ('max_index_bytes', max_index_bytes, 16*1024**3)]:
            if type(value) is not int or not 1 <= value <= ceiling:
                raise ValueError('Invalid stream save bound: ' + name)
        self.mode, self.when = mode, when
        self.checkpoint = None
        self.max_rows, self.max_row_bytes, self.max_key_bytes = max_rows, max_row_bytes, max_key_bytes
        self.max_index_bytes = max_index_bytes
        self._call_validations = {}

    def _close_index(self):
        if self._index is not None:
            self._index.close()
            self._index = None
        if self._index_directory is not None:
            self._index_directory.cleanup()
            self._index_directory = None

    def _open_index(self):
        """Rebuildable membership index; fixed cache, bounded pages, no key set."""
        self._close_index()
        from demiflow.execution.pipeline.checked import current
        checked = current()
        directory = (checked.work_root / 'indexes' if checked is not None and checked.work_root is not None
                     else Path(self.uri).with_suffix('.stream-writer'))
        directory.mkdir(parents=True, exist_ok=True)
        self._index_directory = tempfile.TemporaryDirectory(prefix='keys-', dir=directory)
        try:
            self._index = sqlite3.connect(Path(self._index_directory.name) / 'keys.sqlite', check_same_thread=False)
            self._index.execute('PRAGMA journal_mode=OFF')
            self._index.execute('PRAGMA synchronous=OFF')
            self._index.execute('PRAGMA cache_size=-2048')
            self._index.execute('PRAGMA mmap_size=0')
            self._index.execute('PRAGMA temp_store=FILE')
            page = self._index.execute('PRAGMA page_size').fetchone()[0]
            if self.max_index_bytes < 2 * page:
                raise ValueError('Stream key index budget is too small')
            self._index.execute('PRAGMA max_page_count=' + str(self.max_index_bytes // page))
            self._index.execute('CREATE TABLE keys (digest BLOB PRIMARY KEY) WITHOUT ROWID')
            self._index_failed = False
        except BaseException:
            self._close_index()
            raise

    def _contains(self, digest):
        if self._index_failed or self._index is None:
            raise ValueError('Stream key index requires initialization after failure')
        return self._index.execute('SELECT 1 FROM keys WHERE digest=?', (digest,)).fetchone() is not None

    def _remember(self, digests):
        try:
            self._index.executemany('INSERT INTO keys VALUES(?)', ((d,) for d in digests))
            self._index.commit()
        except sqlite3.DatabaseError as exc:
            # This disposable index has no journal. Never reuse partial state;
            # a new execution rebuilds it from the fixed committed table.
            self._index_failed = True
            raise ValueError('Stream key index exceeds its disk budget or has duplicate keys') from exc

    def key_digest(self, key):
        if any(v is None or v == '' or not isinstance(v, (str, int, bool)) for v in key):
            raise ValueError('stream snapshot keys must be nonempty scalars')
        if any(isinstance(v, str) and len(v) > self.max_key_bytes for v in key):
            raise ValueError('stream snapshot key exceeds max_key_bytes')
        value = _canonical(key).encode()
        if len(value) > self.max_key_bytes:
            raise ValueError('stream snapshot key exceeds max_key_bytes')
        return hashlib.sha256(value).digest()

    def initialize(self):
        """Start an explicitly new delivery; caller owns the run/target lock."""
        Path(self.uri).parent.mkdir(parents=True, exist_ok=True)
        self._open_index(); self.rows=0
        if self.mode == 'append' and Path(self.uri).exists():
            previous = lance.dataset(self.uri)
            if not previous.schema.equals(self.schema, check_metadata=True):
                raise ValueError('stream append schema differs from saved table')
            if previous.count_rows() > self.max_rows:
                raise ValueError('stream snapshot exceeds max_rows')
            fields = self.key if isinstance(self.key, tuple) else (self.key,)
            for batch in previous.scanner(columns=list(fields), batch_size=128,
                    batch_readahead=1, fragment_readahead=1).to_batches():
                if batch.nbytes > 128*self.max_key_bytes:
                    raise ValueError('saved keys exceed max_key_bytes')
                digests = []
                for row in batch.to_pylist():
                    digest = self.key_digest(tuple(row[k] for k in fields))
                    if self._contains(digest) or digest in digests:
                        raise ValueError('saved stream snapshot keys are not unique')
                    digests.append(digest)
                    self.rows += 1
                self._remember(digests)
            self.version = previous.version
            return
        if self.checkpoint is not None and self.checkpoint.declared_outputs is not None:
            with self.checkpoint.lock:
                operation = self.checkpoint.begin_initialize(self.stage)
                receipt = write_lance(LanceWriteSpec(uri=self.uri, mode='create', schema=self.schema),
                                      [pa.Table.from_pylist([], schema=self.schema)], operation_id=operation)
                if receipt.status != 'committed':
                    raise RuntimeError('snapshot initialization is indeterminate')
                self.version = receipt.committed_version
                self.checkpoint.committed(self.stage, self.reference())
            return
        receipt = write_lance(LanceWriteSpec(uri=self.uri, mode='overwrite', schema=self.schema),
                              [pa.Table.from_pylist([], schema=self.schema)])
        if receipt.status != 'committed': raise RuntimeError('snapshot initialization is indeterminate')
        self.version = lance.dataset(self.uri).version

    def _write(self, rows):
        try:
            if self.checkpoint is not None:
                with self.checkpoint.lock:
                    if self.checkpoint.failed:
                        raise ValueError('Checkpoint writer failed; recovery is required')
                    return self._write_locked(rows)
            return self._write_locked(rows)
        except BaseException:
            self._index_failed = True
            if self.checkpoint is not None:
                self.checkpoint.failed = True
            raise

    def _write_locked(self, rows):
        if self.output_ref is not None and any(self.output_ref in row for row in rows):
            raise ValueError('output_ref would overwrite an input field')
        def selected_row(row):
            if self.when is None:
                return True
            validation = self._call_validations.get('when')
            if validation is not None:
                validation.inputs((row,), {})
            result = self.when(row)
            if validation is not None:
                validation.output(result)
            return result
        selected = [r for r in rows if selected_row(r)]
        if not selected:
            return rows
        from demiflow.operators.grouping import row_size
        projected = [{k:r.get(k) for k in self.schema.names} for r in selected]
        for row in projected:
            row_size(row, self.max_row_bytes)
        fields = self.key if isinstance(self.key, tuple) else (self.key,)
        keys = [tuple(r[k] for k in fields) for r in projected]
        digests = [self.key_digest(k) for k in keys]
        known = [self._contains(digest) for digest in digests]
        if len(set(digests)) != len(digests) or self.mode == 'overwrite' and any(known):
            raise ValueError('stream snapshot keys must be nonempty and unique')
        if self.mode == 'append':
            old = [(key, row) for key, seen, row in zip(keys, known, projected) if seen]
            if old:
                table = lance.dataset(self.uri, version=self.version)
                for key, row in old:
                    predicate = ' AND '.join(scalar_equal(name, value) for name, value in zip(fields, key))
                    found = []
                    for batch in table.scanner(filter=predicate, limit=2, batch_size=1,
                            batch_readahead=1, fragment_readahead=1).to_batches():
                        if batch.nbytes > self.max_row_bytes:
                            raise ValueError('saved row exceeds max_row_bytes')
                        found.extend(batch.to_pylist())
                    expected = pa.Table.from_pylist([row], schema=self.schema).to_pylist()
                    if _canonical(found) != _canonical(expected):
                        raise ValueError('stream append key has conflicting saved content')
            fresh = [(k, d, r) for k, d, r, seen in zip(keys, digests, projected, known) if not seen]
            keys = [k for k, d, r in fresh]
            digests = [d for k, d, r in fresh]
            projected = [r for k, d, r in fresh]
        if self.rows + len(projected) > self.max_rows:
            raise ValueError('stream snapshot exceeds max_rows')
        if not projected:
            return self.forward(rows, selected)
        # Reserve index capacity before the external commit. Any subsequent
        # failure invalidates this derived index; committed rows remain in Lance.
        self._remember(digests)
        table = pa.Table.from_pylist(projected, schema=self.schema)
        base = self.version
        if self.checkpoint is not None:
            self.checkpoint.begin(self.stage, self.uri, base, fields, table.to_pylist(), self.max_row_bytes)
        try:
            receipt = write_lance(LanceWriteSpec(uri=self.uri, mode='append', schema=self.schema,
                                               expected_version=base), [table])
            if receipt.status != 'committed': raise RuntimeError('indeterminate stream append')
        except Exception:
            # Resolve ambiguous commit before any resend. An unrelated version or
            # partial/different batch remains a hard failure, never blind append.
            current = lance.dataset(self.uri)
            if current.version != base+1: raise
            expected = {tuple(r[k] for k in fields):_canonical(r) for r in table.to_pylist()}
            found = {}
            for key in keys:
                predicate = ' AND '.join(scalar_equal(name, value) for name, value in zip(fields, key))
                count = 0
                for batch in current.scanner(filter=predicate, limit=2, batch_size=1,
                        batch_readahead=1, fragment_readahead=1).to_batches():
                    if batch.nbytes > self.max_row_bytes:
                        raise ValueError('Reconciled stream row exceeds max_row_bytes')
                    for row in batch.to_pylist():
                        count += 1
                        found[tuple(row[k] for k in fields)] = _canonical(row)
                if count != 1:
                    raise ValueError('Stream commit reconciliation requires one row per key')
            if found != expected:
                raise ValueError('Stream commit reconciliation content differs')
        self.version = base+1
        if self.checkpoint is not None:
            self.checkpoint.committed(self.stage, self.reference())
        self.rows += len(projected)
        return self.forward(rows, selected)

    def forward(self, rows, selected):
        selected_ids = {id(row) for row in selected}
        return [{**{k:v for k,v in r.items() if k not in self.transient_columns},
                 **({self.output_ref:self.reference()} if self.output_ref is not None and id(r) in selected_ids else {})} for r in rows]

    async def __call__(self, rows):
        if self.version is None: raise RuntimeError('initialize the writer before executing the stream')
        # Await the actual thread on cancellation; cancelling an await does not
        # cancel an in-flight storage commit. Downstream only sees verified rows.
        task = asyncio.create_task(asyncio.to_thread(self._write, rows))
        try:
            return await asyncio.shield(task)
        except asyncio.CancelledError:
            await task
            raise

    def reference(self):
        return {'uri':self.uri,'version':self.version}

    async def aclose(self):
        self._close_index()


class StreamLanceSink(StreamLanceWriter):
    """Lazy Dataset.save_lance lifecycle; owns its target lock through drain."""
    is_stream_sink = True
    def __init__(self, uri, schema, *, key, stage, output_ref=None, **options):
        fields = (key,) if isinstance(key, str) else tuple(key)
        if not fields or len(fields) > 32 or len(set(fields)) != len(fields) or any(k not in schema.names for k in fields):
            raise ValueError('save_lance key absent from schema or invalid')
        from urllib.parse import urlsplit, unquote
        parsed=urlsplit(str(uri))
        if parsed.scheme and parsed.scheme!='file': raise ValueError('save_lance currently requires a local table target')
        if parsed.scheme=='file' and parsed.netloc not in {'','localhost'}: raise ValueError('Nonlocal file authority is unsupported')
        target=unquote(parsed.path) if parsed.scheme=='file' else str(uri)
        super().__init__(Path(target).resolve(), schema, key=key,output_ref=output_ref, **options)
        self.stage = stage
        self._lock = None

    async def astart(self):
        from demiflow.execution.artifacts import run_lock
        self.version = None
        self._lock = run_lock(Path(self.uri).with_suffix('.stream-writer'))
        self._lock.__enter__()
        task = asyncio.create_task(asyncio.to_thread(self.initialize))
        try:
            await asyncio.shield(task)
            if self.checkpoint is not None:
                await asyncio.to_thread(self.checkpoint.register, self.stage, self.reference())
        except asyncio.CancelledError:
            await task
            raise

    async def aclose(self):
        await super().aclose()
        if self._lock is not None:
            self._lock.__exit__(None, None, None)
            self._lock = None
