"""Single-writer streaming snapshots with verified microbatch delivery.

Overwrite starts a new snapshot; append keeps prior keys and verifies repeated
content. A StreamCheckpoint can coordinate all sinks before downstream delivery.
Used by batch_map (concurrency=1), without materializing the whole stream.
"""
import asyncio
import hashlib
import json
from pathlib import Path
import lance
import pyarrow as pa
from demiflow.lance.model import LanceWriteSpec
from demiflow.lance.write import write_lance
from .lance_predicate import scalar_equal


def _canonical(row):
    return json.dumps(row, ensure_ascii=False, sort_keys=True, separators=(',',':'))


class StreamLanceWriter:
    def __init__(self, uri, schema, *, key, transient_columns=(), output_ref=None,
                 mode='overwrite', when=None, max_rows=1000000,
                 max_row_bytes=16*1024**2, max_key_bytes=4096):
        self.uri, self.schema, self.key = str(uri), schema, tuple(key) if isinstance(key, (list, tuple)) else key
        self.transient_columns = tuple(transient_columns)
        self.version = None
        self.seen = set()
        self.rows = 0
        if output_ref is not None and (not isinstance(output_ref,str) or not 1<=len(output_ref)<=128
                                       or output_ref in schema.names):
            raise ValueError('output_ref must be a distinct 1..128 character field outside the saved schema')
        self.output_ref=output_ref
        if mode not in {'overwrite', 'append'} or when is not None and not callable(when):
            raise ValueError('Invalid stream save mode/when')
        for name, value, ceiling in [('max_rows', max_rows, 10000000),
                ('max_row_bytes', max_row_bytes, 64*1024**2), ('max_key_bytes', max_key_bytes, 65536)]:
            if type(value) is not int or not 1 <= value <= ceiling:
                raise ValueError('Invalid stream save bound: ' + name)
        self.mode, self.when = mode, when
        self.checkpoint = None
        self.max_rows, self.max_row_bytes, self.max_key_bytes = max_rows, max_row_bytes, max_key_bytes

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
        self.seen.clear(); self.rows=0
        if self.mode == 'append' and Path(self.uri).exists():
            previous = lance.dataset(self.uri)
            if previous.schema != self.schema:
                raise ValueError('stream append schema differs from saved table')
            if previous.count_rows() > self.max_rows:
                raise ValueError('stream snapshot exceeds max_rows')
            fields = self.key if isinstance(self.key, tuple) else (self.key,)
            for batch in previous.scanner(columns=list(fields), batch_size=128,
                    batch_readahead=1, fragment_readahead=1).to_batches():
                if batch.nbytes > 128*self.max_key_bytes:
                    raise ValueError('saved keys exceed max_key_bytes')
                for row in batch.to_pylist():
                    digest = self.key_digest(tuple(row[k] for k in fields))
                    if digest in self.seen:
                        raise ValueError('saved stream snapshot keys are not unique')
                    self.seen.add(digest)
                    self.rows += 1
            self.version = previous.version
            return
        receipt = write_lance(LanceWriteSpec(uri=self.uri, mode='overwrite', schema=self.schema),
                              [pa.Table.from_pylist([], schema=self.schema)])
        if receipt.status != 'committed': raise RuntimeError('snapshot initialization is indeterminate')
        self.version = lance.dataset(self.uri).version

    def _write(self, rows):
        if self.checkpoint is not None:
            with self.checkpoint.lock:
                try:
                    if self.checkpoint.failed:
                        raise ValueError('Checkpoint writer failed; recovery is required')
                    return self._write_locked(rows)
                except BaseException:
                    self.checkpoint.failed = True
                    raise
        return self._write_locked(rows)

    def _write_locked(self, rows):
        if self.output_ref is not None and any(self.output_ref in row for row in rows):
            raise ValueError('output_ref would overwrite an input field')
        selected = [r for r in rows if self.when is None or self.when(r)]
        if not selected:
            return rows
        from .stream_grouping import row_size
        projected = [{k:r.get(k) for k in self.schema.names} for r in selected]
        for row in projected:
            row_size(row, self.max_row_bytes)
        fields = self.key if isinstance(self.key, tuple) else (self.key,)
        keys = [tuple(r[k] for k in fields) for r in projected]
        digests = [self.key_digest(k) for k in keys]
        if len(set(digests)) != len(digests) or self.mode == 'overwrite' and self.seen.intersection(digests):
            raise ValueError('stream snapshot keys must be nonempty and unique')
        if self.mode == 'append':
            old = [(key, row) for key, d, row in zip(keys, digests, projected) if d in self.seen]
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
            fresh = [(k, d, r) for k, d, r in zip(keys, digests, projected) if d not in self.seen]
            keys = [k for k, d, r in fresh]
            digests = [d for k, d, r in fresh]
            projected = [r for k, d, r in fresh]
        if self.rows + len(projected) > self.max_rows:
            raise ValueError('stream snapshot exceeds max_rows')
        if not projected:
            return self.forward(rows, selected)
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
            predicate = ' OR '.join('(' + ' AND '.join(scalar_equal(name, value)
                for name, value in zip(fields, key)) + ')' for key in keys)
            actual = current.to_table(filter=predicate)
            expected = {tuple(r[k] for k in fields):_canonical(r) for r in table.to_pylist()}
            found = {tuple(r[k] for k in fields):_canonical(r) for r in actual.to_pylist()}
            if len(actual) != len(table) or found != expected: raise
        self.version = base+1
        if self.checkpoint is not None:
            self.checkpoint.committed(self.stage, self.reference())
        self.seen.update(digests); self.rows += len(projected)
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
        if self._lock is not None:
            self._lock.__exit__(None, None, None)
            self._lock = None
