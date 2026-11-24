"""Single-writer streaming snapshots with verified microbatch delivery.

Overwrite starts a new snapshot; append keeps prior keys and verifies repeated
content. A StreamCheckpoint can coordinate all sinks before downstream delivery.
Used by batch_map (concurrency=1), without materializing the whole stream.
"""

from dataclasses import asdict
import threading
from demiflow.execution.contracts.dataflow import ArtifactCommand, ArtifactRef, NativeJournalReply, NATIVE_STREAM_LANCE_SCHEMA as SCHEMA
from demiflow.execution.contracts.dataflow_errors import DataflowError
from demiflow.execution.contracts.recovery_contract import identity, document, SourcePosition, UnresolvedCommit
from demiflow.execution.outputs.lance_sink import LanceSink, _BatchSink
from demiflow.execution.checkpoint.commit import CommitCoordinator
from demiflow.execution.pipeline.run_context import RunAction
from demiflow.execution.journal.dataflow_stage_facts import stream_scope, stream_scopes
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
        from demiflow.execution.storage.artifacts import run_lock
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


class _NativeWriter(StreamLanceWriter):
    def __init__(self, declaration, *, action, store, bounds):
        import pyarrow as pa
        value = dict(declaration)
        self.stage = value.pop('stage'); self.batch_size = value.pop('max_batch')
        schema = pa.ipc.read_schema(pa.BufferReader(bytes.fromhex(value.pop('schema'))))
        super().__init__(schema=schema, **value)
        self.action, self.store, self.bounds = action, store, bounds
        self._scratch = None
        self._target_lock = None
        self.baseline = None
        self.last_ordinal = -1
        self.finished = False
        self.confirmed = False

    def _open_index(self):
        # The original store owns the physical inode/lease and the full SQLite
        # page grant before SQLite is allowed to write a byte. No WAL/journal.
        grant = min(self.max_index_bytes, self.bounds.max_spill_bytes // 4,
                    self.max_rows * 128 + 65536)
        if grant < 16384:
            raise DataflowError('RESOURCE_INFEASIBLE', 'Native key index needs at least 16 KiB scratch')
        self._scratch = self.store.scratch(max_bytes=grant, max_io_bytes=65536)
        scratch = self._scratch.__enter__()
        if grant > scratch.reserved:
            scratch.store._journal.artifact(ArtifactCommand(identity([scratch.operation,'index-grant',grant]),
                'grow_staging',None,scratch.operation,grant))
            scratch.reserved = grant
        self._index = sqlite3.connect(scratch.path, check_same_thread=False)
        for pragma in ('journal_mode=OFF','synchronous=OFF','cache_size=-2048','mmap_size=0','temp_store=MEMORY'):
            self._index.execute('PRAGMA '+pragma)
        page = self._index.execute('PRAGMA page_size').fetchone()[0]
        self._index.execute('PRAGMA max_page_count='+str(grant//page))
        self._index.execute('CREATE TABLE keys (digest BLOB PRIMARY KEY) WITHOUT ROWID')
        self._index_failed = False

    def _close_index(self):
        if self._index is not None:
            self._index.close();self._index=None
        if self._scratch is not None:
            self._scratch.__exit__(None,None,None);self._scratch=None

    def head(self):
        from demiflow.lance.storage import inspect_lance
        from demiflow.errors import LanceResourceNotFound
        try: return inspect_lance(self.uri,None,None).resolved_version
        except LanceResourceNotFound: return None

    def start(self, node, *, latest=None, policy=None):
        from demiflow.execution.storage.artifacts import run_lock
        self._target_lock = run_lock(Path(self.uri).with_suffix('.stream-writer'))
        self._target_lock.__enter__()
        run = self.action.context
        key = self.action.action_id+'-stream-target-'+node
        baseline = run.journal.source_binding(key)
        if baseline is None:
            baseline = {'uri':self.uri,'version':self.head()}
            if policy is not None and baseline['version'] != policy['expected_version']:
                raise DataflowError('INVALID_CONTRACT','Stream target differs from sealed create/overwrite CAS permission')
            run.journal.bind_source(run.fence,key,baseline)
        if policy is not None and baseline['version'] != policy['expected_version']:
            raise DataflowError('INCOMPATIBLE_HISTORY','Stream target original baseline differs from sealed permission')
        if baseline['uri'] != self.uri:
            raise DataflowError('INCOMPATIBLE_HISTORY','Stream target binding differs')
        self.baseline = baseline['version']
        self.permission_mode = policy['mode'] if policy is not None else None
        self.version = latest if latest is not None else self.baseline if self.mode == 'append' else None
        self._open_index()

    def rebuild(self):
        import lance
        self.rows = 0
        if self.version is None: return
        table = lance.dataset(self.uri,version=self.version)
        if not table.schema.equals(self.schema,check_metadata=True):
            raise ValueError('stream append schema differs from saved table')
        fields = tuple(self.key)
        for batch in table.scanner(columns=list(fields),batch_size=128,
                batch_readahead=1,fragment_readahead=1).to_batches():
            if batch.nbytes > 128*self.max_key_bytes:
                raise ValueError('saved keys exceed max_key_bytes')
            digests=[]
            for row in batch.to_pylist():
                digest=self.key_digest(tuple(row[k] for k in fields))
                if self._contains(digest) or digest in digests:
                    raise ValueError('saved stream snapshot keys are not unique')
                digests.append(digest);self.rows+=1
                if self.rows > self.max_rows:raise ValueError('stream snapshot exceeds max_rows')
            self._remember(digests)

    def prepare_rows(self, rows, selected):
        import pyarrow as pa
        import lance
        from demiflow.lance.read import scalar_equal
        from demiflow.operators.grouping import row_size
        input_bytes=sum(row_size(row,min(self.max_row_bytes,self.bounds.max_row_bytes)) for row in rows)
        if input_bytes>self.bounds.max_batch_bytes:
            raise DataflowError('BUDGET_EXCEEDED','Stream writer retained rows exceed native batch budget')
        projected=[{key:rows[index].get(key) for key in self.schema.names} for index in selected]
        keys=[tuple(row[k] for k in self.key) for row in projected]
        digests=[self.key_digest(key) for key in keys]
        known=[self._contains(digest) for digest in digests]
        if len(set(digests))!=len(digests) or self.mode=='overwrite' and any(known):
            raise ValueError('stream snapshot keys must be nonempty and unique')
        fresh=[]; additions=[];indices=[]
        for input_index,key,digest,row,seen in zip(selected,keys,digests,projected,known):
            if seen:
                predicate=' AND '.join(scalar_equal(name,value) for name,value in zip(self.key,key))
                found=[]
                for batch in lance.dataset(self.uri,version=self.version).scanner(filter=predicate,limit=2,
                        batch_size=1,batch_readahead=1,fragment_readahead=1).to_batches():
                    if batch.nbytes>self.max_row_bytes:raise ValueError('saved row exceeds max_row_bytes')
                    found.extend(batch.to_pylist())
                if _canonical(found)!=_canonical(pa.Table.from_pylist([row],schema=self.schema).to_pylist()):
                    raise ValueError('stream append key has conflicting saved content')
            else:fresh.append(row);additions.append(digest);indices.append(input_index)
        if self.rows+len(fresh)>self.max_rows:raise ValueError('stream snapshot exceeds max_rows')
        table=pa.Table.from_pylist(fresh,schema=self.schema)
        if input_bytes+table.get_total_buffer_size()>self.bounds.max_batch_bytes:
            raise DataflowError('BUDGET_EXCEEDED','Stream writer Python rows and Arrow coexistence exceed native batch budget')
        self._remember(additions)  # Reserve derived index capacity before publication.
        return table,indices

    def close(self):
        try:self._close_index()
        finally:
            if self._target_lock is not None:
                self._target_lock.__exit__(None,None,None);self._target_lock=None

class NativeStreamLanceService:
    def __init__(self, execution):
        from demiflow.execution.native_queue.dataflow_stream_lance import StreamLanceFactory
        self.ex=execution;self.action=execution._action;self.run=self.action.context
        self._last_acked = {}
        self._acked_ordinals = {}
        self._published = {}
        self.store=execution._store;self.bounds=execution._bounds
        self.writers={};self.partitions={};self.lock=threading.RLock();self.failed=False;self.started=False
        from demiflow.execution.seal.plan import load_sealed_dataflow, verify_dataflow_program
        from demiflow.data.plan import CallableSpec
        _,_,pin=load_sealed_dataflow(execution._sealed,action_id=self.action.action_id,
                                   store=self.store,bounds=self.bounds)
        for node in execution._graph.nodes:
            if node.kernel.kind!='SaveLanceOp':continue
            stage=next(stage for stage in execution._physical.stages if node.occurrence_id in stage.occurrences)
            if len(stage.partitions)!=1:
                raise DataflowError('RESOURCE_INFEASIBLE','A stream target requires its original single writer')
            program=verify_dataflow_program(node.program_ref,expected_pin=pin['programs'][node.occurrence_id],
                                           store=self.store,bounds=self.bounds)
            spec=program.value
            if not isinstance(spec,CallableSpec) or spec.target is not StreamLanceFactory or len(spec.constructor_args)!=2:
                raise DataflowError('INVALID_CONTRACT','Stream target has no original sealed factory')
            declared,_=spec.constructor_args
            if any(w.uri==declared['uri'] or w.stage==declared['stage'] for w in self.writers.values()):
                raise DataflowError('INVALID_CONTRACT','Stream targets and stage names must be unique')
            self.partitions[node.occurrence_id]=stage.partitions[0]
            self.writers[node.occurrence_id]=_NativeWriter(declared,action=self.action,store=self.store,bounds=self.bounds)
        if self.writers:
            self.run.journal.bind_source(self.run.fence,self.action.action_id+'-stream-lance',
                {'schema':SCHEMA,'nodes':sorted(self.writers)})



    def _coordinator(self, key, declaration):
        # A recovered scope must retain the target/schema from the original
        # sealed writer. Validate before constructing a sink or opening ANY
        # table, including reference-only invocations with no native publish.
        from demiflow.lance.schema import schema_identity
        from demiflow.execution.outputs.lance_sink import spec_from_identity
        writer = self.writers.get(declaration['node_id'])
        if writer is None or declaration['schema_identity'] != schema_identity(writer.schema):
            raise DataflowError('CORRUPT_ARTIFACT','Stream scope differs from its declared writer schema')
        reference = declaration['reference']
        if reference is not None and (type(reference) is not dict or set(reference) != {'uri','version'}
                or reference['uri'] != writer.uri or type(reference['version']) is not int
                or reference['version'] < 1):
            raise DataflowError('CORRUPT_ARTIFACT','Stream declared reference differs from its sealed writer')
        if declaration['spec'] is not None:
            spec = spec_from_identity(declaration['spec'])
            if spec.uri != writer.uri or schema_identity(spec.schema) != schema_identity(writer.schema):
                raise DataflowError('CORRUPT_ARTIFACT','Stream native spec differs from its sealed writer')
        scope=RunAction(self.run,key,declaration)
        return CommitCoordinator(scope),_BatchSink(scope,declaration)

    def start(self):
        if self.started:return
        self.started=True
        for node,writer in self.writers.items():
            checked = getattr(self.ex, '_checked', None)
            policy = checked.native_save_policy(self.ex, node, writer) if checked is not None else None
            writer.start(node, policy=policy)
            latest=None
            for key,decl in stream_scopes(self.run.journal, self.run.fence, self.action.action_id, node, max_commits=self.run.limits['max_commits'], max_control_bytes=self.bounds.max_control_bytes):
                coordinator,sink=self._coordinator(key,decl)
                facts=coordinator.recover({'lance':sink})
                for fact in facts:
                    if fact['status']!='acked':continue
                    self._remember_ack(node, key, fact['commit_id'], decl['request']['ordinal'])
                    reference=fact['receipts']['lance']['data']['reference']
                    if reference is not None:latest=reference['version'];writer.confirmed=True
                    if decl['request']['finish']:writer.finished=True
                    else:writer.last_ordinal=max(writer.last_ordinal,decl['request']['ordinal'])
            expected=latest if latest is not None else writer.baseline
            if writer.head()!=expected:
                raise UnresolvedCommit('Stream target head differs from its original native receipts')
            writer.version=latest if latest is not None else writer.baseline if writer.mode=='append' else None
            writer.rebuild()

    def can_recover_without_cut(self):
        return bool(self.started and self.writers and self.action.replaying
                    and not self.run.journal.execution_commits(self.action.action_id))

    def request(self, command, record):
        with self.lock:
            self.run.check_stop()
            if not self.started or self.failed:raise DataflowError('UNKNOWN_EFFECT','Stream writer requires native recovery')
            if command.node_id not in self.writers or command.value_ref is not None or command.reservation is not None or command.options:
                raise DataflowError('FENCED','Stream request differs from its sealed owner')
            request=dict(record)
            if (set(request)!={'schema','invocation','ordinal','partition','records','selected','batch','finish'}
                    or request['schema']!=SCHEMA or type(request['ordinal']) is not int or request['ordinal']<0
                    or type(request['finish']) is not bool):
                raise DataflowError('INVALID_CONTRACT','Invalid stream batch membership')
            operation=identity([SCHEMA,self.action.action_id,command.node_id,request['invocation']])
            if command.operation_id!=operation:
                raise DataflowError('INVALID_CONTRACT','Stream operation identity differs')
            if request['partition'] != self.partitions[command.node_id]:
                raise DataflowError('FENCED','Stream request belongs to another physical partition')
            if type(request['invocation']) is not str or len(request['invocation']) != 64:
                raise DataflowError('INVALID_CONTRACT','Stream invocation must be an original call identity')
            writer=self.writers[command.node_id]
            existing=stream_scope(self.run.journal, self.action.action_id, operation)
            if existing is not None:
                if existing['request_key']!=command.request_key or existing['request']!=request:
                    raise DataflowError('INVALID_CONTRACT','Stream invocation changed payload or record membership')
                declaration=existing
                table=None
            else:
                if writer.finished or request['ordinal']!=writer.last_ordinal+1:
                    raise DataflowError('INVALID_CONTRACT','Stream invocation order changed')
                from demiflow.execution.contracts import state_codec
                ref=ArtifactRef(**request['batch'])
                rows=state_codec.loads(b''.join(self.store.read(ref,max_bytes=self.bounds.max_batch_bytes)),max_bytes=self.bounds.max_batch_bytes)
                selected=request['selected'];ids=request['records']
                if (type(rows) is not list or len(rows)>writer.batch_size or len(ids)!=len(rows)
                        or any(type(x) is not str for x in ids) or type(selected) is not list
                        or any(type(i) is not int or not 0<=i<len(rows) for i in selected)
                        or selected!=sorted(set(selected)) or request['finish'] and rows):
                    raise DataflowError('INVALID_CONTRACT','Stream batch exceeds sealed membership or size')
                if writer.head() != (writer.version if writer.version is not None else writer.baseline):
                    raise UnresolvedCommit('Stream target changed outside its confirmed native writer')
                table,fresh=writer.prepare_rows(rows,selected)
                publish=bool(table.num_rows or request['finish'] and writer.version is None)
                from demiflow.lance.model import LanceWriteSpec
                from demiflow.execution.seal.program_identity import describe
                from demiflow.lance.schema import schema_identity
                if publish:
                    mode=('create' if writer.version is None and writer.permission_mode=='create' else
                          'overwrite' if writer.version is None and writer.mode=='overwrite' else 'append')
                    spec=LanceWriteSpec(uri=writer.uri,mode=mode,schema=writer.schema,
                        expected_version=writer.baseline if writer.version is None else writer.version)
                else:spec=None
                declaration={'schema':SCHEMA,'node_id':command.node_id,'request':request,
                    'request_key':command.request_key,'fresh':fresh,'spec':describe(spec) if spec is not None else None,
                    'schema_identity':schema_identity(writer.schema),'reference':writer.reference() if writer.version is not None else None}
                self.run.journal.bind_commit_scope(self.run.fence,operation,self.action.action_id,declaration)
            coordinator,sink=self._coordinator(operation,declaration)
            if table is None:
                facts=coordinator.recover({'lance':sink})
                if facts and facts[-1]['status']=='acked':
                    self._remember_ack(command.node_id, operation, facts[-1]['commit_id'], request['ordinal'])
                    if request['finish']:self._publish(command.node_id)
                    return NativeJournalReply(command.operation_id,{'status':'acked',
                        'reference':facts[-1]['receipts']['lance']['data']['reference']},None)
                # A pending intent never published; reconstruct ONLY its original
                # selected batch, not the new table head or a new computation.
                from demiflow.execution.contracts import state_codec
                import pyarrow as pa
                ref=ArtifactRef(**request['batch'])
                rows=state_codec.loads(b''.join(self.store.read(ref,max_bytes=self.bounds.max_batch_bytes)),max_bytes=self.bounds.max_batch_bytes)
                table,_=writer.prepare_rows(rows,declaration['fresh'])
            try:
                fact=coordinator.commit(1,outputs={'lance':(sink,[table])},
                    position=SourcePosition(identity([self.action.action_id,command.node_id]),request['ordinal']+1))
            except BaseException:
                self.failed=True
                raise
            self._remember_ack(command.node_id, operation, fact['commit_id'], request['ordinal'])
            if request['finish']:self._publish(command.node_id)
            reference=fact['receipts']['lance']['data']['reference']
            if reference is not None:writer.version=reference['version'];writer.confirmed=True
            if request['finish']:writer.finished=True
            else:writer.last_ordinal=request['ordinal'];writer.rows+=table.num_rows
            return NativeJournalReply(command.operation_id,{'status':'acked','reference':reference},None)

    def _remember_ack(self, node, scope, commit, ordinal):
        # A resumed worker may request its earlier already-confirmed batches.
        # Such replay must not regress the result projected from native history.
        if ordinal >= self._acked_ordinals.get(node, -1):
            self._acked_ordinals[node] = ordinal
            self._last_acked[node] = (scope, commit)

    def _publish(self, node):
        # Product action facts are immutable per attempt. Emit once at real EOI
        # or stopped cleanup; the original per-microbatch ACKs remain queryable
        # in runtime_commit_scopes throughout execution.
        checked = getattr(self.ex, '_checked', None)
        latest = self._last_acked.get(node)
        if checked is not None and latest is not None and self._published.get(node) != latest:
            checked.native_stage_commit(self.ex, *latest)
            self._published[node] = latest

    @property
    def outputs(self):
        return {writer.stage:writer.reference() for writer in self.writers.values() if writer.version is not None and writer.confirmed}

    def close(self):
        with self.lock:
            first=None
            for node, writer in self.writers.items():
                try:self._publish(node)
                except BaseException as exc:first=first or exc
                try:writer.close()
                except BaseException as exc:first=first or exc
            if first is not None:raise first
