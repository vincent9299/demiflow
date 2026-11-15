"""Native streaming Lance publications owned by the original driver journal.

Workers submit immutable batch artifacts. Each invocation is a bounded native
commit scope of its parent Dataset action, using the existing CommitCoordinator
and LanceSink publication marker. The optional SQLite key index is disposable
scratch, never recovery authority. Earlier confirmed microbatches stay visible
if a later batch fails; this is not whole-action atomic publication.
"""
from dataclasses import asdict
import json
from pathlib import Path
import sqlite3
import threading

from .contracts.dataflow import ArtifactCommand, ArtifactRef, NativeJournalCommand, NativeJournalReply
from .contracts.dataflow_errors import DataflowError
from .recovery_contract import identity, document, SourcePosition, UnresolvedCommit
from .stream_lance import StreamLanceWriter, _canonical
from .lance_sink import LanceSink
from .commit import CommitCoordinator
from .run_context import RunAction

SCHEMA = 'demiflow.stream_lance.v3'


class StreamLanceFactory:
    def __new__(cls, declaration, when):
        return StreamLanceCall(declaration, when)


class StreamLanceCall:
    """No writer, database, table handle or target lock can exist in a worker."""
    def __init__(self, declaration, when):
        self.declaration, self.when = declaration, when
        self.services = None
        self.record_ids = ()
        self.ordinal = 0

    def bind(self, services, occurrence):
        self.services, self.occurrence = services, occurrence

    def bind_records(self, ids, ordinal):
        self.record_ids, self.ordinal = tuple(ids), ordinal

    def snapshot_state(self): return {}

    def restore_state(self, state):
        if state != {}:
            raise DataflowError('INCOMPATIBLE_HISTORY', 'Stream publication state belongs to its native scope')

    def _request(self, rows, selected, *, invocation, ordinal, finish=False):
        from .kernels import state_codec
        from ..operator_llm.journal import request_key
        services = self.services
        raw = state_codec.dumps(list(rows), max_bytes=services.bounds.max_batch_bytes)
        ref = services.store.put(iter([raw]), max_bytes=services.bounds.max_batch_bytes,
                                 media_type='application/x-demiflow-stream-lance-batch')
        request = {'schema':SCHEMA, 'invocation':invocation, 'ordinal':ordinal,
            'partition':services.assignment.lease.partition_ids[0],
            'records':list(self.record_ids) if not finish else [],
            'selected':list(selected), 'batch':asdict(ref), 'finish':finish}
        body = document(request, services.bounds.max_control_bytes)
        request_ref = services.store.put(iter([body]), max_bytes=len(body), media_type='application/json')
        action = services.assignment.action_id
        operation = identity([SCHEMA, action, self.occurrence, invocation])
        command = NativeJournalCommand(operation, 'stream_lance', action, self.occurrence,
            request_key(request), request_ref, None, None, {})
        reply = services.journal.native_request(command)
        if reply.operation_id != operation or reply.value_ref is not None:
            raise DataflowError('INTERNAL_PROTOCOL', 'Stream writer returned an unrelated receipt')
        from .contracts.dataflow_codec import _thaw_json
        value = _thaw_json(reply.value)
        if value.get('status') != 'acked':
            raise DataflowError('UNKNOWN_EFFECT', 'Stream batch has no original ACKed receipt')
        return value['reference']

    def __call__(self, rows):
        from .row_scope import current_row_scope
        scope = current_row_scope()
        if scope is None or len(self.record_ids) != len(rows):
            raise DataflowError('INVALID_CONTRACT', 'Stream writer requires original record membership')
        name = self.declaration['output_ref']
        if name is not None and any(name in row for row in rows):
            raise ValueError('output_ref would overwrite an input field')
        selected = [index for index, row in enumerate(rows) if self.when is None or self.when(row)]
        ref = self._request(rows, selected, invocation=scope.occurrence, ordinal=self.ordinal)
        chosen = set(selected)
        return [{**row, **({name:ref} if name is not None and index in chosen else {})}
                for index, row in enumerate(rows)]

    def finish(self, ordinal):
        partition = self.services.assignment.lease.partition_ids[0]
        self._request([], [], invocation=identity([SCHEMA, self.occurrence, partition, 'finish']),
                      ordinal=ordinal, finish=True)


def declaration(operation, actor):
    """Validate the public native actor before replacing it with a passive factory."""
    from .stream_lance import StreamLanceSink
    from ..data.plan import SaveLanceOp
    target = operation.callable.target
    if (type(operation) is not SaveLanceOp or type(actor) is not StreamLanceSink
            or getattr(target, '__func__', None) is not StreamLanceWriter.__call__
            or actor.version is not None or actor._index is not None or actor._lock is not None
            or actor.checkpoint is not None):
        raise DataflowError('INVALID_CONTRACT', 'Save requires its original unstarted native writer')
    for name in ('uri','stage','output_ref','mode','key'):
        expected = getattr(operation, name)
        actual = getattr(actor, name)
        if name == 'key':
            expected = [expected] if isinstance(expected,str) else list(expected)
            actual = [actual] if isinstance(actual,str) else list(actual)
        if expected != actual:
            raise DataflowError('INVALID_CONTRACT', 'Save declaration differs from its native actor')
    value = {name:getattr(actor,name) for name in ('uri','stage','output_ref','mode',
        'max_rows','max_row_bytes','max_key_bytes','max_index_bytes')}
    value['key'] = list(actor.key) if isinstance(actor.key,tuple) else [actor.key]
    from .record_schema import encode_schema
    from .dataflow_api import _PREFLIGHT_BOUNDS
    value['schema'] = encode_schema(actor.schema,bounds=_PREFLIGHT_BOUNDS).hex()
    value['max_batch'] = operation.max_batch
    return value


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
        from ..lance.storage import inspect_lance
        from ..errors import LanceResourceNotFound
        try: return inspect_lance(self.uri,None,None).resolved_version
        except LanceResourceNotFound: return None

    def start(self, node, *, latest=None):
        from .artifacts import run_lock
        self._target_lock = run_lock(Path(self.uri).with_suffix('.stream-writer'))
        self._target_lock.__enter__()
        run = self.action.context
        key = self.action.action_id+'-stream-target-'+node
        baseline = run.journal.source_binding(key)
        if baseline is None:
            baseline = {'uri':self.uri,'version':self.head()}
            run.journal.bind_source(run.fence,key,baseline)
        if baseline['uri'] != self.uri:
            raise DataflowError('INCOMPATIBLE_HISTORY','Stream target binding differs')
        self.baseline = baseline['version']
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
        from .lance_predicate import scalar_equal
        from .stream_grouping import row_size
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


class _BatchSink:
    """Native prepared table publication, or verified reference-only receipt."""
    sink_id='lance'
    def __init__(self, scope, declaration):
        self.scope,self.declaration=scope,declaration
        spec=declaration['spec']
        if spec is not None:
            from .lance_sink import spec_from_identity
            self.native=LanceSink(scope,spec_from_identity(spec))
            self._marker=self.native._marker  # Original coordinator releases its atomic marker staging grant.
        else:self.native=None

    def prepare(self, blocks, context):
        return {'native':self.native.prepare(blocks,context) if self.native else None,
                'reference':self.declaration['reference'],'request_key':self.declaration['request_key']}

    def inspect(self, prepared, context):
        if prepared['request_key']!=self.declaration['request_key']:
            raise DataflowError('CORRUPT_ARTIFACT','Stream prepared request differs')
        if self.native:
            receipt=self.native.inspect(prepared['native'],context)
            if receipt is None:return None
            version=receipt['data']['committed_version']
            reference={'uri':self.native.spec.uri,'version':version}
            native=receipt['data']
        else:
            reference=prepared['reference'];native=None
            if reference is not None:
                from ..lance.storage import open_lance_dataset
                from ..lance.schema import schema_identity
                table=open_lance_dataset(reference['uri'],reference['version'],None)
                if schema_identity(table.schema)!=self.declaration['schema_identity']:
                    raise DataflowError('CORRUPT_ARTIFACT','Stream saved reference schema differs')
        return {'commit_id':context.commit_id,'sink_id':'lance','status':'committed',
                'data':{'reference':reference,'native':native,'request_key':self.declaration['request_key']}}

    def commit(self, prepared, context):
        if self.native:self.native.commit(prepared['native'],context)
        return self.inspect(prepared,context)

    def abort(self, prepared, context):
        if self.native:self.native.abort(prepared['native'],context)


class NativeStreamLanceService:
    def __init__(self, execution):
        self.ex=execution;self.action=execution._action;self.run=self.action.context
        self.store=execution._store;self.bounds=execution._bounds
        self.writers={};self.partitions={};self.lock=threading.RLock();self.failed=False;self.started=False
        from .dataflow_seal import load_sealed_dataflow,verify_dataflow_program
        from ..data.plan import CallableSpec
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

    def _scopes(self,node):
        # The original table remains the authority. Its expression index orders
        # bounded pages without materializing every saved request in Python.
        with self.run.journal._connection() as db:
            if not db.execute("SELECT 1 FROM sqlite_master WHERE name='runtime_commit_scopes'").fetchone():return
        ordinal="CAST(json_extract(declaration,'$.request.ordinal') AS INTEGER)"
        owner="json_extract(declaration,'$.node_id')"
        with self.run.journal.execution_guard(self.run.fence) as db:
            db.execute('CREATE INDEX IF NOT EXISTS runtime_stream_lance_order ON runtime_commit_scopes '
                       '(parent_action_id,'+owner+','+ordinal+',scope_id)')
        previous=(-1,'');count=0
        while True:
            with self.run.journal._connection() as db:
                row=db.execute('SELECT scope_id,declaration,'+ordinal+' FROM runtime_commit_scopes '
                    'WHERE parent_action_id=? AND '+owner+"=? AND json_extract(declaration,'$.schema')=? "
                    'AND ('+ordinal+'>? OR ('+ordinal+'=? AND scope_id>?)) '
                    'ORDER BY '+ordinal+',scope_id LIMIT 1',
                    (self.action.action_id,node,SCHEMA,previous[0],previous[0],previous[1])).fetchone()
            if row is None:return
            key,raw,current=row;count+=1
            if (type(current) is not int or current<0 or count>self.run.limits['max_commits']
                    or len(raw.encode())>min(65536,self.bounds.max_control_bytes)):
                raise DataflowError('CORRUPT_ARTIFACT','Native stream scope exceeds bounded recovery contract')
            previous=(current,key)
            yield key,json.loads(raw)

    def _scope(self, key):
        with self.run.journal._connection() as db:
            if not db.execute("SELECT 1 FROM sqlite_master WHERE name='runtime_commit_scopes'").fetchone():return None
            row=db.execute('SELECT parent_action_id,declaration FROM runtime_commit_scopes WHERE scope_id=?',(key,)).fetchone()
        if row is None:return None
        if row[0]!=self.action.action_id:raise DataflowError('FENCED','Stream scope belongs to another action')
        return json.loads(row[1])

    def _coordinator(self, key, declaration):
        scope=RunAction(self.run,key,declaration)
        return CommitCoordinator(scope),_BatchSink(scope,declaration)

    def start(self):
        if self.started:return
        self.started=True
        for node,writer in self.writers.items():
            writer.start(node)
            latest=None
            for key,decl in self._scopes(node):
                coordinator,sink=self._coordinator(key,decl)
                facts=coordinator.recover({'lance':sink})
                for fact in facts:
                    if fact['status']!='acked':continue
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
            existing=self._scope(operation)
            if existing is not None:
                if existing['request_key']!=command.request_key or existing['request']!=request:
                    raise DataflowError('INVALID_CONTRACT','Stream invocation changed payload or record membership')
                declaration=existing
                table=None
            else:
                if writer.finished or request['ordinal']!=writer.last_ordinal+1:
                    raise DataflowError('INVALID_CONTRACT','Stream invocation order changed')
                from .kernels import state_codec
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
                from ..lance.model import LanceWriteSpec
                from .recovery_identity import describe
                from ..lance.schema import schema_identity
                if publish:
                    mode='overwrite' if writer.version is None and writer.mode=='overwrite' else 'append'
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
                    return NativeJournalReply(command.operation_id,{'status':'acked',
                        'reference':facts[-1]['receipts']['lance']['data']['reference']},None)
                # A pending intent never published; reconstruct ONLY its original
                # selected batch, not the new table head or a new computation.
                from .kernels import state_codec
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
            reference=fact['receipts']['lance']['data']['reference']
            if reference is not None:writer.version=reference['version'];writer.confirmed=True
            if request['finish']:writer.finished=True
            else:writer.last_ordinal=request['ordinal'];writer.rows+=table.num_rows
            return NativeJournalReply(command.operation_id,{'status':'acked','reference':reference},None)

    @property
    def outputs(self):
        return {writer.stage:writer.reference() for writer in self.writers.values() if writer.version is not None and writer.confirmed}

    def close(self):
        with self.lock:
            first=None
            for writer in self.writers.values():
                try:writer.close()
                except BaseException as exc:first=first or exc
            if first is not None:raise first
