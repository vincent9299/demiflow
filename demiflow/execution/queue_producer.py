"""Incremental task publication in the native stream's input-unit barriers."""
import hashlib
import inspect
import json
from pathlib import Path

from ..data import plan as ops
from ..data.sources import FileSource, ItemsSource, MaterializedSource,LanceSource
from .blocking_io import BlockingIOPool
from .commit import CommitCoordinator
from .queue import QueueChannel
from .queue_contract import ProducerRef, ProducerWindowLimit, queue_config
from .queue_commit import producer_action
from .queue_sinks import QueuePublishSink
from .recovery_contract import RecoveryContractError, SourcePosition, document, identity, identifier
from .recovery_identity import describe, source_identity
from .row_scope import RowScope, ScopedRow, current_row_scope


def producer_actor(plan):
    found = []
    for index, operation in enumerate(plan.operations):
        target = getattr(getattr(operation, 'callable', None), 'target', None)
        actor = getattr(target, '__self__', target)
        if type(actor) is QueueProducer:
            if type(operation) is not ops.AsyncMapOp:
                raise RecoveryContractError('Queue publication requires its native async-map adapter')
            found.append((index, actor))
    if len(found) > 1:
        raise RecoveryContractError('A producer graph has exactly one enqueue node')
    return found[0] if found else None


def validate_source(source, specification):
    """Check finite source geometry before native identity hashes its contents."""
    if type(source) is FileSource:
        if source.format != 'json' or dict(source.options) not in ({}, {'lines': True}):
            raise RecoveryContractError('Producer file sources require fixed local JSONL records')
        size = 0
        if len(source.paths) > 128:
            raise RecoveryContractError('Producer source exceeds 128 files')
        for name in source.paths:
            path = Path(name)
            if path.is_symlink() or not path.is_file():
                raise RecoveryContractError('Producer source is unavailable')
            size += path.stat().st_size
            if size > specification.max_bytes:
                raise RecoveryContractError('Producer source exceeds its declared byte budget')
        if specification.sha256 is None:
            raise RecoveryContractError('JSONL sources require a content SHA256')
    elif type(source) is ItemsSource:
        if len(source.items) > specification.max_rows:
            raise RecoveryContractError('Producer literal source exceeds its row budget')
    elif type(source) is MaterializedSource:
        from .executors.local import _LocalMaterializedHandle
        from .dataflow_inputs import InputMaterializedHandle
        if type(source.handle) not in {_LocalMaterializedHandle, InputMaterializedHandle} or any(type(b) not in (tuple, list) for b in source.handle.blocks):
            raise RecoveryContractError('Producer materialization needs fixed literal records; use fixed JSONL for large metadata')
        source.handle.check_open()
        if source.handle.row_count > specification.max_rows:
            raise RecoveryContractError('Producer literal source exceeds its row budget')
    elif type(source) is LanceSource:
        from ..lance.model import LanceScanSpec
        query=source.query
        if (type(query) is not LanceScanSpec or query.version is None or query.storage_options or
                query.filter is not None or query.projection or source.native_options is not None):
            raise RecoveryContractError('Producer Lance source requires a fixed local version and direct columns without filter/projection')
        if specification.kind!='lance' or specification.version!=query.version or specification.uri!=query.uri:
            raise RecoveryContractError('Producer Lance declaration differs from its exact native source')
    else:
        raise RecoveryContractError('Producer requires fixed literals, JSONL or exact local Lance; live iterators cannot prove EOS')


class QueueProducer:
    concurrency = 1
    catch = ()

    def __init__(self, config, *, producer, messages='messages', queue_depth=1):
        self.config = queue_config(config)
        if type(producer) is not ProducerRef:
            raise TypeError('enqueue requires a ProducerRef from QueueChannel.open')
        identifier(messages)
        if type(queue_depth) is not int or not 1 <= queue_depth <= 256:
            raise ValueError('enqueue queue_depth must be 1..256')
        self.producer, self.messages, self.queue_depth = producer, messages, queue_depth
        self.label = 'enqueue'
        self.parent = self.channel = self.io = None
        self.position = self.offset = self.units_bytes = 0
        self.next_cursor = None
        self.contract = None
        self.window_commits = 0
        self.stage_counts = {}

    def recovery_identity(self):
        p = self.producer
        return dict(contract='demiflow.queue.enqueue.v2', config=self.config.declaration(),
                    producer=dict(queue_uuid=p.queue_uuid, pool=p.pool, generation=p.generation,
                                  producer_id=p.producer_id, source=describe(p.source)), messages=self.messages)

    def configure(self, parent, source, plan):
        found = producer_actor(plan)
        if found is None or found[1] is not self or found[0] != len(plan.operations)-1:
            raise RecoveryContractError('enqueue must be the final producer transform')
        validate_source(source, self.producer.source)
        operations = []
        search_nodes = {}
        for index,operation in enumerate(plan.operations[:-1]):
            target = getattr(getattr(operation, 'callable', None), 'target', None)
            if type(operation) in {ops.MapOp, ops.BoundMapOp, ops.FlatMapOp, ops.FilterOp} and inspect.isfunction(target):
                if getattr(operation, 'stream_options', None) is not None:
                    policy = operation.stream_options
                    if policy.catch or policy.concurrency != 1:
                        raise RecoveryContractError('Producer pure transforms require one ordered worker and no dropped exceptions')
                operations.append(describe(operation))
            elif type(operation) is ops.SearchWebOp:
                from ..collect.operators import SearchWeb
                actor = getattr(target, '__self__', target)
                if type(actor) is not SearchWeb or operation.catch or operation.hard_timeout is not None or operation.concurrency != 1:
                    raise RecoveryContractError('Producer search requires its ordered native actor')
                operations.append(actor.recovery_identity())
                search_nodes[actor] = str(index)
            else:
                raise RecoveryContractError('Producer supports pure maps/filter/flat_map and native search before enqueue')
        self.parent, self.source = parent, source
        binding = source_identity(source)
        self._verify_source(source, binding)
        from .queue_contract import resolve_producer_contract
        declaration = dict(protocol='demiflow.queue.producer-transform.v3',
            source=binding, specification=describe(self.producer.source), operations=operations, messages=self.messages)
        self.channel = QueueChannel(self.config)
        self.contract = resolve_producer_contract(self.channel, self.producer, declaration, parent)
        self.search_nodes = search_nodes

    def _verify_source(self, source, binding):
        spec = self.producer.source
        if type(source) is FileSource:
            facts = binding['files']
            actual = facts[0]['sha256'] if len(facts) == 1 else identity(facts)
            if actual != spec.sha256:
                raise RecoveryContractError('Producer source SHA256 differs from fixed declaration')
            self.file_signatures = [(str(Path(n).absolute()), Path(n).stat()) for n in source.paths]
        elif type(source) is not LanceSource:
            sha = hashlib.sha256(); size = 0
            blocks = (source.items,) if type(source) is ItemsSource else source.handle.blocks
            for block in blocks:
                for row in block:
                    raw = document(row, self.parent.context.limits['max_row_bytes'] if self.parent else 16*1024**2)
                    size += len(raw)+1
                    if size > spec.max_bytes:
                        raise RecoveryContractError('Producer source exceeds its declared byte budget')
                    sha.update(raw); sha.update(b'\n')
            if spec.sha256 is not None and sha.hexdigest() != spec.sha256:
                raise RecoveryContractError('Producer literal SHA256 differs from fixed declaration')

    def bind_execution(self, run, node_id):
        if self.parent is None or self.parent.context is not run:
            raise RecoveryContractError('Producer must bind its actual source and transform before startup')

    def snapshot_state(self):
        return {}

    def restore_state(self, state):
        if state != {}:raise RecoveryContractError('Producer progress belongs to queue-native facts')

    async def astart(self):
        if self.parent is None:
            raise RecoveryContractError('Producer has no native source-unit driver')
        self.io = BlockingIOPool(1, name='demiflow-queue-producer')
        await self.io.run(self.channel._attach)
        if self.channel.database.queue_uuid != self.producer.queue_uuid:
            raise RecoveryContractError('Producer belongs to another physical queue')
        from .queue_commit import bind_queue_budget
        await self.io.run(bind_queue_budget,self.parent,self.channel)
        await self.io.run(self._resume)

    def _resume(self):
        p = self.producer
        with self.channel.database.connection(write=True) as db:
            owner = QueueChannel._producer(db, p)
            if owner[4]:
                raise RecoveryContractError('Sealed producer cannot execute')
            contract = db.execute('SELECT producer_contract FROM queue_pools WHERE generation=?', (p.generation,)).fetchone()[0]
            if contract is not None and contract != self.contract:
                raise RecoveryContractError('Producer source/transformation contract changed')
            db.execute('UPDATE queue_pools SET producer_contract=? WHERE generation=?', (self.contract, p.generation))
            pending = db.execute("SELECT s.declaration,c.status FROM runtime_commits c JOIN runtime_commit_scopes s ON c.action_id=s.scope_id WHERE s.parent_action_id=? AND c.status!='acked' LIMIT 2", ('producer-'+p.generation,)).fetchall()
        if len(pending) > 1:
            raise RecoveryContractError('Producer has more than one unresolved native input chunk')
        for raw, status in pending:
            if status != 'pending':
                declaration = json.loads(raw)
                phase=declaration['phase']
                action = producer_action(self.parent, self.channel, p, declaration['occurrence'], self.contract,phase=phase)
                if phase=='source':
                    from ..collect.search_contract import NativeSearchResultSink
                    sinks={'search':NativeSearchResultSink(action)}
                else:sinks={'publish': QueuePublishSink(action, p, self.contract)}
                CommitCoordinator(action).recover(sinks)
        with self.channel.database.connection() as db:
            row = db.execute('SELECT source_position,output_offset,source_cursor,eos FROM queue_pools WHERE generation=?', (p.generation,)).fetchone()
        self.position, self.offset = row[:2]
        self.cursor = json.loads(row[2]) if row[2] is not None else None
        self.eos = bool(row[3])

    def _check_files(self):
        for name, original in self.file_signatures:
            current = Path(name).stat()
            if (current.st_dev,current.st_ino,current.st_size,current.st_mtime_ns,current.st_ctime_ns) != (original.st_dev,original.st_ino,original.st_size,original.st_mtime_ns,original.st_ctime_ns):
                raise RecoveryContractError('Fixed producer source changed during execution')

    def rows(self):
        if self.eos:
            return
        source = self.source
        position = self.position
        if type(source) in {ItemsSource,MaterializedSource}:
            blocks = (source.items,) if type(source) is ItemsSource else source.handle.blocks
            skip = position
            for block in blocks:
                if skip >= len(block):
                    skip -= len(block); continue
                for index in range(skip, len(block)):
                    self.next_cursor = None
                    yield ScopedRow(block[index], RowScope(identity(['producer-unit',self.producer.generation,position])))
                    position += 1
                skip = 0
            return
        if type(source) is LanceSource:
            from dataclasses import replace
            from ..lance.read import _iter_scan
            from ..data.runtime_contract import check_size
            cursor=self.cursor or {'offset':position,'bytes':0}
            if set(cursor)!={'offset','bytes'} or cursor['offset']!=position:
                raise RecoveryContractError('Native Lance producer cursor differs')
            query=replace(source.query,batch_size=1,batch_readahead=1,fragment_readahead=1)
            used=cursor['bytes']
            for batch in _iter_scan(query,batch_size=1,offset=position):
                self.parent.context.check_stop()
                if position>=self.producer.source.max_rows:
                    raise RecoveryContractError('Producer source exceeds its declared record count')
                if batch.num_rows!=1 or batch.get_total_buffer_size()>self.parent.context.limits['max_row_bytes']:
                    raise RecoveryContractError('Native Lance producer row exceeds its retained byte budget')
                row=batch.to_pylist()[0]
                used+=check_size(row,self.parent.context.limits['max_row_bytes'])
                if used>self.producer.source.max_bytes:
                    raise RecoveryContractError('Producer Lance source exceeds its declared byte budget')
                self.next_cursor={'offset':position+1,'bytes':used}
                yield ScopedRow(row,RowScope(identity(['producer-unit',self.producer.generation,position])))
                position+=1
            return
        cursor = self.cursor or {'file':0,'offset':0}
        if set(cursor) != {'file','offset'} or any(type(v) is not int or v<0 for v in cursor.values()):
            raise RecoveryContractError('Invalid native JSONL cursor')
        for file_index in range(cursor['file'],len(source.paths)):
            self._check_files()
            with Path(source.paths[file_index]).open('rb') as stream:
                if file_index == cursor['file']: stream.seek(cursor['offset'])
                while True:
                    self.parent.context.check_stop()
                    maximum = self.parent.context.limits['max_row_bytes']
                    line = stream.readline(maximum+1)
                    if not line: break
                    if len(line)>maximum:
                        raise RecoveryContractError('Producer source record exceeds its row byte budget')
                    if not line.strip(): continue
                    if position>=self.producer.source.max_rows:
                        raise RecoveryContractError('Producer source exceeds its declared record count')
                    self.next_cursor = {'file':file_index,'offset':stream.tell()}
                    yield ScopedRow(json.loads(line),RowScope(identity(['producer-unit',self.producer.generation,position])))
                    position += 1
        self._check_files()

    def begin_unit(self):
        self.chunk = 0
        self.stage_counts = {}

    def observe_expansion(self, stage, row):
        from ..data.runtime_contract import check_size
        count, size = self.stage_counts.get(stage,(0,0))
        count += 1; size += check_size(row,self.parent.context.limits['max_row_bytes'])
        if count>1024 or size>16*1024**2:
            raise RecoveryContractError('Producer input expansion exceeds 1024 records or 16 MiB at a stage')
        self.stage_counts[stage] = (count,size)

    def _publish(self, occurrence, messages, *, unit_end=False, eos=False):
        if self.window_commits>=self.parent.context.limits['max_commits']:
            raise ProducerWindowLimit('Producer run commit window exhausted; pool remains open')
        action = producer_action(self.parent,self.channel,self.producer,occurrence,self.contract)
        sink = QueuePublishSink(action,self.producer,self.contract)
        coordinator = CommitCoordinator(action)
        facts = coordinator.recover({'publish':sink})
        if facts and facts[0]['status']!='pending':
            fact = facts[0]
            prepared = fact['prepared']['sinks']['publish']
            if prepared['occurrence'] != occurrence or prepared['source_position'] != self.position:
                raise RecoveryContractError('Recovered publication belongs to another source unit')
            # A parked worker may replay a call whose native receipt was fixed
            # after its last Dataset cut. Reuse that original ordinal, rather
            # than incrementing an offset which already includes this receipt.
            self.chunk = prepared['output_ordinal']
        else:
            prepared = dict(format=2,occurrence=occurrence,source_position=self.position,output_ordinal=self.chunk,
                            messages=messages,unit_end=unit_end,eos=eos)
            if unit_end and self.next_cursor is not None:prepared['source_cursor']=self.next_cursor
            fact = coordinator.commit(1,outputs={'publish':(sink,prepared)},
                position=SourcePosition(self.producer.source.source_id,self.position))
            self.window_commits += 1
        self.chunk += 1
        self.last_commit_id = fact['commit_id']
        return fact['receipts']['publish']['data']

    async def __call__(self,row):
        self.last_call_commits = []
        if type(row) is not dict or 'queue_publish' in row:
            raise RecoveryContractError('enqueue requires a record with unused queue_publish output')
        scope = current_row_scope()
        if scope is None:
            raise RecoveryContractError('Producer lost native source occurrence')
        values = row[self.messages]
        if type(values) is not list or len(values)>32:
            raise RecoveryContractError('enqueue requires at most 32 messages per output record')
        # Validate each item before constructing a chunk. A row never causes a
        # multi-megabyte serialization followed by splitting of that allocation.
        chunks=[];chunk=[];size=2
        for value in values:
            raw=document(value,min(self.parent.context.limits['max_row_bytes'],self.config.limits.max_payload_bytes+1024))
            if len(raw)>512*1024:
                raise RecoveryContractError('One producer message exceeds its chunk byte budget')
            if chunk and size+len(raw)+1>512*1024:
                chunks.append(chunk);chunk=[];size=2
            chunk.append(value);size+=len(raw)+1
        if chunk or not chunks:chunks.append(chunk)
        result=dict(requested=0,inserted=0,duplicates=0)
        for index,chunk in enumerate(chunks):
            occurrence=identity([scope.occurrence,'chunk',index])
            receipt=await self.io.run(self._publish,occurrence,chunk)
            self.last_call_commits.append(self.last_commit_id)
            for name in result:result[name]+=receipt[name]
        return {**row,'queue_publish':result}

    async def end_unit(self):
        occurrence=identity(['producer-unit-end',self.producer.generation,self.position])
        await self.io.run(self._publish,occurrence,[],unit_end=True)
        self.position += 1

    async def end_source(self):
        if self.eos:return
        self.chunk=0
        occurrence=identity(['producer-source-eos',self.producer.generation,self.position])
        await self.io.run(self._publish,occurrence,[],unit_end=True,eos=True)
        self.eos=True

    async def aclose(self):
        if self.io is not None:await self.io.aclose()
        self.io=None


def producer_facts(actor):
    from .queue_flow import resource
    return dict(native=actor.recovery_identity(), effects=['queue'],
                resources=[resource(actor.config, 'publish')], reserved_bytes=4*1024**2)
