"""Existing stream materialization boundary on the single run commit protocol.

The model has no storage role. The driver captures a fixed source, the terminal
sink prepares/publishes blocks, then RunJournal confirms progress and actor state.
"""
import asyncio

from .block_sink import BlockSink, BLOCK_BYTES
from .commit import CommitCoordinator
from .operator_state import OperatorStates, owner_of
from .recovery_contract import RecoveryContractError, SourcePosition
from .source_snapshot import capture_source
from .state_artifacts import StateArtifacts

BATCH_ROWS = 512
MAX_BATCHES = 16384


def has_stream(plan):
    from ..data.plan import is_stream_operation
    return any(is_stream_operation(op) for op in plan.operations)


def stream_view(executor,source,plan):
    """A view of the original native plan, using its existing stream driver."""
    from ..data.dataset import Dataset
    from ..data.plan import MapOp,BoundMapOp
    actors=[]
    for op in plan.operations:
        # Ordinary map actors are instantiated and closed by StreamMapRuntime,
        # including class declarations with no explicit stream options.
        if isinstance(op,(MapOp,BoundMapOp)):
            continue
        target=getattr(getattr(op,'callable',None),'target',None)
        if target is not None:
            actors.append(getattr(target,'__self__',target))
    return Dataset(source,plan,executor,tuple(actors))


def prepare_stream_blocks(executor,source,plan,action,context):
    """Unpublished bounded staging for one native sink commit.

    This is an internal sink preparation, not a Dataset action or a model cache.
    A prepared final sink is recovered before this producer is invoked again.
    """
    spool=BlockSink(action,'terminal-input',staging_only=True)
    stream_view(executor,source,plan)._run_stream(consume=spool.append)
    prepared=spool.prepare(None,context)
    return bounded_stream_batches(executor,spool.iter_rows(prepared),batch_format='pyarrow',split_bytes=True)


def bounded_stream_batches(executor, rows, *, batch_size=256, batch_format='default',
                           drop_last=False, split_bytes=False):
    """Check row/retained bytes before constructing a native terminal batch.

    Public iterators retain their requested row grouping; exceeding its byte
    ceiling is an explicit resource error. Internal sink batches may split.
    Conversion buffers fit the existing stream residency's fixed batch reserve.
    """
    from ..data.runtime_contract import check_size
    from .run_context import execution_scope
    size=executor._block_size if batch_size is None else batch_size
    if type(size) is not int or size<1:
        raise ValueError('Stream batch_size must be a positive integer')
    with execution_scope() as run:
        iterator=iter(rows)
        batch=[];used=0
        def convert(value):
            if batch_format in ('pyarrow','default',None):
                import pyarrow as pa
                return pa.Table.from_pylist(value)
            if batch_format in ('numpy','pandas'):
                return executor._rows_to_batch(value,batch_format)
            if batch_format=='list':return value
            raise ValueError('Unsupported stream batch format: '+str(batch_format))
        try:
            for row in iterator:
                amount=check_size(row,run.limits['max_row_bytes'])
                if used+amount>run.limits['max_batch_bytes']:
                    if not split_bytes or not batch:
                        raise RecoveryContractError('Stream terminal batch exceeds byte budget')
                    yield convert(batch)
                    batch=[];used=0
                batch.append(row);used+=amount
                if len(batch)==size:
                    yield convert(batch)
                    batch=[];used=0
            if batch and not drop_last:yield convert(batch)
        finally:
            close=getattr(iterator,'close',None)
            if close is not None:close()


class StreamMaterialization:
    def __init__(self, action, rows_factory, *, object_specs=None):
        self.action, self.run = action, action.context
        self.source_sink, self.source = capture_source(action, rows_factory)
        self.sink = BlockSink(action)
        self.row_sink=self.sink
        self.has_objects=bool(object_specs)
        self.batch_rows=128 if object_specs else BATCH_ROWS
        if object_specs:
            from .object_sink import ObjectRowsSink
            self.sink=ObjectRowsSink(action,object_specs=object_specs)
            self.row_sink=self.sink.rows
            action.object_sinks=self.sink.object_sinks
        self.coordinator = CommitCoordinator(action)
        facts = self.coordinator.recover({"output": self.sink})
        self.inputs = self.rows = self.sequence = 0
        self.blocks = []
        restored = {}
        for fact in facts:
            if fact["status"] == "pending":
                continue
            position = fact["intent"]["position"]
            if position["source_id"] != self.source["source_id"] or position["next_ordinal"] < self.inputs:
                raise RecoveryContractError("Stream source progress differs")
            self.inputs = position["next_ordinal"]
            self.sequence = fact["sequence"]
            data = fact["receipts"]["output"]["data"]
            if self.has_objects:data=data['rows']
            self.rows += data["rows"]
            self.blocks.extend(str(self.run.directory / b["path"]) for b in data["blocks"])
            restored = {key: StateArtifacts(self.run).read(ref)
                        for key, ref in fact["prepared"]["state"].items()}
        self.state = OperatorStates(self.run, restored)
        self.ordinal = 0
        self.complete = action.record["result"] is not None
        self.append_lock = asyncio.Lock()
        self.direct_owners = []
        self.whole_source = False
        if self.complete and self.inputs != self.source["data"]["rows"]:
            raise RecoveryContractError("Completed materialization has an incomplete source")

    def source_rows(self):
        return self.source_sink.iter_rows(self.source["data"])

    def select(self, row):
        key = self.ordinal
        self.ordinal += 1
        return None if key < self.inputs else key

    def bind_stages(self, stages):
        from .stream_map import StreamMapRuntime
        for index, stage in enumerate(stages):
            key = "node-" + str(index)
            if (stage.grouping is not None or stage.prefix is not None or
                    stage.max_batch is not None and
                    (stage.concurrency != 1 or stage.flush_interval is not None)):
                raise RecoveryContractError("Buffered native stage needs its P3 state adapter before partial commits")
            if stage.max_batch is not None:
                # A fixed-count batch may straddle any source checkpoint,
                # especially after flat_map/filter. Keep its ordinary grouping
                # and commit this materialization only after complete drain.
                # BlockSink bounds staging; no pending rows become JSON state
                # and no source ACK can overtake an uncommitted partial batch.
                self.whole_source = True
            if isinstance(stage.fn, StreamMapRuntime):
                self.state.validate(stage.fn.operation.callable.target)
                stage.fn.recovery_state = self.state
                stage.fn.recovery_key = key
            else:
                owner = owner_of(stage.fn)
                self.state.validate(owner)
                self.direct_owners.append((owner, key))
            for number, predicate in enumerate(stage.pre_filters + stage.post_filters):
                owner = owner_of(predicate)
                self.state.validate(owner)
                self.direct_owners.append((owner, key + "-filter-" + str(number)))
        if self.whole_source and (self.inputs not in (0, self.source["data"]["rows"])
                                  or self.sequence > 1):
            raise RecoveryContractError("Buffered materialization has partial source progress")

    async def restore_stages(self):
        for owner, key in self.direct_owners:
            await self.state.register(owner, key)

    async def append(self, row):
        async with self.append_lock:
            if self.rows + self.row_sink.rows >= self.run.limits["max_output_rows"]:
                raise RecoveryContractError("Materialization output row budget exceeded")
            await asyncio.to_thread(self.sink.append, row)

    async def confirm(self, keys):
        if not keys or keys != list(range(self.inputs, self.inputs + len(keys))) or len(keys) > BATCH_ROWS:
            raise RecoveryContractError("Stream occurrence range is not contiguous")
        await self._commit(self.inputs + len(keys))

    async def confirm_source(self):
        """Fixed-count batches have one bounded, fully drained sink commit."""
        total = self.source["data"]["rows"]
        if not self.whole_source or self.ordinal != total:
            raise RecoveryContractError("Buffered source did not drain completely")
        if self.inputs == total and self.sequence:
            return  # A prepared/committed terminal was recovered, not replayed.
        if self.inputs:
            raise RecoveryContractError("Buffered materialization has partial source progress")
        await self._commit(total)

    async def _commit(self, next_ordinal):
        states = await self.state.snapshot()
        fact = await asyncio.to_thread(self.coordinator.commit, self.sequence + 1,
            outputs={"output": (self.sink, None)},
            position=SourcePosition(self.source["source_id"], next_ordinal),
            states=states)
        data = fact["receipts"]["output"]["data"]
        if self.has_objects:data=data['rows']
        self.inputs = next_ordinal
        self.rows += data["rows"]
        self.sequence += 1
        self.blocks.extend(str(self.run.directory / b["path"]) for b in data["blocks"])
        self.sink.reset()

    def finish(self):
        if self.inputs != self.source["data"]["rows"] or self.row_sink.rows:
            raise RecoveryContractError("Unconfirmed source or output at stream completion")
        self.action.complete(dict(kind="materialized", rows=self.rows,
                                  inputs=self.inputs, commits=self.sequence))
        self.complete = True
