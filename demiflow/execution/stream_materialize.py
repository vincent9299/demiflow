"""Existing stream materialization boundary on the single run commit protocol.

The model has no storage role. The driver captures a fixed source, the terminal
sink prepares/publishes blocks, then RunJournal confirms progress and actor state.
"""
import asyncio
from contextvars import copy_context
from functools import partial

from .block_sink import BlockSink
from .commit import CommitCoordinator
from .operator_state import OperatorStates, owner_of
from .recovery_contract import RecoveryContractError, SourcePosition
from .source_snapshot import capture_source
from .state_artifacts import StateArtifacts


class StreamStageProgress:
    def source_rows(self):
        return self.source_sink.iter_rows(self.source["data"])

    def select(self, row):
        key = self.ordinal
        self.ordinal += 1
        return None if key < self.inputs else key

    def bind_stages(self, stages):
        from .stream_map import StreamMapRuntime
        import inspect
        self.stages = stages
        ownership = {}

        for index, stage in enumerate(stages):
            key = "node-" + str(index)
            # Native buffer kinds are carried by a per-stage StageBufferAdapter
            # (C05). A kind is opened only once its crash-recovery counterexample
            # passes; anything else keeps the honest fail-closed rejection rather
            # than faking partial-commit recovery. native_row_local stays in the
            # separate native_pending registry below, never in the buffer snapshot.
            from .stage_buffers import classify_stage_buffer, make_stage_buffer_adapter, PROVEN_KINDS
            kind = classify_stage_buffer(stage)
            if kind is not None:
                if kind not in PROVEN_KINDS:
                    raise RecoveryContractError("Buffered native stage needs its P3 state adapter before partial commits")
                restored_buffer = self.state.restored.get(key + '-buffer')
                stage.buffer_adapter = make_stage_buffer_adapter(
                    stage, action=self.action, stage_key=key, restored=restored_buffer)
                stage.scope_id = key
            else:
                stage.buffer_adapter = None
            if isinstance(stage.fn, StreamMapRuntime):
                target = stage.fn.operation.callable.target
                self.state.validate(target)
                stage.fn.recovery_state = self.state
                stage.fn.recovery_key = key
            else:
                target = owner_of(stage.fn)
                if isinstance(target, partial):
                    # A streaming group node binds its pure pack_group helper with
                    # functools.partial; the semantic owner is the function itself,
                    # which carries no mutable state to snapshot or restore.
                    target = target.func
                self.state.validate(target)
                self.direct_owners.append((target, key))
            targets = [target]
            for number, predicate in enumerate(stage.pre_filters + stage.post_filters):
                owner = owner_of(predicate)
                self.state.validate(owner)
                self.direct_owners.append((owner, key + "-filter-" + str(number)))
                targets.append(owner)
            for owner in targets:
                if not (inspect.isfunction(owner) or inspect.isbuiltin(owner) or inspect.isclass(owner)):
                    ownership.setdefault(id(owner), []).append(index)
            capability = getattr(target, 'native_stream_recovery', None)
            if capability is not None:
                stage.native_recovery = capability(self.action, key)
                # Effect replay and worker-buffer alignment are separate. A batch
                # actor borrows the effect journal, but every batch worker must
                # drain its request and contribute its remainder to the cut.
                stage.native_snapshot = stage.buffer_adapter is None and stage.native_recovery is not None and getattr(stage.native_recovery, 'row_local', True) and all(
                    inspect.isfunction(owner_of(predicate)) or inspect.isbuiltin(owner_of(predicate))
                    for predicate in stage.pre_filters + stage.post_filters)
                for number, owner in enumerate(getattr(stage.native_recovery, 'semantic_owners', ())):
                    owner = owner_of(owner)
                    self.state.validate(owner)
                    self.direct_owners.append((owner, key + '-native-owner-' + str(number)))
                    ownership.setdefault(id(owner), []).append(index)
                restored = self.state.restored.get(key + '-inflight')
                if stage.native_snapshot and restored is not None:
                    from .row_scope import ScopedRow, RowScope
                    if restored['data']['rows'] > stage.concurrency or len(restored['occurrences']) != restored['data']['rows']:
                        raise RecoveryContractError('Native in-flight state exceeds worker capacity')
                    reader = BlockSink(self.action, key + '-inflight', staging_only=True)
                    for value, occurrence in zip(reader.iter_rows(restored['data']), restored['occurrences'], strict=True):
                        stage.native_restore.append(ScopedRow(value, RowScope(occurrence)))
        # Only shared semantic actor state extends an alignment group.
        intervals = [indices for indices in ownership.values() if len(indices) > 1]
        self.group_ends = list(range(len(stages)))
        for indices in intervals:
            start, end = min(indices), max(indices)
            for index in range(start, end + 1):
                self.group_ends[index] = max(self.group_ends[index], end)
        for index in reversed(range(len(stages))):
            self.group_ends[index] = max(self.group_ends[index:self.group_ends[index] + 1])
        self.object_cut = len(stages) - 1

    async def restore_stages(self):
        for owner, key in self.direct_owners:
            # Includes predicates folded into a concurrent upstream stage.
            index = int(key.split('-')[1])
            await self.state.register(owner, key, concurrency=self.stages[index].concurrency)

    async def append(self, row):
        if self.stop_requested():
            raise RecoveryContractError("Terminal preparation stopped")
        async with self.append_slots:
            from ..data.runtime_contract import check_size
            size = check_size(row, self.run.limits['max_row_bytes'])
            if self.admitted_rows >= self.run.limits["max_output_rows"]:
                raise RecoveryContractError("Materialization output row budget exceeded")
            self.admitted_rows += 1
            request = [row, size, False, None]
            key = id(request)
            self.pending_appends[key] = request
            try:
                async with self.append_lock:
                    if not request[2]:
                        # Coalesce only already waiting callers. Each still owns
                        # its row until serialization finishes; no lookahead,
                        # timer or second writer is introduced. Both the queue
                        # and one blocking submission have finite bounds.
                        batch, used = [], 0
                        ceiling = max(self.run.limits['max_batch_bytes'], self.run.limits['max_row_bytes'])
                        while self.pending_appends and len(batch) < 64:
                            first = next(iter(self.pending_appends))
                            item = self.pending_appends[first]
                            if batch and used + item[1] > ceiling:
                                break
                            batch.append(self.pending_appends.pop(first))
                            used += item[1]
                        def append_batch():
                            for item in batch:
                                self.sink.append(item[0])
                        try:
                            await self._io(append_batch)
                        except BaseException as error:
                            for item in batch:
                                item[2], item[3] = True, error
                            raise
                        for item in batch:
                            item[2] = True
                    if request[3] is not None:
                        raise request[3]
            finally:
                self.pending_appends.pop(key, None)

    @staticmethod
    async def _io(fn, *args, **kwargs):
        # A cancelled task must join its started writer before cleanup/lease exit.
        # Submit the same context-bound call without an extra asyncio Task and
        # scheduling turn per row. Shield/drain the executor future itself.
        task = asyncio.get_running_loop().run_in_executor(
            None, copy_context().run, partial(fn, *args, **kwargs))
        try:
            return await asyncio.shield(task)
        except asyncio.CancelledError:
            while not task.done():
                try:
                    await asyncio.shield(task)
                except asyncio.CancelledError:
                    continue
            task.result()
            raise

    async def align(self, index, barrier, batch):
        stage = self.stages[index]
        if stage.native_snapshot:
            from ..data.runtime_contract import check_size
            from .row_scope import ScopedRow
            sink = BlockSink(self.action, 'node-' + str(index) + '-inflight', staging_only=True)
            barrier.snapshot_bytes += 1024 + 512 * len(stage.native_pending)
            if barrier.snapshot_bytes > self.run.limits['max_state_bytes']:
                raise RecoveryContractError('Aggregate native snapshot metadata exceeds state budget')
            pending = []
            for value in stage.native_pending.values():
                if type(value) is not ScopedRow or value.scope.delivery is not None:
                    raise RecoveryContractError('Native in-flight state needs a row-local occurrence')
                amount = check_size(value.row, self.run.limits['max_row_bytes']) + 128
                barrier.snapshot_bytes += amount
                if barrier.snapshot_bytes > self.run.limits['max_state_bytes']:
                    raise RecoveryContractError('Aggregate in-flight snapshot exceeds state budget')
                pending.append(value)
            def freeze_pending():
                for value in pending:
                    sink.append(value.row)
                sink._flush()
                return dict(data=sink.descriptor(), occurrences=[value.scope.occurrence for value in pending])
            barrier.states['node-' + str(index) + '-inflight'] = await self._io(freeze_pending)
        adapter = getattr(stage, 'buffer_adapter', None)
        if adapter is not None:
            # ``batch`` is the barrier's per-slot pending mapping for buffered
            # kinds. The adapter freezes every active slot's remainder into the
            # one shared barrier budget; it never publishes downstream itself.
            await adapter.align(barrier, batch)
        if self.group_ends[index] == index:
            prefixes = ['node-' + str(i) for i, end in enumerate(self.group_ends) if end == index]
            barrier.states.update(await self.state.snapshot(prefixes, budget=barrier))
        if self.has_objects and index == self.object_cut:
            barrier.objects = {root: store.detach() for root, store in self.sink.object_sinks.items()}


class StreamMaterialization(StreamStageProgress):
    def __init__(self, action, rows_factory, *, object_specs=None, staging_only=False):
        self.action, self.run = action, action.context
        self.source_sink, self.source = capture_source(action, rows_factory)
        self.sink = BlockSink(action)
        self.row_sink=self.sink
        self.has_objects=bool(object_specs)
        self.batch_rows=self.run.limits["checkpoint_rows"]
        if object_specs:
            from .object_sink import ObjectRowsSink
            self.sink=ObjectRowsSink(action,object_specs=object_specs)
            self.row_sink=self.sink.rows
            action.object_sinks=self.sink.object_sinks
        self.row_sink.staging_only = staging_only
        self.on_checkpoint = None
        self.stop_requested = lambda: False
        self.coordinator = CommitCoordinator(action)
        latest = self.run.journal.latest_execution_commit(action.action_id)
        self.pending_cut = latest['intent']['position']['next_ordinal'] if latest and latest['status'] == 'pending' else None
        if latest and latest['status'] == 'pending':
            latest = self.run.journal.latest_execution_commit(action.action_id, confirmed=True)
        if latest:
            self.coordinator._finish(latest, {'output': self.sink}, None)
            latest = self.run.journal.execution_commit(latest['commit_id'])
        facts = [latest] if latest else []
        self.inputs = self.rows = self.sequence = 0
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
            restored = {key: StateArtifacts(self.run).read(ref)
                        for key, ref in fact["prepared"]["state"].items()}
        if latest:
            summary = restored.get('stream-progress')
            if summary is None:
                raise RecoveryContractError('Stream checkpoint has no protocol progress summary')
            self.rows = summary['rows']
        self.state = OperatorStates(self.run, restored, runtime_only_action=action)
        self.ordinal = 0
        self.complete = action.record["result"] is not None
        self.append_lock = asyncio.Lock()
        self.append_slots = asyncio.Semaphore(64)
        self.pending_appends = {}
        self.direct_owners = []
        self.group_ends = []
        self.buffer_sinks = {}
        self.admitted_rows = self.rows
        if self.complete and self.inputs != self.source["data"]["rows"]:
            raise RecoveryContractError("Completed materialization has an incomplete source")

    def seal(self, barrier):
        from copy import copy
        if barrier.final and barrier.next_ordinal != self.source['data']['rows']:
            raise RecoveryContractError('Final barrier has an incomplete source')
        # Terminal workers have all arrived. Transfer buffers before any resumes;
        # the committing participant never sees subsequent append/reset calls.
        if self.has_objects:
            sealed = copy(self.sink)
            sealed.rows = self.row_sink.detach()
            sealed.object_sinks = barrier.objects
            sealed.objects = next(iter(sealed.object_sinks.values()))
        else:
            sealed = self.sink.detach()
        barrier.states['stream-progress'] = dict(rows=self.rows + sealed.rows.rows if self.has_objects else self.rows + sealed.rows,
                                                final=barrier.final, inputs=barrier.next_ordinal)
        return lambda: self._commit(barrier.next_ordinal, sealed, barrier.states)

    async def _commit(self, next_ordinal, sealed, states):
        fact = await self._io(self.coordinator.commit, self.sequence + 1,
            outputs={"output": (sealed, None)},
            position=SourcePosition(self.source["source_id"], next_ordinal),
            states=states)
        data = fact["receipts"]["output"]["data"]
        if self.has_objects:data=data['rows']
        self.inputs = next_ordinal
        self.rows += data["rows"]
        self.sequence += 1
        if self.on_checkpoint is not None:
            await self._io(self.on_checkpoint, fact['receipts']['output']['data'])


    def finish(self):
        if self.inputs != self.source["data"]["rows"] or self.row_sink.rows:
            raise RecoveryContractError("Unconfirmed source or output at stream completion")
        self.action.complete(dict(kind="materialized", rows=self.rows,
                                  inputs=self.inputs, commits=self.sequence))
        self.complete = True
