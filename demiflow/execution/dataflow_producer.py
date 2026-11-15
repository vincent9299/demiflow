"""Native queue producer units inside the unified worker graph.

One fixed-source unit is admitted at a time. Real workers execute its transforms;
all participants drain before the native unit-end and Dataset cut. Native publish
receipts remain the sole task authority. A same-run uncommitted unit is replayed
against its original receipts; a new run starts at the native source cursor.
"""
from dataclasses import asdict, replace

from .contracts.dataflow import QueueBinding, QueueReply, SourceSplit, thaw_record_value
from .contracts.dataflow_errors import DataflowError
from .dataflow_queue import NativeAcquisitionService
from .dataflow_acquisition_call import AcquisitionCall
from .recovery_contract import identity
from .row_scope import RowScope, set_row_scope
from .run_context import _ACTION


class ProducerCall(AcquisitionCall):
    def __init__(self):
        super().__init__('acquire')

    def bind(self, services, occurrence_id):
        super().bind(services, occurrence_id)
        if self.binding.mode != 'producer':
            raise DataflowError('FENCED', 'Enqueue worker is not bound to a producer')




class NativeProducerService(NativeAcquisitionService):
    def configure(self, source, plan, sink, *, graph):
        from .queue_producer import producer_actor
        found = producer_actor(plan)
        if found is None or len(graph.sources) != 1 or sink is not None:
            raise DataflowError('INVALID_CONTRACT', 'Native producer requires one fixed source and one enqueue tail')
        self._fetch = found[1]
        self._fetch.configure(self._action, source, plan)
        nodes = [node for node in graph.nodes if node.kernel.configuration.get('native_operation') == 'producer']
        if len(nodes) != 1:
            raise DataflowError('INVALID_CONTRACT', 'Producer has no unique sealed worker occurrence')
        producer = self._fetch.producer
        self._generation = producer.generation
        self._binding = QueueBinding(self.action_id, graph.sources[0], 'producer',
            self._fetch.contract, producer.generation, producer.pool,
            graph.sources[0], nodes[0].occurrence_id, None)
        key = self.action_id + '-producer-binding'
        self._prior_binding = self._action.context.journal.source_binding(key)
        self._action.context.journal.bind_source(self._action.context.fence, key, asdict(self._binding))
        self._mode = 'producer'
        self._unit = None
        self._source_eoi = False
        self._restore_image = None
        self._rows = None
        self._state = 'configured'
        return self._binding

    def source_layout(self, snapshot):
        from .contracts.dataflow_codec import _thaw_json
        wrapped = replace(snapshot, kind='producer', replay='seek', finite=True,
            immutable_identity={'original_kind': snapshot.kind,
                'original_identity': _thaw_json(snapshot.immutable_identity),
                'generation': self._binding.generation})
        locator = {'kind': 'producer', 'generation': self._binding.generation}
        split = SourceSplit(identity([snapshot.source_id, locator]), snapshot.source_id,
                            locator, {}, None, self._services.policy.store_id)
        return wrapped, (split,)

    def can_recover_without_dataflow_cut(self):
        return bool(self._action.context.resuming and self._prior_binding == asdict(self._binding)
                    and not self._action.context.journal.execution_commits(self.action_id))

    def start(self):
        super().start()  # native astart reconciles native pending publish receipts first
        if self._action.context.resuming:
            cursors = self._restore_image.source_vector if self._restore_image is not None else ()
            if len(cursors) > 1:
                raise DataflowError('INCOMPATIBLE_HISTORY', 'Producer checkpoint has multiple source owners')
            offset = thaw_record_value(cursors[0].offset) if cursors else {}
            position = offset.get('producer_position', 0)
            if type(position) is not int or position < 0 or position > self._fetch.position:
                raise DataflowError('INCOMPATIBLE_HISTORY', 'Dataset producer frontier is ahead of native progress')
            # The native unit may have committed just before the Dataset cut was
            # lost. Replay that unit with stable native receipts, never skip it.
            self._fetch.position = position
            self._fetch.offset = 0
            self._fetch.cursor = offset.get('producer_cursor')
            self._fetch.eos = False
        self._rows = iter(self._fetch.rows())

    def poll(self, command, *, lease):
        self._check_started()
        self._check_lease(lease)
        self._check_role(lease, 'poll')
        self._validate_command(command, 'poll', require_refs=False)
        self._validate_binding(command.binding)
        return self._run_operation(command, lambda: self._poll_unit(command))

    def _poll_unit(self, command):
        if self._unit is not None:
            return QueueReply(command.operation_id, 'pending', None, None, None, 5)
        if self._source_eoi:
            return QueueReply(command.operation_id, 'eoi', None, None, None, 0)
        scoped = self._run_io(self._fetch.io.run(next, self._rows, None),
                              timeout_s=self._services.bounds.timeout_ms / 1000)
        if scoped is None:
            self._source_eoi = True
            return QueueReply(command.operation_id, 'eoi', None, None, None, 0)
        self._fetch.begin_unit()
        descriptor = {'generation': self._binding.generation, 'position': self._fetch.position,
                      'occurrence': scoped.scope.occurrence, 'next_cursor': self._fetch.next_cursor}
        self._unit = descriptor
        delivery = self._put_json(descriptor, media_type='application/json',
                                 max_bytes=self._services.bounds.max_control_bytes)
        row = self._put_json(scoped.row, media_type='application/json',
                            max_bytes=self._services.bounds.max_row_bytes)
        return QueueReply(command.operation_id, 'record', delivery, row, None, 0)

    def acquire(self, command, *, lease):
        self._check_started()
        self._check_lease(lease)
        self._check_role(lease, 'acquire')
        self._validate_command(command, 'acquire', require_refs=True)
        self._validate_binding(command.binding)
        return self._run_operation(command, lambda: self._publish_row(command))

    def _publish_row(self, command):
        result = self._call_native(command, self._fetch)
        from .queue_commit import QueueCommitStore
        with self._fetch.channel.database.connection() as db:
            proofs = [QueueCommitStore._fact(db, cid) for cid in self._fetch.last_call_commits]
        return self._confirmed(command, result, proofs)

    def _call_native(self, command, actor):
        descriptor = self._read_json(command.delivery_ref, max_bytes=self._services.bounds.max_control_bytes)
        if self._unit is None or descriptor != self._unit:
            raise DataflowError('FENCED', 'Publication does not belong to the active source unit')
        row = self._read_json(command.row_ref, max_bytes=self._services.bounds.max_row_bytes)
        async def publish():
            token = _ACTION.set(self._action)
            set_row_scope(RowScope(command.record_id))
            try:
                return await actor(row)
            finally:
                set_row_scope(None)
                _ACTION.reset(token)
        self._admit_io()
        try:
            result = self._run_io(publish(), timeout_s=self._services.bounds.timeout_ms / 1000)
        finally:
            self._release_io()
        return result




    def before_cut(self):
        if self._unit is not None:
            self._run_io(self._fetch.end_unit(), timeout_s=self._services.bounds.timeout_ms / 1000)
            self._unit = None

    def finish(self):
        if not self._source_eoi or self._unit is not None:
            raise DataflowError('INVALID_CONTRACT', 'Producer EOS requires drained native source units')
        self._run_io(self._fetch.end_source(), timeout_s=self._services.bounds.timeout_ms / 1000)
        return {'source_position': self._fetch.position, 'eos': True}

    def close(self):
        try:
            super().close()
        finally:
            if getattr(self, '_rows', None) is not None:
                self._rows.close()
