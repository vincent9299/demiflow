"""Native streaming Lance publications owned by the original driver journal.

Workers submit immutable batch artifacts. Each invocation is a bounded native
commit scope of its parent Dataset action, using the existing CommitCoordinator
and LanceSink publication marker. The optional SQLite key index is disposable
scratch, never recovery authority. Earlier confirmed microbatches stay visible
if a later batch fails; this is not whole-action atomic publication.
"""
from dataclasses import asdict





from demiflow.execution.contracts.dataflow import NativeJournalCommand
from demiflow.execution.contracts.dataflow_errors import DataflowError
from demiflow.execution.contracts.recovery_contract import identity, document
from demiflow.execution.native_queue.stream_lance import StreamLanceWriter






from demiflow.execution.contracts.dataflow import NATIVE_STREAM_LANCE_SCHEMA as SCHEMA


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
        from demiflow.execution.contracts import state_codec
        from demiflow.operator_llm.journal import request_key
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
        from demiflow.execution.contracts.dataflow_codec import _thaw_json
        value = _thaw_json(reply.value)
        if value.get('status') != 'acked':
            raise DataflowError('UNKNOWN_EFFECT', 'Stream batch has no original ACKed receipt')
        return value['reference']

    def __call__(self, rows):
        from demiflow.execution.worker.row_scope import current_row_scope
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
    from demiflow.execution.native_queue.stream_lance import StreamLanceSink
    from demiflow.data.plan import SaveLanceOp
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
    from demiflow.execution.contracts.record_schema import encode_schema
    from demiflow.planning.dataflow_policy import PREFLIGHT_BOUNDS as _PREFLIGHT_BOUNDS
    value['schema'] = encode_schema(actor.schema,bounds=_PREFLIGHT_BOUNDS).hex()
    value['max_batch'] = operation.max_batch
    return value
