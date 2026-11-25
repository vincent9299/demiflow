"""Worker-side native acquisition call; transactions stay in the driver service."""

from demiflow.execution.contracts.dataflow import QueueReply, RuntimeServices
import json
from dataclasses import asdict, dataclass

from demiflow.execution.contracts.dataflow import QueueBinding, QueueCommand, ArtifactRef
from demiflow.execution.contracts.dataflow_errors import DataflowError
from demiflow.execution.contracts.record_codec import RECORD_MEDIA_TYPE, dumps_row, loads_stored_row
from demiflow.execution.contracts.recovery_contract import identity
from demiflow.execution.worker.row_scope import current_row_scope


@dataclass(frozen=True)
class QueueSourceDeclaration:
    """Sealed source facts only; workers cannot claim through this object."""
    facts: dict

    def snapshot_identity(self):
        return self.facts

    def recovery_identity(self):
        return self.facts


class AcquisitionCall:
    def __init__(self, operation='acquire', keep_row=True):
        self.operation = operation
        self.keep_row = keep_row
        self.services = None

    def bind(self, services, occurrence_id):
        self.services = services
        facts = services.journal.read_configuration()
        self.binding = QueueBinding(**dict(facts['queue_binding']))
        if self.operation == 'acquire' and self.binding.mode == 'raw_fetch' and facts.get('fetch_occurrences'):
            if occurrence_id not in facts['fetch_occurrences']:
                raise DataflowError('FENCED', 'Fetch call differs from the sealed occurrences')
            self.native_occurrence = occurrence_id
            return
        assigned = self.binding.fetch_occurrence if self.operation == 'acquire' else self.binding.final_occurrence
        if assigned != occurrence_id:
            raise DataflowError('FENCED', 'Native acquisition call differs from the sealed occurrence')

    def snapshot_state(self):
        return {}

    def restore_state(self, state):
        if state != {}:
            raise DataflowError('INCOMPATIBLE_HISTORY', 'Native acquisition state belongs to the native journal')

    def __call__(self, row):

        services = self.services
        scope = current_row_scope()
        if scope is None:
            raise DataflowError('INVALID_CONTRACT', 'Acquisition needs a native invocation scope')
        def put(value, maximum):
            raw = json.dumps(value, sort_keys=True, separators=(',', ':'), allow_nan=False).encode()
            return services.store.put(iter([raw]), max_bytes=maximum, media_type='application/json')
        record_id = scope.occurrence
        if self.binding.mode in ('raw_fetch', 'native_search'):
            delivery = put({'occurrence': record_id}, services.bounds.max_control_bytes)
        else:
            delivery = scope.delivery
            if not isinstance(delivery, ArtifactRef):
                raise DataflowError('FENCED', 'Queue row lost its driver-owned delivery ref')
        request = row
        if self.operation == 'finalize':
            # The settled row is written to the queue database as JSON by the queue sink.
            row_ref = put(request, services.bounds.max_row_bytes)
        else:
            row_ref = services.store.put(iter([dumps_row(request, max_bytes=services.bounds.max_row_bytes)]),
                                         max_bytes=services.bounds.max_row_bytes, media_type=RECORD_MEDIA_TYPE)
        command = QueueCommand(identity([self.binding.action_id, record_id, self.operation]),
            self.operation, self.binding, record_id, delivery, row_ref,
            node_id=getattr(self, 'search_occurrence', getattr(self, 'native_occurrence', None)))
        reply = queue_service_request(command, services=services)
        if reply.status != 'confirmed' or reply.row_ref is None or reply.native_fact_ref is None:
            if self.binding.mode in ('sink', 'explicit'):
                from demiflow.execution.contracts.queue_contract import QueueIncomplete
                raise QueueIncomplete('Native task acquisition/settlement remains unconfirmed')
            raise DataflowError('UNKNOWN_EFFECT', 'Native acquisition/settlement was not confirmed')
        result = loads_stored_row(b''.join(services.store.read(reply.row_ref,
            max_bytes=services.bounds.max_row_bytes)), reply.row_ref.media_type)
        if self.operation == 'finalize' and not self.keep_row:
            fact = json.loads(b''.join(services.store.read(reply.native_fact_ref,
                              max_bytes=services.bounds.max_control_bytes)))
            return {'queue_completion': {'task_id': request['_queue_delivery']['task_id'],
                    'final_commit': fact['commit_id'], 'status': 'acked'}}
        return result


def queue_service_request(command: QueueCommand, *, services: RuntimeServices) -> QueueReply:
    """Worker-side typed ``queue`` RPC (Q009); refs only, no native objects.

    QueueReader/fetch/ack kernels call this single entry. It forwards to the
    driver through WorkerJournalPort.queue and never pickles a QueueDatasource,
    native database or lock, nor writes a second queue state.
    """
    if not isinstance(services, RuntimeServices):
        raise DataflowError('INVALID_CONTRACT', 'queue request requires worker RuntimeServices')
    if not isinstance(command, QueueCommand):
        raise DataflowError('INVALID_CONTRACT', 'queue request requires a QueueCommand')
    if command.operation not in {'poll', 'acquire', 'finalize', 'status', 'search'}:
        raise DataflowError('INVALID_CONTRACT', 'unknown queue operation')
    if command.operation in {'acquire', 'finalize', 'search'}:
        if command.delivery_ref is None or command.row_ref is None:
            raise DataflowError('INVALID_CONTRACT', 'acquire/finalize require delivery_ref and row_ref')
    elif command.row_ref is not None or command.delivery_ref is not None:
        raise DataflowError('INVALID_CONTRACT', 'poll/status must not carry business payload refs')
    services.check_stop()
    reply = services.journal.queue(command)
    if not isinstance(reply, QueueReply):
        raise DataflowError('INTERNAL_PROTOCOL', 'queue port returned a non-QueueReply')
    if reply.operation_id != command.operation_id:
        raise DataflowError('INTERNAL_PROTOCOL', 'queue reply operation_id differs from its request')
    if reply.status not in {'record', 'pending', 'eoi', 'confirmed', 'unknown'}:
        raise DataflowError('INTERNAL_PROTOCOL', 'queue reply has an unknown status')
    services.check_stop()
    return reply


class ProducerCall(AcquisitionCall):
    def __init__(self):
        super().__init__('acquire')

    def bind(self, services, occurrence_id):
        super().bind(services, occurrence_id)
        if self.binding.mode != 'producer':
            raise DataflowError('FENCED', 'Enqueue worker is not bound to a producer')
