"""Worker-side native acquisition call; transactions stay in the driver service."""
import json
from dataclasses import asdict, dataclass

from .contracts.dataflow import QueueBinding, QueueCommand, ArtifactRef
from .contracts.dataflow_errors import DataflowError
from .recovery_contract import identity
from .row_scope import current_row_scope


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
        from .dataflow_queue import queue_service_request
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
        row_ref = put(request, services.bounds.max_row_bytes)
        command = QueueCommand(identity([self.binding.action_id, record_id, self.operation]),
            self.operation, self.binding, record_id, delivery, row_ref,
            node_id=getattr(self, 'search_occurrence', getattr(self, 'native_occurrence', None)))
        reply = queue_service_request(command, services=services)
        if reply.status != 'confirmed' or reply.row_ref is None or reply.native_fact_ref is None:
            if self.binding.mode in ('sink', 'explicit'):
                from .queue_contract import QueueIncomplete
                raise QueueIncomplete('Native task acquisition/settlement remains unconfirmed')
            raise DataflowError('UNKNOWN_EFFECT', 'Native acquisition/settlement was not confirmed')
        result = json.loads(b''.join(services.store.read(reply.row_ref,
            max_bytes=services.bounds.max_row_bytes)))
        if self.operation == 'finalize' and not self.keep_row:
            fact = json.loads(b''.join(services.store.read(reply.native_fact_ref,
                              max_bytes=services.bounds.max_control_bytes)))
            return {'queue_completion': {'task_id': request['_queue_delivery']['task_id'],
                    'final_commit': fact['commit_id'], 'status': 'acked'}}
        return result
