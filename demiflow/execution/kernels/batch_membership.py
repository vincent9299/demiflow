"""Original-journal native batch boundaries; no effect result or retry authority.

Timing chooses a fresh batch once. Its record IDs and payload digests are bound
before invoking native effects, so a new attempt can replay the same boundary
without repeating confirmed model requests or regrouping a published write.
"""
import hashlib
from demiflow.execution.contracts.dataflow_kinds import NATIVE_BATCH_SCHEMA as SCHEMA
from demiflow.execution.contracts.dataflow import NativeJournalCommand
from demiflow.execution.contracts.dataflow_errors import DataflowError

from demiflow.execution.contracts.recovery_contract import document, identity


def member(record, bounds):
    from demiflow.execution.contracts.record_codec import encode_document
    raw=encode_document(record,bounds)
    return [record.record_id,hashlib.sha256(raw).hexdigest()]


def select_replay_batch(records, expected, bounds):
    """Restore the original effect order without weakening payload/multiplicity.

    Concurrent upstream calls may finish in a different order on replay. Keep
    their original records in the kernel's bounded pending batch until every
    recorded member is present; later-batch rows stay pending. No new journal.
    """
    from collections import defaultdict, deque
    positions = defaultdict(deque)
    for index, (record_id, _) in enumerate(expected):
        positions[record_id].append(index)
    selected = [None] * len(expected)
    for index, record in enumerate(records):
        slots = positions.get(record.record_id)
        if slots:
            slot = slots.popleft()
            if member(record, bounds) != expected[slot]:
                raise DataflowError('INCOMPATIBLE_HISTORY',
                    'Native replay batch changed original record membership or payload')
            selected[slot] = index
    if any(index is None for index in selected):
        return None
    used = set(selected)
    return tuple(records[index] for index in selected), [r for i, r in enumerate(records) if i not in used]


class NativeBatchMembership:
    def __init__(self,services,node,partition):
        self.services,self.node,self.partition=services,node,partition
        self._cached=(-1,None)

    def _request(self,ordinal,members=None):
        from demiflow.operator_llm.journal import request_key
        services=self.services
        request={'schema':SCHEMA,'partition':self.partition,'ordinal':ordinal,'members':members}
        raw=document(request,services.bounds.max_control_bytes)
        ref=services.store.put(iter([raw]),max_bytes=len(raw),media_type='application/json')
        op=identity([SCHEMA,services.assignment.action_id,self.node,self.partition,ordinal,
                     'read' if members is None else 'bind'])
        reply=services.journal.native_request(NativeJournalCommand(op,'batch_membership',
            services.assignment.action_id,self.node,request_key(request),ref,None,None,{}))
        if reply.operation_id!=op or reply.value_ref is not None:
            raise DataflowError('INTERNAL_PROTOCOL','Batch membership returned an unrelated fact')
        from demiflow.execution.contracts.dataflow_codec import _thaw_json
        return _thaw_json(reply.value)

    def expected(self,ordinal):
        if self._cached[0]!=ordinal:
            self._cached=(ordinal,self._request(ordinal))
        return self._cached[1]

    def bind(self,ordinal,records):
        value=[member(record,self.services.bounds) for record in records]
        bound=self._request(ordinal,value)
        if bound!=value:
            raise DataflowError('INCOMPATIBLE_HISTORY','Native batch membership differs')
