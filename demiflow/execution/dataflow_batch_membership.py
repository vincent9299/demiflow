"""Original-journal native batch boundaries; no effect result or retry authority.

Timing chooses a fresh batch once. Its record IDs and payload digests are bound
before invoking native effects, so a new attempt can replay the same boundary
without repeating confirmed model requests or regrouping a published write.
"""
import hashlib
from .contracts.dataflow import NativeJournalCommand, NativeJournalReply
from .contracts.dataflow_errors import DataflowError
from .recovery_contract import document, identity, identifier

SCHEMA='demiflow.native_batch_membership.v1'
KINDS=frozenset({'embedding','EmbeddingMapOp','SaveLanceOp'})


def bind_contracts(service, graph, physical):
    service._batch_nodes={node.occurrence_id:{
        'max_batch':int(node.kernel.configuration.get('batch_size') or node.kernel.configuration.get('max_batch')
                        or node.kernel.configuration.get('default_batch_size') or 256),
        'partitions':tuple(stage.partitions)}
        for node in graph.nodes if node.kernel.kind in KINDS
        for stage in physical.stages if node.occurrence_id in stage.occurrences}


def member(record, bounds):
    from .dataflow_record_frames import encode_document
    raw=encode_document(record,bounds)
    return [record.record_id,hashlib.sha256(raw).hexdigest()]


class NativeBatchMembership:
    def __init__(self,services,node,partition):
        self.services,self.node,self.partition=services,node,partition
        self._cached=(-1,None)

    def _request(self,ordinal,members=None):
        from ..operator_llm.journal import request_key
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
        from .contracts.dataflow_codec import _thaw_json
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


def request(service,command,record):
    declared=getattr(service,'_batch_nodes',{}).get(command.node_id)
    value=dict(record)
    if (declared is None or command.options or command.reservation is not None or command.value_ref is not None
            or set(value)!={'schema','partition','ordinal','members'} or value['schema']!=SCHEMA
            or value['partition'] not in declared['partitions']
            or type(value['ordinal']) is not int
            or not 0<=value['ordinal']<service._action.context.limits['max_commits']):
        raise DataflowError('INVALID_CONTRACT','Batch membership differs from its sealed native owner')
    members=value['members']
    operation=identity([SCHEMA,command.action_id,command.node_id,value['partition'],value['ordinal'],
                        'read' if members is None else 'bind'])
    if operation!=command.operation_id:
        raise DataflowError('INVALID_CONTRACT','Batch membership operation identity differs')
    key=identity([SCHEMA,command.action_id,command.node_id,value['partition'],value['ordinal']])
    journal=service._journal
    existing=journal.source_binding(key)
    if members is None:
        return NativeJournalReply(command.operation_id,existing['members'] if existing else None,None)
    if type(members) is not list or not 1<=len(members)<=declared['max_batch']:
        raise DataflowError('INVALID_CONTRACT','Native batch membership count exceeds its declaration')
    for item in members:
        if type(item) is not list or len(item)!=2:
            raise DataflowError('INVALID_CONTRACT','Native batch member lacks its payload identity')
        identifier(item[0]);identifier(item[1])
        if len(item[1])!=64:raise DataflowError('INVALID_CONTRACT','Native batch member digest is invalid')
    fact={'schema':SCHEMA,'action':command.action_id,'node':command.node_id,**value}
    raw=document(fact,service._bounds.max_control_bytes).decode()
    limits=service._action.context.limits
    with journal.execution_guard(service._fence) as db:
        old=db.execute('SELECT document FROM runtime_sources WHERE action_id=?',(key,)).fetchone()
        if old is not None and old[0]!=raw:
            raise DataflowError('INCOMPATIBLE_HISTORY','Native batch changed original record membership or payload')
        journal.reserve_budgets_in_transaction(db,key,{
            'native_batch_count':(1,limits['max_commits']),
            'native_batch_records':(len(members),limits['max_output_rows']*service._bounds.max_nodes),
            'native_batch_metadata':(len(raw.encode()),limits['max_control_bytes'])})
        db.execute('INSERT OR IGNORE INTO runtime_sources VALUES (?,?)',(key,raw))
    return NativeJournalReply(command.operation_id,members,None)
