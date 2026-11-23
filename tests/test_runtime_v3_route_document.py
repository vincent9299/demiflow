"""One validated payload encoding per routed cross-worker record."""
from demiflow.execution.contracts import record_codec
from dataclasses import replace

import pytest

from demiflow.execution.contracts.dataflow import ArtifactRef,BlockPartition,Record
from demiflow.execution.contracts.dataflow_errors import DataflowError
from demiflow.execution.exchange import dataflow_bridge as bridge
from demiflow.execution.exchange import dataflow_channels as channels
from demiflow.execution.exchange.dataflow_state import RoutingCounters
from demiflow.execution.contracts.record_schema import SCHEMA_MEDIA_TYPE
from demiflow.execution.contracts.dataflow import clone_record
from demiflow.execution.recovery_contract import identity
from test_runtime_v3_c import make,bounds


def row_record(row=None,**kwargs):
    if row is None:
        from test_runtime_v3_scalar_values import payload
        row=payload()
        row.update(key=(-0.0,b'key'),target='r1',nan=float('nan'),infinity=float('inf'))
    raw=record_codec._encode_payload(row,max_bytes=65536)
    return Record(identity('record'),{'ordinal':7,'parent_record_ids':[identity('parent')],
        'nested':[{'v':1}]},None,row,len(raw),7,**kwargs)


def spec_for(routing='forward',**kwargs):
    return make('ExchangeSpec',channel_id='route',edge_id=identity('edge'),
        producer_partitions=('p0',),consumer_partitions=('r0',) if routing=='forward' else ('r0','r1','r2'),
        mode='memory_incremental',routing=routing,
        keys=('key',) if routing=='hash' else ('target',) if routing=='target' else (),
        credit_bytes=65536,**kwargs)


def state(ordinal=0):return RoutingCounters(values={'route':ordinal},route_epoch=0)


@pytest.mark.parametrize('routing',['forward','hash','rebalance','broadcast','target'])
@pytest.mark.parametrize('optional',[False,True])
def test_joint_route_matches_original_wire_identity_and_resumed_ordinal(routing,optional):
    extra={}
    if optional:
        extra={'schema_ref':ArtifactRef('b'*64,20,'local','schema',SCHEMA_MEDIA_TYPE),
               'block_partition':BlockPartition('c'*64,1,3)}
    record=row_record(**extra);spec=spec_for(routing)
    old_state,new_state=state(19),state(19)
    expected=channels.route_record(record,spec=spec,state=old_state)
    raw=record_codec._encode_record_document(record,max_bytes=65536)
    actual,encoded=channels._route_record_document(record,spec=spec,state=new_state,bounds=bounds(max_row_bytes=65536))
    assert actual==expected and encoded==raw
    assert all(r.record is record for r in actual)
    assert old_state.values()==new_state.values()=={'route':20}
    assert channels._route_record_document(record,spec=spec,state=new_state,bounds=bounds())[0] == channels.route_record(record,spec=spec,state=old_state)


@pytest.mark.parametrize('mode',['memory_incremental','durable_incremental','finite_sealed'])
def test_payload_window_and_full_document_row_budget_are_independent(mode):
    record=row_record({'value':'x'*200})
    raw=record_codec._encode_record_document(record,max_bytes=65536)
    assert record.encoded_bytes<len(raw)
    spec=replace(spec_for(),mode=mode,scope_id='finite' if mode=='finite_sealed' else None,
                 credit_bytes=record.encoded_bytes,storage_bytes=record.encoded_bytes)
    # R is the typed business row limit; provenance has its separate C bound.
    assert channels._route_record_document(record,spec=spec,state=state(),bounds=bounds(max_row_bytes=record.encoded_bytes))[1]==raw
    for limited,limit in [(replace(spec,credit_bytes=record.encoded_bytes-1,storage_bytes=record.encoded_bytes-1),record.encoded_bytes),
                           (spec,record.encoded_bytes-1)]:
        with pytest.raises(DataflowError,match='BUDGET_EXCEEDED'):
            channels._route_record_document(record,spec=limited,state=state(),bounds=bounds(max_row_bytes=limit))
    with pytest.raises(DataflowError,match='control byte budget'):
        channels._route_record_document(record,spec=spec,state=state(),
            bounds=bounds(max_row_bytes=record.encoded_bytes,max_control_bytes=64))


@pytest.mark.parametrize('fault',['parents','schema','low_size','high_size','missing_key','foreign_target','bad_forward'])
def test_bad_metadata_payload_counts_and_route_contracts_are_not_bypassed(fault):
    record=row_record({'key':1,'target':'r1'});spec=spec_for()
    if fault=='parents':record=clone_record(record,source_position={'parent_record_ids':['invalid']})
    elif fault=='schema':record=clone_record(record,schema_ref=ArtifactRef('d'*64,1,'local','s','application/octet-stream'))
    elif fault=='low_size':record=clone_record(record,encoded_bytes=record.encoded_bytes-1)
    elif fault=='high_size':record=clone_record(record,encoded_bytes=record.encoded_bytes+1)
    elif fault=='missing_key':spec=replace(spec_for('hash'),keys=('missing',))
    elif fault=='foreign_target':record=row_record({'key':1,'target':'foreign'});spec=spec_for('target')
    else:spec=replace(spec,consumer_partitions=('r0','r1'))
    old_state,new_state=state(),state()
    with pytest.raises((DataflowError,KeyError)) as old:
        channels.route_record(record,spec=spec,state=old_state)
    with pytest.raises(type(old.value)) as new:
        channels._route_record_document(record,spec=spec,state=new_state,bounds=bounds())
    if isinstance(old.value,DataflowError):assert old.value.code==new.value.code
    # The joint admission rejects malformed encoded_bytes before allocating a
    # routing ordinal. The old split helper advanced before checking payload.
    if fault in ('low_size','high_size'):
        assert new_state.values()=={'route':0}
        assert old_state.values()=={'route':1}
    else:
        assert new_state.values()==old_state.values()


def test_external_payload_keeps_original_reference_channel_window_and_document_bytes():
    ref=ArtifactRef('e'*64,8192,'local','payload','application/octet-stream')
    record=Record('f'*64,{'ordinal':0},ref,None,ref.size_bytes,0)
    raw=record_codec._encode_record_document(record,max_bytes=65536)
    spec=replace(spec_for(),credit_bytes=ref.size_bytes)
    assert channels._route_record_document(record,spec=spec,state=state(),bounds=bounds(max_row_bytes=ref.size_bytes))[1]==raw
    with pytest.raises(DataflowError,match='row byte budget'):
        channels._route_record_document(record,spec=spec,state=state(),bounds=bounds(max_row_bytes=ref.size_bytes-1))
    with pytest.raises(DataflowError,match='channel byte window'):
        channels._route_record_document(record,spec=replace(spec,credit_bytes=ref.size_bytes-1),state=state(),bounds=bounds())
    with pytest.raises(DataflowError,match='misreports'):
        channels._route_record_document(clone_record(record,encoded_bytes=ref.size_bytes-1),spec=spec,state=state(),bounds=bounds())


def test_joint_path_encodes_entire_payload_once_even_for_broadcast(monkeypatch):
    record=row_record({'value':'x'*8192});spec=spec_for('broadcast')
    count=[];encode=record_codec._encode_value
    def observe(value):
        if value is record.inline:count.append(1)
        return encode(value)
    monkeypatch.setattr(record_codec,'_encode_value',observe)
    routes=channels.route_record(record,spec=spec,state=state())
    expected=[record_codec._encode_record_document(r.record,max_bytes=65536) for r in routes]
    assert len(count)==4
    count.clear()
    actual,raw=channels._route_record_document(record,spec=spec,state=state(),bounds=bounds(max_row_bytes=65536))
    assert len(count)==1 and actual==routes
    assert all(value==raw for value in expected)
    assert type(raw) is bytes


def test_real_worker_uses_validated_document_without_changing_results_or_ack(tmp_path,monkeypatch):
    from demiflow import data
    from demiflow.execution.run_context import RunContext
    from demiflow.execution.worker.dataflow_graph_runner import WorkerGraphRunner
    publish=WorkerGraphRunner._publish
    seen=[]
    def observe(runner,record,**kwargs):
        if kwargs.get('spec') is not None:
            raw=kwargs.get('validated_raw')
            assert type(raw) is bytes
            assert raw==record_codec._encode_record_document(record,max_bytes=runner.bounds.max_row_bytes)
            seen.append(kwargs['spec'].mode)
        return publish(runner,record,**kwargs)
    monkeypatch.setattr(WorkerGraphRunner,'_publish',observe)
    async def transform(row):return {**row,'out':row['n']*2}
    with RunContext.start(tmp_path/'run') as run,data.local_execution(workers=4,worker_mode='thread'):
        result=data.from_items([{'n':i} for i in [3,1,2]]).sort('n').map_async(transform,concurrency=1).take_all()
        assert result==[{'n':i,'out':2*i} for i in [1,2,3]]
        assert seen and 'finite_sealed' in seen
        assert run.journal.execution_commits('action-1')[-1]['status']=='acked'


@pytest.mark.parametrize('routing',['forward','hash','rebalance','broadcast','target'])
def test_all_routes_validate_parent_metadata_before_encoding_or_ordinal(routing,monkeypatch):
    record=clone_record(row_record({'key':1,'target':'r1'}),source_position={'parent_record_ids':['bad']})
    calls=[];encode=channels._encode_record_document
    def observe(*args,**kwargs):calls.append(1);return encode(*args,**kwargs)
    monkeypatch.setattr(channels,'_encode_record_document',observe)
    ledger=state()
    with pytest.raises(DataflowError,match='parent_record_id'):
        channels._route_record_document(record,spec=spec_for(routing),state=ledger,bounds=bounds())
    assert calls==[] and ledger.values()=={'route':0}
