"""Borrow immutable finite producer records through the original store and seal.

Component cases use the real RunJournal/ArtifactStore/DTO/route/record codecs.
The public Dataset cases also execute actual workers and committed recovery.
"""
from dataclasses import replace
from datetime import date
from decimal import Decimal
import json
import math
from types import SimpleNamespace

import pytest

from demiflow import data
from demiflow.execution.contracts.dataflow import Record, ProducerCompletion
from demiflow.execution.contracts.dataflow_errors import DataflowError
from demiflow.execution.dataflow_finite_input import validate_finite_members, borrowed_records
from demiflow.execution.dataflow_segments import FINITE_RECORDS_MEDIA
from demiflow.execution.exchange.dataflow_bridge import _encode_payload, _encode_record_document
from demiflow.execution.exchange.dataflow_channels import route_record, seal_scope
from demiflow.execution.exchange.dataflow_state import RoutingCounters
from demiflow.execution.kernels.operator import _SpoolPort
from demiflow.execution.kernels import state_codec
from demiflow.execution.recovery_contract import document, identity
from demiflow.execution.run_context import RunContext
from test_runtime_v3_artifact_accounting import store_fixture, consumed
from test_runtime_v3_c import make, bounds


def record(row, ordinal=0):
    raw = _encode_payload(row, max_bytes=65536)
    return Record(record_id=identity(['borrow', ordinal]), source_position={'ordinal':ordinal},
                  payload_ref=None, inline=row, encoded_bytes=len(raw), sequence=ordinal)


def fixture(store, *, routing='forward', target='c0'):
    readers = ('c0',) if routing == 'forward' else ('c0','c1')
    spec = make('ExchangeSpec', channel_id='borrow-channel', edge_id=identity('borrow-edge'),
        producer_partitions=('p0','p1'), consumer_partitions=readers, mode='finite_sealed',
        routing=routing, keys=('target',) if routing=='target' else ('key',) if routing=='hash' else (),
        scope_id='borrow-scope', route_epoch=0, storage_bytes=1024*1024)
    svc = SimpleNamespace(store=store, bounds=bounds(max_row_bytes=65536,max_batch_bytes=65536,
        max_state_bytes=32768,max_control_bytes=65536,max_spill_bytes=1024*1024),
        assignment=SimpleNamespace(inputs=(spec,),lease=SimpleNamespace(partition_ids=(target,))),
        check_stop=lambda:None)
    return svc,spec


def make_seal(svc,spec,*,edit=None):
    members=[]; expected=[]; refs=[]
    for part in spec.producer_partitions:
        raw_lines=[]
        state=RoutingCounters(values={},route_epoch=spec.route_epoch)
        for i in range(4):
            row={'part':part,'key':i,'target':'c'+str(i%2), 'value':(date(2026,11,16),
                Decimal('1.230'),b'\x00bytes',frozenset({2,3}),-0.0)}
            rec=record(row,i)
            for routed in route_record(rec,spec=spec,state=state):
                item={'record_id':rec.record_id,'sequence':rec.sequence,'target':routed.reader_id,
                      'document':_encode_record_document(rec,max_bytes=svc.bounds.max_row_bytes).decode()}
                raw_lines.append(item)
                if routed.reader_id==svc.assignment.lease.partition_ids[0]:expected.append(row)
        if edit:edit(part,raw_lines)
        payload=b''.join(document(item)+b'\n' for item in raw_lines)
        ref=svc.store.put(iter([payload]),max_bytes=max(1,len(payload)),media_type=FINITE_RECORDS_MEDIA)
        refs.append(ref)
        members.append(ProducerCompletion(part,0,'succeeded',len(raw_lines)-1,ref))
    seal=seal_scope(spec.scope_id,tuple(members),store=svc.store)
    return seal,refs,expected


@pytest.mark.parametrize('routing',['forward','rebalance','broadcast','hash','target'])
def test_borrow_original_members_preserves_routes_typed_rows_and_owned_pins(tmp_path,monkeypatch,routing):
    with store_fixture(tmp_path) as (run,store,_):
        svc,spec=fixture(store,routing=routing)
        seal,refs,expected=make_seal(svc,spec)
        for ref in refs:store.retain(ref,owner='finite-producer')
        charged=consumed(run)
        objects=set((store.root/store.store_id/'objects').iterdir())
        entries=validate_finite_members(svc,seal,spec,target='c0')
        port=_SpoolPort('input',svc,4)
        for entry in entries:port.adopt(entry)
        port.finish()
        image=state_codec.dumps(port.snapshot(),max_bytes=svc.bounds.max_state_bytes)
        restored=_SpoolPort('input',svc,4)
        restored.restore(state_codec.loads(image,max_bytes=svc.bounds.max_state_bytes))
        result=[dict(r.inline) for r in restored.records()]
        assert result==expected
        assert all(math.copysign(1,r['value'][-1])==-1 for r in result)
        assert port._refs==restored._refs==[]
        assert consumed(run)==charged
        assert set((store.root/store.store_id/'objects').iterdir())==objects
        releases=[]
        original=store.release
        def release(ref,*,owner):
            releases.append((ref,owner));return original(ref,owner=owner)
        monkeypatch.setattr(store,'release',release)
        restored.release();port.release()
        assert releases==[]
        assert all(store.inspect(ref) for ref in refs)
        assert b''.join(store.read(seal,max_bytes=svc.bounds.max_control_bytes))


def test_old_owned_prefix_and_pending_snapshot_remain_readable_in_order(tmp_path):
    with store_fixture(tmp_path) as (_,store,_):
        svc,spec=fixture(store)
        seal,_,expected=make_seal(svc,spec)
        port=_SpoolPort('input',svc,4)
        old={'refs':[],'pending':[record({'prefix':1})], 'eoi':False,'count':1,'bytes':0}
        port.restore(old)
        for entry in validate_finite_members(svc,seal,spec,target='c0'):port.adopt(entry)
        port.push(record({'suffix':1}));port.finish()
        image=port.snapshot()
        restored=_SpoolPort('input',svc,4);restored.restore(image)
        assert [dict(r.inline) for r in restored.records()]==[{'prefix':1},*expected,{'suffix':1}]
        assert len(image['refs'])==2 and len(image['borrowed'])==2
        assert all(x['position']==1 for x in image['borrowed'])
        for key,value in [('count',0),('borrowed',image['borrowed']*2)]:
            with pytest.raises(DataflowError):_SpoolPort('input',svc,4).restore({**image,key:value})


@pytest.mark.parametrize('routing', ['forward', 'rebalance', 'broadcast', 'hash', 'target'])
def test_canonical_finite_payload_cannot_bypass_provenance_validation(tmp_path, routing):
    from demiflow.execution.exchange.dataflow_bridge import _codec_dumps
    with store_fixture(tmp_path) as (_, store, _):
        svc, spec = fixture(store, routing=routing)
        def edit(part, items):
            if part != 'p1':
                return
            for item in items:
                payload = json.loads(item['document'])
                payload['sp']['parent_record_ids'] = ['invalid-parent-digest']
                item['document'] = _codec_dumps(payload).decode()
        seal, _, _ = make_seal(svc, spec, edit=edit)
        with pytest.raises(DataflowError, match='parent_record_id'):
            validate_finite_members(svc, seal, spec, target='c0')


@pytest.mark.parametrize('fault',['route','unselected_payload','count','membership','digest','truncated'])
def test_full_scan_rejects_every_member_before_any_reference_is_returned(tmp_path,fault):
    with store_fixture(tmp_path) as (_,store,_):
        svc,spec=fixture(store,routing='rebalance')
        def edit(part,items):
            if part!='p1':return
            if fault=='route':items[-1]['target']='c0'
            if fault=='unselected_payload':
                assert items[-1]['target']=='c1'
                items[-1]['document']='{}'
        seal,refs,_=make_seal(svc,spec,edit=edit)
        if fault in ('count','membership'):
            payload=json.loads(b''.join(store.read(seal,max_bytes=svc.bounds.max_control_bytes)))
            if fault=='count':payload['producers'][-1]['last_sequence']+=1
            else:payload['producers'][-1]=payload['producers'][0]
            raw=document(payload)
            seal=store.put(iter([raw]),max_bytes=len(raw),media_type=seal.media_type)
        if fault in ('digest','truncated'):
            path=store._object_path(refs[-1])
            raw=path.read_bytes()
            path.write_bytes(raw.replace(b'2026',b'2025') if fault=='digest' else raw[:-1])
        before=set((store.root/store.store_id/'objects').iterdir())
        with pytest.raises(DataflowError):
            validate_finite_members(svc,seal,spec,target='c0')
        assert set((store.root/store.store_id/'objects').iterdir())==before


def test_reread_reauthenticates_original_bytes_and_respects_original_limits(tmp_path):
    with store_fixture(tmp_path) as (_,store,_):
        svc,spec=fixture(store)
        seal,refs,_=make_seal(svc,spec)
        entries=validate_finite_members(svc,seal,spec,target='c0')
        limited=SimpleNamespace(**{**vars(svc),'bounds':replace(svc.bounds,max_row_bytes=128)})
        with pytest.raises(DataflowError,match='bound'):
            validate_finite_members(limited,seal,spec,target='c0')
        limited=SimpleNamespace(**{**vars(svc),'bounds':replace(svc.bounds,max_state_bytes=128)})
        with pytest.raises(DataflowError,match='state byte budget'):
            validate_finite_members(limited,seal,spec,target='c0')
        limited=SimpleNamespace(**{**vars(svc),'bounds':replace(svc.bounds,max_spill_bytes=128)})
        with pytest.raises(DataflowError,match='storage bound'):
            validate_finite_members(limited,seal,spec,target='c0')
        path=store._object_path(refs[0])
        path.write_bytes(path.read_bytes().replace(b'2026',b'2025'))
        with pytest.raises(DataflowError,match='reference'):
            list(borrowed_records(svc,entries[0]))


def test_public_typed_sort_borrows_without_second_payload_copy(tmp_path):
    rows=[{'k':i,'value':(date(2026,11,16),b'bytes',Decimal('1.2'),frozenset({i}),-0.0)}
          for i in reversed(range(12))]
    with RunContext.start(tmp_path/'run',limits={'checkpoint_rows':4,
            'max_row_bytes':8192,'max_batch_bytes':16384}) as run:
        result=data.from_items(rows).sort('k').take_all()
        assert result==list(reversed(rows))
        assert all(math.copysign(1,r['value'][-1])==-1 for r in result)
        images=[]
        for path in (run.directory/'.dataflow-store').glob('*/objects/*'):
            raw=path.read_bytes()
            if raw.startswith(b'["demiflow.kernel-state.v3"'):
                image=state_codec.loads(raw,max_bytes=2**20)
                if isinstance(image,dict) and image.get('kind')=='SortOp':images.append(image)
        assert images
        borrowed=[p for image in images for p in image['ports'].values() if p.get('borrowed')]
        assert borrowed and all(not p['refs'] and not p['pending'] for p in borrowed)
        assert all(sum(x['input']['rows'] for x in p['borrowed'])==len(rows) for p in borrowed)
        assert any(c['status']=='acked' for c in run.journal.execution_commits('action-1'))


def test_external_row_and_empty_member_keep_original_payload_and_bounds(tmp_path):
    from demiflow.execution.kernels.dataflow import _materialize_row
    with store_fixture(tmp_path) as (_,store,_):
        svc,spec=fixture(store)
        row={'value':'x'*4096}
        raw=_encode_payload(row,max_bytes=svc.bounds.max_row_bytes)
        payload=store.put(iter([raw]),max_bytes=len(raw),media_type='application/octet-stream')
        rec=Record(identity('external'),{},payload,None,len(raw),0)
        line=document({'record_id':rec.record_id,'sequence':0,'target':'c0',
            'document':_encode_record_document(rec,max_bytes=svc.bounds.max_row_bytes).decode()})+b'\n'
        assert len(line)<len(raw)
        ref=store.put(iter([line]),max_bytes=len(line),media_type=FINITE_RECORDS_MEDIA)
        empty=store.put(iter(()),max_bytes=1,media_type=FINITE_RECORDS_MEDIA)
        seal=seal_scope(spec.scope_id,(ProducerCompletion('p0',0,'succeeded',0,ref),
            ProducerCompletion('p1',0,'succeeded',-1,empty)),store=store)
        entries=validate_finite_members(svc,seal,spec,target='c0')
        port=_SpoolPort('input',svc,4)
        for entry in entries:port.adopt(entry)
        port.finish()
        assert [_materialize_row(r,svc) for r in port.records()]==[row]
        assert [x['rows'] for x in entries]==[1,0]
        # The compact reference frame does not exempt a large external row.
        limited=SimpleNamespace(**{**vars(svc),'bounds':replace(svc.bounds,max_row_bytes=2048)})
        with pytest.raises(DataflowError,match='max_row_bytes'):
            validate_finite_members(limited,seal,spec,target='c0')
        path=store._object_path(payload)
        path.write_bytes(raw.replace(b'x',b'y',1))
        with pytest.raises(DataflowError,match='reference'):
            validate_finite_members(svc,seal,spec,target='c0')
