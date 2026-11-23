"""Original finite scope proof, typed key projection, and original join scratch."""
from dataclasses import replace
import math
from types import SimpleNamespace

import pytest

from demiflow.execution.contracts.dataflow import Record
from demiflow.execution.contracts.dataflow_errors import DataflowError
from demiflow.execution.dataflow_finite_input import validate_finite_members
from demiflow.execution.kernels.operator import _SpoolPort
from demiflow.execution.kernels.join_spool import _EncodedJoinInput, join_pairs
from demiflow.execution.kernels import state_codec
from test_runtime_v3_artifact_accounting import store_fixture, consumed
from test_runtime_v3_finite_borrow import fixture, make_seal, record


def port_for(svc, seal, spec):
    port = _SpoolPort('input', svc, 4)
    for entry in validate_finite_members(svc, seal, spec, target='c0'):
        port.adopt(entry)
    port.finish()
    return port


@pytest.mark.parametrize('routing', ['forward', 'rebalance', 'broadcast', 'hash', 'target'])
@pytest.mark.parametrize('restore', [False, True])
def test_join_projection_keeps_original_routes_and_revalidates_after_restore(tmp_path, monkeypatch, routing, restore):
    from demiflow.execution import dataflow_finite_input as finite
    with store_fixture(tmp_path) as (run, store, _):
        svc, spec = fixture(store, routing=routing)
        seal, _, expected = make_seal(svc, spec)
        port = port_for(svc, seal, spec)
        charged = consumed(run)
        if restore:
            raw = state_codec.dumps(port.snapshot(), max_bytes=svc.bounds.max_state_bytes)
            saved = state_codec.loads(raw, max_bytes=svc.bounds.max_state_bytes)
            assert all(type(item['input']) is dict for item in saved['borrowed'])
            port = _SpoolPort('restored', svc, 4)
            port.restore(saved)
            assert port._borrowed_proofs == {}
        calls = []
        decode = finite._decode_record_document
        def observed(*args, **kwargs):
            calls.append(kwargs['record_id'])
            return decode(*args, **kwargs)
        monkeypatch.setattr(finite, '_decode_record_document', observed)
        records = list(port.records().for_join(('key',)))
        assert len(records) == len(expected)
        assert all(type(item) is _EncodedJoinInput for item in records)
        assert [item.inline['key'] for item in records] == [row['key'] for row in expected]
        assert ('value' not in records[0].inline) if records else True
        # Live scope validation is reused only in this exact attempt. Restored
        # original mappings trigger a full scan, including unselected readers.
        assert bool(calls) is restore
        assert consumed(run) == charged


def test_late_immutable_corruption_cannot_emit_any_join_output(tmp_path):
    with store_fixture(tmp_path) as (run, store, _):
        svc, spec = fixture(store)
        seal, refs, _ = make_seal(svc, spec)
        left, right = port_for(svc, seal, spec), port_for(svc, seal, spec)
        path = store._object_path(refs[-1])
        path.write_bytes(path.read_bytes().replace(b'2026', b'2025'))
        emitted = []
        original_charge = consumed(run)
        with pytest.raises(DataflowError, match='reference'):
            for pair in join_pairs(left.records().for_join(('key',)), right.records().for_join(('key',)),
                    left_keys=('key',), right_keys=('key',), how='inner', services=svc):
                emitted.append(pair)
        assert emitted == []
        assert consumed(run) == original_charge
        assert not list((store.root/store.store_id/'staging').iterdir())


def test_projection_capability_cannot_change_binding_or_cross_store(tmp_path):
    with store_fixture(tmp_path/'first') as (_, store, _):
        svc, spec = fixture(store)
        seal, _, _ = make_seal(svc, spec)
        port = port_for(svc, seal, spec)
        port._borrowed[0]['input']['bytes'] += 1
        with pytest.raises(DataflowError, match='full-scan proof'):
            list(port.records().for_join(('key',)))
        fresh = port_for(svc, seal, spec)
        from demiflow.execution.dataflow_finite_input import borrowed_join_records
        proof = fresh._borrowed_proofs[0]
        foreign = SimpleNamespace(**{**vars(svc), 'store': object()})
        with pytest.raises(DataflowError, match='full-scan proof'):
            list(borrowed_join_records(foreign, fresh._borrowed[0]['input'], proof,
                                      fields=('key',), row_window=65536))
        changed_assignment = SimpleNamespace(**{**vars(svc.assignment), 'attempt':2, 'fence':'new-attempt'})
        next_attempt = SimpleNamespace(**{**vars(svc), 'assignment':changed_assignment})
        with pytest.raises(DataflowError, match='full-scan proof'):
            list(borrowed_join_records(next_attempt, fresh._borrowed[0]['input'], proof,
                                      fields=('key',), row_window=65536))


def test_projection_falls_back_when_same_row_window_cannot_hold_copies(tmp_path):
    from demiflow.execution.dataflow_finite_input import borrowed_join_records
    with store_fixture(tmp_path) as (_, store, _):
        svc, spec = fixture(store)
        seal, _, expected = make_seal(svc, spec)
        port = port_for(svc, seal, spec)
        rows=[]
        for i, item in enumerate(port._borrowed):
            rows.extend(borrowed_join_records(svc,item['input'],port._borrowed_proofs[i],
                                             fields=('key',),row_window=1))
        assert all(type(row) is Record for row in rows)
        assert [dict(row.inline) for row in rows] == expected


@pytest.mark.parametrize('how', ['inner', 'left', 'semi', 'anti'])
def test_join_key_codec_matches_original_for_null_binary_tuple_negative_zero_and_skew(tmp_path, how):
    from demiflow.execution.kernels.join_spool import project_join_document
    from demiflow.execution.exchange.dataflow_bridge import _encode_record_document
    with store_fixture(tmp_path) as (_, store, _):
        svc, _ = fixture(store)
        values = [None, b'bytes', ('a', 7), -0.0, 0.0, 3, *([9]*15)]
        left = [record({'lk':value,'l':i,'opaque':b'x'*2048},i) for i,value in enumerate(values)]
        right = [record({'rk':value,'r':i,'opaque':(b'bytes',{1,2},-0.0)},i)
                 for i,value in enumerate([b'bytes',('a',7),-0.0,None,*([9]*11)])]
        def execute(lrows,rrows):
            return [(dict(l.inline),None if r is None else dict(r.inline)) for l,r in join_pairs(
                iter(lrows),iter(rrows),left_keys=('lk',),right_keys=('rk',),how=how,services=svc)]
        expected = execute(left,right)
        def project(rows,field):
            return [project_join_document(_encode_record_document(row,max_bytes=svc.bounds.max_row_bytes),
                record_id=row.record_id,sequence=row.sequence,max_bytes=svc.bounds.max_row_bytes,
                fields=(field,),row_window=svc.bounds.max_resident_bytes//4) for row in rows]
        actual = execute(project(left,'lk'),project(right,'rk'))
        assert actual == expected
        assert all(math.copysign(1,row['opaque'][-1]) < 0 for _,row in actual if row is not None)


def test_projection_metadata_uses_original_state_window_or_falls_back(tmp_path):
    with store_fixture(tmp_path) as (_, store, _):
        svc, spec = fixture(store)
        seal, _, expected = make_seal(svc, spec)
        original = port_for(svc, seal, spec)
        image = state_codec.dumps(original.snapshot(), max_bytes=svc.bounds.max_state_bytes)
        limited = SimpleNamespace(**{**vars(svc), 'bounds':replace(svc.bounds,max_state_bytes=len(image)+64)})
        port = port_for(limited, seal, spec)
        assert port._borrowed_proofs == {}
        rows = list(port.records().for_join(('key',)))
        assert all(type(row) is Record for row in rows)
        assert [dict(row.inline) for row in rows] == expected


def test_duplicate_declared_keys_keep_original_row_admission(tmp_path):
    with store_fixture(tmp_path) as (_, store, _):
        svc, spec = fixture(store)
        seal, _, expected = make_seal(svc, spec)
        rows = list(port_for(svc, seal, spec).records().for_join(('key','key')))
        assert all(type(row) is Record for row in rows)
        assert [dict(row.inline) for row in rows] == expected


def test_projected_scratch_preserves_original_schema_provenance_and_document_bytes(tmp_path):
    import pyarrow as pa
    from demiflow.execution.record_schema import RecordSchemas
    from demiflow.execution.kernels.join_spool import project_join_document, _Spool
    from demiflow.execution.exchange.dataflow_bridge import _encode_record_document
    with store_fixture(tmp_path) as (_, store, _):
        svc, _ = fixture(store)
        schemas = RecordSchemas(svc)
        schema = pa.schema([('k',pa.int64()),('payload',pa.binary())],metadata={b'origin':b'original'})
        source = replace(record({'k':7,'payload':b'wide'*1024},31), schema_ref=schemas.put(schema))
        raw = _encode_record_document(source,max_bytes=svc.bounds.max_row_bytes)
        projected = project_join_document(raw,record_id=source.record_id,sequence=source.sequence,
            max_bytes=svc.bounds.max_row_bytes,fields=('k',),row_window=svc.bounds.max_resident_bytes//4)
        assert type(projected) is _EncodedJoinInput
        with store.scratch(max_bytes=svc.bounds.max_spill_bytes,max_io_bytes=svc.bounds.max_resident_bytes//4) as file:
            spool = _Spool(file,svc)
            offset,_ = spool.append(projected,('k',))
            recovered = spool.record(offset,spool.header(offset))
            assert recovered == source
            assert _encode_record_document(recovered,max_bytes=svc.bounds.max_row_bytes) == raw
            assert schemas.read(recovered.schema_ref).equals(schema,check_metadata=True)


def test_fixed_lance_probe_filters_before_finite_port_and_preserves_right_multiplicity(tmp_path, monkeypatch):
    import lance
    import pyarrow as pa
    from demiflow import data
    from demiflow.execution import RunContext
    from demiflow.execution.kernels.operator import _SpoolPort
    from demiflow.execution.contracts.relational_region import JOIN_PROBE_SUFFIX
    from demiflow.execution.sealed_plans import load_sealed_plan

    uri = str(tmp_path/'left.lance')
    source = lance.write_dataset(pa.table({'k': pa.array([2, -1, 1, 2, None, 6], type=pa.int32()),
        'seq':range(6), 'w':pa.array([b'wide']*6, type=pa.binary(4))}), uri)
    reads, ports = [], []
    take = lance.LanceDataset._take_rows
    push = _SpoolPort.push
    def hydrate(ds, ids, *args, **kwargs):
        reads.extend(ids)
        return take(ds, ids, *args, **kwargs)
    def observe(port, record):
        ports.append(port.name)
        return push(port, record)
    monkeypatch.setattr(lance.LanceDataset,'_take_rows',hydrate)
    monkeypatch.setattr(_SpoolPort,'push',observe)
    for case, how, right, expected, retained in [
        ('inner','inner', [1,2,None,2], [0,0,2,3,3], 3),
        ('semi','semi', [1,2,None,2], [0,2,3], 3),
        ('empty','inner', [], [], 0),
        ('none','inner', [None], [], 0),
        # First two keys were already read for the optional filter when the
        # out-of-range third key forces fallback. The same closed build port
        # still owns both occurrences of 2 and no right record is evaluated twice.
        ('prefix','inner',[2,1,2**48,2], [0,0,2,3,3], None),
    ]:
        reads.clear()
        ports.clear()
        with RunContext.start(tmp_path/case,limits={'memory_bytes':1<<30,'max_row_bytes':65536,
                'max_batch_bytes':2<<20}), data.local_execution(workers=2,partitions=1,
                worker_mode='thread',memory_bytes=1<<30):
            left = data.read_lance(uri,version=source.version)
            right_table = pa.table({'rk':pa.array(right,type=pa.int64()), 'v':range(len(right))})
            rows = list(left.join(data.from_arrow(right_table), on='k', right_on='rk', how=how).iter_rows())
        plan = load_sealed_plan(tmp_path/case,'action-1',version='v3')['physical']
        assert len(plan.stages) == 1 and plan.stages[0].stage_id.endswith(JOIN_PROBE_SUFFIX)
        assert sorted(row['seq'] for row in rows) == expected
        assert all(row['w'] == b'wide' and ('v' in row) == (how == 'inner') for row in rows)
        assert ports.count('left') == (6 if retained is None else retained)
        assert ports.count('right') == len(right)
        assert len(reads) == (0 if retained is None else retained)


def test_fixed_lance_join_rebuilds_filter_from_closed_right_after_source_cut(tmp_path, monkeypatch):
    import lance
    import pytest
    import pyarrow as pa
    from demiflow import data
    from demiflow.execution import RunContext
    from demiflow.execution.action_driver.dataflow import DataflowExecution

    source = lance.write_dataset(pa.table({'k':range(80)}),str(tmp_path/'left.lance'))
    def rows():
        left = data.read_lance(source.uri,version=1,batch_size=8)
        right = data.from_items([{'rk':k,'v':i} for i,k in enumerate([8,48,48,72])])
        return sorted((row['k'],row['v']) for row in left.join(right,on='k',right_on='rk').iter_rows())
    class Interrupted(RuntimeError):
        pass
    pump = DataflowExecution.pump
    def charged_cut(driver, **kwargs):
        result = pump(driver,**kwargs)
        image = driver._restore_image
        if image is not None and not image.cut.final and any(
                12 <= cursor.next_sequence < 80 for c in image.contributions for cursor in c.sources):
            raise Interrupted()
        return result
    def local():
        return data.local_execution(workers=2,partitions=1,worker_mode='thread',memory_bytes=1<<30)
    with pytest.raises(Interrupted), RunContext.start(tmp_path/'run',limits={'memory_bytes':1<<30,
            'checkpoint_rows':16,'max_row_bytes':65536,'max_batch_bytes':2<<20}) as run, local():
        ref = run.ref
        with monkeypatch.context() as patch:
            patch.setattr(DataflowExecution,'pump',charged_cut)
            rows()
    with RunContext.resume(ref) as run, local():
        assert rows() == [(8,0),(48,1),(48,2),(72,3)]
        assert run.attempt == 2


def test_fixed_join_does_not_skip_unrepresentable_source_payloads(tmp_path):
    import lance
    import pyarrow as pa
    import pytest
    from demiflow import data
    from demiflow.execution import RunContext

    uri = str(tmp_path/'left.lance')
    lance.write_dataset(pa.table({'k':[4,5], 'wide':pa.array([b'x'*8192]*2, type=pa.binary(8192))}),uri)
    with pytest.raises(DataflowError, match='BUDGET_EXCEEDED|exceeds max_row_bytes|byte budget'):
        with RunContext.start(tmp_path/'run',limits={'memory_bytes':1<<30,'max_row_bytes':4096,
                'max_batch_bytes':65536}),data.local_execution(worker_mode='thread',workers=2,partitions=1):
            right = data.from_items([{'rk':27}])
            list(data.read_lance(uri,version=1).join(right,on='k',right_on='rk',how='semi').iter_rows())


def test_early_close_fixed_join_can_read_complete_result_on_resume(tmp_path):
    import lance
    import pyarrow as pa
    from demiflow import data
    from demiflow.execution import RunContext
    from test_runtime_v3_terminal_replay_admission import InterruptedAfterAction

    count = 127
    source = lance.write_dataset(pa.table({'seq':range(count),'k':[1]*count}),str(tmp_path/'left.lance'))
    def rows():
        left = data.read_lance(source.uri,version=1,batch_size=32)
        return left.join(data.from_items([{'rk':1,'v':8},{'rk':1,'v':4}]),on='k',right_on='rk').iter_rows()
    def local():
        return data.local_execution(workers=2,partitions=1,worker_mode='thread',memory_bytes=1<<30)
    with pytest.raises(InterruptedAfterAction):
        with RunContext.start(tmp_path/'run',limits={'memory_bytes':1<<30,'checkpoint_rows':32}) as run, local():
            ref = run.ref
            stream = rows()
            first = next(stream)
            stream.close()
            manifest = run.journal.execution_commits('action-1')[-1]['prepared']['sinks']['iter_rows']['manifest']
            assert not manifest['final'] or manifest['total_rows'] == 2 * count
            raise InterruptedAfterAction()
    with RunContext.resume(ref), local():
        recovered = list(rows())
        assert recovered[0] == first
        assert [(row['seq'], row['v']) for row in recovered] == [
            (i, v) for i in range(count) for v in (8, 4)]


def test_fixed_join_take_completes_at_its_declared_limit(tmp_path):
    import lance
    import pyarrow as pa
    from demiflow import data
    from demiflow.execution import RunContext

    source = lance.write_dataset(pa.table({'seq':range(24),'k':[1]*24}),str(tmp_path/'left.lance'))
    with RunContext.start(tmp_path/'run',limits={'memory_bytes':1<<30}) as run, data.local_execution(
            workers=2,partitions=1,worker_mode='thread',memory_bytes=1<<30):
        joined = data.read_lance(source.uri,version=1).join(data.from_items([{'k':1}]),on='k',how='semi')
        assert [row['seq'] for row in joined.take(3)] == [0,1,2]
        manifest = run.journal.execution_commits('action-1')[-1]['prepared']['sinks']['take']['manifest']
        assert manifest['final'] and manifest['total_rows'] == 3


@pytest.mark.parametrize('right_key', [0, 1])
def test_fixed_join_honors_original_source_credit_once(tmp_path, right_key):
    import lance
    import pyarrow as pa
    from demiflow import data
    from demiflow.execution import RunContext
    from demiflow.execution.source_readers import _encoded_size

    row = {'k':1, 'body':b'x'*512}
    source = lance.write_dataset(pa.table({'k':pa.array([1],type=pa.int64()),
        'body':pa.array([row['body']],type=pa.binary(512))}),str(tmp_path/'left.lance'))
    encoded = _encoded_size(row,max_bytes=65536)
    def evaluate(cap):
        with RunContext.start(tmp_path/str(cap),limits={'memory_bytes':1<<30,'max_row_bytes':65536,
                'checkpoint_unconfirmed_rows':1,'checkpoint_unconfirmed_bytes':cap}),data.local_execution(
                worker_mode='thread',workers=1,partitions=1,memory_bytes=1<<30):
            right = data.from_items([{'rk':right_key}])
            return list(data.read_lance(source.uri,version=1).join(right,on='k',right_on='rk').iter_rows())
    assert evaluate(encoded) == ([{**row,'rk':1}] if right_key else [])
    with pytest.raises(DataflowError, match='BUDGET_EXCEEDED'):
        evaluate(encoded-1)
