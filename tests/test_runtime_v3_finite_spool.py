"""Finite ports group actual encoded bytes without one immutable file per row."""
from dataclasses import replace
from types import SimpleNamespace
import pytest
from demiflow.execution.contracts.dataflow_errors import DataflowError
from demiflow.execution.kernels import state_codec
from demiflow.execution.kernels.operator import _SpoolPort,_segment_rows
from demiflow.execution.run_context import RunContext
from test_runtime_v3_artifact_recovery import _store
from test_runtime_v3_real_seams import PreparationOnly
from test_runtime_v3_k import rec


def test_actual_bytes_group_small_records_in_original_store_and_restore(tmp_path):
    with pytest.raises(PreparationOnly),RunContext.start(tmp_path/'run') as run:
        store,_,bounds=_store(run,ceiling=4*1024**2)
        bounds=replace(bounds,max_row_bytes=65536,max_batch_bytes=65536,max_state_bytes=8192)
        services=SimpleNamespace(store=store,bounds=bounds,check_stop=lambda: None)
        port=_SpoolPort('left',services,_segment_rows(services))
        expected=[rec({'id':i,'value':'x'*20},sequence=i) for i in range(300)]
        for record in expected:port.push(record)
        snap=port.snapshot()
        assert 1<len(snap['refs'])<10
        assert snap['pending']==[]
        assert len(state_codec.dumps(snap,max_bytes=bounds.max_state_bytes))<bounds.max_state_bytes
        for ref in snap['refs']:
            raw=b''.join(store.read(ref,max_bytes=bounds.max_batch_bytes))
            assert len(raw)<=bounds.max_batch_bytes
            assert len(state_codec.loads(raw,max_bytes=bounds.max_batch_bytes))>1
        restored=_SpoolPort('left',services,_segment_rows(services));restored.restore(snap)
        restored.finish()
        assert list(restored.records())==expected
        raise PreparationOnly()


def test_old_snapshot_pending_rows_remain_readable_and_new_input_respects_bytes(tmp_path):
    with pytest.raises(PreparationOnly),RunContext.start(tmp_path/'run') as run:
        store,_,bounds=_store(run,ceiling=4*1024**2)
        bounds=replace(bounds,max_row_bytes=2048,max_batch_bytes=4096)
        services=SimpleNamespace(store=store,bounds=bounds,check_stop=lambda: None)
        first=rec({'text':'a'*1200},sequence=0);second=rec({'text':'b'*1200},sequence=1)
        old={'refs':[],'pending':[first],'eoi':False,'count':1,'bytes':0}
        port=_SpoolPort('left',services,_segment_rows(services));port.restore(old);port.push(second);port.finish()
        assert list(port.records())==[first,second]
        assert all(ref.size_bytes<=bounds.max_batch_bytes for ref in port._refs)
        with pytest.raises(DataflowError,match='row'):
            _SpoolPort('right',services,_segment_rows(services)).push(rec({'text':'x'*2200}))
        raise PreparationOnly()


def test_streamed_port_bytes_equal_original_kernel_state_and_pending_read(tmp_path):
    with pytest.raises(PreparationOnly), RunContext.start(tmp_path/'run') as run:
        store, _, bounds = _store(run, ceiling=4 * 1024**2)
        bounds = replace(bounds, max_row_bytes=16384, max_batch_bytes=65536)
        services = SimpleNamespace(store=store, bounds=bounds, check_stop=lambda: None)
        records = [rec({'id': i, 'blob': b'\x00\xff' * 1000,
                        'tuple': (-0.0, 0.0), 'nested': ['中文', None]}, sequence=i) for i in range(3)]
        original = state_codec.dumps(records, max_bytes=bounds.max_batch_bytes)
        port = _SpoolPort('left', services, _segment_rows(services))
        for record in records:
            port.push(record)
        assert list(port.records()) == records
        snap = port.snapshot()
        assert len(snap['refs']) == 1 and snap['pending'] == []
        actual = b''.join(store.read(snap['refs'][0], max_bytes=bounds.max_batch_bytes))
        assert actual == original
        assert snap['bytes'] == len(original)
        raise PreparationOnly()
