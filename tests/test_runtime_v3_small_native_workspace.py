"""Small native writes share one real residency envelope, including recovery."""
from demiflow.execution.contracts.dataflow import stage_resident_bytes
from types import SimpleNamespace as NS
from hashlib import sha256

import lance
import pytest

from demiflow import data
from demiflow.execution import RunContext
from demiflow.execution.contracts.dataflow_errors import DataflowError
from demiflow.execution.action_driver.dataflow import DataflowExecution
from demiflow.execution.scheduler.dataflow_publication_budget import TERMINAL_WORKSPACE
from demiflow.execution.outputs.lance_sink import LanceSink
from test_runtime_v3_durable_residency import LIMITS


def test_real_small_native_publication_keeps_live_terminal_grant_and_original_receipt(tmp_path, monkeypatch):
    class Interrupted(Exception): pass
    prepared, committed = DataflowExecution.prepare, LanceSink.commit
    executions, publications = [], []
    def prepare(execution):
        result = prepared(execution)
        executions.append(execution)
        bounds = execution._bounds
        assert (bounds.max_resident_bytes, bounds.max_row_bytes, bounds.max_batch_bytes) == (40960,8192,16384)
        node = next(n for n in execution._graph.nodes if n.kind == 'terminal')
        assert node.kernel.configuration['terminal_memory'] == {
            'schema':TERMINAL_WORKSPACE, 'resident_bytes':14336}
        assert execution._scheduler._publication_reservation.bytes == 21504
        return result
    def lost(sink, staged, context):
        execution = executions[-1]
        scheduler = execution._scheduler
        active = {scheduler._lease_stage[a] for a in scheduler._resident_by_allocation}
        reservation = execution._scheduler._publication_reservation
        assert active and active <= reservation.stages
        assert scheduler._resident_used == 14336
        assert scheduler._resident_used + reservation.reserved(active, publishing=True) == 40960
        publications.append(context.commit_id)
        committed(sink, staged, context)
        raise Interrupted('after actual native publication')
    target = tmp_path/'result.lance'
    expected = [{'n':n} for n in range(8)]
    with monkeypatch.context() as patch:
        patch.setattr(DataflowExecution,'prepare',prepare)
        patch.setattr(LanceSink,'commit',lost)
        with pytest.raises(Interrupted, match='actual native publication'):
            with RunContext.start(tmp_path/'run',limits=LIMITS) as run, data.local_execution():
                ref = run.ref
                data.from_items(expected).write_lance(str(target),mode='create',return_receipt=True)
    facts = run.journal.execution_commits('action-1')
    assert len(publications)==1 and facts[-1]['status']=='prepared'
    assert any(f['status']=='acked' and f['prepared']['sinks']['write_lance']['native'] is None for f in facts[:-1])
    def forbidden(*args,**kwargs):pytest.fail('confirmed native publication restarted a worker')
    with monkeypatch.context() as patch:
        patch.setattr(DataflowExecution,'_prepare_launch',forbidden)
        with RunContext.resume(ref) as resumed, data.local_execution():
            receipt=data.from_items(expected).write_lance(str(target),mode='create',return_receipt=True)
            after=resumed.journal.execution_commits('action-1')
            assert after[:-1]==facts[:-1] and after[-1]['status']=='acked'
    assert receipt.committed_version==lance.dataset(str(target)).version==1
    assert lance.dataset(str(target)).to_table().to_pylist()==expected


def test_same_40k_envelope_rejects_a_real_unfit_row_before_native_constructor(tmp_path,monkeypatch):
    import demiflow.execution.outputs.batch_conversion as conversion
    launch = DataflowExecution._prepare_launch
    launched, constructed = [], []
    def launching(execution,lease,stage):
        nodes={n.occurrence_id:n for n in execution._graph.nodes}
        launched.extend(nodes[occ].kind for occ in stage.occurrences)
        return launch(execution,lease,stage)
    def forbidden(*args,**kwargs):
        constructed.append(True)
        raise AssertionError('native constructor ran before worker workspace rejection')
    monkeypatch.setattr(DataflowExecution,'_prepare_launch',launching)
    monkeypatch.setattr(conversion,'native_arrow_table',forbidden)
    target=tmp_path/'blocked.lance'
    with pytest.raises(DataflowError,match='record frame codec workspace') as error:
        with RunContext.start(tmp_path/'run',limits=LIMITS) as run, data.local_execution():
            data.from_items([{'n':n,'value':'x'*4500} for n in range(8)]).write_lance(str(target),mode='create')
    assert error.value.code=='BUDGET_EXCEEDED'
    assert 'source' in launched and 'terminal' in launched
    assert not constructed and not target.exists()
    facts=run.journal.execution_commits('action-1')
    assert facts and all(f['prepared']['sinks']['write_lance']['native'] is None for f in facts if f['prepared'])
    assert run.journal.execution_action('action-1')['result'] is None


def test_real_terminal_switch_and_restore_keep_row_index_and_two_chunks_in_one_window(tmp_path,monkeypatch):
    from demiflow.execution.contracts.dataflow import ArtifactRef, BlockPartition
    from demiflow.execution.contracts.record_codec import frame_parts
    from demiflow.execution.storage.scratch import ScratchStream
    from demiflow.execution.storage.segments import block_chunks
    from demiflow.execution.contracts.record_codec import _encode_payload
    from demiflow.execution.outputs.terminal_blocks import TerminalBlocks
    from test_runtime_v3_artifact_accounting import store_fixture
    from test_runtime_v3_c import bounds, make
    limits=bounds(max_resident_bytes=14336,max_row_bytes=8192,max_batch_bytes=16384,
                  max_control_bytes=8192,max_state_bytes=131072)
    read=ScratchStream.read
    observed=[]
    with store_fixture(tmp_path) as (_,store,_):
        services=NS(store=store,bounds=limits)
        budget={'schema':TERMINAL_WORKSPACE,'resident_bytes':14336}
        blocks=TerminalBlocks(services,'worker',budget=budget)
        def reading(stream,count=-1):
            assert blocks.retained_bytes+blocks._pending_resident+2*count+512 <=14336
            observed.append((blocks._pending_resident,count))
            return read(stream,count)
        monkeypatch.setattr(ScratchStream,'read',reading)
        layout=sha256(b'owned-layout').hexdigest()
        expected=[bytearray(),bytearray()]
        try:
            for index in range(48):
                row={'n':index,'value':'x'*40}
                record=make('Record',inline=row,encoded_bytes=len(_encode_payload(row,max_bytes=8192)),
                    block_partition=BlockPartition(layout,index//24,2))
                size,chunks=frame_parts(record,limits,resident_bytes=stage_resident_bytes(getattr(services, 'assignment', None)),retained=blocks.retained_bytes)
                # Save expected bytes separately; append still uses the original
                # lazy generator rather than retaining this verification copy.
                _,proof=frame_parts(record,limits,resident_bytes=stage_resident_bytes(getattr(services, 'assignment', None)),retained=blocks.retained_bytes)
                expected[index//24].extend(b''.join(proof))
                blocks.append_parts(record,chunks,size=size)
            parts=blocks.manifest_partitions()
            assert [p['rows'] for p in parts]==[24,24]
            for part,wanted in zip(parts,expected):
                assert b''.join(block_chunks(store,ArtifactRef(**part['data_ref']),limits))==bytes(wanted)
            restored=TerminalBlocks(services,'worker',budget=budget)
            try:
                restored.restore({'rows':48,'bytes':sum(map(len,expected)),'partitions':parts})
                assert restored.manifest_partitions()==parts
            finally:
                restored.close()
        finally:
            blocks.close()
    assert any(pending for pending,_ in observed) and len(observed)>4
