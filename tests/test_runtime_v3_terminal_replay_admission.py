"""Real ACKed output replays through original read credit before data decoding."""
import json
import os
from pathlib import Path

import pytest

from demiflow import data
from demiflow.execution.contracts.dataflow_errors import DataflowError
from demiflow.execution.run_context import RunContext


class InterruptedAfterAction(Exception):
    pass


def _fd_count(directory):
    count = 0
    for entry in Path('/proc/self/fd').iterdir():
        try:
            count += str(directory) in os.readlink(entry)
        except FileNotFoundError:
            pass
    return count


@pytest.mark.parametrize('limited', [True, False])
def test_final_ack_replay_admits_each_window_and_preserves_result(tmp_path, monkeypatch, limited):
    from demiflow.execution.action_driver import dataflow
    from demiflow.execution.dataflow_terminals import TerminalSubscription
    from demiflow.execution.exchange import dataflow_bridge
    rows = [{'i': i, 'payload': 'x' * 6000} for i in range(20)]
    source = tmp_path / 'input.jsonl'
    source.write_text(''.join(json.dumps(row) + '\n' for row in rows))
    limits = {'timeout_s': 30, 'max_row_bytes': 16 * 1024, 'max_batch_bytes': 32 * 1024}
    with pytest.raises(InterruptedAfterAction):
        with RunContext.start(tmp_path / 'run', limits=limits) as run, data.local_execution(memory_bytes=256*1024):
            ref = run.ref
            assert data.read_json(str(source)).take_all() == rows
            previous = run.journal.execution_commits('action-1')
            raise InterruptedAfterAction()
    decoder = dataflow_bridge._decode_record_document
    read = TerminalSubscription.read_admitted
    decoded, windows, coexistence = [], [], []
    active = [None]
    def observe_decode(raw, **kwargs):
        record = decoder(raw, **kwargs)
        decoded.append(record.encoded_bytes)
        coexistence.append(sum(decoded) + active[0].buffered_bytes)
        return record
    def observe_read(self, **kwargs):
        active[0] = self
        records = read(self, **kwargs)
        windows.append(sum(record.encoded_bytes for record in records))
        return records
    def forbidden_launch(*args, **kwargs):
        raise AssertionError('ACKed final result must not launch a worker')
    with monkeypatch.context() as patch:
        patch.setattr(dataflow_bridge, '_decode_record_document', observe_decode)
        patch.setattr(TerminalSubscription, 'read_admitted', observe_read)
        patch.setattr(dataflow.DataflowExecution, '_launch_phase', forbidden_launch)
        def replay():
            with RunContext.resume(ref) as run, data.local_execution(workers=1,
                    memory_bytes=(96 if limited else 256)*1024):
                result = data.read_json(str(source)).take_all()
                assert result == rows
                assert run.journal.execution_commits('action-1') == previous
        if limited:
            with pytest.raises(DataflowError, match='collected bytes'):
                replay()
        else:
            replay()
    # A failure may be proven inside the first admitted read, before any
    # complete window is returned. Every actually returned window stays bounded.
    assert all(window <= 32*1024 for window in windows)
    assert limited or windows
    assert (0 < len(decoded) < 20) if limited else len(decoded) == 20
    assert max(coexistence) <= (96 if limited else 256)*1024
    from demiflow.execution.run_journal import RunJournal
    assert RunJournal(tmp_path/'run').execution_commits('action-1') == previous
    assert _fd_count(tmp_path / 'run') == 0



@pytest.mark.parametrize('limit,overflow',[(4,False),(3,True)])
def test_shared_source_respects_total_limit(tmp_path,limit,overflow):
    from collections import Counter
    def work():
        with RunContext.start(tmp_path/'run',limits={'timeout_s':30,'max_output_rows':4}) as run,data.local_execution(workers=4) as scope:
            source=data.from_items([{'i':0},{'i':1}])
            result=source.map(lambda r:{**r,'side':'a'}).union(source.map(lambda r:{**r,'side':'b'})).take_all(limit=limit)
            assert Counter((r['i'],r['side']) for r in result)==Counter((i,s) for i in (0,1) for s in ('a','b'))
            assert run.journal.execution_commits('action-1')[-1]['status']=='acked'
    if overflow:
        with pytest.raises(ValueError,match='take_all limit'):work()
    else:work()


@pytest.mark.parametrize('count,overflow',[(4,False),(5,True)])
def test_checkpoint_prefix_counts_toward_limit(tmp_path,monkeypatch,count,overflow):
    from demiflow.execution.action_driver.dataflow import DataflowExecution
    from demiflow.execution.state_artifacts import StateArtifacts
    limits={'memory_bytes':40*1024,'max_batch_bytes':16*1024,'max_row_bytes':8*1024,'max_state_bytes':128*1024,'timeout_s':30,'checkpoint_rows':2,'checkpoint_interval_ms':60000,'max_output_rows':4}
    original=DataflowExecution._retire_phase;prefix=[]
    class Interrupted(Exception):pass
    def stop(ex,image):
        rows=ex._publication(image)['manifest']['total_rows']
        if rows:
            prefix.append(rows)
            raise Interrupted()
        return original(ex,image)
    rows=[{'i':i} for i in range(count)]
    with pytest.raises(Interrupted):
        with RunContext.start(tmp_path/'run',limits=limits) as run,data.local_execution():
            ref=run.ref
            with monkeypatch.context() as patch:
                patch.setattr(DataflowExecution,'_retire_phase',stop)
                data.from_items(rows).take_all(limit=4)
    assert prefix and prefix[0]==2
    def resume():
        with RunContext.resume(ref) as run,data.local_execution():
            old=run.journal.execution_commits('action-1')
            result=data.from_items(rows).take_all(limit=4)
            assert result==rows
            new=run.journal.execution_commits('action-1')
            assert new[:len(old)]==old
            assert StateArtifacts(run).read(new[-1]['prepared']['state']['dataflow-cut-v3'])['cut']['final']
    if overflow:
        with pytest.raises(ValueError,match='take_all limit'):resume()
    else:resume()


@pytest.mark.parametrize('limit', [0, 1])
def test_original_published_blocks_probe_limit_without_decoding_payload(tmp_path, monkeypatch, limit):
    """A read-level negative proof over real ACKed blocks, with no new commit."""
    from demiflow.execution.action_driver import dataflow
    from demiflow.execution.dataflow_terminals import TerminalSubscription
    from demiflow.execution.exchange import dataflow_bridge
    from demiflow.execution.run_journal import RunJournal
    captured = []
    original = dataflow.DataflowExecution._set_result
    def remember(execution, image):
        captured.append((execution, execution._publication(image)))
        return original(execution, image)
    with monkeypatch.context() as patch:
        patch.setattr(dataflow.DataflowExecution, '_set_result', remember)
        with RunContext.start(tmp_path/'run', limits={'timeout_s':30}) as run, data.local_execution():
            rows = [{'i':i,'typed':(b'\xff', -0.0, None)} for i in range(3)]
            assert data.from_items(rows).repartition(2).take_all() == rows
            before = run.journal.execution_commits('action-1')
    execution, publication = captured[-1]
    subscription = TerminalSubscription(execution._bounds, execution._exchange, ())
    subscription.restore(publication, store=execution._store)
    decoded = []
    decode = dataflow_bridge._decode_record_document
    def observe(raw, **kwargs):
        value = decode(raw, **kwargs)
        decoded.append(value)
        return value
    def admit(size, count, pending_bytes):
        if count >= limit:
            raise ValueError('take_all limit metadata probe')
    try:
        with monkeypatch.context() as patch:
            patch.setattr(dataflow_bridge, '_decode_record_document', observe)
            with pytest.raises(ValueError, match='metadata probe'):
                subscription.read_admitted(max_records=64, deadline_ms=0, admit=admit)
    finally:
        subscription.close()
    assert len(decoded) == limit
    if decoded:
        assert decoded[0].block_partition is not None
        assert decoded[0].inline['typed'] == (b'\xff', -0.0, None)
    assert RunJournal(tmp_path/'run').execution_commits('action-1') == before
    assert _fd_count(tmp_path/'run') == 0


@pytest.mark.parametrize('mode', ['rows', 'batches'])
def test_streaming_does_not_accumulate_the_entire_action_budget(tmp_path, mode):
    source = tmp_path/'stream.jsonl'
    source.write_text(''.join(json.dumps({'i':i,'payload':'x'*6000})+'\n' for i in range(64)))
    limits={'timeout_s':30,'max_row_bytes':8*1024,'max_batch_bytes':32*1024}
    with RunContext.start(tmp_path/'run',limits=limits), data.local_execution(workers=2,memory_bytes=192*1024):
        ds=data.read_json(str(source))
        if mode=='rows':
            count=0
            for row in ds.iter_rows():
                assert row['i']==count and len(row['payload'])==6000
                count+=1
        else:
            count=0
            for batch in ds.iter_batches(batch_size=2,batch_format='pyarrow'):
                assert len(batch)==2
                assert batch['i'].to_pylist()==list(range(count,count+2))
                count+=2
        assert count==64
    assert _fd_count(tmp_path/'run')==0
