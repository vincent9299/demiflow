"""Actual I/O workspaces use the original credit, not four declared max rows."""
import pytest

from demiflow import data
from demiflow.execution.run_context import RunContext
from demiflow.execution.contracts.dataflow_errors import DataflowError
from test_runtime_v3_artifact_accounting import store_fixture
from test_runtime_v3_terminal_replay_admission import InterruptedAfterAction, _fd_count


@pytest.mark.parametrize('terminal', ['take_all', 'iter_rows', 'iter_batches'])
def test_default_single_worker_small_memory_live_and_replay(tmp_path, monkeypatch, terminal):
    from demiflow.execution.action_driver.dataflow import DataflowExecution
    rows = [{'i': 1}]
    def consume():
        ds = data.from_items(rows)
        if terminal == 'take_all':
            return ds.take_all()
        if terminal == 'iter_rows':
            return list(ds.iter_rows())
        return [row for batch in ds.iter_batches(batch_size=2, batch_format='pyarrow')
                for row in batch.to_pylist()]
    with pytest.raises(InterruptedAfterAction):
        with RunContext.start(tmp_path/'run', limits={'timeout_s': 30}) as run, data.local_execution(
                workers=1, memory_bytes=96*1024, worker_mode='thread'):
            ref = run.ref
            assert consume() == rows
            commits = run.journal.execution_commits('action-1')
            assert commits[-1]['status'] == 'acked'
            raise InterruptedAfterAction()
    def forbidden_launch(*args, **kwargs):
        raise AssertionError('final ACK replay launched workers')
    monkeypatch.setattr(DataflowExecution, '_launch_phase', forbidden_launch)
    with RunContext.resume(ref) as run, data.local_execution(
            workers=1, memory_bytes=96*1024, worker_mode='thread'):
        assert consume() == rows
        assert run.journal.execution_commits('action-1') == commits
    assert _fd_count(tmp_path/'run') == 0


def test_store_optional_chunk_is_physical_bounded_and_keeps_hash_and_fd(tmp_path):
    with store_fixture(tmp_path) as (_, store, _):
        raw = b'0123456789' * 1000
        ref = store.put(iter((raw,)), max_bytes=len(raw), media_type='application/octet-stream')
        chunks = list(store.read(ref, max_bytes=len(raw), chunk_bytes=127))
        assert b''.join(chunks) == raw and max(map(len, chunks)) == 127
        assert b''.join(store.read(ref, max_bytes=len(raw))) == raw
        used = store._file_state.used
        for invalid in (True, False, 0, -1, '127', store._bounds.max_row_bytes+1):
            with pytest.raises(DataflowError, match='chunk_bytes'):
                list(store.read(ref, max_bytes=len(raw), chunk_bytes=invalid))
            assert store._file_state.used == used
        with pytest.raises(DataflowError, match='max_bytes'):
            list(store.read(ref, max_bytes=len(raw)-1, chunk_bytes=127))
        assert store._file_state.used == used
        # Same old artifact validation remains in the original store reader.
        path = store._object_path(ref)
        path.write_bytes(b'X' + raw[1:])
        with pytest.raises(DataflowError, match='do not match'):
            list(store.read(ref, max_bytes=len(raw), chunk_bytes=127))
        assert store._file_state.used == used
        store.close()


@pytest.mark.parametrize('limited', [True, False])
def test_schema_change_conversion_retains_real_replay_prefetch(tmp_path, monkeypatch, limited):
    import pyarrow as pa
    from demiflow.execution import dataflow_terminals as terminals
    from demiflow.execution.action_driver.dataflow import DataflowExecution
    first = pa.Table.from_pylist([{'v': None}], schema=pa.schema([('v', pa.binary(16384))]))
    second = pa.Table.from_pylist([{'v': 'x'*500} for _ in range(32)])
    limits = {'timeout_s': 30, 'max_row_bytes': 32768, 'max_batch_bytes': 65536,
              'max_state_bytes': 65536}
    def work():
        return list(data.from_arrow([first, second]).iter_batches(batch_size=33, batch_format='pyarrow'))
    with pytest.raises(InterruptedAfterAction):
        with RunContext.start(tmp_path/'run', limits=limits) as run, data.local_execution(
                workers=1, memory_bytes=256*1024, worker_mode='thread'):
            ref = run.ref
            batches = work()
            assert batches[0].equals(first) and batches[1].equals(second)
            commits = run.journal.execution_commits('action-1')
            raise InterruptedAfterAction()
    def forbidden_launch(*args, **kwargs):
        raise AssertionError('schema replay launched a worker')
    monkeypatch.setattr(DataflowExecution, '_launch_phase', forbidden_launch)
    original = terminals._convert_batch
    reserves, constructor_calls = [], []
    def convert(rows, fmt, handle, **kw):
        reserve = handle.conversion_reserve()
        if len(rows) == 1:
            pending = sum(handle._conversion_read_sizes[1:])
            assert pending > 0 and reserve >= pending + handle.read_buffer_reserve()
            reserves.append(reserve)
            if limited:
                class Table:
                    @staticmethod
                    def from_pylist(*args, **kwargs):
                        constructor_calls.append(True)
                        raise AssertionError('allocation occurred before shared-window rejection')
                with monkeypatch.context() as patch:
                    patch.setattr(pa, 'Table', Table)
                    return original(rows, fmt, handle, **kw)
        return original(rows, fmt, handle, **kw)
    monkeypatch.setattr(terminals, '_convert_batch', convert)
    def resume():
        with RunContext.resume(ref) as run, data.local_execution(
                workers=1, memory_bytes=(64 if limited else 96)*1024, worker_mode='thread'):
            batches = work()
            assert batches[0].equals(first) and batches[1].equals(second)
            assert run.journal.execution_commits('action-1') == commits
    if limited:
        with pytest.raises(DataflowError, match='conversion admission'):
            resume()
    else:
        resume()
    assert reserves and not constructor_calls
    import json
    (tmp_path/'conversion-credit.json').write_text(json.dumps({
        'limited': limited, 'resident_bytes': (64 if limited else 96)*1024,
        'reserved_bytes': reserves, 'constructor_calls_on_rejection': constructor_calls,
        'final_ack_unchanged': run.journal.execution_commits('action-1') == commits,
    }, indent=2))
    assert _fd_count(tmp_path/'run') == 0


def test_suspended_replay_does_not_retain_previous_credit_callback():
    import gc
    import weakref
    from types import SimpleNamespace
    from demiflow.execution.dataflow_replay import PublishedRecordReader
    class Window:
        pass
    window = Window()
    live = weakref.ref(window)
    callback = lambda size, window=window: None
    reader = PublishedRecordReader({'manifest': {'partitions': []}}, store=None,
        bounds=SimpleNamespace(max_row_bytes=1024, max_resident_bytes=65536))
    reader._frames.close()
    def frames():
        yield (b'{}', '0'*64, 0, 0)
    reader._frames = frames()
    try:
        assert reader.peek(check_workspace=callback)[0] == b'{}'
        del callback, window
        gc.collect()
        assert live() is None
    finally:
        reader.close()
