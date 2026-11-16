"""The native reader owns one grant across indices, rows and constructors."""
import lance
import pyarrow as pa
import pytest

from demiflow import data
from demiflow.execution import RunContext
from demiflow.execution.contracts.dataflow_errors import DataflowError


def test_retained_python_rows_reject_before_the_next_decode_or_constructor(tmp_path, monkeypatch):
    import demiflow.execution.dataflow_record_frames as frames
    import demiflow.execution.batch_conversion as conversion
    original = frames.decode_document
    retained = []
    encoded = []
    constructed = []
    def decode(*args, **kwargs):
        retained.append(kwargs['retained'])
        result = original(*args, **kwargs)
        encoded.append(result.encoded_bytes)
        return result
    def forbidden(*args, **kwargs):
        constructed.append(True)
        raise AssertionError('constructor before shared native window admission')
    monkeypatch.setattr(frames, 'decode_document', decode)
    monkeypatch.setattr(conversion, 'native_arrow_table', forbidden)
    target = tmp_path/'blocked.lance'
    limits = dict(memory_bytes=4*1024**2, max_row_bytes=4096,
                  max_batch_bytes=65536, batch_rows=256)
    rows = [{'i':i, 'values':[0]*80} for i in range(160)]
    with pytest.raises(DataflowError, match='record frame codec workspace') as error:
        with RunContext.start(tmp_path/'run', limits=limits) as run, data.local_execution(batch_rows=256):
            data.from_items(rows).write_lance(str(target), mode='create')
    assert error.value.code == 'BUDGET_EXCEEDED'
    # Actual Python rows and the live index exceed their tiny record bytes;
    # the refusal may occur while advancing the index before the next decode.
    assert retained and encoded and max(retained) > 2 * sum(encoded)
    assert not constructed and not target.exists()
    assert run.journal.execution_action('action-1')['result'] is None


def test_multi_segment_wide_small_rows_share_index_buffer_and_writer_budget(tmp_path, monkeypatch):
    import demiflow.execution.dataflow_segments as segments
    original = segments.block_chunks
    seen = []
    def chunks(*args, **kwargs):
        seen.append((kwargs.get('frame_call'), kwargs.get('on_retained')))
        yield from original(*args, **kwargs)
    monkeypatch.setattr(segments, 'block_chunks', chunks)
    rows = [{'i':i, 'values':[i]*50, 'payload':b'x'*(3000 if i%11==0 else 10)} for i in range(96)]
    target = tmp_path/'mixed.lance'
    schema = pa.schema([('i',pa.int64()),('values',pa.list_(pa.int64())),('payload',pa.binary())])
    with RunContext.start(tmp_path/'run', limits=dict(memory_bytes=8*1024**2,
            max_row_bytes=8192,max_batch_bytes=16384,batch_rows=3)) as run, data.local_execution(batch_rows=3):
        receipt = data.from_items(rows).write_lance(str(target), mode='create', schema=schema, return_receipt=True)
        assert receipt.written_rows == 96
        assert run.journal.execution_commits('action-1')[-1]['status'] == 'acked'
    assert lance.dataset(str(target)).to_table().to_pylist() == rows
    assert seen and all(callable(call) and callable(retain) for call,retain in seen)


def test_previous_native_batch_coexists_with_the_next_wide_record(tmp_path):
    payload = b'wide\x00original' * 750000
    rows = [{'i':i, 'payload':payload} for i in range(2)]
    target = tmp_path/'wide-sequence.lance'
    with RunContext.start(tmp_path/'run') as run:
        receipt = data.from_items(rows).write_lance(str(target), mode='create',
            schema=pa.schema([('i',pa.int64()),('payload',pa.large_binary())]), return_receipt=True)
        assert receipt.written_rows == 2
        assert run.journal.execution_commits('action-1')[-1]['status'] == 'acked'
    assert lance.dataset(str(target)).to_table().to_pylist() == rows
