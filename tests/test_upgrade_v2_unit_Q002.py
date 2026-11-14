"""Q002 verify-preserving unit tests: BlockSink.append/commit/iter_rows (CODEC/BOUNDS).

append is unchanged production code; these prove the C06 value fidelity survives
a real durable round-trip (signed-zero, distinct NaN payloads, tuple/bytes types),
that check_size rejects an oversize row before any encode/artifact, that the output
row budget is enforced, and that multi-block publication preserves row order.
"""
import math
import pickle
import struct

import pytest

from demiflow.data.runtime_contract import RuntimeContractError
from demiflow.execution.block_sink import BlockSink
from demiflow.execution.commit import CommitCoordinator
from demiflow.execution.recovery_contract import RecoveryContractError, SourcePosition
from demiflow.execution.run_context import RunContext


def _commit(tmp_path, rows, limits, block_bytes=None):
    with RunContext.start(tmp_path / 'run', limits=limits) as run:
        with run.action({'kind': 'materialize', 'source': {'fixture': 'q002'}}) as action:
            sink = BlockSink(action)
            if block_bytes is not None:
                sink.block_bytes = block_bytes
            fact = CommitCoordinator(action).commit(
                1, outputs={'output': (sink, iter(rows))}, position=SourcePosition('q002', len(rows)))
            prepared = fact['prepared']['sinks']['output']
            out = list(sink.iter_rows(prepared))
            action.complete({'rows': len(rows)})
    return out, prepared


def test_signed_zero_nan_tuple_bytes_round_trip(tmp_path):
    nan = struct.unpack('>d', bytes.fromhex('7ff8000000000001'))[0]
    rows = [{'v': -0.0}, {'v': 0.0}, {'t': (1, b'\x00\xff\x00')}, {'n': nan}]
    out, _ = _commit(tmp_path, rows, {'max_row_bytes': 64 * 1024, 'max_batch_bytes': 64 * 1024})
    assert math.copysign(1.0, out[0]['v']) == -1.0
    assert math.copysign(1.0, out[1]['v']) == 1.0
    assert pickle.dumps(out[0], 4) != pickle.dumps(out[1], 4)  # -0.0 and 0.0 stay distinct
    assert out[2]['t'] == (1, b'\x00\xff\x00')
    assert type(out[2]['t']) is tuple and type(out[2]['t'][1]) is bytes
    assert struct.pack('>d', out[3]['n']) == bytes.fromhex('7ff8000000000001')


def test_check_size_rejects_oversize_before_encode(tmp_path):
    with RunContext.start(tmp_path / 'run', limits={'max_row_bytes': 1024, 'max_batch_bytes': 1024}) as run:
        with run.action({'kind': 'materialize', 'source': {'fixture': 'q002'}}) as action:
            sink = BlockSink(action)
            with pytest.raises(RuntimeContractError, match='byte budget'):
                sink.append({'payload': b'x' * 4096})
            assert sink.rows == 0 and sink.blocks == []  # rejected before pickle/artifact
            action.complete({'rows': 0})


def test_output_row_budget_enforced(tmp_path):
    with RunContext.start(tmp_path / 'run', limits={
            'max_row_bytes': 4096, 'max_batch_bytes': 4096, 'max_output_rows': 2}) as run:
        with run.action({'kind': 'materialize', 'source': {'fixture': 'q002'}}) as action:
            sink = BlockSink(action)
            sink.append({'id': 0})
            sink.append({'id': 1})
            with pytest.raises(RecoveryContractError, match='row budget'):
                sink.append({'id': 2})
            action.complete({'rows': 0})


def test_multi_block_order_preserved(tmp_path):
    rows = [{'id': i, 'payload': bytes(64)} for i in range(30)]
    out, prepared = _commit(tmp_path, rows, {'max_row_bytes': 4096, 'max_batch_bytes': 4096}, block_bytes=64)
    assert len(prepared['blocks']) > 1  # small blocks force multiple pages
    assert out == rows