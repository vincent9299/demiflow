"""A block switch must retain the terminal's original simultaneous byte window."""
import json

import pytest

from demiflow import data
from demiflow.execution.run_context import RunContext


@pytest.mark.parametrize("row_count", [2, 6])
def test_real_block_switch_does_not_hold_extra_wire_during_old_tail_flush(tmp_path, row_count):
    trace = str(tmp_path / 'actual-flush-residency.jsonl')
    read_trace = str(tmp_path / 'actual-read-residency.jsonl')
    def observe(row):
        import sys
        from demiflow.execution.worker.terminal_blocks import TerminalBlocks
        from demiflow.execution.storage.objects import RunArtifactStore
        from demiflow.execution.storage.segments import RECORDS_MEDIA
        from demiflow.execution.storage.scratch import ScratchStream
        if getattr(TerminalBlocks, '_owned_flush_trace', None) != trace:
            append, put, read = TerminalBlocks.append_parts, RunArtifactStore.put, ScratchStream.read
            active = {}
            def appended(blocks, record, chunks, *, size):
                tag = record.block_partition if blocks.partitioned else None
                key = ((tag.occurrence_id, tag.index) if tag is not None
                       else (blocks.partition_id, -1))
                switching = blocks._tail is not None and blocks._tail_key != key
                if switching:
                    # Observe the actual live caller buffer; do not allocate a
                    # substitute or bypass the original scratch/store writer.
                    frame = sys._getframe(1)
                    wire = frame.f_locals.get('delivery_raw')
                    active.update(tail_id=id(blocks._tail), spool_bytes=size,
                        row_window=blocks.bounds.max_row_bytes, batch_window=blocks.bounds.max_batch_bytes,
                        held_wire_bytes=len(wire) if type(wire) is bytes else 0,
                        payload_window=blocks.bounds.max_batch_bytes + blocks.bounds.max_row_bytes)
                    del frame, wire
                try:
                    return append(blocks, record, chunks, size=size)
                finally:
                    active.clear()
            def stored(store, chunks, *, max_bytes, media_type):
                if active and media_type == RECORDS_MEDIA:
                    original_chunks = chunks
                    def observed_chunks():
                        for chunk in original_chunks:
                            with open(trace, 'a') as output:
                                output.write(json.dumps({**active, 'old_tail_chunk_bytes': len(chunk)}) + '\n')
                            yield chunk
                            del chunk
                    chunks = observed_chunks()
                return put(store, chunks, max_bytes=max_bytes, media_type=media_type)
            def read_tail(stream, count=-1):
                previous = {}
                observing = bool(active and id(stream) == active['tail_id'])
                if observing:
                    # Include both the tail generator preparing the next read and
                    # the artifact writer. Their references may alias one buffer.
                    frame = sys._getframe(1)
                    for _ in range(12):
                        if frame is None:
                            break
                        if frame.f_code.co_name in {'chunks', '_put_owned'}:
                            chunk = frame.f_locals.get('chunk')
                            if type(chunk) is bytes:
                                previous[id(chunk)] = len(chunk)
                            del chunk
                            if frame.f_code.co_name == '_put_owned':
                                break
                        frame = frame.f_back
                    del frame
                # Inspect before reading so the observer never retains a stale
                # frame-local snapshot across the next physical allocation.
                raw = read(stream, count)
                if observing and raw:
                    live = {**previous, id(raw): len(raw)}
                    with open(read_trace, 'a') as output:
                        output.write(json.dumps({**active, 'current_chunk_bytes': len(raw),
                            'previous_chunk_bytes': sum(previous.values()), 'live_tail_bytes': sum(live.values()),
                            'scratch_io_window': stream._io}) + '\n')
                return raw
            ScratchStream.read = read_tail
            TerminalBlocks._owned_flush_trace = trace
            TerminalBlocks.append_parts = appended
            RunArtifactStore.put = stored
        return row

    expected = [{'n': i, 'text': 'x' * 6000} for i in range(row_count)]
    limits = {'max_row_bytes': 8192, 'max_batch_bytes': 8192, 'timeout_s': 60}
    with RunContext.start(tmp_path / 'run', limits=limits) as run:
        actual = data.from_items(expected).repartition(2).map(observe).filter(
            lambda row: True).map(lambda row: row).take_all()
        assert actual == expected
        commits = run.journal.execution_commits('action-1')
        assert commits and all(item['status'] == 'acked' for item in commits)
    from pathlib import Path
    observations = [json.loads(line) for line in Path(trace).read_text().splitlines()]
    assert observations
    for item in observations:
        original = item['spool_bytes'] + item['old_tail_chunk_bytes']
        assert item['spool_bytes'] <= item['row_window'] + 1
        assert original <= item['payload_window'], item
        assert original + item['held_wire_bytes'] <= item['payload_window'], item

    reads = [json.loads(line) for line in Path(read_trace).read_text().splitlines()]
    assert reads
    if row_count == 6:
        assert any(item['previous_chunk_bytes'] for item in reads)
    for item in reads:
        assert item['current_chunk_bytes'] <= item['scratch_io_window']
        assert item['live_tail_bytes'] <= item['batch_window'] - 1, item
        assert item['spool_bytes'] + item['live_tail_bytes'] + item['held_wire_bytes'] <= item['payload_window'], item


@pytest.mark.parametrize('batch_bytes', [8193, 262144])
def test_real_store_chunking_preserves_segment_hashes_and_restore(tmp_path, monkeypatch, batch_bytes):
    from hashlib import sha256
    from types import SimpleNamespace
    from demiflow.execution.contracts.dataflow import ArtifactRef, BlockPartition
    from demiflow.execution.storage.scratch import ScratchStream
    from demiflow.execution.storage.segments import block_chunks, segment_refs
    from demiflow.execution.worker.terminal_blocks import TerminalBlocks
    from test_runtime_v3_artifact_accounting import store_fixture
    from test_runtime_v3_c import bounds, make

    limits = bounds(max_batch_bytes=batch_bytes, max_row_bytes=1024,
                    max_control_bytes=8192, max_state_bytes=1024*1024)
    read = ScratchStream.read
    requested = []
    def observe(stream, count=-1):
        requested.append((count, stream._io))
        return read(stream, count)
    monkeypatch.setattr(ScratchStream, 'read', observe)
    with store_fixture(tmp_path) as (_, store, _):
        services = SimpleNamespace(store=store, bounds=limits)
        state = TerminalBlocks(services, 'worker', declaration=(sha256(b'layout').hexdigest(), 2))
        rows = [(json.dumps({'n': i, 'payload': 'x'*900})+'\n').encode() for i in range(40)]
        for i, raw in enumerate(rows):
            tag = BlockPartition(sha256(b'layout').hexdigest(), i // 20, 2)
            state.append(make('Record', block_partition=tag), raw)
        partitions = state.manifest_partitions()
        assert [p['rows'] for p in partitions] == [20, 20]
        for i, part in enumerate(partitions):
            expected = b''.join(rows[i*20:(i+1)*20])
            root = ArtifactRef(**part['data_ref'])
            leaves = list(segment_refs(store, root, limits))
            assert len(leaves) == 1
            assert leaves[0].digest == sha256(expected).hexdigest()
            assert leaves[0].size_bytes == len(expected)
            assert b''.join(block_chunks(store, root, limits)) == expected
        state.close()
        restored = TerminalBlocks(services, 'worker', declaration=(sha256(b'layout').hexdigest(), 2))
        manifest = {'rows': 40, 'bytes': sum(map(len, rows)), 'partitions': partitions}
        restored.restore(manifest)
        assert restored.manifest_partitions() == partitions
        restored.close()
    assert requested
    assert all(0 < count <= min(65536, (batch_bytes - 1) // 2, io) for count, io in requested)
    assert max(count for count, _ in requested) == min(65536, (batch_bytes - 1) // 2, 9216)


@pytest.mark.parametrize('batch_bytes', [1, 2])
def test_impossible_batch_rejects_before_scratch_read(tmp_path, monkeypatch, batch_bytes):
    from types import SimpleNamespace
    from demiflow.execution.contracts.dataflow_errors import DataflowError
    from demiflow.execution.storage.scratch import ScratchStream
    from demiflow.execution.worker.terminal_blocks import TerminalBlocks
    from test_runtime_v3_artifact_accounting import store_fixture
    from test_runtime_v3_c import bounds, make
    with store_fixture(tmp_path) as (_, store, _):
        state = TerminalBlocks(SimpleNamespace(store=store, bounds=bounds(max_batch_bytes=batch_bytes)), 'worker')
        state.append(make('Record'), b'{}\n')
        def forbidden(*args):
            raise AssertionError('scratch read happened before impossible-window refusal')
        monkeypatch.setattr(ScratchStream, 'read', forbidden)
        try:
            with pytest.raises(DataflowError, match='consecutive scratch chunks'):
                state.flush()
        finally:
            state.close()


def test_exact_original_document_limit_plus_newline_remains_legal(tmp_path):
    from types import SimpleNamespace
    from demiflow.execution.contracts.dataflow_errors import DataflowError
    from demiflow.execution.dataflow_sinks import _encode_record
    from demiflow.execution.worker.terminal_blocks import TerminalBlocks
    from test_runtime_v3_artifact_accounting import store_fixture
    from test_runtime_v3_c import bounds, make
    row = {'value': 'x'*32}
    from demiflow.execution.exchange.dataflow_bridge import _encode_payload
    record = make('Record', inline=row, encoded_bytes=len(_encode_payload(row, max_bytes=8192)))
    raw = _encode_record(record, bounds()) + b'\n'
    limits = bounds(max_row_bytes=len(raw)-1)
    # The original full encoder accepts exactly max_row bytes; the existing
    # newline is reserved from the batch window, not a new row rejection.
    assert _encode_record(record, limits) + b'\n' == raw
    with store_fixture(tmp_path) as (_, store, _):
        state = TerminalBlocks(SimpleNamespace(store=store, bounds=limits), 'worker')
        state.append(record, raw)
        parts = state.manifest_partitions()
        assert state.rows == 1 and state.bytes == len(raw)
        from demiflow.execution.contracts.dataflow import ArtifactRef
        from demiflow.execution.storage.segments import block_chunks
        assert b''.join(block_chunks(store, ArtifactRef(**parts[0]['data_ref']), limits)) == raw
        state.close()
