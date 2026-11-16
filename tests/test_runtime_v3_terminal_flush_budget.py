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
        from demiflow.execution.dataflow_store import RunArtifactStore
        from demiflow.execution.dataflow_segments import RECORDS_MEDIA
        from demiflow.execution.dataflow_scratch import ScratchStream
        if getattr(TerminalBlocks, '_owned_flush_trace', None) != trace:
            append, put, read = TerminalBlocks.append, RunArtifactStore.put, ScratchStream.read
            active = {}
            def appended(blocks, record, raw):
                tag = record.block_partition if blocks.partitioned else None
                key = ((tag.occurrence_id, tag.index) if tag is not None
                       else (blocks.partition_id, -1))
                switching = blocks._tail is not None and blocks._tail_key != key
                if switching:
                    # Observe the actual live caller buffer; do not allocate a
                    # substitute or bypass the original scratch/store writer.
                    frame = sys._getframe(1)
                    wire = frame.f_locals.get('delivery_raw')
                    active.update(tail_id=id(blocks._tail), spool_bytes=len(raw),
                        row_window=blocks.bounds.max_row_bytes, batch_window=blocks.bounds.max_batch_bytes,
                        held_wire_bytes=len(wire) if type(wire) is bytes else 0,
                        payload_window=blocks.bounds.max_batch_bytes + blocks.bounds.max_row_bytes)
                    del frame, wire
                try:
                    return append(blocks, record, raw)
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
                    chunks = observed_chunks()
                return put(store, chunks, max_bytes=max_bytes, media_type=media_type)
            def read_tail(stream, count=-1):
                raw = read(stream, count)
                if active and id(stream) == active['tail_id'] and raw:
                    # The original writer still holds its previous chunk/view
                    # while it asks the generator for the next scratch read.
                    frame = sys._getframe(1)
                    live = {id(raw): len(raw)}
                    previous = 0
                    for _ in range(12):
                        if frame is None:
                            break
                        if frame.f_code.co_name == '_put_owned':
                            chunk = frame.f_locals.get('chunk')
                            if type(chunk) is bytes:
                                previous = len(chunk)
                                live[id(chunk)] = len(chunk)
                            del chunk
                            break
                        frame = frame.f_back
                    del frame
                    with open(read_trace, 'a') as output:
                        output.write(json.dumps({**active, 'current_chunk_bytes': len(raw),
                            'previous_chunk_bytes': previous, 'live_tail_bytes': sum(live.values()),
                            'scratch_io_window': stream._io}) + '\n')
                return raw
            ScratchStream.read = read_tail
            TerminalBlocks._owned_flush_trace = trace
            TerminalBlocks.append = appended
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
        assert item['spool_bytes'] <= item['row_window']
        assert original <= item['payload_window'], item
        assert original + item['held_wire_bytes'] <= item['payload_window'], item

    reads = [json.loads(line) for line in Path(read_trace).read_text().splitlines()]
    assert reads
    if row_count == 6:
        assert any(item['previous_chunk_bytes'] for item in reads)
    for item in reads:
        assert item['current_chunk_bytes'] <= item['scratch_io_window']
        assert item['live_tail_bytes'] <= item['batch_window'], item
        assert item['spool_bytes'] + item['live_tail_bytes'] + item['held_wire_bytes'] <= item['payload_window'], item
