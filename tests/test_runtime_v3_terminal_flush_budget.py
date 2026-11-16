"""A block switch must retain the terminal's original simultaneous byte window."""
import json

from demiflow import data
from demiflow.execution.run_context import RunContext


def test_real_block_switch_does_not_hold_extra_wire_during_old_tail_flush(tmp_path):
    trace = str(tmp_path / 'actual-flush-residency.jsonl')
    def observe(row):
        import sys
        from demiflow.execution.worker.terminal_blocks import TerminalBlocks
        from demiflow.execution.dataflow_store import RunArtifactStore
        from demiflow.execution.dataflow_segments import RECORDS_MEDIA
        if getattr(TerminalBlocks, '_owned_flush_trace', None) != trace:
            append, put = TerminalBlocks.append, RunArtifactStore.put
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
                    active.update(spool_bytes=len(raw),
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
            TerminalBlocks._owned_flush_trace = trace
            TerminalBlocks.append = appended
            RunArtifactStore.put = stored
        return row

    expected = [{'n': i, 'text': 'x' * 6000} for i in range(2)]
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
        assert original <= item['payload_window']
        assert original + item['held_wire_bytes'] <= item['payload_window'], item
