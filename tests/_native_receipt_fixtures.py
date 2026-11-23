"""Bounded, read-only inspection of this fixture's original committed row data."""
import json
from pathlib import Path
import demiflow

from demiflow.execution.contracts.dataflow import ArtifactRef, thaw_record_value
from demiflow.execution.storage.streams import _ChunkStream
if (Path(demiflow.__file__).parent / 'execution/storage/segments.py').is_file():
    from demiflow.execution.storage.segments import segment_refs
else:
    from demiflow.execution.dataflow_segments import segment_refs
from demiflow.execution.storage.objects import RunArtifactStore
from demiflow.execution.contracts.record_codec import _decode_record_document, _decode_payload
if (Path(demiflow.__file__).parent / 'execution/seal/sealed_plans.py').is_file():
    from demiflow.execution.seal.sealed_plans import load_sealed_plan
else:
    from demiflow.execution.sealed_plans import load_sealed_plan


def committed_rows(run, action_id, fact, sink_id='materialize'):
    """Inspect one ACKed prefix without fabricating a complete action/handle."""
    assert fact['action_id'] == action_id and fact['status'] == 'acked'
    receipt = fact['receipts'][sink_id]['data']
    assert 0 <= receipt['rows'] <= 1024
    bundle = load_sealed_plan(run.directory, action_id, version='v3')
    bounds = bundle['physical'].bounds
    root_ref = ArtifactRef(**receipt['ref'])
    store = RunArtifactStore(root=Path(run.directory) / '.dataflow-store', store_id=root_ref.store_id,
                             journal=None, bounds=bounds, readonly=True)
    try:
        publication = json.loads(b''.join(store.read(root_ref, max_bytes=bounds.max_control_bytes)))
        assert publication['schema'] == 'demiflow.dataflow_sink_publication.v3'
        manifest = publication['manifest']
        assert manifest['total_rows'] == receipt['rows']
        result = []
        from demiflow.execution.arrow_payload import ArrowBatchCache, is_arrow_record
        arrow = ArrowBatchCache(store, bounds)
        for partition in manifest['partitions']:
            for leaf in segment_refs(store, ArtifactRef(**partition['data_ref']), bounds):
                assert leaf.media_type == 'application/x-demiflow-records-v3'
                stream = _ChunkStream(store.read(leaf, max_bytes=max(1, leaf.size_bytes)))
                while True:
                    line = stream.readline(bounds.max_row_bytes + 1)
                    if not line:
                        break
                    assert len(line) <= bounds.max_row_bytes
                    item = json.loads(line)
                    record = _decode_record_document(item['document'].encode(),
                        record_id=item['record_id'], sequence=item['sequence'], max_bytes=len(line))
                    if is_arrow_record(record):
                        row = arrow.row(record)
                    elif record.inline is not None:
                        row = thaw_record_value(record.inline)
                    else:
                        row = _decode_payload(b''.join(store.read(record.payload_ref,
                            max_bytes=bounds.max_row_bytes)), max_bytes=bounds.max_row_bytes)
                    result.append(row)
                    assert len(result) <= receipt['rows']
        assert len(result) == receipt['rows']
        return result
    finally:
        store.close()
