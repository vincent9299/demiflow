"""Bounded, lazy decoding of the original ACKed terminal record frames.

This is a read cursor, not a recovery or charging authority. Original manifests,
segments and record bytes remain unchanged and are read through their store lease.
"""
from demiflow.execution.contracts.record_codec import record_encoded_bytes
from demiflow.execution.storage.streams import _ReplayLines
import json
import sys

from demiflow.execution.contracts.dataflow import ArtifactRef, BlockPartition
from demiflow.execution.contracts.dataflow_errors import DataflowError








class PublishedRecordReader:
    def __init__(self, publication, *, store, bounds):
        self._store, self._bounds = store, bounds
        self._chunk_bytes = min(4096, bounds.max_row_bytes, max(1, bounds.max_resident_bytes // 32))
        self._frames = self._read_frames(publication)
        self._arrow_record = None
        self._pending = None
        self._stream = None
        self._raw_bytes = 0
        self._check = lambda size: None

    @property
    def buffered_bytes(self):
        from demiflow.execution.outputs.arrow_rows import PublishedArrowRows
        arrow = self._pending.bytes if isinstance(self._pending, PublishedArrowRows) else 0
        return arrow + self._raw_bytes + (self._stream.buffered_bytes if self._stream is not None else 0)

    def _workspace(self, size):
        self._check(size)

    def _read_frames(self, publication):
        from itertools import groupby
        from demiflow.execution.storage.segments import ARROW_STREAM_MEDIA, tree_refs, SEGMENTS_MEDIA
        from demiflow.execution.storage.record_schema import RecordSchemas
        from demiflow.execution.outputs.arrow_rows import PublishedArrowRows
        schemas = None
        schema_ref = publication['manifest'].get('schema_ref')
        for partition in publication['manifest']['partitions']:
            root = ArtifactRef(**partition['data_ref'])
            entries = (tree_refs(self._store, root, self._bounds, leaves_only=True, with_offsets=True)
                       if root.media_type == SEGMENTS_MEDIA
                       else iter(((root, 0, partition['rows']),)))
            for arrow, leaves in groupby(entries, key=lambda item: item[0].media_type == ARROW_STREAM_MEDIA):
                if arrow:
                    if schema_ref is None:
                        raise DataflowError('CORRUPT_ARTIFACT', 'terminal Arrow results require their original schema')
                    if schemas is None:
                        schemas = RecordSchemas(store=self._store, bounds=self._bounds)
                    for ref, offset, rows in leaves:
                        self._workspace(6 * ref.size_bytes + 65536)
                        yield PublishedArrowRows(ref, rows=rows, first=offset, schema_ref=ArtifactRef(**schema_ref),
                                                 schemas=schemas)
                else:
                    def chunks():
                        for ref, _, _ in leaves:
                            yield from self._store.read(ref, max_bytes=max(1, ref.size_bytes), chunk_bytes=self._chunk_bytes)
                    yield from self._record_lines(chunks())

    def _record_lines(self, chunks):
        from demiflow.execution.contracts.record_codec import decode_frame, document_limit, line_limit, raw_workspace
        stream = _ReplayLines(chunks, chunk_bytes=self._chunk_bytes, check=self._workspace)
        self._stream = stream
        try:
            while True:
                line = stream.readline(line_limit(self._bounds))
                if not line:
                    break
                self._workspace(stream.buffered_bytes + raw_workspace(line, typed=False))
                raw, item = decode_frame(line, self._bounds, retained=stream.buffered_bytes)
                record_id, sequence = item['record_id'], item['sequence']
                del line, item
                size = record_encoded_bytes(raw, maximum=self._bounds.max_row_bytes,
                                            frame_maximum=document_limit(self._bounds))
                self._raw_bytes = len(raw)
                yield raw, record_id, sequence, size
                del raw
                self._raw_bytes = 0
        finally:
            stream.close()
            self._stream = None

    def peek_unit(self, *, check_workspace=None):
        self._check = check_workspace if check_workspace is not None else lambda size: None
        try:
            if self._pending is None:
                self._pending = next(self._frames)
            return self._pending
        finally:
            # A suspended reader must not retain the callback's already
            # delivered Record window while the caller converts/consumes it.
            self._check = lambda size: None

    def peek(self, *, check_workspace=None):
        from demiflow.execution.outputs.arrow_rows import PublishedArrowRows
        unit = self.peek_unit(check_workspace=check_workspace)
        if not isinstance(unit, PublishedArrowRows):
            return unit
        if self._arrow_record is None:
            from demiflow.execution.contracts.record_codec import encode_document
            record = unit.record()
            raw = encode_document(record, self._bounds, retained=unit.bytes)
            self._raw_bytes = len(raw)
            self._arrow_record = raw, record.record_id, record.sequence, record.encoded_bytes
        return self._arrow_record

    def pop_batch(self):
        from demiflow.execution.outputs.arrow_rows import PublishedArrowRows
        unit = self._pending
        if not isinstance(unit, PublishedArrowRows):
            return None
        self._pending = self._arrow_record = None
        self._raw_bytes = 0
        return unit

    def pop(self):
        from demiflow.execution.outputs.arrow_rows import PublishedArrowRows
        frame = self.peek()
        if isinstance(self._pending, PublishedArrowRows):
            self._pending.index += 1
            self._arrow_record = None
            self._raw_bytes = 0
            if self._pending.index < self._pending.batch.num_rows:
                return frame
        self._pending = None
        return frame

    def close(self):
        self._pending = self._arrow_record = None
        self._raw_bytes = 0
        self._frames.close()
