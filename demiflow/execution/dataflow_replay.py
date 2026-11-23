"""Bounded, lazy decoding of the original ACKed terminal record frames.

This is a read cursor, not a recovery or charging authority. Original manifests,
segments and record bytes remain unchanged and are read through their store lease.
"""
import json
import sys

from .contracts.dataflow import ArtifactRef, BlockPartition
from .contracts.dataflow_errors import DataflowError


def record_encoded_bytes(raw, *, maximum, frame_maximum=None):
    """Read the canonical control prefix without decoding the business payload.

    Record documents sort keys: optional ``bp`` precedes ``eb``; the potentially
    large typed payload ``p`` follows it. Full decoding still verifies canonical
    bytes, IDs, provenance, schema and the declared payload size after admission.
    """
    if type(raw) is not bytes or len(raw) > (maximum if frame_maximum is None else frame_maximum):
        raise DataflowError('BUDGET_EXCEEDED', 'terminal record frame exceeds its row bound')
    try:
        cursor = len(b'{"eb":')
        if raw.startswith(b'{"bp":'):
            end = raw.find(b'},"eb":', 6, 1024)
            if end < 0:
                raise ValueError('block header exceeds its bounded control prefix')
            BlockPartition(**json.loads(raw[6:end + 1]))
            cursor = end + len(b'},"eb":')
        elif not raw.startswith(b'{"eb":'):
            raise ValueError('record size is not in the canonical header')
        end = raw.find(b',', cursor, cursor + 32)
        digits = raw[cursor:end]
        if end < 0 or not digits or not digits.isdigit():
            raise ValueError('invalid record size')
        size = int(digits)
        if size > maximum:
            raise DataflowError('BUDGET_EXCEEDED', 'terminal payload exceeds its row bound')
        return size
    except (ValueError, TypeError, UnicodeError, IndexError) as exc:
        raise DataflowError('CORRUPT_ARTIFACT', 'invalid terminal record header') from exc


class _ReplayLines:
    """Physical small-chunk reads; every growth is admitted before allocation."""
    def __init__(self, chunks, *, chunk_bytes, check):
        self._chunks = iter(chunks)
        self._chunk_bytes, self._check = chunk_bytes, check
        self._buffer = bytearray()
        self._held_chunk = 0
        self._eof = False

    @property
    def buffered_bytes(self):
        return sys.getsizeof(self._buffer) + self._held_chunk

    def readline(self, maximum):
        while True:
            index = self._buffer.find(b'\n')
            size = index + 1 if index >= 0 else len(self._buffer) if self._eof else None
            if size is not None:
                if size > maximum:
                    raise DataflowError('BUDGET_EXCEEDED', 'stored record frame exceeds its bound')
                # Extracted bytes and bytearray's old/new storage during shrink.
                self._check(2 * sys.getsizeof(self._buffer) + self._held_chunk + size)
                line = bytes(memoryview(self._buffer)[:size])
                del self._buffer[:size]
                return line
            if len(self._buffer) > maximum:
                raise DataflowError('BUDGET_EXCEEDED', 'stored record frame exceeds its bound')
            # The underlying generator retains its last chunk while fetching
            # the next one; extend can briefly retain old and grown bytearrays.
            self._check(2 * sys.getsizeof(self._buffer) + self._held_chunk + 3 * self._chunk_bytes)
            try:
                chunk = next(self._chunks)
            except StopIteration:
                self._held_chunk = 0
                self._eof = True
                continue
            # Advancing chunks may retain new segment-index metadata. Its
            # physical chunk allocation has already been admitted by the
            # original reader; recheck the complete buffer growth with that
            # metadata now live, before bytearray.extend allocates its copy.
            # Keep the previous held chunk charged until this second check.
            self._check(2 * sys.getsizeof(self._buffer) + self._held_chunk + 3 * self._chunk_bytes)
            self._held_chunk = len(chunk)
            self._buffer.extend(chunk)
            del chunk

    def close(self):
        self._buffer.clear()
        self._held_chunk = 0
        self._eof = True
        self._chunks.close()


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
        from .outputs.arrow_rows import PublishedArrowRows
        arrow = self._pending.bytes if isinstance(self._pending, PublishedArrowRows) else 0
        return arrow + self._raw_bytes + (self._stream.buffered_bytes if self._stream is not None else 0)

    def _workspace(self, size):
        self._check(size)

    def _read_frames(self, publication):
        from itertools import groupby
        from .dataflow_segments import ARROW_STREAM_MEDIA, tree_refs, SEGMENTS_MEDIA
        from .record_schema import RecordSchemas
        from .outputs.arrow_rows import PublishedArrowRows
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
        from .dataflow_record_frames import decode_frame, document_limit, line_limit, raw_workspace
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
        from .outputs.arrow_rows import PublishedArrowRows
        unit = self.peek_unit(check_workspace=check_workspace)
        if not isinstance(unit, PublishedArrowRows):
            return unit
        if self._arrow_record is None:
            from .dataflow_record_frames import encode_document
            record = unit.record()
            raw = encode_document(record, self._bounds, retained=unit.bytes)
            self._raw_bytes = len(raw)
            self._arrow_record = raw, record.record_id, record.sequence, record.encoded_bytes
        return self._arrow_record

    def pop_batch(self):
        from .outputs.arrow_rows import PublishedArrowRows
        unit = self._pending
        if not isinstance(unit, PublishedArrowRows):
            return None
        self._pending = self._arrow_record = None
        self._raw_bytes = 0
        return unit

    def pop(self):
        from .outputs.arrow_rows import PublishedArrowRows
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
