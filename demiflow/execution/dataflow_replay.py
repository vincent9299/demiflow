"""Bounded, lazy decoding of the original ACKed terminal record frames.

This is a read cursor, not a recovery or charging authority. Original manifests,
segments and record bytes remain unchanged and are read through their store lease.
"""
import json
import sys

from .contracts.dataflow import ArtifactRef, BlockPartition
from .contracts.dataflow_errors import DataflowError


def record_encoded_bytes(raw, *, maximum):
    """Read the canonical control prefix without decoding the business payload.

    Record documents sort keys: optional ``bp`` precedes ``eb``; the potentially
    large typed payload ``p`` follows it. Full decoding still verifies canonical
    bytes, IDs, provenance, schema and the declared payload size after admission.
    """
    if type(raw) is not bytes or len(raw) > maximum:
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
        self._pending = None
        self._stream = None
        self._raw_bytes = 0
        self._check = lambda size: None

    @property
    def buffered_bytes(self):
        return self._raw_bytes + (self._stream.buffered_bytes if self._stream is not None else 0)

    def _workspace(self, size):
        self._check(size)

    def _read_frames(self, publication):
        from .dataflow_segments import segment_refs
        from ..data.runtime_contract import check_size
        for partition in publication['manifest']['partitions']:
            root = ArtifactRef(**partition['data_ref'])
            def chunks():
                for ref in segment_refs(self._store, root, self._bounds):
                    # Preserve each original digest, file lifetime and lease.
                    yield from self._store.read(ref, max_bytes=max(1, ref.size_bytes),
                                                chunk_bytes=self._chunk_bytes)
            stream = _ReplayLines(chunks(), chunk_bytes=self._chunk_bytes, check=self._workspace)
            self._stream = stream
            try:
                while True:
                    line = stream.readline(self._bounds.max_row_bytes + 1)
                    if not line:
                        break
                    # CPython UTF-8 decoding uses one byte/codepoint for
                    # ASCII and at most four otherwise. The outer record is
                    # metadata plus one serialized document, not row data.
                    text_bound = len(line) + 64 if line.isascii() else 4 * len(line) + 80
                    self._workspace(stream.buffered_bytes + len(line) + text_bound)
                    text = line.decode('utf-8')
                    del line
                    # JSON unescaping cannot produce more codepoints than
                    # the original text. Include the outer dict/scalars.
                    self._workspace(stream.buffered_bytes + sys.getsizeof(text) + 4 * len(text) + 1024)
                    item = json.loads(text)
                    del text
                    record_id, sequence = item['record_id'], item['sequence']
                    document = item['document']
                    raw_bound = len(document) if document.isascii() else 4 * len(document)
                    self._workspace(stream.buffered_bytes + check_size(item, self._bounds.max_resident_bytes) + raw_bound)
                    raw = document.encode('utf-8')
                    del document, item
                    size = record_encoded_bytes(raw, maximum=self._bounds.max_row_bytes)
                    self._raw_bytes = len(raw)
                    yield raw, record_id, sequence, size
                    del raw
                    self._raw_bytes = 0
            finally:
                stream.close()
                self._stream = None

    def peek(self, *, check_workspace=None):
        self._check = check_workspace if check_workspace is not None else lambda size: None
        try:
            if self._pending is None:
                self._pending = next(self._frames)
            return self._pending
        finally:
            # A suspended reader must not retain the callback's already
            # delivered Record window while the caller converts/consumes it.
            self._check = lambda size: None

    def pop(self):
        frame = self.peek()
        self._pending = None
        return frame

    def close(self):
        self._pending = None
        self._raw_bytes = 0
        self._frames.close()
