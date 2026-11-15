"""Bounded, lazy decoding of the original ACKed terminal record frames.

This is a read cursor, not a recovery or charging authority. Original manifests,
segments and record bytes remain unchanged and are read through their store lease.
"""
import json

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


def read_buffer_reserve(bounds):
    # _ChunkStream may hold an unfinished line plus the next bounded store
    # chunk (<2 row frames). The upstream generator still owns that chunk,
    # and one extracted wire frame can coexist. Keep these four encoded-frame
    # windows out of collected business data.
    # This is not an estimate of arbitrary Python/Arrow object RSS.
    return 4 * (bounds.max_row_bytes + 1)


class PublishedRecordReader:
    def __init__(self, publication, *, store, bounds):
        self._store, self._bounds = store, bounds
        self._frames = self._read_frames(publication)
        self._pending = None

    def _read_frames(self, publication):
        from .dataflow_handles import _ChunkStream
        from .dataflow_segments import block_chunks
        for partition in publication['manifest']['partitions']:
            ref = ArtifactRef(**partition['data_ref'])
            stream = _ChunkStream(block_chunks(self._store, ref, self._bounds))
            try:
                while True:
                    line = stream.readline(self._bounds.max_row_bytes + 1)
                    if not line:
                        break
                    text = line.decode('utf-8')
                    del line
                    item = json.loads(text)
                    del text
                    record_id, sequence = item['record_id'], item['sequence']
                    raw = item.pop('document').encode('utf-8')
                    del item
                    size = record_encoded_bytes(raw, maximum=self._bounds.max_row_bytes)
                    yield raw, record_id, sequence, size
                    del raw
            finally:
                stream.close()

    def peek(self):
        if self._pending is None:
            self._pending = next(self._frames)
        return self._pending

    def pop(self):
        frame = self.peek()
        self._pending = None
        return frame

    def close(self):
        self._pending = None
        self._frames.close()
