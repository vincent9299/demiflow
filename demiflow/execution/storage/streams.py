"""Streams implementation."""
from __future__ import annotations

from demiflow.execution.contracts.record_codec import row_upper, mapped_row_bytes, mapped_member, _window, _resident_budget, MAPPED_ARROW_MEDIA

from demiflow.execution.contracts.record_codec import _MAGIC as _ARROW_MAGIC, _HEADER as _ARROW_HEADER

import hashlib
import json
from demiflow.execution.contracts.state_codec import PACK_MEDIA, _MAGIC, _MAX_MEMBERS, _HEADER_BYTES, pack_limit

from demiflow.execution.contracts.record_codec import document_limit, line_limit, read_chunk_bytes, _frame_budget_error, _admit, resident_limit, decode_frame

import sys
from demiflow.execution.contracts.dataflow_errors import DataflowError


class _ChunkStream:
    """File-like view over a bounded store chunk iterator for incremental unpickling.

    Only the bytes needed for the current row are retained; the whole block/result is
    never materialised before slicing.
    """

    def __init__(self, chunks) -> None:
        self._it = iter(chunks)
        self._buf = bytearray()
        self._eof = False

    def _pull(self) -> bool:
        if self._eof:
            return False
        try:
            chunk = next(self._it)
        except StopIteration:
            self._eof = True
            return False
        self._buf.extend(chunk)
        return True

    def read(self, n: int = -1) -> bytes:
        if n is None or n < 0:
            while self._pull():
                pass
            data = bytes(self._buf)
            self._buf.clear()
            return data
        while len(self._buf) < n and self._pull():
            pass
        data = bytes(self._buf[:n])
        del self._buf[:n]
        return data

    def readline(self, max_bytes: int) -> bytes:
        while True:
            index = self._buf.find(b"\n")
            if index >= 0:
                if index + 1 > max_bytes:
                    raise DataflowError("BUDGET_EXCEEDED", "stored record frame exceeds its bound")
                data = bytes(memoryview(self._buf)[: index + 1])
                del self._buf[: index + 1]
                return data
            if len(self._buf) > max_bytes:
                raise DataflowError("BUDGET_EXCEEDED", "stored record frame exceeds its bound")
            if not self._pull():
                data = bytes(self._buf)
                self._buf.clear()
                return data

    def close(self):
        self._eof = True
        self._buf.clear()
        close = getattr(self._it, 'close', None)
        if close is not None:
            close()


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


def read_document(ref, store, bounds, *, resident_bytes=None, retained=0, _maximum=None):
    """Bound existing artifact bytes before reading, including legacy stores."""
    size = ref.size_bytes
    maximum = document_limit(bounds) if _maximum is None else min(_maximum, line_limit(bounds))
    if type(size) is not int or not 0 <= size <= maximum:
        _frame_budget_error('stored document exceeds frame byte bound')
    from demiflow.execution.storage.objects import RunArtifactStore
    chunk_size = min(size, read_chunk_bytes(bounds, resident_bytes)) if isinstance(store, RunArtifactStore) else size
    _admit(retained + 2 * size + 2 * chunk_size + 512, resident_limit(bounds, resident_bytes))
    options = {'chunk_bytes':max(1, chunk_size)} if isinstance(store, RunArtifactStore) else {}
    output = bytearray(size)
    destination = memoryview(output)
    offset = 0
    iterator = store.read(ref, max_bytes=maximum, **options)
    try:
        for chunk in iterator:
            if type(chunk) is not bytes or offset + len(chunk) > size:
                raise DataflowError('CORRUPT_ARTIFACT', 'stored document differs from its reference size')
            destination[offset:offset + len(chunk)] = chunk
            offset += len(chunk)
        chunk = None
        if offset != size:
            raise DataflowError('CORRUPT_ARTIFACT', 'stored document is truncated')
        return bytes(output)
    finally:
        close = getattr(iterator, 'close', None)
        if close is not None: close()

def frame_stream(chunks, bounds, *, resident_bytes=None, check_workspace=None):

    return _ReplayLines(chunks, chunk_bytes=read_chunk_bytes(bounds, resident_bytes),
        check=(check_workspace if check_workspace is not None
               else lambda size: _admit(size, resident_limit(bounds, resident_bytes))))

def read_pending_frame(ref, store, bounds, *, resident_bytes):
    raw = read_document(ref, store, bounds, resident_bytes=resident_bytes, _maximum=line_limit(bounds))
    return decode_frame(raw, bounds, resident_bytes=resident_bytes, newline=False)


def iter_kernel_images(store, ref, bounds):
    """Yield verified member bytes, then authenticate the original outer object.

    A caller must exhaust this iterator before acting on any returned member.
    GC only mutates after its entire root trace; read_kernel_image retains just
    the requested member and exhausts all other members before returning.
    """

    limit = pack_limit(bounds)
    if ref.media_type != PACK_MEDIA or ref.size_bytes > limit:
        raise DataflowError('BUDGET_EXCEEDED', 'kernel pack exceeds original recovery workspace')
    stream = _ChunkStream(store.read(ref, max_bytes=max(1, limit)))
    seen = set()
    try:
        if stream.readline(len(_MAGIC)) != _MAGIC:
            raise ValueError('kernel pack version')
        count = stream.readline(2)
        if count not in tuple(str(n).encode() + b'\n' for n in range(2, _MAX_MEMBERS + 1)):
            raise ValueError('kernel pack member count')
        for _ in range(int(count)):
            value = json.loads(stream.readline(_HEADER_BYTES))
            if (type(value) is not list or len(value) != 4
                    or type(value[0]) is not str or not 1 <= len(value[0]) <= 64
                    or type(value[1]) is not str or not 1 <= len(value[1]) <= 128
                    or type(value[2]) is not int or not 1 <= value[2] <= limit
                    or type(value[3]) is not str or len(value[3]) != 64
                    or any(c not in '0123456789abcdef' for c in value[3])):
                raise ValueError('kernel pack member header')
            key = (value[0], value[1])
            if key in seen:
                raise ValueError('duplicate kernel pack member')
            seen.add(key)
            raw = stream.read(value[2])
            if (len(raw) != value[2] or hashlib.sha256(raw).hexdigest() != value[3]
                    or stream.read(1) != b'\n'):
                raise ValueError('kernel pack member digest/length')
            yield key, raw
            del raw
        if stream.read(1):
            raise ValueError('kernel pack trailing bytes')
    except (ValueError, TypeError, UnicodeError) as error:
        raise DataflowError('CORRUPT_ARTIFACT', str(error)) from error
    finally:
        stream.close()


def put_arrow_batch(batch, raw_header, estimate, media_type, store):
    """Stream one pre-admitted uncompressed IPC batch to the original artifact store."""
    import pyarrow as pa
    sink = pa.BufferOutputStream()
    with pa.ipc.new_stream(sink, batch.schema) as writer:
        writer.write_batch(batch)
    buffer = sink.getvalue()
    size = 12 + len(raw_header) + buffer.size
    if size > estimate:
        raise DataflowError('BUDGET_EXCEEDED', 'Arrow IPC exceeded its pre-admitted encoding window')
    def chunks():
        yield _ARROW_MAGIC + _ARROW_HEADER.pack(len(raw_header)) + raw_header
        for offset in range(0, buffer.size, 65536):
            yield buffer.slice(offset, min(65536, buffer.size - offset)).to_pybytes()
    return store.put(chunks(), max_bytes=estimate, media_type=media_type)


def store_mapped_arrow(batch, bindings, schema_ref, *, store, bounds, assignment=None, retained_bytes):
    """Authenticate output occurrences sharing an IPC artifact; None selects typed rows."""
    import json
    import pyarrow as pa
    from demiflow.execution.contracts.dataflow import ArtifactRef, Record
    from demiflow.execution.contracts.dataflow_codec import _thaw_json as thaw_control_json
    from demiflow.execution.contracts.record_codec import _encode_record_document as encode_record_document
    window = _window(bounds, assignment)
    sizes = []
    for index in range(batch.num_rows):
        size = row_upper(batch, index)
        if size is None:
            return None
        exact = mapped_row_bytes(batch, index, bounds.max_row_bytes)
        if exact > bounds.max_row_bytes:
            return None
        sizes.append(exact)
    members = [mapped_member(key, source.source_position) for key, source in bindings]
    header = json.dumps(dict(v=1, members=members, sizes=sizes, schema=schema_ref.digest,
                             count=batch.num_rows), sort_keys=True, separators=(',', ':')).encode()
    if len(header) > min(8192, bounds.max_control_bytes):
        return None
    estimate = pa.ipc.get_record_batch_size(batch) + schema_ref.size_bytes + len(header) + 256
    if estimate > window or retained_bytes + 8 * estimate + window > _resident_budget(bounds, assignment):
        return None
    probe = ArtifactRef('0' * 64, estimate, schema_ref.store_id, '0' * 64, MAPPED_ARROW_MEDIA)
    def records(ref):
        return [Record(key, thaw_control_json(source.source_position), ref, None, size,
                       source.sequence, schema_ref) for (key, source), size in zip(bindings, sizes)]
    try:
        for record in records(probe):
            encode_record_document(record, max_bytes=bounds.max_row_bytes)
    except DataflowError as exc:
        if exc.code == 'BUDGET_EXCEEDED':
            return None
        raise
    ref = put_arrow_batch(batch, header, estimate, MAPPED_ARROW_MEDIA, store)
    return records(ref)
