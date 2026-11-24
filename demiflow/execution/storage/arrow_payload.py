"""Arrow payload implementation."""
from __future__ import annotations

import threading
import hashlib
import json
from demiflow.execution.contracts.dataflow_errors import DataflowError
from demiflow.execution.contracts.record_codec import ARROW_MEDIA_TYPES, MAPPED_ARROW_MEDIA, _MAGIC, _HEADER, _MAX_ROWS, _arrow_error, row_upper, mapped_row_bytes, mapped_member, _window, _resident_budget, _uncompressed_ipc
from demiflow.execution.storage.streams import put_arrow_batch


class ArrowBatchCache:
    """One authenticated batch per consumer, charged within its original grant."""
    def __init__(self, store, bounds, *, assignment=None):
        self.store, self.bounds, self.assignment = store, bounds, assignment
        self.ref = self.batch = self.header = None
        self._projection = None
        self._lock = threading.RLock()

    def close(self):
        with self._lock:
            self.ref = self.batch = self.header = None

    def load(self, ref):
        import pyarrow as pa
        with self._lock:
            if ref == self.ref:
                if ref.size_bytes > _window(self.bounds, self.assignment):
                    raise DataflowError('BUDGET_EXCEEDED', 'Arrow cached batch exceeds the current grant')
                return self.batch, self.header
            self.close()  # Do not retain the old batch during the next read.
            if ref.media_type not in ARROW_MEDIA_TYPES or ref.size_bytes > _window(self.bounds, self.assignment):
                raise DataflowError('BUDGET_EXCEEDED', 'Arrow batch ref exceeds the original retained-buffer window')
            # Consume the complete iterator: store validates SHA before any
            # Arrow value can escape. Bounded join transient is in _window.
            raw = b''.join(self.store.read(ref, max_bytes=_window(self.bounds, self.assignment)))
            if raw[:8] != _MAGIC or len(raw) < 12:
                _arrow_error('invalid Arrow batch framing')
            size = _HEADER.unpack(raw[8:12])[0]
            if size > min(8192, self.bounds.max_control_bytes) or 12 + size >= len(raw):
                _arrow_error('invalid Arrow batch metadata size')
            raw_header = raw[12:12 + size]
            # Closed scalar fields plus one integer array; reject structural
            # amplification before allocating JSON containers.
            mapped = ref.media_type == MAPPED_ARROW_MEDIA
            if raw_header.count(b'{') != 1 or raw_header.count(b'[') != (2 if mapped else 1):
                _arrow_error('invalid Arrow batch metadata structure')
            try:
                header = json.loads(raw_header)
                expected = {'v', 'schema', 'count', 'sizes'}
                expected.update({'members'} if mapped else {'source_id', 'split_id', 'version', 'fragment_id', 'start'})
                if type(header) is not dict or set(header) != expected or type(header['v']) is not int or header['v'] != 1:
                    _arrow_error('unknown Arrow batch metadata')
                if any(type(header[k]) is not int or header[k] < 0 for k in (('count',) if mapped else ('version','fragment_id','start','count'))):
                    _arrow_error('invalid Arrow batch source position')
                if not 0 < header['count'] <= _MAX_ROWS or type(header['sizes']) is not list or len(header['sizes']) != header['count']:
                    _arrow_error('invalid Arrow batch row bounds')
                if any(type(n) is not int or not 0 < n <= self.bounds.max_row_bytes for n in header['sizes']):
                    _arrow_error('invalid Arrow row byte estimate')
                if mapped:
                    members = header['members']
                    if type(members) is not list or len(members) != header['count']:
                        _arrow_error('invalid Arrow result membership')
                    digests = [header['schema'], *members]
                else:
                    digests = [header[key] for key in ('source_id', 'split_id', 'schema')]
                for digest in digests:
                    if type(digest) is not str or len(digest) != 64 or any(c not in '0123456789abcdef' for c in digest):
                        _arrow_error('invalid Arrow batch binding')
                if mapped and len(set(members)) != len(members):
                    _arrow_error('duplicate Arrow result membership')
                ipc_buffer = pa.py_buffer(raw).slice(12 + size)
                _uncompressed_ipc(ipc_buffer, self.bounds)
                ipc = pa.ipc.open_stream(ipc_buffer)
                batch = ipc.read_next_batch()
                try:
                    ipc.read_next_batch()
                except StopIteration:
                    pass
                else:
                    _arrow_error('Arrow artifact must contain exactly one batch')
                if batch.num_rows != header['count'] or batch.get_total_buffer_size() > _window(self.bounds, self.assignment):
                    _arrow_error('Arrow batch rows/buffers differ from admitted metadata')
                if hashlib.sha256(batch.schema.serialize().to_pybytes()).hexdigest() != header['schema']:
                    _arrow_error('Arrow batch schema digest differs')
                for i, size in enumerate(header['sizes']):
                    upper = row_upper(batch, i)
                    if upper is None or not mapped and upper > self.bounds.max_row_bytes:
                        _arrow_error('Arrow row is outside its admitted type or byte bound')
                    expected_size = mapped_row_bytes(batch, i, self.bounds.max_row_bytes) if mapped else upper
                    if size != expected_size:
                        _arrow_error('Arrow row byte bounds differ from authenticated batch')
            except (ValueError, KeyError, TypeError, pa.ArrowException, StopIteration) as exc:
                raise DataflowError('CORRUPT_ARTIFACT', 'invalid Arrow batch encoding') from exc
            self.ref, self.batch, self.header = ref, batch, header
            return batch, header

    def locate(self, record):
        batch, header = self.load(record.payload_ref)
        pos = record.source_position
        if record.payload_ref.media_type == MAPPED_ARROW_MEDIA:
            token = mapped_member(record.record_id, pos)
            try:
                index = header['members'].index(token)
            except ValueError:
                _arrow_error('Arrow result differs from its authenticated output occurrence')
        else:
            if (pos.get('version') != header['version'] or pos.get('fragment_id') != header['fragment_id']
                    or type(pos.get('row_offset')) is not int):
                _arrow_error('Arrow record source position differs from batch binding')
            index = pos['row_offset'] - header['start']
            from demiflow.execution.contracts.recovery_contract import identity
            from demiflow.execution.contracts.dataflow_codec import _thaw_json as thaw_control_json
            if identity([header['source_id'], header['split_id'], thaw_control_json(pos)]) != record.record_id:
                _arrow_error('Arrow record identity differs from its original source occurrence')
        if not 0 <= index < header['count'] or record.encoded_bytes != header['sizes'][index]:
            _arrow_error('Arrow record offset/bytes differ from batch binding')
        if record.schema_ref is None or record.schema_ref.digest != header['schema']:
            _arrow_error('Arrow record schema differs from batch binding')
        return batch, index

    def project(self, record, indices, names, schemas):
        """Persist a column projection, retaining the original authenticated members."""
        from dataclasses import replace
        with self._lock:
            batch, index = self.locate(record)
            key = (record.payload_ref, tuple(indices), tuple(names))
            if self._projection is not None and self._projection[0] == key:
                _, ref, sizes, schema_ref = self._projection
            else:
                import pyarrow as pa
                schema = pa.schema([batch.schema.field(i).with_name(name) for i, name in zip(indices, names)],
                                   metadata=batch.schema.metadata)
                projected = pa.RecordBatch.from_arrays([batch.column(i) for i in indices], schema=schema)
                if not len(projected.schema):
                    return None  # Zero-column IPC cannot preserve a nonzero row count.
                schema_ref = schemas.put(projected.schema)
                mapped = record.payload_ref.media_type == MAPPED_ARROW_MEDIA
                sizes = [mapped_row_bytes(projected, i, self.bounds.max_row_bytes) if mapped else row_upper(projected, i)
                         for i in range(projected.num_rows)]
                if any(size is None or size > self.bounds.max_row_bytes for size in sizes):
                    return None
                header = json.dumps({**self.header, 'schema': schema_ref.digest, 'sizes': sizes},
                                    sort_keys=True, separators=(',', ':')).encode()
                estimate = pa.ipc.get_record_batch_size(projected) + schema_ref.size_bytes + len(header) + 256
                window = _window(self.bounds, self.assignment)
                if (estimate > window or batch.get_total_buffer_size() + 8 * estimate + window >
                        _resident_budget(self.bounds, self.assignment)):
                    return None
                ref = put_arrow_batch(projected, header, estimate, record.payload_ref.media_type, self.store)
                self._projection = (key, ref, sizes, schema_ref)
            return replace(record, payload_ref=ref, schema_ref=schema_ref, encoded_bytes=sizes[index])

    def row(self, record):
        with self._lock:
            batch, index = self.locate(record)
            return batch.slice(index, 1).to_pylist()[0]

_UNBOUND_ASSIGNMENT = object()

def batch_cache(store, bounds, *, assignment=_UNBOUND_ASSIGNMENT):
    cache = getattr(store, '_arrow_payload_cache', None)
    if cache is None:
        cache = ArrowBatchCache(store, bounds, assignment=None if assignment is _UNBOUND_ASSIGNMENT else assignment)
        store._arrow_payload_cache = cache
    if assignment is not _UNBOUND_ASSIGNMENT:
        cache.assignment = assignment
    return cache

def materialize_arrow_row(record, *, store, bounds, assignment=None):
    return batch_cache(store, bounds, assignment=assignment).row(record)
