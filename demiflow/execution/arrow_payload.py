"""Bounded shared Arrow batches behind the original, still per-row Record.

Only this versioned MIME changes ref payload interpretation. Old row bytes,
identity, source positions and commit/cursor authorities remain unchanged.
"""
from dataclasses import dataclass
import hashlib
import json
import struct
import threading

from .contracts.dataflow_errors import DataflowError

ARROW_BATCH_MEDIA = 'application/vnd.demiflow.arrow-batch.v1'
_MAGIC = b'DFARROW1'
_HEADER = struct.Struct('!I')
_MAX_ROWS = 256


def allows_batch_carrier(graph, physical):
    """Optimize only a sealed, sequential source-to-terminal slice.

    Reordering, forks and mixed sources keep the original row codec. Otherwise
    one-cache consumers could reread an entire batch once per interleaved row.
    This selects a payload representation, never another execution route.
    """
    if len(physical.splits) != 1:
        return False
    if sum(node.kind == 'source' for node in graph.nodes) != 1:
        return False
    if sum(node.kind == 'terminal' for node in graph.nodes) != 1:
        return False
    terminal = next(node for node in graph.nodes if node.kind == 'terminal')
    if terminal.kernel.kind not in ('write_lance', 'materialize', 'iter_rows', 'count'):
        # size_bytes/summary and bounded collectors retain their original exact
        # row accounting. The carrier estimate is a resource bound, not a new
        # public measurement of a row's serialized size.
        return False
    if any(node.kind not in ('source', 'terminal') and node.kernel.kind != 'LimitOp'
           for node in graph.nodes):
        return False
    if len(graph.edges) != len(graph.nodes) - 1:
        return False
    producers = [edge.producer for edge in graph.edges]
    consumers = [edge.consumer for edge in graph.edges]
    return (len(set(producers)) == len(producers)
            and len(set(consumers)) == len(consumers)
            and all(len(edge.producer_partitions) == len(edge.consumer_partitions) == 1
                    for edge in physical.exchanges))


def is_arrow_record(record):
    return record.payload_ref is not None and record.payload_ref.media_type == ARROW_BATCH_MEDIA


def _fail(message):
    raise DataflowError('CORRUPT_ARTIFACT', message)


def _value_upper(array, *, depth=0):
    """Typed-row JSON upper bound from Arrow types/buffers, never Python rows.

    Unknown types and oversized structures use the original exact row codec.
    Scalar conversions below are only list offsets / aggregate validity bits.
    """
    import pyarrow as pa
    import pyarrow.compute as pc
    n, kind = len(array), array.type
    if depth > 24 or n > 65536:
        return None
    if pa.types.is_null(kind):
        return 16 * n
    if pa.types.is_boolean(kind) or pa.types.is_integer(kind):
        return 48 * n
    if pa.types.is_floating(kind):
        if n and pc.any(pc.invert(pc.is_finite(array))).as_py():
            return None  # Preserve the original nonfinite validation.
        return 64 * n
    if pa.types.is_string(kind) or pa.types.is_large_string(kind):
        return 32 * n + 6 * array.nbytes
    if pa.types.is_binary(kind) or pa.types.is_large_binary(kind) or pa.types.is_fixed_size_binary(kind):
        return 32 * n + 4 * ((array.nbytes + 2) // 3)
    if pa.types.is_struct(kind):
        total = 32 * n
        for index, field in enumerate(kind):
            value = _value_upper(array.field(index), depth=depth + 1)
            if value is None:
                return None
            total += n * (len(json.dumps(field.name, ensure_ascii=False).encode()) + 8) + value
        return total
    if pa.types.is_list(kind) or pa.types.is_large_list(kind):
        first = array.offsets[0].as_py()
        last = array.offsets[-1].as_py()
        value = _value_upper(array.values.slice(first, last - first), depth=depth + 1)
        return None if value is None else 32 * n + value
    if pa.types.is_fixed_size_list(kind):
        child = array.values.slice(array.offset * kind.list_size, n * kind.list_size)
        value = _value_upper(child, depth=depth + 1)
        return None if value is None else 32 * n + value
    return None


def row_upper(batch, index):
    total = 32
    for field, column in zip(batch.schema, batch.columns):
        value = _value_upper(column.slice(index, 1))
        if value is None:
            return None
        total += len(json.dumps(field.name, ensure_ascii=False).encode()) + 8 + value
    return total


def _resident_budget(bounds, services=None):
    resident = bounds.max_resident_bytes
    if services is not None:
        stages = getattr(getattr(services, 'assignment', None), 'stages', ())
        for stage in stages:
            resident = min(resident, stage.profile.requests['memory_bytes'])
    return resident


def _window(bounds, services=None):
    resident = _resident_budget(bounds, services)
    # Source input + IPC encoding + authenticated read + prior yielded batch,
    # and downstream row conversion may coexist. Keep explicit finite headroom.
    return max(0, min(bounds.max_batch_bytes // 8, resident // 32))


@dataclass(frozen=True)
class ArrowPayload:
    ref: object
    encoded_bytes: int


def publish_batch(batch, *, reader, fragment_id, start):
    """Return one shared ref or None for the original row fallback.

    All preallocation checks precede IPC serialization. Arrow native scanner
    allocation is additionally constrained by its existing row/batch admission.
    """
    import pyarrow as pa
    from .record_schema import encode_schema
    bounds, services = reader._services.bounds, reader._services
    if not 0 < batch.num_rows <= _MAX_ROWS or batch.get_total_buffer_size() > _window(bounds, services):
        return None
    sizes = [row_upper(batch, i) for i in range(batch.num_rows)]
    # A conservative estimate must not reject an otherwise legal row.
    if any(n is None or n > bounds.max_row_bytes for n in sizes):
        return None
    schema = encode_schema(batch.schema, bounds=bounds)
    header = dict(v=1, source_id=reader._snapshot.source_id, split_id=reader._split.split_id,
        version=reader._version, fragment_id=fragment_id, start=start, count=batch.num_rows,
        schema=hashlib.sha256(schema).hexdigest(), sizes=sizes)
    raw_header = json.dumps(header, sort_keys=True, separators=(',', ':')).encode()
    if len(raw_header) > min(8192, bounds.max_control_bytes):
        return None
    # Arrow's size helper includes the record message metadata and padding;
    # serialized schema includes its encapsulation. Add frame/EOS/alignment
    # headroom, not an unrelated 64 KiB floor that disables ordinary batches.
    estimate = pa.ipc.get_record_batch_size(batch) + len(schema) + len(raw_header) + 256
    prefetched = min(bounds.max_batch_bytes,
                     reader._arrow_scan_rows * bounds.max_row_bytes)
    peak = prefetched + batch.get_total_buffer_size() + _window(bounds, services) + 8*estimate
    if estimate > _window(bounds, services) or peak > _resident_budget(bounds, services):
        return None
    # Ref/provenance metadata can be wider than a tiny legal inline row. Check
    # both the original transfer and terminal/pending frame before publishing
    # any batch artifact. The dummy digest/key have the original store's fixed
    # 64-hex shape; maximum offset/sequence/bytes conservatively bound this batch.
    from .contracts.dataflow import ArtifactRef, Record
    from .exchange.dataflow_bridge import _encode_record_document
    from .recovery_contract import document, RecoveryContractError
    position = dict(version=reader._version, fragment_id=fragment_id,
                    row_offset=start + batch.num_rows - 1)
    probe = Record('0'*64, position,
        ArtifactRef('0'*64, estimate, reader._schema_ref.store_id, '0'*64, ARROW_BATCH_MEDIA),
        None, max(sizes), reader._next_sequence + batch.num_rows - 1, reader._schema_ref)
    try:
        transfer = _encode_record_document(probe, max_bytes=bounds.max_row_bytes)
        document(dict(record_id=probe.record_id, sequence=probe.sequence,
                      document=transfer.decode()), min(bounds.max_row_bytes, bounds.max_control_bytes))
    except DataflowError as exc:
        if exc.code != 'BUDGET_EXCEEDED':
            raise
        return None
    except RecoveryContractError:
        return None
    sink = pa.BufferOutputStream()
    with pa.ipc.new_stream(sink, batch.schema) as writer:
        writer.write_batch(batch)
    buffer = sink.getvalue()
    size = 12 + len(raw_header) + buffer.size
    if size > estimate:
        raise DataflowError('BUDGET_EXCEEDED', 'Arrow IPC exceeded its pre-admitted encoding window')
    def chunks():
        yield _MAGIC + _HEADER.pack(len(raw_header)) + raw_header
        for offset in range(0, buffer.size, 65536):
            yield buffer.slice(offset, min(65536, buffer.size - offset)).to_pybytes()
    ref = services.store.put(chunks(), max_bytes=estimate, media_type=ARROW_BATCH_MEDIA)
    return ref, sizes


def _uncompressed_ipc(buffer, bounds):
    """Inspect bounded IPC messages before Arrow can allocate decoded arrays.

    Our writer emits schema + one uncompressed record batch, no dictionaries.
    The FlatBuffer check reads only Message.header and RecordBatch.compression
    slots; a compressed/custom carrier is not part of this MIME contract.
    """
    import pyarrow as pa
    messages = pa.ipc.MessageReader.open_stream(buffer)
    schema, record = messages.read_next_message(), messages.read_next_message()
    if schema.type != 'schema' or record.type != 'record batch':
        _fail('Arrow carrier requires schema and one record batch')
    try:
        messages.read_next_message()
    except StopIteration:
        pass
    else:
        _fail('Arrow carrier contains extra IPC messages')
    for message in (schema, record):
        if message.metadata.size > min(65536, bounds.max_control_bytes):
            _fail('Arrow IPC metadata exceeds decode bound')
    metadata = memoryview(record.metadata)
    def field(table, index):
        if not 0 <= table <= len(metadata)-4:
            _fail('invalid Arrow IPC table')
        vtable = table - struct.unpack_from('<i', metadata, table)[0]
        if not 0 <= vtable <= len(metadata)-4:
            _fail('invalid Arrow IPC vtable')
        size = struct.unpack_from('<H', metadata, vtable)[0]
        slot = 4 + 2*index
        if slot + 2 > size:
            return None
        offset = struct.unpack_from('<H', metadata, vtable+slot)[0]
        return table+offset if offset else None
    try:
        root = struct.unpack_from('<I', metadata, 0)[0]
        pointer = field(root, 2)  # org.apache.arrow.flatbuf.Message.header
        if pointer is None:
            _fail('Arrow IPC message lacks record header')
        table = pointer + struct.unpack_from('<I', metadata, pointer)[0]
        if field(table, 3) is not None:  # RecordBatch.compression
            _fail('compressed Arrow IPC is not an admitted batch carrier')
    except (struct.error, IndexError) as exc:
        raise DataflowError('CORRUPT_ARTIFACT', 'invalid Arrow IPC record header') from exc


class ArrowBatchCache:
    """One authenticated batch per consumer, charged within its original grant."""
    def __init__(self, store, bounds, *, services=None):
        self.store, self.bounds, self.services = store, bounds, services
        self.ref = self.batch = self.header = None
        self._lock = threading.RLock()

    def close(self):
        with self._lock:
            self.ref = self.batch = self.header = None

    def load(self, ref):
        import pyarrow as pa
        with self._lock:
            if ref == self.ref:
                if ref.size_bytes > _window(self.bounds, self.services):
                    raise DataflowError('BUDGET_EXCEEDED', 'Arrow cached batch exceeds the current grant')
                return self.batch, self.header
            self.close()  # Do not retain the old batch during the next read.
            if ref.media_type != ARROW_BATCH_MEDIA or ref.size_bytes > _window(self.bounds, self.services):
                raise DataflowError('BUDGET_EXCEEDED', 'Arrow batch ref exceeds the original retained-buffer window')
            # Consume the complete iterator: store validates SHA before any
            # Arrow value can escape. Bounded join transient is in _window.
            raw = b''.join(self.store.read(ref, max_bytes=_window(self.bounds, self.services)))
            if raw[:8] != _MAGIC or len(raw) < 12:
                _fail('invalid Arrow batch framing')
            size = _HEADER.unpack(raw[8:12])[0]
            if size > min(8192, self.bounds.max_control_bytes) or 12 + size >= len(raw):
                _fail('invalid Arrow batch metadata size')
            raw_header = raw[12:12 + size]
            # Closed scalar fields plus one integer array; reject structural
            # amplification before allocating JSON containers.
            if raw_header.count(b'{') != 1 or raw_header.count(b'[') != 1:
                _fail('invalid Arrow batch metadata structure')
            try:
                header = json.loads(raw_header)
                expected = {'v','source_id','split_id','version','fragment_id','start','count','schema','sizes'}
                if type(header) is not dict or set(header) != expected or type(header['v']) is not int or header['v'] != 1:
                    _fail('unknown Arrow batch metadata')
                if any(type(header[k]) is not int or header[k] < 0 for k in ('version','fragment_id','start','count')):
                    _fail('invalid Arrow batch source position')
                if not 0 < header['count'] <= _MAX_ROWS or type(header['sizes']) is not list or len(header['sizes']) != header['count']:
                    _fail('invalid Arrow batch row bounds')
                if any(type(n) is not int or not 0 < n <= self.bounds.max_row_bytes for n in header['sizes']):
                    _fail('invalid Arrow row byte estimate')
                for key in ('source_id','split_id','schema'):
                    if type(header[key]) is not str or len(header[key]) != 64 or any(c not in '0123456789abcdef' for c in header[key]):
                        _fail('invalid Arrow batch source binding')
                ipc_buffer = pa.py_buffer(raw).slice(12 + size)
                _uncompressed_ipc(ipc_buffer, self.bounds)
                ipc = pa.ipc.open_stream(ipc_buffer)
                batch = ipc.read_next_batch()
                try:
                    ipc.read_next_batch()
                except StopIteration:
                    pass
                else:
                    _fail('Arrow artifact must contain exactly one batch')
                if batch.num_rows != header['count'] or batch.get_total_buffer_size() > _window(self.bounds, self.services):
                    _fail('Arrow batch rows/buffers differ from admitted metadata')
                if hashlib.sha256(batch.schema.serialize().to_pybytes()).hexdigest() != header['schema']:
                    _fail('Arrow batch schema digest differs')
                if any(row_upper(batch, i) != n for i,n in enumerate(header['sizes'])):
                    _fail('Arrow row byte bounds differ from authenticated batch')
            except (ValueError, KeyError, TypeError, pa.ArrowException, StopIteration) as exc:
                raise DataflowError('CORRUPT_ARTIFACT', 'invalid Arrow batch encoding') from exc
            self.ref, self.batch, self.header = ref, batch, header
            return batch, header

    def locate(self, record):
        batch, header = self.load(record.payload_ref)
        pos = record.source_position
        if (pos.get('version') != header['version'] or pos.get('fragment_id') != header['fragment_id']
                or type(pos.get('row_offset')) is not int):
            _fail('Arrow record source position differs from batch binding')
        index = pos['row_offset'] - header['start']
        if not 0 <= index < header['count'] or record.encoded_bytes != header['sizes'][index]:
            _fail('Arrow record offset/bytes differ from batch binding')
        if record.schema_ref is None or record.schema_ref.digest != header['schema']:
            _fail('Arrow record schema differs from batch binding')
        from .recovery_contract import identity
        from .contracts.dataflow_codec import _thaw_json
        if identity([header['source_id'], header['split_id'], _thaw_json(pos)]) != record.record_id:
            _fail('Arrow record identity differs from its original source occurrence')
        return batch, index

    def row(self, record):
        with self._lock:
            batch, index = self.locate(record)
            return batch.slice(index, 1).to_pylist()[0]


def batch_cache(store, bounds, *, services=None):
    cache = getattr(store, '_arrow_payload_cache', None)
    if cache is None:
        cache = ArrowBatchCache(store, bounds, services=services)
        store._arrow_payload_cache = cache
    if services is not None:
        cache.services = services
    return cache


def materialize_arrow_row(record, services):
    return batch_cache(services.store, services.bounds, services=services).row(record)


def fallback_row(batch, index):
    """The existing typed-row path, also the pure-copy negative-test seam."""
    return batch.slice(index, 1).to_pylist()[0]
