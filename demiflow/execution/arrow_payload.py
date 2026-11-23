"""Bounded shared Arrow batches behind the original, still per-row Record.

Only this versioned MIME changes ref payload interpretation. Old row bytes,
identity, source positions and commit/cursor authorities remain unchanged.
"""
from dataclasses import dataclass
import re
import hashlib
import json
import struct
import threading

from .contracts.dataflow_errors import DataflowError

ARROW_BATCH_MEDIA = 'application/vnd.demiflow.arrow-batch.v1'
MAPPED_ARROW_MEDIA = 'application/vnd.demiflow.mapped-arrow-batch.v1'
ARROW_MEDIA_TYPES = frozenset({ARROW_BATCH_MEDIA, MAPPED_ARROW_MEDIA})
_MAGIC = b'DFARROW1'
_HEADER = struct.Struct('!I')
_MAX_ROWS = 256


def batch_carrier(graph, physical):
    """Optimize only a sealed, sequential source-to-terminal slice.

    Reordering, forks and mixed sources keep the original row codec. Otherwise
    one-cache consumers could reread an entire batch once per interleaved row.
    This selects a payload representation, never another execution route.
    """
    if len(physical.splits) != 1:
        return None
    if sum(node.kind == 'source' for node in graph.nodes) != 1:
        return None
    if sum(node.kind == 'terminal' for node in graph.nodes) != 1:
        return None
    terminal = next(node for node in graph.nodes if node.kind == 'terminal')
    if terminal.kernel.kind not in ('write_lance', 'materialize', 'iter_rows', 'count'):
        # size_bytes/summary and bounded collectors retain their original exact
        # row accounting. The carrier estimate is a resource bound, not a new
        # public measurement of a row's serialized size.
        return None
    mapped_arrow = terminal.kernel.configuration.get('input_carrier') == 'mapped_arrow.v1'
    if any(node.kind not in ('source', 'terminal') and node.kernel.kind != 'LimitOp'
           and not (mapped_arrow and node.kernel.kind == 'MapBatchesOp')
           for node in graph.nodes):
        return None
    if len(graph.edges) != len(graph.nodes) - 1:
        return None
    producers = [edge.producer for edge in graph.edges]
    consumers = [edge.consumer for edge in graph.edges]
    if (len(set(producers)) != len(producers) or len(set(consumers)) != len(consumers)
            or any(len(edge.producer_partitions) != 1 or len(edge.consumer_partitions) != 1
                   for edge in physical.exchanges)):
        return None
    if mapped_arrow:
        return 'mapped_arrow.v1'
    # Existing seals did not request contiguous native batches. Keep their
    # original scan boundaries and reference representation on recovery.
    return 'original_scan_batches'


def is_arrow_record(record):
    return record.payload_ref is not None and record.payload_ref.media_type in ARROW_MEDIA_TYPES


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
    ref = put_arrow_batch(batch, raw_header, estimate, ARROW_BATCH_MEDIA, services.store)
    return ref, sizes


_UTF8_STRING_ESCAPE = re.compile(rb'["\\\x00-\x1f]')


def mapped_row_bytes(batch, index, maximum):
    """Exact tagged-row size for Arrow outputs, without constructing a row or copying its binary values."""
    import pyarrow as pa
    from .dataflow_record_frames import _string_size as json_string_size, _typed_shape as typed_shape
    from .exchange.dataflow_bridge import _MAX_DEPTH, _MAX_NODES
    count = 1
    def value_size(value, depth):
        nonlocal count
        count += 1
        if count > _MAX_NODES or depth > _MAX_DEPTH:
            raise DataflowError('BUDGET_EXCEEDED', 'record structure exceeds depth/node budget')
        if not value.is_valid:
            return 12
        dtype = value.type
        if pa.types.is_binary(dtype) or pa.types.is_large_binary(dtype) or pa.types.is_fixed_size_binary(dtype):
            return 20 + 4 * ((value.as_buffer().size + 2) // 3)
        if pa.types.is_string(dtype) or pa.types.is_large_string(dtype):
            # Count JSON escapes in UTF-8; regular expression scanning stays in C
            # for unescaped long strings. json_string_size uses the same escapes.
            buf = memoryview(value.as_buffer()).cast('B')
            size = len(buf) + 2
            for match in _UTF8_STRING_ESCAPE.finditer(buf):
                char = buf[match.start()]
                size += 1 if char in b'\"\\\b\f\n\r\t' else 5
            return 16 + size
        if pa.types.is_struct(dtype):
            return mapping_size(((field.name, value[field.name]) for field in dtype), depth)
        if pa.types.is_list(dtype) or pa.types.is_large_list(dtype) or pa.types.is_fixed_size_list(dtype):
            values = value.values
            return 19 + max(0, len(values) - 1) + sum(value_size(v, depth + 1) for v in values)
        return typed_shape(value.as_py(), maximum)[0]

    def mapping_size(fields, depth=0):
        size = 18
        for ordinal, (name, value) in enumerate(fields):
            size += int(ordinal > 0) + 3 + json_string_size(name) + value_size(value, depth + 1)
        return size

    return mapping_size((field.name, column[index]) for field, column in zip(batch.schema, batch.columns))


def mapped_member(record_id, position):
    from .contracts.dataflow_codec import _thaw_json as thaw_control_json
    from .recovery_contract import identity
    return identity([record_id, thaw_control_json(position)])


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
        yield _MAGIC + _HEADER.pack(len(raw_header)) + raw_header
        for offset in range(0, buffer.size, 65536):
            yield buffer.slice(offset, min(65536, buffer.size - offset)).to_pybytes()
    return store.put(chunks(), max_bytes=estimate, media_type=media_type)


def store_mapped_arrow(batch, bindings, schema_ref, services, *, retained_bytes):
    """Authenticate output occurrences sharing an IPC artifact; None selects typed rows."""
    import json
    import pyarrow as pa
    from .contracts.dataflow import ArtifactRef, Record
    from .contracts.dataflow_codec import _thaw_json as thaw_control_json
    from .exchange.dataflow_bridge import _encode_record_document as encode_record_document
    bounds = services.bounds
    window = _window(bounds, services)
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
    if estimate > window or retained_bytes + 8 * estimate + window > _resident_budget(bounds, services):
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
    ref = put_arrow_batch(batch, header, estimate, MAPPED_ARROW_MEDIA, services.store)
    return records(ref)


def publish_source_rows(batch, *, reader, position_at, retained_bytes):
    """Share fixed-source columns under the original per-record identities and cursors."""
    from types import SimpleNamespace
    from .recovery_contract import identity
    offset = 0
    while offset < batch.num_rows:
        reader._services.check_stop()
        count = min(_MAX_ROWS, batch.num_rows - offset)
        while True:
            part = batch.slice(offset, count)
            bindings = []
            for index in range(count):
                position = position_at(offset + index)
                key = identity([reader._snapshot.source_id, reader._split.split_id, position])
                bindings.append((key, SimpleNamespace(source_position=position, sequence=0)))
            records = store_mapped_arrow(part, bindings, reader._schema_ref, reader._services,
                                        retained_bytes=batch.get_total_buffer_size() + retained_bytes)
            if records is not None:
                for record in records:
                    yield ArrowPayload(record.payload_ref, record.encoded_bytes)
                break
            if count == 1:
                if part.nbytes > reader._services.bounds.max_row_bytes:
                    raise DataflowError('BUDGET_EXCEEDED', 'source row exceeds max_row_bytes')
                yield fallback_row(part, 0)
                break
            count //= 2
        offset += count


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
        self._projection = None
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
            if ref.media_type not in ARROW_MEDIA_TYPES or ref.size_bytes > _window(self.bounds, self.services):
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
            mapped = ref.media_type == MAPPED_ARROW_MEDIA
            if raw_header.count(b'{') != 1 or raw_header.count(b'[') != (2 if mapped else 1):
                _fail('invalid Arrow batch metadata structure')
            try:
                header = json.loads(raw_header)
                expected = {'v', 'schema', 'count', 'sizes'}
                expected.update({'members'} if mapped else {'source_id', 'split_id', 'version', 'fragment_id', 'start'})
                if type(header) is not dict or set(header) != expected or type(header['v']) is not int or header['v'] != 1:
                    _fail('unknown Arrow batch metadata')
                if any(type(header[k]) is not int or header[k] < 0 for k in (('count',) if mapped else ('version','fragment_id','start','count'))):
                    _fail('invalid Arrow batch source position')
                if not 0 < header['count'] <= _MAX_ROWS or type(header['sizes']) is not list or len(header['sizes']) != header['count']:
                    _fail('invalid Arrow batch row bounds')
                if any(type(n) is not int or not 0 < n <= self.bounds.max_row_bytes for n in header['sizes']):
                    _fail('invalid Arrow row byte estimate')
                if mapped:
                    members = header['members']
                    if type(members) is not list or len(members) != header['count']:
                        _fail('invalid Arrow result membership')
                    digests = [header['schema'], *members]
                else:
                    digests = [header[key] for key in ('source_id', 'split_id', 'schema')]
                for digest in digests:
                    if type(digest) is not str or len(digest) != 64 or any(c not in '0123456789abcdef' for c in digest):
                        _fail('invalid Arrow batch binding')
                if mapped and len(set(members)) != len(members):
                    _fail('duplicate Arrow result membership')
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
                for i, size in enumerate(header['sizes']):
                    upper = row_upper(batch, i)
                    if upper is None or not mapped and upper > self.bounds.max_row_bytes:
                        _fail('Arrow row is outside its admitted type or byte bound')
                    expected_size = mapped_row_bytes(batch, i, self.bounds.max_row_bytes) if mapped else upper
                    if size != expected_size:
                        _fail('Arrow row byte bounds differ from authenticated batch')
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
                _fail('Arrow result differs from its authenticated output occurrence')
        else:
            if (pos.get('version') != header['version'] or pos.get('fragment_id') != header['fragment_id']
                    or type(pos.get('row_offset')) is not int):
                _fail('Arrow record source position differs from batch binding')
            index = pos['row_offset'] - header['start']
            from .recovery_contract import identity
            from .contracts.dataflow_codec import _thaw_json as thaw_control_json
            if identity([header['source_id'], header['split_id'], thaw_control_json(pos)]) != record.record_id:
                _fail('Arrow record identity differs from its original source occurrence')
        if not 0 <= index < header['count'] or record.encoded_bytes != header['sizes'][index]:
            _fail('Arrow record offset/bytes differ from batch binding')
        if record.schema_ref is None or record.schema_ref.digest != header['schema']:
            _fail('Arrow record schema differs from batch binding')
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
                window = _window(self.bounds, self.services)
                if (estimate > window or batch.get_total_buffer_size() + 8 * estimate + window >
                        _resident_budget(self.bounds, self.services)):
                    return None
                ref = put_arrow_batch(projected, header, estimate, record.payload_ref.media_type, self.store)
                self._projection = (key, ref, sizes, schema_ref)
            return replace(record, payload_ref=ref, schema_ref=schema_ref, encoded_bytes=sizes[index])

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


def compact_scan_batches(batches, *, services, target_rows):
    """Coalesce adjacent native pieces of one ordered fragment under its existing grant."""
    import pyarrow as pa
    target_rows = min(_MAX_ROWS, target_rows)
    maximum = _window(services.bounds, services)
    pieces = []
    rows = retained = 0
    def flush():
        if len(pieces) == 1:
            return pieces.pop()
        table = pa.Table.from_batches(pieces).combine_chunks()
        pieces.clear()
        return table.to_batches()[0]
    try:
        for batch in batches:
            services.check_stop()
            if not batch.num_rows:
                continue
            size = batch.get_total_buffer_size()
            if pieces and (rows + batch.num_rows > target_rows or retained + size > maximum):
                yield flush()
                rows = retained = 0
            pieces.append(batch)
            rows += batch.num_rows
            retained += size
            if rows >= target_rows or retained >= maximum:
                yield flush()
                rows = retained = 0
        if pieces:
            yield flush()
    finally:
        batches.close()
