"""Bounded Arrow interchange used inside isolated kernel/native jobs.

Each chunk is an independent uncompressed IPC stream with exactly one batch.
No pickle, Python callback, remote URI or Arrow extension type is accepted.
Decoding/Lance materialization requires the native job's outer resource guard;
byte/row accounting is not an RSS guarantee for third-party native libraries.
"""
from __future__ import annotations

from dataclasses import dataclass
import hashlib
import struct

from demiflow.workbench.analysis import contracts as c

CHUNK_BYTES = 1024*1024
MAX_LOGICAL_BYTES = 32*1024*1024
MAX_ENCODED_BYTES = 64*1024*1024
MAX_ROWS = 100000
MAX_CHUNKS = 4096


def schema_description(schema):
    import pyarrow as pa
    if not isinstance(schema, pa.Schema) or len(schema) > 256:
        c.reject("Arrow schema exceeds field limit", code="resource_limit")
    metadata_bytes = 0
    names = set()
    result = []
    kinds = {pa.bool_(): "bool", pa.int64(): "int64", pa.float64(): "float64",
             pa.string(): "utf8", pa.binary(): "binary"}
    for field in schema:
        c.text(field.name, "Arrow field name", 256)
        if field.name in names:
            c.reject("Duplicate Arrow field name")
        names.add(field.name)
        kind = kinds.get(field.type)
        if kind is None:
            c.reject("Arrow bridge supports bool/int64/float64/utf8/binary only", code="operation_rejected")
        result.append(dict(name=field.name, type=kind, nullable=field.nullable))
    for metadata in [schema.metadata, *(field.metadata for field in schema)]:
        if metadata is None:
            continue
        if len(metadata) > 128:
            c.reject("Arrow metadata exceeds entry limit", code="resource_limit")
        for key, value in metadata.items():
            if len(key) > 256 or len(value) > 16384:
                c.reject("Arrow metadata exceeds entry byte limit", code="resource_limit")
            # Extension deserialization must not select code in a receiver.
            if key.startswith(b"ARROW:extension:"):
                c.reject("Arrow extension metadata is unsupported", code="operation_rejected")
            metadata_bytes += len(key) + len(value)
            if metadata_bytes > 65536:
                c.reject("Arrow metadata exceeds total byte limit", code="resource_limit")
    return result


def _field(data, table, number):
    """Bounded FlatBuffer field lookup, not an Arrow decoder.

    Message.header union is field 2; RecordBatch.compression is field 3.
    See Apache Arrow format/Message.fbs. Validate every offset before reading.
    """
    def check(start, size):
        if start < 0 or start + size > len(data):
            c.reject("Malformed Arrow message metadata")

    check(table, 4)
    vtable = table - struct.unpack_from("<i", data, table)[0]
    check(vtable, 4)
    vlength, object_size = struct.unpack_from("<HH", data, vtable)
    check(vtable, vlength)
    check(table, object_size)
    if vlength < 4 or vlength % 2 or object_size < 4:
        c.reject("Malformed Arrow message vtable")
    slot = 4 + number*2
    if slot + 2 > vlength:
        return None
    offset = struct.unpack_from("<H", data, vtable + slot)[0]
    if offset == 0:
        return None
    if offset < 4 or offset >= object_size:
        c.reject("Malformed Arrow message field")
    return table + offset


def _uoffset(data, position):
    if position is None or position < 0 or position + 4 > len(data):
        c.reject("Missing Arrow metadata reference")
    offset = struct.unpack_from("<I", data, position)[0]
    result = position + offset
    if offset < 4 or result + 4 > len(data):
        c.reject("Invalid Arrow metadata reference")
    return result


def _batch_guard(message):
    """Reject compression/large declared rows before native array decoding."""
    data = memoryview(message.metadata)
    if len(data) < 4 or len(data) > 128*1024:
        c.reject("Arrow metadata exceeds chunk limit", code="resource_limit")
    root = _uoffset(data, 0)
    union_type = _field(data, root, 1)
    if union_type is None or data[union_type] != 3:  # MessageHeader.RecordBatch
        c.reject("Only record batch IPC data is supported")
    batch = _uoffset(data, _field(data, root, 2))
    if _field(data, batch, 3) is not None:
        c.reject("Compressed IPC input is unsupported", code="operation_rejected")
    length = _field(data, batch, 0)
    if length is None:
        rows = 0
    elif length + 8 > len(data):
        c.reject("Malformed Arrow row count")
    else:
        rows = struct.unpack_from("<q", data, length)[0]
    c.integer(rows, "Arrow chunk rows", MAX_ROWS)
    return rows


def decode_chunk(raw):
    import pyarrow as pa
    if type(raw) is not bytes or not 1 <= len(raw) <= CHUNK_BYTES:
        c.reject("Arrow chunk exceeds encoded limit", code="resource_limit")
    source = pa.BufferReader(raw)
    try:
        first = pa.ipc.read_message(source)
        if first.type != "schema" or first.metadata.size > 128*1024 or first.body is not None and first.body.size:
            c.reject("Arrow chunk must start with a bounded schema")
        # Reject extension metadata before read_schema can invoke an installed
        # extension deserializer. The admitted primitive schema has none.
        if b"ARROW:extension:" in first.metadata.to_pybytes():
            c.reject("Arrow extension metadata is unsupported", code="operation_rejected")
        schema = pa.ipc.read_schema(first)
        schema_description(schema)
        message = pa.ipc.read_message(source)
        if message.type != "record batch":
            c.reject("Arrow chunk must contain exactly one record batch")
        declared_rows = _batch_guard(message)
        # pyarrow 25's read_record_batch has no options parameter. Compression
        # is already rejected above; the admitted primitive buffers are read
        # directly without a decompression worker pool.
        batch = pa.ipc.read_record_batch(message, schema)
        batch.validate(full=True)
        if batch.num_rows != declared_rows or batch.nbytes > CHUNK_BYTES or batch.get_total_buffer_size() > 4*CHUNK_BYTES:
            c.reject("Arrow decoded chunk exceeds limit", code="resource_limit")
        try:
            pa.ipc.read_message(source)
        except EOFError:
            pass
        else:
            c.reject("Arrow chunk contains multiple batches")
        if source.tell() != len(raw):
            c.reject("Arrow chunk has trailing bytes")
        return batch
    except (pa.ArrowException, OSError, EOFError) as exc:
        c.reject("Invalid Arrow IPC chunk: " + type(exc).__name__)


@dataclass
class ArrowBudget:
    rows: int = 0
    logical_bytes: int = 0
    encoded_bytes: int = 0
    chunks: int = 0

    def add(self, batch, encoded_bytes):
        if (self.rows + batch.num_rows > MAX_ROWS
                or self.logical_bytes + batch.nbytes > MAX_LOGICAL_BYTES
                or self.encoded_bytes + encoded_bytes > MAX_ENCODED_BYTES
                or self.chunks >= MAX_CHUNKS):
            c.reject("Arrow interchange budget exceeded; no partial result published", code="resource_limit")
        self.rows += batch.num_rows
        self.logical_bytes += batch.nbytes
        self.encoded_bytes += encoded_bytes
        self.chunks += 1


def encode_batches(reader):
    """Stream one frame at a time; split batch views before allocating IPC."""
    import pyarrow as pa
    schema = reader.schema
    schema_description(schema)
    schema_bytes = schema.serialize().size
    if schema_bytes > 128*1024:
        c.reject("Arrow schema metadata exceeds chunk budget", code="resource_limit")
    budget = ArrowBudget()
    found = False
    for incoming in reader:
        found = True
        if not isinstance(incoming, pa.RecordBatch) or not incoming.schema.equals(schema, check_metadata=True):
            c.reject("Arrow stream schema changed")
        if incoming.nbytes > MAX_LOGICAL_BYTES or incoming.get_total_buffer_size() > 4*MAX_LOGICAL_BYTES:
            c.reject("Arrow batch/retained buffers exceed input budget", code="resource_limit")
        pending = [incoming]
        while pending:
            batch = pending.pop()
            estimate = batch.nbytes + schema_bytes + (len(schema) + 1)*128 + 4096
            if estimate > CHUNK_BYTES:
                if batch.num_rows <= 1:
                    c.reject("One Arrow row exceeds chunk budget", code="resource_limit")
                middle = batch.num_rows//2
                pending.extend((batch.slice(middle), batch.slice(0, middle)))
                continue
            # Check global counters before encoding this next frame as well.
            if budget.rows + batch.num_rows > MAX_ROWS or budget.logical_bytes + batch.nbytes > MAX_LOGICAL_BYTES or budget.encoded_bytes + estimate > MAX_ENCODED_BYTES or budget.chunks >= MAX_CHUNKS:
                c.reject("Arrow interchange budget exhausted", code="resource_limit")
            sink = pa.BufferOutputStream()
            with pa.ipc.new_stream(sink, schema, options=pa.ipc.IpcWriteOptions(compression=None, use_threads=False)) as writer:
                writer.write_batch(batch)
            buffer = sink.getvalue()
            if buffer.size > estimate or buffer.size > CHUNK_BYTES:
                c.reject("Arrow encoded overhead exceeded its reservation", code="resource_limit")
            budget.add(batch, buffer.size)
            yield buffer.to_pybytes()
    if not found:
        empty = pa.RecordBatch.from_arrays([pa.array([], type=field.type) for field in schema], schema=schema)
        yield from encode_batches(pa.RecordBatchReader.from_batches(schema, [empty]))


def decoded_batches(chunks):
    schema = None
    budget = ArrowBudget()
    for raw in chunks:
        if type(raw) is not bytes or budget.encoded_bytes + len(raw) > MAX_ENCODED_BYTES or budget.chunks >= MAX_CHUNKS:
            c.reject("Arrow ingress budget exhausted", code="resource_limit")
        batch = decode_chunk(raw)
        if schema is None:
            schema = batch.schema
        elif not schema.equals(batch.schema, check_metadata=True):
            c.reject("Arrow chunk schemas differ")
        budget.add(batch, len(raw))
        yield batch


def schema_digest(schema):
    schema_description(schema)
    return hashlib.sha256(schema.serialize().to_pybytes()).hexdigest()
