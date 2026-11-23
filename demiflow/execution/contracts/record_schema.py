"""Record schema implementation."""
from __future__ import annotations

import base64
KEY = 'declared_arrow_schema'

from demiflow.execution.contracts.dataflow_errors import DataflowError


SCHEMA_MEDIA_TYPE = 'application/vnd.demiflow.arrow-schema.v1'

def encode_schema(schema, *, bounds):
    """Bound a declared schema before serializing metadata; never sample rows."""
    maximum = min(65536, bounds.max_control_bytes, bounds.max_batch_bytes)
    # Reject large names/metadata/frontiers before serializing them.
    pending, size, nodes = [(field, 0) for field in schema], 0, 0
    metadata = [schema.metadata or {}]
    while pending:
        field, depth = pending.pop()
        nodes += 1
        size += len(field.name.encode())
        metadata.append(field.metadata or {})
        if nodes > bounds.max_nodes or depth > bounds.max_depth:
            raise DataflowError('BUDGET_EXCEEDED', 'Arrow schema exceeds structural bounds')
        for index in range(field.type.num_fields):
            pending.append((field.type.field(index), depth + 1))
    for values in metadata:
        size += sum(len(key) + len(value) for key, value in values.items())
    if size > maximum:
        raise DataflowError('BUDGET_EXCEEDED', 'Arrow schema metadata exceeds byte budget')
    raw = schema.serialize().to_pybytes()
    if len(raw) > maximum:
        raise DataflowError('BUDGET_EXCEEDED', 'Arrow schema exceeds byte budget')
    return raw


def encode(schema, *, bounds):
    return base64.b64encode(encode_schema(schema, bounds=bounds)).decode('ascii')

def decode(value, *, bounds):
    maximum = min(65536, bounds.max_control_bytes, bounds.max_batch_bytes)
    if type(value) is not str or len(value) > 4 * ((maximum + 2) // 3):
        raise DataflowError('CORRUPT_ARTIFACT', 'Sealed Arrow schema exceeds metadata bounds')
    import pyarrow as pa
    try:
        raw = base64.b64decode(value, validate=True)
        if len(raw) > maximum:
            raise ValueError('schema byte bound')
        schema = pa.ipc.read_schema(pa.BufferReader(raw))
        encode_schema(schema, bounds=bounds)
        return schema
    except (ValueError, pa.ArrowException) as exc:
        raise DataflowError('CORRUPT_ARTIFACT', 'Invalid sealed Arrow schema') from exc
