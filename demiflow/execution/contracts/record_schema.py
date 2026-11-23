"""Record schema implementation."""
from __future__ import annotations

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
