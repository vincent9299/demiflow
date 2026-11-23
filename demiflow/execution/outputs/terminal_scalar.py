"""Bounded worker-side terminal reduction over the already executed output.

No source or transform is evaluated again. The durable spool is also the
recovery prefix, so metadata and aggregation see exactly the committed rows.
"""
from demiflow.execution.contracts.dataflow import stage_resident_bytes
import base64
import json
from collections.abc import Mapping

from demiflow.execution.contracts.dataflow_errors import DataflowError
from demiflow.execution.contracts.record_codec import decode_frame, decode_document, line_limit
from demiflow.execution.kernels.dataflow import _materialize_row
from demiflow.execution.contracts import state_codec


def terminal_scalar(stream, *, spec, services, schema_ref=None):
    kind = spec.kind
    if kind not in ('schema', 'columns', 'size_bytes', 'collect_summary'):
        return None
    rows, size, schema = 0, 0, None
    from demiflow.execution.record_schema import RecordSchemas
    schemas = RecordSchemas(services)
    schema = schemas.read(schema_ref)
    stream.flush()
    stream.seek(0)
    try:
        while True:
            services.check_stop()
            line = stream.readline(line_limit(services.bounds))
            if not line:
                break
            held = getattr(stream, 'buffered_bytes', 0)
            raw, item = decode_frame(line, services.bounds, resident_bytes=stage_resident_bytes(getattr(services, 'assignment', None)), retained=held)
            del line
            record = decode_document(raw, record_id=item['record_id'],
                sequence=item['sequence'], bounds=services.bounds, resident_bytes=stage_resident_bytes(getattr(services, 'assignment', None)), retained=held)
            del raw
            rows += 1
            size += record.encoded_bytes
            if kind == 'size_bytes':
                continue
            row = _materialize_row(record, services)
            if not isinstance(row, Mapping):
                raise DataflowError('INVALID_CONTRACT',
                    kind + ' requires mapping rows; this result contains non-mapping business values')
            import pyarrow as pa
            next_schema = schemas.read(record.schema_ref)
            if next_schema is None:
                next_schema = pa.Table.from_pylist([row]).schema
            schema = next_schema if schema is None else pa.unify_schemas([schema, next_schema], promote_options='permissive')
        if kind == 'size_bytes':
            return size
        if schema is None:
            import pyarrow as pa
            schema = pa.schema([])
        return {'rows': rows, 'size_bytes': size, 'names': schema.names,
                'arrow_schema': base64.b64encode(schema.serialize().to_pybytes()).decode()}
    finally:
        stream.seek(0, 2)


def aggregate_scalar(values, *, max_bytes):
    """Deliver the original typed manifest scalar after the kernel finishes."""
    return {'typed_result': base64.b64encode(state_codec.dumps(values, max_bytes=max_bytes)).decode()}
