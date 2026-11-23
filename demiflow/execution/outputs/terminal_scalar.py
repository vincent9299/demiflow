"""Bounded worker-side terminal reduction over the already executed output.

No source or transform is evaluated again. The durable spool is also the
recovery prefix, so metadata and aggregation see exactly the committed rows.
"""

import base64
import pickle
from collections.abc import Iterator, Mapping
from contextlib import contextmanager
from contextvars import ContextVar

from demiflow.execution.contracts import state_codec
from demiflow.execution.contracts.dataflow import (
    AggregateState, KernelSpec, RuntimeServices, TerminalResult, stage_resident_bytes,
)
from demiflow.execution.contracts.dataflow_errors import DataflowError
from demiflow.execution.contracts.record_codec import (
    canonical_record_json, decode_document, decode_frame, line_limit,
)
from demiflow.execution.kernels.dataflow import _materialize_row

_FINALIZE_SERVICES = ContextVar("demiflow_kernel_finalize_services", default=None)
AGGREGATE_RESULT_MEDIA_TYPE = "application/x-demiflow-aggregate-result-v3"


def terminal_scalar(stream, *, spec, services, schema_ref=None):
    kind = spec.kind
    if kind not in ('schema', 'columns', 'size_bytes', 'collect_summary'):
        return None
    rows, size, schema = 0, 0, None
    from demiflow.execution.storage.record_schema import RecordSchemas
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


@contextmanager
def bind_finalize(services: RuntimeServices) -> Iterator[None]:
    token = _FINALIZE_SERVICES.set(services)
    try:
        yield
    finally:
        _FINALIZE_SERVICES.reset(token)

def _finalize_services() -> RuntimeServices:
    services = _FINALIZE_SERVICES.get()
    if services is None:
        raise DataflowError("INVALID_CONTRACT", "finalize_aggregate requires bound runtime services")
    return services

def finalize_aggregate(state: AggregateState, *, spec: KernelSpec) -> TerminalResult:
    """K015: restore aggregate names and sum/min/max/mean/std return semantics."""
    services = _finalize_services()
    from demiflow.operators.aggregate import resolve_aggregates, finalize_aggregate_values
    aggregates = resolve_aggregates(spec.configuration, services.programs)
    result = finalize_aggregate_values(state, aggregates, store=services.store,
                                      max_state=services.bounds.max_state_bytes)
    if state.count == 0:
        return TerminalResult(
            kind="aggregate", rows=0, scalar=None, result_ref=None,
            write_result=None, complete=True, completion="source_exhausted",
        )
    raw = pickle.dumps(result, protocol=pickle.HIGHEST_PROTOCOL)
    result_ref = services.store.put(
        iter([raw]), max_bytes=len(raw), media_type=AGGREGATE_RESULT_MEDIA_TYPE
    )
    try:
        canonical_record_json(result)
        scalar = result
    except (ValueError, TypeError):
        scalar = None
    return TerminalResult(
        kind="aggregate", rows=state.count, scalar=scalar, result_ref=result_ref,
        write_result=None, complete=True, completion="source_exhausted",
    )
