"""Bounded worker-side terminal reduction over the already executed output.

No source or transform is evaluated again. The durable spool is also the
recovery prefix, so metadata and aggregation see exactly the committed rows.
"""
import base64
import json

from ..contracts.dataflow_errors import DataflowError
from ..exchange.dataflow_bridge import _decode_record_document
from ..kernels.dataflow import _materialize_row
from ..kernels import state_codec


def terminal_scalar(stream, *, spec, services, occurrence_id, schema_ref=None):
    kind = spec.kind
    if kind not in ('schema', 'columns', 'size_bytes', 'aggregate', 'collect_summary'):
        return None
    aggregates = ()
    if kind == 'aggregate':
        program = services.programs.get(occurrence_id)
        if program is None or program.kind != 'aggregate':
            raise DataflowError('INVALID_CONTRACT', 'terminal aggregate programs are not sealed')
        aggregates = program.value if isinstance(program.value, tuple) else (program.value,)
        states = [a.initial_state() for a in aggregates]
    rows, size, schema = 0, 0, None
    from ..record_schema import RecordSchemas
    schemas = RecordSchemas(services)
    schema = schemas.read(schema_ref)
    stream.flush()
    stream.seek(0)
    try:
        while True:
            services.check_stop()
            line = stream.readline(services.bounds.max_row_bytes + 2)
            if not line:
                break
            if len(line) > services.bounds.max_row_bytes + 1 or not line.endswith(b'\n'):
                raise DataflowError('BUDGET_EXCEEDED', 'terminal record frame exceeds its bound')
            item = json.loads(line)
            record = _decode_record_document(item['document'].encode(), record_id=item['record_id'],
                sequence=item['sequence'], max_bytes=services.bounds.max_row_bytes)
            rows += 1
            size += record.encoded_bytes
            if kind == 'size_bytes':
                continue
            row = _materialize_row(record, services)
            if aggregates:
                for index, aggregate in enumerate(aggregates):
                    partial = aggregate.aggregate_partial((row,))
                    # Admit coexistence before user merge can allocate a copy.
                    state_codec.dumps([states, partial], max_bytes=services.bounds.max_state_bytes)
                    states[index] = aggregate.merge_states(states[index], partial)
                    state_codec.dumps(states, max_bytes=services.bounds.max_state_bytes)
            else:
                import pyarrow as pa
                next_schema = schemas.read(record.schema_ref)
                if next_schema is None:
                    next_schema = pa.Table.from_pylist([row]).schema
                schema = next_schema if schema is None else pa.unify_schemas([schema, next_schema], promote_options='permissive')
        if kind == 'aggregate':
            values = {a.name: a.finalize_state(state) for a, state in zip(aggregates, states)}
            # Typed data permits bytes/tuple/IEEE aggregate results without
            # weakening the control plane's strict-JSON scalar contract.
            encoded = state_codec.dumps(values, max_bytes=services.bounds.max_state_bytes)
            return {'typed_result': base64.b64encode(encoded).decode()}
        if kind == 'size_bytes':
            return size
        if schema is None:
            import pyarrow as pa
            schema = pa.schema([])
        return {'rows': rows, 'size_bytes': size, 'names': schema.names,
                'arrow_schema': base64.b64encode(schema.serialize().to_pybytes()).decode()}
    finally:
        stream.seek(0, 2)
