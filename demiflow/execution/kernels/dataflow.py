"""Worker kernel invocation, scoped effects and bounded record assembly.

The worker owns invocation positions, cancellation, state/observation callbacks
and output record admission. Pure key and batch computation lives in operators;
the versioned record encoding and conversion budgets live in contracts.

Resource accounting: ``encoded_bytes`` is the byte length of the versioned
data-only row codec (``application/vnd.demiflow.record.v3``). It bounds wire/spool size,
not process RSS; Arrow logical bytes and decoded Python objects are accounted
separately where they coexist (see ``invoke_batch``).
"""
from __future__ import annotations

from demiflow.execution.contracts.record_codec import dumps_row as _dumps_row, loads_row as _loads_row, row_bytes as _row_bytes, input_row_bytes as _input_row_bytes
from demiflow.execution.storage.streams import materialize_row as _materialize_row
from demiflow.operators.arrow_batches import batch_to_rows as _batch_to_rows, batch_values as _batch_values, rows_to_batch as _rows_to_batch, single_row_batch as _single_row_batch
from demiflow.operators.ordering import shuffle_key_bytes as _shuffle_key_bytes
from demiflow.execution.contracts.record_codec import record_identity as _hash_id
from demiflow.execution.contracts.dataflow import stage_resident_bytes

import json
from collections.abc import Iterator, Mapping
from typing import Any

from demiflow.execution.contracts.dataflow import (
    ArtifactRef,
    CallableInstance,
    Kernel,
    KernelSpec,
    Record,
    RuntimeServices,
    thaw_record_value,
)
from demiflow.execution.contracts.dataflow_errors import DataflowError
from demiflow.execution.contracts.dataflow_kinds import kinds_with

ROW_MEDIA_TYPE = "application/vnd.demiflow.record.v3"
RECORD_MEDIA_TYPE = "application/x-demiflow-record-v1"

RECORD_DOMAIN = "dataflow-record-v3"

_MISSING = object()


def _err(code: str, message: str, **context: Any) -> DataflowError:
    return DataflowError(code, message, context=context)


def _check_stop(services: RuntimeServices) -> None:
    services.check_stop()


def _cfg(spec: KernelSpec, key: str, default: Any = _MISSING) -> Any:
    value = spec.configuration.get(key, _MISSING)
    if value is _MISSING:
        if default is _MISSING:
            raise _err("INVALID_CONTRACT", "kernel configuration missing field", field=key)
        return default
    return value


def _occurrence(spec: KernelSpec) -> str:
    value = _cfg(spec, "occurrence_id")
    if not isinstance(value, str) or not value:
        raise _err("INVALID_CONTRACT", "occurrence_id must be a non-empty string")
    return value


def _seed(spec: KernelSpec, services: RuntimeServices) -> int:
    value = spec.configuration.get("seed")
    if value is None:
        per_node = services.constants.get('constants', {}).get(_occurrence(spec), {})
        value = per_node.get('seed', per_node.get('random_seed', services.constants.get('random_seed')))
    if isinstance(value, bool) or not isinstance(value, int):
        raise _err("INVALID_CONTRACT", "a resolved integer seed is required")
    return value

















def _measured_input(record, services, retained):
    from demiflow.execution.contracts.record_codec import admit_row_copies
    if record.inline is not None:
        size = admit_row_copies(record.inline, services.bounds, resident_bytes=stage_resident_bytes(getattr(services, 'assignment', None)), retained=retained)
        return _materialize_row(record, store=services.store, bounds=services.bounds, assignment=getattr(services, 'assignment', None)), retained + 2 * size
    value = _materialize_row(record, store=services.store, bounds=services.bounds, assignment=getattr(services, 'assignment', None), measured=True, retained=retained)
    size = admit_row_copies(value, services.bounds, resident_bytes=stage_resident_bytes(getattr(services, 'assignment', None)), retained=retained)
    return value, retained + size


def _output_record(
    record_id: str,
    source_position: Mapping[str, Any],
    row: object,
    sequence: int,
    services: RuntimeServices,
    *, schema_ref=None, retained=None,
) -> Record:
    if retained is not None:
        from demiflow.execution.contracts.record_codec import admit_row_copies
        admit_row_copies(row, services.bounds, resident_bytes=stage_resident_bytes(getattr(services, 'assignment', None)), retained=retained)
    from demiflow.execution.contracts.record_codec import _encode_payload
    from demiflow.execution.contracts.dataflow_codec import _thaw_json
    data = _encode_payload(row, max_bytes=services.bounds.max_row_bytes)
    if len(data) > services.bounds.max_row_bytes:
        raise _err("BUDGET_EXCEEDED", "output row exceeds max_row_bytes", size=len(data))
    return Record(
        record_id=record_id,
        source_position=_thaw_json(source_position),
        payload_ref=None,
        inline=row,
        encoded_bytes=len(data),
        sequence=sequence,
        schema_ref=schema_ref,
    )


class _OutputBudget:
    """Pre-yield output row/byte admission; never truncates (C06)."""

    def __init__(self, spec: KernelSpec, services: RuntimeServices, *, streaming=False) -> None:
        self._max_rows = services.bounds.max_output_rows
        self._max_bytes = spec.output_bound_bytes
        self._streaming = streaming
        self._rows = 0
        self._bytes = 0

    def charge(self, record: Record) -> None:
        self._rows += 1
        self._bytes += record.encoded_bytes
        if self._max_rows > 0 and self._rows > self._max_rows:
            raise _err("BUDGET_EXCEEDED", "output row budget exceeded", rows=self._rows)
        # Finite relational scopes stream successive rows through bounded
        # channels; their resident output allowance is not a lifetime byte cap.
        # UDF expansion/batches retain the cumulative per-invocation allowance.
        admitted = record.encoded_bytes if self._streaming else self._bytes
        if self._max_bytes > 0 and admitted > self._max_bytes:
            raise _err("BUDGET_EXCEEDED", "output byte budget exceeded", bytes=admitted)


def _invoke_scoped(instance, value, records, spec):
    from demiflow.execution.worker.row_scope import RowScope, current_row_scope, set_row_scope
    previous = current_row_scope()
    from demiflow.execution.kernels.concurrent_calls import INVOCATION_SCOPE
    explicit = INVOCATION_SCOPE.get()
    scope = getattr(instance, 'native_scope', None)
    if len(records) == 1 and records[0].source_position.get('queue_delivery_kind') == 'producer':
        # Native producer progress survives into a new RunContext. A run-local
        # call ordinal would replay the previous unit's receipt after seeking.
        occurrence = _hash_id(['producer-call-v3', records[0].record_id, _occurrence(spec)])
    elif explicit is not None:
        occurrence = explicit
    elif scope is not None:
        # Source record identity is deliberately preserved by union. Native
        # effects need the invocation occurrence, including its multiplicity;
        # this cursor is checkpointed with the owning kernel, never inferred
        # from output count (an invocation can emit zero or many rows).
        occurrence = _hash_id(['dataflow-call-v3', *scope, instance.native_call_ordinal])
        instance.native_call_ordinal += 1
    else:
        occurrence = _hash_id(['dataflow-call-v3', _occurrence(spec),
                               [record.record_id for record in records]])
    delivery = None
    if len(records) == 1 and records[0].source_position.get('queue_delivery_ref'):
        import json
        from demiflow.execution.contracts.dataflow import ArtifactRef
        delivery = ArtifactRef(**json.loads(records[0].source_position['queue_delivery_ref']))
    set_row_scope(RowScope(occurrence, delivery))
    try:
        return instance(value)
    finally:
        set_row_scope(previous)


def _business_miss(exc, cfg):
    from demiflow.data.runtime_contract import RuntimeContractError
    from demiflow.execution.recovery_contract import RecoveryContractError
    from demiflow.services.request_limits import ServiceStopped
    from demiflow.operator_llm.journal import UncertainPromptCall
    # Only the callback deadline class certifies that cancellation drained;
    # ordinary control/budget/unknown-effect failures can never become a miss.
    from demiflow.execution.kernels import async_instance
    callback_timeout = getattr(async_instance, 'CallbackDeadlineExceeded', None)
    if (callback_timeout is not None and isinstance(exc, callback_timeout)
            and cfg.get('hard_timeout') is not None):
        names = {cls.__module__ + '.' + cls.__qualname__ for cls in TimeoutError.__mro__}
        return 'TimeoutError' if set(cfg.get('catch', ())) & names else 'hard_timeout'
    if isinstance(exc, (RuntimeContractError, DataflowError,
                        RecoveryContractError, ServiceStopped, UncertainPromptCall)):
        return None
    admitted = set(cfg.get('catch', ()))
    names = {cls.__module__ + '.' + cls.__qualname__ for cls in type(exc).__mro__}
    return type(exc).__name__ if admitted & names else None


def invoke_unary(
    spec: KernelSpec,
    record: Record,
    *,
    instance: CallableInstance,
    services: RuntimeServices,
    schemas=None,
    observe=None,
    state_bytes=None,
) -> Iterator[Record]:
    """K002: per-row map/filter/flat_map/column/sample/limit semantics."""
    _check_stop(services)
    kind = spec.kind
    cfg = spec.configuration
    if kind == 'LimitOp':
        yield record
        return
    if kind in ('SelectColumnsOp', 'DropColumnsOp', 'RenameColumnsOp') and record.payload_ref is not None:
        from demiflow.execution.contracts.record_codec import ARROW_MEDIA_TYPES
        from demiflow.execution.storage.arrow_payload import batch_cache
        from demiflow.operators.relational import project_columns
        if record.payload_ref.media_type in ARROW_MEDIA_TYPES:
            cache = batch_cache(services.store, services.bounds, assignment=getattr(services, 'assignment', None))
            batch, _ = cache.locate(record)
            projection = project_columns(batch.schema.names, kind, cfg)
            if projection is not None:
                if schemas is None:
                    from demiflow.execution.storage.record_schema import RecordSchemas
                    schemas = RecordSchemas(services)
                projected = cache.project(record, *projection, schemas)
                if projected is not None:
                    yield projected
                    return
    low_memory = getattr(services, 'measured_workspace', False)
    output_record = _output_record
    if low_memory or state_bytes is not None:
        initial_state = state_bytes() if state_bytes else 0
        row, retained = _measured_input(record, services, initial_state)
        input_bytes = retained - initial_state
        def output_record(*args, **kwargs):
            current = state_bytes() if state_bytes else 0
            return _output_record(*args, **kwargs, retained=input_bytes + current)
    else:
        row = _materialize_row(record, store=services.store, bounds=services.bounds, assignment=getattr(services, 'assignment', None))
    position = record.source_position
    sequence = record.sequence
    if schemas is None:
        from demiflow.execution.storage.record_schema import RecordSchemas
        schemas = RecordSchemas(services)

    if kind in ("MapOp", "BoundMapOp") and not cfg.get("streaming_map"):
        import time
        started = time.perf_counter_ns()
        try:
            result = instance(row)
        finally:
            if observe is not None:
                observe(time.perf_counter_ns() - started)
        if not isinstance(result, Mapping):
            if position.get('queue_delivery_ref'):
                from demiflow.execution.recovery_contract import RecoveryContractError
                raise RecoveryContractError('Task map must preserve exactly one row')
            raise _err("INVALID_CONTRACT", "map callable must return a mapping row")
        yield output_record(record.record_id, position, result, sequence, services)
    elif kind == "FilterOp":
        selected = instance(row)
        if state_bytes is not None:
            from demiflow.execution.contracts.record_codec import admit_workspace
            admit_workspace(state_bytes(), services.bounds, resident_bytes=stage_resident_bytes(getattr(services, 'assignment', None)), retained=input_bytes)
        if selected:
            yield record
    elif kind == "FlatMapOp":
        occurrence = _occurrence(spec)
        values = instance(row)
        if isinstance(values, Mapping):
            raise _err("INVALID_CONTRACT", "flat_map callable must return an iterable of rows")
        iterator = iter(values)
        budget = _OutputBudget(spec, services)
        try:
            for index, item in enumerate(iterator):
                _check_stop(services)
                if not isinstance(item, Mapping):
                    raise _err("INVALID_CONTRACT", "flat_map output rows must be mappings")
                child_id = _hash_id([RECORD_DOMAIN, record.record_id, occurrence, index])
                out = output_record(child_id, position, item, sequence, services)
                budget.charge(out)
                yield out
            if state_bytes is not None:
                from demiflow.execution.contracts.record_codec import admit_workspace
                admit_workspace(state_bytes(), services.bounds, resident_bytes=stage_resident_bytes(getattr(services, 'assignment', None)), retained=input_bytes)
        finally:
            close = getattr(iterator, "close", None)
            if close is not None:
                close()
    elif kind in ("MapOp", "BoundMapOp", "AsyncMapOp", "DeduplicateOp", "AgentMapOp", "OperatorLLMMapOp",
                  "prompt", "agent", "fetch", "document", "VectorSearchOp", "SearchWebOp",
                  "RegisterDocumentsOp", "ReadDocumentsOp"):
        import time
        started, miss = time.perf_counter_ns(), None
        try:
            try:
                result = _invoke_scoped(instance, row, (record,), spec)
            except Exception as exc:
                miss = _business_miss(exc, cfg)
                if miss is None:
                    raise
                return
            if result is None:
                miss = 'drop'
        finally:
            if observe is not None:
                observe(time.perf_counter_ns() - started, miss)
        if (position.get('queue_delivery_ref') and position.get('queue_delivery_kind') != 'producer'
                and not isinstance(result, Mapping)):
            from demiflow.execution.recovery_contract import RecoveryContractError
            raise RecoveryContractError('Task map must preserve exactly one row')
        if result is None:
            return
        rows = (result,) if isinstance(result, Mapping) else result
        budget = _OutputBudget(spec, services)
        for index, out_row in enumerate(rows):
            _check_stop(services)
            if not isinstance(out_row, Mapping):
                raise _err("INVALID_CONTRACT", "async map must return row, rows or None")
            child = record.record_id if index == 0 else _hash_id([RECORD_DOMAIN, record.record_id, _occurrence(spec), index])
            out = output_record(child, position, out_row, sequence, services)
            budget.charge(out)
            yield out
    elif kind == "SelectColumnsOp":
        columns = tuple(_cfg(spec, "columns"))
        from demiflow.operators.relational import select_columns
        projected = select_columns(row, columns)
        yield output_record(record.record_id, position, projected, sequence, services,
                             schema_ref=schemas.project(record.schema_ref, columns=columns))
    elif kind == "DropColumnsOp":
        columns = tuple(_cfg(spec, "columns"))
        from demiflow.operators.relational import drop_columns
        kept = drop_columns(row, columns)
        yield output_record(record.record_id, position, kept, sequence, services,
                             schema_ref=schemas.project(record.schema_ref, columns=tuple(kept)))
    elif kind == "RenameColumnsOp":
        names = _cfg(spec, "names")
        from demiflow.operators.relational import rename_columns
        renamed = rename_columns(row, names)
        yield output_record(record.record_id, position, renamed, sequence, services,
                             schema_ref=schemas.project(record.schema_ref, names=tuple(renamed)))
    elif kind == "AddColumnOp":
        column = _cfg(spec, "column")
        batch_format = cfg.get("batch_format", "default")
        batch, extra = _single_row_batch(row, batch_format, services.bounds)
        if _input_row_bytes(record) + extra > services.bounds.max_batch_bytes:
            raise _err("BUDGET_EXCEEDED", "add_column batch coexistence exceeds budget")
        values = _batch_values(instance(batch))
        if len(values) != 1:
            raise _err("INVALID_CONTRACT", "add_column result length must match batch")
        yield output_record(
            record.record_id, position, {**row, column: values[0]}, sequence, services
        )
    elif kind == "RandomSampleOp":
        fraction = float(_cfg(spec, "fraction"))
        if not 0.0 <= fraction <= 1.0:
            raise _err("INVALID_CONTRACT", "sample fraction must be in [0, 1]")
        seed = _seed(spec, services)
        key = _shuffle_key_bytes(record.record_id, seed=seed, occurrence_id=_occurrence(spec))
        if int.from_bytes(key[:8], "big") / float(1 << 64) < fraction:
            yield record
    else:
        raise _err("INVALID_CONTRACT", "kind is not a unary kernel", kind=kind)


def _callback_table(records, services, *, zero_copy):
    """Reassemble the original business batch from validated, consecutive IPC slices."""
    import pyarrow as pa
    from demiflow.execution.contracts.record_codec import _resident_budget
    from demiflow.execution.storage.arrow_payload import batch_cache
    cache = batch_cache(services.store, services.bounds, assignment=getattr(services, 'assignment', None))
    limit = min(services.bounds.max_batch_bytes,
                _resident_budget(services.bounds, getattr(services, 'assignment', None)))
    pieces = []
    retained = 0
    # Count each backing buffer once per contiguous artifact run. An
    # interleaving is safe too: a replacement cache may allocate a new parent.
    previous = None
    for record in records:
        services.check_stop()
        if record.payload_ref != previous:
            if retained + 4 * record.payload_ref.size_bytes > limit:
                return None
            previous = record.payload_ref
            new_ref = True
        else:
            new_ref = False
        batch, index = cache.locate(record)
        if new_ref:
            retained += batch.get_total_buffer_size() + record.payload_ref.size_bytes
            if retained > limit:
                return None
        if pieces and not new_ref and pieces[-1][1] + pieces[-1][2] == index:
            pieces[-1][2] += 1
        else:
            pieces.append([batch, index, 1])
    table = pa.Table.from_batches([part.slice(start, count) for part, start, count in pieces])
    # Keep zero-copy slices when there is no room to consolidate. With room,
    # compact fragmented scans columnwise so the callback and writer receive
    # one native chunk per column instead of one artifact per input row.
    if not zero_copy and len(pieces) > 1 and 3 * retained + 256 * len(table.schema) <= limit:
        table = table.combine_chunks()
    return table


def _mapped_arrow_output(result, *, binding, schemas, services, budget, retained_bytes):
    """Consume callback outputs once and leave each original occurrence with its ACK owner."""
    import pyarrow as pa
    from collections.abc import Iterator
    from demiflow.execution.contracts.record_codec import _MAX_ROWS
    from demiflow.execution.contracts.record_codec import _resident_budget
    from demiflow.execution.storage.streams import store_mapped_arrow

    outputs = result if isinstance(result, Iterator) else (result,)
    ordinal = 0
    try:
        for output in outputs:
            if isinstance(output, (pa.RecordBatch, pa.Table)):
                parent_bytes = output.get_total_buffer_size()
                if retained_bytes + parent_bytes > _resident_budget(services.bounds, getattr(services, 'assignment', None)):
                    raise _err('BUDGET_EXCEEDED', 'Arrow callback input and retained result exceed the worker grant')
                # Each membership digest costs 67 bytes in the bounded IPC header.
                max_rows = min(_MAX_ROWS, max(1, (min(8192, services.bounds.max_control_bytes) - 256) // 80))
                table = pa.Table.from_batches([output]) if isinstance(output, pa.RecordBatch) else output
                batches = table.to_reader(max_chunksize=max_rows)
                try:
                    for chunk in batches:
                        _check_stop(services)
                        if not chunk.num_rows:
                            continue
                        schema_ref = schemas.put(chunk.schema)
                        members = [binding(ordinal + i) for i in range(chunk.num_rows)]
                        carried = store_mapped_arrow(chunk, members, schema_ref, store=services.store, bounds=services.bounds, assignment=getattr(services, "assignment", None),
                            retained_bytes=retained_bytes + parent_bytes)
                        if carried is None:
                            # Unsupported types retain their exact historical typed-row validation.
                            for i, (key, source) in enumerate(members):
                                row = chunk.slice(i, 1).to_pylist()[0]
                                record = _output_record(key, source.source_position, row, source.sequence,
                                                        services, schema_ref=schema_ref)
                                _check_stop(services)
                                budget.charge(record)
                                yield record
                        else:
                            for record in carried:
                                _check_stop(services)
                                budget.charge(record)
                                yield record
                        ordinal += chunk.num_rows
                finally:
                    batches.close()
            else:
                schema_ref = None
                def set_schema(value):
                    nonlocal schema_ref
                    schema_ref = schemas.put(value)
                for row in _batch_to_rows(output, 'pyarrow', on_schema=set_schema):
                    _check_stop(services)
                    key, source = binding(ordinal)
                    record = _output_record(key, source.source_position, row, source.sequence,
                                            services, schema_ref=schema_ref)
                    budget.charge(record)
                    yield record
                    ordinal += 1
    finally:
        close = getattr(outputs, 'close', None)
        if close is not None:
            close()


def invoke_batch(
    spec: KernelSpec,
    records: tuple[Record, ...],
    *,
    instance: CallableInstance,
    services: RuntimeServices,
    schemas=None,
    observe=None,
    observe_dead=None,
) -> Iterator[Record]:
    """K003: map_batches (format/zero-copy) and batch_map (max_batch) semantics."""
    _check_stop(services)
    kind = spec.kind
    cfg = spec.configuration
    input_bytes = sum(_input_row_bytes(record) for record in records)
    budget = _OutputBudget(spec, services)
    if schemas is None:
        from demiflow.execution.storage.record_schema import RecordSchemas
        schemas = RecordSchemas(services)
    schema = schemas.read(records[0].schema_ref) if records else None
    if any(record.schema_ref != records[0].schema_ref for record in records):
        raise _err('INVALID_CONTRACT', 'one business batch cannot mix declared schemas')
    output_ref = [None]
    expected = cfg.get("predict_output_schema")
    keyed = expected is not None and cfg.get('batch_size', 1) > 1
    occurrence = spec.configuration.get("occurrence_id")

    def binding(index):
        if index < len(records):
            return records[index].record_id, records[index]
        source = records[-1]
        return _hash_id([RECORD_DOMAIN, source.record_id, occurrence, index]), source

    if (kind == 'MapBatchesOp' and cfg.get('batch_format') == 'pyarrow' and records
            and expected is None):
        from demiflow.execution.contracts.record_codec import is_arrow_record
        if all(is_arrow_record(record) for record in records):
            table = _callback_table(records, services, zero_copy=cfg.get('zero_copy_batch', False))
            if table is not None:
                yield from _mapped_arrow_output(instance(table), binding=binding, schemas=schemas,
                    services=services, budget=budget, retained_bytes=table.get_total_buffer_size())
                return
    retained = None
    if getattr(services, 'measured_workspace', False):
        rows, retained = [], 0
        for record in records:
            value, retained = _measured_input(record, services, retained)
            rows.append(value)
    else:
        rows = [_materialize_row(record, store=services.store, bounds=services.bounds, assignment=getattr(services, 'assignment', None)) for record in records]
    if expected is not None:
        import pyarrow as pa
        from demiflow.data.training_schema import PREDICTION_ROW_ID, validate_columns
        required = cfg['predict_input_schema']
        if schema is not None:
            validate_columns(schema, required)
        else:
            for row in rows:
                validate_columns(pa.RecordBatch.from_pylist([row]).schema, required)
        if keyed:
            if any(PREDICTION_ROW_ID in row for row in rows):
                raise _err('INVALID_CONTRACT', 'Predictor input contains the reserved correlation column')
            input_bytes += sum(len(record.record_id) + len(PREDICTION_ROW_ID) + 64 for record in records)
            if input_bytes > services.bounds.max_batch_bytes:
                raise _err('BUDGET_EXCEEDED', 'Prediction input identities exceed the batch budget')
            # Union can deliver the same source record through two branches.
            # Include its position in this call so both results have to echo
            # their own occurrence even when their source identities match.
            prediction_ids = tuple(_hash_id([rec.record_id, i]) for i, rec in enumerate(records))
            rows = [{**row, PREDICTION_ROW_ID: key} for row, key in zip(rows, prediction_ids)]
            if schema is not None:
                schema = schema.append(pa.field(PREDICTION_ROW_ID, pa.string()))

    def output_schema(value):
        if expected is not None and value is not None:
            validate_columns(value, expected)
            if keyed:
                validate_columns(value, {PREDICTION_ROW_ID: 'string'})
                value = value.remove(value.get_field_index(PREDICTION_ROW_ID))
        output_ref[0] = schemas.put(value)

    if kind == "MapBatchesOp":
        batch_format = cfg.get("batch_format", "default")
        zero_copy = bool(cfg.get("zero_copy_batch", False))
        batch, extra = _rows_to_batch(rows, batch_format, services.bounds, schema=schema)
        # Arrow/pandas cast keeps the source Python rows alive alongside the
        # converted buffers; admit both before invoking the callable (C06/V023).
        if input_bytes + extra > services.bounds.max_batch_bytes:
            raise _err(
                "BUDGET_EXCEEDED",
                "batch conversion coexistence exceeds max_batch_bytes",
                input_bytes=input_bytes,
                converted_bytes=extra,
            )
        result = instance(batch)
        out_rows = _batch_to_rows(result, batch_format, on_schema=output_schema)
    elif kind in ("BatchMapOp", "AddColumnOp", "embedding", "EmbeddingMapOp", "RegisterDocumentsBatchOp", "SaveLanceOp"):
        if input_bytes > services.bounds.max_batch_bytes:
            raise _err("BUDGET_EXCEEDED", "batch exceeds max_batch_bytes", bytes=input_bytes)
        if kind == "AddColumnOp":
            batch_format = cfg.get("batch_format", "default")
            batch, extra = _rows_to_batch(rows, batch_format, services.bounds, schema=schema)
            if input_bytes + extra > services.bounds.max_batch_bytes:
                raise _err("BUDGET_EXCEEDED", "add_column coexistence exceeds budget")
            values = _batch_values(instance(batch))
            if len(values) != len(rows):
                raise _err("INVALID_CONTRACT", "add_column result length must match batch")
            column = _cfg(spec, "column")
            if schema is not None:
                import pyarrow as pa
                fields = [field for field in schema if field.name != column]
                fields.append(pa.field(column, pa.array(values).type))
                output_schema(pa.schema(fields, metadata=schema.metadata))
            out_rows = [{**row, column: value} for row, value in zip(rows, values)]
        else:
            if kind == "SaveLanceOp":
                from demiflow.lance.flow_contract import stream_lance_output_schema
                output_schema(stream_lance_output_schema(schema, cfg.get("output_ref")))
                instance.bind_records(tuple(record.record_id for record in records), instance.native_call_ordinal)
            import time
            started, miss = time.perf_counter_ns(), None
            try:
                try:
                    result = _invoke_scoped(instance, rows, records, spec)
                except Exception as exc:
                    miss = _business_miss(exc, cfg)
                    if miss is None:
                        raise
                    if observe_dead is not None:
                        observe_dead(rows, miss, max_bytes=min(16384, services.bounds.max_control_bytes // 16))
                    return
                if result is None:
                    miss = 'drop'
                    if observe_dead is not None:
                        observe_dead(rows, miss, max_bytes=min(16384, services.bounds.max_control_bytes // 16))
                    return
            finally:
                if observe is not None:
                    observe(time.perf_counter_ns() - started, miss)
            out_rows = _batch_to_rows(result, "rows")
    else:
        raise _err("INVALID_CONTRACT", "kind is not a batch kernel", kind=kind)

    count = 0
    try:
        for index, out_row in enumerate(out_rows):
            _check_stop(services)
            if expected is not None:
                if index >= len(records):
                    raise _err('INVALID_CONTRACT', 'Predictor returned more rows than its input batch')
                if keyed and out_row.pop(PREDICTION_ROW_ID, None) != prediction_ids[index]:
                    raise _err('INVALID_CONTRACT', 'Predictor changed the input occurrence order')
                count += 1
            record_id, source = binding(index)
            if expected is not None and output_ref[0] is None:
                validate_columns(pa.RecordBatch.from_pylist([out_row]).schema, expected)
            out = _output_record(record_id, source.source_position, out_row, source.sequence, services,
                                 schema_ref=output_ref[0], retained=retained)
            budget.charge(out)
            yield out
        if expected is not None and count != len(records):
            raise _err('INVALID_CONTRACT', 'Predictor must return one result row for every input')
    finally:
        close = getattr(out_rows, 'close', None)
        if close is not None:
            close()


def finish_kernel(
    kernel: Kernel, *, port: str, services: RuntimeServices
) -> Iterator[Record]:
    """K004: forward input EOI; the owning kernel flushes only when ports align."""
    _check_stop(services)
    yield from kernel.finish(port)


# Kinds assembled by the single factory. Relational kinds come from KeyedSource
# (join/reduce/groups/exclude_keys) and UnionSource. Each set is the kinds that
# carry the role of the same name in the kind table.
_UNARY_KINDS = kinds_with("unary")
_BATCH_KINDS = kinds_with("batch")
_MODEL_KINDS = kinds_with("model")
_RELATIONAL_KINDS = kinds_with("relational")
_ORDERING_KINDS = kinds_with("ordering")
_AGGREGATE_KINDS = kinds_with("aggregate")
_STREAM_GROUP_KINDS = kinds_with("stream_group")

KERNEL_KINDS = (
    _UNARY_KINDS | _BATCH_KINDS | _MODEL_KINDS | _RELATIONAL_KINDS
    | _ORDERING_KINDS | _AGGREGATE_KINDS | _STREAM_GROUP_KINDS
)


def make_kernel(
    spec: KernelSpec, *, services: RuntimeServices, partition_id: str,
    finite_services: RuntimeServices | None = None,
) -> Kernel:
    """K001: the single registry-backed factory; unknown kinds fail before start."""
    kind = spec.kind
    if kind not in KERNEL_KINDS:
        raise _err("INVALID_CONTRACT", "unknown kernel kind", kind=kind)
    if kind not in _AGGREGATE_KINDS and spec.configuration.get("cache_namespace") is not None:
        from demiflow.execution.kernels.model_state import bind_cached_map

        return bind_cached_map(spec, services=services)
    from demiflow.execution.kernels.operator import OperatorKernel

    return OperatorKernel(spec, services=services, partition_id=partition_id,
                          finite_services=finite_services)


def invoke_native_batch(batch, transform, on_callback_error):
    """Invoke the sealed native batch callback without owning its algorithm or sink."""
    if transform is not None:
        try:
            batch = transform(batch)
        except Exception as error:
            on_callback_error(error)
            raise
    return batch
