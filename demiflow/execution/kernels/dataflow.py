"""Generic kernel dispatch and the shared bounded record helpers (lane K).

K001 ``make_kernel`` is the single factory; K002/K003 are the pure row/batch
compute entry points; K004 forwards input EOI to the owning kernel. The private
helpers below are the file-owner assembly shared by the other four kernel
modules; they implement the versioned business record codec, the fidelity
preserving key encoding and the pre-allocation budget checks required by C06.

Resource accounting: ``encoded_bytes`` is the byte length of the versioned
data-only row codec (``application/vnd.demiflow.record.v3``). It bounds wire/spool size,
not process RSS; Arrow logical bytes and decoded Python objects are accounted
separately where they coexist (see ``invoke_batch``).
"""
from __future__ import annotations

import hashlib
import json
import pickle
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

ROW_MEDIA_TYPE = "application/vnd.demiflow.record.v3"
RECORD_MEDIA_TYPE = "application/x-demiflow-record-v1"
SHUFFLE_DOMAIN = "demiflow-shuffle-v3"
RECORD_DOMAIN = "dataflow-record-v3"

_MISSING = object()


def _canonical(value: Any) -> str:
    return json.dumps(
        thaw_record_value(value), ensure_ascii=False, sort_keys=True, separators=(",", ":"), allow_nan=False
    )


def _hash_id(parts: Any) -> str:
    return hashlib.sha256(_canonical(parts).encode("utf-8")).hexdigest()


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
        value = services.constants.get("random_seed")
    if isinstance(value, bool) or not isinstance(value, int):
        raise _err("INVALID_CONTRACT", "a resolved integer seed is required")
    return value


def _shuffle_key_bytes(record_id: str, *, seed: int, occurrence_id: str) -> bytes:
    # Single versioned formula; ordering.shuffle_key is the public wrapper.
    return hashlib.sha256(
        _canonical([SHUFFLE_DOMAIN, seed, occurrence_id, record_id]).encode("utf-8")
    ).digest()


def _key_token(value: Any) -> Any:
    """Type-tagged encoding keeping -0.0/bytes/tuple/IEEE distinct (C01/C06)."""
    if value is None:
        return ["n"]
    if isinstance(value, bool):
        return ["b", value]
    if isinstance(value, int):
        return ["i", value]
    if isinstance(value, float):
        return ["f", repr(value)]
    if isinstance(value, str):
        return ["s", value]
    if isinstance(value, bytes):
        return ["y", value.hex()]
    if isinstance(value, tuple):
        return ["t", [_key_token(item) for item in value]]
    if isinstance(value, list):
        return ["l", [_key_token(item) for item in value]]
    if isinstance(value, Mapping):
        items = sorted(value.items(), key=lambda kv: _canonical(_key_token(kv[0])))
        return ["d", [[_key_token(k), _key_token(v)] for k, v in items]]
    raise _err("INVALID_CONTRACT", "unsupported key value type", type=type(value).__name__)


def _group_key(row: Mapping[str, Any], fields: tuple[str, ...]) -> str:
    try:
        return _canonical([_key_token(row[field]) for field in fields])
    except KeyError as exc:
        raise _err("INVALID_CONTRACT", "group/join key field missing", field=str(exc)) from exc


def _dumps_row(row: Mapping[str, Any]) -> bytes:
    from ..exchange.dataflow_bridge import _encode_payload
    from ..recovery_contract import MAX_DOCUMENT_BYTES
    return _encode_payload(row, max_bytes=MAX_DOCUMENT_BYTES)


def _loads_row(data: bytes) -> dict[str, Any]:
    from ..exchange.dataflow_bridge import _decode_payload
    return _decode_payload(data, max_bytes=max(1, len(data)))


def _row_bytes(row: Mapping[str, Any]) -> int:
    return len(_dumps_row(row))


def _input_row_bytes(record: Record) -> int:
    if record.inline is not None:
        return record.encoded_bytes
    if record.payload_ref is not None:
        return record.payload_ref.size_bytes
    raise _err("INVALID_CONTRACT", "record carries neither inline nor payload_ref")


def _materialize_row(record: Record, services: RuntimeServices) -> dict[str, Any]:
    if (record.inline is None) == (record.payload_ref is None):
        raise _err("INVALID_CONTRACT", "record must carry exactly one of inline/payload_ref")
    if record.inline is not None:
        return thaw_record_value(record.inline)
    ref = record.payload_ref
    limit = services.bounds.max_row_bytes
    if ref.size_bytes > limit:
        raise _err("BUDGET_EXCEEDED", "single row exceeds max_row_bytes", size=ref.size_bytes)
    data = b"".join(services.store.read(ref, max_bytes=limit))
    if len(data) > limit:
        raise _err("BUDGET_EXCEEDED", "single row exceeds max_row_bytes", size=len(data))
    return _loads_row(data)


def _output_record(
    record_id: str,
    source_position: Mapping[str, Any],
    row: Mapping[str, Any],
    sequence: int,
    services: RuntimeServices,
) -> Record:
    from ..exchange.dataflow_bridge import _encode_payload
    data = _encode_payload(row, max_bytes=services.bounds.max_row_bytes)
    if len(data) > services.bounds.max_row_bytes:
        raise _err("BUDGET_EXCEEDED", "output row exceeds max_row_bytes", size=len(data))
    return Record(
        record_id=record_id,
        source_position=dict(source_position),
        payload_ref=None,
        inline=dict(row),
        encoded_bytes=len(data),
        sequence=sequence,
    )


class _OutputBudget:
    """Pre-yield output row/byte admission; never truncates (C06)."""

    def __init__(self, spec: KernelSpec, services: RuntimeServices) -> None:
        self._max_rows = services.bounds.max_output_rows
        self._max_bytes = spec.output_bound_bytes
        self._rows = 0
        self._bytes = 0

    def charge(self, record: Record) -> None:
        self._rows += 1
        self._bytes += record.encoded_bytes
        if self._max_rows > 0 and self._rows > self._max_rows:
            raise _err("BUDGET_EXCEEDED", "output row budget exceeded", rows=self._rows)
        if self._max_bytes > 0 and self._bytes > self._max_bytes:
            raise _err("BUDGET_EXCEEDED", "output byte budget exceeded", bytes=self._bytes)


def _single_row_batch(
    row: Mapping[str, Any], batch_format: str, services: RuntimeServices
) -> tuple[Any, int]:
    if batch_format in ("default", "pandas"):
        import pandas as pd

        frame = pd.DataFrame([dict(row)])
        return frame, int(frame.memory_usage(deep=True).sum())
    if batch_format == "pyarrow":
        import pyarrow as pa

        table = pa.Table.from_pylist([dict(row)])
        return table, table.nbytes
    raise _err("UNSUPPORTED_CAPABILITY", "unknown batch_format", format=batch_format)


def _batch_values(result: Any) -> list[Any]:
    if hasattr(result, "to_pylist"):
        return list(result.to_pylist())
    return list(result)


def invoke_unary(
    spec: KernelSpec,
    record: Record,
    *,
    instance: CallableInstance,
    services: RuntimeServices,
) -> Iterator[Record]:
    """K002: per-row map/filter/flat_map/column/sample/limit semantics."""
    _check_stop(services)
    kind = spec.kind
    cfg = spec.configuration
    row = _materialize_row(record, services)
    position = record.source_position
    sequence = record.sequence

    if kind in ("MapOp", "BoundMapOp"):
        result = instance(row)
        if not isinstance(result, Mapping):
            raise _err("INVALID_CONTRACT", "map callable must return a mapping row")
        yield _output_record(record.record_id, position, result, sequence, services)
    elif kind == "FilterOp":
        if instance(row):
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
                out = _output_record(child_id, position, item, sequence, services)
                budget.charge(out)
                yield out
        finally:
            close = getattr(iterator, "close", None)
            if close is not None:
                close()
    elif kind in ("AsyncMapOp", "DeduplicateOp", "AgentMapOp", "OperatorLLMMapOp",
                  "prompt", "agent", "fetch", "document", "VectorSearchOp", "SearchWebOp",
                  "RegisterDocumentsOp", "ReadDocumentsOp"):
        try:
            result = instance(row)
        except Exception as exc:
            admitted = set(cfg.get("catch", ()))
            names = {cls.__module__ + "." + cls.__qualname__ for cls in type(exc).__mro__}
            if not (admitted & names):
                raise
            return
        if result is None:
            return
        rows = (result,) if isinstance(result, Mapping) else result
        budget = _OutputBudget(spec, services)
        for index, out_row in enumerate(rows):
            _check_stop(services)
            if not isinstance(out_row, Mapping):
                raise _err("INVALID_CONTRACT", "async map must return row, rows or None")
            child = record.record_id if index == 0 else _hash_id([RECORD_DOMAIN, record.record_id, _occurrence(spec), index])
            out = _output_record(child, position, out_row, sequence, services)
            budget.charge(out)
            yield out
    elif kind == "LimitOp":
        yield record
    elif kind == "SelectColumnsOp":
        columns = tuple(_cfg(spec, "columns"))
        try:
            projected = {column: row[column] for column in columns}
        except KeyError as exc:
            raise _err("INVALID_CONTRACT", "select column missing", field=str(exc)) from exc
        yield _output_record(record.record_id, position, projected, sequence, services)
    elif kind == "DropColumnsOp":
        columns = tuple(_cfg(spec, "columns"))
        missing = [column for column in columns if column not in row]
        if missing:
            raise _err("INVALID_CONTRACT", "drop column missing", fields=missing)
        kept = {key: value for key, value in row.items() if key not in columns}
        yield _output_record(record.record_id, position, kept, sequence, services)
    elif kind == "RenameColumnsOp":
        names = _cfg(spec, "names")
        if isinstance(names, Mapping):
            targets = [names.get(key, key) for key in row]
            if len(targets) != len(set(targets)):
                raise _err("INVALID_CONTRACT", "rename produces duplicate column names")
            renamed = {names.get(key, key): value for key, value in row.items()}
        else:
            targets = tuple(names)
            if len(row) != len(targets):
                raise _err("INVALID_CONTRACT", "rename list length must match schema")
            renamed = dict(zip(targets, row.values()))
        yield _output_record(record.record_id, position, renamed, sequence, services)
    elif kind == "AddColumnOp":
        column = _cfg(spec, "column")
        batch_format = cfg.get("batch_format", "default")
        batch, extra = _single_row_batch(row, batch_format, services)
        if _input_row_bytes(record) + extra > services.bounds.max_batch_bytes:
            raise _err("BUDGET_EXCEEDED", "add_column batch coexistence exceeds budget")
        values = _batch_values(instance(batch))
        if len(values) != 1:
            raise _err("INVALID_CONTRACT", "add_column result length must match batch")
        yield _output_record(
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


def _rows_to_batch(
    rows: list[dict[str, Any]], batch_format: str, services: RuntimeServices
) -> tuple[Any, int]:
    if batch_format in ("default", "numpy"):
        import numpy as np
        columns = rows[0].keys() if rows else ()
        batch = {name: np.asarray([row[name] for row in rows]) for name in columns}
        return batch, sum(v.nbytes for v in batch.values())
    if batch_format == "pandas":
        import pandas as pd

        frame = pd.DataFrame(rows)
        return frame, int(frame.memory_usage(deep=True).sum())
    if batch_format == "pyarrow":
        import pyarrow as pa

        table = pa.Table.from_pylist(rows)
        return table, table.nbytes
    raise _err("UNSUPPORTED_CAPABILITY", "unknown batch_format", format=batch_format)


def _batch_to_rows(result: Any, batch_format: str) -> list[dict[str, Any]]:
    if isinstance(result, Mapping):
        lengths = {len(column) for column in result.values()}
        if len(lengths) > 1:
            raise _err("INVALID_CONTRACT", "batch columns must have equal lengths")
        import numpy as np
        def value(item):
            return item.item() if isinstance(item, np.generic) else item.tolist() if isinstance(item, np.ndarray) else item
        return [{name: value(column[i]) for name, column in result.items()} for i in range(next(iter(lengths), 0))]
    if hasattr(result, "to_pylist"):
        return [dict(row) for row in result.to_pylist()]
    if hasattr(result, "to_dict"):
        return [dict(row) for row in result.to_dict(orient="records")]
    return [dict(row) for row in result]


def invoke_batch(
    spec: KernelSpec,
    records: tuple[Record, ...],
    *,
    instance: CallableInstance,
    services: RuntimeServices,
) -> Iterator[Record]:
    """K003: map_batches (format/zero-copy) and batch_map (max_batch) semantics."""
    _check_stop(services)
    kind = spec.kind
    cfg = spec.configuration
    rows = [_materialize_row(record, services) for record in records]
    input_bytes = sum(_input_row_bytes(record) for record in records)
    budget = _OutputBudget(spec, services)

    if kind == "MapBatchesOp":
        batch_format = cfg.get("batch_format", "default")
        zero_copy = bool(cfg.get("zero_copy_batch", False))
        batch, extra = _rows_to_batch(rows, batch_format, services)
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
        out_rows = _batch_to_rows(result, batch_format)
    elif kind in ("BatchMapOp", "AddColumnOp"):
        if input_bytes > services.bounds.max_batch_bytes:
            raise _err("BUDGET_EXCEEDED", "batch exceeds max_batch_bytes", bytes=input_bytes)
        if kind == "AddColumnOp":
            batch_format = cfg.get("batch_format", "default")
            batch, extra = _rows_to_batch(rows, batch_format, services)
            if input_bytes + extra > services.bounds.max_batch_bytes:
                raise _err("BUDGET_EXCEEDED", "add_column coexistence exceeds budget")
            values = _batch_values(instance(batch))
            if len(values) != len(rows):
                raise _err("INVALID_CONTRACT", "add_column result length must match batch")
            column = _cfg(spec, "column")
            out_rows = [{**row, column: value} for row, value in zip(rows, values)]
        else:
            result = instance(rows)
            out_rows = [dict(row) for row in result]
    else:
        raise _err("INVALID_CONTRACT", "kind is not a batch kernel", kind=kind)

    occurrence = spec.configuration.get("occurrence_id")
    for index, out_row in enumerate(out_rows):
        _check_stop(services)
        if index < len(records):
            source = records[index]
            record_id = source.record_id
            position = source.source_position
        else:
            source = records[-1]
            record_id = _hash_id([RECORD_DOMAIN, source.record_id, occurrence, index])
            position = source.source_position
        out = _output_record(record_id, position, out_row, source.sequence, services)
        budget.charge(out)
        yield out


def finish_kernel(
    kernel: Kernel, *, port: str, services: RuntimeServices
) -> Iterator[Record]:
    """K004: forward input EOI; the owning kernel flushes only when ports align."""
    _check_stop(services)
    yield from kernel.finish(port)


# Kinds assembled by the single factory. Relational kinds come from KeyedSource
# (join/reduce/groups/exclude_keys) and UnionSource; the rest mirror OPERATOR-MATRIX.
_UNARY_KINDS = frozenset(
    {
        "MapOp", "FlatMapOp", "BoundMapOp", "FilterOp", "LimitOp",
        "SelectColumnsOp", "DropColumnsOp", "RenameColumnsOp", "AddColumnOp",
        "RandomSampleOp",
    }
)
_BATCH_KINDS = frozenset(
    {"MapBatchesOp", "BatchMapOp", "EmbeddingMapOp", "RegisterDocumentsBatchOp", "SaveLanceOp"}
)
_MODEL_KINDS = frozenset(
    {
        "OperatorLLMMapOp", "AsyncMapOp", "AgentMapOp", "DeduplicateOp",
        "EmbeddingMapOp", "VectorSearchOp", "SearchWebOp", "RegisterDocumentsOp",
        "RegisterDocumentsBatchOp", "ReadDocumentsOp", "SaveLanceOp", "StreamPrependOp",
    }
)
_RELATIONAL_KINDS = frozenset({"join", "exclude_keys", "reduce", "groups", "union"})
_ORDERING_KINDS = frozenset(
    {"SortOp", "RandomShuffleOp", "RandomizeBlockOrderOp", "RepartitionOp"}
)
_AGGREGATE_KINDS = frozenset({"aggregate"})
_STREAM_GROUP_KINDS = frozenset({"StreamGroupBatchesOp"})

KERNEL_KINDS = (
    _UNARY_KINDS | _BATCH_KINDS | _MODEL_KINDS | _RELATIONAL_KINDS
    | _ORDERING_KINDS | _AGGREGATE_KINDS | _STREAM_GROUP_KINDS
)


def make_kernel(
    spec: KernelSpec, *, services: RuntimeServices, partition_id: str
) -> Kernel:
    """K001: the single registry-backed factory; unknown kinds fail before start."""
    kind = spec.kind
    if kind not in KERNEL_KINDS:
        raise _err("INVALID_CONTRACT", "unknown kernel kind", kind=kind)
    if kind not in _AGGREGATE_KINDS and spec.configuration.get("cache_namespace") is not None:
        from demiflow.execution.dataflow_models import bind_cached_map

        return bind_cached_map(spec, services=services)
    from .operator import OperatorKernel

    return OperatorKernel(spec, services=services, partition_id=partition_id)
