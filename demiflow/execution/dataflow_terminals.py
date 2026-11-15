"""Terminal adapters over one already-open execution (lane T, C11).

Every terminal (take/iter/count/write/inspect) consults the same execution entry
point opened by ``open_terminal``; none starts a second action or re-runs a
source/model. Bounded collection and batching admit against the driver budgets
and fail (never truncate) when a budget is exceeded.

Cross-lane runtime edges (frozen vocabulary, producer implemented by the owning lane):

* T->G ``demiflow.planning.dataflow_registry.terminal_registry()`` (G009) maps each
  registered terminal kind to its ``TerminalDefinition``. ``open_terminal`` rejects
  an unregistered kind with INVALID_CONTRACT; there is no fallback for unknown kinds.
* T->E the handle binds a ``DataflowExecution`` (E001) and reads its terminal output
  subscription; ``materialized_handle`` pins through ``execution.services.store``.
* T->D materialized results reuse the sealed manifest handled by
  ``StoredMaterializedHandle``; write results reuse the existing ``WriteResult``.
"""
from __future__ import annotations

import pickle
import uuid
from typing import TYPE_CHECKING, Iterator

from demiflow.data.runtime_contract import check_size
from demiflow.execution.dataflow_handles import (
    RuntimeActionHandle,
    StoredMaterializedHandle,
    _error,
)

if TYPE_CHECKING:  # runtime-type imports stay deferred (C04); no import cycle
    from demiflow.execution.contracts.dataflow import (
        ActionHandle,
        ActionRequest,
        Bounds,
        MaterializedHandle,
        Record,
        Row,
        TerminalResult,
        WriteResult,
    )
    from demiflow.execution.dataflow import DataflowExecution

_BATCH_FORMATS = ("default", "numpy", "pandas", "pyarrow", "list")
_INSPECT_KINDS = ("schema", "columns", "size_bytes", "stats", "collect_summary")
_DEFAULT_BATCH_SIZE = 256


def open_terminal(request: "ActionRequest", *, execution: "DataflowExecution") -> "ActionHandle":
    """Bind one execution's terminal output; the registry gates the kind (no fallback)."""
    from demiflow.planning.dataflow_registry import terminal_registry

    registry = terminal_registry()
    if request.kind not in registry:
        raise _error("INVALID_CONTRACT", "unknown terminal kind", context={"kind": request.kind})
    return RuntimeActionHandle(execution, bounds=execution.services.bounds)


def _record_row(record: "Record", handle: "ActionHandle") -> "Row":
    if record.inline is not None:
        from .contracts.dataflow import thaw_record_value
        return thaw_record_value(record.inline)
    if record.payload_ref is not None:
        ref = record.payload_ref
        chunks = handle.services.store.read(ref, max_bytes=ref.size_bytes)
        from .exchange.dataflow_bridge import _decode_payload
        return _decode_payload(b"".join(chunks), max_bytes=handle.bounds.max_row_bytes)
    raise _error("INVALID_CONTRACT", "record has neither inline nor payload_ref")


def collect_rows(handle: "ActionHandle", *, limit: "int | None", bounds: "Bounds", schema_receiver=None) -> "list[Row]":
    """Bounded driver collection. limit=0 reads nothing; exceeding a budget fails
    outright (never a truncated success). Reaching the limit closes the subscription
    and keeps already-committed effect facts."""
    if limit is not None:
        if isinstance(limit, bool) or not isinstance(limit, int) or limit < 0:
            raise _error("INVALID_CONTRACT", "limit must be a non-negative integer or None")
        if limit > bounds.max_output_rows:
            raise _error("BUDGET_EXCEEDED", "take limit exceeds the output row budget")
    if limit == 0:
        handle.result()
        handle.close()
        return []
    rows: list = []
    total_bytes = 0
    records = handle.rows()
    try:
        for record in records:
            if schema_receiver is not None:
                schema_receiver(record.schema_ref)
            rows.append(_record_row(record, handle))
            total_bytes += record.encoded_bytes
            if len(rows) > bounds.max_output_rows:
                raise _error("BUDGET_EXCEEDED", "collected rows exceed the output row budget")
            if total_bytes > bounds.max_resident_bytes:
                raise _error("BUDGET_EXCEEDED", "collected bytes exceed the driver resident budget")
            if limit is not None and len(rows) >= limit:
                if hasattr(handle._execution, "satisfy_terminal"):
                    handle._execution.satisfy_terminal()
                    handle.result()
                break
    except BaseException:
        handle._cleanup()  # cancel producers without masking the active failure
        raise
    records.close()
    if not rows and schema_receiver is not None:
        from .dataflow_schema import result_schema_ref
        ref = result_schema_ref(handle.result(), services=handle.services)
        if ref is not None:
            schema_receiver(ref)
    handle.close()
    return rows


def iterate_rows(handle: "ActionHandle") -> "Iterator[Row]":
    """Hold the action context until iteration completes or closes; read DataRefs on
    demand. An explicit close cancels the necessary producers without waiting for GC."""
    def gen():
        records = handle.rows()
        try:
            for record in records:
                yield _record_row(record, handle)
        except GeneratorExit:
            # records.close() commits the bounded delivered prefix. Do not
            # cancel the execution before that native drain can run.
            raise
        except BaseException:
            handle._cleanup()
            raise
        else:
            handle.close()
        finally:
            records.close()

    return gen()


def _convert_batch(rows: "list[Row]", batch_format: str, handle: "ActionHandle", *, schema=None):
    bounds = handle.bounds
    total = 0
    for row in rows:
        total += check_size(row, bounds.max_row_bytes)
    if total > bounds.max_batch_bytes:
        raise _error("BUDGET_EXCEEDED", "batch conversion exceeds the batch byte budget")
    if batch_format == "pyarrow":
        import pyarrow as pa

        result = pa.Table.from_pylist(rows, schema=schema)
        if total + result.nbytes > bounds.max_batch_bytes:
            raise _error('BUDGET_EXCEEDED', 'Arrow batch conversion coexistence exceeds byte budget')
        return result
    if batch_format == "pandas":
        import pandas as pd

        if schema is None:
            result = pd.DataFrame(rows)
        else:
            import pyarrow as pa
            result = pa.Table.from_pylist(rows, schema=schema).to_pandas()
        if total + int(result.memory_usage(deep=True).sum()) > bounds.max_batch_bytes:
            raise _error('BUDGET_EXCEEDED', 'Pandas batch conversion coexistence exceeds byte budget')
        return result
    if batch_format in ("default", "numpy"):
        import numpy as np

        columns = list(rows[0].keys()) if rows else []
        return {name: np.asarray([row[name] for row in rows]) for name in columns}
    if batch_format == "list":
        return rows
    raise _error("INVALID_CONTRACT", "unsupported batch format", context={"batch_format": batch_format})


def iterate_batches(
    handle: "ActionHandle", *, batch_size: "int | None", batch_format: str, drop_last: bool
) -> "Iterator[object]":
    """Preserve the original batch format and tail-batch convention. The conversion
    peak is admitted against the batch byte budget; an unmet budget is rejected, not
    silently copied or truncated."""
    if batch_format not in _BATCH_FORMATS:
        raise _error("INVALID_CONTRACT", "unsupported batch format", context={"batch_format": batch_format})
    if batch_size is not None and (
        isinstance(batch_size, bool) or not isinstance(batch_size, int) or batch_size <= 0
    ):
        raise _error("INVALID_CONTRACT", "batch_size must be a positive integer or None")
    size = batch_size if batch_size is not None else _DEFAULT_BATCH_SIZE

    def gen():
        from .record_schema import RecordSchemas
        schemas = RecordSchemas(handle.services)
        rows_iter = handle.rows()
        batch: list = []
        schema_ref = None
        try:
            for record in rows_iter:
                current_ref = getattr(record, 'schema_ref', None)
                if batch and current_ref != schema_ref:
                    if not (drop_last and len(batch) < size):
                        yield _convert_batch(batch, batch_format, handle, schema=schemas.read(schema_ref))
                    batch = []
                schema_ref = current_ref
                batch.append(_record_row(record, handle))
                if len(batch) >= size:
                    yield _convert_batch(batch, batch_format, handle, schema=schemas.read(schema_ref))
                    batch = []
            if batch and not (drop_last and len(batch) < size):
                yield _convert_batch(batch, batch_format, handle, schema=schemas.read(schema_ref))
        except GeneratorExit:
            raise
        except BaseException:
            handle._cleanup()
            raise
        else:
            handle.close()
        finally:
            rows_iter.close()

    return gen()


def materialized_handle(result: "TerminalResult", *, execution: "DataflowExecution") -> "MaterializedHandle":
    """Wrap a sealed result as a re-readable handle; each handle owns an independent
    pin so release never drops another owner's reference, and reads never re-run the
    source/model."""
    owner = execution.services.action.action_id + "-" + uuid.uuid4().hex
    return StoredMaterializedHandle(result, store=execution.services.store, owner=owner,
                                    bounds=execution.services.bounds)


def write_result(result: "TerminalResult", *, kind: str) -> "WriteResult":
    """Convert a confirmed sink result to the legacy return shape. An unconfirmed or
    failed commit is never a success just because written_rows is non-zero."""
    from demiflow.data.datasink import validate_write_result

    if result.kind != kind:
        raise _error("INVALID_CONTRACT", "terminal kind mismatch", context={"expected": kind, "actual": result.kind})
    if result.write_result is None:
        raise _error("INVALID_CONTRACT", "terminal result has no write result")
    if not result.complete:
        if result.completion == "cancelled":
            raise _error("CANCELLED", "write cancelled before commit")
        raise _error("UNKNOWN_EFFECT", "write result is not confirmed complete", retryable=False)
    checked = validate_write_result(result.write_result)
    if kind == "write_lance":
        from ..lance.model import LanceWriteReceipt
        return LanceWriteReceipt.from_dict(checked.metadata["native_receipt"]["data"])
    return checked


def _columns_from_schema(schema):
    if isinstance(schema, dict):
        if "names" in schema:
            return list(schema["names"])
        if "columns" in schema:
            return _columns_from_schema(schema["columns"])
        raise _error("INVALID_CONTRACT", "schema has no column names")
    if isinstance(schema, list):
        return [item["name"] if isinstance(item, dict) else item for item in schema]
    raise _error("INVALID_CONTRACT", "unsupported schema representation")


def inspect_result(handle: "ActionHandle", *, kind: str) -> object:
    """Consume only the real metadata or bounded execution result for the requested
    kind. Unknown schema follows fetch_if_missing (applied by the compiler) and is
    never guessed as an empty table; unexecuted stats are explicit, not silent."""
    if kind not in _INSPECT_KINDS:
        raise _error("INVALID_CONTRACT", "unsupported inspect kind", context={"kind": kind})
    result = handle.result()
    from .contracts.dataflow import thaw_record_value
    scalar = thaw_record_value(result.scalar)
    if kind in ("schema", "columns"):
        if scalar is None:
            # Unknown schema: fetch_if_missing was applied when the action compiled.
            # Do not probe here or guess an empty schema/table as success.
            return None
        if kind == "columns":
            return _columns_from_schema(scalar)
        if isinstance(scalar, dict) and 'arrow_schema' in scalar:
            import base64
            import pyarrow as pa
            return pa.ipc.read_schema(pa.BufferReader(base64.b64decode(scalar['arrow_schema'])))
        return scalar
    if kind == "size_bytes":
        return scalar
    if kind == "stats":
        if scalar is None:
            raise _error("INVALID_CONTRACT", "stats were not executed for this action")
        return scalar
    return scalar
