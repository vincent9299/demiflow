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

from .contracts.dataflow_errors import DataflowError
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
    from demiflow.execution.action_driver.dataflow import DataflowExecution

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
        from .arrow_payload import is_arrow_record, batch_cache
        if is_arrow_record(record):
            return batch_cache(handle.services.store, handle.bounds).row(record)
        ref = record.payload_ref
        chunks = handle.services.store.read(ref, max_bytes=ref.size_bytes)
        from demiflow.execution.contracts.record_codec import _decode_payload
        return _decode_payload(b"".join(chunks), max_bytes=handle.bounds.max_row_bytes)
    raise _error("INVALID_CONTRACT", "record has neither inline nor payload_ref")


def collect_rows(handle: "ActionHandle", *, limit: "int | None", bounds: "Bounds",
                 schema_receiver=None, verify_complete: bool = False) -> "list[Row]":
    """Bounded driver collection. limit=0 reads nothing; exceeding a budget fails
    outright (never a truncated success). Reaching the limit closes the subscription
    and keeps already-committed effect facts. With verify_complete (take_all),
    one extra Record proves overflow without decoding or collecting it. Thus
    an exact-limit result needs no extra slot in the returned collection."""
    if limit is not None:
        if isinstance(limit, bool) or not isinstance(limit, int) or limit < 0:
            raise _error("INVALID_CONTRACT", "limit must be a non-negative integer or None")
        if limit > bounds.max_output_rows:
            raise _error("BUDGET_EXCEEDED", "take limit exceeds the output row budget")
    if limit == 0 and not verify_complete:
        handle.result()
        handle.close()
        return []
    rows: list = []
    total_bytes = 0
    records = handle.rows()
    try:
        for record in records:
            if verify_complete and limit is not None and len(rows) == limit:
                raise ValueError(f"Dataset contains more than the take_all limit of {limit} rows")
            if len(rows) >= bounds.max_output_rows:
                raise _error("BUDGET_EXCEEDED", "collected rows exceed the output row budget")
            next_bytes = total_bytes + record.encoded_bytes
            if next_bytes > bounds.max_resident_bytes:
                raise _error("BUDGET_EXCEEDED", "collected bytes exceed the driver resident budget")
            if schema_receiver is not None:
                schema_receiver(record.schema_ref)
            rows.append(_record_row(record, handle))
            total_bytes = next_bytes
            del record
            if not verify_complete and limit is not None and len(rows) >= limit:
                if hasattr(handle._execution, "satisfy_terminal"):
                    handle._execution.satisfy_terminal()
                    handle.result()
                break
    except BaseException:
        handle._cleanup()  # cancel producers without masking the active failure
        try:
            records.close()
        except Exception:
            pass  # the active collection error remains authoritative
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
        if isinstance(handle, RuntimeActionHandle):
            records = handle.values()
        else:
            records = handle.rows()
        try:
            if isinstance(handle, RuntimeActionHandle):
                yield from records
            else:
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
    from .batch_conversion import admit_batch_conversion
    reserve = getattr(handle, 'conversion_reserve', lambda: 0)()
    total, maximum = admit_batch_conversion(rows, batch_format, schema=schema, bounds=handle.bounds,
                                           reserved_bytes=reserve)
    if batch_format == "pyarrow":
        import pyarrow as pa

        result = pa.Table.from_pylist(rows, schema=schema)
        if total + result.get_total_buffer_size() > maximum:
            raise _error('BUDGET_EXCEEDED', 'Arrow batch conversion coexistence exceeds byte budget')
        return result
    if batch_format == "pandas":
        import pandas as pd

        if schema is None:
            result = pd.DataFrame(rows)
        else:
            import pyarrow as pa
            result = pa.Table.from_pylist(rows, schema=schema).to_pandas()
        if total + int(result.memory_usage(deep=True).sum()) > maximum:
            raise _error('BUDGET_EXCEEDED', 'Pandas batch conversion coexistence exceeds byte budget')
        return result
    if batch_format in ("default", "numpy"):
        import numpy as np

        columns = list(rows[0].keys()) if rows else []
        result = {name: np.asarray([row[name] for row in rows]) for name in columns}
        if total + sum(value.nbytes for value in result.values()) > maximum:
            raise _error('BUDGET_EXCEEDED', 'NumPy batch conversion coexistence exceeds byte budget')
        return result
    if batch_format == "list":
        return rows
    raise _error("INVALID_CONTRACT", "unsupported batch format", context={"batch_format": batch_format})


def iterate_batches(
    handle: "ActionHandle", *, batch_size: "int | None", batch_format: str, drop_last: bool
) -> "Iterator[object]":
    """Preserve the original batch size and tail convention. Admit the accumulation
    window before decoding each record; conversion retains its existing size and
    coexistence checks (these are not a pre-allocation RSS guarantee). An unmet
    byte budget fails instead of silently shrinking the requested batch size."""
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
        batch_bytes = 0
        window_bytes = min(handle.bounds.max_batch_bytes, handle.bounds.max_resident_bytes)
        schema_ref = None
        if hasattr(handle, 'set_read_admission'):
            def admit_read(record_bytes, pending_rows, pending_bytes):
                # Current business batch and the next decoded read window must
                # coexist. The per-batch byte check below keeps its schema/tail
                # semantics; this credit only bounds their simultaneous input.
                if (batch_bytes + pending_bytes + record_bytes + handle.read_buffer_reserve()
                        > handle.bounds.max_resident_bytes):
                    raise _error('BUDGET_EXCEEDED', 'batch and prefetched input exceed driver resident budget')
            handle.set_read_admission(admit_read, read_count=lambda: size - len(batch))
        try:
            for record in rows_iter:
                current_ref = getattr(record, 'schema_ref', None)
                if batch and current_ref != schema_ref:
                    if not (drop_last and len(batch) < size):
                        yield _convert_batch(batch, batch_format, handle, schema=schemas.read(schema_ref))
                    batch = []
                    batch_bytes = 0
                schema_ref = current_ref
                next_bytes = batch_bytes + record.encoded_bytes
                if next_bytes > window_bytes:
                    raise _error("BUDGET_EXCEEDED", "batch accumulation exceeds the batch byte budget")
                batch.append(_record_row(record, handle))
                batch_bytes = next_bytes
                del record
                if len(batch) >= size:
                    yield _convert_batch(batch, batch_format, handle, schema=schemas.read(schema_ref))
                    batch = []
                    batch_bytes = 0
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


class TerminalSubscription:
    """One action's ordered result window and replay cursor, sharing transport credit."""

    def __init__(self, bounds, exchange, endpoints, *, store=None):
        from collections import deque
        self._bounds = bounds
        self._exchange = exchange
        self._endpoints = endpoints
        self._store = store
        self._live_arrow = None
        self._queue = deque()
        self._replay = None
        self.closed = False

    def restore(self, publication, *, store):
        from .dataflow_replay import PublishedRecordReader
        self._replay = PublishedRecordReader(publication, store=store, bounds=self._bounds)

    def receive(self, payload, producer):
        if not self.closed and len(self._queue) >= self._bounds.max_pending_events:
            raise DataflowError("BUDGET_EXCEEDED", "terminal subscription credit exceeded")
        key = payload['delivery_id']
        self._exchange.terminal(key, producer)
        if self.closed:
            self._exchange.acknowledge(key, None)
        else:
            self._queue.append(payload)

    def read(self, *, max_records, deadline_ms):
        return self.read_admitted(max_records=max_records, deadline_ms=deadline_ms)

    @property
    def buffered_bytes(self):
        replay = self._replay
        retained = replay.buffered_bytes if replay is not None else 0
        if self._live_arrow is not None:
            retained += self._live_arrow.bytes
        return max(retained, getattr(self, '_working_bytes', 0))

    def read_admitted(self, *, max_records, deadline_ms, admit=None, admit_workspace=None, batch_values=False):
        from .exchange.dataflow_transport import read_peer_many, MAX_PEER_BATCH, peer_batch_limit
        from demiflow.execution.contracts.record_codec import decode_document, document_limit
        from demiflow.execution.contracts.record_codec import record_encoded_bytes
        records = []
        byte_window = min(self._bounds.max_batch_bytes, self._bounds.max_resident_bytes)
        admitted = payload_bytes = 0

        def workspace(size):
            self._working_bytes = size
            if payload_bytes + size > self._bounds.max_resident_bytes:
                raise DataflowError('BUDGET_EXCEEDED', 'terminal read workspace exceeds resident budget')
            if admit_workspace is not None:
                admit_workspace(len(records), payload_bytes)

        def decode_admission(required, *, record_bytes, reader_bytes=0):
            workspace(reader_bytes + required)
            if admit is not None:
                # This must observe the updated codec workspace together with
                # the next row, not two independent smaller peak estimates.
                admit(record_bytes, len(records), payload_bytes)

        try:
            while not self.closed and self._replay is not None and len(records) < max_records:
                try:
                    if batch_values:
                        self._replay.peek_unit(check_workspace=workspace)
                        if not records and (arrow := self._replay.pop_batch()) is not None:
                            return (arrow,)
                    raw, record_id, sequence, size = self._replay.peek(check_workspace=workspace)
                except StopIteration:
                    self._replay.close()
                    self._replay = None
                    break
                required = max(len(raw), size)
                if records and admitted + required > byte_window:
                    break
                if required > byte_window:
                    raise DataflowError('BUDGET_EXCEEDED', 'terminal replay row exceeds admitted read window')
                # Existing raw frame, the typed decode/canonical re-encode
                # workspace and the original leased chunk remain simultaneous.
                records.append(decode_document(raw, record_id=record_id,
                    sequence=sequence, bounds=self._bounds, retained=payload_bytes + self._replay.buffered_bytes,
                    check_workspace=lambda required: decode_admission(required,
                        record_bytes=size, reader_bytes=self._replay.buffered_bytes)))
                admitted += required
                payload_bytes += size
                self._replay.pop()
                del raw
            from itertools import islice
            # A historical prefix stays before all newly produced records.
            while not self.closed and self._replay is None and self._queue and len(records) < max_records:
                if 'result_batch' in self._queue[0]:
                    if records:
                        break
                    from .outputs.arrow_rows import PublishedArrowRows
                    from .record_schema import RecordSchemas
                    from .contracts.dataflow import ArtifactRef
                    header = self._queue[0]
                    if self._live_arrow is None:
                        self._live_arrow = PublishedArrowRows(ArtifactRef(**header['result_batch']),
                            rows=header['rows'], first=header['sequence'], schema_ref=ArtifactRef(**header['schema_ref']),
                            schemas=RecordSchemas(store=self._store, bounds=self._bounds))
                    arrow = self._live_arrow
                    if batch_values:
                        records.append(arrow)
                    else:
                        while arrow.index < arrow.batch.num_rows and len(records) < max_records:
                            record = arrow.record()
                            if records and payload_bytes + record.encoded_bytes > byte_window:
                                break
                            payload_bytes += record.encoded_bytes
                            workspace(arrow.bytes)
                            records.append(record)
                            arrow.index += 1
                    if batch_values or arrow.index == arrow.batch.num_rows:
                        self._live_arrow = None
                        self._queue.popleft()
                        self._exchange.acknowledge(header['delivery_id'], None)
                    break
                batch, size, endpoint, limit = [], 0, None, MAX_PEER_BATCH
                for payload in islice(self._queue, min(MAX_PEER_BATCH, max_records - len(records))):
                    if 'result_batch' in payload:
                        break
                    peer = payload['peer']
                    limit = min(limit, peer_batch_limit(peer))
                    if len(batch) >= limit:
                        break
                    owner = (payload.get('producer_partition'), payload.get('edge_id'),
                             payload.get('route_epoch'), peer['host'], peer['port'], peer['token'])
                    if endpoint is not None and owner != endpoint:
                        break
                    if (records or batch) and admitted + size + peer['bytes'] > byte_window:
                        break
                    if peer['bytes'] > byte_window:
                        raise DataflowError('BUDGET_EXCEEDED', 'terminal row exceeds admitted read window')
                    endpoint = owner
                    batch.append(payload)
                    size += peer['bytes']
                if not batch:
                    break
                largest = max(item['peer']['bytes'] for item in batch)
                # read_peer_many retains its prior frame while _read builds a
                # bytearray, recv fragment and returned bytes for the next one.
                receive_bytes = 3 * largest + min(65536, largest) + 256
                workspace(receive_bytes)  # before opening/receiving payload data
                frames = read_peer_many(tuple(item['peer'] for item in batch), bounds=self._bounds)
                try:
                    from .dataflow_hosts import batch_business_commands
                    with batch_business_commands(self._endpoints):
                        for payload in batch:
                            workspace(receive_bytes)
                            raw = next(frames)
                            size = record_encoded_bytes(raw, maximum=self._bounds.max_row_bytes,
                                frame_maximum=document_limit(self._bounds))
                            records.append(decode_document(raw, record_id=payload['record_id'],
                                sequence=payload['sequence'], bounds=self._bounds, retained=payload_bytes,
                                check_workspace=lambda required: decode_admission(required, record_bytes=size)))
                            self._queue.popleft()
                            self._exchange.acknowledge(payload['delivery_id'], None)
                            admitted += payload['peer']['bytes']
                            payload_bytes += size
                            del raw
                finally:
                    frames.close()
            return tuple(records)
        finally:
            self._working_bytes = 0

    def close(self):
        if self.closed:
            return
        self.closed = True
        self._live_arrow = None
        replay, self._replay = self._replay, None
        if replay is not None:
            replay.close()
        while self._queue:
            payload = self._queue.popleft()
            self._exchange.acknowledge(payload["delivery_id"], None)
