"""Terminal action and materialized-result handles (lane T, C11).

RuntimeActionHandle adapts one already-open DataflowExecution into the streaming
``ActionHandle`` surface; StoredMaterializedHandle re-reads a sealed result manifest.
Neither starts a second action, re-runs a source/model, or owns a second ledger.

Cross-lane runtime edges (frozen vocabulary, producer implemented by the owning lane):

* T->E ``DataflowExecution`` exposes, besides the frozen ``pump/finish/cancel/close``,
  ``subscribe_terminal() -> TerminalSubscription`` (a credit-limited terminal output
  reader), and the read-only ``request`` / ``services`` it was constructed with.
  ``TerminalSubscription.read`` returns the records currently available (never more
  than ``max_records``); ``close`` writes the unsubscribe fact. ``finish`` raises the
  original business failure when the action failed.
* T->D the materialized manifest referenced by ``TerminalResult.result_ref`` is a
  bounded JSON document ``demiflow.materialized_manifest.v3`` listing block
  ``ArtifactRef``s, the total ``rows`` and optional ``columns``. Each block artifact is
  a pickle(protocol 4) stream of business rows, so tuple/bytes/None/IEEE floats survive
  a round trip unchanged. Pins go through the original RunJournal via the store's
  ``retain``/``release`` (owner scoped); no new ledger is created here.
"""
from __future__ import annotations

from .contracts.dataflow_errors import DataflowError

import json
import pickle
import threading
import time
from typing import TYPE_CHECKING, Iterator, Protocol

if TYPE_CHECKING:  # runtime-type imports stay deferred (C04); no import cycle
    from demiflow.execution.contracts.dataflow import (
        ActionHandle,
        ActionRequest,
        ArtifactRef,
        ArtifactStore,
        Bounds,
        DriverServices,
        MaterializedHandle,
        Record,
        Row,
        TerminalResult,
    )
    from demiflow.execution.action_driver.dataflow import DataflowExecution

# Bounded prefetch / control polling. These cap this handle's own read-ahead and
# each control poll; the subscription's exchange credit bounds total in-flight data.
_PULL_CREDIT_RECORDS = 64
_CONTROL_POLL_MS = 100

MANIFEST_SCHEMA = "demiflow.materialized_manifest.v3"
_RELEASED_MESSAGE = "Materialized Dataset was released"


def _error(code: str, message: str, *, retryable: bool = False, context=None):
    from demiflow.execution.contracts.dataflow_errors import DataflowError

    return DataflowError(code, message, retryable=retryable, context=context or {})


def _artifact_ref(payload) -> "ArtifactRef":
    from demiflow.execution.contracts.dataflow import ArtifactRef

    if not isinstance(payload, dict):
        raise _error("CORRUPT_ARTIFACT", "manifest block reference is not an object")
    return ArtifactRef(**payload)


class TerminalSubscription(Protocol):
    """Credit-limited terminal output reader provided by DataflowExecution (T->E)."""

    def read(self, *, max_records: int, deadline_ms: int) -> tuple["Record", ...]: ...

    def close(self) -> None: ...


class RuntimeActionHandle:
    """Bind one execution's terminal output; the iterator owns it for its whole life.

    Constructing the handle does not mean the action succeeded. Early close cancels
    the producers that are still needed; a finished action only releases its
    subscription. Already-committed effects are never rolled back by close.
    """

    def __init__(self, execution: "DataflowExecution", *, bounds: "Bounds") -> None:
        self._execution = execution
        self._bounds = bounds
        self._subscription = execution.subscribe_terminal()
        self._lock = threading.Lock()
        self._closed = False
        self._close_called = False
        self._finished = False
        self._subscription_released = False
        self._rows_started = False
        self._result: "TerminalResult | None" = None
        self._failure: BaseException | None = None
        self._cleanup_errors: list[BaseException] = []
        self._yielded_rows = 0
        self._yielded_bytes = 0
        self._read_admission = None
        self._read_count = None

    @property
    def request(self) -> "ActionRequest":
        return self._execution.request

    @property
    def services(self) -> "DriverServices":
        return self._execution.services

    @property
    def bounds(self) -> "Bounds":
        return self._bounds

    # -- internal lifecycle -------------------------------------------------
    def _release_subscription(self) -> None:
        with self._lock:
            if self._subscription_released:
                return
            self._subscription_released = True
        try:
            self._subscription.close()
        except Exception as exc:  # cleanup must not hide the business outcome
            self._cleanup_errors.append(exc)

    def _cleanup(self) -> None:
        with self._lock:
            if self._closed:
                return
            self._closed = True
            finished = self._finished
        if not finished:
            try:
                reason = 'terminal handle closed before completion'
                # Only the original run owner can establish cooperative user
                # cancellation. Early handle close/failure/deadline alone cannot.
                action = getattr(self._execution.request, 'action', None)
                context = getattr(action, 'context', None)
                journal = getattr(context, 'journal', None)
                if journal is not None and journal.execution_status()['status'] in {'stopping', 'cancelled'}:
                    from .request_limits import COOPERATIVE_RUN_STOP_REASON
                    reason = COOPERATIVE_RUN_STOP_REASON
                self._execution.cancel(reason)
            except Exception as exc:
                self._cleanup_errors.append(exc)
        self._release_subscription()
        try:
            self._execution.close()
        except Exception as exc:
            self._cleanup_errors.append(exc)

    def _check_open(self) -> None:
        if self._closed:
            raise _error("CANCELLED", "action handle is closed")

    def _deadline(self) -> float:
        return time.monotonic() + max(0, self._bounds.timeout_ms) / 1000.0

    def _raise_deadline(self) -> None:
        try:
            self._execution.cancel("terminal deadline exceeded")
        except Exception as exc:
            self._cleanup_errors.append(exc)
        raise _error("DEADLINE_EXCEEDED", "terminal action exceeded its time budget")

    def _pump(self, deadline_ms):
        try:
            return self._execution.pump(deadline_ms=deadline_ms)
        except BaseException as exc:
            # pump can surface a worker failure before it returns a status.
            # Preserve that cause when close subsequently reports cleanup errors.
            self._failure = exc
            raise

    def _terminal_failure(self) -> BaseException:
        # finish() surfaces the original business failure for a failed action.
        try:
            self._execution.finish()
        except Exception as exc:
            return exc
        return _error("INTERNAL_PROTOCOL", "execution failed without a business error")

    def set_read_admission(self, admit, *, read_count=None):
        """Internal consumer credit; no persistent reservation or charge."""
        self._read_admission, self._read_count = admit, read_count

    def read_buffer_reserve(self):
        return getattr(self._subscription, 'buffered_bytes', 0)

    def conversion_reserve(self):
        """Live read buffers plus the bounded read window not yet consumed.

        Include the current yield conservatively: a schema boundary converts
        the prior batch while its first following Record is still in the caller.
        A completed collector has already closed this subscription.
        """
        if self._closed:
            return 0
        sizes = getattr(self, '_conversion_read_sizes', ())
        consumed = max(0, self._yielded_rows - getattr(self, '_conversion_read_start', self._yielded_rows))
        retained = sum(sizes[max(0, min(len(sizes), consumed) - 1):])
        return self.read_buffer_reserve() + retained

    def _admit_collection(self, size, pending_rows, pending_bytes):
        kind = getattr(self.request, 'kind', None)
        if kind not in {'take', 'take_all', 'take_batch'}:
            return
        count = self._yielded_rows + pending_rows
        arguments = self.request.arguments
        maximum = (arguments.get('limit') if kind in {'take', 'take_all'}
                   else arguments.get('batch_size', 20))
        if size is not None and kind == 'take_all' and maximum is not None and count >= maximum:
            raise ValueError(f'Dataset contains more than the take_all limit of {maximum} rows')
        if size is not None and count >= self._bounds.max_output_rows:
            raise _error('BUDGET_EXCEEDED', 'collected rows exceed the output row budget')
        if (self._yielded_bytes + pending_bytes + (size or 0) + self.read_buffer_reserve()
                > self._bounds.max_resident_bytes):
            raise _error('BUDGET_EXCEEDED', 'collected bytes exceed the driver resident budget')

    def _read_records(self):
        maximum = _PULL_CREDIT_RECORDS
        kind = getattr(self.request, 'kind', None)
        if kind in {'take', 'take_batch'}:
            limit = (self.request.arguments.get('limit', 20) if kind == 'take'
                     else self.request.arguments.get('batch_size', 20))
            if limit is not None:
                maximum = min(maximum, max(1, limit - self._yielded_rows))
        if self._read_count is not None:
            maximum = min(maximum, max(1, self._read_count()))
        def admit(size, pending_rows, pending_bytes):
            self._admit_collection(size, pending_rows, pending_bytes)
            if self._read_admission is not None:
                self._read_admission(size, pending_rows, pending_bytes)
        def workspace(pending_rows, pending_bytes):
            # No next business row is claimed: EOF/frame assembly must not
            # consume a row slot or turn an exact take_all limit into overflow.
            self._admit_collection(None, pending_rows, pending_bytes)
            if self._read_admission is not None:
                self._read_admission(0, pending_rows, pending_bytes)
        read = getattr(self._subscription, 'read_admitted', None)
        try:
            if read is not None:
                bounded = kind in {'take', 'take_all', 'take_batch'} or self._read_admission is not None
                records = read(max_records=maximum, deadline_ms=0, admit=admit if bounded else None,
                               admit_workspace=workspace if bounded else None)
            else:
                records = self._subscription.read(max_records=maximum, deadline_ms=0)
            self._conversion_read_start = self._yielded_rows
            self._conversion_read_sizes = tuple(record.encoded_bytes for record in records)
            return records
        except BaseException as exc:
            # An admission/decode error is as authoritative as a worker failure;
            # a cancelled peer's late ACK/close error must not replace it.
            self._failure = exc
            raise

    # -- ActionHandle surface ----------------------------------------------
    def rows(self) -> Iterator["Record"]:
        self._check_open()
        with self._lock:
            if self._rows_started:
                raise _error("INVALID_CONTRACT", "terminal output is already being consumed")
            self._rows_started = True
        return self._iter_records()

    def _iter_records(self) -> Iterator["Record"]:
        deadline = self._deadline()
        completed = False
        try:
            while True:
                self._check_open()
                remaining_ms = int((deadline - time.monotonic()) * 1000)
                if remaining_ms <= 0:
                    self._raise_deadline()
                status = self._pump(min(_CONTROL_POLL_MS, remaining_ms))
                from collections import deque
                records = deque(self._read_records())
                while records:
                    self._yielded_rows += 1
                    self._yielded_bytes += records[0].encoded_bytes
                    yield records.popleft()
                del records
                if status.state == "failed":
                    failure = self._terminal_failure()
                    self._failure = failure
                    raise failure
                if status.state in ("succeeded", "cancelled"):
                    while True:
                        tail = deque(self._read_records())
                        if not tail:
                            del tail
                            break
                        while tail:
                            self._yielded_rows += 1
                            self._yielded_bytes += tail[0].encoded_bytes
                            yield tail.popleft()
                        del tail
                    if status.state == "cancelled":
                        failure = _error("CANCELLED", "action cancelled while reading output")
                        self._failure = failure
                        raise failure
                    completed = True
                    break
        finally:
            if completed:
                with self._lock:
                    self._finished = True
                self._release_subscription()
            elif not self._finished:
                # A consumer's normal early close is a bounded terminal demand,
                # not a failed run. Seal the delivered prefix and all already
                # confirmed native effects before releasing worker ownership.
                import sys
                try:
                    if (isinstance(sys.exc_info()[1], GeneratorExit)
                            and self._failure is None and not self._closed):
                        self._execution.satisfy_terminal()
                        # A cut cannot complete while unread terminal envelopes
                        # retain producer credit. Close also retires later real
                        # envelopes through the execution's original ACK path.
                        self._release_subscription()
                        self.result()
                finally:
                    self._cleanup()

    def result(self) -> "TerminalResult":
        self._check_open()
        if self._result is not None:
            return self._result
        deadline = self._deadline()
        while True:
            remaining_ms = int((deadline - time.monotonic()) * 1000)
            if remaining_ms <= 0:
                self._raise_deadline()
            status = self._pump(min(_CONTROL_POLL_MS, remaining_ms))
            if status.state in ("succeeded", "failed", "cancelled"):
                break
        if status.state == "failed":
            failure = self._terminal_failure()
            self._failure = failure
            raise failure
        if status.state == "cancelled":
            with self._lock:
                self._finished = True
            failure = _error("CANCELLED", "action cancelled before a final receipt")
            self._failure = failure
            raise failure
        result = self._execution.finish()
        if not result.complete:
            with self._lock:
                self._finished = True
            if result.completion == "cancelled":
                failure = _error("CANCELLED", "action cancelled before a final receipt")
            else:
                failure = _error(
                    "UNKNOWN_EFFECT", "terminal result is not confirmed complete"
                )
            self._failure = failure
            raise failure
        with self._lock:
            self._finished = True
            self._result = result
        return result

    def close(self) -> None:
        with self._lock:
            if self._close_called:
                return
            self._close_called = True
        self._cleanup()
        if self._failure is not None:
            for error in self._cleanup_errors:
                if error is not self._failure:
                    self._failure.add_note('Action cleanup also failed: ' + str(error))
            raise self._failure
        if self._cleanup_errors:
            raise self._cleanup_errors[0]

    def __del__(self) -> None:
        try:
            self._cleanup()
        except Exception:
            pass


class _ChunkStream:
    """File-like view over a bounded store chunk iterator for incremental unpickling.

    Only the bytes needed for the current row are retained; the whole block/result is
    never materialised before slicing.
    """

    def __init__(self, chunks) -> None:
        self._it = iter(chunks)
        self._buf = bytearray()
        self._eof = False

    def _pull(self) -> bool:
        if self._eof:
            return False
        try:
            chunk = next(self._it)
        except StopIteration:
            self._eof = True
            return False
        self._buf.extend(chunk)
        return True

    def read(self, n: int = -1) -> bytes:
        if n is None or n < 0:
            while self._pull():
                pass
            data = bytes(self._buf)
            self._buf.clear()
            return data
        while len(self._buf) < n and self._pull():
            pass
        data = bytes(self._buf[:n])
        del self._buf[:n]
        return data

    def readline(self, max_bytes: int) -> bytes:
        while True:
            index = self._buf.find(b"\n")
            if index >= 0:
                if index + 1 > max_bytes:
                    raise DataflowError("BUDGET_EXCEEDED", "stored record frame exceeds its bound")
                data = bytes(memoryview(self._buf)[: index + 1])
                del self._buf[: index + 1]
                return data
            if len(self._buf) > max_bytes:
                raise DataflowError("BUDGET_EXCEEDED", "stored record frame exceeds its bound")
            if not self._pull():
                data = bytes(self._buf)
                self._buf.clear()
                return data

    def close(self):
        self._eof = True
        self._buf.clear()
        close = getattr(self._it, 'close', None)
        if close is not None:
            close()


class StoredMaterializedHandle:
    """Re-read a sealed result manifest; never re-runs the source or model.

    Holds the manifest pin, schema and row count (not a full payload cache). release
    drops only this handle's owner pins; other owners and historical references keep
    their data, while derived views of this handle become invalid.
    """

    def __init__(
        self, result: "TerminalResult", *, store: "ArtifactStore", owner: str, bounds: "Bounds"
    ) -> None:
        if result.result_ref is None:
            raise _error("INVALID_CONTRACT", "materialized result has no stored manifest")
        if not result.complete:
            raise _error("INVALID_CONTRACT", "cannot materialize an incomplete result")
        self._store = store
        self._bounds = bounds
        self._owner = owner
        self._manifest_ref = result.result_ref
        self._lock = threading.Lock()
        self._released = False
        self._blocks: tuple["ArtifactRef", ...] = ()
        self._rows = 0
        self._schema = None
        self._declared_schema_ref = None
        self._declared_schema = None
        from .storage.objects import RunArtifactStore
        self._count_proof = {} if type(store) is RunArtifactStore else None
        self._verified_frame_rows = 0
        pinned: list["ArtifactRef"] = []
        blocks, pinned_tree_count = (), 0
        try:
            store.retain(self._manifest_ref, owner=owner)
            pinned.append(self._manifest_ref)
            manifest = self._read_manifest()
            blocks = tuple(_artifact_ref(entry) for entry in manifest["blocks"])
            rows = manifest["rows"]
            if result.rows is not None and result.rows != rows:
                raise _error("CORRUPT_ARTIFACT", "manifest row count differs from result")
            from .dataflow_segments import tree_refs
            for block in blocks:
                for ref in tree_refs(store, block, self._bounds):
                    self._retain_block(ref)
                    pinned_tree_count += 1
            if self._verified_frame_rows != rows:
                self._count_proof = None
            self._blocks = blocks
            self._rows = rows
            self._schema = manifest.get("columns")
            if manifest.get('schema_ref') is not None:
                from .record_schema import RecordSchemas
                self._declared_schema_ref = _artifact_ref(manifest['schema_ref'])
                self._declared_schema = RecordSchemas(store=store, bounds=self._bounds).read(self._declared_schema_ref)
                store.retain(self._declared_schema_ref, owner=owner)
                pinned.append(self._declared_schema_ref)
        except Exception:
            from itertools import islice
            from .dataflow_segments import tree_refs
            try:
                for ref in islice((ref for block in blocks for ref in tree_refs(store, block, self._bounds)), pinned_tree_count):
                    store.release(ref, owner=owner)
            except Exception:
                pass
            for ref in pinned:
                try:
                    store.release(ref, owner=owner)
                except Exception:
                    pass
            raise

    def _retain_block(self, ref):
        from .dataflow_segments import RECORDS_MEDIA, SEGMENTS_MEDIA
        from .dataflow_record_frames import (decode_document, decode_frame, frame_stream,
                                             line_limit, read_chunk_bytes)
        proof = self._count_proof
        budget = min(self._bounds.max_state_bytes, self._bounds.max_resident_bytes // 8)
        if (proof is None or len(proof) >= budget // 1024
                or ref.media_type not in (RECORDS_MEDIA, SEGMENTS_MEDIA)):
            self._count_proof = None
            self._store.retain(ref, owner=self._owner)
            return
        before = self._store.object_signature(ref)
        if ref.media_type == SEGMENTS_MEDIA:
            self._store.retain(ref, owner=self._owner)
        else:
            def observe(chunks):
                stream = frame_stream(chunks, self._bounds)
                try:
                    while line := stream.readline(line_limit(self._bounds)):
                        raw, item = decode_frame(line, self._bounds, retained=stream.buffered_bytes)
                        record = decode_document(raw, record_id=item['record_id'],
                            sequence=item['sequence'], bounds=self._bounds, retained=stream.buffered_bytes)
                        self._verified_frame_rows += 1
                        for dependency in (record.payload_ref, record.schema_ref):
                            if dependency is None or dependency in proof:
                                continue
                            if len(proof) >= budget // 1024:
                                self._count_proof = None
                                return
                            stamp = self._store.object_signature(dependency)
                            if (stamp is None or not self._store.inspect(dependency)
                                    or stamp != self._store.object_signature(dependency)):
                                self._count_proof = None
                                return
                            proof[dependency] = stamp
                except (ValueError, DataflowError):
                    # A historical frame without a reusable proof keeps its original read path.
                    self._count_proof = None
                finally:
                    stream.close()
            self._store.retain_observed(ref, owner=self._owner, observe=observe,
                                        chunk_bytes=read_chunk_bytes(self._bounds))
        if before is None or before != self._store.object_signature(ref):
            self._count_proof = None
        elif self._count_proof is not None:
            proof[ref] = before

    def verified_row_count(self):
        """Only the live pin owner can reuse a checksum established at publication."""
        with self._lock:
            self._check_open()
            if self._count_proof is None:
                return None
            manifest = self._read_manifest()
            if manifest['rows'] != self._rows:
                raise _error("CORRUPT_ARTIFACT", "materialized row count differs from manifest")
            if self._declared_schema_ref is not None and not self._store.inspect(self._declared_schema_ref):
                raise _error("CORRUPT_ARTIFACT", "materialized schema is absent")
            if any(self._store.object_signature(ref) != stamp for ref, stamp in self._count_proof.items()):
                return None
            return self._rows

    def _read_manifest(self) -> dict:
        if self._manifest_ref.size_bytes > self._bounds.max_control_bytes:
            raise _error("BUDGET_EXCEEDED", "materialized manifest exceeds its original control budget")
        chunks = self._store.read(
            self._manifest_ref, max_bytes=self._manifest_ref.size_bytes
        )
        raw = b"".join(chunks)
        if len(raw) > self._manifest_ref.size_bytes:
            raise _error("CORRUPT_ARTIFACT", "manifest exceeds its declared size")
        try:
            manifest = json.loads(raw)
        except (ValueError, UnicodeDecodeError) as exc:
            raise _error("CORRUPT_ARTIFACT", "manifest is not valid JSON") from exc
        if isinstance(manifest, dict) and manifest.get("schema") == "demiflow.dataflow_sink_publication.v3":
            manifest = {"schema": MANIFEST_SCHEMA,
                        "blocks": [p["data_ref"] for p in manifest["manifest"]["partitions"]],
                        "rows": manifest["manifest"]["total_rows"],
                        "columns": manifest["manifest"]["columns"],
                        **({'schema_ref': manifest['manifest']['schema_ref']}
                           if 'schema_ref' in manifest['manifest'] else {})}
        if not isinstance(manifest, dict) or manifest.get("schema") != MANIFEST_SCHEMA:
            raise _error("CORRUPT_ARTIFACT", "unknown materialized manifest schema")
        if not isinstance(manifest.get("blocks"), list):
            raise _error("CORRUPT_ARTIFACT", "manifest blocks must be a list")
        rows = manifest.get("rows")
        if not isinstance(rows, int) or isinstance(rows, bool) or rows < 0:
            raise _error("CORRUPT_ARTIFACT", "manifest row count is invalid")
        return manifest

    def _check_open(self) -> None:
        if self._released:
            raise RuntimeError(_RELEASED_MESSAGE)

    def __getstate__(self):
        # Sealed source programs contain immutable location facts, never the
        # driver's live journal, pin authority or locks.
        self._check_open()
        state = {"root": self._store.root, "store_id": self._store.store_id,
                "bounds": self._bounds, "manifest": self._manifest_ref,
                "blocks": self._blocks, "rows": self._rows, "schema": self._schema}
        if self._declared_schema_ref is not None:
            state['schema_ref'] = self._declared_schema_ref
        return state

    def __setstate__(self, state):
        from .storage.objects import RunArtifactStore
        self._store = RunArtifactStore(root=state["root"], store_id=state["store_id"],
            journal=None, bounds=state["bounds"], readonly=True)
        self._count_proof = None
        self._bounds = state["bounds"]
        self._manifest_ref, self._blocks = state["manifest"], state["blocks"]
        self._rows, self._schema = state["rows"], state["schema"]
        from .record_schema import RecordSchemas
        self._declared_schema_ref = state.get('schema_ref')
        self._declared_schema = RecordSchemas(store=self._store,
            bounds=self._bounds).read(self._declared_schema_ref)
        self._owner, self._released, self._lock = None, False, threading.Lock()

    @property
    def declared_schema(self):
        self._check_open()
        return self._declared_schema

    @property
    def row_count(self):
        self._check_open()
        return self._rows

    @property
    def recovery_reference(self):
        self._check_open()
        from dataclasses import asdict
        return {"schema": "demiflow.materialized-source.v3", "manifest": asdict(self._manifest_ref)}

    def num_blocks(self) -> int:
        self._check_open()
        return len(self._blocks)

    def rows(self) -> Iterator[object]:
        self._check_open()
        return self._iter_rows()

    def typed_rows(self):
        self._check_open()
        return self._iter_rows(with_schema=True)

    def _iter_rows(self, *, with_schema=False) -> Iterator[object]:
        from .record_schema import RecordSchemas
        schemas = RecordSchemas(store=self._store, bounds=self._bounds) if with_schema else None
        count = 0
        from .arrow_payload import ArrowBatchCache, is_arrow_record
        arrow = ArrowBatchCache(self._store, self._bounds)
        from .dataflow_segments import segment_refs
        from .dataflow_record_frames import decode_frame, decode_document, line_limit, frame_stream, read_chunk_bytes
        try:
            for ref in (leaf for block in self._blocks
                        for leaf in segment_refs(self._store, block, self._bounds)):
                stream = (frame_stream(self._store.read(ref, max_bytes=max(1, ref.size_bytes),
                    chunk_bytes=read_chunk_bytes(self._bounds)), self._bounds)
                    if ref.media_type == "application/x-demiflow-records-v3" else
                    _ChunkStream(self._store.read(ref, max_bytes=max(1, ref.size_bytes))))
                try:
                    while True:
                        try:
                            if ref.media_type == "application/x-demiflow-records-v3":
                                line = stream.readline(line_limit(self._bounds))
                                if not line:
                                    break
                                raw, item = decode_frame(line, self._bounds, retained=stream.buffered_bytes)
                                del line
                                from .contracts.dataflow import thaw_record_value
                                record = decode_document(raw, record_id=item["record_id"],
                                    sequence=item["sequence"], bounds=self._bounds, retained=stream.buffered_bytes)
                                del raw
                                if is_arrow_record(record):
                                    row = arrow.row(record)
                                elif record.inline is None:
                                    from .exchange.dataflow_bridge import _decode_payload
                                    ceiling = self._bounds.max_row_bytes
                                    row = _decode_payload(b"".join(self._store.read(record.payload_ref,
                                        max_bytes=ceiling)), max_bytes=ceiling)
                                else:
                                    row = thaw_record_value(record.inline)
                            else:
                                row = pickle.load(stream)
                        except EOFError:
                            break
                        count += 1
                        if count > self._rows:
                            raise _error("CORRUPT_ARTIFACT", "materialized row count differs")
                        if with_schema:
                            schema = (schemas.read(record.schema_ref)
                                      if ref.media_type == "application/x-demiflow-records-v3" else None)
                            yield row, schema
                        else:
                            yield row
                finally:
                    stream.close()
            if count != self._rows:
                raise _error("CORRUPT_ARTIFACT", "materialized row count differs")
        finally:
            arrow.close()

    def release(self) -> None:
        with self._lock:
            if self._released:
                return
            if self._owner is not None:
                release = getattr(self._store, 'release_materialized', self._store.release)
                from .dataflow_segments import tree_refs
                for block in self._blocks:
                    for ref in tree_refs(self._store, block, self._bounds):
                        release(ref, owner=self._owner)
                release(self._manifest_ref, owner=self._owner)
                if self._declared_schema_ref is not None:
                    release(self._declared_schema_ref, owner=self._owner)
            # An unknown/failed unpin remains retryable. Immutable data and
            # committed owners remain intact throughout a partial release.
            self._released = True
