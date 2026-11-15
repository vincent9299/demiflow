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
    from demiflow.execution.dataflow import DataflowExecution

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
                self._execution.cancel("terminal handle closed before completion")
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

    def _terminal_failure(self) -> BaseException:
        # finish() surfaces the original business failure for a failed action.
        try:
            self._execution.finish()
        except Exception as exc:
            return exc
        return _error("INTERNAL_PROTOCOL", "execution failed without a business error")

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
                status = self._execution.pump(deadline_ms=min(_CONTROL_POLL_MS, remaining_ms))
                records = self._subscription.read(
                    max_records=_PULL_CREDIT_RECORDS, deadline_ms=0
                )
                for record in records:
                    yield record
                if status.state == "failed":
                    failure = self._terminal_failure()
                    self._failure = failure
                    raise failure
                if status.state in ("succeeded", "cancelled"):
                    while True:
                        tail = self._subscription.read(
                            max_records=_PULL_CREDIT_RECORDS, deadline_ms=0
                        )
                        if not tail:
                            break
                        for record in tail:
                            yield record
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
            else:
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
            status = self._execution.pump(deadline_ms=min(_CONTROL_POLL_MS, remaining_ms))
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

    def readline(self) -> bytes:
        while True:
            index = self._buf.find(b"\n")
            if index >= 0:
                data = bytes(self._buf[: index + 1])
                del self._buf[: index + 1]
                return data
            if not self._pull():
                data = bytes(self._buf)
                self._buf.clear()
                return data


class StoredMaterializedHandle:
    """Re-read a sealed result manifest; never re-runs the source or model.

    Holds the manifest pin, schema and row count (not a full payload cache). release
    drops only this handle's owner pins; other owners and historical references keep
    their data, while derived views of this handle become invalid.
    """

    def __init__(
        self, result: "TerminalResult", *, store: "ArtifactStore", owner: str
    ) -> None:
        if result.result_ref is None:
            raise _error("INVALID_CONTRACT", "materialized result has no stored manifest")
        if not result.complete:
            raise _error("INVALID_CONTRACT", "cannot materialize an incomplete result")
        self._store = store
        self._owner = owner
        self._manifest_ref = result.result_ref
        self._lock = threading.Lock()
        self._released = False
        self._blocks: tuple["ArtifactRef", ...] = ()
        self._rows = 0
        self._schema = None
        pinned: list["ArtifactRef"] = []
        try:
            store.retain(self._manifest_ref, owner=owner)
            pinned.append(self._manifest_ref)
            manifest = self._read_manifest()
            blocks = tuple(_artifact_ref(entry) for entry in manifest["blocks"])
            rows = manifest["rows"]
            if result.rows is not None and result.rows != rows:
                raise _error("CORRUPT_ARTIFACT", "manifest row count differs from result")
            for ref in blocks:
                store.retain(ref, owner=owner)
                pinned.append(ref)
            self._blocks = blocks
            self._rows = rows
            self._schema = manifest.get("columns")
        except Exception:
            for ref in pinned:
                try:
                    store.release(ref, owner=owner)
                except Exception:
                    pass
            raise

    def _read_manifest(self) -> dict:
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

    def num_blocks(self) -> int:
        self._check_open()
        return len(self._blocks)

    def rows(self) -> Iterator["Row"]:
        self._check_open()
        return self._iter_rows()

    def _iter_rows(self) -> Iterator["Row"]:
        count = 0
        for ref in self._blocks:
            stream = _ChunkStream(self._store.read(ref, max_bytes=ref.size_bytes))
            while True:
                try:
                    row = pickle.load(stream)
                except EOFError:
                    break
                count += 1
                if count > self._rows:
                    raise _error("CORRUPT_ARTIFACT", "materialized row count differs")
                yield row
        if count != self._rows:
            raise _error("CORRUPT_ARTIFACT", "materialized row count differs")

    def release(self) -> None:
        with self._lock:
            if self._released:
                return
            self._released = True
        errors: list[BaseException] = []
        for ref in (self._manifest_ref, *self._blocks):
            try:
                self._store.release(ref, owner=self._owner)
            except Exception as exc:
                errors.append(exc)
        if errors:
            raise errors[0]