"""Atomic, replayable Lance checkpoints with an enforced explicit schema.

A checkpoint writes one fresh attempt table into an exclusive
``<uri>.attempt-<id>`` directory, verifies the commit, records a pending
receipt, publishes by atomic rename, and finally registers completion in a
receipt in the table’s sibling _demiflow control directory. Replays with the same fingerprint reopen
the committed version at the *caller-resolved* URI without re-executing the
plan; the receipt's historical ``uri`` is provenance only. A different
fingerprint at the same location is an error. An existing table at the
target without a receipt is never deleted: it is either recovered through a
matching pending receipt (interrupted between rename and sidecar), or the
write refuses with an explicit error for manual reconciliation. A failed
attempt only ever removes its own attempt directory. Concurrent writers are
rejected through an advisory file lock, so this module targets local
filesystem URIs only.
"""
from __future__ import annotations

import fcntl
import json
import os
import shutil
import uuid
from collections.abc import Mapping
from pathlib import Path
from .control import control_directory, table_lock_path
from urllib.parse import urlsplit


from ..errors import (
    InvalidLanceRequest, LanceExecutionError, LanceResourceNotFound,
    LanceWriteConflict,
)
from .storage import (
    normalize_lance_uri, normalize_storage_options, open_lance_dataset,
    require_compatible_schema, require_lance, schema_hash,
)

_CHECKPOINT_SCHEMA = "demiflow_lance_checkpoint_v1"
_DEFAULT_MAX_ROWS_PER_BATCH = 8192


def checkpoint_sidecar_path(uri: str) -> str:
    return str(control_directory(normalize_lance_uri(uri)) / 'checkpoint.json')


def read_checkpoint_record(uri: str) -> dict | None:
    """Return the registered checkpoint record, or None when absent."""
    sidecar = Path(checkpoint_sidecar_path(uri))
    if not sidecar.exists():
        return None
    try:
        payload = json.loads(sidecar.read_text())
    except (OSError, json.JSONDecodeError) as exc:
        raise LanceExecutionError(
            "Lance checkpoint sidecar is unreadable; inspect it manually",
        ) from exc
    _validate_record(payload, sidecar)
    return payload


def checkpoint_lance(
    dataset, uri: str, *, schema, fingerprint: str,
    storage_options: Mapping[str, str] | None = None,
    max_rows_per_batch: int = _DEFAULT_MAX_ROWS_PER_BATCH,
):
    """Execute ``dataset`` into ``uri`` and return a version-pinned Dataset.

    ``schema`` is enforced on every row: unknown fields and incompatible
    values fail the attempt, and a zero-row result commits a valid empty
    table. ``fingerprint`` is the caller's logical identity for the attempt;
    replays must repeat it exactly. See the module docstring for the
    completion, retry, and concurrency contract.
    """
    import pyarrow as pa

    if not isinstance(schema, pa.Schema):
        raise InvalidLanceRequest("checkpoint_lance requires an explicit pyarrow.Schema")
    if (
        not isinstance(fingerprint, str) or not fingerprint
        or fingerprint != fingerprint.strip()
    ):
        raise InvalidLanceRequest("checkpoint fingerprint must be a normalized non-empty string")
    if (
        isinstance(max_rows_per_batch, bool)
        or not isinstance(max_rows_per_batch, int)
        or max_rows_per_batch <= 0
    ):
        raise InvalidLanceRequest("max_rows_per_batch must be a positive integer")

    normalized_uri = normalize_lance_uri(uri)
    if urlsplit(normalized_uri).scheme not in {"", "file"}:
        raise InvalidLanceRequest(
            "checkpoint_lance currently supports local filesystem targets only",
        )
    options = normalize_storage_options(storage_options)
    sidecar = Path(checkpoint_sidecar_path(normalized_uri))
    lock_path = table_lock_path(normalized_uri)
    sidecar.parent.mkdir(parents=True, exist_ok=True)

    with lock_path.open("a") as lock:
        try:
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as exc:
            raise LanceWriteConflict(
                "another writer holds this Lance checkpoint",
            ) from exc
        record = read_checkpoint_record(normalized_uri)
        if record is not None:
            _discard_stale_pending(normalized_uri)
            if record["fingerprint"] != fingerprint:
                raise InvalidLanceRequest(
                    "checkpoint fingerprint changed; use a new attempt path",
                )
            _verify_registered_table(normalized_uri, record, options)
            return _pinned_dataset(dataset, normalized_uri, record, options)

        if _target_table_exists(normalized_uri):
            # R1: a table without a receipt is not provably an interrupted
            # attempt of ours. Only a pending receipt that still matches the
            # table's committed state proves an interrupted publish.
            recovered = _recover_pending_publish(normalized_uri, options)
            if recovered is None:
                raise LanceExecutionError(
                    "target holds a Lance table without a checkpoint receipt "
                    "and no pending publish proves ownership; refusing to "
                    f"overwrite: {normalized_uri} (reconcile manually)",
                )
            if recovered["fingerprint"] != fingerprint:
                raise InvalidLanceRequest(
                    "recovered pending publish carries a different "
                    "fingerprint; use a new attempt path",
                )
            _write_sidecar(sidecar, recovered)
            _discard_stale_pending(normalized_uri)
            _verify_registered_table(normalized_uri, recovered, options)
            return _pinned_dataset(dataset, normalized_uri, recovered, options)

        record = _write_attempt(
            dataset, normalized_uri, schema, fingerprint, options,
            max_rows_per_batch, sidecar,
        )
        return _pinned_dataset(dataset, normalized_uri, record, options)


def _iter_dataset_rows(dataset, max_rows_per_batch: int):
    """Yield plan rows for the checkpoint writer, bridging async operators.

    R6: a plan containing AsyncMapOp cannot run through the synchronous
    executor. Mirror the native JSONL checkpoint bridge: execute the prefix
    before the first async op synchronously, then drive the remainder with
    run_stream in a worker thread, pumping rows through a bounded queue.
    """
    from ..data.plan import AsyncMapOp, LogicalPlan
    from ..data.dataset import Dataset
    from ..data.api import DataAPI

    ops = dataset._plan.operations
    first = next(
        (i for i, op in enumerate(ops) if isinstance(op, AsyncMapOp)), None,
    )
    if first is None:
        yield from dataset.iter_rows()
        return

    import asyncio
    import queue as queue_mod
    import threading

    q: queue_mod.Queue = queue_mod.Queue(maxsize=64)
    done_marker = object()
    failure: dict = {}

    async def _pump(row):
        while True:
            try:
                q.put_nowait(row)
                break
            except queue_mod.Full:
                await asyncio.sleep(0.02)
        return row

    def _run():
        try:
            prefix = Dataset(
                dataset._source, LogicalPlan(ops[:first]), dataset._executor,
            )
            source = DataAPI(dataset._executor).from_iter(prefix.iter_rows)
            stream = Dataset(
                source._source, LogicalPlan(ops[first:]), dataset._executor,
                dataset._stages,
            )
            stream.map_async(_pump).run_stream(log_every=0)
            q.put(done_marker)
        except BaseException as exc:  # noqa: BLE001 - transported to consumer
            failure["exc"] = exc
            q.put(done_marker)

    worker = threading.Thread(
        target=_run, name="demiflow-lance-checkpoint-bridge", daemon=True,
    )
    worker.start()
    try:
        while True:
            item = q.get()
            if item is done_marker:
                if "exc" in failure:
                    raise failure["exc"]
                return
            yield item
    finally:
        worker.join(timeout=30.0)


def _write_attempt(
    dataset, uri: str, schema, fingerprint: str, options,
    max_rows_per_batch: int, sidecar: Path,
) -> dict:
    import pyarrow as pa

    names = tuple(schema.names)
    allowed = frozenset(names)
    rows = 0
    validation_error: InvalidLanceRequest | None = None

    def record_batches():
        nonlocal rows, validation_error
        try:
            chunk: list[dict] = []
            for row in _iter_dataset_rows(dataset, max_rows_per_batch):
                if not isinstance(row, Mapping):
                    raise InvalidLanceRequest("checkpoint rows must be mappings")
                unknown = set(row) - allowed
                if unknown:
                    raise InvalidLanceRequest(
                        f"row fields outside the checkpoint schema: {sorted(unknown)!r}",
                    )
                chunk.append({name: row.get(name) for name in names})
                if len(chunk) >= max_rows_per_batch:
                    yield _batch(chunk, schema)
                    rows += len(chunk)
                    chunk = []
            if chunk:
                yield _batch(chunk, schema)
                rows += len(chunk)
        except InvalidLanceRequest as exc:
            # Lance rewraps generator failures as opaque storage errors;
            # keep the original so the caller sees the real contract break.
            validation_error = exc
            raise

    # R1: the attempt writes into an exclusive directory; the published
    # target and any pre-existing table or version history stay untouched
    # until the attempt is fully verified and published by rename.
    attempt_id = uuid.uuid4().hex
    attempt_dir = control_directory(uri) / ('attempt-' + attempt_id + '.lance')
    pending_path = control_directory(uri) / ('pending-' + attempt_id + '.json')
    if attempt_dir.exists():
        shutil.rmtree(attempt_dir, ignore_errors=True)
    reader = pa.RecordBatchReader.from_batches(schema, record_batches())
    try:
        committed = require_lance().write_dataset(
            reader, str(attempt_dir), mode="overwrite", commit_lock=None,
            storage_options=dict(options) or None,
            commit_message=f"demiflow-lance-checkpoint:{fingerprint}",
        )
    except BaseException:
        shutil.rmtree(attempt_dir, ignore_errors=True)
        if validation_error is not None:
            raise validation_error
        raise
    version = getattr(committed, "version", None)
    if isinstance(version, bool) or not isinstance(version, int) or version <= 0:
        shutil.rmtree(attempt_dir, ignore_errors=True)
        raise LanceExecutionError("Lance checkpoint commit returned an invalid version")

    record = None
    try:
        reopened = open_lance_dataset(str(attempt_dir), version, options)
        require_compatible_schema(reopened.schema, schema)
        if reopened.count_rows() != rows:
            raise LanceExecutionError(
                "Lance checkpoint row count differs from the executed plan",
            )
        record = {
            "schema_version": _CHECKPOINT_SCHEMA,
            "fingerprint": fingerprint,
            "uri": uri,
            "committed_version": version,
            "row_count": rows,
            "schema_hash": schema_hash(schema),
        }
        # Pending receipt first: after the rename lands, this is the only
        # proof that the receiptless table at the target is our publish.
        _write_receipt_atomic(pending_path, record)
        try:
            os.rename(attempt_dir, Path(uri))
        except BaseException:
            pending_path.unlink(missing_ok=True)
            raise
    except BaseException:
        shutil.rmtree(attempt_dir, ignore_errors=True)
        pending_path.unlink(missing_ok=True)
        raise
    _write_receipt_atomic(sidecar, record)
    pending_path.unlink(missing_ok=True)
    return record


def _write_receipt_atomic(path: Path, record: Mapping) -> None:
    partial = path.with_name(path.name + "." + uuid.uuid4().hex + ".partial")
    payload = json.dumps(record, sort_keys=True, ensure_ascii=False)
    with partial.open("w") as handle:
        handle.write(payload)
        handle.flush()
        os.fsync(handle.fileno())
    os.replace(partial, path)


def _pending_paths(uri: str):
    return sorted(control_directory(uri).glob('pending-*.json'))


def _discard_stale_pending(uri: str) -> None:
    # Only called with the lock held and a valid sidecar present.
    for pending in _pending_paths(uri):
        pending.unlink(missing_ok=True)


def _target_table_exists(uri: str) -> bool:
    return Path(uri).exists()


def _recover_pending_publish(uri: str, options):
    for pending in _pending_paths(uri):
        try:
            payload = json.loads(pending.read_text())
            _validate_record(payload, pending)
        except (OSError, json.JSONDecodeError, LanceExecutionError):
            continue  # unproven receipt never authorizes anything
        if payload.get("uri") != uri:
            continue
        try:
            dataset = open_lance_dataset(uri, payload["committed_version"], options)
            if (schema_hash(dataset.schema) == payload["schema_hash"]
                    and dataset.count_rows() == payload["row_count"]):
                return payload
        except LanceResourceNotFound:
            continue
    return None


def _batch(chunk: list[dict], schema):
    import pyarrow as pa

    try:
        return pa.RecordBatch.from_pylist(chunk, schema=schema)
    except pa.ArrowInvalid as exc:
        raise InvalidLanceRequest(
            f"checkpoint row does not match the schema: {exc}",
        ) from exc


def _verify_registered_table(uri: str, record: dict, options) -> None:
    try:
        dataset = open_lance_dataset(uri, record["committed_version"], options)
    except LanceResourceNotFound:
        raise
    if schema_hash(dataset.schema) != record["schema_hash"]:
        raise LanceExecutionError(
            "registered Lance checkpoint table no longer matches its schema hash",
        )
    if dataset.count_rows() != record["row_count"]:
        raise LanceExecutionError(
            "registered Lance checkpoint table row count drifted",
        )


def _write_sidecar(sidecar: Path, record: dict) -> None:
    _write_receipt_atomic(sidecar, record)


def _pinned_dataset(dataset, uri: str, record: dict, options):
    from ..data.api import DataAPI

    # R4: the read end binds the caller-resolved URI for this invocation;
    # record["uri"] is historical provenance (e.g. before a data-root move)
    # and must never be reopened directly.
    return DataAPI(dataset._executor).read_lance(
        uri, version=record["committed_version"],
        storage_options=dict(options) or None,
    )


def _validate_record(payload, sidecar: Path) -> None:
    required = {
        "schema_version", "fingerprint", "uri", "committed_version",
        "row_count", "schema_hash",
    }
    if not isinstance(payload, dict) or set(payload) != required:
        raise LanceExecutionError(
            f"Lance checkpoint sidecar has unsupported fields: {sidecar.name}",
        )
    if payload["schema_version"] != _CHECKPOINT_SCHEMA:
        raise LanceExecutionError(
            f"unsupported Lance checkpoint schema: {payload['schema_version']!r}",
        )


__all__ = ["checkpoint_lance", "checkpoint_sidecar_path", "read_checkpoint_record"]
