"""Direct and distributed Lance create-or-append execution."""
from __future__ import annotations

import uuid
from collections.abc import Iterable, Iterator
from datetime import timedelta

import pyarrow as pa

from demiflow._compat.error_transport import error_from_exception, make_error

from ..errors import InvalidLanceRequest, LanceWriteConflict
from .fragment import decode_lance_fragments, encode_lance_fragment
from .model import (
    LanceWriteReceipt, LanceWriteSpec, _LanceFragmentReceipt,
    _PreparedLanceAppend,
)
from .storage import (
    inspect_lance, lance_commit_conflict_error, open_lance_dataset,
    require_compatible_schema, require_lance, schema_hash,
    storage_fingerprint,
)

_COMMIT_TIMEOUT_SECONDS = 1800


def append_lance(
    spec: LanceWriteSpec,
    batches: Iterable[pa.RecordBatch | pa.Table],
) -> LanceWriteReceipt:
    if spec.expected_version is None:
        return _append_or_create(spec, batches)
    prepared = prepare_lance_append(spec)
    if prepared is None:
        raise AssertionError("expected-version Lance append was not prepared")
    fragment = write_lance_fragment(prepared, task_index=0, batches=batches)
    return commit_lance_append(prepared, (fragment,))


def prepare_lance_append(
    spec: LanceWriteSpec,
) -> _PreparedLanceAppend | None:
    if spec.expected_version is None:
        return None
    inspection = inspect_lance(spec.uri, None, spec.storage_options)
    if inspection.resolved_version != spec.expected_version:
        raise LanceWriteConflict(
            f"Lance append expected version {spec.expected_version}, "
            f"current is {inspection.resolved_version}"
        )
    return _PreparedLanceAppend(
        operation_id=str(uuid.uuid4()), request_hash=spec.content_hash,
        uri=spec.uri, expected_version=spec.expected_version,
        schema=inspection.schema, schema_hash=schema_hash(inspection.schema),
        storage_fingerprint=storage_fingerprint(spec.uri, spec.storage_options),
        storage_options=spec.storage_options,
    )


def write_lance_fragment(
    prepared: _PreparedLanceAppend, *, task_index: int,
    batches: Iterable[pa.RecordBatch | pa.Table],
) -> _LanceFragmentReceipt:
    _validate_prepared(prepared)
    dataset = open_lance_dataset(
        prepared.uri, prepared.expected_version, prepared.storage_options,
    )
    require_compatible_schema(dataset.schema, prepared.schema)
    iterator = iter(_normalize_record_batches(batches))
    first = next((batch for batch in iterator if batch.num_rows), None)
    if first is None:
        raise InvalidLanceRequest("Lance append fragment requires at least one row")
    rows = 0

    def validated() -> Iterator[pa.RecordBatch]:
        nonlocal rows
        for batch in _prepend(first, iterator):
            require_compatible_schema(batch.schema, prepared.schema)
            if batch.num_rows:
                rows += batch.num_rows
            yield batch

    reader = pa.RecordBatchReader.from_batches(prepared.schema, validated())
    metadata = require_lance().fragment.LanceFragment.create(
        prepared.uri, reader, schema=prepared.schema, mode="append",
        storage_options=dict(prepared.storage_options) or None,
    )
    if rows <= 0 or metadata.physical_rows != rows:
        raise InvalidLanceRequest("Lance fragment row count is invalid")
    return encode_lance_fragment(metadata, prepared, task_index)


def commit_lance_append(
    prepared: _PreparedLanceAppend,
    receipts: tuple[_LanceFragmentReceipt, ...],
) -> LanceWriteReceipt:
    _validate_prepared(prepared)
    fragments, rows, expected_paths = decode_lance_fragments(prepared, receipts)
    current = open_lance_dataset(prepared.uri, None, prepared.storage_options)
    if current.version != prepared.expected_version:
        raise LanceWriteConflict(
            f"Lance append expected version {prepared.expected_version}, "
            f"current is {current.version}"
        )
    lance = require_lance()
    CommitConflictError = lance_commit_conflict_error()
    try:
        committed = lance.LanceDataset.commit(
            prepared.uri, lance.LanceOperation.Append(fragments),
            read_version=prepared.expected_version, commit_lock=None,
            storage_options=dict(prepared.storage_options) or None,
            max_retries=0,
            commit_message=f"demiflow-lance:{prepared.operation_id}",
            commit_timeout=timedelta(seconds=_COMMIT_TIMEOUT_SECONDS),
        )
    except CommitConflictError:
        raise
    except Exception as exc:
        return _receipt(
            prepared, rows, None, status="indeterminate",
            error=error_from_exception(exc),
        )
    committed_version = getattr(committed, "version", None)
    if committed_version != prepared.expected_version + 1:
        return _receipt(
            prepared, rows,
            committed_version if isinstance(committed_version, int) else None,
            status="indeterminate",
            error=make_error(
                module=__name__, type_name="CommitVersionUnexpected",
                message="Lance append committed an unexpected version",
            ),
        )
    try:
        reopened = open_lance_dataset(
            prepared.uri, committed_version, prepared.storage_options,
        )
        transaction = reopened.read_transaction(reopened.version)
        marker = (transaction.transaction_properties or {}).get(
            "__lance_commit_message"
        )
        actual_paths = {
            file.path for fragment in reopened.get_fragments()
            for file in fragment.metadata.files
        }
        if (
            marker != f"demiflow-lance:{prepared.operation_id}"
            or not expected_paths.issubset(actual_paths)
        ):
            return _receipt(
                prepared, rows, committed_version, status="indeterminate",
                error=make_error(
                    module=__name__, type_name="CommitVerificationFailed",
                    message=(
                        "Lance append transaction marker or fragments "
                        "could not be verified"
                    ),
                ),
            )
    except Exception as exc:
        return _receipt(
            prepared, rows, committed_version, status="indeterminate",
            error=error_from_exception(exc),
        )
    return _receipt(prepared, rows, committed_version, status="committed")


def _append_or_create(
    spec: LanceWriteSpec,
    batches: Iterable[pa.RecordBatch | pa.Table],
) -> LanceWriteReceipt:
    iterator = iter(_normalize_tables(batches))
    first = next((table for table in iterator if table.num_rows), None)
    if first is None:
        raise InvalidLanceRequest("Lance write requires at least one row")
    input_schema = first.schema
    rows = 0

    def validated() -> Iterator[pa.RecordBatch]:
        nonlocal rows
        for table in _prepend(first, iterator):
            require_compatible_schema(table.schema, input_schema)
            for batch in table.to_batches():
                if batch.num_rows:
                    rows += batch.num_rows
                yield batch

    reader = pa.RecordBatchReader.from_batches(input_schema, validated())
    try:
        committed = require_lance().write_dataset(
            reader, spec.uri, mode="append", commit_lock=None,
            storage_options=dict(spec.storage_options) or None,
            commit_message=f"demiflow-lance:{spec.content_hash}",
        )
    except Exception as exc:
        return LanceWriteReceipt(
            request_hash=spec.content_hash, uri=spec.uri,
            expected_version=None, committed_version=None,
            input_rows=rows, written_rows=None,
            schema_hash=schema_hash(input_schema), status="indeterminate",
            error=error_from_exception(exc),
        )
    version = getattr(committed, "version", None)
    if isinstance(version, bool) or not isinstance(version, int) or version <= 0:
        return LanceWriteReceipt(
            request_hash=spec.content_hash, uri=spec.uri,
            expected_version=None, committed_version=None,
            input_rows=rows, written_rows=None,
            schema_hash=schema_hash(input_schema), status="indeterminate",
            error=make_error(
                module=__name__, type_name="CommitVersionInvalid",
                message="Lance append returned an invalid committed version",
            ),
        )
    return LanceWriteReceipt(
        request_hash=spec.content_hash, uri=spec.uri,
        expected_version=None, committed_version=version,
        input_rows=rows, written_rows=rows,
        schema_hash=schema_hash(input_schema), status="committed",
    )


def _receipt(
    prepared: _PreparedLanceAppend, rows: int, committed_version: int | None,
    *, status: str, error=None,
) -> LanceWriteReceipt:
    return LanceWriteReceipt(
        request_hash=prepared.request_hash, uri=prepared.uri,
        expected_version=prepared.expected_version,
        committed_version=committed_version, input_rows=rows,
        written_rows=rows if status == "committed" else None,
        schema_hash=prepared.schema_hash, status=status, error=error,
    )


def _validate_prepared(prepared: _PreparedLanceAppend) -> None:
    if schema_hash(prepared.schema) != prepared.schema_hash:
        raise InvalidLanceRequest("prepared Lance append schema hash differs")
    if storage_fingerprint(
        prepared.uri, prepared.storage_options,
    ) != prepared.storage_fingerprint:
        raise InvalidLanceRequest("Lance storage changed after append preparation")


def _normalize_record_batches(
    batches: Iterable[pa.RecordBatch | pa.Table],
) -> Iterator[pa.RecordBatch]:
    for block in batches:
        if isinstance(block, pa.RecordBatch):
            yield block
        elif isinstance(block, pa.Table):
            yield from block.to_batches()
        else:
            raise InvalidLanceRequest(
                "Lance write input must contain RecordBatch or Table values"
            )


def _normalize_tables(
    batches: Iterable[pa.RecordBatch | pa.Table],
) -> Iterator[pa.Table]:
    for block in batches:
        if isinstance(block, pa.Table):
            yield block
        elif isinstance(block, pa.RecordBatch):
            yield pa.Table.from_batches([block])
        else:
            raise InvalidLanceRequest(
                "Lance write input must contain RecordBatch or Table values"
            )


def _prepend(first, iterator):
    yield first
    yield from iterator


__all__ = [
    "append_lance", "commit_lance_append", "prepare_lance_append",
    "write_lance_fragment",
]
