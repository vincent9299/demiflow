"""通用 Lance 追加/覆盖提交；只处理 Arrow 数据、版本和写入回执。"""
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


def write_lance(
    spec: LanceWriteSpec,
    batches: Iterable[pa.RecordBatch | pa.Table],
) -> LanceWriteReceipt:
    """执行一次表写入；覆盖始终一次提交，不逐批清空目标。"""
    if spec.mode == 'merge':
        from .mutate import merge_lance
        return merge_lance(spec, batches)
    if spec.schema is not None:
        batches = (table.cast(spec.schema) for table in _normalize_tables(batches))
    if spec.expected_version is None or spec.mode == "overwrite":
        return _write_direct(spec, batches)
    prepared = prepare_lance_append(spec)
    if prepared is None:
        raise AssertionError("expected-version Lance append was not prepared")
    fragment = write_lance_fragment(prepared, task_index=0, batches=batches)
    return commit_lance_append(prepared, (fragment,))


def prepare_lance_append(
    spec: LanceWriteSpec,
) -> _PreparedLanceAppend | None:
    if spec.mode != "append":
        raise InvalidLanceRequest("append preparation requires mode=append")
    if spec.expected_version is None:
        return None
    inspection = inspect_lance(spec.uri, None, spec.storage_options)
    if inspection.resolved_version != spec.expected_version:
        raise LanceWriteConflict(
            f"Lance append expected version {spec.expected_version}, "
            f"current is {inspection.resolved_version}"
        )
    if spec.schema is not None:
        require_compatible_schema(spec.schema, inspection.schema)
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
        err = verify_committed_append(
            prepared, committed_version, expected_paths,
        )
    except Exception as exc:
        return _receipt(
            prepared, rows, committed_version, status="indeterminate",
            error=error_from_exception(exc),
        )
    if err is not None:
        return _receipt(
            prepared, rows, committed_version, status="indeterminate",
            error=err,
        )
    return _receipt(prepared, rows, committed_version, status="committed")


def verify_committed_append(
    prepared: _PreparedLanceAppend, committed_version: int,
    expected_paths: frozenset[str] | set[str],
):
    """Verify a stored commit; return an error payload or None when verified.

    Confirms the transaction marker of ``prepared`` at ``committed_version``
    and that every expected fragment data file is part of that version.
    """
    reopened = open_lance_dataset(
        prepared.uri, committed_version, prepared.storage_options,
    )
    transaction = reopened.read_transaction(committed_version)
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
        return make_error(
            module=__name__, type_name="CommitVerificationFailed",
            message=(
                "Lance append transaction marker or fragments "
                "could not be verified"
            ),
        )
    return None


def _write_direct(
    spec: LanceWriteSpec,
    batches: Iterable[pa.RecordBatch | pa.Table],
) -> LanceWriteReceipt:
    # 覆盖的版本检查先于消费输入；提交时还会由 Lance 原子检查同一版本。
    if spec.expected_version is not None:
        current = inspect_lance(spec.uri, None, spec.storage_options)
        if current.resolved_version != spec.expected_version:
            raise LanceWriteConflict(
                f"Lance overwrite expected version {spec.expected_version}, "
                f"current is {current.resolved_version}"
            )
    iterator = iter(_normalize_tables(batches))
    first = next((table for table in iterator if table.num_rows), None)
    if first is None:
        if spec.schema is None:
            raise InvalidLanceRequest("Lance write requires at least one row or an explicit schema")
        first = pa.Table.from_batches([], schema=spec.schema)
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
    lance = require_lance()
    CommitConflictError = lance_commit_conflict_error()
    try:
        if spec.expected_version is None:
            committed = lance.write_dataset(
                reader, spec.uri, mode=spec.mode, commit_lock=None,
                storage_options=dict(spec.storage_options) or None,
                commit_message=f"demiflow-lance:{spec.content_hash}",
            )
        else:
            # 先写未提交的数据文件，再以 expected_version 原子替换快照；
            # 不使用“检查后普通覆盖”，避免覆盖期间的其他提交被悄悄吞掉。
            fragments = lance.fragment.write_fragments(
                reader, spec.uri, mode="overwrite",
                storage_options=dict(spec.storage_options) or None,
            )
            committed = lance.LanceDataset.commit(
                spec.uri, lance.LanceOperation.Overwrite(input_schema, fragments),
                read_version=spec.expected_version, max_retries=0,
                storage_options=dict(spec.storage_options) or None,
                commit_message=f"demiflow-lance:{spec.content_hash}",
                commit_timeout=timedelta(seconds=_COMMIT_TIMEOUT_SECONDS),
            )
    except CommitConflictError as exc:
        raise LanceWriteConflict(str(exc)) from exc
    except Exception as exc:
        return LanceWriteReceipt(
            request_hash=spec.content_hash, uri=spec.uri,
            expected_version=spec.expected_version, committed_version=None,
            input_rows=rows, written_rows=None,
            schema_hash=schema_hash(input_schema), status="indeterminate",
            error=error_from_exception(exc),
        )
    version = getattr(committed, "version", None)
    if (isinstance(version, bool) or not isinstance(version, int) or version <= 0
            or (spec.expected_version is not None and version != spec.expected_version + 1)):
        return LanceWriteReceipt(
            request_hash=spec.content_hash, uri=spec.uri,
            expected_version=spec.expected_version, committed_version=None,
            input_rows=rows, written_rows=None,
            schema_hash=schema_hash(input_schema), status="indeterminate",
            error=make_error(
                module=__name__, type_name="CommitVersionInvalid",
                message="Lance write returned an invalid committed version",
            ),
        )
    return LanceWriteReceipt(
        request_hash=spec.content_hash, uri=spec.uri,
        expected_version=spec.expected_version, committed_version=version,
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
    "write_lance", "commit_lance_append", "prepare_lance_append",
    "verify_committed_append", "write_lance_fragment",
]
