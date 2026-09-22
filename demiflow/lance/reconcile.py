"""Reconciliation of indeterminate Lance append receipts.

An append that raised after the commit may or may not have landed. These
helpers re-read the stored table and decide from durable evidence only: the
per-operation transaction marker and the committed fragment paths. They never
write, so reconciliation is always safe to repeat.
"""
from __future__ import annotations

import dataclasses
from collections.abc import Mapping, Sequence

from ..errors import LanceExecutionError
from .fragment import decode_lance_fragments
from .model import LanceWriteReceipt, _PreparedLanceAppend
from .storage import open_lance_dataset, normalize_storage_options
from .write import verify_committed_append

_MAX_VERSION_SCAN = 64


def find_committed_append(
    uri: str, *, operation_id: str,
    committed_version: int | None = None,
    storage_options: Mapping[str, str] | Sequence[tuple[str, str]] | None = None,
) -> int | None:
    """Return the version committed by ``operation_id``'s marker, if any.

    Checks ``committed_version`` first when provided, then scans the newest
    versions. A ``None`` result means no stored manifest carries the marker:
    the append did not commit and the attempt is safe to discard.
    """
    dataset = open_lance_dataset(uri, None, normalize_storage_options(storage_options))
    entries = getattr(dataset, "versions", None)
    if callable(entries):  # pylance >= 12 exposes versions() as a method
        entries = entries()
    candidates: list[int] = []
    if committed_version is not None:
        candidates.append(committed_version)
    for entry in list(entries or [])[-_MAX_VERSION_SCAN:][::-1]:
        version = _entry_version(entry)
        if version is not None and version not in candidates:
            candidates.append(version)
    marker = f"demiflow-lance:{operation_id}"
    seen: set[int] = set()
    for version in candidates:
        if version in seen or not isinstance(version, int) or version <= 0:
            continue
        seen.add(version)
        pinned = open_lance_dataset(uri, version, normalize_storage_options(storage_options))
        transaction = pinned.read_transaction(version)
        properties = transaction.transaction_properties or {}
        if properties.get("__lance_commit_message") == marker:
            return version
    return None


def reconcile_lance_append(
    prepared: _PreparedLanceAppend,
    fragment_receipts: Sequence,
    receipt: LanceWriteReceipt,
) -> LanceWriteReceipt:
    """Resolve an indeterminate compare-and-append receipt, if possible.

    A receipt becomes ``committed`` only when both the transaction marker and
    the fragment paths of ``prepared`` verify at a stored version. Anything
    less returns the original indeterminate receipt unchanged; the caller
    keeps it for audit and decides discard or retry.
    """
    if receipt.status == "committed":
        return receipt
    if (
        receipt.request_hash != prepared.request_hash
        or receipt.expected_version != prepared.expected_version
    ):
        # The receipt belongs to a different attempt; do not resolve it
        # against this prepared append's commit evidence.
        return receipt
    version = receipt.committed_version or find_committed_append(
        prepared.uri, operation_id=prepared.operation_id,
        storage_options=prepared.storage_options,
    )
    if version is None:
        return receipt
    _, _, expected_paths = decode_lance_fragments(prepared, tuple(fragment_receipts))
    if verify_committed_append(prepared, version, expected_paths) is not None:
        return receipt
    return dataclasses.replace(
        receipt, committed_version=version,
        written_rows=receipt.input_rows, status="committed", error=None,
    )


def _entry_version(entry) -> int | None:
    for accessor in (
        lambda: entry.version,
        lambda: entry["version"],
        lambda: entry[0],
    ):
        try:
            value = accessor()
        except (AttributeError, KeyError, IndexError, TypeError):
            continue
        if isinstance(value, bool) or not isinstance(value, int):
            continue
        return value
    raise LanceExecutionError("Lance returned an unreadable version history entry")


__all__ = ["find_committed_append", "reconcile_lance_append"]
