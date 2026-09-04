"""Closed Lance fragment metadata codec for distributed append."""
from __future__ import annotations

import json
from pathlib import PurePosixPath
from typing import Mapping

from ..errors import InvalidLanceRequest
from .model import _LanceFragmentReceipt, _PreparedLanceAppend
from .storage import require_lance

_MAX_RECEIPTS = 100_000
_MAX_FILES_PER_FRAGMENT = 1024


def encode_lance_fragment(
    metadata, prepared: _PreparedLanceAppend, task_index: int,
) -> _LanceFragmentReceipt:
    token = metadata.to_json()
    if not isinstance(token, dict):
        raise InvalidLanceRequest("Lance fragment metadata is not JSON")
    rows = getattr(metadata, "physical_rows", None)
    return _LanceFragmentReceipt(
        prepared.operation_id, task_index, rows, prepared.schema_hash,
        json.dumps(token, sort_keys=True, separators=(",", ":"), allow_nan=False),
    )


def decode_lance_fragments(
    prepared: _PreparedLanceAppend,
    receipts: tuple[_LanceFragmentReceipt, ...],
):
    if not receipts or len(receipts) > _MAX_RECEIPTS:
        raise InvalidLanceRequest(
            "distributed Lance append requires a bounded non-empty fragment set"
        )
    tasks: set[int] = set()
    paths: set[str] = set()
    result = []
    rows = 0
    FragmentMetadata = require_lance().fragment.FragmentMetadata
    for receipt in receipts:
        if (
            receipt.operation_id != prepared.operation_id
            or receipt.schema_hash != prepared.schema_hash
        ):
            raise InvalidLanceRequest("Lance fragment does not match prepared append")
        if receipt.task_index in tasks:
            raise InvalidLanceRequest("duplicate Lance fragment task index")
        tasks.add(receipt.task_index)
        token = json.loads(receipt.fragment_json)
        expected = {
            "id", "files", "physical_rows", "deletion_file", "row_id_meta",
            "created_at_version_meta", "last_updated_at_version_meta",
        }
        if set(token) != expected:
            raise InvalidLanceRequest("unsupported Lance fragment metadata fields")
        if token["deletion_file"] is not None or token["row_id_meta"] is not None:
            raise InvalidLanceRequest(
                "Lance append fragments cannot contain deletion or row-id metadata"
            )
        files = token["files"]
        if (
            not isinstance(files, list) or not files
            or len(files) > _MAX_FILES_PER_FRAGMENT
        ):
            raise InvalidLanceRequest("Lance fragment files are invalid")
        for item in files:
            path = item.get("path") if isinstance(item, Mapping) else None
            if (
                not isinstance(path, str) or not path
                or PurePosixPath(path).is_absolute()
                or ".." in PurePosixPath(path).parts
            ):
                raise InvalidLanceRequest("Lance fragment file path is invalid")
            if path in paths:
                raise InvalidLanceRequest("duplicate Lance fragment file path")
            paths.add(path)
        try:
            metadata = FragmentMetadata.from_json(receipt.fragment_json)
        except Exception as exc:
            raise InvalidLanceRequest("invalid Lance fragment metadata") from exc
        if metadata.physical_rows != receipt.rows:
            raise InvalidLanceRequest("Lance fragment row count differs")
        rows += receipt.rows
        result.append(metadata)
    return result, rows, paths


__all__ = ["decode_lance_fragments", "encode_lance_fragment"]
