"""Lance URI, optional runtime, dataset opening, and schema invariants."""
from __future__ import annotations

import hashlib
import json
import os
from pathlib import Path
from types import MappingProxyType
from typing import Any, Mapping, Sequence
from urllib.parse import urlsplit, urlunsplit

from ..errors import (
    InvalidLanceRequest, LanceExecutionError, LanceResourceNotFound,
    LanceUnavailable,
)

_ALLOWED_SCHEMES = frozenset({"s3", "gs", "az", "file"})
_ALLOWED_STORAGE_OPTIONS = frozenset({
    "endpoint", "region", "virtual_hosted_style_request",
})


def normalize_lance_uri(value: Any) -> str:
    if not isinstance(value, str) or not value or value != value.strip():
        raise InvalidLanceRequest("Lance uri must be a normalized non-empty string")
    parsed = urlsplit(value)
    if parsed.query or parsed.fragment or parsed.username or parsed.password:
        raise InvalidLanceRequest("Lance uri must not contain credentials, query, or fragment")
    if parsed.scheme:
        if parsed.scheme not in _ALLOWED_SCHEMES:
            raise InvalidLanceRequest(f"unsupported Lance URI scheme: {parsed.scheme}")
        if parsed.scheme != "file" and not parsed.netloc:
            raise InvalidLanceRequest("Lance object URI requires a bucket or host")
        if parsed.scheme == "file" and parsed.netloc not in {"", "localhost"}:
            raise InvalidLanceRequest("Lance file URI must be local")
        return urlunsplit((parsed.scheme, parsed.netloc, parsed.path, "", ""))
    path = Path(os.path.expanduser(value))
    if not path.is_absolute():
        raise InvalidLanceRequest("Lance uri must be an absolute path or supported URI")
    return str(path.resolve(strict=False))


def normalize_storage_options(
    value: Mapping[str, str] | Sequence[tuple[str, str]] | None,
) -> tuple[tuple[str, str], ...]:
    if value is None:
        return ()
    try:
        items = tuple(value.items()) if isinstance(value, Mapping) else tuple(value)
    except (TypeError, ValueError) as exc:
        raise InvalidLanceRequest("storage_options must be a string mapping") from exc
    result: dict[str, str] = {}
    for item in items:
        if not isinstance(item, tuple) or len(item) != 2:
            raise InvalidLanceRequest("storage_options must contain key/value pairs")
        key, entry = item
        if (
            not isinstance(key, str) or not key or key != key.strip()
            or not isinstance(entry, str) or not entry
        ):
            raise InvalidLanceRequest("storage_options must contain normalized strings")
        if key not in _ALLOWED_STORAGE_OPTIONS:
            raise InvalidLanceRequest(f"unsupported Lance storage option: {key}")
        if key in result:
            raise InvalidLanceRequest(f"duplicate Lance storage option: {key}")
        result[key] = entry
    return tuple(sorted(result.items()))


def require_lance():
    try:
        import lance
    except ImportError as exc:
        raise LanceUnavailable("Lance I/O requires the 'pylance' optional dependency") from exc
    return lance


def lance_commit_conflict_error():
    """Return Lance's native conflict type without leaking SDK imports."""
    require_lance()
    try:
        from lance.commit import CommitConflictError
    except ImportError as exc:
        raise LanceUnavailable("Lance commit API is unavailable") from exc
    return CommitConflictError


def open_lance_dataset(
    uri: str, version: int | None,
    storage_options: tuple[tuple[str, str], ...] = (),
):
    normalized = normalize_lance_uri(uri)
    resolved_version = _optional_version(version)
    if _local_dataset_missing(normalized):
        raise LanceResourceNotFound(f"Lance dataset not found: {normalized}")
    return require_lance().dataset(
        normalized, version=resolved_version,
        storage_options=dict(normalize_storage_options(storage_options)) or None,
    )


def inspect_lance(
    uri: str, version: int | None = None,
    storage_options: Mapping[str, str] | Sequence[tuple[str, str]] | None = None,
):
    import pyarrow as pa

    from .model import LanceInspection

    normalized = normalize_lance_uri(uri)
    options = normalize_storage_options(storage_options)
    dataset = open_lance_dataset(normalized, version, options)
    schema = dataset.schema
    if not isinstance(schema, pa.Schema):
        raise LanceExecutionError("Lance returned a non-Arrow schema")
    resolved = getattr(dataset, "version", None)
    if isinstance(resolved, bool) or not isinstance(resolved, int) or resolved <= 0:
        raise LanceExecutionError("Lance returned an invalid dataset version")
    vector_fields = _vector_fields(schema)
    payload = {
        "protocol": "demiflow_lance_v1", "uri": normalized,
        "requested_version": version, "resolved_version": resolved,
        "schema": _schema_value(schema), "vector_fields": vector_fields,
        "storage_fingerprint": storage_fingerprint(normalized, options),
    }
    return LanceInspection(
        uri=normalized, requested_version=version, resolved_version=resolved,
        schema=schema,
        vector_fields=MappingProxyType({
            name: MappingProxyType(dict(item))
            for name, item in vector_fields.items()
        }),
        inspection_hash=_json_hash(payload),
    )


def storage_fingerprint(
    uri: str, storage_options: tuple[tuple[str, str], ...] = (),
) -> str:
    return _json_hash({
        "protocol": "demiflow_lance_v1",
        "uri": normalize_lance_uri(uri),
        "storage_options": dict(normalize_storage_options(storage_options)),
    })


def schema_hash(schema) -> str:
    import pyarrow as pa

    if not isinstance(schema, pa.Schema):
        raise InvalidLanceRequest("Lance schema must be a pyarrow.Schema")
    return _json_hash(_schema_value(schema))


def require_compatible_schema(actual, expected) -> None:
    import pyarrow as pa

    if not isinstance(actual, pa.Schema) or not isinstance(expected, pa.Schema):
        raise InvalidLanceRequest("Lance append schemas must be pyarrow.Schema")
    if len(actual) != len(expected):
        raise InvalidLanceRequest("Lance append schema field count differs from target")
    for left, right in zip(actual, expected):
        if (
            left.name != right.name
            or left.type != right.type
            or left.nullable != right.nullable
        ):
            raise InvalidLanceRequest(
                f"Lance append schema differs at field {right.name!r}"
            )


def _optional_version(value: Any) -> int | None:
    if value is None:
        return None
    if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
        raise InvalidLanceRequest("Lance version must be a positive integer")
    return value


def _local_dataset_missing(uri: str) -> bool:
    parsed = urlsplit(uri)
    if parsed.scheme not in {"", "file"}:
        return False
    path = Path(parsed.path if parsed.scheme == "file" else uri)
    try:
        os.stat(path)
    except FileNotFoundError:
        return True
    except OSError as exc:
        raise LanceExecutionError(
            f"failed to access Lance dataset path: {path.name}"
        ) from exc
    return False


def _vector_fields(schema) -> dict[str, dict[str, Any]]:
    import pyarrow as pa

    result = {}
    for field in schema:
        if (
            pa.types.is_fixed_size_list(field.type)
            and pa.types.is_floating(field.type.value_type)
        ):
            result[field.name] = {
                "dimension": field.type.list_size,
                "metrics": ["l2", "cosine", "dot"],
            }
    return result


def _schema_value(schema) -> list[dict[str, Any]]:
    return [
        {"name": field.name, "type": str(field.type), "nullable": field.nullable}
        for field in schema
    ]


def _json_hash(value: Any) -> str:
    raw = json.dumps(
        value, sort_keys=True, separators=(",", ":"), ensure_ascii=False,
        allow_nan=False,
    ).encode("utf-8")
    return "sha256:" + hashlib.sha256(raw).hexdigest()


__all__ = [
    "inspect_lance", "normalize_lance_uri", "normalize_storage_options",
    "lance_commit_conflict_error", "open_lance_dataset",
    "require_compatible_schema", "require_lance",
    "schema_hash", "storage_fingerprint",
]
