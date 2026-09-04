"""Pure immutable values for Demiflow's built-in Lance I/O."""
from __future__ import annotations

import hashlib
import json
import math
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Literal, Mapping, Sequence, TypeAlias

from demiflow._compat.error_transport import validate_error

from ..errors import InvalidLanceRequest
from .storage import normalize_lance_uri, normalize_storage_options

if TYPE_CHECKING:
    import pyarrow as pa

Metric: TypeAlias = Literal["l2", "cosine", "dot"]
WriteStatus: TypeAlias = Literal["committed", "indeterminate"]
StorageOptions: TypeAlias = tuple[tuple[str, str], ...]
_MAX_FILTER_BYTES = 64 * 1024
_MAX_FRAGMENT_TOKEN_BYTES = 1024 * 1024


@dataclass(frozen=True)
class LanceScanSpec:
    uri: str
    version: int | None = None
    columns: tuple[str, ...] = ()
    filter: str | None = None
    limit: int | None = None
    storage_options: StorageOptions = ()

    def __post_init__(self) -> None:
        object.__setattr__(self, "uri", normalize_lance_uri(self.uri))
        object.__setattr__(self, "version", _optional_positive_int(self.version, "version"))
        object.__setattr__(self, "columns", normalize_columns(self.columns))
        object.__setattr__(self, "filter", normalize_filter(self.filter))
        object.__setattr__(self, "limit", _optional_positive_int(self.limit, "limit"))
        object.__setattr__(self, "storage_options", normalize_storage_options(self.storage_options))

    def to_dict(self) -> dict[str, Any]:
        return {
            "kind": "scan", "uri": self.uri, "version": self.version,
            "columns": list(self.columns), "filter": self.filter,
            "limit": self.limit, "storage_options": dict(self.storage_options),
        }

    @property
    def content_hash(self) -> str:
        return _json_hash(self.to_dict())


@dataclass(frozen=True)
class LanceVectorSearchSpec:
    uri: str
    vector: tuple[float, ...]
    vector_column: str
    top_k: int
    version: int | None = None
    columns: tuple[str, ...] = ()
    filter: str | None = None
    metric: Metric | None = None
    storage_options: StorageOptions = ()

    def __post_init__(self) -> None:
        object.__setattr__(self, "uri", normalize_lance_uri(self.uri))
        object.__setattr__(self, "vector", normalize_vector(self.vector))
        object.__setattr__(self, "vector_column", _normalized_text(self.vector_column, "vector_column"))
        object.__setattr__(self, "top_k", _positive_int(self.top_k, "top_k"))
        object.__setattr__(self, "version", _optional_positive_int(self.version, "version"))
        object.__setattr__(self, "columns", normalize_columns(self.columns))
        object.__setattr__(self, "filter", normalize_filter(self.filter))
        if self.metric not in {None, "l2", "cosine", "dot"}:
            raise InvalidLanceRequest(f"unsupported Lance metric: {self.metric!r}")
        object.__setattr__(self, "storage_options", normalize_storage_options(self.storage_options))

    def to_dict(self) -> dict[str, Any]:
        return {
            "kind": "vector_search", "uri": self.uri,
            "vector": list(self.vector), "vector_column": self.vector_column,
            "top_k": self.top_k, "version": self.version,
            "columns": list(self.columns), "filter": self.filter,
            "metric": self.metric, "storage_options": dict(self.storage_options),
        }

    @property
    def content_hash(self) -> str:
        return _json_hash(self.to_dict())


LanceQuerySpec: TypeAlias = LanceScanSpec | LanceVectorSearchSpec


@dataclass(frozen=True)
class LanceWriteSpec:
    uri: str
    expected_version: int | None = None
    storage_options: StorageOptions = ()

    def __post_init__(self) -> None:
        object.__setattr__(self, "uri", normalize_lance_uri(self.uri))
        object.__setattr__(
            self, "expected_version",
            _optional_positive_int(self.expected_version, "expected_version"),
        )
        object.__setattr__(self, "storage_options", normalize_storage_options(self.storage_options))

    def to_dict(self) -> dict[str, Any]:
        return {
            "kind": "append", "uri": self.uri,
            "expected_version": self.expected_version,
            "storage_options": dict(self.storage_options),
        }

    @property
    def content_hash(self) -> str:
        return _json_hash(self.to_dict())


@dataclass(frozen=True)
class LanceInspection:
    uri: str
    requested_version: int | None
    resolved_version: int
    schema: "pa.Schema"
    vector_fields: Mapping[str, Mapping[str, Any]]
    inspection_hash: str

    def to_dict(self) -> dict[str, Any]:
        return {
            "uri": self.uri, "requested_version": self.requested_version,
            "resolved_version": self.resolved_version,
            "schema": [
                {"name": field.name, "type": str(field.type), "nullable": field.nullable}
                for field in self.schema
            ],
            "vector_fields": {
                name: dict(value) for name, value in self.vector_fields.items()
            },
            "inspection_hash": self.inspection_hash,
        }


@dataclass(frozen=True)
class LanceWriteReceipt:
    request_hash: str
    uri: str
    expected_version: int | None
    committed_version: int | None
    input_rows: int
    written_rows: int | None
    schema_hash: str
    status: WriteStatus
    error: Mapping[str, Any] | None = None

    def __post_init__(self) -> None:
        _normalized_text(self.request_hash, "request_hash")
        object.__setattr__(self, "uri", normalize_lance_uri(self.uri))
        object.__setattr__(
            self, "expected_version",
            _optional_positive_int(self.expected_version, "expected_version"),
        )
        object.__setattr__(
            self, "committed_version",
            _optional_positive_int(self.committed_version, "committed_version"),
        )
        _nonnegative_int(self.input_rows, "input_rows")
        if self.written_rows is not None:
            _nonnegative_int(self.written_rows, "written_rows")
        _normalized_text(self.schema_hash, "schema_hash")
        if self.status not in {"committed", "indeterminate"}:
            raise InvalidLanceRequest(f"unsupported Lance write status: {self.status!r}")
        if self.status == "committed":
            if self.committed_version is None or self.written_rows is None:
                raise InvalidLanceRequest("committed Lance receipt is incomplete")
            if self.written_rows != self.input_rows:
                raise InvalidLanceRequest("committed Lance row counts differ")
            if self.error is not None:
                raise InvalidLanceRequest("committed Lance receipt must not contain an error")
        else:
            if self.written_rows is not None or self.error is None:
                raise InvalidLanceRequest("indeterminate Lance receipt requires only an error")
            object.__setattr__(self, "error", validate_error(self.error))

    def to_dict(self) -> dict[str, Any]:
        return {
            "schema_version": "demiflow_lance_write_receipt_v1",
            "request_hash": self.request_hash, "uri": self.uri,
            "expected_version": self.expected_version,
            "committed_version": self.committed_version,
            "input_rows": self.input_rows, "written_rows": self.written_rows,
            "schema_hash": self.schema_hash, "status": self.status,
            "error": dict(self.error) if self.error is not None else None,
        }

    @classmethod
    def from_dict(cls, value: Mapping[str, Any]) -> "LanceWriteReceipt":
        fields = {
            "schema_version", "request_hash", "uri", "expected_version",
            "committed_version", "input_rows", "written_rows", "schema_hash",
            "status", "error",
        }
        if not isinstance(value, Mapping) or set(value) != fields:
            raise InvalidLanceRequest("Lance write receipt fields are unsupported")
        if value.get("schema_version") != "demiflow_lance_write_receipt_v1":
            raise InvalidLanceRequest("unsupported Lance write receipt schema")
        return cls(
            request_hash=value["request_hash"], uri=value["uri"],
            expected_version=value["expected_version"],
            committed_version=value["committed_version"],
            input_rows=value["input_rows"], written_rows=value["written_rows"],
            schema_hash=value["schema_hash"], status=value["status"],
            error=value["error"],
        )


@dataclass(frozen=True)
class _LanceScanPartition:
    query: LanceScanSpec
    schema: "pa.Schema"
    schema_hash: str
    partition_index: int
    partition_count: int
    fragment_ids: tuple[int, ...]
    source_inventory_hash: str
    storage_fingerprint: str

    def __post_init__(self) -> None:
        if self.query.version is None or self.query.limit is not None:
            raise InvalidLanceRequest("Lance partition requires an unlimited exact-version scan")
        if (
            isinstance(self.partition_index, bool)
            or not isinstance(self.partition_index, int)
            or isinstance(self.partition_count, bool)
            or not isinstance(self.partition_count, int)
            or self.partition_count <= 0
            or not 0 <= self.partition_index < self.partition_count
        ):
            raise InvalidLanceRequest("Lance partition index/count is invalid")
        if any(isinstance(value, bool) or not isinstance(value, int) or value < 0 for value in self.fragment_ids):
            raise InvalidLanceRequest("Lance fragment IDs are invalid")
        if len(set(self.fragment_ids)) != len(self.fragment_ids):
            raise InvalidLanceRequest("Lance partition contains duplicate fragment IDs")
        _normalized_text(self.schema_hash, "schema_hash")
        _normalized_text(self.source_inventory_hash, "source_inventory_hash")
        _normalized_text(self.storage_fingerprint, "storage_fingerprint")


@dataclass(frozen=True)
class _PreparedLanceAppend:
    operation_id: str
    request_hash: str
    uri: str
    expected_version: int
    schema: "pa.Schema"
    schema_hash: str
    storage_fingerprint: str
    storage_options: StorageOptions

    def __post_init__(self) -> None:
        _normalized_text(self.operation_id, "operation_id")
        _normalized_text(self.request_hash, "request_hash")
        object.__setattr__(self, "uri", normalize_lance_uri(self.uri))
        object.__setattr__(self, "expected_version", _positive_int(self.expected_version, "expected_version"))
        _normalized_text(self.schema_hash, "schema_hash")
        _normalized_text(self.storage_fingerprint, "storage_fingerprint")
        object.__setattr__(self, "storage_options", normalize_storage_options(self.storage_options))


@dataclass(frozen=True)
class _LanceFragmentReceipt:
    operation_id: str
    task_index: int
    rows: int
    schema_hash: str
    fragment_json: str

    def __post_init__(self) -> None:
        _normalized_text(self.operation_id, "operation_id")
        if isinstance(self.task_index, bool) or not isinstance(self.task_index, int) or self.task_index < 0:
            raise InvalidLanceRequest("Lance fragment task_index is invalid")
        if isinstance(self.rows, bool) or not isinstance(self.rows, int) or self.rows <= 0:
            raise InvalidLanceRequest("Lance fragment rows must be positive")
        _normalized_text(self.schema_hash, "schema_hash")
        token = _canonical_fragment_json(self.fragment_json)
        object.__setattr__(self, "fragment_json", token)

    def to_dict(self) -> dict[str, Any]:
        return {
            "operation_id": self.operation_id, "task_index": self.task_index,
            "rows": self.rows, "schema_hash": self.schema_hash,
            "fragment_token": json.loads(self.fragment_json),
        }

    @classmethod
    def from_dict(cls, value: Mapping[str, Any]) -> "_LanceFragmentReceipt":
        fields = {"operation_id", "task_index", "rows", "schema_hash", "fragment_token"}
        if not isinstance(value, Mapping) or set(value) != fields:
            raise InvalidLanceRequest("Lance fragment receipt fields are unsupported")
        return cls(
            value["operation_id"], value["task_index"], value["rows"],
            value["schema_hash"], _strict_json(value["fragment_token"]),
        )


def normalize_columns(value: Sequence[str] | tuple[str, ...] | None) -> tuple[str, ...]:
    if value is None:
        return ()
    if isinstance(value, str) or not isinstance(value, Sequence):
        raise InvalidLanceRequest("columns must be a sequence of names")
    result = tuple(_normalized_text(item, "column") for item in value)
    if len(result) != len(set(result)):
        raise InvalidLanceRequest("columns must be unique")
    return result


def normalize_filter(value: str | None) -> str | None:
    if value is None:
        return None
    result = _normalized_text(value, "filter")
    if len(result.encode("utf-8")) > _MAX_FILTER_BYTES:
        raise InvalidLanceRequest("filter exceeds 64 KiB")
    return result


def normalize_vector(value: Sequence[float] | tuple[float, ...]) -> tuple[float, ...]:
    if isinstance(value, (str, bytes)) or not isinstance(value, Sequence) or not value:
        raise InvalidLanceRequest("vector must be a non-empty sequence")
    result = []
    for item in value:
        if isinstance(item, bool) or not isinstance(item, (int, float)) or not math.isfinite(float(item)):
            raise InvalidLanceRequest("vector must contain finite numbers")
        result.append(float(item))
    return tuple(result)


def _canonical_fragment_json(value: str) -> str:
    if not isinstance(value, str):
        raise InvalidLanceRequest("Lance fragment token must be JSON text")
    try:
        decoded = json.loads(value)
    except json.JSONDecodeError as exc:
        raise InvalidLanceRequest("Lance fragment token is invalid JSON") from exc
    if not isinstance(decoded, dict):
        raise InvalidLanceRequest("Lance fragment token must be an object")
    result = _strict_json(decoded)
    if len(result.encode("utf-8")) > _MAX_FRAGMENT_TOKEN_BYTES:
        raise InvalidLanceRequest("Lance fragment token exceeds 1 MiB")
    return result


def _strict_json(value: Any) -> str:
    try:
        return json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False)
    except (TypeError, ValueError) as exc:
        raise InvalidLanceRequest("value must contain strict JSON") from exc


def _json_hash(value: Mapping[str, Any]) -> str:
    return "sha256:" + hashlib.sha256(_strict_json(value).encode("utf-8")).hexdigest()


def _normalized_text(value: Any, label: str) -> str:
    if not isinstance(value, str) or not value or value != value.strip():
        raise InvalidLanceRequest(f"{label} must be a normalized non-empty string")
    return value


def _positive_int(value: Any, label: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
        raise InvalidLanceRequest(f"{label} must be a positive integer")
    return value


def _optional_positive_int(value: Any, label: str) -> int | None:
    return None if value is None else _positive_int(value, label)


def _nonnegative_int(value: Any, label: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise InvalidLanceRequest(f"{label} must be a non-negative integer")
    return value


__all__ = [
    "LanceInspection", "LanceQuerySpec", "LanceScanSpec",
    "LanceVectorSearchSpec", "LanceWriteReceipt", "LanceWriteSpec",
]
