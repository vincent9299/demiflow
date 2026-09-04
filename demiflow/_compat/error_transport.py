"""Strict JSON error values transported across process and storage boundaries."""
from __future__ import annotations

from typing import Any, Mapping

ERROR_SCHEMA_VERSION = "error_v1"
_ERROR_FIELDS = frozenset({
    "schema_version", "module", "type", "message", "context", "causes", "redacted",
})
_MAX_CAUSES = 8

__all__ = [
    "ERROR_SCHEMA_VERSION",
    "error_from_exception",
    "make_error",
    "validate_error",
    "is_error_value",
    "contains_error_value",
]


def make_error(
    *, module: str, type_name: str, message: str,
    context: Mapping[str, Any] | None = None,
    causes: list[Mapping[str, Any]] | None = None,
    redacted: bool = False,
) -> dict[str, Any]:
    return validate_error({
        "schema_version": ERROR_SCHEMA_VERSION,
        "module": str(module),
        "type": str(type_name),
        "message": str(message),
        "context": dict(context) if context else None,
        "causes": [dict(cause) for cause in (causes or ())],
        "redacted": bool(redacted),
    })


def validate_error(value: Any) -> dict[str, Any]:
    if not isinstance(value, Mapping) or set(value) != _ERROR_FIELDS:
        raise ValueError("transported error value has an unsupported shape")
    if value["schema_version"] != ERROR_SCHEMA_VERSION:
        raise ValueError("transported error schema is unsupported")
    for field in ("module", "type", "message"):
        if not isinstance(value[field], str):
            raise ValueError("transported error field is invalid: " + field)
    context = value["context"]
    if context is not None and (
        not isinstance(context, Mapping)
        or any(not isinstance(key, str) for key in context)
    ):
        raise ValueError("transported error context is invalid")
    causes = value["causes"]
    if not isinstance(causes, list) or len(causes) > _MAX_CAUSES:
        raise ValueError("transported error causes are invalid")
    for cause in causes:
        if not isinstance(cause, Mapping) or cause.get("schema_version") != ERROR_SCHEMA_VERSION:
            raise ValueError("transported error cause is invalid")
    if not isinstance(value["redacted"], bool):
        raise ValueError("transported error redaction flag is invalid")
    return {
        "schema_version": ERROR_SCHEMA_VERSION,
        "module": value["module"],
        "type": value["type"],
        "message": value["message"],
        "context": dict(context) if context is not None else None,
        "causes": [dict(cause) for cause in causes],
        "redacted": value["redacted"],
    }


def error_from_exception(
    exc: BaseException, context: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    causes: list[dict[str, Any]] = []
    seen = {id(exc)}
    current = exc.__cause__ if exc.__cause__ is not None else exc.__context__
    while current is not None and id(current) not in seen and len(causes) < _MAX_CAUSES:
        seen.add(id(current))
        causes.append({
            "schema_version": ERROR_SCHEMA_VERSION,
            "module": type(current).__module__ or "",
            "type": type(current).__name__,
            "message": str(current),
            "context": None,
            "causes": [],
            "redacted": False,
        })
        current = current.__cause__ if current.__cause__ is not None else current.__context__
    return make_error(
        module=type(exc).__module__ or "",
        type_name=type(exc).__name__,
        message=str(exc),
        context=context,
        causes=causes,
    )


def is_error_value(value: Any) -> bool:
    return (
        isinstance(value, Mapping)
        and set(value) == _ERROR_FIELDS
        and value.get("schema_version") == ERROR_SCHEMA_VERSION
    )


def contains_error_value(value: Any) -> bool:
    if is_error_value(value):
        return True
    if isinstance(value, Mapping):
        return any(contains_error_value(item) for item in value.values())
    if isinstance(value, (list, tuple)):
        return any(contains_error_value(item) for item in value)
    return False
