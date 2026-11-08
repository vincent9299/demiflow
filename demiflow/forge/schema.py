"""Closed JSON contracts and their Arrow representation."""
from __future__ import annotations

import hashlib
import json
import math
import re
from typing import Any, Mapping


class ForgeError(ValueError):
    CODES = frozenset({'invalid_request', 'not_found', 'conflict', 'busy',
                       'resource_limit', 'operation_rejected', 'operation_failed'})

    def __init__(self, message, *, code='operation_rejected'):
        if code not in self.CODES:
            raise ValueError('Unknown Forge error code')
        self.code = code
        super().__init__(message)


_NAME = re.compile(r"[a-z][a-z0-9_-]{0,79}")


def canonical(value: Any) -> bytes:
    return json.dumps(
        value, ensure_ascii=False, sort_keys=True, separators=(",", ":"),
        allow_nan=False,
    ).encode("utf-8")


def digest(value: Any) -> str:
    return hashlib.sha256(canonical(value)).hexdigest()


def copy_json(value: Any) -> Any:
    return json.loads(canonical(value))


def exact_fields(value: Any, fields: set[str], label: str) -> Mapping[str, Any]:
    if not isinstance(value, Mapping) or set(value) != fields:
        raise ForgeError(f"{label} must have exactly {sorted(fields)}")
    return value


def required_text(value: Any, label: str, *, maximum: int = 100_000) -> str:
    if not isinstance(value, str) or not value.strip() or len(value.encode("utf-8")) > maximum:
        raise ForgeError(f"{label} must be non-empty text within {maximum} bytes")
    return value


def safe_name(value: Any, label: str) -> str:
    if not isinstance(value, str) or _NAME.fullmatch(value) is None:
        raise ForgeError(f"{label} must be a lowercase identifier")
    return value


def positive_int(value: Any, label: str, *, maximum: int) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or not 1 <= value <= maximum:
        raise ForgeError(f"{label} must be an integer in 1..{maximum}")
    return value


def normalize_schema(value: Any, *, depth: int = 0) -> dict[str, Any]:
    if depth > 8 or not isinstance(value, Mapping):
        raise ForgeError("JSON schema is invalid")
    kind = value.get("type")
    common = {"type", "enum"}
    allowed = {
        "object": common | {"properties", "required", "additionalProperties"},
        "array": common | {"items", "minItems", "maxItems"},
        "string": common | {"minLength", "maxLength"},
        "integer": common | {"minimum", "maximum"},
        "number": common | {"minimum", "maximum"},
        "boolean": common,
        "null": common,
    }
    if kind not in allowed or set(value) - allowed[kind]:
        raise ForgeError("JSON schema fields are invalid")
    if kind != "object" and depth == 0:
        raise ForgeError("Root JSON schema must be an object")
    result: dict[str, Any] = {"type": kind}
    if "enum" in value:
        enum = value["enum"]
        if not isinstance(enum, list) or not enum or len(enum) > 128:
            raise ForgeError("JSON schema enum is invalid")
        result["enum"] = [copy_json(item) for item in enum]
    if kind == "object":
        properties = value.get("properties")
        required = value.get("required")
        if (
            value.get("additionalProperties") is not False
            or not isinstance(properties, Mapping)
            or len(properties) > 128
            or not isinstance(required, list)
            or any(not isinstance(name, str) or not name for name in required)
            or len(required) != len(set(required))
            or not set(required) <= set(properties)
            or any(not isinstance(name, str) or not name for name in properties)
        ):
            raise ForgeError("JSON schema object is invalid")
        result["properties"] = {
            name: normalize_schema(spec, depth=depth + 1)
            for name, spec in properties.items()
        }
        result["required"] = list(required)
        result["additionalProperties"] = False
    elif kind == "array":
        result["items"] = normalize_schema(value.get("items"), depth=depth + 1)
        _schema_bounds(value, result, "minItems", "maxItems")
    elif kind == "string":
        _schema_bounds(value, result, "minLength", "maxLength")
    elif kind in {"integer", "number"}:
        for name in ("minimum", "maximum"):
            if name in value:
                bound = value[name]
                if isinstance(bound, bool) or not isinstance(bound, (int, float)) or not math.isfinite(bound):
                    raise ForgeError("JSON schema numeric bound is invalid")
                result[name] = bound
        if "minimum" in result and "maximum" in result and result["minimum"] > result["maximum"]:
            raise ForgeError("JSON schema numeric bounds are invalid")
    return result


def _schema_bounds(source: Mapping[str, Any], result: dict[str, Any], lower: str, upper: str) -> None:
    for name in (lower, upper):
        if name in source:
            value = source[name]
            if isinstance(value, bool) or not isinstance(value, int) or value < 0:
                raise ForgeError("JSON schema size bound is invalid")
            result[name] = value
    if lower in result and upper in result and result[lower] > result[upper]:
        raise ForgeError("JSON schema size bounds are invalid")


def validate_data(value: Any, schema: Mapping[str, Any], path: str = "$") -> Any:
    kind = schema["type"]
    if kind == "object":
        if not isinstance(value, Mapping):
            raise ForgeError(f"{path} expected object")
        properties = schema["properties"]
        if set(value) - set(properties) or not set(schema["required"]) <= set(value):
            raise ForgeError(f"{path} object fields do not match schema")
        result = {
            name: validate_data(item, properties[name], f"{path}.{name}")
            for name, item in value.items()
        }
    elif kind == "array":
        if not isinstance(value, list):
            raise ForgeError(f"{path} expected array")
        _validate_length(value, schema, path, "minItems", "maxItems")
        result = [validate_data(item, schema["items"], f"{path}[{index}]") for index, item in enumerate(value)]
    elif kind == "string":
        if not isinstance(value, str):
            raise ForgeError(f"{path} expected string")
        _validate_length(value, schema, path, "minLength", "maxLength")
        result = value
    elif kind == "integer":
        if isinstance(value, bool) or not isinstance(value, int):
            raise ForgeError(f"{path} expected integer")
        result = value
    elif kind == "number":
        if isinstance(value, bool) or not isinstance(value, (int, float)) or not math.isfinite(value):
            raise ForgeError(f"{path} expected finite number")
        result = value
    elif kind == "boolean":
        if not isinstance(value, bool):
            raise ForgeError(f"{path} expected boolean")
        result = value
    elif kind == "null":
        if value is not None:
            raise ForgeError(f"{path} expected null")
        result = None
    else:
        raise ForgeError(f"{path} has unsupported schema type")
    if kind in {"integer", "number"}:
        if "minimum" in schema and result < schema["minimum"]:
            raise ForgeError(f"{path} is below minimum")
        if "maximum" in schema and result > schema["maximum"]:
            raise ForgeError(f"{path} is above maximum")
    if "enum" in schema and result not in schema["enum"]:
        raise ForgeError(f"{path} is outside enum")
    return result


def _validate_length(value: Any, schema: Mapping[str, Any], path: str, lower: str, upper: str) -> None:
    if lower in schema and len(value) < schema[lower]:
        raise ForgeError(f"{path} violates {lower}")
    if upper in schema and len(value) > schema[upper]:
        raise ForgeError(f"{path} violates {upper}")


def arrow_schema(schema: Mapping[str, Any]):
    import pyarrow as pa

    if schema.get("type") != "object":
        raise ForgeError("Arrow output requires an object schema")
    return pa.schema([pa.field(name, _arrow_type(spec, pa)) for name, spec in schema["properties"].items()])


def _arrow_type(schema: Mapping[str, Any], pa):
    kind = schema["type"]
    if kind == "object":
        return pa.struct([pa.field(name, _arrow_type(spec, pa)) for name, spec in schema["properties"].items()])
    if kind == "array":
        return pa.list_(_arrow_type(schema["items"], pa))
    return {
        "string": pa.string(),
        "integer": pa.int64(),
        "number": pa.float64(),
        "boolean": pa.bool_(),
        "null": pa.null(),
    }[kind]


__all__ = [
    "ForgeError", "arrow_schema", "canonical", "copy_json", "digest",
    "exact_fields", "normalize_schema", "positive_int", "required_text",
    "safe_name", "validate_data",
]
