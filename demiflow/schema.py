"""Closed JSON-schema subset for Operator LLM prompt responses."""
from __future__ import annotations

from datetime import date, datetime
from typing import Any, Mapping

_ALLOWED = {
    "type", "properties", "required", "additionalProperties", "items",
    "minItems", "maxItems", "minLength", "maxLength", "minimum", "maximum",
    "exclusiveMinimum", "exclusiveMaximum", "enum", "const", "format",
}
_TYPES = {"object", "array", "string", "integer", "number", "boolean", "null"}
_FORMATS = {"date", "date-time"}
_MAX_DEPTH = 12
_MAX_NODES = 512


class SchemaError(ValueError):
    pass


class SchemaValidationError(ValueError):
    pass


def _compile_schema_strict(value: Any) -> dict[str, Any]:
    if not isinstance(value, Mapping):
        raise SchemaError("schema must be an object")
    schema = _compile(dict(value), "$", 0, [0])
    if schema.get("type") != "object":
        raise SchemaError("schema root type must be object")
    return schema



def inspect_schema(value: Any) -> tuple[dict[str, Any] | None, tuple[tuple[str, str], ...]]:
    """Collect independent schema errors and return a compiled schema when valid."""
    issues: list[tuple[str, str]] = []
    nodes = [0]

    def visit(raw: Any, path: str, depth: int) -> None:
        nodes[0] += 1
        if depth > _MAX_DEPTH or nodes[0] > _MAX_NODES:
            issues.append((path, "schema complexity limit exceeded"))
            return
        if not isinstance(raw, Mapping):
            issues.append((path, f"{path} must be a schema object"))
            return
        extra = sorted(set(raw) - _ALLOWED)
        for name in extra:
            issues.append((f"{path}.{name}", f"{path} contains unsupported schema field: {name}"))
        kind = raw.get("type")
        if kind is None:
            if path == "$": issues.append((f"{path}.type", "schema root requires type"))
            return
        if kind not in _TYPES:
            issues.append((f"{path}.type", f"{path}.type must be one of {sorted(_TYPES)}"))
            return
        if "enum" in raw and (not isinstance(raw["enum"], list) or not raw["enum"]):
            issues.append((f"{path}.enum", f"{path}.enum must be a non-empty array"))
        for name in ("minItems", "maxItems", "minLength", "maxLength"):
            if name in raw and (isinstance(raw[name], bool) or not isinstance(raw[name], int) or raw[name] < 0):
                issues.append((f"{path}.{name}", f"{path}.{name} must be a non-negative integer"))
        for name in ("minimum", "maximum", "exclusiveMinimum", "exclusiveMaximum"):
            if name in raw and (isinstance(raw[name], bool) or not isinstance(raw[name], (int, float))):
                issues.append((f"{path}.{name}", f"{path}.{name} must be numeric"))
        for low, high in (("minItems","maxItems"),("minLength","maxLength"),("minimum","maximum")):
            if low in raw and high in raw and isinstance(raw[low], (int,float)) and isinstance(raw[high], (int,float)) and raw[low] > raw[high]:
                issues.append((f"{path}.{low}", f"{path}.{low} cannot exceed {high}"))
        if kind == "object":
            props = raw.get("properties") or {}
            if not isinstance(props, Mapping):
                issues.append((f"{path}.properties", f"{path}.properties must be an object"))
            else:
                for name, child in sorted(props.items(), key=lambda item: str(item[0])):
                    visit(child, f"{path}.properties.{name}", depth + 1)
            required = raw.get("required") or []
            if not isinstance(required, list) or any(not isinstance(x, str) or not x for x in required):
                issues.append((f"{path}.required", f"{path}.required must be a string array"))
            elif len(required) != len(set(required)) or (isinstance(props, Mapping) and set(required) - set(props)):
                issues.append((f"{path}.required", f"{path}.required must name unique properties"))
            if not isinstance(raw.get("additionalProperties", False), bool):
                issues.append((f"{path}.additionalProperties", f"{path}.additionalProperties must be boolean"))
        elif kind == "array":
            items = raw.get("items")
            if not isinstance(items, Mapping):
                issues.append((f"{path}.items", f"{path}.items must be a schema"))
            else:
                visit(items, f"{path}.items", depth + 1)
        elif kind == "string" and "format" in raw and raw["format"] not in _FORMATS:
            issues.append((f"{path}.format", f"{path}.format must be one of {sorted(_FORMATS)}"))
    visit(value, "$", 0)
    if issues:
        return None, tuple(issues)
    return _compile_schema_strict(value), ()


def compile_schema(value: Any) -> dict[str, Any]:
    schema, diagnostics = inspect_schema(value)
    if diagnostics or schema is None:
        raise SchemaError("; ".join(message for _, message in diagnostics))
    return schema

def validate_instance(value: Any, schema: Mapping[str, Any], *, label: str = "value") -> None:
    _validate(value, schema, "$", label)


def required_properties(schema: Mapping[str, Any]) -> tuple[str, ...]:
    return tuple(str(item) for item in schema.get("required") or ())


def _compile(schema: dict[str, Any], path: str, depth: int, nodes: list[int]) -> dict[str, Any]:
    nodes[0] += 1
    if depth > _MAX_DEPTH or nodes[0] > _MAX_NODES:
        raise SchemaError("schema complexity limit exceeded")
    extra = sorted(set(schema) - _ALLOWED)
    if extra:
        raise SchemaError(f"{path} contains unsupported schema fields: {extra}")
    kind = schema.get("type")
    if kind is None:
        if path == "$":
            raise SchemaError("schema root requires type")
        return schema
    if kind not in _TYPES:
        raise SchemaError(f"{path}.type must be one of {sorted(_TYPES)}")
    if "enum" in schema and (not isinstance(schema["enum"], list) or not schema["enum"]):
        raise SchemaError(f"{path}.enum must be a non-empty array")
    for name in ("minItems", "maxItems", "minLength", "maxLength"):
        if name in schema and (isinstance(schema[name], bool) or not isinstance(schema[name], int) or schema[name] < 0):
            raise SchemaError(f"{path}.{name} must be a non-negative integer")
    for name in ("minimum", "maximum", "exclusiveMinimum", "exclusiveMaximum"):
        if name in schema and (isinstance(schema[name], bool) or not isinstance(schema[name], (int, float))):
            raise SchemaError(f"{path}.{name} must be numeric")
    if "minItems" in schema and "maxItems" in schema and schema["minItems"] > schema["maxItems"]:
        raise SchemaError(f"{path}.minItems cannot exceed maxItems")
    if "minLength" in schema and "maxLength" in schema and schema["minLength"] > schema["maxLength"]:
        raise SchemaError(f"{path}.minLength cannot exceed maxLength")
    if "minimum" in schema and "maximum" in schema and schema["minimum"] > schema["maximum"]:
        raise SchemaError(f"{path}.minimum cannot exceed maximum")
    if kind == "object":
        props = schema.get("properties") or {}
        if not isinstance(props, Mapping):
            raise SchemaError(f"{path}.properties must be an object")
        required = schema.get("required") or []
        if not isinstance(required, list) or any(not isinstance(x, str) or not x for x in required):
            raise SchemaError(f"{path}.required must be a string array")
        if len(required) != len(set(required)) or set(required) - set(props):
            raise SchemaError(f"{path}.required must name unique properties")
        additional = schema.get("additionalProperties", False)
        if not isinstance(additional, bool):
            raise SchemaError(f"{path}.additionalProperties must be boolean")
        schema["properties"] = {
            str(name): _compile(dict(child), f"{path}.properties.{name}", depth + 1, nodes)
            for name, child in props.items()
            if isinstance(child, Mapping)
        }
        if len(schema["properties"]) != len(props):
            raise SchemaError(f"{path}.properties values must be schemas")
        schema["required"] = list(required)
        schema["additionalProperties"] = additional
    elif kind == "array":
        items = schema.get("items")
        if not isinstance(items, Mapping):
            raise SchemaError(f"{path}.items must be a schema")
        schema["items"] = _compile(dict(items), f"{path}.items", depth + 1, nodes)
    elif kind == "string" and "format" in schema and schema["format"] not in _FORMATS:
        raise SchemaError(f"{path}.format must be one of {sorted(_FORMATS)}")
    return schema


def _validate(value: Any, schema: Mapping[str, Any], path: str, label: str) -> None:
    kind_value = schema.get("type")
    if kind_value is None:
        return
    kind = str(kind_value)
    valid = {
        "object": isinstance(value, Mapping),
        "array": isinstance(value, list),
        "string": isinstance(value, str),
        "integer": isinstance(value, int) and not isinstance(value, bool),
        "number": isinstance(value, (int, float)) and not isinstance(value, bool),
        "boolean": isinstance(value, bool),
        "null": value is None,
    }[kind]
    if not valid:
        raise SchemaValidationError(f"{label} {path}: expected {kind}, got {type(value).__name__}")
    if "const" in schema and value != schema["const"]:
        raise SchemaValidationError(f"{label} {path}: value does not match const")
    if "enum" in schema and value not in schema["enum"]:
        raise SchemaValidationError(f"{label} {path}: value is not in enum")
    if kind == "object":
        required = set(schema.get("required") or ())
        missing = sorted(required - set(value))
        if missing:
            raise SchemaValidationError(f"{label} {path}: missing required properties {missing}")
        props = schema.get("properties") or {}
        extra = sorted(set(value) - set(props))
        if extra and not schema.get("additionalProperties", False):
            raise SchemaValidationError(f"{label} {path}: unexpected properties {extra}")
        for name, child in props.items():
            if name in value:
                _validate(value[name], child, f"{path}.{name}", label)
    elif kind == "array":
        _bound(len(value), schema, "minItems", "maxItems", path, label)
        for index, item in enumerate(value):
            _validate(item, schema["items"], f"{path}[{index}]", label)
    elif kind == "string":
        _bound(len(value), schema, "minLength", "maxLength", path, label)
        fmt = schema.get("format")
        try:
            if fmt == "date": date.fromisoformat(value)
            elif fmt == "date-time": datetime.fromisoformat(value.replace("Z", "+00:00"))
        except ValueError as exc:
            raise SchemaValidationError(f"{label} {path}: invalid {fmt}") from exc
    elif kind in {"integer", "number"}:
        if "minimum" in schema and value < schema["minimum"]:
            raise SchemaValidationError(f"{label} {path}: below minimum")
        if "maximum" in schema and value > schema["maximum"]:
            raise SchemaValidationError(f"{label} {path}: above maximum")
        if "exclusiveMinimum" in schema and value <= schema["exclusiveMinimum"]:
            raise SchemaValidationError(f"{label} {path}: below exclusiveMinimum")
        if "exclusiveMaximum" in schema and value >= schema["exclusiveMaximum"]:
            raise SchemaValidationError(f"{label} {path}: above exclusiveMaximum")


def _bound(value: int, schema: Mapping[str, Any], low: str, high: str, path: str, label: str) -> None:
    if low in schema and value < int(schema[low]):
        raise SchemaValidationError(f"{label} {path}: shorter than {low}")
    if high in schema and value > int(schema[high]):
        raise SchemaValidationError(f"{label} {path}: longer than {high}")
