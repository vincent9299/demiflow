"""Pure signature binding shared by native callables and static authoring tools."""

from __future__ import annotations

import inspect
import math


def default_value(value):
    """Describe a bounded literal default without evaluating repr or user code.

    Display signatures are deliberately not a serialization format. An opaque
    default remains unknown; callers can still supply that parameter explicitly.
    """
    remaining = 4096
    byte_budget = 16384

    def encode(item, depth):
        nonlocal remaining, byte_budget
        remaining -= 1
        if depth > 16 or remaining < 0:
            raise ValueError("Default exceeds structural budget")
        kind = type(item)
        if item is None or kind in (bool, int, float, str):
            if kind is float and not math.isfinite(item):
                raise ValueError("Nonfinite default")
            if kind is int and item.bit_length() > 4096:
                raise ValueError("Integer default exceeds budget")
            if kind is str:
                if len(item) > byte_budget:
                    raise ValueError("Default exceeds byte budget")
                byte_budget -= len(item.encode("utf-8"))
            if byte_budget < 0:
                raise ValueError("Default exceeds byte budget")
            return {"kind": "literal", "value": item}
        if kind is bytes:
            byte_budget -= len(item) * 2
            if byte_budget < 0:
                raise ValueError("Default exceeds byte budget")
            return {"kind": "bytes", "hex": item.hex()}
        if kind in (tuple, list):
            if len(item) > remaining:
                raise ValueError("Default exceeds item budget")
            return {"kind": kind.__name__, "items": [encode(v, depth + 1) for v in item]}
        if kind is dict and all(type(k) is str for k in item):
            if len(item) > remaining:
                raise ValueError("Default exceeds item budget")
            return {"kind": "dict", "items": [[encode(k, depth + 1), encode(v, depth + 1)] for k, v in item.items()]}
        raise ValueError("Default is not a literal")

    try:
        return {"status": "known", "value": encode(value, 0)}
    except ValueError as exc:
        return {"status": "opaque", "reason": str(exc)}


def decode_default(document):
    """Read only the machine representation; never guess a displayed repr."""
    remaining = 4096

    def decode(item, depth):
        nonlocal remaining
        remaining -= 1
        if depth > 16 or remaining < 0 or type(item) is not dict:
            raise ValueError("Invalid default value")
        kind = item.get("kind")
        if kind == "literal" and set(item) == {"kind", "value"}:
            value = item["value"]
            if value is None or type(value) in (bool, int, float, str):
                return value
        elif kind == "bytes" and set(item) == {"kind", "hex"} and type(item["hex"]) is str:
            if len(item["hex"]) <= 16384:
                return bytes.fromhex(item["hex"])
        elif kind in {"tuple", "list", "dict"} and set(item) == {"kind", "items"}:
            items = item["items"]
            if type(items) is not list or len(items) > remaining:
                raise ValueError("Invalid default container")
            if kind == "dict":
                result = {}
                for pair in items:
                    if type(pair) is not list or len(pair) != 2:
                        raise ValueError("Invalid default mapping")
                    key, value = (decode(v, depth + 1) for v in pair)
                    if type(key) is not str or key in result:
                        raise ValueError("Invalid default key")
                    result[key] = value
                return result
            values = [decode(v, depth + 1) for v in items]
            return tuple(values) if kind == "tuple" else values
        raise ValueError("Invalid default representation")

    if type(document) is not dict or document.get("status") != "known":
        raise ValueError("Opaque default requires an explicit argument")
    value = decode(document["value"], 0)
    if default_value(value) != document:
        raise ValueError("Invalid or oversized default representation")
    return value


def bind_native_parameters(parameters, args, kwargs):
    values = bind_parameters(parameters, args, kwargs)
    for parameter in parameters:
        name, kind = parameter["name"], parameter["kind"]
        if name in values:
            continue
        if kind in {"VAR_POSITIONAL", "VAR_KEYWORD"}:
            values[name] = () if kind == "VAR_POSITIONAL" else {}
        elif not parameter["required"]:
            try:
                values[name] = decode_default(parameter.get("default_value"))
            except ValueError as exc:
                raise ValueError(f"{name}: {exc}") from exc
    return values


def signature_parameters(signature, *, drop_self=False):
    return [
        {
            "name": p.name,
            "kind": p.kind.name,
            "required": p.default is p.empty
            and p.kind not in (p.VAR_POSITIONAL, p.VAR_KEYWORD),
        }
        for p in signature.parameters.values()
        if not (drop_self and p.name == "self")
    ]


def validate_field_binding(parameters, inputs, fixed, *, label="callable"):
    """Validate facts, without importing, constructing or invoking a callable."""
    bindable, required = set(), set()
    for parameter in parameters:
        name, kind = parameter["name"], parameter["kind"]
        if name in {"row", "raw", "record", "_row"}:
            raise TypeError(f"{label}: field-bound map fn must not accept {name!r}")
        if kind in {"VAR_POSITIONAL", "VAR_KEYWORD", "POSITIONAL_ONLY"}:
            detail = {
                "VAR_POSITIONAL": "accept *args",
                "VAR_KEYWORD": "accept **kwargs",
                "POSITIONAL_ONLY": "use positional-only parameters",
            }[kind]
            raise TypeError(f"{label}: field-bound map fn must not {detail}")
        bindable.add(name)
        if parameter["required"]:
            required.add(name)
    unknown = set(fixed) - bindable
    if unknown:
        raise TypeError(
            f"{label}: fn_kwargs bind unknown parameter(s): {sorted(unknown)}"
        )
    overlap = set(inputs) & set(fixed)
    if overlap:
        raise TypeError(
            f"{label}: parameters cannot be bound by both inputs and fn_kwargs: {sorted(overlap)}"
        )
    unknown = set(inputs) - bindable
    if unknown:
        raise TypeError(f"{label}: inputs bind unknown parameter(s): {sorted(unknown)}")
    missing = required - set(inputs) - set(fixed)
    if missing:
        raise TypeError(f"{label}: missing input binding(s): {sorted(missing)}")


def bind_parameters(parameters, args, kwargs, *, defaults=None):
    """Bind already parsed values using Python's signature rules, without eval."""
    sentinel = object()
    signature = inspect.Signature(
        [
            inspect.Parameter(
                p["name"],
                getattr(inspect.Parameter, p["kind"]),
                default=(
                    inspect.Parameter.empty
                    if p["required"] or p["kind"] in {"VAR_POSITIONAL", "VAR_KEYWORD"}
                    else sentinel
                ),
            )
            for p in parameters
        ]
    )
    bound = signature.bind(*args, **kwargs)
    result = dict(defaults or {})
    result.update(bound.arguments)
    return result
