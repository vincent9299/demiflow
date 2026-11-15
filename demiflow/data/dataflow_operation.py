"""Lazy native operation declarations for the unified v3 dataflow.

A NativeOperation carries only a kind, pure Json configuration/bindings and a
CallableSpec factory. It never holds a live client, model, journal, lock or
file descriptor; those are constructed inside workers from the sealed program.
"""
from __future__ import annotations

import math
from dataclasses import dataclass
from types import MappingProxyType
from typing import Any, Mapping

from .plan import CallableSpec, LogicalOp

_MAX_JSON_DEPTH = 32
_MAX_JSON_NODES = 65536
_MAX_JSON_STRING_BYTES = 1024 * 1024


def _freeze_json(value: Any, depth: int = 0, budget: list[int] | None = None) -> Any:
    """Validate and deep-freeze a strict Json declaration value.

    Tuples, bytes, sets and arbitrary objects are rejected: business Record
    codecs may preserve them, but a native declaration is identity-bearing Json.
    """
    if budget is None:
        budget = [_MAX_JSON_NODES]
    budget[0] -= 1
    if budget[0] < 0:
        raise ValueError("native operation declaration exceeds node budget")
    if depth > _MAX_JSON_DEPTH:
        raise ValueError("native operation declaration exceeds depth budget")
    if value is None or type(value) in (bool, int):
        return value
    if type(value) is float:
        if not math.isfinite(value):
            raise ValueError("native operation declaration forbids NaN/Infinity")
        return value
    if type(value) is str:
        if len(value.encode("utf-8")) > _MAX_JSON_STRING_BYTES:
            raise ValueError("native operation declaration string exceeds byte budget")
        return value
    if type(value) is dict:
        frozen = {}
        for key, item in value.items():
            if type(key) is not str:
                raise ValueError("native operation declaration keys must be strings")
            frozen[key] = _freeze_json(item, depth + 1, budget)
        return MappingProxyType(frozen)
    if type(value) is list:
        return tuple(_freeze_json(item, depth + 1, budget) for item in value)
    raise ValueError(
        "native operation declaration must be strict Json, got " + type(value).__name__
    )


@dataclass(frozen=True)
class NativeOperation(LogicalOp):
    """A pure, lazily constructed native node declaration.

    kind selects the registered native operator; configuration and bindings are
    deep-frozen Json; factory is a CallableSpec assembled by the worker, never an
    already-constructed actor.
    """

    kind: str
    configuration: Mapping[str, Any]
    bindings: Mapping[str, Any]
    factory: CallableSpec

    def __post_init__(self) -> None:
        if type(self.kind) is not str or not self.kind or len(self.kind) > 128:
            raise ValueError("native operation kind must be a non-empty bounded string")
        if not isinstance(self.factory, CallableSpec):
            raise TypeError("native operation factory must be a CallableSpec")
        for name in ("configuration", "bindings"):
            value = getattr(self, name)
            if not isinstance(value, Mapping):
                raise TypeError(f"native operation {name} must be a mapping")
            frozen = _freeze_json(dict(value))
            if not isinstance(frozen, MappingProxyType):
                raise TypeError(f"native operation {name} must be a Json object")
            object.__setattr__(self, name, frozen)