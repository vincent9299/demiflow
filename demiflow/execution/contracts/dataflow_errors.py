"""Unified v3 dataflow error vocabulary (C01, cards C006/C075).

A single bounded, sanitized error type for the whole dataflow engine. The code
set is closed; ``UNKNOWN_EFFECT`` may never be marked retryable because an
unknown paid outcome must not be re-executed. Context carries only bounded
identity/location facts, is deep-frozen and detached from the caller, and is
rejected outright when it looks like a credential or oversized model body.
"""
from __future__ import annotations

import math
import re
from types import MappingProxyType
from typing import Mapping

DATAFLOW_ERROR_CODES = frozenset({
    "INVALID_CONTRACT",
    "UNSUPPORTED_CAPABILITY",
    "RESOURCE_INFEASIBLE",
    "CAPACITY_PENDING",
    "BUDGET_EXCEEDED",
    "SOURCE_CHANGED",
    "CODE_MISMATCH",
    "FENCED",
    "CANCELLED",
    "DEADLINE_EXCEEDED",
    "CORRUPT_ARTIFACT",
    "UNKNOWN_EFFECT",
    "INCOMPATIBLE_HISTORY",
    "INTERNAL_PROTOCOL",
})

_MAX_MESSAGE_CHARS = 2048
_MAX_CONTEXT_KEYS = 32
_MAX_CONTEXT_KEY_CHARS = 128
_MAX_CONTEXT_STRING_CHARS = 512
_MAX_CONTEXT_DEPTH = 8
_MAX_CONTEXT_NODES = 256
_MAX_CONTEXT_BYTES = 4096

_CONTEXT_KEY = re.compile(r"[A-Za-z0-9_.:\-]{1,128}\Z")
_CREDENTIAL = re.compile(
    r"(?i)(?:api[_-]?key|api[_-]?secret|secret[_-]?key|access[_-]?key|"
    r"private[_-]?key|password|passwd|authorization|bearer|token)"
    r"\s*[:=]\s*\S+"
)
_BEARER = re.compile(r"(?i)bearer\s+[A-Za-z0-9._\-]+")
_VENDOR_KEY = re.compile(r"sk-[A-Za-z0-9_\-]{16,}")
_SECRET_KEY_SUBSTR = re.compile(
    r"(?i)(?:password|passwd|secret|credential|authorization|private[_-]?key)"
)
_SECRET_KEY_EXACT = re.compile(
    r"(?i)^(?:token|pwd|api[_-]?key|access[_-]?key|auth[_-]?token|"
    r"access[_-]?token|refresh[_-]?token|session[_-]?token|bearer[_-]?token)$"
)


def _looks_secret(text: str) -> bool:
    return bool(_CREDENTIAL.search(text) or _BEARER.search(text) or _VENDOR_KEY.search(text))


def _secret_key(key: str) -> bool:
    return bool(_SECRET_KEY_SUBSTR.search(key) or _SECRET_KEY_EXACT.fullmatch(key))


def _freeze_context_value(value, depth: int, budget: list[int]):
    if depth > _MAX_CONTEXT_DEPTH:
        raise ValueError("error context exceeds depth budget")
    budget[0] += 1
    if budget[0] > _MAX_CONTEXT_NODES:
        raise ValueError("error context exceeds node budget")
    if value is None or type(value) is bool:
        return value
    if type(value) is int:
        return value
    if type(value) is float:
        if not math.isfinite(value):
            raise ValueError("error context must not carry nonfinite numbers")
        return value
    if type(value) is str:
        if len(value) > _MAX_CONTEXT_STRING_CHARS:
            raise ValueError("error context string exceeds bound")
        if _looks_secret(value):
            raise ValueError("error context must not carry credentials or model content")
        budget[1] += len(value.encode("utf-8"))
        return value
    if type(value) is dict:
        frozen = {}
        for key, item in value.items():
            if type(key) is not str or not _CONTEXT_KEY.fullmatch(key):
                raise ValueError("error context keys must be bounded identifiers")
            if _secret_key(key):
                raise ValueError("error context must not carry credential-named keys")
            frozen[key] = _freeze_context_value(item, depth + 1, budget)
        return MappingProxyType(frozen)
    if type(value) is list:
        return tuple(_freeze_context_value(item, depth + 1, budget) for item in value)
    raise ValueError("error context values must be finite Json")


def _freeze_context(context) -> Mapping[str, object]:
    if context is None:
        return MappingProxyType({})
    if not isinstance(context, Mapping):
        raise ValueError("error context must be a mapping")
    if len(context) > _MAX_CONTEXT_KEYS:
        raise ValueError("error context exceeds key budget")
    budget = [0, 0]
    frozen = {}
    for key, value in context.items():
        if type(key) is not str or not _CONTEXT_KEY.fullmatch(key):
            raise ValueError("error context keys must be bounded identifiers")
        if _secret_key(key):
            raise ValueError("error context must not carry credential-named keys")
        frozen[key] = _freeze_context_value(value, 1, budget)
    if budget[1] > _MAX_CONTEXT_BYTES:
        raise ValueError("error context exceeds byte budget")
    return MappingProxyType(frozen)


def _thaw(value):
    if isinstance(value, MappingProxyType):
        return {key: _thaw(item) for key, item in value.items()}
    if type(value) is tuple:
        return [_thaw(item) for item in value]
    return value


def _reconstruct_error(code, message, retryable, context):
    return DataflowError(code, message, retryable=retryable, context=context)


class DataflowError(Exception):
    """Bounded engine error with a closed code and deep-frozen context."""

    def __init__(
        self,
        code: str,
        message: str,
        *,
        retryable: bool = False,
        context: Mapping[str, object] | None = None,
    ) -> None:
        if type(code) is not str or code not in DATAFLOW_ERROR_CODES:
            raise ValueError("unknown dataflow error code: " + repr(code))
        if type(message) is not str or not 1 <= len(message) <= _MAX_MESSAGE_CHARS:
            raise ValueError("error message must be a bounded nonempty string")
        if _looks_secret(message):
            raise ValueError("error message must not carry credentials or model content")
        if type(retryable) is not bool:
            raise ValueError("retryable must be a bool")
        if code in {"UNKNOWN_EFFECT", "BUDGET_EXCEEDED"} and retryable:
            raise ValueError(code + " must not be retryable")
        frozen = _freeze_context(context)
        self.code = code
        self.retryable = retryable
        self.context = frozen
        self._message = message
        super().__init__(code + ": " + message)

    def __reduce__(self):
        return (
            _reconstruct_error,
            (self.code, self._message, self.retryable, _thaw(self.context)),
        )

from ..recovery_contract import RecoveryContractError, UnresolvedCommit


class DataflowRecoveryError(DataflowError, RecoveryContractError):
    """A native recovery rejection, preserving both old and v3 catch surfaces."""


def _reconstruct_unresolved(message, context):
    return DataflowUnresolvedCommit(message, context=context)


class DataflowUnresolvedCommit(DataflowError, UnresolvedCommit):
    """Unknown native publication: catchable by both APIs, never retryable."""

    def __init__(self, message, *, context=None):
        super().__init__('UNKNOWN_EFFECT', message, retryable=False, context=context)

    def __reduce__(self):
        return _reconstruct_unresolved, (self._message, _thaw(self.context))
