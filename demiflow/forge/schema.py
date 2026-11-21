"""Structured errors and canonical JSON identity for Forge and the workbench."""
from __future__ import annotations

import hashlib
import json
from typing import Any


class ForgeError(ValueError):
    CODES = frozenset({'invalid_request', 'not_found', 'conflict', 'inputs_changed', 'inspection_incomplete', 'busy',
                       'resource_limit', 'operation_rejected', 'operation_failed'})

    def __init__(self, message, *, code='operation_rejected'):
        if code not in self.CODES:
            raise ValueError('Unknown Forge error code')
        self.code = code
        super().__init__(message)


def canonical(value: Any) -> bytes:
    return json.dumps(
        value, ensure_ascii=False, sort_keys=True, separators=(",", ":"),
        allow_nan=False,
    ).encode("utf-8")


def digest(value: Any) -> str:
    return hashlib.sha256(canonical(value)).hexdigest()


def required_text(value: Any, label: str, *, maximum: int = 100_000) -> str:
    if not isinstance(value, str) or not value.strip() or len(value.encode("utf-8")) > maximum:
        raise ForgeError(f"{label} must be non-empty text within {maximum} bytes")
    return value
