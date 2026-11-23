"""Native diagnostics implementation."""
from __future__ import annotations

import json
import os

FILE_BYTES = 64 * 1024


def write_document(path, value):
    raw = bytearray()
    for chunk in json.JSONEncoder(ensure_ascii=False, allow_nan=False).iterencode(
        value
    ):
        encoded = chunk.encode()
        if len(raw) + len(encoded) > FILE_BYTES:
            value = {"diagnostics_truncated": True, "reason": "document_byte_budget"}
            raw = bytearray(json.dumps(value).encode())
            break
        raw.extend(encoded)
    temporary = path.with_suffix(".tmp")
    try:
        temporary.write_bytes(raw)
        os.replace(temporary, path)
    finally:
        temporary.unlink(missing_ok=True)
