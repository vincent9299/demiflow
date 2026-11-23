"""Ordering implementation."""
from __future__ import annotations

import hashlib
from demiflow.execution.contracts.record_codec import canonical_record_json as _canonical


SHUFFLE_DOMAIN = "demiflow-shuffle-v3"

def shuffle_key_bytes(record_id: str, *, seed: int, occurrence_id: str) -> bytes:
    # Single versioned formula; ordering.shuffle_key is the public wrapper.
    return hashlib.sha256(
        _canonical([SHUFFLE_DOMAIN, seed, occurrence_id, record_id]).encode("utf-8")
    ).digest()
