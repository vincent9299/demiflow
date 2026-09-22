"""Atomic text projections of pinned Lance snapshots for file consumers.

The caller owns the projection and its versioned name. The platform owns cache
identity, writer exclusion, temporary-file cleanup and atomic publication.
"""
from __future__ import annotations

import fcntl
import hashlib
import json
import os
from pathlib import Path
import uuid


def export_snapshot(dataset, cache_dir, *, projection: str, suffix: str, write) -> Path:
    identity = {"uri": dataset.uri, "version": dataset.version, "projection": projection}
    key = hashlib.sha256(json.dumps(identity, sort_keys=True).encode()).hexdigest()
    if not suffix.startswith(".") or "/" in suffix or "\\" in suffix:
        raise ValueError("suffix must be a filename extension")
    target = Path(cache_dir) / (key + suffix)
    target.parent.mkdir(parents=True, exist_ok=True)
    with target.with_suffix(target.suffix + ".lock").open("a") as lock:
        fcntl.flock(lock, fcntl.LOCK_EX)
        if target.is_file():
            return target
        partial = target.with_name(target.name + "." + uuid.uuid4().hex + ".partial")
        try:
            with partial.open("w", encoding="utf-8") as handle:
                write(handle)
                handle.flush()
                os.fsync(handle.fileno())
            os.replace(partial, target)
        finally:
            partial.unlink(missing_ok=True)
    return target
