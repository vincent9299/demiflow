"""Safe local JSON and durable file writes for Forge control files."""
from __future__ import annotations

import json
import os
from pathlib import Path
import uuid

from .schema import ForgeError


def no_symlinks(path: Path) -> Path:
    for part in (path, *path.parents):
        if part.is_symlink():
            raise ForgeError(f"Symlink in Forge path: {part}")
    return path


def read_json(path: Path, *, maximum: int = 2 * 1024 * 1024):
    no_symlinks(path)
    if not path.is_file():
        raise ForgeError(f"Expected regular JSON file: {path}")
    if path.stat().st_size > maximum:
        raise ForgeError(f"JSON file exceeds {maximum} bytes: {path}")
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (UnicodeError, json.JSONDecodeError) as exc:
        raise ForgeError(f"Invalid JSON: {path}: {exc}") from exc


def atomic_write_text(path: Path, value: str) -> None:
    no_symlinks(path.parent)
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.{os.getpid()}.{uuid.uuid4().hex}.tmp")
    try:
        with temporary.open("x", encoding="utf-8") as handle:
            handle.write(value)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, path)
        fsync_directory(path.parent)
    finally:
        temporary.unlink(missing_ok=True)


def atomic_write_json(path: Path, value) -> None:
    atomic_write_text(path, json.dumps(value, ensure_ascii=False, indent=2, sort_keys=True, allow_nan=False) + "\n")


def write_exclusive(path: Path, value: str) -> None:
    no_symlinks(path.parent)
    path.parent.mkdir(parents=True, exist_ok=True)
    data = value.encode("utf-8")
    fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    try:
        with os.fdopen(fd, "wb") as handle:
            handle.write(data)
            handle.flush()
            os.fsync(handle.fileno())
    except Exception:
        path.unlink(missing_ok=True)
        raise


def fsync_directory(path: Path) -> None:
    descriptor = os.open(path, os.O_RDONLY)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


__all__ = ["atomic_write_json", "atomic_write_text", "no_symlinks", "read_json", "write_exclusive"]
