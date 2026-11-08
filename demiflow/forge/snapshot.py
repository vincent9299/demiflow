"""Bounded content snapshots for Forge project revisions."""
from __future__ import annotations

import hashlib
import os
from pathlib import Path

from .io import no_symlinks
from .schema import ForgeError, digest


def snapshot(project: Path, contract: dict) -> dict[str, str]:
    root = project.absolute()
    no_symlinks(root)
    if not root.is_dir():
        raise ForgeError(f"Project is not a directory: {root}")
    limits = contract["limits"]
    ignored = set(contract["layout"]["ignored_dirs"])
    runtime = set(contract["layout"]["runtime_dirs"])
    result = {}
    total = 0
    entries_seen = 0

    def visit(directory: Path, relative: str):
        nonlocal total, entries_seen
        with os.scandir(directory) as entries:
            for entry in entries:
                entries_seen += 1
                if entries_seen > limits["max_files"]:
                    raise ForgeError("Project has too many source entries")
                path = Path(entry.path)
                label = f"{relative}/{entry.name}" if relative else entry.name
                no_symlinks(path)
                if entry.is_dir(follow_symlinks=False):
                    if entry.name in ignored or (not relative and entry.name in runtime):
                        continue
                    visit(path, label)
                    continue
                if not entry.is_file(follow_symlinks=False):
                    raise ForgeError(f"Special file in project: {label}")
                before = path.stat()
                if before.st_size > limits["max_file_bytes"]:
                    raise ForgeError(f"Project file exceeds size limit: {label}")
                total += before.st_size
                if total > limits["max_total_bytes"]:
                    raise ForgeError("Project source bytes exceed contract limit")
                result[label] = _file_hash(path, before)

    visit(root, "")
    return result


def _file_hash(path: Path, before) -> str:
    result = hashlib.sha256()
    with path.open("rb") as handle:
        info = os.fstat(handle.fileno())
        if (info.st_dev, info.st_ino, info.st_size, info.st_mtime_ns) != (before.st_dev, before.st_ino, before.st_size, before.st_mtime_ns):
            raise ForgeError(f"File changed before hashing: {path}")
        while chunk := handle.read(1024 * 1024):
            result.update(chunk)
        info = os.fstat(handle.fileno())
        if (info.st_size, info.st_mtime_ns) != (before.st_size, before.st_mtime_ns):
            raise ForgeError(f"File changed during hashing: {path}")
    return result.hexdigest()


def revision(files: dict[str, str]) -> str:
    return digest(files)


def changes(before: dict[str, str], after: dict[str, str]) -> dict:
    return {
        "added": sorted(after.keys() - before.keys()),
        "removed": sorted(before.keys() - after.keys()),
        "changed": sorted(key for key in before.keys() & after.keys() if before[key] != after[key]),
    }


__all__ = ["changes", "revision", "snapshot"]
