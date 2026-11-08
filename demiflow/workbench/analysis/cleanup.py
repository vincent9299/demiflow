"""Bounded, nonce-scoped private IO cleanup AFTER the runtime process witness.

No recursive deletion of user paths, ready objects, pipeline history or unknown
files. Unexpected links/types/ownership retain the lease and charge. All paths
originate in this helper's existing DB/Runtime lease, never an HTTP body.
"""
from __future__ import annotations

import os
from pathlib import Path
import stat

from demiflow.forge.io import no_symlinks
from demiflow.workbench.analysis import contracts as c
from demiflow.workbench.analysis.oci import object_id


def _sync(path):
    fd = os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_DIRECTORY)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


def remove_tree(target, *, maximum_entries, maximum_bytes, witness):
    """Internal bounded owned tree; witness must check actual original job IO."""
    if not callable(witness) or witness() is not True:
        c.reject("Owned process/mount cleanup has not been proved", code="conflict")
    target = no_symlinks(Path(target))
    if not target.exists():
        return True
    files, directories, pending = [], [], [(target, 0)]
    entries = total = 0
    while pending:
        directory, depth = pending.pop()
        info = directory.stat()
        if not stat.S_ISDIR(info.st_mode) or info.st_uid != os.geteuid() or depth > 10:
            c.reject("Owned cleanup directory type/owner/depth differs", code="conflict")
        directories.append(directory)
        with os.scandir(directory) as children:
            for item in children:
                entries += 1
                if entries > maximum_entries or item.is_symlink():
                    c.reject("Owned cleanup entry limit exceeded or link found", code="conflict")
                if item.is_dir(follow_symlinks=False):
                    pending.append((Path(item.path), depth+1)); continue
                value = item.stat(follow_symlinks=False)
                if not stat.S_ISREG(value.st_mode) or value.st_nlink != 1 or value.st_uid != os.geteuid():
                    c.reject("Owned cleanup file type/owner differs", code="conflict")
                total += max(value.st_size, value.st_blocks*512)
                if total > maximum_bytes:
                    c.reject("Owned cleanup exceeds its admitted physical byte budget", code="resource_limit")
                files.append(Path(item.path))
    if witness() is not True:
        c.reject("Owned cleanup witness changed before deletion", code="conflict")
    for directory in directories:
        directory.chmod(0o700)
    for path in files:
        path.unlink()
    for directory in reversed(directories):
        _sync(directory); directory.rmdir()
    _sync(target.parent)
    return not target.exists()


def incomplete_object(artifacts, state_root, project, identifier, *, witness):
    """Keep every ready object; only a pending owned reservation may be freed."""
    from demiflow.workbench.analysis.artifacts import _row
    object_id(identifier)
    with artifacts.database.locked() as db:
        row = _row(db, project, identifier)
        if row["state"] == "ready":
            return True  # committed fact retained, no deletion or free
        if row["state"] != "cleanup_pending":
            c.reject("Unready artifact needs its original cleanup intent", code="conflict")
        publish_id = c.identity(row["publish_id"])
    state_root = Path(state_root)
    stage, target = state_root/".staging"/publish_id, state_root/"objects"/identifier
    if target.exists():
        # Renamed objects must have the original canonical seed, even if DB
        # ready was never committed. Do not adopt an arbitrary directory.
        from demiflow.workbench.analysis.deployment import _read
        manifest = c.decode(_read(target/"manifest.json"))
        if manifest.get("object_id") != identifier or manifest.get("project") != project:
            c.reject("Unready renamed object identity differs", code="conflict")
    for path in (stage, target):
        remove_tree(path, maximum_entries=128, maximum_bytes=128*1024*1024, witness=witness)
    return not stage.exists() and not target.exists() and witness() is True
