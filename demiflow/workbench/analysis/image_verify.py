"""Read-only, bounded per-file verification of a concrete worker image.

A matching dependency-lock filename is insufficient. This hashes every regular
file, refuses extra files/links/devices and checks immutable modes/ownership.
It executes no loader, module, image Python or runtime and proves no isolation.
"""
from __future__ import annotations

import hashlib
import json
import os
from pathlib import Path
from pathlib import PurePosixPath
import stat

from demiflow.forge.io import no_symlinks
from demiflow.workbench.analysis import contracts as c


def verify_rootfs(rootfs, lock, *, allowed_owners):
    rootfs = no_symlinks(Path(rootfs))
    if type(lock) is not dict or lock.get("format") != "demiflow_analysis_image_lock_v1":
        c.reject("Unsupported worker dependency lock")
    files = lock.get("files")
    if type(files) is not dict or not 1 <= len(files) <= 32768:
        c.reject("Worker image file count exceeds release limit", code="resource_limit")
    c.sha(lock.get("image_tree_sha256"), "worker image tree")
    total = 0
    tree = hashlib.sha256(b"{")
    for count, (name, item) in enumerate(sorted(files.items())):
        c.text(name, "image relative file", 4096)
        path = PurePosixPath(name)
        if path.is_absolute() or len(path.parts) > 64 or str(path) != name or any(part in {".", ".."} for part in path.parts):
            c.reject("Invalid relative worker image file")
        c.fields(item, {"sha256", "bytes", "mode", "source", "provenance"}, label="locked image file")
        c.sha(item["sha256"]); c.integer(item["bytes"], "locked file size", 512*1024*1024)
        if type(item["mode"]) is not int or item["mode"] not in {0o444, 0o555}:
            c.reject("Worker image has mutable or unsupported file modes")
        total += item["bytes"]
        if total > 2*1024**3:
            c.reject("Worker image exceeds its release byte bound", code="resource_limit")
        if count:
            tree.update(b",")
        tree.update(json.dumps(name, ensure_ascii=True).encode("ascii")); tree.update(b":")
        tree.update(json.dumps({key: item[key] for key in ("sha256", "bytes", "mode")}, sort_keys=True, separators=(",", ":")).encode("ascii"))
    tree.update(b"}")
    if lock.get("files_count") != len(files) or lock.get("bytes") != total or tree.hexdigest() != lock["image_tree_sha256"]:
        c.reject("Worker image lock content/tree receipt differs", code="conflict")
    observed = set()
    entries = directories = 0
    pending = [(rootfs, 0)]
    while pending:
        current, depth = pending.pop()
        info = current.stat()
        if not stat.S_ISDIR(info.st_mode) or stat.S_IMODE(info.st_mode) != 0o555 or info.st_uid not in allowed_owners:
            c.reject("Worker image directories must be owned immutable physical directories")
        directories += 1
        if directories > 65536 or depth > 64:
            c.reject("Worker image directory limit exceeded", code="resource_limit")
        with os.scandir(current) as children:
            for child in children:
                entries += 1
                if entries > 98304 or child.is_symlink():
                    c.reject("Worker image entry limit exceeded or symlink present", code="resource_limit")
                if child.is_dir(follow_symlinks=False):
                    pending.append((Path(child.path), depth + 1))
                    continue
                name = Path(child.path).relative_to(rootfs).as_posix()
                if name not in files:
                    c.reject("Worker image contains an unreviewed file", code="conflict")
                expected = files[name]
                descriptor = os.open(child.path, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
                with os.fdopen(descriptor, "rb") as source:
                    before = os.fstat(source.fileno())
                    if (not stat.S_ISREG(before.st_mode) or before.st_nlink != 1 or before.st_uid not in allowed_owners
                            or stat.S_IMODE(before.st_mode) != expected["mode"] or before.st_size != expected["bytes"]):
                        c.reject("Worker image file type/mode/ownership/size differs", code="conflict")
                    content = hashlib.sha256()
                    for chunk in iter(lambda: source.read(1024*1024), b""):
                        content.update(chunk)
                    after = os.fstat(source.fileno())
                if (before.st_ino, before.st_size, before.st_mtime_ns, before.st_ctime_ns) != (after.st_ino, after.st_size, after.st_mtime_ns, after.st_ctime_ns) or content.hexdigest() != expected["sha256"]:
                    c.reject("Worker image file content changed or differs", code="conflict")
                observed.add(name)
    if len(observed) != len(files):
        c.reject("Worker image is incomplete", code="conflict")
    return dict(files=len(files), bytes=total, directories=directories, image_tree_sha256=tree.hexdigest(),
                image_executed=False, isolation_verified=False)
