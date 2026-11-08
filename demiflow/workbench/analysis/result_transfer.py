"""Stream completed native files to the separate, quota-owning result receiver.

Only a trusted native/media role is given /broker/result.sock. The kernel's
data socket cannot grant native file publication. Paths here are relative to a
completed, owned scratch result; no receiving API follows a host path.
"""
from __future__ import annotations

from demiflow.workbench.analysis.job_context import job_path
from demiflow.workbench.analysis.job_context import current

from contextlib import closing
import hashlib
import os
from pathlib import Path
from pathlib import PurePosixPath
import re
import socket
import stat

from demiflow.forge.io import no_symlinks
from demiflow.workbench.analysis import contracts as c
from demiflow.workbench.analysis.arrow_bridge import CHUNK_BYTES
from demiflow.workbench.analysis.arrow_bridge import MAX_ENCODED_BYTES
from demiflow.workbench.analysis.framing import Frames

_COMPONENT = re.compile(r"[A-Za-z0-9_.-]{1,128}\Z")


def relative_file(value):
    c.text(value, "native result file", 1024)
    path = PurePosixPath(value)
    if path.is_absolute() or len(path.parts) > 8 or str(path) != value or any(part in {".", ".."} or not _COMPONENT.fullmatch(part) for part in path.parts):
        c.reject("Invalid relative native result file")
    return value


def _file_manifest(path, available):
    """Hash the one opened regular file within its admitted byte count.

    DirEntry metadata can precede the filesystem's completed close/write
    metadata. Use the actual opened descriptor on both sides of the hash,
    then verify the path still names that exact file. Never follow a swapped
    symlink or read an extending file until an unbounded EOF.
    """
    descriptor=None
    def identity(info):
        return (info.st_dev,info.st_ino,info.st_size,info.st_mtime_ns,info.st_ctime_ns,info.st_nlink)
    try:
        descriptor=os.open(path,os.O_RDONLY|os.O_NOFOLLOW|os.O_NONBLOCK)
        info=os.fstat(descriptor)
        if not stat.S_ISREG(info.st_mode) or info.st_nlink!=1:
            c.reject("Native result requires owned regular files")
        if info.st_size>available:
            c.reject("Native result exceeds publication byte/file limit",code="resource_limit")
        digest=hashlib.sha256();remaining=info.st_size
        while remaining:
            chunk=os.read(descriptor,min(CHUNK_BYTES,remaining))
            if not chunk:c.reject("Native result changed during publication",code="conflict")
            digest.update(chunk);remaining-=len(chunk)
        if os.read(descriptor,1):
            c.reject("Native result changed during publication",code="conflict")
        if (identity(os.fstat(descriptor))!=identity(info)
                or identity(path.stat(follow_symlinks=False))!=identity(info)):
            c.reject("Native result changed during publication",code="conflict")
        return dict(bytes=info.st_size,sha256=digest.hexdigest())
    except OSError:
        c.reject("Native result changed or became unavailable during publication",code="conflict")
    finally:
        if descriptor is not None:os.close(descriptor)


def files_manifest(directory):
    directory = no_symlinks(Path(directory))
    result = {}
    total = 0
    pending = [(directory, 0)]
    entries = 0
    while pending:
        current, depth = pending.pop()
        if depth > 8:
            c.reject("Native result directory depth exceeds limit", code="resource_limit")
        with os.scandir(current) as scan:
            for item in scan:
                entries += 1
                if entries > 128:
                    c.reject("Native result directory entry limit exceeded", code="resource_limit")
                if item.is_symlink():
                    c.reject("Native result cannot contain a symlink")
                if item.is_dir(follow_symlinks=False):
                    pending.append((Path(item.path), depth + 1))
                    continue
                if len(result) >= 64:
                    c.reject("Native result exceeds publication byte/file limit", code="resource_limit")
                relative = relative_file(Path(item.path).relative_to(directory).as_posix())
                result[relative] = _file_manifest(Path(item.path),MAX_ENCODED_BYTES-total)
                total += result[relative]['bytes']
    if not result:
        c.reject("Native result has no committed files")
    c.encoded(result)
    return dict(sorted(result.items()))


def send_result(directory, *, publish_id, description):
    c.identity(publish_id, "owned publish ID")
    c.encoded(description)
    files = files_manifest(directory)
    candidate = dict(publish_id=publish_id, description=description, files=files)
    connection = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    connection.settimeout(40)
    try:
        with closing(connection):
            connection.connect(job_path("/broker/result.sock"))
            frames = Frames(connection)
            frames.send("json", dict(version=1, operation="native-result", candidate=candidate))
            if frames.json() != {"accepted": True}:
                c.reject("Native result receiver did not admit publication", code="operation_rejected")
            for relative, metadata in files.items():
                frames.send("json", dict(file=relative, bytes=metadata["bytes"]))
                digest = hashlib.sha256(); count = 0
                with no_symlinks(Path(directory)/relative).open("rb") as source:
                    for chunk in iter(lambda: source.read(CHUNK_BYTES), b""):
                        if count + len(chunk) > metadata["bytes"]:
                            c.reject("Native result grew during transfer", code="conflict")
                        count += len(chunk); digest.update(chunk)
                        frames.send("blob", chunk)
                if count != metadata["bytes"] or digest.hexdigest() != metadata["sha256"]:
                    c.reject("Native result changed during transfer", code="conflict")
                frames.send("end")
            frames.send("end")
            reply = frames.json()
            c.fields(reply, {"committed", "manifest_digest"}, label="native publication receipt")
            if reply["committed"] is not True:
                c.reject("Native result publication is not confirmed", code="operation_failed")
            c.sha(reply["manifest_digest"], "committed result digest")
            return reply
    except OSError as exc:
        c.reject("Native result receipt is unknown for publish_id=" + publish_id + ": " + type(exc).__name__, code="operation_failed")
