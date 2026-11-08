"""Helper-owned upload spools; opaque IPC bytes, no controller Arrow decoding."""
from __future__ import annotations

import hashlib
import os
from pathlib import Path
import re
import stat
import struct

from demiflow.forge.io import no_symlinks
from demiflow.workbench.analysis import contracts as c
from demiflow.workbench.analysis.database import private_directory
from demiflow.workbench.analysis.materialize import materialize_config


def input_id(value):
    if type(value) is not str or not re.fullmatch(r"i[0-9a-f]{32}", value):
        c.reject("Invalid owned Arrow ingress nonce")
    return value


def _sync(path):
    fd = os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_DIRECTORY)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


class Upload:
    def __init__(self, database, state_root, config, *, authorize):
        materialize_config(config)
        if not callable(authorize) or authorize() is not True:
            c.reject("Current upload permission is absent", code="operation_rejected")
        self.database, self.config, self.authorize = database, config, authorize
        root = private_directory(Path(state_root)/"transfer")
        self.parent = private_directory(root/"ingress")
        self.path = self.parent/input_id(config["input_id"])
        self.path.mkdir(mode=0o700, exist_ok=False)
        self.descriptor = os.open(self.path/"data.bin", os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600)
        self.bytes = self.chunks = 0
        self.digest = hashlib.sha256()
        self.sealed = False

    def consume(self, kind, value):
        if self.sealed or self.descriptor is None or self.authorize() is not True:
            c.reject("Upload stopped or its permission was revoked", code="operation_rejected")
        if kind == "arrow":
            if type(value) is not bytes or not 1 <= len(value) <= 1024*1024:
                c.reject("Arrow upload frame exceeds byte limit", code="resource_limit")
            prefix = struct.pack("!I", len(value))
            if self.bytes+4+len(value) > self.config["encoded_bytes"] or self.chunks >= self.config["chunks"]:
                c.reject("Arrow stream exceeds its admitted commitment", code="resource_limit")
            with self.database.guard.object_write():
                for part in (prefix, value):
                    remaining = memoryview(part)
                    while remaining:
                        count = os.write(self.descriptor, remaining)
                        if count <= 0:
                            c.reject("Arrow ingress write made no progress", code="operation_failed")
                        remaining = remaining[count:]
            self.digest.update(prefix); self.digest.update(value)
            self.bytes += 4+len(value); self.chunks += 1
            return None
        if kind != "end":
            c.reject("Arrow upload requires Arrow frames and an end frame")
        if self.bytes != self.config["encoded_bytes"] or self.chunks != self.config["chunks"] or self.digest.hexdigest() != self.config["stream_sha256"]:
            c.reject("Arrow stream differs from its complete content commitment", code="conflict")
        os.fsync(self.descriptor); os.fchmod(self.descriptor, 0o400); os.close(self.descriptor); self.descriptor = None
        _sync(self.path); self.path.chmod(0o500); _sync(self.parent)
        if self.authorize() is not True:
            c.reject("Upload permission was revoked before materializer admission", code="operation_rejected")
        self.sealed = True
        return dict(input_id=self.config["input_id"], sealed=True, bytes=self.bytes, stream_sha256=self.digest.hexdigest())

    def close(self):
        if self.descriptor is not None:
            os.close(self.descriptor); self.descriptor = None
        # The quota remains held until owned-job/file cleanup is proved.


class ExportCopy:
    """One bounded opaque frame per controller turn from the admitted outbox."""
    def __init__(self,database,state_root,outbox_root,config,*,web_uid,helper_gid,authorize):
        materialize_config(config)
        self.config=config
        path=no_symlinks(Path(outbox_root)/input_id(config["input_id"])/"data.bin")
        self.fd=os.open(path,os.O_RDONLY|os.O_NOFOLLOW|os.O_NONBLOCK)
        self.upload=None
        try:
            self.before=os.fstat(self.fd)
            if (not stat.S_ISREG(self.before.st_mode) or self.before.st_nlink!=1 or self.before.st_uid!=web_uid
                    or self.before.st_gid!=helper_gid or stat.S_IMODE(self.before.st_mode)!=0o440 or self.before.st_size!=config["encoded_bytes"]):
                c.reject("Sealed source export file identity/size differs",code="conflict")
            self.upload=Upload(database,state_root,config,authorize=authorize)
        except BaseException:
            self.close();raise

    def _read(self,size):
        result=bytearray(size);position=0
        while position<size:
            raw=os.read(self.fd,size-position)
            if not raw:c.reject("Source export has an incomplete frame",code="conflict")
            result[position:position+len(raw)]=raw;position+=len(raw)
        return bytes(result)

    def step(self):
        if self.fd is None or self.upload is None:c.reject("Original source copy is closed",code="conflict")
        if self.upload.chunks<self.config["chunks"]:
            prefix=self._read(4);size=struct.unpack("!I",prefix)[0]
            if not 1<=size<=1024*1024 or self.upload.bytes+4+size>self.config["encoded_bytes"]:
                c.reject("Source export frame exceeds its admitted commitment",code="resource_limit")
            self.upload.consume("arrow",self._read(size))
            return False
        if os.read(self.fd,1):c.reject("Source export exceeds its committed frame count",code="conflict")
        after=os.fstat(self.fd)
        if (self.before.st_ino,self.before.st_dev,self.before.st_size,self.before.st_mtime_ns,self.before.st_ctime_ns)!=(after.st_ino,after.st_dev,after.st_size,after.st_mtime_ns,after.st_ctime_ns):
            c.reject("Sealed source export changed during copy",code="conflict")
        self.upload.consume("end",None)
        os.close(self.fd);self.fd=None
        return True

    def close(self):
        if self.fd is not None:os.close(self.fd);self.fd=None
        if self.upload is not None:self.upload.close()


def remove_input(state_root, identifier, *, processes_and_mounts_clean):
    """Internal lifecycle call after Runtime's original cgroup/runtime proof."""
    input_id(identifier)
    if not callable(processes_and_mounts_clean) or processes_and_mounts_clean() is not True:
        c.reject("Cannot remove input before owned processes/mounts are gone", code="conflict")
    parent = no_symlinks(Path(state_root)/"transfer/ingress")
    target = no_symlinks(parent/identifier)
    if not target.exists():
        return True
    info = target.stat()
    if not stat.S_ISDIR(info.st_mode) or info.st_uid != os.geteuid():
        c.reject("Owned input directory identity/owner differs", code="conflict")
    with os.scandir(target) as scan:
        for count, item in enumerate(scan):
            if count or item.name != "data.bin" or item.is_symlink():
                c.reject("Owned input has unexpected files/links", code="conflict")
            value = item.stat(follow_symlinks=False)
            if not stat.S_ISREG(value.st_mode) or value.st_nlink != 1 or value.st_uid != os.geteuid() or value.st_size > 64*1024*1024:
                c.reject("Owned input file differs", code="conflict")
    target.chmod(0o700)
    data = target/"data.bin"
    if data.exists():
        data.unlink()
    _sync(target); target.rmdir(); _sync(parent)
    return not target.exists()
