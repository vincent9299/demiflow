"""Finite nonblocking Unix frames and kernel-supplied process identities.

No descriptor passing, path supplied by a caller, reconnect or request replay.
The event loop limits the number of connections separately; each connection
holds at most one1 MiB input frame and two1 MiB output frames (plus headers).
Control-only listeners set a64 KiB input/output budget instead.
"""
from __future__ import annotations

from collections import deque
from dataclasses import dataclass
import os
from pathlib import Path
import socket
import stat
import struct
import time

from demiflow.workbench.process import process_identity
from demiflow.forge.io import no_symlinks
from demiflow.workbench.analysis import contracts as c

HEADER = struct.Struct("!cI")
TAGS = {b"J": "json", b"A": "arrow", b"B": "blob", b"E": "end"}


@dataclass(frozen=True)
class Peer:
    pid: int
    uid: int
    gid: int
    identity: dict

    @classmethod
    def capture(cls, connection, *, allowed_uids):
        pid, uid, gid = struct.unpack("3i", connection.getsockopt(socket.SOL_SOCKET, socket.SO_PEERCRED, struct.calcsize("3i")))
        if uid not in allowed_uids:
            c.reject("Analysis IPC peer UID is not authorized", code="operation_rejected")
        original = process_identity(pid)
        if original is None:
            c.reject("Analysis IPC peer process is unavailable", code="operation_rejected")
        return cls(pid, uid, gid, original)

    def live(self):
        return process_identity(self.pid) == self.identity

    def job(self, delegated, lease, *, init_only=False):
        if not self.live() or self.uid != os.geteuid():
            return False
        if init_only and self.identity != lease["init_identity"]:
            return False
        with open(f"/proc/{self.pid}/cgroup", "rb") as source:
            raw = source.read(4097)
        if len(raw) > 4096:
            return False
        expected = str((Path(delegated)/"jobs"/lease["job_id"]).relative_to("/sys/fs/cgroup"))
        return raw.decode("ascii").splitlines() == ["0::/" + expected] and self.live()


class Listener:
    def __init__(self, path, *, public=False):
        self.path = no_symlinks(Path(path))
        if not self.path.is_absolute() or len(os.fsencode(self.path)) > 107:
            c.reject("Fixed Unix listener path is too long or relative")
        parent = self.path.parent.stat()
        if parent.st_uid not in {0, os.geteuid()} or parent.st_mode & 0o022:
            c.reject("Analysis socket parent must be owned and immutable to other users")
        self.socket = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM | socket.SOCK_NONBLOCK | socket.SOCK_CLOEXEC)
        self.owned = None
        try:
            self.socket.bind(str(self.path))  # never overwrite an unknown/stale listener
            info = self.path.stat()
            self.owned = (info.st_dev, info.st_ino)
            self.path.chmod(0o660 if public else 0o600)
            self.socket.listen(8)
        except BaseException:
            self.close()
            raise

    def close(self):
        self.socket.close()
        if self.owned is not None:
            try:
                info = self.path.lstat()
                if stat.S_ISSOCK(info.st_mode) and (info.st_dev, info.st_ino) == self.owned:
                    self.path.unlink()
            except FileNotFoundError:
                pass
            self.owned = None


class Channel:
    def __init__(self, connection, *, binary=False, now=None):
        connection.setblocking(False)
        self.connection = connection
        self.binary = binary
        self.header = bytearray()
        self.payload = None
        self.kind = None
        self.position = 0
        self.outgoing = deque()
        self.outgoing_bytes = 0
        self.output_limit = 2*1024*1024 + 10 if binary else 65536 + 5
        self.created = self.progress = time.monotonic() if now is None else now
        self.closed = False

    def receive(self):
        """Consume at most one frame per turn; None means incomplete input."""
        if self.payload is None:
            try:
                raw = self.connection.recv(HEADER.size - len(self.header))
            except BlockingIOError:
                return None
            if not raw:
                raise EOFError("analysis peer disconnected")
            self.progress = time.monotonic()
            self.header.extend(raw)
            if len(self.header) < HEADER.size:
                return None
            tag, size = HEADER.unpack(self.header)
            if tag not in TAGS:
                c.reject("Unknown analysis frame tag")
            maximum = 65536 if tag == b"J" else 1024*1024 if self.binary and tag in {b"A", b"B"} else 0
            if size > maximum or tag != b"E" and size == 0 or not self.binary and tag != b"J":
                c.reject("Analysis IPC frame exceeds the listener budget", code="resource_limit")
            self.kind, self.payload, self.position = TAGS[tag], bytearray(size), 0
        if self.position < len(self.payload):
            try:
                target = memoryview(self.payload)[self.position:]
                count = self.connection.recv_into(target)
                del target
            except BlockingIOError:
                return None
            if count == 0:
                raise EOFError("analysis peer disconnected mid-frame")
            self.progress = time.monotonic(); self.position += count
            if self.position < len(self.payload):
                return None
        kind, payload = self.kind, bytes(self.payload)
        self.header.clear(); self.payload = self.kind = None; self.position = 0
        return kind, c.decode(payload) if kind == "json" else None if kind == "end" else payload

    def queue(self, kind, value=None):
        if kind == "json":
            data, tag = c.encoded(value), b"J"
        elif self.binary and kind in {"arrow", "blob"}:
            if type(value) is not bytes or not 1 <= len(value) <= 1024*1024:
                c.reject("Outgoing analysis binary frame exceeds byte limit", code="resource_limit")
            data, tag = value, b"A" if kind == "arrow" else b"B"
        elif self.binary and kind == "end" and value is None:
            data, tag = b"", b"E"
        else:
            c.reject("Invalid outgoing analysis frame")
        size = HEADER.size + len(data)
        if self.outgoing_bytes + size > self.output_limit or len(self.outgoing) >= 32:
            c.reject("Analysis peer is not consuming its bounded output", code="resource_limit")
        self.outgoing.append(memoryview(HEADER.pack(tag, len(data))))
        if data:
            self.outgoing.append(memoryview(data))
        self.outgoing_bytes += size

    def flush(self):
        """One bounded send per readiness turn preserves control responsiveness."""
        if not self.outgoing:
            return
        view = self.outgoing[0]
        try:
            size = self.connection.send(view[:65536])
        except BlockingIOError:
            return
        if size <= 0:
            raise EOFError("analysis peer stopped reading")
        self.progress = time.monotonic(); self.outgoing_bytes -= size
        if size == len(view):
            self.outgoing.popleft()
        else:
            self.outgoing[0] = view[size:]

    def expired(self, *, wall_seconds=140, idle_seconds=5):
        now = time.monotonic()
        return now-self.created > wall_seconds or now-self.progress > idle_seconds

    def close(self):
        if not self.closed:
            self.connection.close()
            self.outgoing.clear(); self.outgoing_bytes = 0
            self.payload = None
            self.closed = True
