"""Finite JSON/Arrow frames for the restricted Unix data-broker protocol.

This is not the privileged helper socket. Kernel peers have no ability to
provide host paths, OCI arguments or global run controls through this protocol.
There is no reconnect/retry in a partially completed operation.
"""
from __future__ import annotations

from demiflow.workbench.analysis.job_context import job_path
from demiflow.workbench.analysis.job_context import current

import socket
import struct

from demiflow.workbench.analysis import contracts as c
from demiflow.workbench.analysis.arrow_bridge import CHUNK_BYTES

_HEADER = struct.Struct("!cI")


class Frames:
    def __init__(self, connection):
        self.connection = connection

    def _read(self, size):
        result = bytearray(size)
        remaining = memoryview(result)
        while remaining:
            count = self.connection.recv_into(remaining)
            if count == 0:
                c.reject("Analysis connection closed; query the original operation receipt", code="operation_failed")
            remaining = remaining[count:]
        return bytes(result)

    def send(self, kind, payload=None):
        if kind == "json":
            tag, data = b"J", c.encoded(payload)
        elif kind in {"arrow", "blob"}:
            if type(payload) is not bytes or not 1 <= len(payload) <= CHUNK_BYTES:
                c.reject("Arrow frame exceeds limit", code="resource_limit")
            tag, data = b"A" if kind == "arrow" else b"B", payload
        elif kind == "end":
            if payload is not None:
                c.reject("End frame must be empty")
            tag, data = b"E", b""
        else:
            c.reject("Unknown analysis frame type")
        self.connection.sendall(_HEADER.pack(tag, len(data)))
        if data:
            self.connection.sendall(data)

    def receive(self):
        tag, size = _HEADER.unpack(self._read(_HEADER.size))
        if tag not in {b"J", b"A", b"B", b"E"}:
            c.reject("Unknown analysis frame tag")
        maximum = 65536 if tag == b"J" else CHUNK_BYTES if tag in {b"A", b"B"} else 0
        if size > maximum or tag != b"E" and size == 0:
            c.reject("Analysis frame exceeds limit", code="resource_limit")
        data = self._read(size)
        if tag == b"J":
            return "json", c.decode(data)
        return ("arrow", data) if tag == b"A" else ("blob", data) if tag == b"B" else ("end", None)

    def json(self):
        kind, value = self.receive()
        if kind != "json":
            c.reject("Expected analysis control response")
        return value

    def end_read(self):
        """Keep the authenticated reader alive until its final frame is seen.

        This bounded acknowledgement confirms frame receipt, not publication,
        fresh authority or process cleanup. Those checks remain with the owner.
        The existing socket deadline applies; there is no reconnect or replay.
        """
        self.send("end")
        value = self.json()
        c.fields(value, {"format", "accepted"}, label="fixed read completion")
        if value["format"] != "demiflow_analysis_read_end_v1" or value["accepted"] is not True:
            c.reject("Fixed read completion was not acknowledged", code="operation_failed")

    def arrow(self):
        while True:
            kind, value = self.receive()
            if kind == "end":
                return
            if kind == "json":
                c.fields(value, {"error", "code"}, label="analysis stream failure")
                c.text(value["error"], "analysis error", 1024)
                c.reject(value["error"], code=value["code"] if type(value["code"]) is str and value["code"] in {"conflict", "resource_limit", "operation_rejected", "operation_failed"} else "operation_failed")
            if kind != "arrow":
                c.reject("Non-Arrow frame on an Arrow data stream")
            yield value


def connect_broker():
    """The image has exactly this per-session, data-only socket mount."""
    connection = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    connection.settimeout(40)
    try:
        connection.connect(job_path("/broker/data.sock"))
        return connection
    except OSError:
        connection.close()
        c.reject("Restricted analysis data broker is unavailable; there is no local execution fallback", code="operation_rejected")
