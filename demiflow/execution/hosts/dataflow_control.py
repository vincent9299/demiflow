"""Dedicated bounded control transports for the dataflow worker (H045-H048, C09).

LocalControlTransport drives a private length-prefixed socket frame channel so
control never shares the business data socket. RayControlTransport drives the
worker's own bounded mailbox; the driver polls drain() and delivers replies, so
there is no driver journal actor and no ray.data import. Neither transport holds
a driver RunJournal or sqlite connection. A lost effect confirmation stays
UNKNOWN_EFFECT and is never silently replayed.
"""
from __future__ import annotations

import threading
import time
from typing import TYPE_CHECKING

from demiflow.execution.contracts.dataflow_codec import decode_wire, encode_wire
from demiflow.execution.contracts.dataflow_errors import DataflowError
from demiflow.execution.contracts.recovery_contract import identifier

if TYPE_CHECKING:
    from demiflow.execution.contracts.dataflow import Bounds, ControlReply, SocketConnection, WorkerControlMailbox, WorkerEvent, WorkerLease


class LocalControlTransport:
    def __init__(self, connection: "SocketConnection", *, lease: "WorkerLease", bounds: "Bounds"):
        identifier(lease.allocation_id)
        identifier(lease.boot_id)
        self._connection = connection
        self._lease = lease
        self._bounds = bounds
        self._pending: dict[str, "WorkerEvent"] = {}
        self._lock = threading.Lock()
        self._closed = False
        self.store = None

    def request(self, event: "WorkerEvent", *, deadline_ms: int) -> "ControlReply":
        if event.allocation_id != self._lease.allocation_id or event.boot_id != self._lease.boot_id:
            raise DataflowError("FENCED", "control event identity does not match the worker lease")
        deadline = time.monotonic() + deadline_ms / 1000.0
        with self._lock:
            if self._closed:
                raise DataflowError("CANCELLED", "control transport is closed")
            if event.operation_id in self._pending:
                raise DataflowError("INVALID_CONTRACT", "duplicate inflight control operation")
            self._pending[event.operation_id] = event
        try:
            self._send(event)
            return self._recv(event.operation_id, deadline)
        finally:
            with self._lock:
                self._pending.pop(event.operation_id, None)

    def _send(self, event: "WorkerEvent") -> None:
        raw = encode_wire(event, type_name="WorkerEvent", bounds=self._bounds)
        if len(raw) > self._bounds.max_control_bytes:
            raise DataflowError("BUDGET_EXCEEDED", "control event exceeds the frame budget")
        self._connection.send_bytes(len(raw).to_bytes(4, "big") + raw)

    def _recv(self, operation_id: str, deadline: float) -> "ControlReply":
        while True:
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise DataflowError("UNKNOWN_EFFECT", "control reply deadline elapsed; remote effect unknown")
            if not self._connection.poll(remaining):
                raise DataflowError("UNKNOWN_EFFECT", "control reply deadline elapsed; remote effect unknown")
            header = self._connection.recv_bytes(4)
            if len(header) != 4:
                raise DataflowError("INTERNAL_PROTOCOL", "truncated control frame header")
            length = int.from_bytes(header, "big")
            if length > self._bounds.max_control_bytes:
                raise DataflowError("BUDGET_EXCEEDED", "control frame declaration exceeds the budget")
            raw = self._connection.recv_bytes(length)
            if len(raw) != length:
                raise DataflowError("INTERNAL_PROTOCOL", "truncated control frame body")
            reply = decode_wire(raw, type_name="ControlReply", bounds=self._bounds)
            if reply.operation_id != operation_id:
                raise DataflowError("INVALID_CONTRACT", "control reply correlated to a different operation")
            return reply

    def close(self) -> None:
        with self._lock:
            self._closed = True
            self._pending.clear()
        self._connection.close()

    def check_stop(self) -> None:
        if self._closed:
            raise DataflowError("CANCELLED", "control transport is closed")


class RayControlTransport:
    def __init__(self, mailbox: "WorkerControlMailbox", *, bounds: "Bounds"):
        self._mailbox = mailbox
        self._bounds = bounds
        self._closed = False
        self.store = None

    def request(self, event: "WorkerEvent", *, deadline_ms: int) -> "ControlReply":
        return self._mailbox.request(event, deadline_ms=deadline_ms)

    def close(self) -> None:
        self._closed = True
        self._mailbox.close()

    def check_stop(self) -> None:
        if self._closed:
            raise DataflowError("CANCELLED", "control transport is closed")
