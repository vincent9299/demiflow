"""Mailbox implementation."""
from __future__ import annotations

import threading
import time
from collections import deque
from typing import Callable, TYPE_CHECKING
from demiflow.execution.contracts.dataflow_errors import DataflowError
from demiflow.execution.contracts.dataflow_codec import wire_digest as _wire_digest
if TYPE_CHECKING:
    from demiflow.execution.contracts.dataflow import Bounds, ControlReply, WorkerEvent


class _Waiter:
    __slots__ = ("operation_id", "digest", "reply", "done", "error")

    def __init__(self, operation_id: str, digest: str):
        self.operation_id = operation_id
        self.digest = digest
        self.reply = None
        self.done = False
        self.error = None

class ControlMailbox:
    def __init__(self, *, bounds: "Bounds", check_stop: Callable[[], None],
                 on_outbox: Callable[[], None] | None = None):
        self._bounds = bounds
        self._check_stop = check_stop
        self._outbox: deque = deque()
        self._pending: dict[str, _Waiter] = {}
        self._condition = threading.Condition()
        self._closed = False
        self._max = bounds.max_pending_events
        self._on_outbox = on_outbox

    def request(self, event: "WorkerEvent", *, deadline_ms: int) -> "ControlReply":
        self._check_stop()
        digest = _wire_digest(event, "WorkerEvent", self._bounds)
        deadline = time.monotonic() + deadline_ms / 1000.0
        owned = False
        with self._condition:
            if self._closed:
                raise DataflowError("CANCELLED", "control mailbox is closed")
            waiter = self._pending.get(event.operation_id)
            if waiter is not None:
                if waiter.digest != digest:
                    raise DataflowError("INVALID_CONTRACT", "operation id reused with a different payload")
            else:
                while len(self._pending) >= self._max or len(self._outbox) >= self._max:
                    remaining = deadline - time.monotonic()
                    if remaining <= 0:
                        raise DataflowError("BUDGET_EXCEEDED", "control mailbox pending ceiling reached")
                    self._condition.wait(remaining)
                    if self._closed:
                        raise DataflowError("CANCELLED", "control mailbox is closed")
                if len(self._pending) >= self._max:
                    raise DataflowError("BUDGET_EXCEEDED", "control mailbox pending ceiling reached")
                waiter = _Waiter(event.operation_id, digest)
                self._pending[event.operation_id] = waiter
                was_empty = not self._outbox
                self._outbox.append(event)
                if was_empty and self._on_outbox is not None:
                    self._on_outbox()
                owned = True
            try:
                while not waiter.done:
                    remaining = deadline - time.monotonic()
                    if remaining <= 0:
                        raise DataflowError("UNKNOWN_EFFECT", "control reply deadline elapsed; remote effect unknown")
                    self._condition.wait(remaining)
                if waiter.error is not None:
                    raise waiter.error
                if waiter.reply is None:
                    raise DataflowError("CANCELLED", "control mailbox closed before a reply arrived")
                return waiter.reply
            finally:
                if owned and self._pending.get(event.operation_id) is waiter:
                    del self._pending[event.operation_id]
                    self._condition.notify_all()

    def send(self, event):
        # Notifications share the bounded outbox but never wait for a journal RPC.
        deadline = time.monotonic() + self._bounds.timeout_ms / 1000
        with self._condition:
            while len(self._outbox) >= self._max:
                self._check_stop()
                remaining = deadline - time.monotonic()
                if self._closed or remaining <= 0:
                    raise DataflowError("DEADLINE_EXCEEDED", "worker notification backpressure deadline")
                self._condition.wait(min(.05, remaining))
            if self._closed:
                raise DataflowError("CANCELLED", "control mailbox closed")
            was_empty = not self._outbox
            self._outbox.append(event)
            if was_empty and self._on_outbox is not None:
                self._on_outbox()
            self._condition.notify_all()

    def drain(self, *, max_events: int) -> tuple["WorkerEvent", ...]:
        if max_events < 1:
            raise DataflowError("INVALID_CONTRACT", "drain max_events must be positive")
        limit = min(max_events, self._max)
        with self._condition:
            batch = []
            while self._outbox and len(batch) < limit:
                batch.append(self._outbox.popleft())
            self._condition.notify_all()
            return tuple(batch)

    def deliver(self, reply: "ControlReply") -> None:
        digest = _wire_digest(reply, "ControlReply", self._bounds)
        with self._condition:
            waiter = self._pending.get(reply.operation_id)
            if waiter is None:
                return
            if waiter.done:
                if waiter.reply is not None and _wire_digest(waiter.reply, "ControlReply", self._bounds) != digest:
                    raise DataflowError("INVALID_CONTRACT", "conflicting duplicate control reply")
                return
            waiter.reply = reply
            waiter.done = True
            self._condition.notify_all()

    def cancel_pending(self) -> None:
        """Wake business RPC waits before joining their owner on cancellation.

        A lost reply remains unknown. The port stays open for lifecycle cleanup;
        neither cancellation nor a late reply manufactures an effect receipt.
        """
        with self._condition:
            for waiter in self._pending.values():
                if not waiter.done:
                    waiter.error = DataflowError('UNKNOWN_EFFECT', 'RPC cancelled; remote effect remains unknown')
                    waiter.done = True
            self._condition.notify_all()

    def close(self) -> None:
        with self._condition:
            self._closed = True
            for operation_id in list(self._pending):
                waiter = self._pending[operation_id]
                if waiter.done and waiter.reply is not None:
                    del self._pending[operation_id]
                    continue
                if not waiter.done:
                    waiter.error = DataflowError("UNKNOWN_EFFECT", "driver lost; control effect unknown")
                    waiter.done = True
            self._outbox.clear()
            self._condition.notify_all()
