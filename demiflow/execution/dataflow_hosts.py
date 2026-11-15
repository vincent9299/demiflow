"""Multi-host endpoints, connectors and the bounded worker control mailbox (C09).

ControlMailbox is the worker-side bounded outbox/pending/condition table the
driver polls (drain) and answers (deliver). It holds no large payload and never
creates unbounded futures: at the pending ceiling it backpressures, and a lost
driver or cancel wakes every waiter with UNKNOWN_EFFECT evidence rather than
hanging. Host endpoints and connectors are added below as the lane progresses.
"""
from __future__ import annotations

import hashlib
import importlib.util
import json
import sys
import threading
import time
import uuid
from collections import deque
from dataclasses import asdict
from typing import TYPE_CHECKING, Callable

from .contracts.dataflow_codec import encode_wire, decode_wire
from .contracts.dataflow_errors import DataflowError
from .worker.bootstrap import LocalControlConnector

if TYPE_CHECKING:
    from .contracts.dataflow import Bounds, ControlReply, CutRequest, DriverServices, WorkerAssignment, WorkerEvent, WorkerLease

_RAY_AVAILABLE = importlib.util.find_spec("ray") is not None


class _Waiter:
    __slots__ = ("operation_id", "digest", "reply", "done", "error")

    def __init__(self, operation_id: str, digest: str):
        self.operation_id = operation_id
        self.digest = digest
        self.reply = None
        self.done = False
        self.error = None


def _wire_digest(value: object, type_name: str, bounds: "Bounds") -> str:
    return hashlib.sha256(encode_wire(value, type_name=type_name, bounds=bounds)).hexdigest()


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


def _event_from_dict(data: dict, *, bounds) -> "WorkerEvent":
    raw = json.dumps({"schema": "demiflow.runtime_wire.v3", "type": "WorkerEvent", "payload": data}).encode()
    return decode_wire(raw, type_name="WorkerEvent", bounds=bounds)


def _reply_frame(reply: "ControlReply", *, bounds) -> dict:
    return {"type": "control_reply", "wire": encode_wire(reply, type_name="ControlReply", bounds=bounds).decode()}


class DataflowLocalConnector(LocalControlConnector):
    """Local socketpair connector for the v3 dataflow worker (H055).

    Reuses the existing bounded socket/FD staging and launch protocol; the v3
    entrypoint is dispatched by the existing bootstrap version switch. A failed
    spawn runs the inherited abort cleanup so no scope or fd is leaked.
    """

    def __init__(self):
        super().__init__()
        self._bounds_by_allocation = {}

    def stage(self, allocation_id, launch_spec):
        from .contracts.dataflow import Bounds
        config = launch_spec.get("dataflow_v3")
        if not isinstance(config, dict) or not isinstance(config.get("bounds"), dict):
            raise DataflowError("INVALID_CONTRACT", "v3 launch requires explicit bounds")
        self._bounds_by_allocation[allocation_id] = Bounds(**config["bounds"])
        super().stage(allocation_id, launch_spec)

    def abort(self, allocation_id):
        try:
            super().abort(allocation_id)
        finally:
            # Inherited close() also calls abort after physical retirement.
            # Do not retain one Bounds object per parked worker generation.
            self._bounds_by_allocation.pop(allocation_id, None)

    def spawn_payload(self, allocation_id: str) -> dict:
        launch_spec = self._staged[allocation_id]
        v3 = launch_spec.get("dataflow_v3")
        if type(v3) is not dict or v3.get("entrypoint") != "dataflow_worker_v3":
            raise DataflowError("INVALID_CONTRACT", "v3 local launch requires the dataflow_worker_v3 entrypoint")
        payload = super().spawn_payload(allocation_id)
        # A workbench driver runs from a sealed snapshot under Python -I.
        # sys.path edits are not inherited by subprocesses: pin the exact
        # loaded runtime ahead of editable installs or the caller's PYTHONPATH.
        import os
        from pathlib import Path
        root = str(Path(__file__).resolve().parents[2])
        inherited = os.environ.get("PYTHONPATH", "")
        payload["extra_env"]["PYTHONPATH"] = root + (os.pathsep + inherited if inherited else "")
        return payload

    def request_cut(self, allocation_id: str, request: "CutRequest") -> None:
        from .worker.bootstrap import send_message
        bounds = self._bounds_by_allocation[allocation_id]
        send_message(self.driver_socks[allocation_id], {"type": "cut_request", "wire":
            encode_wire(request, type_name="CutRequest", bounds=bounds).decode()})

    def deliver_control_reply(self, allocation_id: str, reply: "ControlReply") -> None:
        from .worker.bootstrap import send_message
        bounds = self._bounds_by_allocation[allocation_id]
        send_message(self.driver_socks[allocation_id], _reply_frame(reply, bounds=bounds))


class _EndpointBase:
    def __init__(self, lease: "WorkerLease", *, services: "DriverServices"):
        self._lease = lease
        self._services = services
        self._pending: dict[str, "CutRequest"] = {}
        self._cuts: dict[str, "CutRequest"] = {}
        self._scope_id = None
        self._allocation_id = lease.allocation_id
        self._boot_id = lease.boot_id
        self._activated = False
        self._closed_flag = False
        self._cleanup_completed = False
        self._grant = None
        self._model = None
        self._manager = services.resources
        self._state = self._manager.state
        self._provider = self._manager.provider
        self._connector = self._provider._control
        self._assignment = None
        self._events = deque()

    def _build_launch(self, assignment: "WorkerAssignment") -> tuple[dict, dict, dict]:
        run = self._services.action.context
        lease = self._lease
        from .contracts.dataflow_codec import runtime_scope_id
        scope_id = runtime_scope_id(assignment.plan_id, assignment.attempt)
        raw = b"".join(self._services.store.read(lease.grant_ref,
                        max_bytes=assignment.bounds.max_control_bytes))
        grant = json.loads(raw)
        if (grant["scope_id"] != scope_id or grant["allocation_id"] != lease.allocation_id
                or grant["boot_id"] != lease.boot_id):
            raise DataflowError("FENCED", "worker grant differs from staged allocation")
        # The v3 grant artifact also carries scheduling metadata; K3 consumes
        # its frozen nine-field authority projection, not those extra fields.
        grant = {key: grant[key] for key in ("grant_id", "run_id", "attempt", "fence",
                 "scope_id", "allocation_id", "boot_id", "controller_epoch", "expires_at_ms")}
        code_pin = assignment.sealed.code_pin_ref.digest
        manifest_ref = {"manifest_id": code_pin, "sha256": code_pin}
        launch_spec = {
            "directory": str(run.directory), "run_id": run.ref.run_id, "scope_id": scope_id,
            "allocation_id": lease.allocation_id, "boot_id": lease.boot_id,
            "controller_epoch": run.attempt, "capabilities": ["cpu-map", "stream"], "grant": grant,
            "artifact_digest_observed": code_pin,
            "dataflow_v3": {"entrypoint": "dataflow_worker_v3", "bounds": asdict(assignment.bounds),
                            "limits": dict(run.limits), "code_pin": code_pin,
                            "host_id": lease.host_id,
                            "cache": str(run.directory / "worker-cache"),
                            "store_root": str(self._services.store.root), "store_id": self._services.store.store_id,
                            "assignment": {"wire": encode_wire(assignment, type_name="WorkerAssignment",
                                                   bounds=assignment.bounds).decode("utf-8")}},
        }
        if getattr(self, "_recovery_image", None) is not None:
            launch_spec["dataflow_v3"]["recovery_image_wire"] = encode_wire(self._recovery_image,
                type_name="RecoveryImage", bounds=assignment.bounds).decode()
        return launch_spec, grant, manifest_ref

    def prepare(self, assignment: "WorkerAssignment") -> None:
        if self._assignment is not None:
            if self._assignment != assignment:
                raise DataflowError("FENCED", "allocation assignment changed after staging")
            return
        launch, grant, _ = self._build_launch(assignment)
        self._grant = grant
        self._scope_id = grant["scope_id"]
        self._assignment = assignment
        self._connector.stage(self._allocation_id, launch)

    def _check_event_identity(self, event):
        assignment = self._assignment
        if (event.run_id != assignment.run_id or event.attempt != assignment.attempt
                or event.fence != assignment.fence or event.allocation_id != self._allocation_id
                or event.boot_id != self._boot_id or event.plan_revision != assignment.plan_revision):
            raise DataflowError("FENCED", "Worker event differs from its allocation/attempt")

    def _k3_envelope(self, kind: str, payload: dict) -> dict:
        from .contracts.envelope import WORKER_PROTOCOL_VERSION
        run = self._services.action.context
        return {"protocol_version": dict(WORKER_PROTOCOL_VERSION), "kind": kind,
                "scope_id": self._scope_id, "run_id": run.ref.run_id, "operation_id": uuid.uuid4().hex,
                "controller_epoch": run.attempt, "authority_ref": self._grant["grant_id"],
                "allocation_id": self._allocation_id, "boot_id": self._boot_id, "payload": payload}

    def send_business(self, command):
        if hasattr(self, "_sock"):
            from .worker.bootstrap import send_message
            send_message(self._sock, {"type": "dataflow_command", "command": command})
        else:
            ray.get(self._connector._actor(self._allocation_id).receive.remote(command),
                    timeout=self._services.policy.control_timeout_ms / 1000)

    def _early_event(self, event):
        self._check_event_identity(event)
        if event.kind in ("ready", "data", "ack", "idle", "terminal", "eoi", "cut", "failure", "closed", "progress"):
            if len(self._events) >= self._services.bounds.max_pending_events:
                raise DataflowError("BUDGET_EXCEEDED", "activation event queue exceeds bound")
            self._events.append(event)
        else:
            self.deliver_reply(self._services.journal_service.dispatch(event))

    def request_cut(self, request: "CutRequest") -> None:
        if not self._activated:
            raise DataflowError("FENCED", "cut requested before activation")
        existing = self._cuts.get(request.cut_id)
        if existing is not None:
            if existing != request:
                raise DataflowError("INVALID_CONTRACT", "conflicting cut identity")
            return
        self._cuts[request.cut_id] = request
        self._connector.request_cut(self._allocation_id, request)

    def drain(self, *, deadline_ms: int) -> None:
        if self._cleanup_completed or not self._activated:
            return
        self._send_k3("Drain", {"reason": "execution_close",
                                "deadline_ms": deadline_ms})

    def cancel(self, reason: str, *, deadline_ms: int) -> None:
        try:
            self._send_k3("Cancel", {"operation_id": uuid.uuid4().hex, "reason": reason})
        except DataflowError:
            pass

    def closed(self) -> bool:
        if self._scope_id is None:
            return self._closed_flag
        snapshot = self._manager.get(self._scope_id)
        allocations = snapshot.get("allocations") or []
        if not allocations:
            return self._closed_flag
        return any(a["allocation_id"] == self._allocation_id and a.get("cleanup") == "CONFIRMED"
                   for a in allocations)


class LocalHostEndpoint(_EndpointBase):
    _entrypoint = "demiflow-worker"

    def __init__(self, lease: "WorkerLease", *, services: "DriverServices"):
        super().__init__(lease, services=services)
        self._sock = None
        self._argv = (sys.executable, "-B", "-m", "demiflow.execution.worker.bootstrap")

    def activate(self, assignment: "WorkerAssignment") -> None:
        from .contracts.worker import WorkerProtocolModel
        from .worker.bootstrap import recv_message
        from .worker.dataflow_host import make_worker_control_assignment
        if self._assignment != assignment:
            raise DataflowError("FENCED", "worker assignment was not staged before allocation")
        _, grant, manifest_ref = self._build_launch(assignment)
        self._sock = self._connector.driver_socks[self._allocation_id]
        self._sock.settimeout(self._services.policy.control_timeout_ms / 1000.0)
        run = self._services.action.context
        self._model = WorkerProtocolModel(
            scope_id=self._scope_id, run_id=run.ref.run_id, allocation_id=self._allocation_id,
            boot_id=self._boot_id, controller_epoch=run.attempt, grant=grant, plan_revision=0,
            capabilities=["cpu-map", "stream"])
        register = recv_message(self._sock)
        if register is None or register.get("kind") != "Register":
            raise DataflowError("INTERNAL_PROTOCOL", "local worker did not register")
        self._model.handle(register, now_ms=int(time.time() * 1000))
        control = make_worker_control_assignment(assignment, grant=grant, manifest_ref=manifest_ref,
                                                 ready_deadline_ms=60000)
        self._send_k3("VerifyArtifactAndCapabilities",
                      {"artifact_manifest_ref": manifest_ref, "required_capabilities": ["cpu-map", "stream"]})
        self._send_k3("Restore", {"assignment_ref": assignment.action_id,
                                  "restore_point": control["restore_point"], "state_refs": []})
        self._send_k3("Activate", {"grant_ref": grant["grant_id"], "controller_epoch": run.attempt,
                                   "operation_id": uuid.uuid4().hex})
        self._send_k3("Assign", {"assignment": control})
        self._activated = True

    def _send_k3(self, kind: str, payload: dict) -> dict:
        from .worker.bootstrap import recv_message, send_message
        envelope = self._k3_envelope(kind, payload)
        send_message(self._sock, envelope)
        deadline = time.monotonic() + self._services.policy.control_timeout_ms / 1000
        while True:
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise DataflowError("DEADLINE_EXCEEDED", "Local worker control command timed out")
            import select
            if not select.select([self._sock], [], [], min(.05, remaining))[0]:
                continue
            message = recv_message(self._sock)
            if message is None:
                raise DataflowError("UNKNOWN_EFFECT", "local worker closed before responding")
            if message.get("response_to") == envelope["operation_id"]:
                if "error" in message:
                    from .dataflow_failures import restore_failure
                    raise restore_failure(message["error"])
                if kind in ("Drain", "Quiesce", "Cancel"):
                    self._cleanup_completed = True
                return message["receipt"]
            if message.get("type") == "worker_event":
                event = _event_from_dict(message["event"], bounds=self._services.bounds)
                self._early_event(event)
            if message.get("kind") in ("Register", "Ready", "Heartbeat"):
                self._model.handle(message, now_ms=int(time.time() * 1000))

    def poll(self, *, max_events: int, deadline_ms: int):
        import select
        from .worker.bootstrap import recv_message
        events = []
        while self._events and len(events) < max_events:
            events.append(self._events.popleft())
        # Wait before reading a frame; a timeout halfway through a frame must
        # never discard its prefix and corrupt the next message boundary.
        self._sock.settimeout(self._services.policy.control_timeout_ms / 1000)
        while len(events) < max_events and select.select([self._sock], [], [], 0)[0]:
            frame = recv_message(self._sock)
            if frame is None:
                raise DataflowError("UNKNOWN_EFFECT", "worker connection closed during execution")
            if frame.get("type") == "worker_event":
                events.append(_event_from_dict(frame["event"], bounds=self._services.bounds))
            elif "error" in frame:
                raise DataflowError("INTERNAL_PROTOCOL", "worker control failed: " + str(frame["error"]))
        return tuple(events)

    def deliver_reply(self, reply: "ControlReply") -> None:
        self._connector.deliver_control_reply(self._allocation_id, reply)


def make_host_endpoint(lease: "WorkerLease", *, services: "DriverServices"):
    host = next((h for h in services.hosts if h.host_id == lease.host_id), None)
    if host is None:
        raise DataflowError("INVALID_CONTRACT", "lease host is not a declared driver host")
    if host.provider == "local":
        if services.policy.worker_mode == "thread":
            from .dataflow_thread_host import ThreadHostEndpoint
            return ThreadHostEndpoint(lease, services=services)
        return LocalHostEndpoint(lease, services=services)
    if host.provider == "ray_core":
        if not _RAY_AVAILABLE:
            raise DataflowError("UNSUPPORTED_CAPABILITY", "Ray Core provider requires the ray package")
        return RayHostEndpoint(lease, services=services)
    raise DataflowError("INVALID_CONTRACT", "unsupported host provider: " + str(host.provider))


if _RAY_AVAILABLE:
    import ray
    from ray.exceptions import GetTimeoutError, RayActorError
    from .worker.ray_host import RayControlConnector as _RayControlConnector
else:
    _RayControlConnector = object


class DataflowRayConnector(_RayControlConnector):
    """Ray actor connector hosting the v3 dataflow worker (H054)."""

    def __init__(self, **kwargs):
        if not _RAY_AVAILABLE:
            raise DataflowError("UNSUPPORTED_CAPABILITY", "Ray Core provider requires the ray package")
        super().__init__(**kwargs)

    def host_class(self):
        from .worker.dataflow_host import HostedDataflowWorker
        return ray.remote(max_restarts=0, max_task_retries=0, max_concurrency=8)(HostedDataflowWorker)

    def placement_options(self, allocation_id):
        from ray.util.scheduling_strategies import NodeAffinitySchedulingStrategy
        host_id = self.launch_spec(allocation_id)['dataflow_v3']['host_id']
        if not host_id.startswith('ray-'):
            raise DataflowError('INVALID_CONTRACT', 'Worker lacks a verified Ray host')
        return {'scheduling_strategy': NodeAffinitySchedulingStrategy(host_id[4:], soft=False)}

    def request_cut(self, allocation_id: str, request: "CutRequest") -> None:
        ray.get(self._actor(allocation_id).request_cut.remote(request),
                timeout=self._command_timeout_s)

    def deliver_control_reply(self, allocation_id: str, reply: "ControlReply") -> None:
        ray.get(self._actor(allocation_id).deliver_control_reply.remote(reply),
                timeout=self._command_timeout_s)

class RayHostEndpoint(_EndpointBase):
    _entrypoint = "demiflow-worker-actor"

    def __init__(self, lease: "WorkerLease", *, services: "DriverServices"):
        if not _RAY_AVAILABLE:
            raise DataflowError("UNSUPPORTED_CAPABILITY", "Ray Core provider requires the ray package")
        super().__init__(lease, services=services)
        self._argv = (sys.executable, "-B", "-m", "demiflow.execution.worker.dataflow_host")

    def activate(self, assignment: "WorkerAssignment") -> None:
        from .worker.dataflow_host import make_worker_control_assignment
        if self._assignment != assignment:
            raise DataflowError("FENCED", "worker assignment was not staged before allocation")
        _, grant, manifest_ref = self._build_launch(assignment)
        control = make_worker_control_assignment(assignment, grant=grant, manifest_ref=manifest_ref,
                                                 ready_deadline_ms=60000)
        run = self._services.action.context
        self._send_k3("VerifyArtifactAndCapabilities",
                      {"artifact_manifest_ref": manifest_ref, "required_capabilities": ["cpu-map", "stream"]})
        self._send_k3("Restore", {"assignment_ref": assignment.action_id,
                                  "restore_point": control["restore_point"], "state_refs": []})
        self._send_k3("Activate", {"grant_ref": grant["grant_id"], "controller_epoch": run.attempt,
                                   "operation_id": uuid.uuid4().hex})
        self._send_k3("Assign", {"assignment": control})
        self._activated = True

    def _send_k3(self, kind: str, payload: dict) -> dict:
        actor = self._connector._actor(self._allocation_id)
        command = actor.handle.remote(self._k3_envelope(kind, payload))
        deadline = time.monotonic() + self._services.policy.control_timeout_ms / 1000
        while True:
            ready, _ = ray.wait([command], timeout=0.01)
            if ready:
                result = ray.get(command)
                break
            if time.monotonic() >= deadline:
                raise DataflowError("DEADLINE_EXCEEDED", "Ray worker control command timed out")
            for event in self._connector.pull(self._allocation_id, limit=self._services.bounds.max_pending_events):
                self._early_event(event)
        if "error" in result:
            raise DataflowError("INTERNAL_PROTOCOL", "worker command failed: " + str(result["error"]))
        if kind in ("Drain", "Quiesce", "Cancel"):
            self._cleanup_completed = True
        return result.get("receipt")

    def poll(self, *, max_events: int, deadline_ms: int):
        events = []
        while self._events and len(events) < max_events:
            events.append(self._events.popleft())
        if len(events) < max_events:
            try:
                events.extend(self._connector.pull(self._allocation_id, limit=max_events - len(events)))
            except (RayActorError, GetTimeoutError) as exc:
                raise DataflowError("UNKNOWN_EFFECT", "Ray worker became unreachable") from exc
        return tuple(events)

    def deliver_reply(self, reply: "ControlReply") -> None:
        self._connector.deliver_control_reply(self._allocation_id, reply)
