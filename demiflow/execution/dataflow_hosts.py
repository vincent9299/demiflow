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
import sys
import threading
import time
import uuid
from collections import deque
from dataclasses import asdict
from typing import TYPE_CHECKING, Callable

from .contracts.dataflow_codec import encode_wire
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
    def __init__(self, *, bounds: "Bounds", check_stop: Callable[[], None]):
        self._bounds = bounds
        self._check_stop = check_stop
        self._outbox: deque = deque()
        self._pending: dict[str, _Waiter] = {}
        self._condition = threading.Condition()
        self._closed = False
        self._max = bounds.max_pending_events

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
                while len(self._pending) >= self._max:
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
                self._outbox.append(event)
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

    def drain(self, *, max_events: int) -> tuple["WorkerEvent", ...]:
        if max_events < 1:
            raise DataflowError("INVALID_CONTRACT", "drain max_events must be positive")
        limit = min(max_events, self._max)
        with self._condition:
            batch = []
            while self._outbox and len(batch) < limit:
                batch.append(self._outbox.popleft())
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


def _event_from_dict(data: dict) -> "WorkerEvent":
    from .contracts.dataflow import ArtifactRef, WorkerEvent
    payload_ref = data.get("payload_ref")
    if isinstance(payload_ref, dict):
        payload_ref = ArtifactRef(**payload_ref)
    fields = dict(data)
    fields["payload_ref"] = payload_ref
    return WorkerEvent(**fields)


def _reply_frame(reply: "ControlReply") -> dict:
    return {"type": "control_reply", "operation_id": reply.operation_id, "status": reply.status,
            "payload": reply.payload, "error_code": reply.error_code}


class DataflowLocalConnector(LocalControlConnector):
    """Local socketpair connector for the v3 dataflow worker (H055).

    Reuses the existing bounded socket/FD staging and launch protocol; the v3
    entrypoint is dispatched by the existing bootstrap version switch. A failed
    spawn runs the inherited abort cleanup so no scope or fd is leaked.
    """

    def spawn_payload(self, allocation_id: str) -> dict:
        launch_spec = self._staged[allocation_id]
        v3 = launch_spec.get("dataflow_v3")
        if type(v3) is not dict or v3.get("entrypoint") != "dataflow_worker_v3":
            raise DataflowError("INVALID_CONTRACT", "v3 local launch requires the dataflow_worker_v3 entrypoint")
        return super().spawn_payload(allocation_id)

    def request_cut(self, allocation_id: str, request: "CutRequest") -> None:
        from .worker.bootstrap import send_message
        send_message(self.driver_socks[allocation_id], {"type": "cut_request", "cut": asdict(request)})

    def deliver_control_reply(self, allocation_id: str, reply: "ControlReply") -> None:
        from .worker.bootstrap import send_message
        send_message(self.driver_socks[allocation_id], _reply_frame(reply))


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
        self._grant = None
        self._model = None

    def _build_launch(self, assignment: "WorkerAssignment") -> tuple[dict, dict, dict]:
        run = self._services.action.context
        lease = self._lease
        scope_id = assignment.action_id
        grant = {"grant_id": uuid.uuid4().hex, "run_id": run.ref.run_id, "attempt": run.attempt,
                 "fence": run.fence, "scope_id": scope_id, "allocation_id": lease.allocation_id,
                 "boot_id": lease.boot_id, "controller_epoch": run.attempt,
                 "expires_at_ms": int(time.time() * 1000) + max(120000, int(run.limits["timeout_s"] * 1000))}
        code_pin = assignment.sealed.code_pin_ref.digest
        manifest_ref = {"manifest_id": code_pin, "sha256": code_pin}
        launch_spec = {
            "directory": str(run.directory), "run_id": run.ref.run_id, "scope_id": scope_id,
            "allocation_id": lease.allocation_id, "boot_id": lease.boot_id,
            "controller_epoch": run.attempt, "capabilities": ["cpu-map", "stream"], "grant": grant,
            "artifact_digest_observed": code_pin,
            "dataflow_v3": {"entrypoint": "dataflow_worker_v3", "bounds": asdict(assignment.bounds),
                            "limits": dict(run.limits), "code_pin": code_pin,
                            "cache": str(run.directory / "worker-cache"), "assignment": asdict(assignment)},
        }
        return launch_spec, grant, manifest_ref

    def _resource_request(self, assignment: "WorkerAssignment", scope_id: str, code_pin: str):
        from ..resources.contracts import (RESOURCE_PROTOCOL_VERSION, ResourceProfile,
                                           WorkerLaunchSpec, WorkerRequest)
        run = self._services.action.context
        profile = ResourceProfile(requests={"cpu_millis": 1000, "memory_bytes": 1024 ** 3})
        launch = WorkerLaunchSpec(
            artifact_refs={"code_pin": {"digest": code_pin, "location": "file://" + str(run.directory)}},
            entrypoint=self._entrypoint, argv=self._argv, bootstrap_ref="bootstrap-ref",
            access_refs={}, runtime_protocol_version=RESOURCE_PROTOCOL_VERSION)
        return WorkerRequest(uuid.uuid4().hex, self._allocation_id, profile, launch,
                             provision_deadline_ms=60000)

    def _open_scope(self, scope_id: str, provider: str) -> None:
        from ..resources.contracts import ResourceScope
        run = self._services.action.context
        self._state.open_scope(run.fence, ResourceScope(
            scope_id, run.ref.run_id, {"provider": provider}, run.fence,
            {"cpu_millis": 8000, "memory_bytes": 8 * 1024 ** 3, "max_workers": 4}))

    def _apply(self, scope_id: str, request) -> None:
        from ..resources.contracts import RESOURCE_PROTOCOL_VERSION, MutationContext, ResourcePlan
        run = self._services.action.context
        self._manager.apply(
            MutationContext(RESOURCE_PROTOCOL_VERSION, scope_id, run.attempt, run.fence, uuid.uuid4().hex, 0),
            ResourcePlan(scope_id, (request,), ()))
        self._manager.reconcile_once(run.fence, time.monotonic() + 30, 16)
        snapshot = self._manager.get(scope_id)
        phase = snapshot["allocations"][0]["phase"] if snapshot["allocations"] else None
        if phase != "RUNNING":
            raise DataflowError("RESOURCE_INFEASIBLE", "worker did not reach RUNNING: " + str(phase))

    def _k3_envelope(self, kind: str, payload: dict) -> dict:
        from .contracts.envelope import WORKER_PROTOCOL_VERSION
        run = self._services.action.context
        return {"protocol_version": dict(WORKER_PROTOCOL_VERSION), "kind": kind,
                "scope_id": self._scope_id, "run_id": run.ref.run_id, "operation_id": uuid.uuid4().hex,
                "controller_epoch": run.attempt, "authority_ref": self._grant["grant_id"],
                "allocation_id": self._allocation_id, "boot_id": self._boot_id, "payload": payload}

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

    def cancel(self, reason: str, *, deadline_ms: int) -> None:
        try:
            self._send_k3("Cancel", {"operation_id": uuid.uuid4().hex, "reason": reason})
        except DataflowError:
            pass
        identity = {"scope_id": self._scope_id, "allocation_id": self._allocation_id}
        evidence = self._provider.ensure_absent(identity, deadline_ms)
        self._closed_flag = evidence.get("cleanup") == "CONFIRMED"

    def closed(self) -> bool:
        if self._scope_id is None:
            return self._closed_flag
        snapshot = self._manager.get(self._scope_id)
        allocations = snapshot.get("allocations") or []
        if not allocations:
            return self._closed_flag
        return allocations[0].get("phase") == "TERMINATED"


class LocalHostEndpoint(_EndpointBase):
    _entrypoint = "demiflow-worker"

    def __init__(self, lease: "WorkerLease", *, services: "DriverServices"):
        super().__init__(lease, services=services)
        from ..resources.capacity import LocalCapacityPool
        from ..resources.local import LocalResourceProvider
        from ..resources.manager import ManagerBounds, ResourceManager
        from ..resources.process_unit import ManagedProcessUnit
        from .resource_state import RunResourceState
        run = services.action.context
        root = run.directory.with_name(run.directory.name + "-resources")
        pool = LocalCapacityPool(root / "capacity", capacity={"cpu_millis": 4000, "memory_bytes": 4 * 1024 ** 3})
        unit = ManagedProcessUnit(root / "units")
        self._connector = DataflowLocalConnector()
        self._provider = LocalResourceProvider(pool, unit, control=self._connector)
        self._state = RunResourceState(run.journal)
        self._manager = ResourceManager(self._provider, self._state, bounds=ManagerBounds())
        self._sock = None
        self._argv = (sys.executable, "-B", "-m", "demiflow.execution.worker.bootstrap")

    def activate(self, assignment: "WorkerAssignment") -> None:
        from .contracts.worker import WorkerProtocolModel
        from .worker.bootstrap import recv_message
        from .worker.dataflow_host import make_worker_control_assignment
        launch_spec, grant, manifest_ref = self._build_launch(assignment)
        self._grant = grant
        self._scope_id = assignment.action_id
        self._connector.stage(self._allocation_id, launch_spec)
        self._open_scope(self._scope_id, "local")
        self._apply(self._scope_id, self._resource_request(assignment, self._scope_id, manifest_ref["sha256"]))
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
        while True:
            message = recv_message(self._sock)
            if message is None:
                raise DataflowError("UNKNOWN_EFFECT", "local worker closed before responding")
            if message.get("response_to") == envelope["operation_id"]:
                if "error" in message:
                    raise DataflowError("INTERNAL_PROTOCOL", "worker command failed: " + str(message["error"]))
                return message["receipt"]
            if message.get("kind") in ("Register", "Ready", "Heartbeat"):
                self._model.handle(message, now_ms=int(time.time() * 1000))

    def poll(self, *, max_events: int, deadline_ms: int) -> tuple["WorkerEvent", ...]:
        import socket as _socket
        from .worker.bootstrap import recv_message
        self._sock.settimeout(deadline_ms / 1000.0)
        events = []
        for _ in range(max_events):
            try:
                frame = recv_message(self._sock)
            except (_socket.timeout, OSError):
                break
            if frame is None:
                break
            if frame.get("type") == "worker_event":
                events.append(_event_from_dict(frame["event"]))
        return tuple(events)

    def deliver_reply(self, reply: "ControlReply") -> None:
        self._connector.deliver_control_reply(self._allocation_id, reply)


def make_host_endpoint(lease: "WorkerLease", *, services: "DriverServices"):
    host = next((h for h in services.hosts if h.host_id == lease.host_id), None)
    if host is None:
        raise DataflowError("INVALID_CONTRACT", "lease host is not a declared driver host")
    if host.provider == "local":
        return LocalHostEndpoint(lease, services=services)
    if host.provider == "ray_core":
        if not _RAY_AVAILABLE:
            raise DataflowError("UNSUPPORTED_CAPABILITY", "Ray Core provider requires the ray package")
        return RayHostEndpoint(lease, services=services)
    raise DataflowError("INVALID_CONTRACT", "unsupported host provider: " + str(host.provider))


if _RAY_AVAILABLE:
    import ray
    from ray.exceptions import GetTimeoutError, RayActorError

    from .worker.ray_host import RayControlConnector

    class DataflowRayConnector(RayControlConnector):
        """Ray actor connector hosting the v3 dataflow worker (H054)."""

        def host_class(self):
            from .worker.dataflow_host import HostedDataflowWorker
            return ray.remote(max_restarts=0, max_task_retries=0)(HostedDataflowWorker)

        def request_cut(self, allocation_id: str, request: "CutRequest") -> None:
            ray.get(self._actor(allocation_id).request_cut.remote(request),
                    timeout=self._command_timeout_s)

        def deliver_control_reply(self, allocation_id: str, reply: "ControlReply") -> None:
            ray.get(self._actor(allocation_id).deliver_control_reply.remote(reply),
                    timeout=self._command_timeout_s)

    class RayHostEndpoint(_EndpointBase):
        _entrypoint = "demiflow-worker-actor"

        def __init__(self, lease: "WorkerLease", *, services: "DriverServices"):
            super().__init__(lease, services=services)
            from ..resources.manager import ManagerBounds, ResourceManager
            from ..resources.ray import RayResourceProvider
            from .resource_state import RunResourceState
            run = services.action.context
            self._connector = DataflowRayConnector(command_timeout_s=services.policy.control_timeout_ms / 1000.0)
            self._provider = RayResourceProvider(self._connector)
            self._state = RunResourceState(run.journal)
            self._manager = ResourceManager(self._provider, self._state, bounds=ManagerBounds())
            self._argv = (sys.executable, "-B", "-m", "demiflow.execution.worker.dataflow_host")

        def activate(self, assignment: "WorkerAssignment") -> None:
            from .worker.dataflow_host import make_worker_control_assignment
            launch_spec, grant, manifest_ref = self._build_launch(assignment)
            self._grant = grant
            self._scope_id = assignment.action_id
            self._connector.stage(self._allocation_id, launch_spec)
            self._open_scope(self._scope_id, "ray")
            self._apply(self._scope_id, self._resource_request(assignment, self._scope_id, manifest_ref["sha256"]))
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
            result = self._connector.handle(self._allocation_id, self._k3_envelope(kind, payload))
            if "error" in result:
                raise DataflowError("INTERNAL_PROTOCOL", "worker command failed: " + str(result["error"]))
            return result.get("receipt")

        def poll(self, *, max_events: int, deadline_ms: int) -> tuple["WorkerEvent", ...]:
            try:
                return tuple(self._connector.pull(self._allocation_id, limit=max_events))
            except (RayActorError, GetTimeoutError):
                return ()

        def deliver_reply(self, reply: "ControlReply") -> None:
            self._connector.deliver_control_reply(self._allocation_id, reply)