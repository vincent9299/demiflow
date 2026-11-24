"""Local and Ray endpoints, actual process connections and bounded transport."""
from __future__ import annotations

from demiflow.execution.contracts.worker import COORD_COMMANDS, next_worker_state, apply_assignment, validate_message
from demiflow.execution.contracts.envelope import check_boot, check_controller_epoch, check_grant
from demiflow.execution.contracts.errors import ProtocolError
from demiflow.execution.worker.control_receipts import OperationReceipts

import importlib.util
import json
import sys
import time
import uuid
from collections import deque
from contextlib import contextmanager, ExitStack
from dataclasses import asdict
from typing import TYPE_CHECKING

from demiflow.execution.contracts.dataflow_codec import encode_wire, decode_wire, event_from_dict as _event_from_dict, reply_frame as _reply_frame
from demiflow.execution.contracts.dataflow_errors import DataflowError
from demiflow.execution.hosts.worker_bootstrap import LocalControlConnector

if TYPE_CHECKING:
    from demiflow.execution.contracts.dataflow import Bounds, ControlReply, CutRequest, DriverServices, WorkerAssignment, WorkerEvent, WorkerLease

_RAY_AVAILABLE = importlib.util.find_spec("ray") is not None


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
        from demiflow.execution.contracts.dataflow import Bounds
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
        root = str(Path(__file__).resolve().parents[3])
        inherited = os.environ.get("PYTHONPATH", "")
        payload["extra_env"]["PYTHONPATH"] = root + (os.pathsep + inherited if inherited else "")
        return payload

    def request_cut(self, allocation_id: str, request: "CutRequest") -> None:
        from demiflow.execution.hosts.worker_bootstrap import send_message
        bounds = self._bounds_by_allocation[allocation_id]
        send_message(self.driver_socks[allocation_id], {"type": "cut_request", "wire":
            encode_wire(request, type_name="CutRequest", bounds=bounds).decode()})

    def deliver_control_reply(self, allocation_id: str, reply: "ControlReply") -> None:
        from demiflow.execution.hosts.worker_bootstrap import send_message
        bounds = self._bounds_by_allocation[allocation_id]
        send_message(self.driver_socks[allocation_id], _reply_frame(reply, bounds=bounds))


@contextmanager
def batch_business_commands(endpoints):
    """Flush already-ready commands before the original driver advances cuts."""
    with ExitStack() as stack:
        for endpoint in endpoints:
            scope = getattr(endpoint, 'batch_business', None)
            if scope is not None:
                stack.enter_context(scope())
        yield


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
        self._business_batch = None
        self._business_bytes = 0
        self._activity_generation = 0

    def _build_launch(self, assignment: "WorkerAssignment") -> tuple[dict, dict, dict]:
        run = self._services.action.context
        lease = self._lease
        from demiflow.execution.contracts.dataflow_codec import runtime_scope_id
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
        def wire_ref(value, type_name, media_type):
            raw = encode_wire(value, type_name=type_name, bounds=assignment.bounds)
            # The driver's existing writer lease protects these immutable
            # carriers through worker startup. Historical state refs keep
            # their original journal roots; launch carriers can be collected
            # after physical worker cleanup like other unrooted metadata.
            return asdict(self._services.store.put(iter([raw]),
                max_bytes=assignment.bounds.max_control_bytes, media_type=media_type))
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
                            "assignment": {"ref": wire_ref(assignment, "WorkerAssignment",
                                "application/x-demiflow-worker-assignment-v3")}},
        }
        if getattr(self, "_recovery_image", None) is not None:
            launch_spec["dataflow_v3"]["recovery_image_ref"] = wire_ref(self._recovery_image,
                "RecoveryImage", "application/x-demiflow-recovery-image-v3")
        return launch_spec, grant, manifest_ref

    def prepare(self, assignment: "WorkerAssignment", *, recovery_image=None) -> None:
        if self._assignment is not None:
            if self._assignment != assignment:
                raise DataflowError("FENCED", "allocation assignment changed after staging")
            return
        self._recovery_image = recovery_image
        launch, grant, _ = self._build_launch(assignment)
        self._grant = grant
        self._scope_id = grant["scope_id"]
        self._assignment = assignment
        self._connector.stage(self._allocation_id, launch)

    def discard_prepared(self) -> None:
        """Release staged launch data before apply or after confirmed scope cleanup."""
        if not self._activated:
            self._services.journal_service.discard_prepared_worker(self._lease)
            self._connector.abort(self._allocation_id)

    def _check_event_identity(self, event):
        assignment = self._assignment
        if (event.run_id != assignment.run_id or event.attempt != assignment.attempt
                or event.fence != assignment.fence or event.allocation_id != self._allocation_id
                or event.boot_id != self._boot_id or event.plan_revision != assignment.plan_revision):
            raise DataflowError("FENCED", "Worker event differs from its allocation/attempt")

    def _k3_envelope(self, kind: str, payload: dict) -> dict:
        from demiflow.execution.contracts.envelope import WORKER_PROTOCOL_VERSION
        run = self._services.action.context
        return {"protocol_version": dict(WORKER_PROTOCOL_VERSION), "kind": kind,
                "scope_id": self._scope_id, "run_id": run.ref.run_id, "operation_id": uuid.uuid4().hex,
                "controller_epoch": run.attempt, "authority_ref": self._grant["grant_id"],
                "allocation_id": self._allocation_id, "boot_id": self._boot_id, "payload": payload}

    @contextmanager
    def batch_business(self):
        if self._business_batch is not None:
            yield
            return
        self._business_batch = []
        try:
            yield
        finally:
            try:
                self.flush_business()
            finally:
                self._business_batch = None
                self._business_bytes = 0

    def flush_business(self):
        pending = getattr(self, '_business_batch', None)
        if not pending:
            return
        # A failed/unknown send is never retried as if it were unsent. The
        # original execution failure/recovery path owns that outcome.
        self._business_batch = []
        self._business_bytes = 0
        if hasattr(self, "_sock"):
            from demiflow.execution.hosts.worker_bootstrap import _send_encoded_messages
            _send_encoded_messages(self._sock, (raw for _, raw in pending))
        else:
            actor = self._connector._actor(self._allocation_id)
            ray.get(actor.receive_many.remote(tuple(command for command, _ in pending)),
                    timeout=self._services.policy.control_timeout_ms / 1000)

    def _activity_command(self, command):
        # This is a live, per-boot control watermark, never a data identity or
        # durable frontier. ACK/receipt/lease traffic does not admit work and
        # must not repeatedly invalidate an otherwise current idle report.
        if command.get('kind') not in ('data', 'finite_input', 'edge_eoi',
                'close_output', 'satisfy', 'pause', 'park', 'resume', 'park_policy'):
            return command
        generation = getattr(self, '_activity_generation', 0) + 1
        if generation >= 2**63:
            raise DataflowError('BUDGET_EXCEEDED', 'worker activity generation exhausted')
        return dict(command, activity_generation=generation)

    def send_business(self, command):
        from demiflow.execution.hosts.worker_bootstrap import _send_encoded_messages, _MAX_CONTROL_BYTES, _CONTROL_BATCH_EVENTS
        from demiflow.execution.contracts.recovery_contract import document
        command = self._activity_command(command)
        raw = document({"type": "dataflow_command", "command": command})
        if getattr(self, '_business_batch', None) is None:
            self._activity_generation = command.get('activity_generation', getattr(self, '_activity_generation', 0))
            if hasattr(self, '_sock'):
                _send_encoded_messages(self._sock, (raw,))
            else:
                ray.get(self._connector._actor(self._allocation_id).receive.remote(command),
                        timeout=self._services.policy.control_timeout_ms / 1000)
            return
        size = len(raw) + 4
        if (self._business_batch and self._business_bytes + size > _MAX_CONTROL_BYTES
                or len(self._business_batch) >= min(_CONTROL_BATCH_EVENTS, self._services.bounds.max_pending_events)):
            self.flush_business()
        self._business_batch.append((command, raw))
        self._activity_generation = command.get('activity_generation', getattr(self, '_activity_generation', 0))
        self._business_bytes += size
        if command.get('kind') not in ('data', 'ack') or self._business_bytes >= _MAX_CONTROL_BYTES:
            self.flush_business()

    def _early_event(self, event):
        self._check_event_identity(event)
        if event.kind in ("ready", "data", "ack", "idle", "terminal", "eoi", "cut", "failure", "closed", "progress", "checkpoint_wait"):
            if len(self._events) >= self._services.bounds.max_pending_events:
                raise DataflowError("BUDGET_EXCEEDED", "activation event queue exceeds bound")
            self._events.append(event)
        else:
            self.deliver_reply(self._services.journal_service.dispatch_reply(event))

    def _accept_final_observations(self, result):
        raw = result.get('final_progress', [])
        from demiflow.execution.contracts.recovery_contract import document
        document(raw, self._services.bounds.max_control_bytes)
        if type(raw) is not list:
            raise DataflowError('INVALID_CONTRACT', 'final observations must be a bounded list')
        for item in raw:
            event = _event_from_dict(item, bounds=self._services.bounds)
            if event.kind != 'progress' or not any(event.payload.get(field) is not None
                    for field in ('actor_lifecycle', 'callback_observations', 'output_handoff',
                                  'prompt_usage', 'model_observations')):
                raise DataflowError('INVALID_CONTRACT', 'cleanup only carries actual worker progress')
            self._early_event(event)

    def take_cleanup_observations(self):
        # Preserve unrelated events: cleanup does not manufacture their ACKs.
        remaining = deque()
        observed = []
        while self._events:
            event = self._events.popleft()
            (observed if event.kind == 'progress' else remaining).append(event)
        self._events = remaining
        return tuple(observed)

    def request_cut(self, request: "CutRequest") -> None:
        self.flush_business()
        if not self._activated:
            raise DataflowError("FENCED", "cut requested before activation")
        existing = self._cuts.get(request.cut_id)
        if existing is not None:
            if existing != request:
                raise DataflowError("INVALID_CONTRACT", "conflicting cut identity")
            return
        self._cuts[request.cut_id] = request
        self._connector.request_cut(self._allocation_id, request)

    def _deliver_pending_journal_replies(self):
        # Normal pump may have submitted a paid response just before stopping.
        # Its async reply still belongs to the same original service queue.
        # K3 cleanup consumes only this endpoint's boot; other workers keep
        # their original replies and readiness notification in place.
        for completed in self._services.journal_service.poll_worker_replies(
                max_replies=self._services.bounds.max_pending_events,
                allocation_id=self._allocation_id, boot_id=self._boot_id):
            self.deliver_reply(completed.reply)

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
        self._argv = (sys.executable, "-B", "-m", 'demiflow.execution.hosts.worker_bootstrap')

    def activate(self, assignment: "WorkerAssignment") -> None:
        
        from demiflow.execution.hosts.worker_bootstrap import ControlFrameReader
        from demiflow.execution.contracts.worker import make_worker_control_assignment
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
        register = self._receive_registration()
        self._model.handle(register, now_ms=int(time.time() * 1000))
        self._control_reader = ControlFrameReader(self._sock,
            timeout_s=self._services.policy.control_timeout_ms / 1000)
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

    def _receive_registration(self):
        from demiflow.execution.hosts.worker_bootstrap import recv_message
        context = {"allocation_id": self._allocation_id, "boot_id": self._boot_id}

        def lost_registration():
            process = getattr(self._provider, '_spawned', {}).get(self._allocation_id)
            if process is not None:
                # Preserve observed exit evidence only. EOF may precede process
                # exit; it must not manufacture a signal or automatically retry.
                context['worker_exit_code'] = process.poll()
            return DataflowError("UNKNOWN_EFFECT", "local worker connection closed before registration",
                                 context=context)

        try:
            register = recv_message(self._sock)
        except (ConnectionResetError, ConnectionAbortedError) as exc:
            raise lost_registration() from exc
        except TimeoutError as exc:
            raise DataflowError("DEADLINE_EXCEEDED", "local worker registration deadline elapsed",
                                context=context) from exc
        if register is None:
            raise lost_registration()
        if not isinstance(register, dict) or register.get("kind") != "Register":
            raise DataflowError("INTERNAL_PROTOCOL", "local worker sent an invalid registration frame",
                                context=context)
        return register

    def _read_control(self, *, max_messages):
        try:
            return self._control_reader.read_ready(max_messages=max_messages)
        except (ConnectionResetError, ConnectionAbortedError) as exc:
            # A killed worker can terminate with a reset instead of EOF. Neither
            # proves whether its last operation completed; retain original
            # journal/ACK authority and never retry the unconfirmed command.
            # Framing, JSON, budget and timeout errors remain their own errors.
            raise DataflowError("UNKNOWN_EFFECT", "worker control connection terminated before confirmation") from exc

    def _send_k3(self, kind: str, payload: dict) -> dict:
        from demiflow.execution.hosts.worker_bootstrap import send_message
        self.flush_business()
        envelope = self._k3_envelope(kind, payload)
        send_message(self._sock, envelope)
        deadline = time.monotonic() + self._services.policy.control_timeout_ms / 1000
        while True:
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise DataflowError("DEADLINE_EXCEEDED", "Local worker control command timed out")
            self._deliver_pending_journal_replies()
            messages = self._read_control(max_messages=self._services.bounds.max_pending_events)
            if messages is None:
                raise DataflowError("UNKNOWN_EFFECT", "local worker closed before responding")
            response = None
            for message in messages:
                if message.get("response_to") == envelope["operation_id"]:
                    response = message
                if message.get("type") == "worker_event":
                    event = _event_from_dict(message["event"], bounds=self._services.bounds)
                    self._early_event(event)
                if message.get("kind") in ("Register", "Ready", "Heartbeat"):
                    self._model.handle(message, now_ms=int(time.time() * 1000))
            if response is not None:
                self._accept_final_observations(response)
                if "error" in response:
                    from demiflow.execution.contracts.dataflow_failures import restore_failure
                    raise restore_failure(response["error"])
                if kind in ("Drain", "Quiesce", "Cancel"):
                    self._cleanup_completed = True
                return response["receipt"]
            if not messages:
                import select
                select.select([self._sock], [], [], min(.05, remaining))

    def poll(self, *, max_events: int, deadline_ms: int):
        events = []
        while self._events and len(events) < max_events:
            events.append(self._events.popleft())
        if len(events) < max_events:
            frames = self._read_control(max_messages=max_events - len(events))
            if frames is None:
                raise DataflowError("UNKNOWN_EFFECT", "worker connection closed during execution")
            for frame in frames:
                if frame.get("type") == "worker_event":
                    events.append(_event_from_dict(frame["event"], bounds=self._services.bounds))
                elif "error" in frame:
                    raise DataflowError("INTERNAL_PROTOCOL", "worker control failed: " + str(frame["error"]))
        return tuple(events)

    def deliver_reply(self, reply: "ControlReply") -> None:
        self.flush_business()
        self._connector.deliver_control_reply(self._allocation_id, reply)


def make_host_endpoint(lease: "WorkerLease", *, services: "DriverServices"):
    host = next((h for h in services.hosts if h.host_id == lease.host_id), None)
    if host is None:
        raise DataflowError("INVALID_CONTRACT", "lease host is not a declared driver host")
    if host.provider == "local":
        if services.policy.worker_mode == "thread":
            from demiflow.execution.hosts.thread import ThreadHostEndpoint
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
    from demiflow.execution.hosts.ray_worker_host import RayControlConnector as _RayControlConnector
else:
    _RayControlConnector = object


class DataflowRayConnector(_RayControlConnector):
    """Ray actor connector hosting the v3 dataflow worker (H054)."""

    def __init__(self, *, device_topology=None, **kwargs):
        if not _RAY_AVAILABLE:
            raise DataflowError("UNSUPPORTED_CAPABILITY", "Ray Core provider requires the ray package")
        super().__init__(**kwargs)
        self._environments = {}
        self._device_topology = device_topology

    def device_host_capacity(self, host_id):
        return 0 if self._device_topology is None else self._device_topology.capacity(host_id)

    def device_host_feasible(self, host_id, profile):
        if not profile.requests.get('accelerators'):
            return True
        return self._device_topology is not None and self._device_topology.feasible(host_id, profile)

    def validate_device_binding(self, allocation_id, identity, *, accelerators,
                                device_memory_bytes=None):
        host_id = self.launch_spec(allocation_id)['dataflow_v3']['host_id']
        binding = identity.get('accelerator_binding')
        if self._device_topology is None:
            if (accelerators or type(binding) is not dict or binding.get('devices') != []
                    or binding.get('node_id') != host_id.removeprefix('ray-')):
                raise DataflowError('UNSUPPORTED_CAPABILITY', 'Worker has no verified GPU deployment grant')
            return ()
        return self._device_topology.verify(binding, host_id=host_id,
            accelerators=accelerators, device_memory_bytes=device_memory_bytes)

    def stage_environment(self, allocation_id, environment):
        self._environments[allocation_id] = dict(environment)

    def adopt(self, allocation_id, actor):
        super().adopt(allocation_id, actor)
        self._environments.pop(allocation_id, None)

    def abort(self, allocation_id):
        try:
            super().abort(allocation_id)
        finally:
            self._environments.pop(allocation_id, None)

    def close(self, allocation_id):
        try:
            super().close(allocation_id)
        finally:
            self._environments.pop(allocation_id, None)

    def host_class(self):
        from demiflow.execution.hosts.dataflow_worker_host import HostedDataflowWorker
        return ray.remote(max_restarts=0, max_task_retries=0, max_concurrency=8)(HostedDataflowWorker)

    def placement_options(self, allocation_id):
        from ray.util.scheduling_strategies import NodeAffinitySchedulingStrategy
        host_id = self.launch_spec(allocation_id)['dataflow_v3']['host_id']
        if not host_id.startswith('ray-'):
            raise DataflowError('INVALID_CONTRACT', 'Worker lacks a verified Ray host')
        options = {'scheduling_strategy': NodeAffinitySchedulingStrategy(host_id[4:], soft=False)}
        environment = self._environments.get(allocation_id)
        if environment:
            options['runtime_env'] = {'env_vars': dict(environment)}
        return options

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
        self._argv = (sys.executable, "-B", "-m", 'demiflow.execution.hosts.dataflow_worker_host')
        self._pending_pull = None
        self._pull_limit = 0
        self._pull_started = 0.0

    def _pull_ready(self, *, limit, wait_s=0.0, start=True):
        """One bounded, non-retried mailbox read shared by pump and K3.

        An empty/remote mailbox must not serialize every other worker behind
        a round trip. The outstanding read owns the drained events until this
        endpoint consumes them; it is never replaced by a second reader.
        """
        if self._pending_pull is None:
            if not start:
                return ()
            self._pull_limit = min(limit, self._services.bounds.max_pending_events)
            if self._pull_limit < 1:
                raise DataflowError('BUDGET_EXCEEDED', 'Ray event read has no remaining credit')
            self._pull_started = time.monotonic()
            try:
                self._pending_pull = self._connector._actor(self._allocation_id).pull_events.remote(self._pull_limit)
            except (RayActorError, GetTimeoutError) as exc:
                raise DataflowError('UNKNOWN_EFFECT', 'Ray worker became unreachable') from exc
        timeout = self._services.policy.control_timeout_ms / 1000
        remaining = timeout - (time.monotonic() - self._pull_started)
        if remaining <= 0:
            raise DataflowError('UNKNOWN_EFFECT', 'Ray worker event read exceeded its deadline')
        try:
            ready, _ = ray.wait([self._pending_pull], timeout=min(wait_s, remaining))
            if not ready:
                if time.monotonic() - self._pull_started >= timeout:
                    raise DataflowError('UNKNOWN_EFFECT', 'Ray worker event read exceeded its deadline')
                return ()
            pending, self._pending_pull = self._pending_pull, None
            events = ray.get(pending)
        except (RayActorError, GetTimeoutError) as exc:
            raise DataflowError('UNKNOWN_EFFECT', 'Ray worker became unreachable') from exc
        if type(events) not in (tuple, list) or len(events) > self._pull_limit:
            raise DataflowError('INVALID_CONTRACT', 'Ray mailbox exceeded its requested event credit')
        return tuple(events)

    def prepare(self, assignment: "WorkerAssignment", *, recovery_image=None) -> None:
        if self._assignment is None:
            from demiflow.execution.hosts.environment import allocation_environment
            environment = allocation_environment(assignment, store=self._services.store,
                                                 bounds=self._services.bounds)
            self._connector.stage_environment(self._allocation_id, environment)
        try:
            super().prepare(assignment, recovery_image=recovery_image)
        except BaseException:
            self._connector.abort(self._allocation_id)
            raise

    def activate(self, assignment: "WorkerAssignment") -> None:
        from demiflow.execution.contracts.worker import make_worker_control_assignment
        if self._assignment != assignment:
            raise DataflowError("FENCED", "worker assignment was not staged before allocation")
        binding = self._services.resources.state.read_allocation_identity(self._scope_id, self._allocation_id)
        observed = binding['observation'] if binding is not None else None
        if (observed is None or observed.get('phase') != 'RUNNING'
                or observed.get('boot_id') != self._boot_id
                or observed.get('allocation_id') != self._allocation_id):
            raise DataflowError('FENCED', 'Ray worker lacks its original running allocation')
        effective = observed['effective_resources']
        self._allocation_envelope = {'allocation_id': self._allocation_id,
            'spec_hash': binding['spec_hash'], 'cpu_millis': effective['cpu_millis'],
            'memory_bytes': effective['memory_bytes'],
            'resource_directory': str(self._services.action.context.directory / 'worker-cache' / self._allocation_id)}
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
        self.flush_business()
        actor = self._connector._actor(self._allocation_id)
        command = actor.handle.remote(self._k3_envelope(kind, payload),
            resource_envelope=getattr(self, '_allocation_envelope', None))
        deadline = time.monotonic() + self._services.policy.control_timeout_ms / 1000
        while True:
            self._deliver_pending_journal_replies()
            ready, _ = ray.wait([command], timeout=0.01)
            if ready:
                result = ray.get(command)
                # A previous nonblocking poll may already own the final events.
                # Resolve it before declaring cleanup complete or dropping the
                # endpoint. Do not issue another mailbox read on this path.
                for event in self._pull_ready(limit=self._services.bounds.max_pending_events,
                        wait_s=max(0.0, deadline - time.monotonic()), start=False):
                    self._early_event(event)
                if self._pending_pull is not None:
                    raise DataflowError('UNKNOWN_EFFECT', 'Ray mailbox read did not settle before control completion')
                break
            if time.monotonic() >= deadline:
                raise DataflowError("DEADLINE_EXCEEDED", "Ray worker control command timed out")
            for event in self._pull_ready(limit=self._services.bounds.max_pending_events - len(self._events)):
                self._early_event(event)
        self._accept_final_observations(result)
        if "error" in result:
            from demiflow.execution.contracts.dataflow_failures import restore_failure
            raise restore_failure(result["error"])
        if kind in ("Drain", "Quiesce", "Cancel"):
            self._cleanup_completed = True
        return result.get("receipt")

    def poll(self, *, max_events: int, deadline_ms: int):
        events = []
        while self._events and len(events) < max_events:
            events.append(self._events.popleft())
        if len(events) < max_events:
            room = max_events - len(events)
            batch = self._pull_ready(limit=room)
            events.extend(batch[:room])
            # The caller can shrink its window while a read is in flight.
            # Retain the bounded remainder in this same endpoint, in order.
            self._events.extend(batch[room:])
        return tuple(events)

    def deliver_reply(self, reply: "ControlReply") -> None:
        self.flush_business()
        self._connector.deliver_control_reply(self._allocation_id, reply)


class WorkerProtocolModel:
    """Pure conformance model: one worker boot, no transport or scheduling.

    Commands are fenced by controller epoch/boot/grant, deduplicated by
    operation_id, and gated by the inherited state machine. Business input is
    admitted only after activate with a live grant (resource v1 activation rule).
    """

    def __init__(self, *, scope_id, run_id, allocation_id, boot_id,
                 controller_epoch, grant, plan_revision=0, capabilities=()):
        self.scope_id, self.run_id = scope_id, run_id
        self.allocation_id, self.boot_id = allocation_id, boot_id
        self.controller_epoch = controller_epoch
        self.grant = grant
        self.plan_revision = plan_revision
        self.capabilities = list(capabilities)
        self.state = "REGISTERED"
        self.receipts = OperationReceipts()
        self.assignment = None

    def handle(self, envelope, *, now_ms):
        validate_message(envelope)
        if envelope["scope_id"] != self.scope_id or envelope["run_id"] != self.run_id:
            raise ProtocolError("FENCED", "message belongs to another scope/run")
        check_controller_epoch(self.controller_epoch, envelope["controller_epoch"])
        kind, payload = envelope["kind"], envelope["payload"]
        if kind in COORD_COMMANDS or kind in ("Ready", "Heartbeat"):
            check_boot(self.boot_id, envelope["boot_id"])
        if kind in ("Register", "Ready", "Heartbeat"):
            check_boot(self.boot_id, payload["boot_id"])
        if kind == "Register":
            if payload["boot_id"] != self.boot_id or payload["allocation_id"] != self.allocation_id:
                raise ProtocolError("BOOT_FENCED", "registration boot/allocation mismatch")

        def execute():
            return self._apply(kind, payload, now_ms)

        return self.receipts.apply(envelope["operation_id"], payload, execute)

    def _apply(self, kind, payload, now_ms):
        if kind == "Register":
            return {"kind": "Registered", "state": self.state}
        if kind == "VerifyArtifactAndCapabilities":
            from demiflow.execution.contracts.envelope import require_capabilities
            require_capabilities(self.capabilities, payload["required_capabilities"])
            self.state = next_worker_state(self.state, "VERIFIED")
            return {"kind": "Verified", "state": self.state}
        if kind == "Restore":
            self.state = next_worker_state(self.state, "RESTORING")
            self.state = next_worker_state(self.state, "READY")
            return {"kind": "Restored", "state": self.state, "state_checksums": []}
        if kind == "Ready":
            if self.state == "VERIFIED":
                self.state = next_worker_state(self.state, "READY")
            return {"kind": "ReadyAck", "state": self.state}
        if kind == "Activate":
            check_grant(self.grant, controller_epoch=self.controller_epoch,
                        allocation_id=self.allocation_id, boot_id=self.boot_id, now_ms=now_ms)
            self.state = next_worker_state(self.state, "ACTIVATED")
            return {"kind": "Activated", "state": self.state}
        if kind == "Assign":
            self.plan_revision = apply_assignment(self.plan_revision, payload["assignment"])
            self.assignment = payload["assignment"]
            return {"kind": "Assigned", "plan_revision": self.plan_revision}
        if kind == "CheckpointRequest":
            return {"kind": "CheckpointAck", "state_refs": [], "inflight_desc": [],
                    "channel_cuts": []}
        if kind in ("Quiesce", "Drain"):
            if self.state == "ACTIVATED":
                self.state = next_worker_state(self.state, "DRAINING")
            return {"kind": "DrainProof", "state": self.state, "release_hint": "DRAINED"}
        if kind == "Cancel":
            if self.state != "RETIRED":
                self.state = next_worker_state(self.state, "RETIRED")
            return {"kind": "Cancelled", "state": self.state}
        if kind == "Heartbeat":
            # Liveness only: never extends control leases or admits input.
            return {"kind": "HeartbeatAck", "state": self.state}
        raise ProtocolError("INVALID_SPEC", "unhandled kind")

    def admit_business_input(self, *, now_ms):
        if self.state != "ACTIVATED":
            raise ProtocolError("FENCED", "business input requires activation")
        check_grant(self.grant, controller_epoch=self.controller_epoch,
                    allocation_id=self.allocation_id, boot_id=self.boot_id, now_ms=now_ms)
        return True
