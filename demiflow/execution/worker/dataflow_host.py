"""Hosted dataflow worker shell for Local process and Ray actor (H038-H053, H060).

The same shell hosts the v3 WorkerRuntime + DataflowWorkerExecutor + bounded
ControlMailbox in both providers. The constructor opens no driver database and
runs no user actor code; the executor is assembled by the runtime on Assign from
the sealed launch payload. K3 lifecycle commands flow through WorkerRuntime
without bypassing its state machine; worker->driver events flow through the
bounded mailbox the driver drains and answers. A lost effect confirmation stays
UNKNOWN_EFFECT and is never silently replayed.
"""
from __future__ import annotations

import hashlib
import threading
import time
import uuid
from collections import deque
from pathlib import Path
from typing import TYPE_CHECKING

from ..artifacts import read as _read_artifact
from ..contracts.dataflow_errors import DataflowError
from ..contracts.envelope import WORKER_PROTOCOL_VERSION
from ..contracts.errors import ProtocolError
from ..contracts.worker import validate_assignment
from demiflow.execution.worker.mailbox import ControlMailbox
from .runtime import WorkerRuntime

if TYPE_CHECKING:
    from ..contracts.dataflow import ControlReply, CutRequest, WorkerAssignment, WorkerEvent

_MAX_CONTROL_OUTBOX = 4096


def make_worker_control_assignment(assignment: "WorkerAssignment", *, grant: dict,
                                   manifest_ref: dict, ready_deadline_ms: int) -> dict:
    lease = assignment.lease
    stages = [stage.stage_id for stage in assignment.stages]
    # K3 identifiers exclude ':'; keep the physical v3 IDs in the sealed
    # assignment and use a deterministic transport-only projection here.
    from ..recovery_contract import identity
    partitions = [identity(["dataflow-k3-partition-v3", value]) for value in lease.partition_ids]
    inputs = [{"channel_id": exchange.channel_id} for exchange in assignment.inputs]
    outputs = [exchange.channel_id for exchange in assignment.outputs]
    restore_point = None
    if assignment.restore_refs:
        restore_point = {"checkpoint_id": assignment.restore_refs[0].digest}
    control = {
        "schema_version": "demiflow.worker_assignment.v1",
        "assignment_id": assignment.action_id,
        "operation_id": uuid.uuid4().hex,
        "scope_id": grant["scope_id"],
        "allocation_id": lease.allocation_id,
        "boot_id": lease.boot_id,
        "worker_id": lease.host_id,
        "controller_epoch": assignment.attempt,
        "grant_ref": grant["grant_id"],
        "plan_revision": assignment.plan_revision,
        "expected_plan_revision": assignment.plan_revision,
        "artifact_manifest_ref": dict(manifest_ref),
        "stages": stages,
        "partitions": partitions,
        "channels": {"inputs": inputs, "outputs": outputs},
        "restore_point": restore_point,
        "route_epoch": 0,
        "topology_revision": 0,
        "ready_deadline_ms": int(ready_deadline_ms),
    }
    return validate_assignment(control)


class HostedDataflowWorker:
    def __init__(self, launch_spec: dict, spec_hash: str, boot_id: str):
        self._spec_hash = spec_hash
        self._boot_id = boot_id
        self._resource_envelope = None
        self._closed = False
        self._activated = False
        self._cancel_event = threading.Event()
        self._lock = threading.Lock()
        self._control_outbox: deque = deque()
        config = dict(launch_spec.get("dataflow_v3") or {})
        self._ray_host = str(config.get('host_id', '')).startswith('ray-')
        self._resource_directory = (str(Path(config['cache']) / launch_spec['allocation_id'])
                                    if self._ray_host else None)
        if self._ray_host:
            import ray
            import os
            if not ray.get_runtime_context().get_accelerator_ids().get('GPU'):
                # Ray may leave visibility unset for a zero-GPU actor. Narrow
                # this owned shell before any business import or activation.
                os.environ['CUDA_VISIBLE_DEVICES'] = ''
        from ..contracts.dataflow import Bounds
        bounds = Bounds(**config["bounds"])

        def check_stop():
            if self._cancel_event.is_set():
                raise DataflowError("CANCELLED", "worker has been cancelled")

        self._mailbox = ControlMailbox(bounds=bounds, check_stop=check_stop)
        from ..storage.objects import RunArtifactStore
        self._mailbox.check_stop = check_stop
        self._mailbox.store = RunArtifactStore(root=Path(config["store_root"]),
            store_id=config["store_id"], journal=None, bounds=bounds)
        config["mailbox"] = self._mailbox
        config["transport"] = self._mailbox
        launch_spec = dict(launch_spec)
        launch_spec["dataflow_v3"] = config
        directory = launch_spec["directory"]

        def manifest_loader(ref):
            return _read_artifact(Path(directory) / (ref["sha256"] + ".json"))

        def control(envelope):
            with self._lock:
                if len(self._control_outbox) >= _MAX_CONTROL_OUTBOX:
                    raise DataflowError("BUDGET_EXCEEDED", "worker control outbox exceeded its budget")
                self._control_outbox.append(envelope)

        self._runtime = WorkerRuntime(launch_spec, control=control, manifest_loader=manifest_loader)
        self._runtime.register()

    @property
    def _executor(self):
        return getattr(self._runtime, "_v3_executor", None)

    def identity(self) -> dict:
        result = {"spec_hash": self._spec_hash, "boot_id": self._boot_id}
        if self._ray_host:
            from demiflow.execution.hosts.ray_devices import worker_device_binding
            result['accelerator_binding'] = worker_device_binding()
        return result

    def activate(self, assignment: "WorkerAssignment") -> None:
        from ..contracts.dataflow_codec import encode_wire
        grant = self._runtime.grant
        if grant is None:
            raise DataflowError("INVALID_CONTRACT", "worker has no delivered grant")
        manifest_ref = {"manifest_id": assignment.sealed.code_pin_ref.digest,
                        "sha256": assignment.sealed.code_pin_ref.digest}
        v3_config = getattr(self._runtime, "_v3_config", None)
        if v3_config is not None:
            v3_config["assignment"] = assignment
        control = make_worker_control_assignment(
            assignment, grant=grant, manifest_ref=manifest_ref,
            ready_deadline_ms=int(assignment.bounds.timeout_ms) + 60000)
        now_ms = int(time.time() * 1000)
        self._command("VerifyArtifactAndCapabilities",
                      {"artifact_manifest_ref": manifest_ref, "required_capabilities": []}, now_ms)
        self._command("Restore", {"assignment_ref": assignment.action_id,
                                  "restore_point": control["restore_point"], "state_refs": []}, now_ms)
        self._command("Activate", {"grant_ref": grant["grant_id"],
                                   "controller_epoch": assignment.attempt,
                                   "operation_id": uuid.uuid4().hex}, now_ms)
        self._command("Assign", {"assignment": control}, now_ms)
        self._activated = True

    def _command(self, kind: str, payload: dict, now_ms: int) -> dict:
        envelope = {
            "protocol_version": dict(WORKER_PROTOCOL_VERSION), "kind": kind,
            "scope_id": self._runtime.scope_id, "run_id": self._runtime.run_id,
            "operation_id": uuid.uuid4().hex, "controller_epoch": self._runtime.controller_epoch,
            "authority_ref": self._runtime.grant["grant_id"] if self._runtime.grant else self._boot_id,
            "allocation_id": self._runtime.allocation_id, "boot_id": self._boot_id, "payload": payload,
        }
        return self._runtime.handle(envelope, now_ms=now_ms)

    def _final_observations(self, message, executor):
        if message.get('kind') not in ('Drain', 'Quiesce', 'Cancel'):
            return {}
        events = getattr(executor, '_final_progress', ())
        return {'final_progress': list(events)} if events else {}

    def _bind_resource_envelope(self, value):
        """Project the existing allocation; this grants no new resources."""
        from ...resources.capacity import allocation_envelope
        if not self._ray_host:
            raise DataflowError('FENCED', 'only the Ray host borrows a delivered resource envelope')
        with allocation_envelope(value):  # original closed shape and byte bound
            if (value['allocation_id'] != self._runtime.allocation_id
                    or value['spec_hash'] != self._spec_hash
                    or value['resource_directory'] != self._resource_directory):
                raise DataflowError('FENCED', 'Ray envelope differs from original allocation identity')
            import ray
            actual = ray.get_runtime_context().get_assigned_resources()
            if (type(value['cpu_millis']) is not int or value['cpu_millis'] <= 0
                    or type(value['memory_bytes']) is not int or value['memory_bytes'] <= 0
                    or value['cpu_millis'] != round(actual.get('CPU', 0) * 1000)
                    or value['memory_bytes'] != actual.get('memory', 0)):
                raise DataflowError('FENCED', 'Ray envelope differs from actual assigned CPU or memory')
        with self._lock:
            if self._closed:
                raise DataflowError('FENCED', 'Ray resource envelope belongs to a closed worker')
            if self._resource_envelope is not None and self._resource_envelope != value:
                raise DataflowError('FENCED', 'Ray resource envelope changed after binding')
            self._resource_envelope = dict(value)

    def _allocation_context(self):
        from contextlib import nullcontext
        from ...resources.capacity import allocation_envelope
        return (allocation_envelope(self._resource_envelope)
                if self._resource_envelope is not None else nullcontext())

    def handle(self, message: dict, *, resource_envelope=None) -> dict:
        if resource_envelope is not None:
            self._bind_resource_envelope(resource_envelope)
        if (self._ray_host and self._resource_envelope is None
                and message.get('kind') in {'VerifyArtifactAndCapabilities', 'Restore', 'Activate', 'Assign'}):
            raise DataflowError('FENCED', 'Ray business activation requires its original resource envelope')
        # GraphRunner copies this context into its business thread. Ray's actor
        # pool threads return to their previous context after each command.
        with self._allocation_context():
            return self._handle(message)

    def _handle(self, message: dict) -> dict:
        response_to = message.get("response_to")
        if response_to is not None:
            self.deliver_receipt(message.get("receipt", {}))
            return {"receipt": None}
        executor = self._executor
        try:
            receipt = self._runtime.handle(message, now_ms=int(time.time() * 1000))
            if message["kind"] in ("Drain", "Quiesce", "Cancel"):
                self._runtime.close()
        except ProtocolError as exc:
            return {"error": exc.to_dict(), **self._final_observations(message, executor)}
        except Exception as exc:
            from demiflow.execution.contracts.dataflow_failures import failure_payload
            return {"error": failure_payload(exc), **self._final_observations(message, executor)}
        if message["kind"] == "Restore":
            self._runtime.ready(message["payload"]["assignment_ref"])
        if message["kind"] == "Assign":
            self._activated = True
        return {"receipt": receipt, **self._final_observations(message, executor)}

    def poll(self, *, max_events: int, deadline_ms: int) -> tuple["WorkerEvent", ...]:
        return self.pull_events(max_events)

    def pull_events(self, limit: int) -> tuple["WorkerEvent", ...]:
        # K3 Register/Ready are lifecycle envelopes, never scheduling ReadyWork.
        return self._mailbox.drain(max_events=limit)

    def pull_control(self, limit: int) -> tuple[dict, ...]:
        if type(limit) is not int or limit < 1:
            raise DataflowError("INVALID_CONTRACT", "pull limit must be positive")
        with self._lock:
            batch = []
            while self._control_outbox and len(batch) < limit:
                batch.append(self._control_outbox.popleft())
        return tuple(batch)

    def request_cut(self, request: "CutRequest") -> None:
        if not self._activated:
            raise DataflowError("FENCED", "cut requested before activation")
        executor = self._executor
        if executor is None:
            raise DataflowError("FENCED", "worker executor is not assigned")
        executor.request_cut(request)

    def receive(self, command):
        self._runtime._v3_ensure_executor().receive(command)

    def receive_many(self, commands):
        """One transport RPC; every original command retains its own receive."""
        from .bootstrap import _CONTROL_BATCH_EVENTS, _MAX_CONTROL_BYTES
        from ..recovery_contract import document
        if type(commands) not in (tuple, list) or not 1 <= len(commands) <= min(
                _CONTROL_BATCH_EVENTS, self._mailbox._bounds.max_pending_events):
            raise DataflowError('BUDGET_EXCEEDED', 'control batch exceeds event budget')
        total = 0
        for command in commands:
            raw = document({'type': 'dataflow_command', 'command': command})
            total += len(raw) + 4
            if total > _MAX_CONTROL_BYTES and len(commands) != 1:
                raise DataflowError('BUDGET_EXCEEDED', 'control batch exceeds byte budget')
        for command in commands:
            self.receive(command)

    def deliver_receipt(self, receipt: dict) -> None:
        if receipt.get("boot_id") not in (None, self._boot_id):
            raise DataflowError("FENCED", "commit receipt belongs to a different boot")
        executor = self._executor
        if executor is None:
            raise DataflowError("FENCED", "worker executor is not assigned")
        commit_id = receipt.get("commit_id")
        cut_id = receipt.get("cut_id")
        if type(commit_id) is not str or type(cut_id) is not str:
            raise DataflowError("INVALID_CONTRACT", "commit receipt needs commit_id and cut_id")
        executor.accept_receipt(commit_id, cut_id=cut_id)

    def deliver_control_reply(self, reply: "ControlReply") -> None:
        self._mailbox.deliver(reply)

    def cancel(self, reason: str, *, deadline_ms: int) -> None:
        self._cancel_event.set()
        executor = self._executor
        if executor is not None:
            executor.cancel(reason, deadline_ms=deadline_ms)
        self._mailbox.close()

    def closed(self) -> bool:
        executor = self._executor
        if executor is None:
            return self._closed
        return bool(getattr(executor, "_closed", False))

    def close(self) -> None:
        with self._lock:
            if self._closed:
                return
            self._closed = True
        self._cancel_event.set()
        self._mailbox.close()
        try:
            with self._allocation_context():
                self._runtime.close()
        finally:
            self._control_outbox.clear()
            self._resource_envelope = None
