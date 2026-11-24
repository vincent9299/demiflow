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



import threading


from collections import deque
from pathlib import Path
from typing import TYPE_CHECKING

from demiflow.execution.storage.artifacts import read as _read_artifact
from demiflow.execution.contracts.dataflow_errors import DataflowError



from demiflow.execution.worker.mailbox import ControlMailbox
from demiflow.execution.worker.runtime import WorkerRuntime

if TYPE_CHECKING:
    from demiflow.execution.contracts.dataflow import ControlReply, CutRequest, WorkerAssignment, WorkerEvent

_MAX_CONTROL_OUTBOX = 4096





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
        from demiflow.execution.contracts.dataflow import Bounds
        bounds = Bounds(**config["bounds"])

        def check_stop():
            if self._cancel_event.is_set():
                raise DataflowError("CANCELLED", "worker has been cancelled")

        self._mailbox = ControlMailbox(bounds=bounds, check_stop=check_stop)
        from demiflow.execution.storage.objects import RunArtifactStore
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

    

    def identity(self) -> dict:
        result = {"spec_hash": self._spec_hash, "boot_id": self._boot_id}
        if self._ray_host:
            from demiflow.execution.hosts.ray_devices import worker_device_binding
            result['accelerator_binding'] = worker_device_binding()
        return result

    def activate(self, assignment: "WorkerAssignment") -> None:
        self._runtime.activate_assignment(assignment)
        self._activated = True

    

    

    def _bind_resource_envelope(self, value):
        """Project the existing allocation; this grants no new resources."""
        from demiflow.resources.capacity import allocation_envelope
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
        from demiflow.resources.capacity import allocation_envelope
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
        if message.get("response_to") is not None:
            self._runtime.deliver_receipt(message.get("receipt", {}))
            return {"receipt": None}
        result = self._runtime.handle_host_command(message)
        if 'error' not in result and message['kind'] == 'Assign':
            self._activated = True
        return result

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
        self._runtime.request_cut(request)

    def receive(self, command):
        self._runtime.receive(command)

    def receive_many(self, commands):
        """One transport RPC; every original command retains its own receive."""
        from demiflow.execution.hosts.worker_bootstrap import _CONTROL_BATCH_EVENTS, _MAX_CONTROL_BYTES
        from demiflow.execution.contracts.recovery_contract import document
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
        self._runtime.deliver_receipt(receipt)

    def deliver_control_reply(self, reply: "ControlReply") -> None:
        self._mailbox.deliver(reply)

    def cancel(self, reason: str, *, deadline_ms: int) -> None:
        self._cancel_event.set()
        self._runtime.cancel_execution(reason, deadline_ms=deadline_ms)
        self._mailbox.close()

    def closed(self) -> bool:
        return self._runtime.execution_closed(default=self._closed)

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
