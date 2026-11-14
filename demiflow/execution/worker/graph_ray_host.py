"""Ray actor graph worker host and control connector (U3/S5, exit gate 3 Ray).

Additive sibling of the U2 Ray actor host (ray_host) and the Local graph host
(graph_host): the actor hosts the *same* WorkerRuntime + GraphWorkerExecutor +
GraphWorkerProgress as the Local process graph worker. Only the host shell and the
control transport differ — driver->worker commands (K3 lifecycle, the additive
graph_checkpoint wire, contribution receipts) are synchronous actor method calls;
worker->driver events (Register/Ready/Heartbeat, CutContribution) flow through a
bounded outbox the driver pulls. The data plane is the Ray carriage (carriage_ray):
business rows flow worker->worker over Ray channel actors / ray_object plasma and
never transit the driver. max_restarts=0/max_task_retries=0: a dead actor is never
auto-replayed; host replacement is a fresh allocation driven by whole-graph restore.
"""
import threading
import time
from collections import deque
from pathlib import Path

import ray
from ray.exceptions import GetTimeoutError, RayActorError

from ..artifacts import read as _read_artifact
from ..contracts.errors import ProtocolError
from ..graph_wire import GRAPH_CHECKPOINT_SCHEMA, validate_graph_checkpoint
from .graph_execution import GraphWorkerExecutor
from .runtime import WorkerRuntime

_MAX_OUTBOX = 4096
_PULL_LIMIT = 64


@ray.remote(max_restarts=0, max_task_retries=0)
class _RayGraphWorkerHost:
    def __init__(self, launch_spec, spec_hash, boot_id):
        self._spec_hash = spec_hash
        self._boot_id = boot_id
        self._outbox = deque()
        self._lock = threading.Lock()
        self._closed = False

        def send(frame):
            with self._lock:
                if len(self._outbox) >= _MAX_OUTBOX:
                    raise RuntimeError("Worker outbox exceeded its bounded budget")
                self._outbox.append(frame)

        directory = launch_spec["directory"]

        def manifest_loader(ref):
            return _read_artifact(Path(directory) / (ref["sha256"] + ".json"))

        self.executor = GraphWorkerExecutor(launch_spec, send=send)
        self.runtime = WorkerRuntime(
            launch_spec, control=send, manifest_loader=manifest_loader,
            executor=self.executor)
        self.runtime.register()

    def identity(self):
        return {"spec_hash": self._spec_hash, "boot_id": self._boot_id}

    def handle(self, message):
        if message.get("schema_version") == GRAPH_CHECKPOINT_SCHEMA:
            self.executor.request_cut(validate_graph_checkpoint(message))
            return {"receipt": None}
        response_to = message.get("response_to")
        if response_to is not None and response_to in self.executor.pending_contributions():
            self.executor.deliver_receipt(response_to, message.get("receipt", {}))
            return {"receipt": None}
        try:
            receipt = self.runtime.handle(message, now_ms=int(time.time() * 1000))
        except ProtocolError as exc:
            return {"error": exc.to_dict()}
        if message["kind"] == "Restore":
            self.runtime.ready(message["payload"]["assignment_ref"])
        return {"receipt": receipt}

    def pull_events(self, limit):
        if self.executor.error is not None:
            raise self.executor.error
        with self._lock:
            batch = []
            while self._outbox and len(batch) < limit:
                batch.append(self._outbox.popleft())
            return batch

    def close(self):
        if self._closed:
            return
        self._closed = True
        self.executor.join(timeout_s=5)
        self.runtime.close()


class RayGraphControlConnector:
    """Actor-method control connector for the graph (mirrors RayControlConnector).

    Adds the additive graph_checkpoint command and contribution-receipt delivery on
    top of the U2 stage/adopt/handle/pull/terminate/close shape, so the graph driver
    drives the frozen K3 lifecycle and the single-commit cut handshake over actors.
    """

    def __init__(self, *, command_timeout_s=120):
        self._staged = {}
        self._actors = {}
        self._command_timeout_s = command_timeout_s

    def host_class(self):
        return _RayGraphWorkerHost

    def stage(self, allocation_id, launch_spec):
        self._staged[allocation_id] = launch_spec

    def launch_spec(self, allocation_id):
        return self._staged[allocation_id]

    def adopt(self, allocation_id, actor):
        self._actors[allocation_id] = actor
        self._staged.pop(allocation_id, None)

    def abort(self, allocation_id):
        self._staged.pop(allocation_id, None)
        self._actors.pop(allocation_id, None)

    def _actor(self, allocation_id):
        return self._actors[allocation_id]

    def handle(self, allocation_id, message):
        return ray.get(self._actor(allocation_id).handle.remote(message),
                       timeout=self._command_timeout_s)

    def send_graph_checkpoint(self, allocation_id, command):
        return ray.get(self._actor(allocation_id).handle.remote(command),
                       timeout=self._command_timeout_s)

    def send_receipt(self, allocation_id, operation_id, receipt):
        return ray.get(self._actor(allocation_id).handle.remote(
            {"response_to": operation_id, "receipt": receipt}),
            timeout=self._command_timeout_s)

    def pull(self, allocation_id, limit=_PULL_LIMIT):
        return ray.get(self._actor(allocation_id).pull_events.remote(limit),
                       timeout=self._command_timeout_s)

    def terminate(self, allocation_id):
        actor = self._actors.get(allocation_id)
        if actor is not None:
            try:
                ray.kill(actor, no_restart=True)
            except Exception:
                pass

    def close(self, allocation_id):
        actor = self._actors.pop(allocation_id, None)
        self._staged.pop(allocation_id, None)
        if actor is None:
            return
        try:
            ray.get(actor.close.remote(), timeout=10)
        except (RayActorError, GetTimeoutError):
            pass
