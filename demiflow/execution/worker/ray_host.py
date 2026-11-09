"""Ray actor worker host and control connector (U2/S5, D3 single worker host).

The actor hosts the *same* WorkerRuntime + WorkerChainExecutor + WorkerStreamProgress
as the Local process worker (design §8: worker bootstrap/runtime/progress/stream are
byte-identical across providers). Only the host shell and the control transport
differ: driver->worker commands are synchronous actor method calls; worker->driver
events (Register/Ready/Heartbeat) and CommitCut requests flow through a bounded
outbox the driver pulls. CommitReceipt returns via an actor method that unblocks the
execution thread's lock-step publish. This is worker-host承载差异, not an Exchange
data transfer (D3): result blocks still hand off as staging descriptors on the
shared same-host run directory.

The actor is created with max_restarts=0/max_task_retries=0 (A design §08): a dead
actor is never automatically restarted into a business replay; host replacement is
a fresh allocation driven by native recovery.
"""
import threading
import time
from collections import deque
from pathlib import Path

import ray
from ray.exceptions import GetTimeoutError, RayActorError

from ..artifacts import read as _read_artifact
from ..contracts.errors import ProtocolError
from .execution import WorkerChainExecutor
from .runtime import WorkerRuntime

_MAX_OUTBOX = 4096
_PULL_LIMIT = 64


@ray.remote(max_restarts=0, max_task_retries=0)
class _RayWorkerHost:
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

        self.executor = (WorkerChainExecutor(launch_spec, send=send)
                         if "execution" in launch_spec else None)
        self.runtime = WorkerRuntime(
            launch_spec, control=send, manifest_loader=manifest_loader,
            executor=self.executor)
        self.runtime.register()

    def identity(self):
        return {"spec_hash": self._spec_hash, "boot_id": self._boot_id}

    def handle(self, message):
        try:
            receipt = self.runtime.handle(message, now_ms=int(time.time() * 1000))
        except ProtocolError as exc:
            return {"error": exc.to_dict()}
        if message["kind"] == "Restore":
            self.runtime.ready(message["payload"]["assignment_ref"])
        return {"receipt": receipt}

    def pull_events(self, limit):
        with self._lock:
            batch = []
            while self._outbox and len(batch) < limit:
                batch.append(self._outbox.popleft())
            return batch

    def deliver_receipt(self, receipt):
        if self.executor is not None:
            self.executor.deliver_receipt(receipt)

    def close(self):
        if self._closed:
            return
        self._closed = True
        if self.executor is not None:
            self.executor.join(timeout_s=5)
        self.runtime.close()


class RayControlConnector:
    """Actor-method control connector injected into RayResourceProvider (S5).

    Mirrors the Local LocalControlConnector staging/adopt/close shape, but the
    spawn payload is the launch spec handed to the actor constructor and the
    driver end is a set of actor methods plus a bounded outbox pull instead of an
    inherited socketpair. The provider creates the named actor and adopts it here
    so the chain can drive the frozen K3 lifecycle and the commit handshake.
    """

    def __init__(self, *, command_timeout_s=120):
        self._staged = {}
        self._actors = {}
        self._command_timeout_s = command_timeout_s

    def host_class(self):
        return _RayWorkerHost

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

    def pull(self, allocation_id, limit=_PULL_LIMIT):
        return ray.get(self._actor(allocation_id).pull_events.remote(limit),
                       timeout=self._command_timeout_s)

    def deliver_receipt(self, allocation_id, receipt):
        ray.get(self._actor(allocation_id).deliver_receipt.remote(receipt),
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