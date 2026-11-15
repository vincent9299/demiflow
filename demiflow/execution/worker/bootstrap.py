"""Worker host entry and Local control connector (U2/S3).

Local control channel = an inherited socketpair carrying length-prefixed strict
JSON (``document()``), bounded. Driver->worker frames are K3 command envelopes;
worker->driver frames are K3 events (Register/Ready/Heartbeat) and command
responses correlated by operation_id. The Ray actor-method connector is S5.

The worker reads a private launch spec, builds a WorkerRuntime, registers, then
serves commands until Drain/Quiesce/Cancel. It never constructs a RunContext.
"""
import json
import os
import socket
import subprocess
import sys
import tempfile
import threading
import time
from dataclasses import asdict
from pathlib import Path

from ..artifacts import read
from ..contracts.errors import ProtocolError
from ..recovery_contract import RecoveryContractError, document

LAUNCH_ENV = "DEMIFLOW_WORKER_LAUNCH"
CONTROL_FD_ENV = "DEMIFLOW_WORKER_CONTROL_FD"
_MAX_CONTROL_BYTES = 1 << 20
_MAX_LAUNCH_BYTES = 1 << 16


def send_message(sock, message):
    raw = document(message)
    if len(raw) > _MAX_CONTROL_BYTES:
        raise RecoveryContractError("Control message exceeds byte budget")
    sock.sendall(len(raw).to_bytes(4, "big") + raw)


def _recv_exactly(sock, count):
    chunks = []
    remaining = count
    while remaining:
        chunk = sock.recv(remaining)
        if not chunk:
            if remaining == count:
                return None
            raise RecoveryContractError("Truncated control message")
        chunks.append(chunk)
        remaining -= len(chunk)
    return b"".join(chunks)


def recv_message(sock):
    header = _recv_exactly(sock, 4)
    if header is None:
        return None
    length = int.from_bytes(header, "big")
    if length > _MAX_CONTROL_BYTES:
        raise RecoveryContractError("Control message exceeds byte budget")
    raw = _recv_exactly(sock, length)
    if raw is None:
        raise RecoveryContractError("Truncated control message")
    return json.loads(raw)


def _manifest_loader(directory):
    def load(ref):
        return read(Path(directory) / (ref["sha256"] + ".json"))
    return load


class _SocketControlTransport:
    """Local v3 control transport over the inherited socketpair (C07).

    Business-thread request/wait; the driver drains events and delivers replies.
    Frames reuse the bounded length-prefixed JSON document channel.
    """

    def __init__(self, sock, *, send_lock, bounds):
        self._sock = sock
        self._send_lock = send_lock
        self._bounds = bounds

    def request(self, event, *, deadline_ms):
        from ..contracts.dataflow import ControlReply
        from ..contracts.dataflow_errors import DataflowError
        frame = {"type": "worker_event", "event": asdict(event), "deadline_ms": deadline_ms}
        with self._send_lock:
            send_message(self._sock, frame)
            reply = recv_message(self._sock)
        if reply is None:
            raise DataflowError("FENCED", "control channel closed before reply")
        return ControlReply(operation_id=reply["operation_id"], status=reply["status"],
                            payload=reply.get("payload"), payload_ref=None,
                            error_code=reply.get("error_code"))


def _run_v3_worker(launch_spec, sock):
    """Assemble the unified v3 worker; verify artifact/code pin before business."""
    from ..contracts.dataflow_errors import DataflowError
    from .runtime import WorkerRuntime
    send_lock = threading.Lock()

    def send(frame):
        with send_lock:
            send_message(sock, frame)

    v3 = launch_spec["dataflow_v3"]
    bounds = v3.get("bounds")
    transport = _SocketControlTransport(sock, send_lock=send_lock, bounds=bounds)
    v3 = dict(v3)
    v3["transport"] = transport
    launch_spec = dict(launch_spec)
    launch_spec["dataflow_v3"] = v3
    runtime = WorkerRuntime(
        launch_spec, control=send,
        manifest_loader=_manifest_loader(launch_spec["directory"]))
    runtime.register()
    try:
        while True:
            message = recv_message(sock)
            if message is None:
                break
            if message.get("type") == "control_reply":
                continue
            if "kind" not in message:
                continue
            operation_id = message["operation_id"]
            try:
                receipt = runtime.handle(message, now_ms=int(time.time() * 1000))
                send({"response_to": operation_id, "receipt": receipt})
            except ProtocolError as exc:
                send({"response_to": operation_id, "error": exc.to_dict()})
            kind = message["kind"]
            if kind == "Restore":
                runtime.ready(message["payload"]["assignment_ref"])
            if kind in ("Drain", "Quiesce", "Cancel"):
                break
    finally:
        runtime.close()
        sock.close()
    return runtime


def run_worker(launch_spec, sock):
    import threading

    from ..chain import COMMIT_RECEIPT_SCHEMA
    from .execution import WorkerChainExecutor
    from .queue_transform import QueueTransformExecutor, TRANSFORM_REQUEST_SCHEMA
    from .runtime import WorkerRuntime
    if "dataflow_v3" in launch_spec:
        return _run_v3_worker(launch_spec, sock)
    send_lock = threading.Lock()

    def send(frame):
        with send_lock:
            send_message(sock, frame)

    if "execution" in launch_spec:
        executor = WorkerChainExecutor(launch_spec, send=send)
    elif "queue_transform" in launch_spec:
        executor = QueueTransformExecutor(launch_spec, send=send)
    else:
        executor = None
    runtime = WorkerRuntime(
        launch_spec, control=send,
        manifest_loader=_manifest_loader(launch_spec["directory"]),
        executor=executor)
    runtime.register()
    try:
        while True:
            message = recv_message(sock)
            if message is None:
                break
            if message.get("schema_version") == COMMIT_RECEIPT_SCHEMA:
                if executor is not None:
                    executor.deliver_receipt(message)
                continue
            if message.get("schema_version") == TRANSFORM_REQUEST_SCHEMA:
                if executor is not None:
                    executor.feed(message["request_id"], message["row_ref"])
                continue
            if "kind" not in message:
                continue
            operation_id = message["operation_id"]
            try:
                receipt = runtime.handle(message, now_ms=int(time.time() * 1000))
                send({"response_to": operation_id, "receipt": receipt})
            except ProtocolError as exc:
                send({"response_to": operation_id, "error": exc.to_dict()})
            kind = message["kind"]
            if kind == "Restore":
                runtime.ready(message["payload"]["assignment_ref"])
            if kind in ("Drain", "Quiesce", "Cancel"):
                break
    finally:
        if executor is not None:
            executor.join(timeout_s=5)
        runtime.close()
        sock.close()
    return runtime


def _validate_v3_launch(launch_spec):
    """Fail before business on a wrong entrypoint or malformed code pin (B019)."""
    from ..contracts.dataflow_errors import DataflowError
    v3 = launch_spec.get("dataflow_v3")
    if not isinstance(v3, dict):
        raise DataflowError("INVALID_CONTRACT", "v3 launch payload is missing")
    if v3.get("entrypoint") != "dataflow_worker_v3":
        raise DataflowError("INVALID_CONTRACT", "unsupported v3 worker entrypoint",
                            context={"entrypoint": str(v3.get("entrypoint"))})
    pin = v3.get("code_pin")
    if not isinstance(pin, str) or len(pin) != 64:
        raise DataflowError("CODE_MISMATCH", "v3 code pin must be a 64-hex digest")
    if "assignment" not in v3:
        raise DataflowError("INVALID_CONTRACT", "v3 assignment artifact is missing")


def main():
    launch_path = os.environ[LAUNCH_ENV]
    control_fd = int(os.environ[CONTROL_FD_ENV])
    raw = Path(launch_path).read_bytes()
    if len(raw) > _MAX_LAUNCH_BYTES:
        raise RecoveryContractError("Worker launch spec exceeds byte budget")
    launch_spec = json.loads(raw)
    Path(launch_path).unlink(missing_ok=True)
    if "dataflow_v3" in launch_spec:
        _validate_v3_launch(launch_spec)
    run_worker(launch_spec, socket.socket(fileno=control_fd))


def spawn_local_worker(launch_spec, *, log_path=None):
    """Driver-side Local connector: private launch spec + inherited socketpair.

    Returns (process, driver_socket, launch_path). The managed process_unit
    spawn (unit lock, owner-alive bootstrap guard, capacity) is wired by chain.py
    in S4; this exercises the real worker process lifecycle over a real channel.
    """
    driver_sock, child_sock = socket.socketpair()
    descriptor, launch_path = tempfile.mkstemp(prefix="demiflow-worker-launch-", suffix=".json")
    try:
        with os.fdopen(descriptor, "wb") as stream:
            stream.write(document(launch_spec))
        os.chmod(launch_path, 0o600)
        env = dict(os.environ)
        env[LAUNCH_ENV] = launch_path
        env[CONTROL_FD_ENV] = str(child_sock.fileno())
        argv = [sys.executable, "-B", "-m", "demiflow.execution.worker.bootstrap"]
        stdout = subprocess.DEVNULL if log_path is None else open(log_path, "ab")
        try:
            process = subprocess.Popen(
                argv, env=env, stdin=subprocess.DEVNULL, stdout=stdout,
                stderr=subprocess.STDOUT, pass_fds=(child_sock.fileno(),),
                start_new_session=True)
        finally:
            if log_path is not None:
                stdout.close()
    except BaseException:
        driver_sock.close()
        Path(launch_path).unlink(missing_ok=True)
        raise
    finally:
        child_sock.close()
    return process, driver_sock, launch_path


class LocalControlConnector:
    """Socketpair control connector for the managed Local process unit (S4).

    The chain stages a private worker launch spec per allocation; the resource
    provider asks for the spawn payload (inherited child fd + launch env) just
    before ``process_unit.spawn`` performs the supervised Popen, then confirms
    the spawn so the driver keeps its end and releases the child end. This is the
    Local half of the provider-specific control connector (Ray actor methods are
    S5); the worker bootstrap/runtime/progress code is identical across providers.
    """

    def __init__(self):
        self._staged = {}
        self._pending = {}
        self.driver_socks = {}

    def stage(self, allocation_id, launch_spec):
        self._staged[allocation_id] = launch_spec

    def spawn_payload(self, allocation_id):
        launch_spec = self._staged.pop(allocation_id)
        driver_sock, child_sock = socket.socketpair()
        descriptor, launch_path = tempfile.mkstemp(prefix="demiflow-worker-launch-", suffix=".json")
        try:
            with os.fdopen(descriptor, "wb") as stream:
                stream.write(document(launch_spec))
            os.chmod(launch_path, 0o600)
        except BaseException:
            driver_sock.close()
            child_sock.close()
            Path(launch_path).unlink(missing_ok=True)
            raise
        self._pending[allocation_id] = (driver_sock, child_sock, launch_path)
        return {"extra_env": {LAUNCH_ENV: launch_path, CONTROL_FD_ENV: str(child_sock.fileno())},
                "extra_pass_fds": (child_sock.fileno(),)}

    def spawned(self, allocation_id):
        driver_sock, child_sock, launch_path = self._pending.pop(allocation_id)
        child_sock.close()
        self.driver_socks[allocation_id] = driver_sock
        return driver_sock

    def abort(self, allocation_id):
        pending = self._pending.pop(allocation_id, None)
        self._staged.pop(allocation_id, None)
        if pending is not None:
            driver_sock, child_sock, launch_path = pending
            driver_sock.close()
            child_sock.close()
            Path(launch_path).unlink(missing_ok=True)

    def close(self, allocation_id):
        sock = self.driver_socks.pop(allocation_id, None)
        if sock is not None:
            sock.close()
        self.abort(allocation_id)


if __name__ == "__main__":
    main()