"""Graph worker host entry and Local spawn (U3/S3).

Additive sibling of the U2 bootstrap: the control plane is the same inherited
socketpair carrying length-prefixed strict JSON and the same WorkerRuntime
lifecycle. Data-plane sockets are inherited through ``pass_fds`` (the existing
ManagedProcessUnit hook) and reconstructed from the launch spec, so business
payloads flow worker->worker and never transit the driver. The loop intercepts the
additive graph_checkpoint command and CutContribution receipts; K3 itself is
unchanged. Keeping this in a separate module leaves the U2 bootstrap bytes intact.
"""
import json
import os
import socket
import subprocess
import sys
import tempfile
import threading
import time
from pathlib import Path

from ..contracts.errors import ProtocolError
from ..graph_wire import GRAPH_CHECKPOINT_SCHEMA, validate_graph_checkpoint
from ..recovery_contract import RecoveryContractError, document
from .bootstrap import CONTROL_FD_ENV, LAUNCH_ENV, _manifest_loader, recv_message, send_message
from .graph_execution import GraphWorkerExecutor
from .runtime import WorkerRuntime


def run_graph_worker(launch_spec, sock):
    send_lock = threading.Lock()

    def send(frame):
        with send_lock:
            send_message(sock, frame)

    executor = GraphWorkerExecutor(launch_spec, send=send)
    runtime = WorkerRuntime(
        launch_spec, control=send,
        manifest_loader=_manifest_loader(launch_spec["directory"]), executor=executor)
    runtime.register()
    try:
        while True:
            message = recv_message(sock)
            if message is None:
                break
            if message.get("schema_version") == GRAPH_CHECKPOINT_SCHEMA:
                executor.request_cut(validate_graph_checkpoint(message))
                continue
            response_to = message.get("response_to")
            if response_to is not None and response_to in executor.pending_contributions():
                executor.deliver_receipt(response_to, message.get("receipt", {}))
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
        executor.join(timeout_s=5)
        runtime.close()
        sock.close()
    return runtime


def main():
    launch_path = os.environ[LAUNCH_ENV]
    control_fd = int(os.environ[CONTROL_FD_ENV])
    raw = Path(launch_path).read_bytes()
    launch_spec = json.loads(raw)
    Path(launch_path).unlink(missing_ok=True)
    run_graph_worker(launch_spec, socket.socket(fileno=control_fd))


def spawn_graph_worker(launch_spec, data_fds, *, log_path=None):
    """Driver-side Local spawn: control socketpair + inherited data-channel fds.

    ``data_fds`` are the child-end data socket descriptors already embedded in the
    launch spec's graph channels; they ride pass_fds alongside the control fd. The
    driver keeps its control end and closes the child control end after spawn.
    """
    driver_sock, child_sock = socket.socketpair()
    descriptor, launch_path = tempfile.mkstemp(prefix="demiflow-graph-launch-", suffix=".json")
    try:
        with os.fdopen(descriptor, "wb") as stream:
            stream.write(document(launch_spec))
        os.chmod(launch_path, 0o600)
        env = dict(os.environ)
        env[LAUNCH_ENV] = launch_path
        env[CONTROL_FD_ENV] = str(child_sock.fileno())
        argv = [sys.executable, "-B", "-m", "demiflow.execution.worker.graph_host"]
        stdout = subprocess.DEVNULL if log_path is None else open(log_path, "ab")
        try:
            process = subprocess.Popen(
                argv, env=env, stdin=subprocess.DEVNULL, stdout=stdout,
                stderr=subprocess.STDOUT, pass_fds=(child_sock.fileno(), *data_fds),
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


if __name__ == "__main__":
    main()