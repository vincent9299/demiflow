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
from .runtime import validate_worker_launch

LAUNCH_ENV = "DEMIFLOW_WORKER_LAUNCH"
CONTROL_FD_ENV = "DEMIFLOW_WORKER_CONTROL_FD"
_MAX_CONTROL_BYTES = 1 << 20
_MAX_LAUNCH_BYTES = 1 << 16
_CONTROL_BATCH_EVENTS = 64
_CONTROL_READ_CHUNK = 1 << 16


def send_message(sock, message):
    raw = document(message)
    if len(raw) > _MAX_CONTROL_BYTES:
        raise RecoveryContractError("Control message exceeds byte budget")
    sock.sendall(len(raw).to_bytes(4, "big") + raw)


def send_messages(sock, messages):
    """Coalesce only already available, unchanged length-prefixed frames.

    The packet payload ceiling is the original single-message ceiling, not
    that ceiling multiplied by the event count. A maximum-size legacy frame
    retains its four-byte header allowance and is sent alone.
    """
    _send_encoded_messages(sock, (document(message) for message in messages))


def _send_encoded_messages(sock, messages):
    """Send frames already encoded by the original strict document validator."""
    packet = bytearray()
    count = 0
    for raw in messages:
        if len(raw) > _MAX_CONTROL_BYTES:
            raise RecoveryContractError("Control message exceeds byte budget")
        size = 4 + len(raw)
        if packet and (len(packet) + size > _MAX_CONTROL_BYTES
                       or count == _CONTROL_BATCH_EVENTS):
            sock.sendall(packet)
            packet.clear()
            count = 0
        packet.extend(len(raw).to_bytes(4, 'big'))
        packet.extend(raw)
        count += 1
        if len(packet) >= _MAX_CONTROL_BYTES:
            sock.sendall(packet)
            packet.clear()
            count = 0
    if packet:
        sock.sendall(packet)


class ControlFrameReader:
    """One socket owner retains partial frames across nonblocking polls.

    Read-ahead is at most 64 KiB. A larger frame is received only up to its
    declared boundary, then decoded from the detached buffer, without copying
    that buffer. Thus pending raw bytes plus an explicit decode slice stay
    within the original frame ceiling (plus its fixed four-byte header).
    """

    def __init__(self, sock, *, timeout_s):
        self.sock = sock
        self.timeout_s = timeout_s
        self._buffer = bytearray()
        self._partial_at = None
        self._eof = False

    def read_ready(self, *, max_messages):
        import select
        if type(max_messages) is not int or max_messages < 1:
            raise RecoveryContractError('Control read needs a positive event budget')
        maximum = min(max_messages, _CONTROL_BATCH_EVENTS)
        messages, decoded_bytes, read_bytes = [], 0, 0
        while len(messages) < maximum:
            length = int.from_bytes(self._buffer[:4], 'big') if len(self._buffer) >= 4 else None
            if length is not None and length > _MAX_CONTROL_BYTES:
                raise RecoveryContractError('Control message exceeds byte budget')
            frame_bytes = None if length is None else length + 4
            if frame_bytes is not None and len(self._buffer) >= frame_bytes:
                if messages and decoded_bytes + frame_bytes > _MAX_CONTROL_BYTES:
                    break
                if len(self._buffer) == frame_bytes:
                    raw, self._buffer = self._buffer, bytearray()
                    del raw[:4]
                else:
                    # Coalesced bytes fit the read-ahead window; this slice
                    # and the remaining input together remain bounded.
                    raw = self._buffer[4:frame_bytes]
                    del self._buffer[:frame_bytes]
                messages.append(json.loads(raw))
                del raw
                decoded_bytes += frame_bytes
                self._partial_at = None
                continue
            if self._eof:
                if self._buffer:
                    raise RecoveryContractError('Truncated control message')
                return tuple(messages) if messages else None
            if self._buffer:
                if self._partial_at is None:
                    self._partial_at = time.monotonic()
                elif time.monotonic() - self._partial_at >= self.timeout_s:
                    raise TimeoutError('Partial control message deadline elapsed')
            if read_bytes >= _MAX_CONTROL_BYTES or not select.select([self.sock], [], [], 0)[0]:
                break
            needed = (_CONTROL_READ_CHUNK if frame_bytes is None
                      else min(_CONTROL_READ_CHUNK, frame_bytes - len(self._buffer)))
            needed = min(needed, _MAX_CONTROL_BYTES - read_bytes)
            chunk = self.sock.recv(needed)
            if not chunk:
                self._eof = True
                continue
            self._buffer.extend(chunk)
            read_bytes += len(chunk)
            del chunk
        return tuple(messages)


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


def _run_v3_worker(launch_spec, sock):
    """One socket reader serves replies while the command thread verifies/restores.

    A command may need the driver journal port. Reading its reply on the same
    command thread would deadlock or steal another command's frame. The socket
    loop therefore owns all reads and the bounded mailbox correlates replies.
    """
    import queue
    import select
    import socket
    from ..contracts.dataflow import Bounds
    from ..contracts.dataflow_codec import encode_wire, decode_wire
    from ..contracts.dataflow_errors import DataflowError
    from ..dataflow_hosts import ControlMailbox
    from ..storage.objects import RunArtifactStore
    from .runtime import WorkerRuntime

    config = dict(launch_spec["dataflow_v3"])
    bounds = Bounds(**config["bounds"])
    stop = threading.Event()
    send_lock = threading.Lock()
    # The business thread can need a journal RPC just after its preceding
    # reply. Wake the single socket reader instead of adding a polling tick
    # to every fenced disk allocation. This carries no payload or authority.
    wake_reader, wake_writer = socket.socketpair()
    wake_reader.setblocking(False)
    wake_writer.setblocking(False)

    def wake_outbox():
        try:
            wake_writer.send(b'\0')
        except BlockingIOError:
            pass  # A wake is already pending in the bounded kernel buffer.
        except OSError:
            if not stop.is_set():
                raise

    def check_stop():
        if stop.is_set():
            raise DataflowError("CANCELLED", "worker host is stopping")

    def send(frame):
        with send_lock:
            send_message(sock, frame)

    mailbox = ControlMailbox(bounds=bounds, check_stop=check_stop, on_outbox=wake_outbox)
    mailbox.check_stop = check_stop
    # The initial store only reads the authenticated grant; RuntimeServices
    # replaces it with the worker journal port before any write is admitted.
    mailbox.store = RunArtifactStore(root=Path(config["store_root"]),
        store_id=config["store_id"], journal=None, bounds=bounds)
    config.update(transport=mailbox, mailbox=mailbox)
    launch_spec = dict(launch_spec, dataflow_v3=config)
    runtime = WorkerRuntime(launch_spec, control=send)
    commands = queue.Queue(maxsize=bounds.max_pending_events)

    def serve_commands():
        try:
            while not stop.is_set():
                try:
                    message = commands.get(timeout=0.05)
                except queue.Empty:
                    continue
                executor = getattr(runtime, '_v3_executor', None)
                def final_observations():
                    if message.get('kind') not in ('Drain', 'Quiesce', 'Cancel'):
                        return {}
                    events = getattr(executor, '_final_progress', ())
                    return {'final_progress': list(events)} if events else {}
                try:
                    if message.get("type") == "dataflow_command":
                        runtime._v3_ensure_executor().receive(message["command"])
                        continue
                    if message.get("type") == "cut_request":
                        request = decode_wire(message["wire"].encode(), type_name="CutRequest", bounds=bounds)
                        runtime._v3_ensure_executor().request_cut(request)
                        continue
                    receipt = runtime.handle(message, now_ms=int(time.time() * 1000))
                    if message["kind"] in ("Drain", "Quiesce", "Cancel"):
                        # Acknowledgment means lifecycle cleanup finished. The
                        # main socket loop still services journal RPCs meanwhile.
                        runtime.close()
                    send({"response_to": message["operation_id"], "receipt": receipt, **final_observations()})
                    if message["kind"] == "Restore":
                        runtime.ready(message["payload"]["assignment_ref"])
                    if message["kind"] in ("Drain", "Quiesce", "Cancel"):
                        stop.set()
                except (ProtocolError, DataflowError) as exc:
                    error = exc.to_dict() if isinstance(exc, ProtocolError) else {"code": exc.code, "message": str(exc)}
                    send({"response_to": message.get("operation_id"), "error": error, **final_observations()})
                except BaseException as exc:
                    from demiflow.execution.contracts.dataflow_failures import failure_payload
                    send({"response_to": message.get("operation_id"), "error": failure_payload(exc), **final_observations()})
                finally:
                    commands.task_done()
        finally:
            stop.set()
            wake_outbox()

    runtime.register()
    reader = ControlFrameReader(sock, timeout_s=bounds.timeout_ms / 1000)
    command_thread = threading.Thread(target=serve_commands, name="dataflow-commands", daemon=True)
    command_thread.start()
    try:
        while not stop.is_set():
            messages = reader.read_ready(max_messages=bounds.max_pending_events)
            if messages is None:
                break
            for message in messages:
                if message.get("type") == "control_reply":
                    reply = decode_wire(message["wire"].encode(), type_name="ControlReply", bounds=bounds)
                    mailbox.deliver(reply)
                else:
                    try:
                        commands.put_nowait(message)
                    except queue.Full:
                        raise DataflowError("BUDGET_EXCEEDED", "worker command queue is full")
            events = mailbox.drain(max_events=min(bounds.max_pending_events, _CONTROL_BATCH_EVENTS))
            with send_lock:
                send_messages(sock, ({"type": "worker_event", "event":
                    json.loads(encode_wire(event, type_name="WorkerEvent", bounds=bounds))["payload"]}
                    for event in events))
            if messages or events:
                continue
            ready = select.select([sock, wake_reader], [], [], 0.05)[0]
            if wake_reader in ready:
                try:
                    while wake_reader.recv(4096):
                        pass
                except BlockingIOError:
                    pass
    finally:
        stop.set()
        mailbox.close()
        try:
            command_thread.join(timeout=bounds.timeout_ms / 1000)
            if not command_thread.is_alive():
                runtime.close()
        finally:
            wake_reader.close()
            wake_writer.close()
            sock.close()
    return runtime


def run_worker(launch_spec, sock):
    if "dataflow_v3" in launch_spec:
        return _run_v3_worker(launch_spec, sock)
    from .runtime import WorkerRuntime

    def send(frame):
        send_message(sock, frame)

    runtime = WorkerRuntime(
        launch_spec, control=send,
        manifest_loader=_manifest_loader(launch_spec["directory"]))
    runtime.register()
    try:
        while True:
            message = recv_message(sock)
            if message is None:
                break
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
    validate_worker_launch(launch_spec)
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
        validate_worker_launch(launch_spec)
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
