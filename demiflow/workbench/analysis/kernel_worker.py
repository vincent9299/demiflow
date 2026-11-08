"""Jupyter gateway inside the helper-created kernel namespace/cgroup.

The outer controller never deserializes raw multipart/ZMQ messages. Cancellation
invalidates this kernel generation; the helper must prove the entire owned
cgroup stopped before releasing its lease. This worker cannot provide that host
proof and never claims it has cleaned descendants by shutting down ipykernel.
"""
from __future__ import annotations

from contextlib import closing
import hashlib
import ctypes
from pathlib import Path
from queue import Empty
import select
import socket
import subprocess
import time

from demiflow.workbench.analysis import contracts as c
from demiflow.workbench.analysis.framing import Frames
from demiflow.workbench.analysis.jupyter_protocol import ExecutionOutput
from demiflow.workbench.analysis.job_context import require_worker as require_job
from demiflow.workbench.analysis.job_context import job_path
from demiflow.workbench.analysis.job_context import current
from demiflow.workbench.analysis.job_context import worker_limit
from demiflow.forge.schema import ForgeError
from demiflow.workbench.analysis.resource_profile import profile

_KERNEL = profile()[0]["kernel"]
IDLE_SECONDS = _KERNEL["idle_seconds"]
LIFETIME_SECONDS = _KERNEL["lifetime_seconds"]
CELL_SECONDS = _KERNEL["cell_seconds"]
WIRE_BYTES = 8 * 1024 * 1024
WIRE_FRAMES = 16
WIRE_HWM = 4


def _receive_bounded(sock, session):
    """Check zero-copy frame views before Jupyter copies or parses JSON.

    libzmq may already hold an entire multipart message: its native allocation
    remains inside the kernel cgroup. Never drain an unbounded rejected message
    or attempt to reuse a socket stopped partway through a multipart message.
    """
    import zmq

    parts, total = [], 0
    try:
        while True:
            part = sock.recv(flags=zmq.NOBLOCK, copy=False)
            total += len(part)
            if len(part) > WIRE_BYTES or total > WIRE_BYTES:
                c.reject("Jupyter wire byte limit exceeded", code="resource_limit")
            parts.append(part)
            if not sock.getsockopt(zmq.RCVMORE):
                break
            if len(parts) >= WIRE_FRAMES:
                c.reject("Jupyter wire frame limit exceeded", code="resource_limit")
        # Both bounds have passed before feed_identities/deserialize can copy.
        _, message = session.feed_identities(parts, copy=False)
        if len(message) < 5:
            c.reject("Malformed Jupyter multipart envelope")
        return session.deserialize(message, copy=False)
    except zmq.Again:
        if not parts:
            raise Empty from None
        sock.close(linger=0)
        c.reject("Incomplete Jupyter multipart envelope")
    except ForgeError:
        sock.close(linger=0)
        raise
    except (ValueError, TypeError, KeyError, IndexError, OverflowError, RecursionError, zmq.ZMQError):
        sock.close(linger=0)
        c.reject("Malformed Jupyter multipart message")


def _bounded_client(manager):
    import zmq
    from jupyter_client.channels import ZMQSocketChannel

    class BoundedChannel(ZMQSocketChannel):
        def _recv(self, **kwargs):
            return _receive_bounded(self.socket, self.session)

    client = manager.blocking_client()
    # The client owns a fresh context. Defaults apply BEFORE any connect and to
    # every channel, including control and heartbeat/recreated heartbeat sockets.
    for option, value in ((zmq.MAXMSGSIZE, WIRE_BYTES), (zmq.RCVHWM, WIRE_HWM),
                          (zmq.SNDHWM, WIRE_HWM), (zmq.SNDTIMEO, 100), (zmq.LINGER, 0)):
        client.context.setsockopt(option, value)
    for name in ("iopub", "shell", "stdin", "control"):
        setattr(client, name + "_channel_class", BoundedChannel)
    return client


def execution_command(value, *, generation):
    c.fields(value, {"operation", "execution_id", "generation", "cell_id", "source", "source_sha256", "language", "result_name"}, label="kernel execution command")
    if value["operation"] != "execute" or value["generation"] != generation:
        c.reject("Kernel execution generation differs", code="conflict")
    c.identity(value["execution_id"], "execution ID"); c.identity(value["cell_id"], "cell ID")
    c.sha(value["source_sha256"], "cell source digest")
    c.text(value["source"], "cell source", 16384, empty=True)
    if hashlib.sha256(value["source"].encode("utf-8")).hexdigest() != value["source_sha256"]:
        c.reject("Cell source differs from the admitted execution", code="conflict")
    c.one_of(value["language"], {"python", "sql"}, "executable cell language")
    if value["language"] == "python":
        if value["result_name"] is not None:
            c.reject("Python source defines its own variables")
        return value["source"]
    name = value["result_name"]
    if name is not None:
        c.result_name(name)
    c.text(value["source"], "SQL source", 16384)
    query = "__import__('demiflow.workbench.analysis.client', fromlist=['sql']).sql(" + repr(value["source"]) + ", request_id=" + repr(value["execution_id"]) + ")"
    return query if name is None else name + " = " + query + "\n" + name


def _manager():
    from jupyter_client import KernelManager
    from jupyter_client.kernelspec import KernelSpec
    from jupyter_client.kernelspec import KernelSpecManager
    from demiflow.workbench.analysis.provisioner import kernel_argv
    from demiflow.workbench.analysis.provisioner import kernel_environment

    class FixedSpec(KernelSpecManager):
        def get_kernel_spec(self, kernel_name):
            if kernel_name != "demiflow-analysis":
                c.reject("Only the fixed DemiFlow analysis kernel is available")
            return KernelSpec(argv=kernel_argv(), env={}, language="python", display_name="Python 3",
                resource_dir=job_path("/work") if current() is not None else "/opt/demiflow/kernel", metadata={} if current() is not None else {"kernel_provisioner": {
                    "provisioner_name": "demiflow-sandbox", "config": {}}})

    Path(job_path("/work/jupyter")).mkdir(mode=0o700)
    manager = KernelManager(kernel_name="demiflow-analysis", kernel_spec_manager=FixedSpec(),
        transport="tcp" if current() is not None else "ipc", ip="127.0.0.1" if current() is not None else "/work/jupyter/ipc", connection_file=job_path("/work/jupyter/connection.json"),
        autorestart=False, shutdown_wait_time=2)
    client = None
    try:
        manager.start_kernel(cwd=current().value["cwd"] if current() is not None else "/work", env=kernel_environment(),
            stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
            **({"pass_fds": (current().value["unit_fd"],)} if current() is not None else {}))
        client = _bounded_client(manager)
        client.start_channels()
        for channel in (client.iopub_channel, client.shell_channel, client.stdin_channel, client.control_channel):
            channel.socket.setsockopt(__import__("zmq").LINGER, 0)
        client.wait_for_ready(timeout=15)
        return manager, client
    except BaseException:
        # The helper still owns the cgroup if this call fails or hangs.
        if client is not None:
            client.stop_channels()
        manager.shutdown_kernel(now=True)
        raise


def _persist_output(job, command, state, status):
    value = {**state.snapshot(), "execution_id": command["execution_id"], "cell_id": command["cell_id"],
             "source_sha256": command["source_sha256"], "language": command["language"],
             "session_id": job["config"]["session_id"], "generation": job["config"]["generation"],
             "final_status": status, "raw_fd_output_collected": False}
    raw = c.encoded(value, maximum=16*1024*1024, nodes=32768)
    digest = hashlib.sha256(raw).hexdigest()
    connection = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    connection.settimeout(10)
    with closing(connection):
        connection.connect(job_path("/broker/output.sock"))
        frames = Frames(connection)
        frames.send("json", {"operation": "notebook-output", "execution_id": command["execution_id"],
                             "bytes": len(raw), "sha256": digest})
        if frames.json() != {"accepted": True}:
            c.reject("Notebook output publication was not admitted", code="operation_rejected")
        for offset in range(0, len(raw), 1024*1024):
            frames.send("blob", raw[offset:offset + 1024*1024])
        frames.send("end")
        reply = frames.json()
        c.fields(reply, {"committed", "output_id", "sha256"}, label="Notebook output receipt")
        if reply["committed"] is not True or reply["sha256"] != digest:
            c.reject("Notebook output publication is not confirmed", code="operation_failed")
        c.identity(reply["output_id"], "output ID")
        return reply


def _execute(job, frames, connection, manager, client, command, *, lifetime_deadline):
    generation = job["config"]["generation"]
    code = execution_command(command, generation=generation)
    parent = client.execute(code, allow_stdin=False, store_history=False, stop_on_error=True)
    state = ExecutionOutput(generation=generation, msg_id=parent, output_bytes=16*1024*1024 - 65536)
    frames.send("json", dict(event="executing", execution_id=command["execution_id"], generation=generation))
    started = time.monotonic()
    interrupted = None
    reason = None
    protocol_failed = False
    while not state.complete:
        if time.monotonic() >= min(started + worker_limit("cell_seconds", CELL_SECONDS), lifetime_deadline) and interrupted is None:
            interrupted, reason = time.monotonic(), "timeout"
            manager.interrupt_kernel()
        if interrupted is not None and time.monotonic() - interrupted >= 2:
            reason = reason or "cancelled"
            break
        if select.select([connection], [], [], 0)[0]:
            control = frames.json()
            c.fields(control, {"operation", "execution_id", "generation"}, label="execution cancellation")
            if control["operation"] != "cancel" or control["execution_id"] != command["execution_id"] or control["generation"] != generation:
                frames.send("json", dict(event="rejected", code="conflict", execution_id=command["execution_id"], generation=generation))
            elif interrupted is None:
                interrupted, reason = time.monotonic(), "cancelled"
                manager.interrupt_kernel()
                frames.send("json", dict(event="interrupting", execution_id=command["execution_id"], generation=generation,
                                         generation_will_stop=True))
        if protocol_failed:
            # Keep cancellation and the two-second stop grace responsive; the
            # failed channel is closed and must never be read again.
            time.sleep(0.01)
            continue
        for channel, receive in (("shell", client.get_shell_msg), ("iopub", client.get_iopub_msg), ("stdin", client.get_stdin_msg)):
            # Finite drain keeps the cancellation/lease channel responsive.
            for _ in range(8):
                try:
                    message = receive(timeout=0)
                    state.consume(channel, message, generation=generation)
                except Empty:
                    break
                except ForgeError as exc:
                    protocol_failed = True
                    if interrupted is None:
                        interrupted, reason = time.monotonic(), "output_limit" if exc.code == "resource_limit" else "protocol_error"
                        manager.interrupt_kernel()
                    break
            if protocol_failed:
                break
        if not manager.is_alive():
            reason = reason or "kernel_dead"
            break
        time.sleep(0.01)
    status = reason or state.status
    output = _persist_output(job, command, state, status)
    frames.send("json", dict(event="finished", execution_id=command["execution_id"], generation=generation,
                             status=status, output=output, cleanup_proven=False, generation_will_stop=reason is not None))
    return reason is None


def main():
    job = require_job("kernel")
    # Notebook code shares this job's UID and PID namespace. Keep its parent
    # gateway out of same-UID /proc memory inspection even on hosts with a
    # permissive ptrace policy. This changes only this isolated job process.
    libc = ctypes.CDLL(None, use_errno=True)
    if libc.prctl(4, 0, 0, 0, 0) != 0 or libc.prctl(3, 0, 0, 0, 0) != 0:
        c.reject("Kernel gateway must disable process memory inspection", code="operation_rejected")
    c.fields(job["config"], {"session_id", "generation"}, label="kernel job configuration")
    session_id = c.identity(job["config"]["session_id"], "analysis session ID")
    generation = c.identity(job["config"]["generation"], "kernel generation")
    connection = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    connection.settimeout(5)
    manager = client = None
    try:
        with closing(connection):
            connection.connect(job_path("/broker/kernel.sock"))
            frames = Frames(connection)
            manager, client = _manager()
            frames.send("json", dict(event="ready", session_id=session_id, generation=generation,
                cancel_mode="generation_stops", idle_ttl_s=IDLE_SECONDS, lifetime_s=LIFETIME_SECONDS))
            started = last_used = time.monotonic()
            while time.monotonic() - started < LIFETIME_SECONDS and time.monotonic() - last_used < IDLE_SECONDS:
                if not manager.is_alive():
                    break
                if not select.select([connection], [], [], 0.25)[0]:
                    continue
                command = frames.json()
                if type(command) is dict and command.get("operation") == "stop":
                    c.fields(command, {"operation", "generation"}, label="kernel stop")
                    if command["generation"] != generation:
                        c.reject("Kernel stop generation differs", code="conflict")
                    break
                if not _execute(job, frames, connection, manager, client, command, lifetime_deadline=started + LIFETIME_SECONDS):
                    break
                last_used = time.monotonic()
    finally:
        if client is not None:
            client.stop_channels()
        if manager is not None:
            manager.shutdown_kernel(now=True)
        # No stopped/cleanup-complete claim: the external helper must prove
        # pidfds ended, cgroup populated=0 and mounts/staging cleaned.


if __name__ == "__main__":
    main()
