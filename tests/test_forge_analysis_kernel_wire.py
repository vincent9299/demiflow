"""Real local ZMQ/Session wire tests; no kernel, OCI or cgroup execution."""
from contextlib import contextmanager
from types import SimpleNamespace

import pytest
import zmq
from jupyter_client import KernelManager
from jupyter_client.session import Session

from demiflow.workbench.analysis import kernel_worker as worker
from demiflow.forge.schema import ForgeError
from test_forge_analysis_kernel_contract import command


@contextmanager
def wire():
    context = zmq.Context()
    sender, receiver = context.socket(zmq.PAIR), context.socket(zmq.PAIR)
    sender.bind("inproc://bounded-wire")
    receiver.connect("inproc://bounded-wire")
    try:
        yield sender, receiver
    finally:
        sender.close(linger=0)
        receiver.close(linger=0)
        context.term()


@pytest.mark.parametrize("parts", [
    [b"a" * (worker.WIRE_BYTES + 1)],
    [b"a" * (worker.WIRE_BYTES // 2), b"b" * (worker.WIRE_BYTES // 2 + 1)],
    [b"a"] * (worker.WIRE_FRAMES + 1),
])
def test_limits_close_real_socket_before_identity_copy_or_json(parts):
    session = SimpleNamespace(feed_identities=lambda *a, **k: pytest.fail("copied before bounds"),
                              deserialize=lambda *a, **k: pytest.fail("decoded before bounds"))
    with wire() as (sender, receiver):
        sender.send_multipart(parts)
        assert receiver.poll(1000)
        with pytest.raises(ForgeError) as exc:
            worker._receive_bounded(receiver, session)
        assert exc.value.code == "resource_limit"
        assert receiver.closed


def test_exact_byte_and_frame_boundary_preserves_signed_message():
    session = Session(key=b"fixed-local-test-key")
    raw = session.serialize(session.msg("stream", {"name": "stdout", "text": "hello"}))
    raw += [b""] * (worker.WIRE_FRAMES - len(raw))
    raw[-1] = b"x" * (worker.WIRE_BYTES - sum(map(len, raw)))
    assert len(raw) == 16 and sum(map(len, raw)) == 8 * 1024 * 1024
    with wire() as (sender, receiver):
        sender.send_multipart(raw)
        assert receiver.poll(1000)
        message = worker._receive_bounded(receiver, session)
        assert message["content"] == {"name": "stdout", "text": "hello"}
        assert sum(buf.nbytes for buf in message["buffers"]) == len(raw[-1])
        assert not receiver.closed


@pytest.mark.parametrize("kind", ["delimiter", "short", "signature", "json"])
def test_malformed_real_multipart_is_closed_and_normalized(kind):
    session = Session(key=b"fixed-local-test-key")
    raw = session.serialize(session.msg("stream", {"name": "stdout", "text": "hello"}))
    if kind == "delimiter":
        raw[0] = b"no delimiter"
    elif kind == "short":
        raw = raw[:3]
    elif kind == "signature":
        raw[1] = b"0" * 64
    else:
        raw[-1] = b"{invalid"
        raw[1] = session.sign(raw[2:6])
    with wire() as (sender, receiver):
        sender.send_multipart(raw)
        assert receiver.poll(1000)
        with pytest.raises(ForgeError, match="Malformed"):
            worker._receive_bounded(receiver, session)
        assert receiver.closed


def test_all_client_sockets_are_bounded_before_connect_and_use_guard(tmp_path, monkeypatch):
    observed = []
    original = zmq.Socket.connect

    def connect(sock, endpoint):
        observed.append(tuple(sock.getsockopt(key) for key in
            (zmq.MAXMSGSIZE, zmq.RCVHWM, zmq.SNDHWM, zmq.SNDTIMEO)))
        return original(sock, endpoint)

    monkeypatch.setattr(zmq.Socket, "connect", connect)
    manager = KernelManager(transport="ipc", ip=str(tmp_path / "ipc"))
    client = worker._bounded_client(manager)
    try:
        for name in ("shell", "iopub", "stdin", "control"):
            channel = getattr(client, name + "_channel")
            channel.socket.setsockopt(zmq.LINGER, 0)
            calls = []
            with monkeypatch.context() as local:
                local.setattr(worker, "_receive_bounded", lambda sock, session: calls.append((sock, session)))
                channel._recv()
            assert calls == [(channel.socket, client.session)]
        client.hb_channel._create_socket()
        assert observed == [(8 * 1024 * 1024, 4, 4, 100)] * 5
    finally:
        client.hb_channel.close()
        client.stop_channels()
        manager.cleanup_resources()


@pytest.mark.parametrize("failure,cancel_first,expected", [
    ("resource_limit", False, "output_limit"),
    ("operation_rejected", False, "protocol_error"),
    ("resource_limit", True, "cancelled"),
])
def test_wire_failure_does_not_redrain_and_preserves_cancel_stop_boundary(monkeypatch, failure, cancel_first, expected):
    clock, receives, interrupts, sent = [0.0], [], [], []
    monkeypatch.setattr(worker.time, "monotonic", lambda: clock[0])
    monkeypatch.setattr(worker.time, "sleep", lambda seconds: clock.__setitem__(0, clock[0] + seconds))
    monkeypatch.setattr(worker.select, "select", lambda *a: ([True] if cancel_first or clock[0] > 0 else [], [], []))
    request = command()
    frames = SimpleNamespace(send=lambda kind, value: sent.append(value), json=lambda: dict(
        operation="cancel", execution_id=request["execution_id"], generation=request["generation"]))

    def receive(**kwargs):
        receives.append(True)
        raise ForgeError("test rejected multipart", code=failure)

    client = SimpleNamespace(execute=lambda *a, **k: "parent-one", get_shell_msg=receive,
        get_iopub_msg=lambda **k: pytest.fail("must not drain after wire failure"),
        get_stdin_msg=lambda **k: pytest.fail("must not drain after wire failure"))
    manager = SimpleNamespace(interrupt_kernel=lambda: interrupts.append(True), is_alive=lambda: True)
    monkeypatch.setattr(worker, "_persist_output", lambda *a: {"status": a[-1]})
    assert worker._execute({"config": {"generation": request["generation"]}}, frames, object(),
        manager, client, request, lifetime_deadline=60) is False
    assert len(receives) == len(interrupts) == 1 and 2 <= clock[0] < 2.1
    assert sent[-1]["status"] == expected
    assert sent[-1]["cleanup_proven"] is False and sent[-1]["generation_will_stop"] is True
