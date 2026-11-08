"""Real Unix framing/peer credentials, bounded partial input and backpressure."""
from contextlib import contextmanager
import os
import socket
import struct

import pytest

from demiflow.workbench.process import process_identity
from demiflow.workbench.analysis import contracts as c
from demiflow.workbench.analysis.framing import Frames
from demiflow.workbench.analysis.ipc import Channel
from demiflow.workbench.analysis.ipc import Peer
from demiflow.forge.schema import ForgeError


@contextmanager
def pair(*, binary=False):
    sender, receiver = socket.socketpair()
    channel = Channel(receiver, binary=binary)
    try:
        yield sender, channel
    finally:
        sender.close(); channel.close()


def receive(channel):
    for _ in range(8):
        value = channel.receive()
        if value is not None:
            return value
    raise AssertionError("complete buffered frame was not consumed")


def test_partial_frames_and_control_listener_rejects_binary_before_allocation():
    raw = c.encoded(dict(operation="ping"))
    with pair() as (sender, channel):
        sender.sendall(b"J")
        assert channel.receive() is None and channel.payload is None
        sender.sendall(struct.pack("!I", len(raw)) + raw[:2])
        assert channel.receive() is None
        sender.sendall(raw[2:])
        assert receive(channel) == ("json", {"operation": "ping"})
        sender.sendall(struct.pack("!cI", b"B", 1024*1024))
        with pytest.raises(ForgeError, match="budget"):
            channel.receive()
        assert channel.payload is None
    with pair(binary=True) as (sender, channel):
        sender.sendall(struct.pack("!cI", b"B", 1024*1024+1))
        with pytest.raises(ForgeError, match="budget"):
            channel.receive()
        assert channel.payload is None


def test_binary_frame_output_limit_and_half_closed_peer():
    with pair(binary=True) as (sender, channel):
        data = b"x"*65536
        sender.sendall(struct.pack("!cI", b"A", len(data)) + data)
        assert receive(channel) == ("arrow", data)
        chunk = b"a"*(1024*1024)
        channel.queue("blob", chunk); channel.queue("blob", chunk)
        with pytest.raises(ForgeError, match="not consuming"):
            channel.queue("end")
        assert channel.outgoing_bytes == 2*len(chunk)+10
    with pair() as (sender, channel):
        sender.sendall(b"J\x00"); sender.shutdown(socket.SHUT_WR)
        assert channel.receive() is None
        with pytest.raises(EOFError):
            channel.receive()
        assert channel.expired(wall_seconds=-1)


def test_kernel_supplied_peer_identity_and_json_reply():
    with pair() as (sender, channel):
        peer = Peer.capture(channel.connection, allowed_uids={os.geteuid()})
        assert peer.pid == os.getpid() and peer.identity == process_identity(os.getpid()) and peer.live()
        with pytest.raises(ForgeError, match="UID"):
            Peer.capture(channel.connection, allowed_uids={os.geteuid()+1})
        channel.queue("json", {"accepted": True})
        for _ in range(4):
            channel.flush()
        sender.settimeout(1)
        assert Frames(sender).json() == {"accepted": True}
