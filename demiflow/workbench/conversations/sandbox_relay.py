"""Bounded transport to a controller-fixed Unix endpoint, never a host proxy.

Only the trusted host half knows the destination. The namespace half exposes a
loopback TCP stream and owns a preconnected anonymous channel. Model credentials,
provider/model selection and request authorization remain in the fixed server.
"""
import fcntl
import select
import socket
import struct
import threading
import time

CHUNK = 32768
MAX_BYTES = 64 * 1024**2
MAX_PACKETS = 16384


class Budget:
    def __init__(self, seconds):
        self.deadline = time.monotonic() + seconds
        self.used = self.packets = 0

    def consume(self, count):
        self.used += count
        self.packets += 1
        if time.monotonic() >= self.deadline or self.used > MAX_BYTES or self.packets > MAX_PACKETS:
            raise RuntimeError('Fixed channel time/byte/frame budget exceeded')


def _packet(channel, budget):
    data = channel.recv(CHUNK + 6)
    budget.consume(len(data))
    if len(data) < 5 or len(data) > CHUNK + 5:
        raise ConnectionError('Fixed channel closed or exceeded frame budget')
    return data


def _pump(channel, stream, budget, session):
    stream.settimeout(1)
    channel.settimeout(1)
    while time.monotonic() < budget.deadline:
        ready, _, _ = select.select([channel, stream], [], [], .1)
        if channel in ready:
            frame = _packet(channel, budget)
            if frame[1:5] != session:
                continue  # a close from the previous completed stream
            if frame[:1] == b'C':
                return
            if frame[:1] != b'D':
                raise RuntimeError('Unexpected fixed-channel frame')
            stream.sendall(frame[5:])
        if stream in ready:
            data = stream.recv(CHUNK)
            budget.consume(len(data))
            channel.sendall((b'D' + session + data) if data else b'C' + session)
            if not data:
                return
    raise TimeoutError('Fixed channel deadline exceeded')


def host_channel(channel, target, seconds):
    """One route, one bounded stream at a time; target is controller-owned."""
    budget = Budget(seconds)
    channel.settimeout(1)
    try:
        for _ in range(256):
            while not select.select([channel], [], [], .1)[0]:
                if time.monotonic() >= budget.deadline:
                    return
            opened = _packet(channel, budget)
            if opened[:1] == b'C':
                continue
            if opened[:1] != b'O' or len(opened) != 5:
                raise RuntimeError('Fixed channel requires an open frame')
            session = opened[1:5]
            with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as upstream:
                upstream.settimeout(2)
                try:
                    upstream.connect(target)
                except OSError:
                    channel.sendall(b'C' + session)
                    continue
                channel.sendall(b'K' + session)
                _pump(channel, upstream, budget, session)
    except (OSError, RuntimeError, TimeoutError):
        pass
    finally:
        channel.close()


def loopback():
    with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as control:
        request = struct.pack('16sh', b'lo', 0)
        current = fcntl.ioctl(control.fileno(), 0x8913, request)
        flags = struct.unpack('16sh', current[:18])[1]
        fcntl.ioctl(control.fileno(), 0x8914, struct.pack('16sh', b'lo', flags | 1))


def namespace_listener():
    listener = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    listener.bind(('127.0.0.1', 0))
    listener.listen(4)
    listener.settimeout(.1)
    return listener


def namespace_channel(channel, listener, seconds, failed):
    budget = Budget(seconds)
    channel.settimeout(2)
    try:
        for sequence in range(256):
            while time.monotonic() < budget.deadline:
                try:
                    stream, _ = listener.accept()
                    break
                except socket.timeout:
                    pass
            else:
                return
            with stream:
                session = struct.pack('!I', sequence)
                channel.sendall(b'O' + session)
                ready = _packet(channel, budget)
                while ready[1:5] != session:
                    ready = _packet(channel, budget)
                if ready[:1] == b'C':
                    continue
                if ready[:1] != b'K':
                    raise RuntimeError('Fixed channel open failed')
                _pump(channel, stream, budget, session)
    except (OSError, RuntimeError, TimeoutError):
        failed.set()
    finally:
        channel.close()
        listener.close()


def start_host(channel, target, seconds):
    thread = threading.Thread(target=host_channel, args=(channel, target, seconds), daemon=True)
    thread.start()
    return thread
