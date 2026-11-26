"""One bounded stdio reader; independent RPC responses and server requests."""
from __future__ import annotations

from collections import deque
import os
import selectors
import time

from .common import decode, reject, wire


class Transport:
    def __init__(self, process, limits, *, acp=False):
        self.process, self.limits, self.acp = process, limits, acp
        self.selector = selectors.DefaultSelector()
        self.buffer = bytearray(); self.stderr = bytearray(); self.stderr_total = 0
        self.events, self.queued_bytes = deque(), 0
        self.pending, self.responses, self.next_id = set(), {}, 0
        self.response_bytes, self.response_sizes = 0, {}
        self.output_bytes = self.frame_count = 0
        self.out = bytearray(); self.closed = False
        self.stdout_registered = True
        for name, stream in (('stdout', process.stdout), ('stderr', process.stderr)):
            os.set_blocking(stream.fileno(), False)
            self.selector.register(stream, selectors.EVENT_READ, name)
        os.set_blocking(process.stdin.fileno(), False)

    def send(self, value):
        if self.acp: value = dict(jsonrpc='2.0', **value)
        raw = wire(value, self.limits.frame_bytes) + b'\n'
        if len(self.out) + len(raw) > 2 * self.limits.frame_bytes:
            reject('Provider write queue exceeded budget', 'resource_limit')
        self.out.extend(raw)
        deadline = time.monotonic() + 5
        while self.out:
            try:
                written = os.write(self.process.stdin.fileno(), self.out)
                del self.out[:written]
            except BlockingIOError:
                if time.monotonic() > deadline: reject('Provider input backpressure timeout', 'resource_limit')
                self.poll(0.05)

    def request(self, method, params):
        if len(self.pending) >= 16: reject('Too many provider RPC requests', 'resource_limit')
        self.next_id += 1
        key = 'client-' + str(self.next_id)
        self.pending.add(key)
        self.send(dict(id=key, method=method, params=params))
        return key

    def take(self, key):
        if key not in self.responses: return None
        response = self.responses.pop(key); self.pending.remove(key)
        self.response_bytes -= self.response_sizes.pop(key)
        if 'error' in response: reject('Provider RPC failed: ' + str(response['error'])[:2048])
        return response['result']

    def _decode_buffer(self):
        """Admit complete frames only while the bounded consumer queue has room."""
        while True:
            if len(self.events) >= 128 or self.queued_bytes >= 2 * self.limits.frame_bytes:
                return False
            end = self.buffer.find(b'\n')
            if end < 0:
                if len(self.buffer) > self.limits.frame_bytes:
                    reject('Provider frame exceeded byte budget', 'resource_limit')
                return True
            if end > self.limits.frame_bytes:
                reject('Provider frame exceeded byte budget', 'resource_limit')
            if self.queued_bytes + end > 2 * self.limits.frame_bytes:
                return False
            raw = bytes(self.buffer[:end]); del self.buffer[:end + 1]
            if not raw: continue
            self.frame_count += 1
            event = decode(raw)
            if 'id' in event and 'method' not in event:
                identity = event['id']
                if type(identity) is not str or identity not in self.pending or identity in self.responses:
                    reject('Unknown or duplicate provider response identity')
                if self.response_bytes + len(raw) > 2 * self.limits.frame_bytes:
                    reject('Provider response map byte budget exceeded', 'resource_limit')
                self.response_bytes += len(raw); self.response_sizes[identity] = len(raw)
                self.responses[identity] = event
            else:
                self.events.append((event, len(raw))); self.queued_bytes += len(raw)

    def _read_stdout(self, enabled):
        # A full queue must stop reading the OS pipe, rather than repeatedly
        # waking on readable stdout while wait() retains pre-ack notifications.
        if self.closed or enabled == self.stdout_registered: return
        if enabled:
            self.selector.register(self.process.stdout, selectors.EVENT_READ, 'stdout')
        else:
            self.selector.unregister(self.process.stdout)
        self.stdout_registered = enabled

    def poll(self, timeout=0.1):
        self._read_stdout(self._decode_buffer())
        for key, _ in self.selector.select(timeout):
            maximum = self.limits.frame_bytes + 1 - len(self.buffer) if key.data == 'stdout' else 65536
            chunk = os.read(key.fd, min(65536, maximum))
            if not chunk:
                self.selector.unregister(key.fileobj)
                if key.data == 'stdout':
                    self.closed = True; self.stdout_registered = False
                continue
            if key.data == 'stderr':
                self.stderr_total += len(chunk)
                if self.stderr_total > self.limits.stderr_bytes:
                    reject('Provider stderr exceeded budget', 'resource_limit')
                self.stderr.extend(chunk)
                continue
            self.output_bytes += len(chunk)
            if self.limits.output_bytes and self.output_bytes > self.limits.output_bytes:
                reject('Provider total stdout byte budget exceeded', 'resource_limit')
            self.buffer.extend(chunk)
            self._read_stdout(self._decode_buffer())

    def pop(self):
        event, size = self.events.popleft(); self.queued_bytes -= size
        return event

    def close(self):
        self.selector.close()
        for stream in (self.process.stdin, self.process.stdout, self.process.stderr):
            stream.close()
