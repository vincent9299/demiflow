"""Bounded worker-to-worker byte transport; control messages carry locators only.

Live memory channels never write their records to the artifact store. A producer
keeps a credit-owned immutable buffer until the consumer acknowledges delivery.
Durable channels use the same typed record document in the authenticated store.
This transport executes no business code and has no journal/commit authority.
"""
from __future__ import annotations

import hashlib
import json
import secrets
import socket
import socketserver
import struct
import threading

from ..contracts.dataflow_errors import DataflowError

MAX_PEER_BATCH = 32


def _read(sock, size):
    data = bytearray()
    while len(data) < size:
        part = sock.recv(min(65536, size - len(data)))
        if not part:
            raise DataflowError('CORRUPT_ARTIFACT', 'peer closed an incomplete data frame')
        data.extend(part)
    return bytes(data)


class PeerBuffers:
    def __init__(self, *, bounds, host='127.0.0.1'):
        self._bounds = bounds
        self._buffers = {}
        self._bytes = 0
        self._lock = threading.Lock()
        self._token = secrets.token_hex(32)
        owner = self

        class Handler(socketserver.BaseRequestHandler):
            def handle(self):
                self.request.settimeout(bounds.timeout_ms / 1000)
                size = struct.unpack('!I', _read(self.request, 4))[0]
                if size > 2048:
                    return
                request = json.loads(_read(self.request, size))
                if not secrets.compare_digest(str(request.get('token', '')), owner._token):
                    return
                keys = request.get('keys', [request.get('key')])
                if type(keys) is not list or not 1 <= len(keys) <= MAX_PEER_BATCH:
                    return
                if any(type(key) is not str or len(key) != 32 for key in keys):
                    return
                for key in keys:
                    with owner._lock:
                        data = owner._buffers.get(key)
                    if data is None:
                        return
                    # Stream one already credit-owned frame at a time. A
                    # transport batch does not collect another payload batch.
                    self.request.sendall(struct.pack('!Q', len(data)) + data)

        # One bounded connection at a time; no unbounded thread-per-record pool.
        self._server = socketserver.TCPServer((host, 0), Handler)
        self._thread = threading.Thread(target=self._server.serve_forever,
                                        kwargs={'poll_interval': .02}, daemon=True,
                                        name='dataflow-peer-bytes')
        self._thread.start()
        self._address = self._server.server_address

    def put(self, data):
        with self._lock:
            if len(data) > self._bounds.max_row_bytes:
                raise DataflowError('BUDGET_EXCEEDED', 'peer record exceeds max_row_bytes')
            if (len(self._buffers) >= self._bounds.max_pending_events or
                    self._bytes + len(data) > self._bounds.max_resident_bytes):
                raise DataflowError('CAPACITY_PENDING', 'peer buffer credit exhausted', retryable=True)
            key = secrets.token_hex(16)
            self._buffers[key] = data
            self._bytes += len(data)
        return {'host': self._address[0], 'port': self._address[1], 'key': key,
                'token': self._token, 'bytes': len(data), 'sha256': hashlib.sha256(data).hexdigest()}

    def release(self, key):
        with self._lock:
            self._bytes -= len(self._buffers.pop(key, b''))

    def close(self):
        self._server.shutdown()
        self._server.server_close()
        self._thread.join(timeout=1)
        with self._lock:
            self._buffers.clear()
            self._bytes = 0


def read_peer(locator, *, bounds):
    frames = read_peer_many((locator,), bounds=bounds)
    try:
        return next(frames)
    finally:
        frames.close()


def read_peer_many(locators, *, bounds):
    """Fetch a bounded existing credit window over one short-lived connection.

    Each frame retains its own locator size/hash and original ACK. No business
    row is combined, reordered, persisted or acknowledged by this transport.
    """
    if not isinstance(locators, (tuple, list)) or not 1 <= len(locators) <= MAX_PEER_BATCH:
        raise DataflowError('BUDGET_EXCEEDED', 'peer transport batch exceeds its record bound')
    first = locators[0]
    endpoint = (first['host'], first['port'], first['token'])
    for locator in locators:
        size = locator['bytes']
        if type(size) is not int or size < 0 or size > bounds.max_row_bytes:
            raise DataflowError('BUDGET_EXCEEDED', 'peer document exceeds row byte bound')
        if (locator['host'], locator['port'], locator['token']) != endpoint:
            raise DataflowError('INVALID_CONTRACT', 'peer transport batch crosses producer authority')
    request = json.dumps({'key': first['key'], 'token': first['token']} if len(locators) == 1
        else {'keys': [item['key'] for item in locators], 'token': first['token']}).encode()
    if len(request) > 2048:
        raise DataflowError('BUDGET_EXCEEDED', 'peer transport request exceeds control bound')
    with socket.create_connection(endpoint[:2], timeout=bounds.timeout_ms / 1000) as sock:
        sock.sendall(struct.pack('!I', len(request)) + request)
        for locator in locators:
            observed = struct.unpack('!Q', _read(sock, 8))[0]
            if observed != locator['bytes']:
                raise DataflowError('CORRUPT_ARTIFACT', 'peer frame size differs from locator')
            data = _read(sock, observed)
            if hashlib.sha256(data).hexdigest() != locator['sha256']:
                raise DataflowError('CORRUPT_ARTIFACT', 'peer frame checksum differs from locator')
            yield data
