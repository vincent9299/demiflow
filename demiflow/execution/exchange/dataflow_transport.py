"""Bounded worker-to-worker byte transport; control messages carry locators only.

Live memory channels never write their records to the artifact store. A producer
keeps a credit-owned immutable buffer until the consumer acknowledges delivery.
Durable channels use the same typed record document in the authenticated store.
This transport executes no business code and has no journal/commit authority.
"""
from __future__ import annotations

import hashlib
import json
import queue
import secrets
import socket
import socketserver
import struct
import threading
import time

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
    def __init__(self, *, bounds, host='127.0.0.1', store=None):
        self._bounds = bounds
        self._buffers = {}
        self._bytes = 0
        self._lock = threading.Condition()
        self._sending = {}
        self._retired = set()
        self._stopping = threading.Event()
        self._accept_stopped = threading.Event()
        self._active = set()
        self._file_grant = None
        self._connections = 1
        # The original listener/base connection retain their provider owner.
        # Additional concurrent descriptors use the existing store authority,
        # with its staging headroom left available. A snapshot is not a grant.
        if store is not None and store.available_file_slots() >= store.PUT_FILES + 3:
            grant = store.file_slots(3)
            try:
                grant.__enter__()
            except DataflowError as exc:
                if exc.code != 'BUDGET_EXCEEDED':
                    raise
            else:
                self._file_grant = grant
                self._connections = 4
        self._permits = threading.BoundedSemaphore(self._connections)
        self._requests = queue.Queue(maxsize=self._connections)
        self._token = secrets.token_hex(32)
        owner = self

        class Handler(socketserver.BaseRequestHandler):
            def handle(self):
                self.request.settimeout(bounds.timeout_ms / 1000)
                self.request.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
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
                        data = None if key in owner._retired else owner._buffers.get(key)
                        if data is None:
                            return
                        owner._sending[key] = owner._sending.get(key, 0) + 1
                    try:
                        # No header+payload copy. The original credit remains
                        # owned until this borrowed send reference is gone.
                        self.request.sendall(struct.pack('!Q', len(data)))
                        self.request.sendall(memoryview(data))
                    finally:
                        del data
                        with owner._lock:
                            owner._sending[key] -= 1
                            if not owner._sending[key]:
                                del owner._sending[key]
                            owner._lock.notify_all()

        class Server(socketserver.TCPServer):
            def get_request(self):
                # Acquire before accept: even queued accepted sockets consume
                # one of the fixed, already granted connection slots.
                while not owner._stopping.is_set():
                    if owner._permits.acquire(timeout=.02):
                        try:
                            request, address = super().get_request()
                            with owner._lock:
                                owner._active.add(request)
                            return request, address
                        except BaseException:
                            owner._permits.release()
                            raise
                raise OSError('peer transport closed')

            def process_request(self, request, address):
                owner._requests.put_nowait((request, address))

        try:
            self._server = Server((host, 0), Handler)
        except BaseException:
            if self._file_grant is not None:
                self._file_grant.__exit__(None, None, None)
            raise
        # Fixed I/O-only workers; neither business kernels nor an unbounded
        # thread-per-request scheduler run here.
        self._threads = tuple(threading.Thread(target=self._serve_request,
            daemon=True, name='dataflow-peer-io') for _ in range(self._connections))
        for thread in self._threads:
            thread.start()
        self._thread = threading.Thread(target=self._server.serve_forever,
                                        kwargs={'poll_interval': .02}, daemon=True,
                                        name='dataflow-peer-bytes')
        self._thread.start()
        self._address = self._server.server_address

    def _serve_request(self):
        while not self._accept_stopped.is_set() or not self._requests.empty():
            try:
                request, address = self._requests.get(timeout=.02)
            except queue.Empty:
                continue
            try:
                self._server.finish_request(request, address)
            except Exception:
                # The consumer observes the original incomplete-frame failure.
                pass
            finally:
                self._server.shutdown_request(request)
                with self._lock:
                    self._active.discard(request)
                self._permits.release()
                self._requests.task_done()

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
                'token': self._token, 'bytes': len(data), 'sha256': hashlib.sha256(data).hexdigest(),
                'max_batch_records': MAX_PEER_BATCH if self._connections > 1 else 1}

    def release(self, key):
        deadline = time.monotonic() + self._bounds.timeout_ms / 1000
        with self._lock:
            self._retired.add(key)
            while self._sending.get(key, 0):
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    raise DataflowError('DEADLINE_EXCEEDED', 'peer sender still owns acknowledged bytes')
                self._lock.wait(remaining)
            self._bytes -= len(self._buffers.pop(key, b''))
            self._retired.discard(key)

    def close(self):
        self._stopping.set()
        self._server.shutdown()
        self._accept_stopped.set()
        self._server.server_close()
        with self._lock:
            active = tuple(self._active)
        for request in active:
            try:
                request.shutdown(socket.SHUT_RDWR)
            except OSError:
                pass
        deadline = time.monotonic() + self._bounds.timeout_ms / 1000
        for thread in (self._thread, *self._threads):
            thread.join(timeout=max(0, deadline - time.monotonic()))
        if any(thread.is_alive() for thread in (self._thread, *self._threads)):
            raise DataflowError('DEADLINE_EXCEEDED', 'peer sender still owns its buffers and descriptors')
        with self._lock:
            self._buffers.clear()
            self._bytes = 0
        if self._file_grant is not None:
            self._file_grant.__exit__(None, None, None)
            self._file_grant = None


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
            del data
