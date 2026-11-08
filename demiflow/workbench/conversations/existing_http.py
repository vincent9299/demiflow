"""Bounded client of one existing development owner; no Store or provider.

The upstream remains authoritative for project grants, CAS and durable intent.
Closing this client only closes its sockets. It never signals the old owner or
submits cancellation/recovery to it. Wire-buffer observations exclude decoded
JSON/native allocations and do not claim an RSS or hard isolation guarantee.
"""
from __future__ import annotations

from collections import deque
from contextlib import contextmanager
from dataclasses import dataclass
import hashlib
import http.client
import os
from pathlib import Path
import re
import socket
import stat
import threading
import time
from urllib.parse import urlencode
from urllib.parse import urlsplit

from demiflow.workbench.conversations.common import read_file
from demiflow.workbench.conversations.common import reject
from demiflow.workbench.conversations.common import root_identity
from demiflow.workbench.conversations.common import strict_json
from demiflow.workbench.conversations.common import wire
from demiflow.workbench.conversations.policy import Deployment
from demiflow.workbench.process import process_identity

FORMAT = 'demiflow_development_existing_http_v1'
REQUEST_BYTES = 65536
RESPONSE_BYTES = 9 * 1024**2
HEADER_BYTES = 65536


class ForwardedError(Exception):
    """Preserve an authenticated upstream error without losing dispatch facts."""
    def __init__(self, status, payload):
        super().__init__(str(payload.get('error', 'Existing development request failed')))
        self.status, self.payload = status, payload


@dataclass(eq=False)
class _Request:
    deadline: float
    consumer_closed: object
    name: str
    mutation: bool
    connection: object = None
    original_socket: object = None
    reason: str | None = None
    dispatched: bool = False
    headers: int = 0
    body: int = 0
    request_bytes: int = 0


class _HeaderFile:
    def __init__(self, stream, service, request):
        self.stream, self.service, self.request = stream, service, request
        self.headers_complete = False

    def readline(self, size=-1):
        value = self.stream.readline(size)
        if not self.headers_complete:
            self.request.headers += len(value)
            self.service._observe(self.request)
            if self.request.headers > HEADER_BYTES:
                raise http.client.HTTPException('Existing development header budget exceeded')
        return value

    def __getattr__(self, name):
        return getattr(self.stream, name)


class _SocketView:
    def __init__(self, sock, service, request):
        self.sock, self.service, self.request = sock, service, request

    def makefile(self, *args, **kwargs):
        return _HeaderFile(self.sock.makefile(*args, **kwargs), self.service, self.request)


class ExistingHTTP:
    remote = True

    def __init__(self, config_path):
        value = strict_json(read_file(config_path, REQUEST_BYTES))
        required = {'format', 'upstream', 'upstream_identity', 'source_deployment_config',
                    'token_file', 'projects'}
        optional = {'read_seconds', 'write_seconds'}
        if type(value) is not dict or not required <= value.keys() or value.keys() - required - optional:
            reject('Invalid existing development client configuration')
        if value['format'] != FORMAT:
            reject('Unsupported existing development client configuration')
        upstream = urlsplit(value['upstream'])
        try:
            port = upstream.port
        except ValueError:
            reject('Existing development requires a fixed literal loopback origin')
        if (upstream.scheme != 'http' or upstream.hostname != '127.0.0.1' or
                not port or upstream.netloc != '127.0.0.1:' + str(port) or
                upstream.path or upstream.query or upstream.fragment):
            reject('Existing development requires a fixed literal loopback origin')
        identity = value['upstream_identity']
        if (type(identity) is not dict or set(identity) != {'pid', 'start', 'boot'} or
                type(identity['pid']) is not int or identity['pid'] <= 0 or
                type(identity['start']) is not str or not re.fullmatch(r'[0-9]{1,24}', identity['start']) or
                type(identity['boot']) is not str or not re.fullmatch(r'[a-f0-9-]{36}', identity['boot'])):
            reject('A fixed original development process identity is required')
        source = value['source_deployment_config']
        if (type(source) is not dict or set(source) != {'path', 'sha256'} or
                type(source['sha256']) is not str or not re.fullmatch(r'[a-f0-9]{64}', source['sha256'])):
            reject('A fixed original development policy reference is required')
        self.policy_path = self._physical(source['path'])
        self.policy_sha = source['sha256']
        if hashlib.sha256(read_file(self.policy_path, REQUEST_BYTES)).hexdigest() != self.policy_sha:
            reject('Existing development policy changed', 'conflict')
        # Declaration validation is read-only. In particular, do not construct
        # Service/Store, lock service.lock, reserve disk or recover old turns.
        self.deployment = Deployment.load(self.policy_path)
        self.token_file = self._physical(value['token_file'])
        self._token_metadata()
        projects = value['projects']
        if type(projects) is not dict or not 1 <= len(projects) <= 128:
            reject('Existing development projects must be explicitly allowlisted')
        self.projects = {}
        for workspace, project in projects.items():
            if (not re.fullmatch(r'[wl][a-f0-9]{32}', workspace) or type(project) is not dict or
                    set(project) != {'root', 'identity', 'mode'} or
                    project['mode'] not in {'history_only', 'existing_grants'}):
                reject('Invalid existing development project binding')
            identity = root_identity(self._physical(project['root']))
            if project['identity'] != identity:
                reject('Existing development root identity changed', 'conflict')
            self.deployment.project(workspace, Path(project['root']))
            if self.token_file.is_relative_to(Path(project['root'])):
                reject('Existing console credential must be outside development projects')
            self.projects[workspace] = dict(project)
        self.upstream, self.port = value['upstream'], port
        self.identity = dict(value['upstream_identity'])
        self.read_seconds = self._seconds(value.get('read_seconds', 5), 5)
        self.write_seconds = self._seconds(value.get('write_seconds', 60), 60)
        self._slots = threading.BoundedSemaphore(2)
        self._condition = threading.Condition()
        self._active = set()
        self._closed = threading.Event()
        self._watcher = None
        self._control_waiting = False
        self.peak_connections = self.peak_wire_buffer_bytes = 0
        self.observations = deque(maxlen=16)
        self._guard()

    @staticmethod
    def _physical(value):
        if type(value) is not str or not Path(value).is_absolute() or '..' in Path(value).parts:
            reject('Existing development requires physical absolute paths')
        path = Path(value)
        if path != path.resolve(strict=True):
            reject('Existing development paths must not traverse symlinks')
        return path

    @staticmethod
    def _seconds(value, maximum):
        if type(value) not in (int, float) or not 0 < value <= maximum:
            reject('Existing development deadline exceeds its bound')
        return value

    def _token_metadata(self):
        info = self.token_file.stat()
        if (not stat.S_ISREG(info.st_mode) or info.st_uid != os.geteuid() or
                info.st_mode & 0o077 or info.st_size > 4096):
            reject('Existing console token reference must be private and bounded')

    def _guard(self):
        if self._closed.is_set():
            reject('Existing development client is closed')
        if process_identity(self.identity['pid']) != self.identity:
            reject('Original development process identity changed', 'conflict')
        if (self.policy_path != self.policy_path.resolve(strict=True) or
                hashlib.sha256(read_file(self.policy_path, REQUEST_BYTES)).hexdigest() != self.policy_sha):
            reject('Original development policy changed', 'conflict')
        for workspace, project in self.projects.items():
            if root_identity(project['root']) != project['identity']:
                reject('Original development root identity changed', 'conflict')
            self.deployment.project(workspace, Path(project['root']))
        self._token_metadata()

    def project(self, workspace, root):
        project = self.projects.get(workspace)
        if project is None or root_identity(root) != project['identity']:
            reject('Existing development is not authorized for this project')
        self._guard()
        return project

    def _observe(self, request):
        with self._condition:
            current = sum(row.headers + row.body + row.request_bytes for row in self._active)
            self.peak_wire_buffer_bytes = max(self.peak_wire_buffer_bytes, current)

    def _abort(self, request, reason):
        with self._condition:
            request.reason = request.reason or reason
            sock = request.original_socket
        # HTTP/1.0 getresponse may detach connection.sock while its file still
        # blocks on a body. Retain only that original socket, shutdown first.
        if sock is not None:
            try: sock.shutdown(socket.SHUT_RDWR)
            except OSError: pass
        if request.connection is not None:
            request.connection.close()

    def _watch(self):
        while not self._closed.wait(.05):
            with self._condition:
                requests = tuple(self._active)
            if not requests:
                continue
            try:
                self._guard()
                guard_error = None
            except Exception:
                guard_error = 'Original development policy/process/root is no longer current'
            for request in requests:
                try:
                    closed = bool(request.consumer_closed and request.consumer_closed())
                except Exception:
                    closed = True
                if guard_error or closed or time.monotonic() >= request.deadline:
                    self._abort(request, guard_error or ('Original caller disconnected' if closed else 'Absolute development deadline exceeded'))

    def _check(self, request):
        self._guard()
        if request.reason or time.monotonic() >= request.deadline:
            raise OSError(request.reason or 'Absolute development deadline exceeded')
        if request.consumer_closed and request.consumer_closed():
            raise OSError('Original caller disconnected')

    @contextmanager
    def _request(self, name, mutation, consumer_closed):
        request = _Request(time.monotonic() + (self.write_seconds if mutation else self.read_seconds), consumer_closed, name, mutation)
        if not self._acquire(request):
            raise ForwardedError(503, dict(code='busy', error='Existing development bridge is busy', operation_started=False))
        started = time.monotonic()
        try:
            with self._condition:
                if self._closed.is_set():
                    reject('Existing development client is closed')
                self._active.add(request)
                self.peak_connections = max(self.peak_connections, len(self._active))
                if self._watcher is None:
                    self._watcher = threading.Thread(target=self._watch, daemon=True)
                    try:self._watcher.start()
                    except BaseException:
                        self._watcher = None
                        raise
            yield request
        except ForwardedError:
            raise
        except Exception as error:
            unknown = mutation and request.dispatched
            message = ('Original development write outcome is unknown; retain its request/submission ID and inspect existing history'
                       if unknown else 'Existing development connection is unavailable; no development write was sent')
            raise ForwardedError(502 if unknown else 503, dict(code='operation_failed' if unknown else 'operation_rejected',
                error=message, **({'dispatch': 'unknown'} if unknown else {'operation_started': False}))) from error
        finally:
            self._abort(request, request.reason or 'Request complete')
            with self._condition:
                self._active.discard(request)
                self.observations.append(dict(name=name, mutation=mutation, dispatched=request.dispatched,
                    seconds=time.monotonic()-started, cancelled=request.reason != 'Request complete',
                    wire_buffer_bytes=request.headers+request.body+request.request_bytes))
                self._slots.release()
                self._condition.notify_all()

    def _acquire(self, request):
        # An explicit stop/input/recovery must not wait behind two history
        # polls. Yield at most one of OUR read transports; never interrupt a
        # mutation or submit cancellation to the upstream owner. One bounded
        # control waiter, still at most two admitted upstream requests.
        with self._condition:
            if self._closed.is_set() or self._control_waiting:
                return False
            if self._slots.acquire(blocking=False):
                return True
            if request.name not in {'dev-cancel', 'dev-input', 'dev-recover'}:
                return False
            victim = next((row for row in self._active if not row.mutation), None)
            if victim is None:
                return False
            self._control_waiting = True
        try:
            self._abort(victim, 'Read transport yielded for explicit development control')
            deadline = min(request.deadline, time.monotonic()+.25)
            with self._condition:
                while not self._closed.is_set():
                    if self._slots.acquire(blocking=False):
                        return True
                    remaining = deadline-time.monotonic()
                    if remaining <= 0:
                        break
                    self._condition.wait(min(.02, remaining))
            return False
        finally:
            with self._condition:
                self._control_waiting = False
                self._condition.notify_all()

    def _roundtrip(self, request, workspace, name, query=None, payload=None, encoded=None):
        self._check(request)
        token = read_file(self.token_file, 4096).decode('ascii').strip()
        if not token or len(token) > 512 or '\r' in token or '\n' in token:
            reject('Existing console token is invalid')
        path = '/api/workspaces/' + workspace + '/' + name
        if query:
            path += '?' + urlencode(query)
        if len(path) > 8192:
            reject('Existing development query exceeds byte bound', 'resource_limit')
        raw = None if payload is None else (encoded if encoded is not None else wire(payload, REQUEST_BYTES))
        request.headers = request.body = 0
        if raw is not None:request.request_bytes = len(raw)
        self._observe(request)
        connection = http.client.HTTPConnection('127.0.0.1', self.port, timeout=min(1, max(.001, request.deadline-time.monotonic())))
        request.connection = connection
        service = self
        class Response(http.client.HTTPResponse):
            def __init__(self, sock, *args, **kwargs):
                super().__init__(_SocketView(sock, service, request), *args, **kwargs)
            def begin(self):
                super().begin()
                self.fp.headers_complete = True
        connection.response_class = Response
        response = None
        try:
            connection.connect()
            request.original_socket = connection.sock
            self._check(request)
            connection.sock.settimeout(max(.001, request.deadline-time.monotonic()))
            if payload is not None:
                request.dispatched = True  # before any possibly partial write
            connection.request('GET' if payload is None else 'POST', path, body=raw,
                headers={'X-Forge-Token': token, 'Accept': 'application/json', 'Origin': self.upstream,
                         **({'Content-Type': 'application/json'} if raw is not None else {})})
            response = connection.getresponse()
            lengths = response.headers.get_all('Content-Length', [])
            if (response.chunked or len(lengths) != 1 or not re.fullmatch(r'[0-9]{1,10}', lengths[0]) or
                    int(lengths[0]) > RESPONSE_BYTES or response.getheader('X-Forge-API-Contract') != 'demiflow_forge_http_v1' or
                    not response.getheader('Content-Type', '').lower().startswith('application/json')):
                raise http.client.HTTPException('Existing development response violates its bounded JSON contract')
            if 300 <= response.status < 400:
                raise http.client.HTTPException('Existing development redirects are not followed')
            # Reserve the validated encoded body before read(), including a
            # truncated-body exception's partial allocation. Decoded JSON and
            # standard-library object overhead are explicitly not this metric.
            request.body = int(lengths[0])
            self._observe(request)
            body = response.read(int(lengths[0])+1)
            request.body = len(body)
            self._observe(request)
            if len(body) != int(lengths[0]):
                raise http.client.IncompleteRead(body)
            value = strict_json(body)
            if type(value) is not dict:
                raise ValueError('Existing development response must be an object')
            self._check(request)
            if response.status >= 400:
                if (not 400 <= response.status <= 599 or type(value.get('code')) is not str or
                        type(value.get('error')) is not str):
                    raise ValueError('Invalid existing development error response')
                if response.status in {401, 403} and value.get('code') == 'unauthorized':
                    value = dict(value, code='upstream_unauthorized', upstream_code='unauthorized')
                raise ForwardedError(response.status, value)
            expected = 202 if name == 'dev-turn-start' else 200
            if response.status != expected:
                raise ValueError('Unexpected existing development response status')
            return value
        finally:
            if response is not None:
                response.close()
            connection.close()
            request.connection = request.original_socket = None

    def _capability_value(self, value):
        if (value.get('format') != 'demiflow_development_chat_v1' or
                type(value.get('enabled')) is not bool or type(value.get('start')) is not bool or
                value.get('mode') != self.deployment.mode or type(value.get('providers')) is not list or
                not 1 <= len(value['providers']) <= 2):
            raise ValueError('Invalid original development capability contract')
        seen = set()
        for provider in value['providers']:
            if type(provider) is not dict or provider.get('id') not in self.deployment.providers:
                raise ValueError('Original development provider differs from pinned policy')
            expected = self.deployment.providers[provider['id']]
            if (provider['id'] in seen or provider.get('model') != expected['model'] or
                    provider.get('version', expected['version']) != expected['version'] or
                    type(provider.get('available')) is not bool):
                raise ValueError('Original development provider changed')
            seen.add(provider['id'])
        return dict(value, start=value['enabled'] and value['start'],
            providers=[dict(provider, available=value['enabled'] and provider['available']) for provider in value['providers']])

    def forward(self, workspace, root, name, arguments, *, mutation=False, consumer_closed=None):
        from demiflow.workbench.conversations.http import GET_FIELDS
        from demiflow.workbench.conversations.http import POST_FIELDS
        fields = POST_FIELDS if mutation else GET_FIELDS
        if name not in fields or name == 'dev-capabilities':
            reject('Unsupported existing development endpoint', 'invalid_request')
        required, optional = fields[name]
        if type(arguments) is not dict or not required <= arguments.keys() or arguments.keys()-required-optional:
            reject('Invalid existing development request fields', 'invalid_request')
        if not mutation and any(type(value) is not str for value in arguments.values()):
            reject('Existing development query values must be strings', 'invalid_request')
        project = self.project(workspace, root)
        if mutation and project['mode'] == 'history_only':
            raise ForwardedError(403, dict(code='forbidden', error='This existing development connection is history-only', operation_started=False))
        with self._request(name, mutation, consumer_closed) as request:
            encoded = wire(arguments, REQUEST_BYTES) if mutation else None
            if encoded is not None:
                request.request_bytes = len(encoded)
                self._observe(request)
            if mutation:
                # Dynamic upstream authority, under the same absolute deadline
                # and slot. start:false alone must NOT prevent cancel/recovery.
                caps = self._capability_value(self._roundtrip(request, workspace, 'dev-capabilities'))
                if caps.get('enabled') is not True:
                    raise ForwardedError(403, dict(code='forbidden', error='Original backend no longer permits this project', operation_started=False))
            return self._roundtrip(request, workspace, name, payload=arguments if mutation else None,
                                   query=None if mutation else arguments, encoded=encoded)

    def capabilities(self, workspace, root, *, consumer_closed=None):
        if workspace not in self.projects or root_identity(root) != self.projects[workspace]['identity']:
            return dict(format='demiflow_development_chat_v1', enabled=False, start=False, mode=None,
                service_configured=True, project_authorized=False, backend='existing_http',
                reason='本项目未授予开发能力；已有开发服务的授权不会自动转给此项目。', providers=[])
        try:
            self.project(workspace, root)
            with self._request('dev-capabilities', False, consumer_closed) as request:
                value = self._capability_value(self._roundtrip(request, workspace, 'dev-capabilities'))
            value = dict(value, service_configured=True, project_authorized=value.get('enabled') is True,
                         backend='existing_http', read_only=self.projects[workspace]['mode']=='history_only')
            if value['read_only']:
                value.update(start=False, reason='已有开发历史 · 只读。原服务和原授权保持；本连接不发送开发操作。',
                             providers=[dict(item, available=False) for item in value['providers']])
            return value
        except Exception as error:
            return dict(format='demiflow_development_chat_v1', enabled=False, start=False, mode=None,
                service_configured=True, project_authorized=True, backend='existing_http',
                reason='已有开发服务连接不可用或来源已变化；请核对连接。', providers=[],
                connection_error=type(error).__name__)

    def diagnostics(self):
        with self._condition:
            return dict(active_connections=len(self._active), peak_connections=self.peak_connections,
                peak_encoded_wire_buffer_bytes=self.peak_wire_buffer_bytes,
                control_waiters=int(self._control_waiting),
                monitor_threads=1 if self._watcher is not None and self._watcher.is_alive() else 0,
                closed=self._closed.is_set(), observations=list(self.observations))

    def close(self):
        self._closed.set()
        with self._condition:
            requests = tuple(self._active)
        for request in requests:
            self._abort(request, 'Existing development client closed')
        deadline = time.monotonic()+2
        if self._watcher is not None and self._watcher.ident is not None:
            self._watcher.join(max(0, deadline-time.monotonic()))
        with self._condition:
            while (self._active or self._control_waiting) and time.monotonic() < deadline:
                self._condition.wait(max(0, deadline-time.monotonic()))
            if self._active or self._control_waiting or (self._watcher is not None and self._watcher.is_alive()):
                reject('Existing development client transport cleanup is still pending', 'busy')
