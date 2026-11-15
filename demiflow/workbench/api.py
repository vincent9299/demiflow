"""Loopback workspace viewer; execution requires explicit enable_runs mode."""

from __future__ import annotations

import base64
from contextlib import contextmanager
import hmac
import hashlib
import html as html_markup
from http.server import BaseHTTPRequestHandler
from http.server import ThreadingHTTPServer
import json
import math
import os
from pathlib import Path
import re
import secrets
import select
import socket
import stat
import time
from collections import OrderedDict
import threading
from urllib.parse import parse_qs
from urllib.parse import urlsplit

from demiflow.workbench.projects import workspace as api
from demiflow.workbench.projects import registry as console
from demiflow.workbench.runs import operations as run_operations
from demiflow.workbench.runs.authority import for_http as run_authority
from demiflow.forge.platform import bounded_json
from demiflow.forge.platform_environment import read_bounded
from demiflow.forge.schema import ForgeError
from demiflow.workbench.conversations import http as development_http
from demiflow.workbench.analysis import http as analysis_http

ASSETS = Path(__file__).with_name("web")
HTTP_CONTRACT = 'demiflow_forge_http_v1'
BODY_BYTES = 65536
RESPONSE_BYTES = 9 * 1024**2
# Required and optional wire fields; these describe transport, never Flow operators.
GET_FIELDS = {
    'forge-context': (set(), {'task'}),
    'forge-tools': (set(), set()),
    'run-context': ({'run_id'}, {'attempt'}),
    'run-tools': (set(), set()),
    'graph': ({'basis_kind'}, {'entry','params','report_digest','run_id','attempt','node_id','node_offset'}),
    'graph-source': ({'graph_binding','node_id','kind'}, {'resource','logical_node_id','occurrence_id'}),
    'parameter-state': ({'path'}, set()),
    'run-observations': ({'run_id','attempt'}, {'node_id','action_id','cursor','limit'}),
    'project-discovery': (set(), set()),
    'project-document': ({'path', 'expected'}, set()),
    'project-table': ({'id', 'expected'}, {'version'}),
    'project-sample': ({'id', 'expected', 'version', 'binding'}, {'columns', 'offset', 'limit', 'row_id'}),
    'project-table-profile': ({'id','expected','version','binding','columns'},set()),
    'project-status': (set(), set()),
    'audit': (set(), set()),
    'context': (set(), set()), 'changes': (set(), set()),
    'reports': (set(), {'offset', 'limit', 'cursor'}),
    'run-snapshot': ({'run_id'}, {'attempt','revision'}),
    'run-events': ({'run_id'}, {'attempt','cursor','limit'}),
    'run-metrics': ({'run_id'}, {'attempt','points','resolution'}),
    'run-request': ({'request_id'}, set()),
    'run-compare': ({'left', 'right'}, set()),
    'run-source': ({'run_id', 'path'}, set()),
    'run-evidence': ({'run_id', 'reference'}, set()),
    'parameters': ({'path'}, set()), 'source': ({'path'}, {'expected_sha256'}),
    'table': ({'alias'}, {'expected_binding', 'version', 'version_binding'}),
    'table-versions': ({'alias'}, {'expected_binding', 'version'}),
    'table-profile': ({'alias','expected_binding','columns'},{'version', 'version_binding'}),
    'sample': ({'alias'}, {'columns', 'offset', 'limit', 'row_id', 'match', 'expected_binding', 'version', 'version_binding'}),
    'media': ({'alias', 'row_id'}, {'field', 'field_path', 'expected_binding', 'version', 'version_binding'}),
    'table-evidence': ({'alias', 'row_id', 'field_path'}, {'expected_binding', 'version', 'version_binding'}),
    'report': ({'digest'}, set()), 'diff': ({'left', 'right'}, set()),
}
POST_FIELDS = {
    'project-capabilities': ({'workspace_id','expected','capabilities'}, set()),
    'parameter-preview': ({'path','expected_sha256','graph_binding','replace'}, set()),
    'parameters-update': ({'path','expected_sha256','proposed_sha256','graph_binding','replace'}, set()),
    'project-create': ({'root', 'name'}, set()),
    'project-register': ({'root'}, {'name'}),
    'project-remove': ({'workspace_id', 'expected'}, set()),
    'project-permissions': ({'workspace_id', 'expected', 'run_submit', 'run_control', 'attach'}, set()),
    'flow-register': ({'entry', 'params'}, set()),
    'project-check': (set(), set()),
    'audit-check': ({'kind', 'expected_policy'}, {'digest'}),
    'inspect': ({'entry', 'params'}, set()),
    'run-admission': ({'entry','params'}, {'limits'}),
    'run-request-reconcile': ({'request_id'},set()),
    'run-reconcile': ({'run_id', 'expected_attempt'}, set()),
    'run-attach': ({'run_id', 'attempt', 'output', 'alias', 'expected_result'}, set()),
}
GET_FIELDS.update({name: spec.wire_fields() for name, spec in run_operations.HTTP_GET.items()})
POST_FIELDS.update({name: spec.wire_fields() for name, spec in run_operations.HTTP_POST.items()})
GET_FIELDS.update(development_http.GET_FIELDS)
POST_FIELDS.update(development_http.POST_FIELDS)
GET_FIELDS.update(analysis_http.GET_FIELDS)
POST_FIELDS.update(analysis_http.POST_FIELDS)

ERROR_STATUS = {'invalid_request': 400, 'not_found': 404, 'conflict': 409,
                'busy': 503, 'resource_limit': 413, 'operation_rejected': 422,
                'operation_failed': 500}


class SessionUnauthorized(PermissionError):
    """A client must obtain a valid session, rather than retry its request."""


class RequestNotDispatched(ForgeError):
    """Capacity rejected the request before its operation was invoked."""

    def __init__(self, message):
        super().__init__(message, code='busy')


def _session_token(path):
    if path is None:
        return secrets.token_urlsafe(32)
    fd = os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
    with os.fdopen(fd, 'rb') as source:
        info = os.fstat(source.fileno())
        if (not stat.S_ISREG(info.st_mode) or info.st_uid != os.getuid()
                or stat.S_IMODE(info.st_mode) != 0o600 or info.st_size > 256):
            raise ForgeError('Token file must be owned by this user, regular, mode 0600 and at most 256 bytes')
        raw = source.read(257)
    if len(raw) > 256 or not re.fullmatch(rb'[A-Za-z0-9_-]{43,128}\n?', raw):
        raise ForgeError('Invalid token file content')
    return raw.decode('ascii').rstrip('\n')


def _json(raw):
    def pairs(items):
        result = {}
        for key, value in items:
            if key in result:
                raise ValueError('Duplicate JSON field')
            result[key] = value
        return result
    def constant(value):
        raise ValueError('Nonfinite JSON number')
    def number(value):
        result = float(value)
        if not math.isfinite(result):
            raise ValueError('Nonfinite JSON number')
        return result
    try:
        return json.loads(raw, object_pairs_hook=pairs, parse_constant=constant, parse_float=number)
    except (ValueError, RecursionError) as exc:
        raise ForgeError('Invalid JSON request', code='invalid_request') from exc


def _fields(value, spec):
    required, optional = spec
    if type(value) is not dict or not required <= value.keys() or value.keys() - required - optional:
        raise ForgeError('Missing or unknown request fields', code='invalid_request')


class WorkspaceServer(ThreadingHTTPServer):
    daemon_threads = True
    allow_reuse_address = False
    request_queue_size = 8

    def __init__(self, root=None, port=0, *, enable_runs=False, control_only=False, registry=None, api_only=False, token_file=None, development_config=None,
                 enable_config_write=False, workspace_config_write=False, analysis_config=None):
        if (root is None) == (registry is None):
            raise ForgeError('Select one workspace root or console registry')
        self.registry = Path(registry).absolute() if registry is not None else None
        self.root = Path(root).absolute() if root is not None else None
        if self.registry is not None:
            if workspace_config_write:
                raise ForgeError('Single-workspace grant cannot be used for a console registry')
            console.load_registry(self.registry)
        else:
            _, value, _ = api.load_workspace(self.root)
            self.single = dict(format=console.FORMAT_V3, projects={api.workspace_id(self.root, value):
                dict(root=str(self.root), name=value['name'], permissions={
                    **{k:True for k in console.PERMISSIONS},
                    **{k:bool(workspace_config_write) if k=='config_write' else False for k in console.CAPABILITIES}})})
        if enable_runs and control_only:
            raise ForgeError("Choose enable_runs or control_only")
        self.enable_runs = bool(enable_runs)
        self.enable_config_write = bool(enable_config_write)
        self.control_only = bool(control_only)
        self.api_only = bool(api_only)
        self.token = _session_token(token_file)
        self.requests = threading.BoundedSemaphore(10)
        self.workers = threading.BoundedSemaphore(1)
        from demiflow.workbench.reader import Reader
        self.native_reader = Reader(owner_loop=True)
        self.control_workers = threading.BoundedSemaphore(1)
        self.control_waiters = threading.BoundedSemaphore(3)
        self.snapshots=OrderedDict()
        self.snapshot_bytes=0
        self.development_readers = threading.BoundedSemaphore(2)
        self.development = None
        self.analysis = None
        self.analysis_requests = threading.BoundedSemaphore(2)
        self.analysis_stops = threading.BoundedSemaphore(1)
        from demiflow.workbench.analysis.media_tickets import MediaTickets
        self.media_tickets=MediaTickets(authorization_epoch=lambda:hashlib.sha256(self.token.encode()).hexdigest())
        if development_config is not None:
            from demiflow.workbench.conversations.service import connect
            self.development = connect(development_config)
            try:
                partition = self.development.deployment.validate_runtime(required=self.enable_runs or self.control_only)
                if partition is not None:
                    from demiflow.workbench.host_partition import location
                    if str(location()) != self.development.deployment.value['runtime_domain']:
                        raise ForgeError('HTTP runtime domain differs from development host partition')
            except BaseException:
                self.development.close()
                raise
        try:
            if analysis_config is not None:
                from demiflow.workbench.analysis.web_service import WebService
                self.analysis = WebService(self, analysis_config)
            super().__init__(("127.0.0.1", port), Handler)
        except BaseException:
            self.native_reader.close()
            if self.analysis is not None:
                self.analysis.close()
            if self.development is not None:
                self.development.close()
            raise
        self.origin = f"http://127.0.0.1:{self.server_port}"

    def server_close(self):
        from contextlib import ExitStack
        try:
            # Every independent owned resource is closed even if another
            # cleanup reports unknown/pending; no live helper is dropped.
            with ExitStack() as cleanup:
                if self.development is not None:cleanup.callback(self.development.close)
                if self.analysis is not None:cleanup.callback(self.analysis.close)
                if hasattr(self,'media_tickets'):cleanup.callback(self.media_tickets.close)
                cleanup.callback(self.native_reader.close)
        finally:
            self.development=None;self.analysis=None
            super().server_close()

    def serve_forever(self,poll_interval=.5):
        self.native_reader.begin_owner()
        try:return super().serve_forever(poll_interval=poll_interval)
        finally:self.native_reader.end_owner()

    def service_actions(self):
        self.native_reader.launch_pending()
        # Housekeeping shares the original native-read admission. It must not
        # hold the reader mutex while a foreground request owns that lane.
        if self.workers.acquire(blocking=False):
            try:
                try:self.native_reader.maintain()
                except ForgeError:
                    # Unknown cleanup keeps its original ownership held.
                    pass
            finally:self.workers.release()
        super().service_actions()

    def inventory(self):
        return console.load_registry(self.registry) if self.registry is not None else self.single

    def analysis_execution_profile(self):
        reader=getattr(self.analysis,'execution_profile',None)
        return reader() if callable(reader) else None

    def permission_ceiling(self):
        analysis = self.analysis.ceiling() if self.analysis is not None else dict(analysis_execute=False,analysis_write=False,case_annotate=False)
        return dict(run_submit=self.enable_runs,run_control=self.enable_runs or self.control_only,
                     attach=self.enable_runs,config_write=self.enable_config_write,
                     **analysis)

    def effective_permissions(self,item):
        ceiling=self.permission_ceiling()
        return {key:value and ceiling[key] for key,value in
                {**console.permissions(item),**console.capabilities(item)}.items()}

    def resolve(self, path):
        inventory = self.inventory()
        if path.startswith('/api/workspaces/'):
            parts = path.split('/')
            if len(parts) != 5 or not parts[4]:
                raise ForgeError('Invalid workspace endpoint', code='invalid_request')
            key, endpoint = parts[3], '/api/' + parts[4]
        elif self.registry is None:
            key, endpoint = next(iter(inventory['projects'])), path
        else:
            raise ForgeError('An explicit registered workspace ID is required', code='invalid_request')
        root, allowed = console.resolve_workspace(inventory, key)
        permissions=self.effective_permissions(inventory['projects'][key])
        return endpoint, root, permissions

    def snapshot(self,root,**query):
        from demiflow.workbench.runs.view import snapshot
        key=(str(root),query['run_id'],query.get('attempt'))
        now=time.monotonic();cached=self.snapshots.get(key)
        if cached is None or now-cached[0]>1:
            value=snapshot(root,query['run_id'],attempt=query.get('attempt'))
            size=len(bounded_json(value,128*1024))
            if key in self.snapshots:
                self.snapshot_bytes-=self.snapshots.pop(key)[2]
            while self.snapshots and (len(self.snapshots)>=32 or self.snapshot_bytes+size>8*1024**2):
                self.snapshot_bytes-=self.snapshots.popitem(last=False)[1][2]
            self.snapshots[key]=(now,value,size);self.snapshot_bytes+=size
            cached=self.snapshots[key]
        age=now-cached[0];value=cached[1]
        freshness={**value.get('freshness',{}),'server_cache_age_s':age}
        if freshness.get('telemetry_age_s') is not None:
            freshness['telemetry_age_s']+=age
        if query.get('revision') and query['revision']==value.get('revision'):
            return dict(run_id=value['run_id'],attempt=value['attempt'],revision=value['revision'],unchanged=True,freshness=freshness)
        return {**value,'freshness':freshness}

    def process_request(self, request, address):
        if not self.requests.acquire(blocking=False):
            try:
                raw = b'{"code":"busy","error":"Request capacity exceeded","operation_started":false}'
                request.settimeout(1)
                request.sendall(
                    b'HTTP/1.1 503 Busy\r\nContent-Type: application/json\r\nCache-Control: no-store\r\n'
                    b'Content-Length: ' + str(len(raw)).encode() + b'\r\nConnection: close\r\n\r\n' + raw
                )
            except OSError:
                pass
            finally:
                self.shutdown_request(request)
            return
        try:
            super().process_request(request, address)
        except BaseException:
            self.requests.release()
            raise

    def process_request_thread(self, request, address):
        try:
            super().process_request_thread(request, address)
        finally:
            self.requests.release()


class Handler(BaseHTTPRequestHandler):
    server: WorkspaceServer

    def setup(self):
        super().setup()
        self.connection.settimeout(65)

    def log_message(self, *args):
        pass

    def _send(self, value, status=200, *, mime="application/json; charset=utf-8", binding=None,headers=None):
        raw = (
            bounded_json(value, RESPONSE_BYTES) if not isinstance(value, bytes) else value
        )
        self.send_response(status)
        self.send_header("Content-Type", mime)
        self.send_header("Content-Length", str(len(raw)))
        self.send_header("Cache-Control", "no-store")
        self.send_header("X-Content-Type-Options", "nosniff")
        self.send_header("Referrer-Policy", "no-referrer")
        self.send_header('X-Forge-API-Contract', HTTP_CONTRACT)
        if binding is not None:
            self.send_header("X-Forge-Binding", binding)
        self.send_header(
            "Content-Security-Policy",
            "default-src 'self'; script-src 'self'; worker-src 'self'; style-src 'self'; img-src 'self' blob:; media-src 'self' blob:; connect-src 'self'; frame-ancestors 'none'; base-uri 'none'",
        )
        for key,value in (headers or {}).items():
            if "\r" in key+str(value) or "\n" in key+str(value):raise ValueError("Invalid internal response header")
            self.send_header(key,str(value))
        self.end_headers()
        if self.command!="HEAD":self.wfile.write(raw)

    def _request_origin(self):
        # SSH forwards TCP unchanged: the browser's loopback authority can use
        # a different name/port from the server listener. Trust only literal
        # loopback authorities, never arbitrary hosts or forwarded headers.
        hosts = self.headers.get_all("Host", [])
        host = hosts[0] if len(hosts) == 1 else ""
        match = re.fullmatch(r"(localhost|127\.0\.0\.1|\[::1\])(?::([1-9][0-9]{0,4}))?", host)
        if not match or (match[2] is not None and int(match[2]) > 65535):
            raise PermissionError("Invalid Host")
        import ssl
        return ("https://" if isinstance(self.connection,ssl.SSLSocket) else "http://") + host

    def _consumer_closed(self):
        import ssl
        if isinstance(self.connection,ssl.SSLSocket):return self.connection.fileno()<0
        try:
            # Python's positive socket timeout can wait before recv even with
            # MSG_DONTWAIT. Only peek an immediately readable descriptor;
            # this check must not delay dispatching the owned output read.
            if not select.select([self.connection],[],[],0)[0]:return False
            return self.connection.recv(1,socket.MSG_PEEK|socket.MSG_DONTWAIT)==b""
        except (BlockingIOError,TimeoutError):return False
        except (OSError,ValueError):return True

    def _authorize(self):
        origin = self._request_origin()
        origins = self.headers.get_all("Origin", [])
        if len(origins) > 1 or origins and origins[0] != origin:
            raise PermissionError("Invalid Origin")
        tokens = self.headers.get_all('X-Forge-Token', [])
        if (len(tokens) != 1 or not tokens[0].isascii()
                or not hmac.compare_digest(tokens[0], self.server.token)):
            raise SessionUnauthorized("Invalid workspace session token")

    @contextmanager
    def _work(self, *, control=False):
        semaphore=self.server.control_workers if control else self.server.workers
        waiter=self.server.control_waiters if control else None
        if waiter is not None and not waiter.acquire(blocking=False):
            raise RequestNotDispatched('Control request queue is full')
        acquired=False
        try:
            acquired=semaphore.acquire(timeout=1 if control else 5)
            if not acquired:
                raise RequestNotDispatched('Worker capacity exceeded')
            if control:yield
            else:
                from demiflow.workbench.reader import using
                with using(getattr(self.server,'native_reader',None),self._consumer_closed):yield
        finally:
            if acquired:semaphore.release()
            if waiter is not None:waiter.release()

    def _failure(self, exc):
        from demiflow.workbench.conversations.existing_http import ForwardedError
        if isinstance(exc, ForwardedError):
            try:self._send(exc.payload, exc.status)
            except (BrokenPipeError, ConnectionResetError, TimeoutError):pass
            return
        if isinstance(exc, (BrokenPipeError, ConnectionResetError)):
            return
        if isinstance(exc, PermissionError):
            value, status = {'code': 'unauthorized' if isinstance(exc, SessionUnauthorized) else 'forbidden',
                             'error': str(exc)}, 403
        elif isinstance(exc, ForgeError):
            value, status = {'code': exc.code, 'error': str(exc)[:2048]}, ERROR_STATUS[exc.code]
            if getattr(exc,'reason',None)=='range_not_satisfiable':status=416
            if isinstance(exc, RequestNotDispatched):
                value['operation_started'] = False
            from demiflow.workbench.runs.view import ObservationResetRequired
            if isinstance(exc,ObservationResetRequired):
                value.update(reset_required=True,reason=exc.reason)
        elif isinstance(exc, FileNotFoundError):
            value, status = {'code': 'not_found', 'error': 'Requested resource does not exist'}, 404
        else:
            value, status = {'code': 'operation_failed', 'error': 'Operation failed; inspect the saved state before retrying a write'}, 500
        try:
            self._send(value, status)
        except (BrokenPipeError, ConnectionResetError, TimeoutError):
            pass

    def _query(self, url):
        name = url.path.removeprefix('/api/')
        if name not in GET_FIELDS:
            raise ForgeError('Unknown endpoint', code='not_found')
        try:
            fields = parse_qs(url.query, max_num_fields=16, strict_parsing=True, keep_blank_values=True)
        except ValueError as exc:
            raise ForgeError('Invalid query', code='invalid_request') from exc
        if any(len(v) != 1 or not v[0] for v in fields.values()):
            raise ForgeError('Empty or repeated query parameters', code='invalid_request')
        query = {k: v[0] for k, v in fields.items()}
        if name in run_operations.HTTP_GET:
            return run_operations.http_arguments(run_operations.HTTP_GET[name], query)
        _fields(query, GET_FIELDS[name])
        for key in ('offset', 'limit', 'row_id', 'attempt', 'version','points','node_offset'):
            if key in query:
                if not re.fullmatch(r'[0-9]{1,20}', query[key]):
                    raise ForgeError('Invalid integer query field: ' + key, code='invalid_request')
                number = int(query[key])
                low, high = (1,600) if key=='points' else (1, 100) if key == 'limit' else (1, 32) if key == 'attempt' else (1, 2**64 - 1) if key == 'version' else (0, 2**64 - 1)
                if not low <= number <= high:
                    raise ForgeError('Query field out of range: ' + key, code='invalid_request')
        for key in ('report','compact'):
            if key in query and query[key] not in {'0','1'}:
                raise ForgeError(key+' must be 0 or 1',code='invalid_request')
        for key, kind in [('columns', list), ('field_path', list), ('reference', dict), ('match', dict),('graph_binding',dict)]:
            if key in query:
                query[key] = _json(query[key])
                if type(query[key]) is not kind:
                    raise ForgeError('Invalid JSON query field: ' + key, code='invalid_request')
        if name == 'media' and (('field' in query) == ('field_path' in query)):
            raise ForgeError('Media requires one field or field_path', code='invalid_request')
        if name in {'table', 'sample', 'table-profile', 'media', 'table-evidence'}:
            if ('version' in query) != ('version_binding' in query):
                raise ForgeError('Historical version and version_binding must be provided together', code='invalid_request')
            if 'version_binding' in query and not re.fullmatch(r'[0-9a-f]{64}', query['version_binding']):
                raise ForgeError('Invalid version binding digest', code='invalid_request')
            if 'version' in query and 'expected_binding' not in query:
                raise ForgeError('Historical reads require expected_binding', code='invalid_request')
        return query

    def _body(self, name):
        lengths = self.headers.get_all('Content-Length', [])
        if (self.headers.get_all('Transfer-Encoding', []) or len(lengths) != 1
                or not re.fullmatch(r'[0-9]{1,10}', lengths[0])):
            raise ForgeError('A single Content-Length is required', code='invalid_request')
        length = int(lengths[0])
        if not 0 < length <= BODY_BYTES:
            raise ForgeError('Request body exceeds budget or is empty', code='resource_limit')
        raw = self.rfile.read(length)
        if len(raw) != length:
            raise ForgeError('Incomplete request body', code='invalid_request')
        request = _json(raw)
        if name in run_operations.HTTP_POST:
            return run_operations.http_arguments(run_operations.HTTP_POST[name], request)
        _fields(request, POST_FIELDS[name])
        if name in development_http.POST_FIELDS or name in analysis_http.POST_FIELDS:
            # The fixed services own their closed, typed value schemas.
            return request
        for key, value in request.items():
            if key in {'expected_attempt', 'attempt'}:
                if value is not None and (type(value) is not int or not 1 <= value <= 32):
                    raise ForgeError('Invalid attempt', code='invalid_request')
            elif key in {'inputs', 'outputs', 'limits','graph_binding','capabilities'}:
                if type(value) is not dict:
                    raise ForgeError('Expected an object: ' + key, code='invalid_request')
            elif key=='replace':
                if type(value) is not list or not 1<=len(value)<=32:
                    raise ForgeError('Expected 1..32 parameter replacements',code='invalid_request')
            elif key in {'new_outputs','telemetry','compact','run_submit','run_control','attach'}:
                if type(value) is not bool:
                    raise ForgeError(key+' must be boolean', code='invalid_request')
            elif type(value) is not str or not value or len(value.encode()) > 4096:
                raise ForgeError('Invalid text field: ' + key, code='invalid_request')
        return request

    def do_GET(self):
        try:
            from demiflow.workbench.analysis import media_http
            if media_http.get(self):return
            url = urlsplit(self.path)
            if len(self.path) > 8192:
                raise ForgeError('Request path exceeds budget', code='resource_limit')
            self._request_origin()
            assets = {
                "/": ("index.html", "text/html; charset=utf-8"),
                "/app.js": ("app.js", "text/javascript; charset=utf-8"),
                "/development.js": ("development.js", "text/javascript; charset=utf-8"),
                "/analysis-state.js": ("analysis-state.js", "text/javascript; charset=utf-8"),
                "/analysis.js": ("analysis.js", "text/javascript; charset=utf-8"),
                "/analysis-render.js": ("analysis-render.js", "text/javascript; charset=utf-8"),
                "/analysis-output.js": ("analysis-output.js", "text/javascript; charset=utf-8"),
                "/analysis-output-media.js": ("analysis-output-media.js", "text/javascript; charset=utf-8"),
                "/analysis-transport.js": ("analysis-transport.js", "text/javascript; charset=utf-8"),
                "/analysis-output.css": ("analysis-output.css", "text/css; charset=utf-8"),
                "/analysis-case.css": ("analysis-case.css", "text/css; charset=utf-8"),
                "/analysis-case-media.js": ("analysis-case-media.js", "text/javascript; charset=utf-8"),
                "/analysis-case.js": ("analysis-case.js", "text/javascript; charset=utf-8"),
                "/analysis.css": ("analysis.css", "text/css; charset=utf-8"),
                "/workbench-state.js": ("workbench-state.js", "text/javascript; charset=utf-8"),
                "/workbench-commands.js": ("workbench-commands.js", "text/javascript; charset=utf-8"),
                "/workbench-graph.js": ("workbench-graph.js", "text/javascript; charset=utf-8"),
                "/workbench-observer.js": ("workbench-observer.js", "text/javascript; charset=utf-8"),
                "/workbench.js": ("workbench.js", "text/javascript; charset=utf-8"),
                "/workbench.css": ("workbench.css", "text/css; charset=utf-8"),
                "/development.css": ("development.css", "text/css; charset=utf-8"),
                "/view.js": ("view.js", "text/javascript; charset=utf-8"),
                "/style.css": ("style.css", "text/css; charset=utf-8"),
                "/reading.css": ("reading.css", "text/css; charset=utf-8"),
                "/reading.js": ("reading.js", "text/javascript; charset=utf-8"),
                "/markdown-worker.js": ("markdown-worker.js", "text/javascript; charset=utf-8"),
                "/markdown-it.min.js": ("markdown-it.min.js", "text/javascript; charset=utf-8"),
                "/markdown-it.LICENSE.txt": ("markdown-it.LICENSE.txt", "text/plain; charset=utf-8"),
                "/markdown-it.NOTICES.txt": ("markdown-it.NOTICES.txt", "text/plain; charset=utf-8"),
                "/markdown-it.provenance.json": ("markdown-it.provenance.json", "application/json; charset=utf-8"),
            }
            if url.path in assets:
                if self.server.api_only:
                    raise ForgeError('Web assets are disabled', code='not_found')
                filename, mime = assets[url.path]
                self._send(read_bounded(ASSETS / filename, 512 * 1024), mime=mime)
                return
            self._authorize()
            if url.path == '/api/capabilities':
                if url.query:
                    raise ForgeError('Capabilities accepts no query', code='invalid_request')
                self._send(dict(contract=HTTP_CONTRACT, web_enabled=not self.server.api_only,
                    operations={'GET': sorted(GET_FIELDS), 'POST': sorted(POST_FIELDS)},
                    permissions={'read': True, 'inspect': True, 'audit_check': True, 'runs': self.server.enable_runs,
                                 'run_submit':self.server.enable_runs,'run_control':self.server.enable_runs or self.server.control_only,'attach':self.server.enable_runs,
                                 'project_manage': self.server.registry is not None, 'project_check': True},
                    workbench_permissions={k:self.server.permission_ceiling()[k] for k in console.CAPABILITIES},
                    workbench_contract='demiflow_workbench_graph_v1',console_v3_supported=True,
                    analysis_connected=self.server.analysis is not None,
                    analysis_execution_profile=self.server.analysis_execution_profile(),
                    analysis_features={'annotation_definitions':True,'case_cursor':True,'catalog_profile':True,'case_media':getattr(self.server.analysis,'features',{}).get('case_media') is True,'annotation_media_basis':getattr(self.server.analysis,'features',{}).get('annotation_media_basis') is True,'case_output_groups':getattr(self.server.analysis,'features',{}).get('case_output_groups') is True,'case_review_filter':getattr(self.server.analysis,'features',{}).get('case_review_filter') is True,'notebook_history':getattr(self.server.analysis,'features',{}).get('notebook_history') is True},
                    audit_contract='demiflow_forge_audit_status_v1', runtime_contract='demiflow_forge_runtime_v1',
                    limits=dict(body_bytes=BODY_BYTES, response_bytes=RESPONSE_BYTES, path_bytes=8192,
                                page_items=100, requests=10, workers=2, queue=8, worker_wait_seconds=5)))
                return
            if url.path == '/api/projects':
                if url.query:
                    raise ForgeError('Projects endpoint accepts no query', code='invalid_request')
                inventory=self.server.inventory()
                value = console.projects(inventory)
                for row in value['projects']:
                    permissions=self.server.effective_permissions(inventory['projects'][row['workspace_id']])
                    row['configured_permissions']=console.permissions(inventory['projects'][row['workspace_id']])
                    row['configured_capabilities']=console.capabilities(inventory['projects'][row['workspace_id']])
                    row['permissions']={k:permissions[k] for k in console.PERMISSIONS}
                    row['capabilities']={k:permissions[k] for k in console.CAPABILITIES}
                    row['allow_runs']=row['permissions']['run_submit']
                self._send(value)
                return
            endpoint, root, permissions = self.server.resolve(url.path)
            url = url._replace(path=endpoint)
            query = self._query(url)
            name = endpoint.removeprefix('/api/')
            if name in development_http.GET_FIELDS:
                key = self.path.split('/')[3] if self.path.startswith('/api/workspaces/') else next(iter(self.server.single['projects']))
                self._send(development_http.get(self.server, key, root, name, query, consumer_closed=self._consumer_closed))
                return
            if name in analysis_http.GET_FIELDS:
                key = self.path.split('/')[3] if self.path.startswith('/api/workspaces/') else next(iter(self.server.single['projects']))
                value=analysis_http.get(self.server,key,name,query,range_header=self.headers.get('Range'),consumer_closed=self._consumer_closed)
                from demiflow.workbench.analysis.output_transport import OutputRange
                if isinstance(value,OutputRange):media_http.send_range(self,value)
                else:self._send(value)
                return
            with self._work(control=url.path in {'/api/run-snapshot','/api/run-events'}):
                if url.path == "/api/context":
                    value = api.workspace_context(root)
                    value['runs_enabled'] = permissions['run_submit']
                    value['run_permissions'] = {k:permissions[k] for k in console.PERMISSIONS}
                    value['capabilities'] = {k:permissions[k] for k in console.CAPABILITIES}
                    value['analysis_connected'] = self.server.analysis is not None
                    value['analysis_execution_profile']=self.server.analysis_execution_profile()
                    value['analysis_features']={'annotation_definitions':True,'case_cursor':True,'catalog_profile':True,'case_media':getattr(self.server.analysis,'features',{}).get('case_media') is True,'annotation_media_basis':getattr(self.server.analysis,'features',{}).get('annotation_media_basis') is True,'case_output_groups':getattr(self.server.analysis,'features',{}).get('case_output_groups') is True,'case_review_filter':getattr(self.server.analysis,'features',{}).get('case_review_filter') is True,'notebook_history':getattr(self.server.analysis,'features',{}).get('notebook_history') is True}
                elif url.path == '/api/forge-context':
                    from demiflow.forge.tools import invoke
                    value = invoke('forge.context', root, query)
                elif url.path == '/api/forge-tools':
                    from demiflow.forge.tools import describe
                    value = describe()
                elif url.path == '/api/run-context':
                    from demiflow.workbench.runs.context import query as run_context
                    value = run_context(root, query['run_id'],
                                        attempt=int(query['attempt']) if 'attempt' in query else None,
                                        authority=run_authority(self.server, self.path, root, 'read'))
                elif url.path == '/api/run-tools':
                    from demiflow.workbench.runs.tools import describe
                    value = describe()
                elif url.path == '/api/graph':
                    from demiflow.workbench.graph.model import get
                    args=dict(query)
                    for key in ('attempt','node_offset'):
                        if key in args:args[key]=int(args[key])
                    value=get(root,**args)
                elif url.path == '/api/graph-source':
                    from demiflow.workbench.graph.source_view import get
                    value=get(root,**query)
                elif url.path == '/api/parameter-state':
                    from demiflow.workbench.graph.parameters import state
                    value=state(root,**query)
                elif url.path == '/api/run-observations':
                    from demiflow.workbench.runs.view import observations
                    args=dict(query)
                    for key in ('attempt','limit'):
                        if key in args:args[key]=int(args[key])
                    value=observations(root,**args)
                elif url.path in {'/api/run-snapshot','/api/run-events','/api/run-metrics'}:
                    from demiflow.workbench.runs import view as run_view
                    args=dict(query)
                    for key in ('attempt','points','limit'):
                        if key in args:args[key]=int(args[key])
                    value=(self.server.snapshot(root,**args) if url.path=='/api/run-snapshot' else {'/api/run-events':run_view.events,'/api/run-metrics':run_view.metrics}[url.path](root,**args))
                elif url.path == '/api/run-request':
                    from demiflow.workbench.runs.requests import read
                    value=read(root,query['request_id'])
                elif url.path == '/api/project-status':
                    value = api.project_check_status(root)
                elif url.path == '/api/project-discovery':
                    value = api.discover_project(root)
                elif url.path == '/api/project-document':
                    value = api.project_document(root, query['path'], expected=query['expected'])
                elif url.path == '/api/project-table':
                    value = api.project_table(root, query['id'], expected=query['expected'],
                                              version=int(query['version']) if 'version' in query else None)
                elif url.path == '/api/project-sample':
                    value = api.project_sample(root, query['id'], expected=query['expected'],
                        version=int(query['version']), binding=query['binding'], columns=query.get('columns'),
                        offset=int(query.get('offset', 0)), limit=int(query.get('limit', 20)),
                        row_id=int(query['row_id']) if 'row_id' in query else None)
                elif url.path == '/api/changes':
                    value = api.workspace_changes(root)
                elif url.path == '/api/audit':
                    value = api.audit_status(root)
                elif url.path == '/api/reports':
                    value = api.list_reports(root, offset=int(query.get('offset', 0)), limit=int(query.get('limit', 20)), cursor=query.get('cursor'))
                elif name in run_operations.HTTP_GET:
                    spec = run_operations.HTTP_GET[name]
                    value = run_operations.perform(spec.name, root,
                        authority=run_authority(self.server, self.path, root, spec.permission), **query)
                elif url.path in {'/api/run-compare', '/api/run-source', '/api/run-evidence'}:
                    from demiflow.workbench.runs import manager as runs
                    if url.path == '/api/run-compare':
                        value = runs.compare(root, query['left'], query['right'])
                    elif url.path == '/api/run-source':
                        value = runs.source(root, query['run_id'], query['path'])
                    else:
                        value = runs.evidence(root, query['run_id'], query['reference'])
                elif url.path == '/api/parameters':
                    value = api.read_parameters(root, query['path'])
                elif url.path == "/api/source":
                    value = api.source_text(root, query["path"], expected_sha256=query.get("expected_sha256"))
                elif url.path == "/api/table":
                    value = api.describe_table(root, query["alias"], expected_binding=query.get("expected_binding"),
                                               version=int(query["version"]) if "version" in query else None,
                                               version_binding=query.get("version_binding"))
                elif url.path == "/api/table-versions":
                    value = api.table_versions(root, query["alias"], expected_binding=query.get("expected_binding"),
                                               version=int(query["version"]) if "version" in query else None)
                elif url.path=="/api/table-profile":
                    value=api.profile_table(root,query["alias"],expected_binding=query["expected_binding"],columns=query["columns"],
                                            version=int(query["version"]) if "version" in query else None,
                                            version_binding=query.get("version_binding"))
                elif url.path=="/api/project-table-profile":
                    value=api.project_profile(root,query["id"],expected=query["expected"],version=int(query["version"]),binding=query["binding"],columns=query["columns"])
                elif url.path == "/api/report":
                    value = api.report_status(root, query["digest"])
                elif url.path == "/api/sample":
                    match = query["match"] if "match" in query else None
                    value = api.sample_table(
                        root,
                        query["alias"],
                        columns=(
                            query["columns"] if "columns" in query else None
                        ),
                        offset=int(query.get("offset", "0")),
                        limit=int(query.get("limit", "20")),
                        row_id=int(query["row_id"]) if "row_id" in query else None,
                        match=match,
                        expected_binding=query.get("expected_binding"),
                        version=int(query["version"]) if "version" in query else None,
                        version_binding=query.get("version_binding"),
                    )
                elif url.path in {"/api/media", '/api/table-evidence'}:
                    value = (api.table_media if url.path == '/api/media' else api.table_evidence)(
                        root,
                        alias=query["alias"],
                        row_id=int(query["row_id"]),
                        **({'field_path': query['field_path']} if 'field_path' in query else {'field': query['field']}),
                        expected_binding=query.get("expected_binding"),
                        version=int(query["version"]) if "version" in query else None,
                        version_binding=query.get("version_binding"),
                    )
                    if url.path == '/api/media':
                        self._send(base64.b64decode(value["data"]), mime=value["mime"], binding=value["binding"]["digest"])
                    else:
                        self._send(value)
                    return
                elif url.path == "/api/diff":
                    value = api.diff_reports(
                        root, query["left"], query["right"]
                    )
                else:
                    self._send({"error": "Unknown endpoint"}, 404)
                    return
            self._send(value)
        except Exception as exc:
            self._failure(exc)

    def do_HEAD(self):
        try:
            from demiflow.workbench.analysis import media_http
            if media_http.get(self,head=True):return
            self._authorize();self._send({'error':'Unknown HEAD endpoint'},404)
        except Exception as exc:self._failure(exc)

    def do_POST(self):
        try:
            from demiflow.workbench.analysis import media_http
            if media_http.post(self):return
            self._authorize()
            if len(self.path) > 8192 or urlsplit(self.path).query:
                raise ForgeError('Invalid mutation endpoint', code='invalid_request')
            if self.path in {'/api/project-create', '/api/project-register', '/api/project-remove', '/api/project-permissions','/api/project-capabilities'}:
                if self.server.registry is None:
                    raise ForgeError('Project management requires a console registry', code='operation_rejected')
                request = self._body(self.path.removeprefix('/api/'))
                if 'root' in request and not Path(request['root']).is_absolute():
                    raise ForgeError('Choose an absolute directory on the server', code='invalid_request')
                if self.path == '/api/project-permissions':
                    # Explicit authenticated configuration, bounded by deployment mode.
                    # Listing or registering a project never grants execution access.
                    ceiling = dict(run_submit=self.server.enable_runs,
                                   run_control=self.server.enable_runs or self.server.control_only,
                                   attach=self.server.enable_runs)
                    if not any(ceiling.values()) or any(request[key] and not allowed for key, allowed in ceiling.items()):
                        raise PermissionError('Requested permissions exceed the service deployment mode')
                if self.path == '/api/project-capabilities':
                    grants=request['capabilities']
                    if (type(grants) is not dict or set(grants)!=console.CAPABILITIES
                            or any(type(v) is not bool for v in grants.values())):
                        raise ForgeError('Capabilities require four explicit booleans',code='invalid_request')
                    ceiling=self.server.permission_ceiling()
                    inventory=self.server.inventory()
                    configured=console.capabilities(inventory['projects'].get(request['workspace_id'],{}))
                    if any(grants[key] and not configured[key] and not ceiling[key] for key in console.CAPABILITIES):
                        raise PermissionError('Requested capability exceeds the service deployment ceiling')
                with self._work():
                    action = {'/api/project-create': console.create, '/api/project-register': console.add,
                              '/api/project-remove': console.remove,
                              '/api/project-permissions': console.set_permissions,
                              '/api/project-capabilities':console.set_capabilities}[self.path]
                    value = action(self.server.registry, **request, **({'analysis_service':self.server.analysis}
                        if self.path=='/api/project-capabilities' else {}))
                self._send(value)
                return
            endpoint, root, permissions = self.server.resolve(self.path)
            name = endpoint.removeprefix('/api/')
            if name not in POST_FIELDS:
                raise ForgeError('Unknown endpoint', code='not_found')
            if name in {'project-create', 'project-register', 'project-remove', 'project-permissions','project-capabilities'}:
                raise ForgeError('Project management requires a console endpoint', code='invalid_request')
            if name in development_http.POST_FIELDS:
                key = self.path.split('/')[3] if self.path.startswith('/api/workspaces/') else next(iter(self.server.single['projects']))
                self._send(development_http.post(self.server, key, root, name, self._body(name), consumer_closed=self._consumer_closed), 202 if name == 'dev-turn-start' else 200)
                return
            if name in analysis_http.POST_FIELDS:
                key = self.path.split('/')[3] if self.path.startswith('/api/workspaces/') else next(iter(self.server.single['projects']))
                self._send(analysis_http.post(self.server,key,root,name,self._body(name),consumer_closed=self._consumer_closed))
                return
            permission=(run_operations.HTTP_POST[name].permission if name in run_operations.HTTP_POST else
                        'config_write' if name in {'parameter-preview','parameters-update'} else
                        'run_control' if name in {'run-cancel','run-reconcile','run-request-reconcile'} else
                        'attach' if name=='run-attach' else 'run_submit')
            if name not in {'inspect','audit-check','project-check','run-admission'} and not permissions[permission]:
                raise PermissionError('Required run permission is disabled: '+permission)
            request = self._body(name)
            with self._work(control=name=='run-cancel'):
                if name in {'parameter-preview','parameters-update'}:
                    from demiflow.workbench.graph import parameters
                    def authorize():
                        _,current_root,current_permissions=self.server.resolve(self.path)
                        if current_root!=root or not current_permissions['config_write']:
                            raise PermissionError('Project membership or config-write grant changed')
                    authorize()
                    value=(parameters.preview(root,**request) if name=='parameter-preview'
                           else parameters.update(root,authorize=authorize,**request))
                elif name in run_operations.HTTP_POST:
                    spec = run_operations.HTTP_POST[name]
                    value = run_operations.perform(spec.name, root,
                        authority=run_authority(self.server, self.path, root, spec.permission), **request)
                elif name == 'flow-register':
                    value = api.register_flow(root, **request)
                elif name == 'run-admission':
                    from demiflow.workbench.runs.admission import describe
                    value=describe(root,**request)
                elif name == 'run-request-reconcile':
                    from demiflow.workbench.runs.requests import reconcile
                    value=reconcile(root,request['request_id'])
                elif name == 'inspect':
                    value = api.inspect_flow(root, **request)
                elif name == 'project-check':
                    value = api.check_project(root)
                elif name == 'audit-check':
                    value = api.audit_check(root, kind=request['kind'], expected_policy=request['expected_policy'],
                                            report_digest=request.get('digest'))
                else:
                    from demiflow.workbench.runs import manager as runs
                    if name == 'run-reconcile':
                        value = runs.reconcile(root, **request)
                    else:
                        value = runs.attach_result(root, **request)
            self._send(value)
        except Exception as exc:
            self._failure(exc)


def serve(root=None, *, port=0, enable_runs=False, control_only=False, registry=None, api_only=False, token_file=None, development_config=None,
          enable_config_write=False,workspace_config_write=False,analysis_config=None):
    server = WorkspaceServer(root, port, enable_runs=enable_runs, control_only=control_only, registry=registry,
                             api_only=api_only, token_file=token_file, development_config=development_config,
                             enable_config_write=enable_config_write,workspace_config_write=workspace_config_write,
                             analysis_config=analysis_config)
    print(server.origin + ('/' if token_file is not None else '/#token=' + server.token), flush=True)
    try:
        server.serve_forever(poll_interval=0.2)
    except KeyboardInterrupt:
        pass
    finally:
        server.server_close()


def export_report(root, digest, path):
    report = api.load_report(root, digest)
    payload = (
        json.dumps(report, ensure_ascii=False, allow_nan=False)
        .replace("<", "\\u003c")
        .replace(">", "\\u003e")
        .replace("&", "\\u0026")
    )
    html = read_bounded(ASSETS / "index.html", 512 * 1024).decode()
    # Inline only sealed Web assets; document text never becomes HTML/script.
    # Escape any script terminator inside vendored or application JavaScript.
    for name in ("style.css", "reading.css", "development.css", "workbench.css"):
        css = read_bounded(ASSETS / name, 512 * 1024).decode()
        css = re.sub(r"</style", lambda match: "<\\/" + match[0][2:], css, flags=re.IGNORECASE)
        html = html.replace(f'<link rel="stylesheet" href="/{name}">', "<style>" + css + "</style>")
    for name in ("markdown-it.min.js", "view.js", "reading.js", "development.js", "workbench-state.js", "workbench-commands.js",
                 "workbench-graph.js", "workbench-observer.js", "workbench.js", "app.js"):
        script = read_bounded(ASSETS / name, 512 * 1024).decode()
        script = re.sub(r"</script", lambda match: "<\\/" + match[0][2:], script, flags=re.IGNORECASE)
        prefix = "<script>window.FORGE_EXPORT=" + payload + ";</script>" if name == "app.js" else ""
        html = html.replace(f'<script src="/{name}" defer></script>', prefix + "<script>" + script + "</script>")
    notices = "\n\n".join(read_bounded(ASSETS / name, 512 * 1024).decode()
                            for name in ("markdown-it.LICENSE.txt", "markdown-it.NOTICES.txt"))
    html = html.replace("</body>", '<details class="export-notices"><summary>第三方许可证与通知</summary><pre>'
                        + html_markup.escape(notices) + "</pre></details></body>")
    raw = html.encode("utf-8")
    if len(raw) > 16 * 1024**2:
        raise ForgeError("Export exceeds HTML budget")
    path = Path(path).absolute()
    api.no_symlinks(path)
    with path.open("xb") as output:
        output.write(raw)
    return {
        "status": "exported",
        "path": str(path),
        "digest": digest,
        "samples_included": False,
        "current_binding": "not_live",
    }
