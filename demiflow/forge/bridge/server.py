"""Authenticated Unix-socket control bridge server owned by Forge."""
from __future__ import annotations

from contextlib import nullcontext
import hmac
import os
from pathlib import Path
import secrets
import socket
import sys
import tempfile
import threading

from .common import decode, reject, wire
from .tools import CATALOG_TOOL, INPUT_TOOL, PROJECT_LAYOUT_TOOL, REVIEW_TOOL, run_project_layout


class BridgeServer:
    """Bounded control IPC owned by Forge and hosted by either Forge CLI or Workbench.

    The MCP subprocess or sandbox shim receives only an endpoint and one-turn capability token.
    Every request is verified against token, turn_id, epoch, active-turn check, and fixed methods.
    """

    def __init__(self, input_access, *, delivery_review=None, project_root=None, protected_snapshot=None,
                 catalog=None, allowed_read_roots=None, denied_paths=()):
        self.lock = threading.Lock()
        self.input_access = input_access
        self.catalog = catalog
        self.protected_snapshot = self._control_snapshot(project_root, protected_snapshot)
        self.delivery_review = delivery_review
        self.project_root = Path(project_root).absolute() if project_root is not None else None
        self.allowed_read_roots = allowed_read_roots
        self.denied_paths = tuple(denied_paths or ())
        self.token = secrets.token_urlsafe(32)
        self.directory = Path(tempfile.mkdtemp(prefix='demiflow-dev-control-'))
        self.path = self.directory / 'bridge.sock'
        self.socket = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        self.socket.bind(str(self.path))
        os.chmod(self.path, 0o600)
        self.socket.listen(4)
        self.socket.settimeout(.2)
        self.stopping = threading.Event()
        self.count = 0
        self.authorized_layout_paths: set[str] = set()
        self.thread = threading.Thread(target=self._run, name='development-control', daemon=True)
        self.thread.start()

    @staticmethod
    def _control_snapshot(root, snapshot):
        if snapshot is None and root is not None:
            from demiflow.forge.project_guard import capture_protected_files
            return capture_protected_files(root)
        return snapshot

    def finish_turn(self) -> list[str]:
        with self.lock:
            self.input_access = self.catalog = self.delivery_review = self.project_root = self.protected_snapshot = None
            self.allowed_read_roots = None
            self.denied_paths = ()
            paths = sorted(self.authorized_layout_paths)
            self.authorized_layout_paths.clear()
            return paths

    def _record_layout_paths(self, action, answer):
        if not isinstance(answer, dict):
            return
        recorded: set[str] = set()
        if action == 'ensure_dirs':
            for d in answer.get('created') or ():
                if isinstance(d, str) and d:
                    recorded.add(d)
        elif action == 'move_path':
            for key in ('src', 'dst'):
                val = answer.get(key)
                if isinstance(val, str) and val:
                    recorded.add(val)
        elif action == 'remove_path':
            for p in answer.get('removed') or ():
                if isinstance(p, str) and p:
                    recorded.add(p)
        elif action in ('register_flow', 'bind_params'):
            params = answer.get('params')
            if isinstance(params, str) and params:
                recorded.add(params)
            elif isinstance(params, list):
                recorded.update(p for p in params if isinstance(p, str) and p)
        elif action == 'sync_env_example':
            recorded.add('.env.example')
            if answer.get('gitignore_updated'):
                recorded.add('.gitignore')
        self.authorized_layout_paths.update(recorded)

    def set_turn(self, input_access, *, delivery_review=None, project_root=None, protected_snapshot=None,
                 catalog=None, allowed_read_roots=None, denied_paths=()):
        with self.lock:
            self.input_access = input_access
            self.protected_snapshot = self._control_snapshot(project_root, protected_snapshot)
            self.catalog = catalog
            self.delivery_review = delivery_review
            self.project_root = Path(project_root).absolute() if project_root is not None else None
            self.allowed_read_roots = allowed_read_roots
            self.denied_paths = tuple(denied_paths or ())
            self.count = 0
            if input_access is not None:
                self.authorized_layout_paths.clear()

    def _answer(self, request):
        with self.lock:
            active = self.input_access
            if (active is None or type(request) is not dict or set(request) != {'token', 'turn_id', 'epoch', 'method', 'params'}
                    or type(request['token']) is not str or not hmac.compare_digest(request['token'], self.token)
                    or (request['turn_id'], request['epoch']) != (active.tid, active.epoch)):
                reject('Development control capability does not match this turn', 'not_found')
            active.require_active()
        return self.dispatch(request['method'], request['params'], active)

    def dispatch(self, method, params, active):
        from demiflow.forge.project_guard import guarded_control_writes
        with self.lock:
            if active is None or active is not self.input_access:
                reject('Development control capability does not match this turn', 'not_found')
            active.require_active()
            tools = [INPUT_TOOL]
            if self.delivery_review is not None:
                tools.append(REVIEW_TOOL)
            if self.project_root is not None:
                tools.append(PROJECT_LAYOUT_TOOL)
            if self.catalog is not None:
                tools.append(CATALOG_TOOL)
            if method == 'forge/dep':
                if self.project_root is None:
                    reject('Dependency updates require an active editable project turn', 'operation_rejected')
                if type(params) is not dict or params.get('action') not in {'vendor', 'remove'}:
                    reject('Unsupported restricted dependency operation', 'invalid_request')
                from demiflow.forge.dependencies import remove as dep_remove, vendor as dep_vendor
                action = params['action']
                pkg_name = params.get('name')
                if not isinstance(pkg_name, str) or not pkg_name:
                    reject('Package name is required', 'invalid_request')
                if action == 'vendor':
                    source_raw = params.get('source')
                    if not isinstance(source_raw, str) or not source_raw:
                        reject('Vendor source directory is required', 'invalid_request')
                    answer = dep_vendor(
                        self.project_root,
                        pkg_name,
                        Path(source_raw),
                        allowed_roots=self.allowed_read_roots,
                        denied_paths=self.denied_paths,
                    )
                    self.authorized_layout_paths.update({'packages', f'packages/{pkg_name}'})
                else:
                    answer = dep_remove(self.project_root, pkg_name)
                    self.authorized_layout_paths.update({'packages', f'packages/{pkg_name}'})
                return answer
            if method == 'tools/list' and params == {}:
                return dict(tools=tools)
            if (method != 'tools/call' or type(params) is not dict or set(params) != {'name', 'arguments'}
                    or params['name'] not in [tool['name'] for tool in tools]):
                reject('Unsupported development control operation')
            name, arguments = params['name'], params['arguments']
            if name == PROJECT_LAYOUT_TOOL['name']:
                action = arguments.get('action') if isinstance(arguments, dict) else None
                guard = nullcontext()
                if action in ('register_workflow', 'register_flow', 'bind_params', 'slim_manifest'):
                    guard = guarded_control_writes(self.project_root, self.protected_snapshot, active.require_active)
                with guard:
                    answer = run_project_layout(self.project_root, arguments)
                self._record_layout_paths(action, answer)
            elif name == REVIEW_TOOL['name']:
                answer = self.delivery_review.request(arguments)
            elif name == CATALOG_TOOL['name']:
                answer = self.catalog(arguments)
        if name == INPUT_TOOL['name']:
            answer = active.invoke(arguments)
        return dict(content=[dict(type='text', text=wire(answer, 60000).decode())], isError=False)

    def mcp_server(self, tid, epoch):
        return dict(
            name='development-protocol',
            command=sys.executable,
            args=['-I', '-B', str(Path(__file__).with_name('mcp_bridge.py')), '--connect',
                  str(self.path), self.token, tid, epoch],
            env=[],
        )

    def _run(self):
        while not self.stopping.is_set():
            try:
                connection, _ = self.socket.accept()
            except socket.timeout:
                continue
            except OSError:
                return
            with connection:
                connection.settimeout(2)
                try:
                    with self.lock:
                        if self.count >= 256:
                            reject('Development control request budget exceeded', 'resource_limit')
                        self.count += 1
                    with connection.makefile('rb') as stream:
                        raw = stream.readline(65537)
                    if len(raw) > 65536 or not raw.endswith(b'\n'):
                        reject('Development control frame exceeds budget')
                    result = dict(result=self._answer(decode(raw)))
                except Exception as exc:
                    result = dict(error=dict(code=-32602, message=str(exc)[:2048]))
                try:
                    connection.sendall(wire(result, 65536) + b'\n')
                except OSError:
                    pass

    def close(self):
        self.stopping.set()
        self.socket.close()
        self.thread.join(timeout=3)
        try:
            self.path.unlink()
        except FileNotFoundError:
            pass
        try:
            self.directory.rmdir()
        except OSError:
            pass
