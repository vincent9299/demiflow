"""Own a bounded number of native Jupyter processes behind the workbench."""
from __future__ import annotations

from dataclasses import dataclass
import hashlib
import hmac
import http.client
from itertools import islice
import json
import os
from pathlib import Path
import secrets
import signal
import shutil
import socket
import subprocess
import tempfile
import threading
import time

from demiflow.forge.schema import ForgeError
from demiflow.forge.io import no_symlinks


@dataclass
class Instance:
    project: str
    root: Path
    port: int
    token: str
    base_url: str
    process: subprocess.Popen
    state: Path
    notebooks: Path
    executable: bool
    grants: dict
    channels: int = 0
    requests: int = 0


class Notebooks:
    def __init__(self, server, python):
        self.server = server
        self.python = Path(python).absolute()
        if not self.python.is_file() or not os.access(self.python, os.X_OK):
            raise ForgeError('Notebook Python interpreter is unavailable', code='operation_rejected')
        self.instances = {}
        self.roots = {}
        self.lock = threading.RLock()
        self.closed = threading.Event()
        self.sockets = set()
        self.channels = threading.BoundedSemaphore(4)
        self.temp = tempfile.TemporaryDirectory(prefix='demiflow-jupyter-')
        self.monitor = threading.Thread(target=self._monitor, name='native-notebook-owner', daemon=True)
        self.monitor.start()

    def ceiling(self):
        return dict(analysis_execute=True, analysis_write=True, case_annotate=False)

    def assert_ready(self):
        if self.closed.is_set():
            raise ForgeError('Notebook service is stopping', code='busy')
        check = subprocess.run([str(self.python), '-I', '-c',
            'import notebook, jupyter_server, ipykernel; assert notebook.__version__.split(".")[0] == "7"'],
            stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, timeout=15)
        if check.returncode:
            raise ForgeError('Configured Python needs Notebook 7 and ipykernel', code='operation_rejected')

    def credentials(self, project, root):
        data = (project + '\0' + str(root)).encode()
        return hmac.new(self.server.token.encode(), b'jupyter\0' + data, hashlib.sha256).hexdigest()

    def authorize(self, project):
        _, root, permissions = self.server.resolve('/api/workspaces/' + project + '/context')
        return root, permissions

    def get(self, project, *, initialize=True):
        root, permissions = self.authorize(project)
        grants = dict(writable=permissions['analysis_write'], execute=permissions['analysis_execute'])
        with self.lock:
            if self.closed.is_set():
                raise ForgeError('Notebook service is stopping', code='busy')
            current = self.instances.get(project)
            if project in self.roots and self.roots[project] != root:
                raise ForgeError('Project root changed; original Notebook session was not rebound', code='conflict')
            if current and current.root == root and current.process.poll() is None:
                current.executable |= permissions['analysis_execute']
                if current.grants != grants:
                    self._grants(current.state, grants)
                    current.grants = grants
                return current
            if current:
                self._retire(current)
                del self.instances[project]
            if len(self.instances) >= 4:
                for key, instance in tuple(self.instances.items()):
                    if self._idle(instance):
                        self._retire(instance)
                        del self.instances[key]
                        break
                else:
                    raise ForgeError('已有四个活动 Notebook 工作区；请先保存并等待运行结束。', code='resource_limit')
            self.roots[project] = root
            directory = root / 'notebooks'
            no_symlinks(directory)
            if not directory.exists():
                if not permissions['analysis_write']:
                    raise ForgeError('此项目尚无 Notebook，且未开启 Notebook 编辑权限。', code='operation_rejected')
                directory.mkdir(mode=0o700)
            if not directory.is_dir():
                raise ForgeError('Project notebooks path is not a directory', code='conflict')
            self._import_legacy(project, directory, permissions)
            if permissions['analysis_write']:
                from .table_code import migrate_readers
                migrate_readers(root, directory, self.documents(directory))
            if (initialize and permissions['analysis_write'] and not self.documents(directory)
                    and not (directory/'.demiflow-trash').exists()):
                self.new_document(directory)
            state = Path(self.temp.name) / (project + '-' + secrets.token_hex(4))
            state.mkdir(mode=0o700, exist_ok=True)
            for name in ('runtime', 'config', 'data', 'ipython'):
                (state / name).mkdir(mode=0o700, exist_ok=True)
            self._grants(state, grants)
            token = secrets.token_urlsafe(32)
            with socket.socket() as listener:
                listener.bind(('127.0.0.1', 0))
                port = listener.getsockname()[1]
            base = '/jupyter/' + project + '/'
            code_root = Path(__file__).resolve().parents[3]
            config = dict(root=str(root), notebooks=str(directory), token=token, port=port,
                          base_url=base, code_root=str(code_root), writable=permissions['analysis_write'], parent_pid=os.getpid())
            config_path = state / 'server.json'
            self._create(config_path, config)
            preferences = state / 'settings/@jupyterlab/notebook-extension'
            preferences.mkdir(parents=True, mode=0o700, exist_ok=True)
            self._create(preferences/'tracker.jupyterlab-settings', {'autoStartDefaultKernel': permissions['analysis_execute']})
            doc_settings = state / 'settings/@jupyterlab/docmanager-extension'
            doc_settings.mkdir(parents=True, exist_ok=True)
            self._create(doc_settings/'plugin.jupyterlab-settings', {'autosave': False})
            # Native kernel imports the same reviewed platform and works at the
            # project root. This is a trusted Python environment, not a sandbox.
            kernelspec = state / 'data/kernels/python3'
            kernelspec.mkdir(parents=True, mode=0o700, exist_ok=True)
            self._create(kernelspec / 'kernel.json', dict(argv=[str(self.python), '-m', 'ipykernel_launcher',
                '-f', '{connection_file}'], display_name='Python 3', language='python',
                env={'PYTHONPATH': str(code_root), 'DEMIFLOW_NOTEBOOK_ROOT': str(root)}))
            env = dict(os.environ, JUPYTER_RUNTIME_DIR=str(state/'runtime'),
                JUPYTER_CONFIG_DIR=str(state/'config'), JUPYTER_DATA_DIR=str(state/'data'),
                IPYTHONDIR=str(state/'ipython'), PYTHONPATH=str(code_root),
                JUPYTER_PLATFORM_DIRS='1')
            with (state/'server.log').open('ab') as log:
                process = subprocess.Popen([str(self.python), '-I', str(Path(__file__).with_name('server.py')),
                    str(config_path)], cwd=code_root, env=env, stdin=subprocess.DEVNULL,
                    stdout=log, stderr=log, start_new_session=True)
            instance = Instance(project, root, port, token, base, process, state, directory,
                                permissions['analysis_execute'], grants)
            self.instances[project] = instance
            try:
                deadline = time.monotonic() + 40
                while time.monotonic() < deadline:
                    if self.closed.is_set() or process.poll() is not None:
                        raise ForgeError('Jupyter did not start; check the configured Notebook environment', code='operation_failed')
                    connection = http.client.HTTPConnection('127.0.0.1', port, timeout=.5)
                    try:
                        connection.request('GET', base+'api', headers={'Authorization': 'token '+token})
                        response = connection.getresponse()
                        if response.status == 200:
                            return instance
                    except OSError:
                        pass
                    finally:
                        connection.close()
                    self.closed.wait(.1)
                raise ForgeError('Jupyter startup timed out', code='operation_failed')
            except BaseException:
                self._retire(instance, failed=True)
                del self.instances[project]
                raise

    @staticmethod
    def documents(directory):
        names = []
        with os.scandir(directory) as entries:
            for index, entry in enumerate(entries):
                if index >= 512:
                    raise ForgeError('Notebook directory exceeds 512 entries', code='resource_limit')
                if entry.name.endswith('.ipynb') and not entry.name.startswith('.') and entry.is_file(follow_symlinks=False):
                    names.append(entry.name)
        return sorted(names, key=lambda name: (name.removesuffix('.ipynb').casefold(), name))

    def new_document(self, directory):
        initial = dict(nbformat=4, nbformat_minor=5, metadata={
            'kernelspec': {'display_name': 'Python 3', 'language': 'python', 'name': 'python3'}},
            cells=[dict(id=secrets.token_hex(8), cell_type='code', metadata={},
                        execution_count=None, outputs=[], source='')])
        with os.scandir(directory) as entries:
            if sum(1 for _ in islice(entries, 512)) >= 512:
                raise ForgeError('Notebook directory is full', code='resource_limit')
        for number in range(1, 513):
            name = '分析.ipynb' if number == 1 else f'分析 {number}.ipynb'
            if self._create(directory / name, initial):
                return name
        raise ForgeError('Notebook directory is full', code='resource_limit')

    @staticmethod
    def _grants(state, grants):
        temporary = state / 'grants.tmp'
        temporary.write_text(json.dumps(grants))
        temporary.chmod(0o600)
        temporary.replace(state / 'grants.json')

    @staticmethod
    def _create(path, value):
        try:
            with path.open('x', encoding='utf-8') as stream:
                path.chmod(0o600)
                json.dump(value, stream, ensure_ascii=False)
            return True
        except FileExistsError:
            return False

    def _import_legacy(self, project, directory, permissions):
        service = self.server.analysis
        if service is None or not permissions['analysis_write']:
            return
        from demiflow.workbench.analysis.contracts import export_notebook
        # Historical authority stays in its original store. Copy source once;
        # never overwrite a native file, mark old outputs as newly executed, or
        # mutate a legacy document/revision while opening the native UI.
        documents = service.call(project, 'documents', {'kind': 'notebook', 'limit': 32})
        for item in documents['items']:
            destination = directory / ('历史-' + item['id'] + '.ipynb')
            if destination.exists() or (directory/'.demiflow-trash'/destination.name).is_dir():
                continue
            cells, offset, title = [], 0, item.get('title') or 'Notebook'
            while True:
                page = service.call(project, 'notebook', dict(notebook_id=item['id'], revision=item['revision'],
                                                             offset=offset, limit=16))
                cells.extend(page['document']['cells'])
                title = page['document']['title']
                previous = offset
                offset = page['next_offset']
                if len(cells) > 64 or offset is not None and (offset <= previous or offset >= 64):
                    raise ForgeError('Legacy Notebook exceeds its 64-cell import budget', code='resource_limit')
                if offset is None:
                    break
            notebook = export_notebook(dict(title=title, cells=cells))
            for cell, original in zip(notebook['cells'], cells):
                if original['language'] == 'sql':
                    cell.update(cell_type='raw', source=original['source'])
                    cell.pop('execution_count', None); cell.pop('outputs', None)
            notebook['metadata']['demiflow']['legacy'] = dict(id=item['id'], revision=item['revision'])
            self._create(destination, notebook)

    @staticmethod
    def _idle(instance):
        if instance.channels or instance.requests:
            return False
        if instance.process.poll() is not None:
            return True
        connection = http.client.HTTPConnection('127.0.0.1', instance.port, timeout=2)
        try:
            connection.request('GET', instance.base_url+'api/kernels', headers={'Authorization': 'token '+instance.token})
            response = connection.getresponse()
            data = response.read(64*1024+1)
            return response.status == 200 and len(data) <= 64*1024 and all(
                kernel['execution_state'] == 'idle' for kernel in json.loads(data))
        except (OSError, ValueError, KeyError):
            return False
        finally:
            connection.close()

    def _retire(self, instance, *, failed=False):
        self._stop(instance)
        if failed and (instance.state/'server.log').is_file():
            # One bounded private diagnostic; do not accumulate launch dirs.
            with (instance.state/'server.log').open('rb') as stream:
                stream.seek(max(0, os.fstat(stream.fileno()).st_size-64*1024))
                last = stream.read(64*1024).replace(instance.token.encode(), b'REDACTED')
            target = Path(self.temp.name)/'last-startup-failure.log'
            target.write_bytes(last); target.chmod(0o600)
        shutil.rmtree(instance.state)

    def _stop(self, instance):
        if instance.process.poll() is not None:
            return
        # SIGTERM is Jupyter's normal shutdown and closes its kernel managers.
        import psutil
        try:
            descendants = psutil.Process(instance.process.pid).children(recursive=True)
            instance.process.terminate()
        except (psutil.NoSuchProcess, ProcessLookupError):
            return
        try:
            instance.process.wait(timeout=15)
        except subprocess.TimeoutExpired:
            os.killpg(instance.process.pid, signal.SIGKILL)
            instance.process.wait(timeout=5)
        # Kernel provisioners can use their own process groups. Account for
        # only the descendants observed under this original server identity.
        remaining = []
        for child in descendants:
            try:
                if child.is_running() and child.status() != psutil.STATUS_ZOMBIE:
                    remaining.append(child)
            except psutil.NoSuchProcess:
                pass
        for child in remaining:
            try: child.terminate()
            except psutil.NoSuchProcess: pass
        _, remaining = psutil.wait_procs(remaining, timeout=3)
        for child in remaining:
            try: child.kill()
            except psutil.NoSuchProcess: pass
        psutil.wait_procs(remaining, timeout=3)

    def _monitor(self):
        while not self.closed.wait(2):
            if not self.lock.acquire(blocking=False):
                continue
            try:
                for project, instance in tuple(self.instances.items()):
                    try:
                        root, permissions = self.authorize(project)
                        revoked = root != instance.root or instance.executable and not permissions['analysis_execute']
                    except (ForgeError, PermissionError, FileNotFoundError):
                        revoked = True
                    if revoked:
                        self._stop(instance)
                        # Retain the root binding until the workbench restarts;
                        # an old document must not autosave into a different root.
            finally:
                self.lock.release()

    def close(self):
        self.closed.set()
        self.monitor.join(timeout=20)
        with self.lock:
            for connection in tuple(self.sockets):
                try: connection.shutdown(socket.SHUT_RDWR)
                except OSError: pass
            for instance in self.instances.values():
                self._stop(instance)
            self.instances.clear()
            self.temp.cleanup()
