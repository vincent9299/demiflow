"""Deferred lifecycle for arbitrary local HTTP services (no shell/model policy).

Commands, GPU selection and offload/memory flags are caller declarations. A node
owns its process group; the CLI supervisor can instead keep it across runs.
"""
from __future__ import annotations

import inspect
import json
from demiflow.execution.contracts.dataflow_errors import DataflowError
from demiflow.execution.contracts.recovery_contract import document
import asyncio
from contextlib import ExitStack, suppress, asynccontextmanager
import fcntl
import math
import logging
import os
from pathlib import Path
import signal
import socket
import subprocess
import time
from urllib.parse import urlsplit
from .contracts import ModelServiceError, served_model_names, bounded_metadata_get

logger = logging.getLogger(__name__)


def _endpoint(value):
    url = urlsplit(value)
    if (url.scheme != 'http' or url.hostname not in {'127.0.0.1', 'localhost'}
            or url.port is None or url.username or url.password or url.query or url.fragment):
        raise ValueError('Managed HTTP service requires a loopback HTTP URL with an explicit port')
    return url


class ManagedHTTPService:
    """Pure declaration, usable by prompt nodes or custom async actors.

    ``command`` is argv, never shell text. GPU/memory/offload policy stays in
    ``gpus``, command flags and explicit ``env``; no weights are downloaded.
    ``ready_path`` is checked without proxies; ``expected_model`` optionally
    verifies /v1/models too. Default gpus=() explicitly selects CPU-only execution.
    """
    def __init__(self, command, *, base_url, root=None, gpus=(), env=None,
                 ready_path='/health', expected_model=None, log_path=None,
                 startup_timeout_s=600, shutdown_timeout_s=30,
                 readiness_timeout_s=2, poll_interval_s=0.5,
                 resource_wait_timeout_s=0, startup_log_interval_s=15,
                 model_match='listed', required_paths=(), log=None,
                 resource_directory=None, request_concurrency=None, log_max_bytes=8*1024*1024):
        if (not isinstance(command, (list, tuple)) or not command
                or any(not isinstance(part, str) or not part for part in command)):
            raise ValueError('command must be a nonempty argv sequence')
        _endpoint(base_url)
        if (not isinstance(ready_path, str) or not ready_path.startswith('/')
                or ready_path.startswith('//') or '#' in ready_path):
            raise ValueError('ready_path must be an origin-relative path')
        if (not isinstance(gpus, (tuple, list)) or len(set(gpus)) != len(gpus)
                or any(type(g) is not int or g < 0 for g in gpus)):
            raise ValueError('gpus must contain unique nonnegative indices')
        if any(not isinstance(k,str) or not isinstance(v,str) or not k or '=' in k or '\0' in k+v
               for k,v in (env or {}).items()):
            raise ValueError('env must contain string assignments')
        if 'CUDA_VISIBLE_DEVICES' in (env or {}):
            raise ValueError('Declare GPU placement using gpus, not env')
        for value in (startup_timeout_s,shutdown_timeout_s,readiness_timeout_s,poll_interval_s):
            if type(value) not in (int,float) or not math.isfinite(value) or value<=0:
                raise ValueError('service timeouts must be positive and finite')
        if (type(resource_wait_timeout_s) not in (int, float)
                or not math.isfinite(resource_wait_timeout_s) or resource_wait_timeout_s < 0):
            raise ValueError('resource_wait_timeout_s must be finite and nonnegative')
        if (type(startup_log_interval_s) not in (int, float)
                or not math.isfinite(startup_log_interval_s) or startup_log_interval_s <= 0):
            raise ValueError('startup_log_interval_s must be positive and finite')
        if model_match not in {'listed', 'exact'}:
            raise ValueError('model_match must be listed or exact')
        if (not isinstance(required_paths, (list, tuple))
                or any(not isinstance(p, (str, Path)) or not str(p) for p in required_paths)):
            raise ValueError('required_paths must contain nonempty local paths')
        if log is not None and not callable(log):
            raise TypeError('log must be callable')
        if request_concurrency is not None and (type(request_concurrency) is not int or not 1<=request_concurrency<=512):
            raise ValueError('request_concurrency must be 1..512')
        self.command, self.base_url = tuple(command), base_url.rstrip('/')
        self.root = Path(root or Path.cwd()).resolve()
        self.gpus,self.env = tuple(gpus),dict(env or {})
        self.ready_path,self.expected_model = ready_path,expected_model
        self.log_path = (self.root/ log_path if log_path else
                         self.root/'_demiflow/services'/f'http_{_endpoint(base_url).port}.log')
        self.startup_timeout_s,self.shutdown_timeout_s = startup_timeout_s,shutdown_timeout_s
        self.readiness_timeout_s,self.poll_interval_s = readiness_timeout_s,poll_interval_s
        self.resource_wait_timeout_s = resource_wait_timeout_s
        self.startup_log_interval_s = startup_log_interval_s
        self.model_match, self.log = model_match, log
        self.required_paths = tuple(self.root / p for p in required_paths)
        self.resource_directory = (Path(resource_directory).resolve() if resource_directory else
                                   self.root/'_demiflow/local_model_gpus')
        self.request_concurrency = request_concurrency
        if type(log_max_bytes) is not int or not 1024<=log_max_bytes<=1024**3:
            raise ValueError('log_max_bytes must be 1 KiB..1 GiB')
        self.log_max_bytes=log_max_bytes

    def bind(self, model=None):
        if model is not None:
            if model.transport not in {'openai_compatible', 'systemone'}:
                raise ValueError('ManagedHTTPService requires openai_compatible or systemone transport')
            endpoint = model.base_url or os.environ.get(model.base_url_env, '')
            if endpoint.rstrip('/') != self.base_url:
                raise ValueError('Prompt endpoint differs from managed service declaration')
            if self.expected_model is not None and self.expected_model != model.name:
                raise ValueError('Prompt model differs from managed service declaration')
        return _HTTPOwner(self)


# Keep the original methods available to the worker-declaration validator. An
# overridden instance/class method must not silently become a native service.
_NATIVE_MANAGED_HTTP_METHODS = (ManagedHTTPService.__init__, ManagedHTTPService.bind)


class _HTTPOwner:
    def __init__(self, declaration):
        self.declaration = declaration
        self._stack,self._process,self._startup = ExitStack(),None,None
        self._closed = False
        self._resource_fds = ()
        self._log_pump = None
        self._slots = (asyncio.Semaphore(declaration.request_concurrency)
                       if declaration.request_concurrency is not None else None)

    @property
    def base_url(self):
        return self.declaration.base_url

    def _report(self, message):
        try:
            (self.declaration.log or logger.info)(message)
        except Exception:
            logger.warning('Service logging callback failed', exc_info=True)

    @asynccontextmanager
    async def request_slot(self):
        if self._closed: raise ModelServiceError('Service owner has already closed')
        if self._slots is None:
            yield
        else:
            async with asyncio.timeout(self.declaration.startup_timeout_s):
                await self._slots.acquire()
            try:
                if self._closed: raise ModelServiceError('Service owner has already closed')
                yield
            finally: self._slots.release()

    async def _acquire_resources(self, port):
        spec = self.declaration
        control = spec.resource_directory
        control.mkdir(parents=True, exist_ok=True)
        # One host/user coordination domain across roots, plus the declared
        # legacy lock directory for callers already sharing that root.
        shared = Path(os.environ.get('DEMIFLOW_RESOURCE_HOME',str(Path.home()/'.cache/demiflow/resources')))/socket.gethostname()
        shared.mkdir(parents=True,exist_ok=True)
        controls = sorted({control.resolve(),shared.resolve()},key=str)
        started = time.monotonic()
        last_log = None
        resources = [*(f'gpu_{g}' for g in sorted(spec.gpus)), f'port_{port}']
        while True:
            blocked = None
            acquired_fds = []
            with ExitStack() as attempt:
                for directory in controls:
                    for resource in resources:
                        stream = attempt.enter_context((directory/(resource+'.lock')).open('a'))
                        try:
                            fcntl.flock(stream, fcntl.LOCK_EX | fcntl.LOCK_NB)
                            acquired_fds.append(stream.fileno())
                        except BlockingIOError:
                            blocked = 'Model resource ' + resource + ' is occupied'
                            break
                    if blocked: break
                if blocked is None:
                    with socket.socket() as probe:
                        if probe.connect_ex(('127.0.0.1', port)) == 0:
                            blocked = 'HTTP port is already occupied; refusing to attach or terminate its owner'
                if blocked is None:
                    self._stack.enter_context(attempt.pop_all())
                    self._resource_fds = tuple(acquired_fds)
                    return
            elapsed = time.monotonic() - started
            remaining = spec.resource_wait_timeout_s - elapsed
            if remaining <= 0:
                suffix = '; resource wait timed out' if spec.resource_wait_timeout_s else ''
                raise ModelServiceError(blocked + suffix)
            if last_log is None or elapsed-last_log >= spec.startup_log_interval_s:
                self._report(f'Waiting for model resources: {blocked}; waited {elapsed:.1f}s')
                last_log = elapsed
            await asyncio.sleep(min(spec.poll_interval_s, remaining))

    async def __aenter__(self):
        try:
            await self.ensure_ready()
        except BaseException:
            # __aexit__ is not invoked when entering the scope is cancelled.
            await self.aclose()
            raise
        return self

    async def __aexit__(self,*args):
        await self.aclose()

    async def ensure_ready(self):
        if self._closed:
            raise ModelServiceError('Service owner has already closed')
        if self._startup is None:
            self._startup = asyncio.create_task(self._start())
        await asyncio.shield(self._startup)
        if self._log_pump is not None and self._log_pump.error is not None:
            raise ModelServiceError('Service log failed: '+str(self._log_pump.error))
        if self._process.poll() is not None:
            raise ModelServiceError(f'Managed HTTP service exited ({self._process.returncode})')

    async def _start(self):
        import httpx
        spec = self.declaration
        url = _endpoint(spec.base_url)
        origin = f'{url.scheme}://{url.netloc}'
        try:
            for path in spec.required_paths:
                if not path.exists():
                    raise FileNotFoundError(path)
            await self._acquire_resources(url.port)
            spec.log_path.parent.mkdir(parents=True,exist_ok=True)
            env = {**os.environ,**spec.env,'CUDA_VISIBLE_DEVICES':','.join(map(str,spec.gpus)), 'PYTHONUNBUFFERED':'1'}
            self._process = self._spawn_process(env)
            from .process_log import ProcessLog
            self._log_pump=ProcessLog(self._process.stdout,spec.log_path,spec.log_max_bytes)
            deadline = time.monotonic()+spec.startup_timeout_s
            last_log = time.monotonic()
            async with httpx.AsyncClient(trust_env=False,timeout=spec.readiness_timeout_s) as client:
                while time.monotonic()<deadline:
                    if self._process.poll() is not None:
                        raise ModelServiceError(f'HTTP service exited ({self._process.returncode}); see {spec.log_path}')
                    try:
                        response = await asyncio.wait_for(bounded_metadata_get(client,origin+spec.ready_path), max(0.01,deadline-time.monotonic()))
                        response.raise_for_status()
                        if spec.expected_model is not None:
                            response = await asyncio.wait_for(bounded_metadata_get(client,spec.base_url+'/models'),max(0.01,deadline-time.monotonic()))
                            response.raise_for_status()
                            names = served_model_names(response.json())
                            matched = (names == [spec.expected_model] if spec.model_match == 'exact'
                                       else spec.expected_model in names)
                            if not matched:
                                raise ModelServiceError('Managed endpoint does not serve the declared model')
                        self._report(f'Model ready: {spec.expected_model or spec.base_url}, PID={self._process.pid}')
                        return
                    except (httpx.HTTPError, asyncio.TimeoutError):
                        if time.monotonic()-last_log >= spec.startup_log_interval_s:
                            self._report(f'Model still loading: {spec.expected_model or spec.base_url}; log={spec.log_path}')
                            last_log = time.monotonic()
                        await asyncio.sleep(min(spec.poll_interval_s,max(0,deadline-time.monotonic())))
            raise ModelServiceError('HTTP service readiness timed out; see '+str(spec.log_path))
        except BaseException as error:
            await self._release()
            if isinstance(error,(asyncio.CancelledError,ModelServiceError)):
                raise
            raise ModelServiceError(str(error)) from error

    def _spawn_process(self, env):
        return subprocess.Popen(self.declaration.command,cwd=self.declaration.root,env=env,
            stdin=subprocess.DEVNULL,stdout=subprocess.PIPE,stderr=subprocess.STDOUT,start_new_session=True)

    async def aclose(self):
        if self._closed:
            return
        self._closed = True
        if self._startup is not None:
            if not self._startup.done():
                self._startup.cancel()
            with suppress(asyncio.CancelledError,ModelServiceError):
                await self._startup
        await self._release()

    async def _release(self):
        process,self._process = self._process,None
        try:
            if process is not None:
                with suppress(ProcessLookupError):
                    os.killpg(process.pid,signal.SIGTERM)
                deadline=time.monotonic()+self.declaration.shutdown_timeout_s
                while time.monotonic()<deadline:
                    process.poll()
                    try:os.killpg(process.pid,0)
                    except ProcessLookupError:break
                    await asyncio.sleep(min(.05,max(0,deadline-time.monotonic())))
        finally:
            try:
                if process is not None:
                    with suppress(ProcessLookupError):
                        os.killpg(process.pid,signal.SIGKILL)
                    process.wait(timeout=self.declaration.shutdown_timeout_s)
            finally:
                if self._log_pump is not None:
                    pump,self._log_pump=self._log_pump,None
                    await asyncio.to_thread(pump.close)
                self._stack.close()


_SCHEMA = 'demiflow.managed_http_declaration.v1'

_MAX_BYTES = 65536

_PATHS = frozenset({'root', 'log_path', 'resource_directory'})

_FIELDS = frozenset({'command', 'base_url', 'root', 'gpus', 'env', 'ready_path',
    'expected_model', 'log_path', 'startup_timeout_s', 'shutdown_timeout_s',
    'readiness_timeout_s', 'poll_interval_s', 'resource_wait_timeout_s',
    'startup_log_interval_s', 'model_match', 'required_paths', 'log',
    'resource_directory', 'request_concurrency', 'log_max_bytes'})

def _invalid(reason):
    return DataflowError('INVALID_CONTRACT', reason)

def freeze_managed_http(service):

    if type(service) is not ManagedHTTPService:
        raise DataflowError('UNSUPPORTED_CAPABILITY', 'Embedding service requires a passive native ManagedHTTPService')
    if (inspect.getattr_static(ManagedHTTPService, '__init__'),
            inspect.getattr_static(ManagedHTTPService, 'bind')) != _NATIVE_MANAGED_HTTP_METHODS:
        raise _invalid('Managed HTTP service methods differ from the native declaration')
    values = vars(service)
    if set(values) != _FIELDS:
        raise _invalid('Managed HTTP service contains modified methods or unknown live state')
    if values['log'] is not None:
        raise DataflowError('UNSUPPORTED_CAPABILITY', 'Managed embedding service log callbacks require a code-bound declaration')
    parameters = {}
    for name, value in values.items():
        if name in _PATHS:
            if type(value) is not type(Path()):
                raise _invalid('Managed HTTP paths must be passive native paths')
            value = str(value)
        elif name == 'required_paths':
            if type(value) is not tuple or len(value) > 256 or any(type(p) is not type(Path()) for p in value):
                raise _invalid('Managed HTTP required paths must be bounded native paths')
            value = [str(p) for p in value]
        parameters[name] = value
    # Validate structural/byte limits before allocating the normalized copy.
    encoded = document({'schema': _SCHEMA, 'parameters': parameters}, _MAX_BYTES)
    return json.loads(encoded)

def restore_managed_http(value):

    if (type(value) is not dict or set(value) != {'schema', 'parameters'}
            or value['schema'] != _SCHEMA or type(value['parameters']) is not dict
            or set(value['parameters']) != _FIELDS):
        raise _invalid('Invalid sealed managed HTTP declaration')
    document(value, _MAX_BYTES)
    service = ManagedHTTPService(**value['parameters'])
    if freeze_managed_http(service) != value:
        raise _invalid('Managed HTTP declaration was not canonically sealed')
    return service

def managed_http_digest(service):
    import hashlib
    return hashlib.sha256(document(freeze_managed_http(service), _MAX_BYTES)).hexdigest()
