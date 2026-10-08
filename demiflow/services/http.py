"""Deferred lifecycle for arbitrary local HTTP services (no shell/model policy).

Commands, GPU selection and offload/memory flags are caller declarations. A node
owns its process group; the CLI supervisor can instead keep it across runs.
"""
from __future__ import annotations
import asyncio
from contextlib import ExitStack, suppress
import fcntl
import math
import os
from pathlib import Path
import signal
import socket
import subprocess
import time
from urllib.parse import urlsplit
from .vllm import ModelServiceError


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
                 readiness_timeout_s=2, poll_interval_s=0.5):
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
        self.command, self.base_url = tuple(command), base_url.rstrip('/')
        self.root = Path(root or Path.cwd()).resolve()
        self.gpus,self.env = tuple(gpus),dict(env or {})
        self.ready_path,self.expected_model = ready_path,expected_model
        self.log_path = (self.root/ log_path if log_path else
                         self.root/'_demiflow/services'/f'http_{_endpoint(base_url).port}.log')
        self.startup_timeout_s,self.shutdown_timeout_s = startup_timeout_s,shutdown_timeout_s
        self.readiness_timeout_s,self.poll_interval_s = readiness_timeout_s,poll_interval_s

    def bind(self, model=None):
        if model is not None:
            if model.transport != 'openai_compatible':
                raise ValueError('ManagedHTTPService prompt binding requires openai_compatible transport')
            endpoint = model.base_url or os.environ.get(model.base_url_env, '')
            if endpoint.rstrip('/') != self.base_url:
                raise ValueError('Prompt endpoint differs from managed service declaration')
            if self.expected_model is not None and self.expected_model != model.name:
                raise ValueError('Prompt model differs from managed service declaration')
        return _HTTPOwner(self)


class _HTTPOwner:
    def __init__(self, declaration):
        self.declaration = declaration
        self._stack,self._process,self._startup = ExitStack(),None,None
        self._closed = False

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
        if self._process.poll() is not None:
            raise ModelServiceError(f'Managed HTTP service exited ({self._process.returncode})')

    async def _start(self):
        import httpx
        spec = self.declaration
        url = _endpoint(spec.base_url)
        origin = f'{url.scheme}://{url.netloc}'
        try:
            control = spec.root/'_demiflow/local_model_gpus'
            control.mkdir(parents=True,exist_ok=True)
            for resource in [*(f'gpu_{g}' for g in sorted(spec.gpus)), f'port_{url.port}']:
                stream = self._stack.enter_context((control/(resource+'.lock')).open('a'))
                try:
                    fcntl.flock(stream,fcntl.LOCK_EX|fcntl.LOCK_NB)
                except BlockingIOError as error:
                    raise ModelServiceError('Model resource is occupied: '+resource) from error
            with socket.socket() as probe:
                if probe.connect_ex(('127.0.0.1',url.port))==0:
                    raise ModelServiceError('HTTP port is already occupied; refusing to attach or terminate its owner')
            spec.log_path.parent.mkdir(parents=True,exist_ok=True)
            output = self._stack.enter_context(spec.log_path.open('ab'))
            env = {**os.environ,**spec.env,'CUDA_VISIBLE_DEVICES':','.join(map(str,spec.gpus)), 'PYTHONUNBUFFERED':'1'}
            self._process = subprocess.Popen(spec.command,cwd=spec.root,env=env,stdin=subprocess.DEVNULL,
                stdout=output,stderr=subprocess.STDOUT,start_new_session=True)
            deadline = time.monotonic()+spec.startup_timeout_s
            async with httpx.AsyncClient(trust_env=False,timeout=spec.readiness_timeout_s) as client:
                while time.monotonic()<deadline:
                    if self._process.poll() is not None:
                        raise ModelServiceError(f'HTTP service exited ({self._process.returncode}); see {spec.log_path}')
                    try:
                        response = await asyncio.wait_for(client.get(origin+spec.ready_path), max(0.01,deadline-time.monotonic()))
                        response.raise_for_status()
                        if spec.expected_model is not None:
                            response = await asyncio.wait_for(client.get(spec.base_url+'/models'),max(0.01,deadline-time.monotonic()))
                            response.raise_for_status()
                            if spec.expected_model not in [row['id'] for row in response.json()['data']]:
                                raise ModelServiceError('Managed endpoint does not serve the declared model')
                        return
                    except (httpx.HTTPError, asyncio.TimeoutError):
                        await asyncio.sleep(min(spec.poll_interval_s,max(0,deadline-time.monotonic())))
            raise ModelServiceError('HTTP service readiness timed out; see '+str(spec.log_path))
        except BaseException as error:
            await self._release()
            if isinstance(error,(asyncio.CancelledError,ModelServiceError)):
                raise
            raise ModelServiceError(str(error)) from error

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
                self._stack.close()
