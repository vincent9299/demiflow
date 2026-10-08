"""Declarative local vLLM resources for native prompt and embedding actors.

Configuration has no process or filesystem side effects. At execution the actor
binds the prompt's existing model endpoint, starts on its first cache miss and
closes this run's process group through its normal aclose hook. No business rows,
request journals, model downloads or stage selection live here.
"""
from __future__ import annotations

import asyncio
from collections.abc import Mapping
from contextlib import ExitStack, suppress
import logging
import json
import math
import os
from pathlib import Path
import signal
import socket
import subprocess
import sys
import time
from urllib.parse import urlparse

logger = logging.getLogger(__name__)


class ModelServiceError(RuntimeError):
    """Node resource failure; must stop the stream rather than fail every row."""


def vllm_config(value):
    """补齐服务默认值并校验；None 使用外部服务，本函数不加载模型。"""
    if value is None:
        return None
    defaults = dict(python=sys.executable, cuda_bin='/usr/local/cuda/bin', gpus=[0],
                    data_parallel_size=1, tensor_parallel_size=1,
                    gpu_memory_utilization=0.9, max_model_len=8192,
                    max_num_seqs=16, max_num_batched_tokens=16384,
                    api_server_count=1, enforce_eager=False, enable_logging_iteration_details=False,
                    startup_timeout_s=600, shutdown_timeout_s=30, readiness_timeout_s=2,
                    resource_wait_timeout_s=0,
                    poll_interval_s=1, startup_log_interval_s=15, limit_mm_per_prompt=None,
                    runner=None, chat_template=None, dtype=None, trust_remote_code=False,
                    mm_processor_cache_gb=None, enable_prefix_caching=None,
                    language_model_only=False, omp_num_threads=None)
    if not isinstance(value, Mapping) or set(value) - (set(defaults) | {'model_path'}):
        raise ValueError('model service requires known vLLM configuration fields')
    result = {**defaults, **value}
    for key in ('model_path', 'python', 'cuda_bin'):
        if not isinstance(result.get(key), (str, Path)) or not str(result[key]).strip():
            raise ValueError('model service requires ' + key)
        result[key] = str(result[key])
    gpus = result['gpus']
    if (not isinstance(gpus, (list, tuple)) or not gpus
            or any(type(g) is not int or g < 0 for g in gpus) or len(set(gpus)) != len(gpus)):
        raise ValueError('model service gpus must contain unique nonnegative GPU indices')
    result['gpus'] = list(gpus)
    if result['runner'] not in (None, 'auto', 'generate', 'pooling'):
        raise ValueError('runner must be auto, generate or pooling')
    if result['dtype'] not in (None, 'auto', 'half', 'float16', 'bfloat16', 'float', 'float32'):
        raise ValueError('Unsupported vLLM dtype')
    if type(result['trust_remote_code']) is not bool:
        raise ValueError('trust_remote_code must be boolean')
    if type(result['language_model_only']) is not bool:
        raise ValueError('language_model_only must be boolean')
    if result['omp_num_threads'] is not None and (type(result['omp_num_threads']) is not int
                                                or result['omp_num_threads'] < 1):
        raise ValueError('omp_num_threads must be a positive integer or None')
    cache = result['mm_processor_cache_gb']
    if cache is not None and (type(cache) not in (int, float) or not math.isfinite(cache) or cache < 0):
        raise ValueError('mm_processor_cache_gb must be finite and nonnegative or None')
    if result['enable_prefix_caching'] is not None and type(result['enable_prefix_caching']) is not bool:
        raise ValueError('enable_prefix_caching must be boolean or None')
    if result['chat_template'] is not None:
        if not isinstance(result['chat_template'], (str, Path)) or not str(result['chat_template']).strip():
            raise ValueError('chat_template must be a local template path')
        result['chat_template'] = str(result['chat_template'])
    limits = result['limit_mm_per_prompt']
    if limits is not None:
        if (not isinstance(limits, Mapping) or not limits
                or any(not isinstance(k, str) or not k or type(v) is not int or v < 0
                       for k, v in limits.items())):
            raise ValueError('limit_mm_per_prompt requires modality names and nonnegative integer limits')
        result['limit_mm_per_prompt'] = dict(limits)
    for key in ('data_parallel_size', 'tensor_parallel_size', 'max_model_len', 'max_num_seqs',
                'max_num_batched_tokens', 'api_server_count'):
        if type(result[key]) is not int or result[key] < 1:
            raise ValueError('model service requires positive integer ' + key)
    if result['data_parallel_size'] * result['tensor_parallel_size'] != len(gpus):
        raise ValueError('DP × TP must equal the configured GPU count')
    wait = result['resource_wait_timeout_s']
    if type(wait) not in (int, float) or not math.isfinite(wait) or wait < 0:
        raise ValueError('resource_wait_timeout_s must be finite and nonnegative')
    for key in ('gpu_memory_utilization', 'startup_timeout_s', 'shutdown_timeout_s', 'readiness_timeout_s',
                'poll_interval_s', 'startup_log_interval_s'):
        if type(result[key]) not in (int, float) or not math.isfinite(result[key]) or result[key] <= 0:
            raise ValueError('model service requires positive finite ' + key)
    if result['gpu_memory_utilization'] >= 1 or type(result['enforce_eager']) is not bool:
        raise ValueError('invalid model service memory fraction or enforce_eager')
    if type(result['enable_logging_iteration_details']) is not bool:
        raise ValueError('enable_logging_iteration_details must be boolean')
    return result


def _service_command(spec, model, root):
    """配置的权重/GPU/批量预算 → 无 shell 的 vLLM argv 与子进程环境。"""
    url = urlparse(model['base_url'])
    if (url.scheme != 'http' or url.hostname not in {'127.0.0.1', 'localhost'}
            or url.port is None or not 1 <= url.port <= 65535 or url.path.rstrip('/') != '/v1'
            or url.username or url.password or url.query or url.fragment):
        raise ValueError('managed model service requires a direct local endpoint with an explicit port and /v1 path')
    argv = [spec['python'], '-m', 'vllm.entrypoints.cli.main', 'serve',
            str(root / spec['model_path']), '--served-model-name', model['name'],
            '--host', '127.0.0.1', '--port', str(url.port)]
    for key in ('data_parallel_size', 'tensor_parallel_size', 'gpu_memory_utilization',
                'max_model_len', 'max_num_seqs', 'max_num_batched_tokens', 'api_server_count'):
        argv += ['--' + key.replace('_', '-'), str(spec[key])]
    if spec['enforce_eager']:
        argv.append('--enforce-eager')
    if spec['enable_logging_iteration_details']:
        argv.append('--enable-logging-iteration-details')
    if spec['mm_processor_cache_gb'] is not None:
        argv += ['--mm-processor-cache-gb', str(spec['mm_processor_cache_gb'])]
    if spec['enable_prefix_caching'] is not None:
        argv.append('--enable-prefix-caching' if spec['enable_prefix_caching'] else '--no-enable-prefix-caching')
    for key in ('runner', 'dtype'):
        if spec[key] is not None:
            argv += ['--' + key, spec[key]]
    if spec['chat_template'] is not None:
        argv += ['--chat-template', str(root / spec['chat_template'])]
    if spec['trust_remote_code']:
        argv.append('--trust-remote-code')
    if spec['language_model_only']:
        argv.append('--language-model-only')
    if spec['limit_mm_per_prompt'] is not None:
        argv += ['--limit-mm-per-prompt', json.dumps(spec['limit_mm_per_prompt'], sort_keys=True)]
    env = os.environ.copy()
    env['CUDA_VISIBLE_DEVICES'] = ','.join(map(str, spec['gpus']))
    env['PATH'] = os.pathsep.join([str(Path(spec['python']).parent), spec['cuda_bin'], env.get('PATH', '')])
    env['PYTHONUNBUFFERED'] = '1'
    if spec['omp_num_threads'] is not None:
        env['OMP_NUM_THREADS'] = str(spec['omp_num_threads'])
    return argv, env


class VLLMService:
    """A deferred service declaration for map_prompt_async / map_embeddings.

    ``config`` is copied and normalized by :func:`vllm_config`. ``root`` resolves
    local paths and the cooperative GPU/port lock directory. Use the same root
    for jobs sharing GPUs. ``log_path`` receives server stdout/stderr; ``log`` is
    an optional progress callback (default: Python logging). No model name or URL
    is duplicated here: both come from this node's model declaration.

    This is node-owned, not a persistent/shared server registry. None on the
    operator means an externally managed endpoint. vLLM is a separate executable
    dependency and is not imported or installed by demiflow.
    """

    def __init__(self, config, *, root=None, log_path=None, log=None):
        self._config = vllm_config(config)
        if self._config is None:
            raise ValueError('VLLMService requires configuration; use service=None for an external endpoint')
        self.root = Path(root or Path.cwd()).absolute()
        self.log_path = self.root / log_path if log_path is not None else None
        self.log = log

    def bind(self, model):
        """Bind a fresh execution owner; still no process or network side effects."""
        if model.transport != 'openai_compatible':
            raise ValueError('VLLMService requires openai_compatible transport')
        endpoint = model.base_url or os.environ.get(model.base_url_env, '')
        resolved = {'name': model.name, 'base_url': endpoint}
        argv, env = _service_command(self._config, resolved, self.root)
        return _VLLMOwner(self, resolved, argv, env)


class _VLLMOwner:
    """One actor execution's process, readiness task and cooperative locks."""

    def __init__(self, declaration, model, argv, env):
        self.spec = declaration._config
        self.root = declaration.root
        self.model, self.argv, self.env = model, argv, env
        port = urlparse(model['base_url']).port
        self.log_path = declaration.log_path or self.root / '_demiflow' / 'services' / f'vllm_{port}.log'
        self.log = declaration.log or logger.info
        self._stack = ExitStack()
        self._process = None
        self._startup = None

    def _report(self, message):
        # A failed user logging callback must never skip process cleanup.
        try:
            self.log(message)
        except Exception:
            logger.warning('Model service progress callback failed', exc_info=True)

    async def ensure_ready(self):
        # All workers await one startup task. A cancelled row cannot cancel
        # another worker's startup; actor aclose owns cancellation and cleanup.
        if self._startup is None:
            self._startup = asyncio.create_task(self._start())
        await asyncio.shield(self._startup)
        if self._process.poll() is not None:
            raise ModelServiceError(f'Model service exited ({self._process.returncode}); see {self.log_path}')

    async def _acquire_resources(self, control, names, port):
        """Optional bounded queue; release partial acquisitions before waiting.

        No child starts and no other owner is interrupted while queued. Waiting
        is cancellable by aclose and separate from model startup's timeout.
        Locks scale with the configured GPU count, not rows or wait duration.
        """
        import fcntl

        started = time.monotonic()
        last_log = None
        while True:
            blocked = None
            with ExitStack() as attempt:
                for name in names:
                    lock = attempt.enter_context((control / f'{name}.lock').open('a'))
                    try:
                        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
                    except BlockingIOError:
                        blocked = f'Model resource {name} is occupied under {control}'
                        break
                if blocked is None:
                    with socket.socket() as probe:
                        if probe.connect_ex(('127.0.0.1', port)) == 0:
                            blocked = f'Model port {port} is occupied; use its known external service or another port'
                if blocked is None:
                    self._stack.enter_context(attempt.pop_all())
                    return
            elapsed = time.monotonic() - started
            remaining = self.spec['resource_wait_timeout_s'] - elapsed
            if remaining <= 0:
                raise RuntimeError(blocked + (f'; resource wait timed out after {elapsed:.1f}s'
                                             if self.spec['resource_wait_timeout_s'] else ''))
            if last_log is None or elapsed - last_log >= self.spec['startup_log_interval_s']:
                self._report(f'等待模型资源：{blocked}；已等 {elapsed:.0f}s，剩余 {remaining:.0f}s')
                last_log = elapsed
            await asyncio.sleep(min(self.spec['poll_interval_s'], remaining))

    async def _start(self):
        import httpx

        try:
            weights = self.root / self.spec['model_path']
            if not weights.is_dir():
                raise FileNotFoundError(weights)
            port = urlparse(self.model['base_url']).port
            control = self.root / '_demiflow' / 'local_model_gpus'
            control.mkdir(parents=True, exist_ok=True)
            # Only coordinate owned launches; never attach to or terminate an
            # unknown listener. Locks remain held until this process is reaped.
            names = [f'gpu_{gpu}' for gpu in sorted(self.spec['gpus'])] + [f'port_{port}']
            await self._acquire_resources(control, names, port)
            self.log_path.parent.mkdir(parents=True, exist_ok=True)
            stream = self._stack.enter_context(self.log_path.open('a', encoding='utf-8'))
            self._report(f'加载模型 {self.model["name"]}：GPU={self.spec["gpus"]}，'
                         f'DP={self.spec["data_parallel_size"]}，TP={self.spec["tensor_parallel_size"]}；'
                         f'服务日志={self.log_path}')
            # Popen returns before any await, so cancellation cannot lose a newly
            # spawned process between creation and registering its cleanup owner.
            self._process = subprocess.Popen(
                self.argv, env=self.env, cwd=self.root, stdin=subprocess.DEVNULL,
                stdout=stream, stderr=subprocess.STDOUT, start_new_session=True)
            started = last_log = time.monotonic()
            async with httpx.AsyncClient(trust_env=False, timeout=self.spec['readiness_timeout_s']) as client:
                while True:
                    if self._process.poll() is not None:
                        raise RuntimeError(f'Model service exited ({self._process.returncode}); see {self.log_path}')
                    remaining = self.spec['startup_timeout_s'] - (time.monotonic() - started)
                    if remaining <= 0:
                        raise TimeoutError(f'Model startup timed out; see {self.log_path}')
                    try:
                        response = await asyncio.wait_for(
                            client.get(self.model['base_url'].rstrip('/') + '/models'), remaining)
                        response.raise_for_status()
                    except (httpx.HTTPError, asyncio.TimeoutError):
                        pass
                    else:
                        names = [m['id'] for m in response.json()['data']]
                        if names != [self.model['name']]:
                            raise RuntimeError(f'Model endpoint differs: {names}; see {self.log_path}')
                        break
                    now = time.monotonic()
                    if now - last_log >= self.spec['startup_log_interval_s']:
                        self._report(f'模型仍在加载：{self.model["name"]}，已等 {now - started:.0f}s；日志={self.log_path}')
                        last_log = now
                    remaining = self.spec['startup_timeout_s'] - (now - started)
                    await asyncio.sleep(min(self.spec['poll_interval_s'], max(0, remaining)))
            self._report(f'模型就绪：{self.model["name"]}，PID={self._process.pid}，'
                         f'加载耗时={time.monotonic() - started:.1f}s')
        except BaseException as exc:
            await self._release()
            if isinstance(exc, asyncio.CancelledError):
                raise
            raise ModelServiceError(str(exc)) from exc

    async def aclose(self):
        if self._startup is not None:
            if not self._startup.done():
                self._startup.cancel()
            with suppress(asyncio.CancelledError, ModelServiceError):
                await self._startup
        await self._release()

    async def _release(self):
        process, self._process = self._process, None
        try:
            if process is None:
                return
            self._report(f'释放模型：{self.model["name"]}，PID={process.pid}')
            with suppress(ProcessLookupError):
                os.killpg(process.pid, signal.SIGTERM)
            deadline = time.monotonic() + self.spec['shutdown_timeout_s']
            while True:
                process.poll()  # Reap parent; children may still own the group.
                try:
                    os.killpg(process.pid, 0)
                except ProcessLookupError:
                    break
                if time.monotonic() >= deadline:
                    break
                await asyncio.sleep(min(self.spec['poll_interval_s'], max(0, deadline - time.monotonic())))
        finally:
            try:
                if process is not None:
                    # Also catch descendants left behind after the parent exits,
                    # and force cleanup if cancellation interrupts graceful wait.
                    with suppress(ProcessLookupError):
                        os.killpg(process.pid, signal.SIGKILL)
                    process.wait(timeout=self.spec['shutdown_timeout_s'])
            finally:
                self._stack.close()
