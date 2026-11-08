"""Declarative local vLLM resources for native prompt and embedding actors.

Configuration has no process or filesystem side effects. At execution the actor
binds the prompt's existing model endpoint, starts on its first cache miss and
closes this run's process group through its normal aclose hook. No business rows,
request journals, model downloads or stage selection live here.
"""
from __future__ import annotations

from collections.abc import Mapping
import json
import math
import os
from pathlib import Path
import subprocess
import sys
from urllib.parse import urlparse


from .contracts import ModelServiceError


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
        from .http import ManagedHTTPService
        # vLLM owns argument construction; the HTTP owner owns all processes.
        environment = {key: value for key, value in env.items() if key != 'CUDA_VISIBLE_DEVICES'}
        cfg = self._config
        declaration = ManagedHTTPService(
            argv, base_url=endpoint, root=self.root, gpus=cfg['gpus'], env=environment,
            ready_path='/v1/models', expected_model=model.name, model_match='exact',
            required_paths=[cfg['model_path']], log_path=self.log_path, log=self.log,
            startup_timeout_s=cfg['startup_timeout_s'], shutdown_timeout_s=cfg['shutdown_timeout_s'],
            readiness_timeout_s=cfg['readiness_timeout_s'], poll_interval_s=cfg['poll_interval_s'],
            resource_wait_timeout_s=cfg['resource_wait_timeout_s'],
            startup_log_interval_s=cfg['startup_log_interval_s'])
        return declaration.bind(model)
