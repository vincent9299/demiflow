"""Inert model deployment declarations and lazy action/persistent leases."""
from __future__ import annotations

import asyncio
from contextlib import asynccontextmanager
import hashlib
import json
import os
from pathlib import Path
import weakref
from collections.abc import Mapping

from .contracts import ModelServiceError, service_request_slot


PROFILE_VERSION = 'demiflow-model-services-v1'
PROFILES = {'vllm': 'openai_compatible', 'autotrust-jev': 'systemone',
            'pplx': 'systemone', 'amos': 'systemone', 'd3': 'systemone'}
FIELDS = {'profile', 'weights', 'revision', 'gpus', 'lifetime', 'python', 'directory',
          'options', 'request_concurrency', 'startup_timeout_s', 'shutdown_timeout_s',
          'batch_evidence', 'batch_max_size', 'replicas', 'performance'}


def canonical(value):
    def encode(item):
        if isinstance(item,Mapping): return dict(item)
        raise TypeError('Deployment values must be JSON values')
    return json.dumps(value, sort_keys=True, ensure_ascii=False, separators=(',', ':'), allow_nan=False,default=encode)


def fingerprint(value):
    return hashlib.sha256(canonical(value).encode()).hexdigest()


def deployment_config(value):
    """Pure, closed validation; no model files, environment or hardware are read."""
    if not isinstance(value, Mapping) or set(value)-FIELDS:
        raise ValueError('deployment requires known fields: '+', '.join(sorted(FIELDS)))
    result = json.loads(canonical(value))
    for key in ('profile', 'weights', 'revision'):
        if not isinstance(result.get(key), str) or not result[key].strip():
            raise ValueError('deployment requires nonempty '+key)
    if result['profile'] not in PROFILES:
        raise ValueError('Unknown deployment profile: '+result['profile'])
    if 'batch_evidence' in result or 'batch_max_size' in result:
        if (result['profile'] not in {'pplx','autotrust-jev'}
                or not isinstance(result.get('batch_evidence'),str)
                or not Path(result['batch_evidence']).is_absolute()
                or type(result.get('batch_max_size')) is not int or not 2<=result['batch_max_size']<=64):
            raise ValueError('Native batch requires an absolute batch_evidence path and batch_max_size 2..64')
    # Relative paths would change meaning when a bundle/worker changes cwd.
    if not Path(result['weights']).is_absolute():
        raise ValueError('deployment.weights must be an absolute local path')
    gpus = result.get('gpus')
    if (not isinstance(gpus, (tuple, list)) or not gpus or len(gpus)>64
            or any(type(g) is not int or g < 0 for g in gpus) or len(set(gpus)) != len(gpus)):
        raise ValueError('deployment.gpus must explicitly list 1..64 unique GPU indices')
    result['gpus'] = list(gpus)
    replicas=result.get('replicas',1)
    if type(replicas) is not int or not 1<=replicas<=16 or len(gpus)%replicas:
        raise ValueError('replicas must be 1..16 and evenly divide the GPU list')
    if replicas>1 and 'batch_evidence' in result:
        raise ValueError('Replica deployments use isolated single requests; batch evidence is per device')
    result.setdefault('lifetime', 'action')
    if result['lifetime'] not in {'action', 'persistent'}:
        raise ValueError('deployment.lifetime must be action or persistent')
    for key in ('python', 'directory'):
        if key in result and (not isinstance(result[key], str) or not Path(result[key]).is_absolute()):
            raise ValueError('deployment.'+key+' must be an absolute path')
    performance = result.get('performance', 'native')
    if not isinstance(performance, str) or performance not in {'native', 'optimized', 'auto'}:
        raise ValueError('deployment.performance must be native, optimized or auto')
    options = result.setdefault('options', {})
    if not isinstance(options, dict):
        raise ValueError('Unsupported deployment.options')
    if performance in {'optimized', 'auto'}:
        if 'batch_evidence' in result:
            raise ValueError('Optimized performance requires independent single requests')
        if result['profile'] in {'pplx', 'amos', 'd3', 'autotrust-jev'}:
            options.setdefault('cuda_graphs', {})
        else:
            raise ValueError('No verified performance recipe for this profile')
    allowed = {'dtype', 'max_model_len', 'max_num_seqs', 'max_num_batched_tokens',
               'tensor_parallel_size', 'data_parallel_size', 'gpu_memory_utilization',
               'cpu_threads', 'max_length', 'cuda_graphs', 'request_pipelining', 'pplx_fast', 'batching'}
    if not isinstance(options, dict) or set(options)-allowed:
        raise ValueError('Unsupported deployment.options')
    profile_options = (allowed-{'cpu_threads','max_length','pplx_fast','batching'} if result['profile']=='autotrust-jev'
                       else allowed-{'cpu_threads','max_length','cuda_graphs','request_pipelining','pplx_fast','batching'} if result['profile']=='vllm'
                       else {'cpu_threads','max_length','cuda_graphs','pplx_fast','batching'} if result['profile']=='pplx'
                       else {'cpu_threads','max_length','cuda_graphs'} if result['profile'] in {'pplx','amos'}
                       else {'cpu_threads','cuda_graphs'})
    if set(options)-profile_options:
        raise ValueError('Options are not supported by this deployment profile')
    import math
    if 'pplx_fast' in options:
        from .pplx_fast import fast_config
        if 'cuda_graphs' in options or performance != 'native':
            raise ValueError('pplx_fast selects native FastOptions and cannot combine with other performance recipes')
        options['pplx_fast'] = fast_config(options['pplx_fast'])
        if options.get('max_length',32768)>options['pplx_fast']['max_input_tokens']:
            raise ValueError('max_length exceeds pplx_fast.max_input_tokens')
    if 'batching' in options:
        from .batching import batching_config
        if 'pplx_fast' not in options or replicas != 1:
            raise ValueError('Service batching currently requires one pplx_fast instance')
        options['batching'] = batching_config(options['batching'])
        if 'batch_max_size' in result and result['batch_max_size'] != options['batching']['max_size']:
            raise ValueError('Batch evidence size must match service batching.max_size')
    if 'cuda_graphs' in options:
        from .cuda_graphs import graph_config
        if 'request_pipelining' in options:
            raise ValueError('request_pipelining is for the native JEV HTTP backend; CUDA graphs manage their own lanes')
        if result['profile']=='autotrust-jev' and isinstance(options['cuda_graphs'],dict):
            options['cuda_graphs']={'memory_limit_mib':75776,**options['cuda_graphs']}
        options['cuda_graphs']=graph_config(options['cuda_graphs'])
        if replicas!=1 or 'batch_evidence' in result:
            raise ValueError('CUDA graphs require one replica without native batching')
    for key, val in options.items():
        if key in {'cuda_graphs', 'pplx_fast', 'batching'}:
            continue
        elif key == 'request_pipelining':
            if type(val) is not bool: raise ValueError('request_pipelining must be boolean')
        elif key == 'cpu_threads':
            if type(val) is not int or not 1 <= val <= 64:
                raise ValueError('deployment.options.cpu_threads must be 1..64')
        elif key == 'dtype':
            if val not in {'auto', 'float16', 'bfloat16', 'float32'}:
                raise ValueError('Invalid deployment dtype')
        elif key == 'gpu_memory_utilization':
            if type(val) not in (int,float) or not math.isfinite(val) or not 0 < val < 1:
                raise ValueError('gpu_memory_utilization must be between zero and one')
        elif type(val) is not int or val < 1:
            raise ValueError('deployment.options.'+key+' must be a positive integer')
    result.setdefault('request_concurrency', 8)
    if type(result['request_concurrency']) is not int or not 1 <= result['request_concurrency'] <= 512:
        raise ValueError('request_concurrency must be 1..512')
    if result['request_concurrency']<replicas:
        raise ValueError('request_concurrency must admit at least one call per replica')
    if 'cuda_graphs' in options and result['request_concurrency']<options['cuda_graphs']['streams']:
        raise ValueError('request_concurrency must admit all configured CUDA streams')
    for key, default in (('startup_timeout_s',600),('shutdown_timeout_s',30)):
        result.setdefault(key, default)
        if type(result[key]) not in (int,float) or not math.isfinite(result[key]) or result[key] <= 0:
            raise ValueError(key+' must be positive and finite')
    if result['profile'] not in {'vllm', 'autotrust-jev'} and len(gpus) != replicas:
        raise ValueError('This profile requires one GPU per model instance')
    if result['profile'] in {'vllm', 'autotrust-jev'}:
        tp = options.get('tensor_parallel_size', len(gpus)//replicas)
        dp = options.get('data_parallel_size', 1)
        if replicas>1 and dp!=1:
            raise ValueError('Use replicas or native vLLM data parallelism, not both')
        if tp*dp*replicas != len(gpus):
            raise ValueError('DP × TP × replicas must equal the configured GPU count')
    if result['profile'] == 'autotrust-jev':
        if 'cuda_graphs' in options and (len(gpus)!=1 or options.get('tensor_parallel_size',1)!=1
                or options.get('data_parallel_size',1)!=1 or options.get('dtype','bfloat16')!='bfloat16'
                or options.get('max_model_len',32768)>32768 or options.get('max_num_seqs',64)>64):
            raise ValueError('JEV CUDA graphs require one GPU, bf16, max_model_len <= 32768 and max_num_seqs <= 64')
        if (options.get('max_num_seqs')==1 or options.get('request_pipelining')) and 'batch_evidence' in result:
            raise ValueError('Single-sequence pipelining and native batch evidence cannot be combined')
        if options.get('dtype', 'bfloat16') not in {'bfloat16', 'float16'}:
            raise ValueError('JEV requires its validated bf16/fp16 execution profile')
        if options.get('max_num_batched_tokens', options.get('max_model_len',32768)) < options.get('max_model_len',32768):
            raise ValueError('JEV requires max_num_batched_tokens >= max_model_len')
    return json.loads(canonical(result))


def semantic_contract(name, config):
    options=dict(config['options'])
    if config['profile'] in {'vllm','autotrust-jev'}:
        options.setdefault('tensor_parallel_size',len(config['gpus'])//config.get('replicas',1))
        options.setdefault('data_parallel_size',1)
    return {'name': name, 'revision': config['revision'], 'profile': config['profile'],
            'profile_version': PROFILE_VERSION, 'options': options}


def logical_base(name, config):
    return 'http://demiflow.invalid/'+fingerprint(semantic_contract(name, config))+'/v1'


def service_directory(config):
    # Per-user, host-local default; explicit directories support controlled deployments/tests.
    return Path(config.get('directory') or os.environ.get('DEMIFLOW_SERVICES_HOME') or
                (Path.home()/'.cache/demiflow/model-services')).resolve()


def deployment_id(name, config):
    return fingerprint({'name':name,'config':config,'profile_version':PROFILE_VERSION})[:32]


_action_owners = weakref.WeakKeyDictionary()


class DeploymentService:
    def __init__(self, name, configuration):
        self.name = name
        self.configuration = deployment_config(configuration)

    def bind(self, model=None):
        if model is not None:
            if model.name != self.name or model.transport != PROFILES[self.configuration['profile']]:
                raise ValueError('Model and deployment differ')
            if model.base_url != logical_base(self.name, self.configuration):
                raise ValueError('Managed model must use its logical endpoint')
        return _DeploymentLease(self)


class _DeploymentLease:
    def __init__(self, declaration):
        self.declaration = declaration
        self.owner = None
        self.key = None
        self.registry = None
        self._closed = False
        self._closing = None
        self._prepare_lock = asyncio.Lock()

    @property
    def base_url(self):
        if self.owner is None:
            raise ModelServiceError('Deployment is not ready')
        return self.owner.base_url

    async def ensure_ready(self):
        if self.owner is None:
            async with self._prepare_lock:
                await self._ensure_ready()
        else:
            await self._ensure_ready()

    async def _ensure_ready(self):
        if self._closed:
            raise ModelServiceError('Deployment lease is closed')
        if self.owner is None:
            from .profiles import build_service
            from .prepare import read_prepared
            spec = self.declaration
            manifest = await asyncio.to_thread(read_prepared, spec.name, spec.configuration)
            if self._closed:
                raise ModelServiceError('Deployment lease is closed')
            if self.owner is not None:
                await self.owner.ensure_ready()
                return
            # No await between registry lookup and increment: one loop owns this registry.
            self.registry = _action_owners.setdefault(asyncio.get_running_loop(), {})
            self.key = (str(service_directory(spec.configuration)), deployment_id(spec.name,spec.configuration))
            entry = self.registry.get(self.key)
            if entry is None:
                from .prepare import _versions
                current = await asyncio.to_thread(_versions, manifest['python'], spec.configuration['profile'])
                if current != manifest['runtime']:
                    raise ModelServiceError('Prepared runtime changed; prepare a new revision before execution')
                if self._closed: raise ModelServiceError('Deployment lease is closed')
                # Runtime inspection yielded: another caller may have registered it.
                entry = self.registry.get(self.key)
            while entry is not None and entry[2] is not None:
                # The old process still owns its GPU/port until close completes.
                # Keep that transition visible to a new action in the same loop.
                await asyncio.shield(entry[2])
                if self._closed: raise ModelServiceError('Deployment lease is closed')
                entry = self.registry.get(self.key)
            if entry is None:
                entry = [build_service(spec.name, spec.configuration, manifest).bind(), 0, None]
                self.registry[self.key] = entry
            if self.owner is not None:
                await self.owner.ensure_ready()
                return
            entry[1] += 1
            self.owner = entry[0]
        await self.owner.ensure_ready()

    @asynccontextmanager
    async def request_slot(self):
        async with service_request_slot(self.owner) as endpoint:
            yield endpoint

    async def aclose(self):
        if self._closed:
            if self._closing is not None: await asyncio.shield(self._closing)
            return
        self._closed = True
        if self.owner is None:
            return
        entry = self.registry[self.key]
        entry[1] -= 1
        if entry[1] == 0:
            async def close():
                try: await self.owner.aclose()
                finally:
                    if self.registry.get(self.key) is entry: del self.registry[self.key]
            self._closing = entry[2] = asyncio.create_task(close())
            await asyncio.shield(self._closing)
