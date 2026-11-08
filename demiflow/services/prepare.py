"""Explicit preparation of isolated model runtimes. Never called by row execution."""
from __future__ import annotations
import json
import hashlib
import os
from pathlib import Path
import shutil
import socket
import sys
import time
from .preparation_io import locked, run_bounded

from .contracts import ModelServiceError
from .deployment import (PROFILE_VERSION, deployment_config, deployment_id,
                         fingerprint, semantic_contract, service_directory)


# Pinned profile recipes. Existing interpreters can be attached read-only.
REQUIREMENTS = {
    'vllm': ['vllm==0.31.0'],
    'autotrust-jev': ['vllm==0.31.0', 'transformers==5.17.0',
        'torch==2.13.0', 'flashinfer-python==0.7.0.post1', 'nvidia-cutlass-dsl==4.7.1',
        'startlux-decide @ git+https://github.com/StartLuxLabs/StartLux-Decision@cb1d11f1ac9e9b43b95ef3a6468fa29b607c40fd'],
    'pplx': ['pplx-decider==0.3.0'],
    'amos': ['amos-serve==1.0.0'],
    # d3 ships its runtime alongside weights; attach an explicitly provisioned interpreter.
    'd3': [],
}
PACKAGES = {'vllm':['vllm'], 'autotrust-jev':['vllm','transformers','startlux-decide',
                                         'flashinfer-python','nvidia-cutlass-dsl'],
            'pplx':['pplx-decider','transformers'], 'amos':['amos-serve','transformers'], 'd3':['torch','transformers']}
for _names in PACKAGES.values():
    if 'torch' not in _names: _names.append('torch')


def adapter_version():
    return fingerprint({name:hashlib.sha256(Path(__file__).with_name(name).read_bytes()).hexdigest()
        for name in ('model_worker.py','model_adapters.py','pplx_graphs.py','pplx_fast.py','batching.py','cuda_graphs.py','native_graphs.py',
                     'jev_graphs.py','jev_runtime.py','profiles.py','quality.py',
                     'capture_kernels.py','pointwise_cuda.py','triton_dispatch.py','tuning.py','tuning_runner.py')})


def weights_signature(directory):
    """Detect changed local artifacts without re-reading tens of GiB each run.

    Weights must still be treated as immutable under the declared revision.
    Stat identity catches ordinary replacement/in-place edits, not malicious
    timestamp spoofing. Metadata/code files additionally receive content hashes.
    """
    root=Path(directory); records=[]; visited=0; inspected_bytes=0
    for parent,dirs,files in os.walk(root):
        dirs[:]=[d for d in dirs if d not in {'.git','.cache','__pycache__'}]
        visited+=1
        if visited>10000: raise ValueError('Model directory exceeds 10000-entry inspection budget')
        for name in sorted(files):
            visited+=1
            if visited>10000: raise ValueError('Model directory exceeds 10000-file inspection budget')
            path=Path(parent)/name
            if path.suffix not in {'.safetensors','.bin','.pt','.pth','.json','.model','.txt','.py','.tiktoken','.jinja','.jinja2'}:
                continue
            stat=path.stat(); digest=None
            if path.suffix in {'.json','.py','.jinja','.jinja2'} and stat.st_size<=1024*1024:
                inspected_bytes+=stat.st_size
                if inspected_bytes>64*1024*1024:
                    raise ValueError('Model metadata exceeds 64 MiB inspection budget')
                digest=hashlib.sha256(path.read_bytes()).hexdigest()
            records.append((str(path.relative_to(root)),stat.st_size,stat.st_mtime_ns,digest))
    return fingerprint(sorted(records))


def _read(path):
    with Path(path).open('rb') as stream:
        raw = stream.read(1024*1024+1)
    if len(raw) > 1024*1024:
        raise ValueError('Service manifest exceeds 1 MiB')
    return json.loads(raw)


def _write(path, value):
    from .manage import _write as atomic_write
    atomic_write(Path(path), value)


def _versions(python, profile, *, timeout_s=30):
    code = '''import importlib.metadata as metadata,json,sys,hashlib
sources={'triton':['triton/runtime/autotuner.py'],
         'fla-core':['fla/ops/utils/cache.py'],
         'startlux-decide':['startlux_decide/vllm_plugin.py'],
         'vllm':['vllm/model_executor/layers/mamba/gdn/qwen_gdn_linear_attn.py',
                 'vllm/model_executor/layers/utils.py','vllm/forward_context.py'],
         'torch':['torch/version.py'],
         'flashinfer-python':['flashinfer/utils.py','flashinfer/gdn_prefill.py',
             'flashinfer/gdn_kernels/delta_rule_dsl/delta_rule_sm90.py',
             'flashinfer/gdn_kernels/delta_rule_dsl/delta_rule_cp_sm90.py'],
         'pplx-decider':['pplx_decider/model.py','pplx_decider/protocol.py','pplx_decider/fast.py'],
         'transformers':['transformers/models/qwen3_5/modeling_qwen3_5.py'],
         'amos-serve':['amos/serve.py','amos/packing.py','amos/decoder.py']}
provenance={}; hashes={}; kernels={}
for name in ['triton','flash-linear-attention','fla-core','causal-conv1d','flash-attn','tilelang',
             'flashinfer-python','flashinfer-cubin','flashinfer-jit-cache',
             'nvidia-cublas','nvidia-cublas-cu12','nvidia-cublas-cu13','nvidia-cutlass-dsl']:
    try: kernels[name]=metadata.version(name)
    except metadata.PackageNotFoundError: kernels[name]=None
for name in [*sys.argv[1:],*(n for n in kernels if kernels[n] and n in sources)]:
    distribution=metadata.distribution(name)
    direct=distribution.read_text('direct_url.json')
    if direct: provenance[name]=json.loads(direct).get('vcs_info',{})
    for relative in sources.get(name,[]):
        path=distribution.locate_file(relative)
        # Attached generic vLLM versions need not contain the JEV backend.
        # Absence is part of the identity; it is not a dependency to install.
        if name in {'vllm','flashinfer-python'} and not path.is_file():
            hashes[relative]=None
            continue
        with path.open('rb') as stream: raw=stream.read(1024*1024+1)
        if len(raw)>1024*1024: raise ValueError('Runtime source exceeds inspection budget')
        hashes[relative]=hashlib.sha256(raw).hexdigest()
print(json.dumps({'python':sys.version.split()[0],
    'packages':{n:metadata.version(n) for n in sys.argv[1:]},
    'provenance':provenance,'source_sha256':hashes,'kernel_packages':kernels}))
'''
    raw = run_bounded([str(python), '-c', code, *PACKAGES[profile]],
                      timeout_s=timeout_s, output_bytes=65536)
    return json.loads(raw)


def _allocate_ports(count, used=()):
    # Cooperative reservation: a global preparation lock serializes manifests.
    # OS conflicts with unrelated processes are still checked at actual startup.
    sockets = []
    try:
        for _ in range(256):
            sock = socket.socket()
            sock.bind(('127.0.0.1',0))
            if sock.getsockname()[1] in used:
                sock.close()
                continue
            sockets.append(sock)
            if len(sockets)==count: break
        if len(sockets)!=count: raise RuntimeError('Cannot allocate unused service ports')
        return [sock.getsockname()[1] for sock in sockets]
    finally:
        for sock in sockets: sock.close()


def prepare_deployment(name, configuration, *, timeout_s=1800):
    config = deployment_config(configuration)
    root = service_directory(config)
    weights = Path(config['weights'])
    if not weights.is_dir(): raise FileNotFoundError('Model weights directory: '+str(weights))
    for relative in ({'autotrust-jev':['jev_format.py','vllm_readout.json'],
                       'd3':['d3_server.py']}.get(config['profile'],[])):
        if not (weights/relative).is_file(): raise FileNotFoundError(weights/relative)
    if config['profile']=='d3' and not config.get('python'):
        raise ValueError('d3 requires python pointing to an existing compatible runtime')
    root.mkdir(parents=True, exist_ok=True)
    identifier = deployment_id(name, config)
    directory = root/'deployments'/identifier
    directory.mkdir(parents=True, exist_ok=True)
    with locked(directory/'prepare.lock', timeout_s):
        target = directory/'manifest.json'
        if target.exists():
            manifest = read_prepared(name, config, resolve_tuning=False)
            current = _versions(manifest['python'], config['profile'])
            if current != manifest['runtime']:
                raise ValueError('Prepared runtime changed; declare a new revision and prepare again')
            return manifest
        python = config.get('python')
        runtime_root = None
        if python is None:
            uv = shutil.which('uv')
            if sys.version_info[:2] != (3,12) and uv is None:
                raise ValueError('Install demiflow[services] for Python 3.12 provisioning, or set deployment.python')
            runtime_root = root/'runtimes'/fingerprint({'requirements':REQUIREMENTS[config['profile']],
                'python':sys.version, 'profile_version':PROFILE_VERSION})
            runtime_root.parent.mkdir(parents=True, exist_ok=True)
            with locked(runtime_root.with_suffix('.lock'), timeout_s):
                if not (runtime_root/'ready.json').is_file():
                    runtime_root.mkdir(exist_ok=True)
                    # Only this recipe-owned unpublished venv can be replaced.
                    shutil.rmtree(runtime_root/'venv', ignore_errors=True)
                    try:
                        # Build at its final path: venv script shebangs must remain valid.
                        command = ([uv,'venv','--python','3.12','--seed',str(runtime_root/'venv')]
                                   if uv else [sys.executable,'-m','venv',str(runtime_root/'venv')])
                        run_bounded(command, timeout_s=min(300,timeout_s), disk_root=runtime_root)
                        executable = runtime_root/'venv/bin/python'
                        run_bounded([str(executable),'-m','pip','install','--no-cache-dir',*REQUIREMENTS[config['profile']]],
                            timeout_s=timeout_s, log_path=runtime_root/'install.log', disk_root=runtime_root)
                        runtime = _versions(executable,config['profile'])
                        _write(runtime_root/'ready.json',runtime)
                    except BaseException:
                        # Preserve the diagnostic log, remove only this failed environment.
                        shutil.rmtree(runtime_root/'venv',ignore_errors=True)
                        raise
                python = str(runtime_root/'venv/bin/python')
        runtime = _versions(python, config['profile'])
        for requirement in REQUIREMENTS[config['profile']]:
            if '==' in requirement:
                package,version=requirement.split('==')
                if runtime['packages'].get(package)!=version:
                    raise ValueError('Prepared runtime must provide '+requirement)
        if config['profile'] in {'vllm','autotrust-jev'} and runtime['packages']['vllm'] != '0.31.0':
            raise ValueError('This deployment profile is validated for vLLM 0.31.0 only')
        with locked(root/'ports.lock', timeout_s):
            used = set()
            with os.scandir(root/'deployments') as entries:
                for index, entry in enumerate(entries):
                    if index>=256: raise ValueError('Service preparation supports at most 256 deployment records')
                    other = Path(entry.path)/'manifest.json'
                    if other.is_file(): used.update(_read(other).get('ports',[]))
            from .profiles import component_suffixes
            ports = _allocate_ports(len(component_suffixes(config)), used)
            manifest = {'schema_version':'demiflow_prepared_service_v1', 'id':identifier,'name':name,
                'profile_version':PROFILE_VERSION, 'configuration':config,'python':str(python),
                'runtime':runtime, 'ports':ports,'created_at':time.time(),
                'model_contract':semantic_contract(name,config), 'adapter_version':adapter_version(),
                'weights_signature':weights_signature(weights),
                'batch_verified':False}
            _write(target,manifest)
        return manifest


def read_prepared(name, configuration, *, resolve_tuning=True):
    config = deployment_config(configuration)
    path = service_directory(config)/'deployments'/deployment_id(name,config)/'manifest.json'
    try:
        value = _read(path)
    except FileNotFoundError as exc:
        raise ModelServiceError('Deployment is not prepared; run demiflow services prepare --config <prompt.yaml>') from exc
    if (value.get('schema_version')!='demiflow_prepared_service_v1'
            or value.get('configuration')!=config or value.get('name')!=name
            or value.get('profile_version')!=PROFILE_VERSION):
        raise ModelServiceError('Prepared deployment identity mismatch')
    if value.get('adapter_version')!=adapter_version():
        raise ModelServiceError('Prepared adapter changed; use a new revision and prepare again')
    if value.get('weights_signature')!=weights_signature(config['weights']):
        raise ModelServiceError('Prepared model artifacts changed; declare a new revision and prepare again')
    ports = value.get('ports')
    from .profiles import component_suffixes
    if (not isinstance(ports,list) or len(ports)!=len(component_suffixes(config))
            or len(set(ports))!=len(ports) or any(type(p) is not int or not 1<=p<=65535 for p in ports)):
        raise ModelServiceError('Prepared deployment has invalid ports')
    if not Path(value['python']).is_file(): raise ModelServiceError('Prepared interpreter is missing')
    if resolve_tuning and config.get('performance') == 'auto':
        from .tuning import resolve
        _, evidence = resolve(name, config, value)
        value['_execution'] = evidence['selected']
        value['_tuning'] = evidence
    return value
