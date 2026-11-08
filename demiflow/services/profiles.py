"""Model-specific launch recipes; lifecycle and request scheduling stay generic."""
from pathlib import Path
import json
import os

from .deployment import service_directory, fingerprint, canonical
from .http import ManagedHTTPService
from .group import ServiceGroup
from .shared_http import SharedHTTPService


def component_suffixes(config):
    names=['engine','wrapper'] if config['profile']=='autotrust-jev' and 'cuda_graphs' not in config['options'] else ['engine']
    count=config.get('replicas',1)
    return names if count==1 else [f'replica-{i}-{name}' for i in range(count) for name in names]


def build_service(name, config, manifest, *, validation_batch_size=1):
    frozen = manifest.get('_execution')
    if frozen is not None:
        from .tuning import execution_config
        config = execution_config(config, frozen)
    elif config.get('performance') == 'auto':
        raise ValueError('Auto deployment requires a validated frozen execution profile')
    if 'cuda_graphs' in config['options'] and validation_batch_size!=1:
        raise ValueError('CUDA graph execution does not support native batch validation')
    batching = config['options'].get('batching')
    if batching and not config.get('batch_evidence') and validation_batch_size < batching['max_size']:
        raise ValueError('Service batching requires quality evidence or explicit batch validation')
    if config['profile']=='autotrust-jev' and (config['options'].get('max_num_seqs')==1 or config['options'].get('request_pipelining')) and validation_batch_size!=1:
        raise ValueError('Single-sequence pipelining does not perform native batch validation')
    count=config.get('replicas',1)
    if count>1:
        from .replicas import ReplicaService
        if validation_batch_size!=1:
            raise ValueError('Validate native batching on one replica; replica execution uses independent singles')
        width=len(config['gpus'])//count
        port_count=2 if config['profile']=='autotrust-jev' else 1
        replicas=[]
        for index in range(count):
            child={**config,'replicas':1,'gpus':config['gpus'][index*width:(index+1)*width],
                   'request_concurrency':1}
            prepared={**manifest,'ports':manifest['ports'][index*port_count:(index+1)*port_count],
                      'component_prefix':f'replica-{index}-'}
            replicas.append(build_service(name,child,prepared))
        return ReplicaService(replicas)
    root = service_directory(config)
    python, weights = manifest['python'], config['weights']
    profile, options = config['profile'], config['options']
    ports = manifest['ports']
    runner = str(Path(__file__).with_name('model_worker.py'))
    services = []
    identity = {'model_contract':manifest['model_contract'],'runtime':manifest['runtime'],
                'adapter_version':manifest['adapter_version'],'weights_signature':manifest.get('weights_signature')}
    if frozen is not None:
        identity['execution_configuration'] = frozen
        identity['tuning'] = manifest.get('_tuning', {'state':'candidate-validation'})
    execution_environment={key:value for key,value in os.environ.items()
        if key.startswith(('VLLM_','STARTLUX_','TORCH_','NVIDIA_',
                           'CUBLAS_','CUBLASLT_','FLASHINFER_'))
        or key == 'OMP_NUM_THREADS'}
    if profile=='autotrust-jev':
        execution_environment.update(STARTLUX_PREFIX_BIDIR='1',VLLM_USE_FLASHINFER_SAMPLER='0')
        if 'cuda_graphs' in options:
            execution_environment.update(VLLM_ENABLE_V1_MULTIPROCESSING='0',OMP_NUM_THREADS='8')
    identity['execution_environment_sha256']=fingerprint(execution_environment)
    wrapper_options = ['--identity',json.dumps(identity),'--validation-batch-size',str(validation_batch_size)]
    if frozen is not None and frozen['mode'] == 'native':
        wrapper_options += ['--native-reference']
    if config.get('batch_evidence'):
        wrapper_options += ['--batch-evidence',config['batch_evidence'],
                            '--batch-max-size',str(config['batch_max_size'])]

    def component(command, port, suffix, *, gpus=(), environment=None, ready='/health'):
        suffix=manifest.get('component_prefix','')+suffix
        configuration = dict(command=command, base_url=f'http://127.0.0.1:{port}/v1',
            gpus=list(gpus), env=environment or {}, expected_model=name, ready_path=ready,
            log_path=str(Path(manifest.get('_log_directory',root/'deployments'/manifest['id']))/(suffix+'.log')),
            log_max_bytes=8*1024**2,
            resource_directory=str(root/'resources'), required_paths=[weights],
            startup_timeout_s=config['startup_timeout_s'], shutdown_timeout_s=config['shutdown_timeout_s'])
        if config['lifetime']=='persistent':
            service = SharedHTTPService(root=root, name=manifest['id']+'-'+suffix,
                configuration=configuration, request_concurrency=config['request_concurrency'])
        else:
            service = ManagedHTTPService(root=root, **configuration,
                                         request_concurrency=config['request_concurrency'])
        services.append(service)

    if profile=='autotrust-jev' and 'cuda_graphs' in options:
        component([python,runner,'--profile',profile,'--model',weights,'--name',name,
            '--port',str(ports[0]),'--max-inflight',str(config['request_concurrency']),
            '--cuda-graphs',canonical(options['cuda_graphs']),
            '--jev-options',canonical({k:v for k,v in options.items() if k not in {'cuda_graphs','request_pipelining'}}),
            *wrapper_options],ports[0],'engine',gpus=config['gpus'],environment={
                'STARTLUX_PREFIX_BIDIR':'1','VLLM_USE_FLASHINFER_SAMPLER':'0',
                'VLLM_ENABLE_V1_MULTIPROCESSING':'0','OMP_NUM_THREADS':'8'})
    elif profile in {'vllm','autotrust-jev'}:
        from .vllm import vllm_config, _service_command
        allowed = {'dtype','max_model_len','max_num_seqs','max_num_batched_tokens',
                   'tensor_parallel_size','data_parallel_size','gpu_memory_utilization'}
        config_values = {key:value for key,value in options.items() if key in allowed}
        if profile=='autotrust-jev':
            config_values = {'dtype':'bfloat16','max_model_len':32768,'max_num_batched_tokens':32768,
                             'max_num_seqs':64,'gpu_memory_utilization':.85,**config_values}
            if 'max_num_batched_tokens' not in options:
                config_values['max_num_batched_tokens'] = config_values['max_model_len']
        spec = vllm_config(dict(model_path=weights, python=python, gpus=config['gpus'],
            **{'tensor_parallel_size':len(config['gpus']),**config_values}))
        command, full_env = _service_command(spec, {'name':name,'base_url':f'http://127.0.0.1:{ports[0]}/v1'},root)
        environment = {'PATH':full_env['PATH']}
        if profile=='autotrust-jev':
            environment.update(STARTLUX_PREFIX_BIDIR='1',VLLM_USE_FLASHINFER_SAMPLER='0')
            command += ['--no-enable-prefix-caching','--no-enable-chunked-prefill',
                        '--logprobs-mode','processed_logprobs','--max-logprobs','255',
                        '--attention-backend','FLASH_ATTN','--generation-config','vllm',
                        '--limit-mm-per-prompt','{"image":0,"video":0}']
        component(command,ports[0],'engine',gpus=config['gpus'],environment=environment,ready='/v1/models')
        if profile=='autotrust-jev':
            # Let CPU/HTTP preparation overlap only when our owned backend
            # structurally admits one sequence per forward. This is not batch
            # evidence or multiple concurrent GPU forwards.
            pipeline_options=(['--backend-single-sequence'] if options.get('max_num_seqs')==1
                              else ['--backend-serialized'] if options.get('request_pipelining') else [])
            component([python,runner,'--profile','autotrust-jev','--model',weights,'--name',name,
                '--port',str(ports[1]),'--backend',f'http://127.0.0.1:{ports[0]}/v1',
                '--max-inflight',str(config['request_concurrency']),'--max-length',str(config_values['max_model_len']),
                *wrapper_options,*pipeline_options],ports[1],'wrapper',
                environment={'DEMIFLOW_BACKEND_GPUS':','.join(map(str,config['gpus']))})
    elif profile in {'pplx', 'amos', 'd3'}:
        command = [python,runner,'--profile',profile,'--model',weights,'--name',name,
                   '--port',str(ports[0]),'--max-inflight',str(config['request_concurrency']),*wrapper_options]
        for key in ('cpu_threads','max_length'):
            if key in options: command += ['--'+key.replace('_','-'),str(options[key])]
        if 'cuda_graphs' in options:
            command += ['--cuda-graphs',canonical(options['cuda_graphs'])]
        for key in ('pplx_fast','batching'):
            if key in options:
                command += ['--'+key.replace('_','-'),canonical(options[key])]
        component(command,ports[0],'engine',gpus=config['gpus'])
    else:
        raise ValueError('Unknown model service profile')
    return services[0] if len(services)==1 else ServiceGroup(services)
