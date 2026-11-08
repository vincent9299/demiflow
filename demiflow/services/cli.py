"""One explicit CLI for model-service preparation, diagnostics and lifecycle."""
import json
import subprocess
from pathlib import Path

import click

from .deployment import deployment_id, service_directory
from .prepare import prepare_deployment, read_prepared, _versions


def declarations(path):
    from demiflow.operator_llm.parser import parse_prompt_pack
    from demiflow.operator_llm.model import parse_prompt_model
    import yaml
    with Path(path).open('rb') as stream:
        raw = stream.read(1024*1024+1)
    if len(raw)>1024*1024: raise ValueError('Service configuration exceeds 1 MiB')
    text = raw.decode()
    value = yaml.safe_load(text)
    if not isinstance(value, dict): raise ValueError('Configuration must be an object')
    if value.get('schema_version')=='demiflow_model_service_v1':
        if set(value)!={'schema_version','model'}:
            raise ValueError('A model-service declaration contains only schema_version and model')
        models = [parse_prompt_model(value['model'],label='service model')]
    elif value.get('schema_version')=='demiflow_prompt_pack_v2':
        models = [prompt.model for prompt in parse_prompt_pack(text).prompts]
    elif value.get('schema_version')=='demiflow_agent_v2':
        models = [parse_prompt_model(value.get('model'),label='agent model')]
    else:
        raise ValueError('Expected a model-service, prompt-pack or agent configuration')
    result = {}
    for model in models:
        if model.deployment is not None:
            identifier = deployment_id(model.name,model.deployment)
            result[identifier] = (model.name,model.deployment)
    if not result: raise ValueError('Configuration contains no managed model.deployment')
    return list(result.values())


def _emit(value): click.echo(json.dumps(value,ensure_ascii=False,indent=2))


def _run(fn):
    try: return fn()
    except (ValueError, OSError, RuntimeError, subprocess.SubprocessError) as exc:
        raise click.ClickException(str(exc)) from exc


@click.group('services')
def services():
    """Prepare and inspect model services declared in the prompt/agent configuration."""


def configuration(fn):
    return click.option('--config',required=True,type=click.Path(exists=True,dir_okay=False,path_type=Path))(fn)


@services.command('prepare')
@configuration
@click.option('--timeout-seconds',default=1800,type=click.IntRange(1,86400),show_default=True)
def prepare(config,timeout_seconds):
    """Explicitly install/verify runtimes; never start a model or use a GPU."""
    def execute():
        values = [prepare_deployment(name,value,timeout_s=timeout_seconds)
                  for name,value in declarations(config)]
        _emit([{'name':v['name'],'id':v['id'],'python':v['python'],'runtime':v['runtime'],
                'batch_verified':False,'state':'prepared'} for v in values])
    _run(execute)


@services.command('doctor')
@configuration
def doctor(config):
    """Read-only environment checks. Does not claim GPU/model quality validation."""
    def execute():
        results=[]
        for name,value in declarations(config):
            try:
                manifest=read_prepared(name,value)
                current=_versions(manifest['python'],value['profile'])
                if current!=manifest['runtime']: raise ValueError('Runtime differs from prepared manifest')
                if not Path(value['weights']).is_dir(): raise ValueError('Weights directory is missing')
                result={'name':name,'ready':True,'runtime':current,'gpu_validation':'pending',
                        'batch_validation':'pending'}
                if manifest.get('_tuning'):
                    result.update(tuning=manifest['_tuning'],gpu_validation='native-and-holdout-passed')
                results.append(result)
            except (OSError,RuntimeError,ValueError,subprocess.SubprocessError) as exc:
                results.append({'name':name,'ready':False,'error':str(exc)})
        _emit(results)
        if any(not row['ready'] for row in results): raise click.ClickException('Service checks failed')
    _run(execute)


def component_names(name,value):
    from .profiles import component_suffixes
    identifier=deployment_id(name,value)
    return [identifier+'-'+suffix for suffix in component_suffixes(value)]


@services.command('start')
@configuration
def start(config):
    """Explicitly start prepared persistent deployments (loads models on GPUs)."""
    import asyncio
    from .deployment import DeploymentService
    async def execute():
        declarations_list=declarations(config)
        if any(value['lifetime']!='persistent' for _,value in declarations_list):
            raise ValueError('services start requires lifetime=persistent; action services start with Dataset execution')
        results=[]
        for name,value in declarations_list:
            owner=DeploymentService(name,value).bind()
            try:
                await owner.ensure_ready()
                results.append({'name':name,'base_url':owner.base_url,
                    'endpoints':list(getattr(owner.owner,'base_urls',(owner.base_url,))),
                    'replicas':value.get('replicas',1),'state':'ready'})
            finally: await owner.aclose()
        _emit(results)
    _run(lambda:asyncio.run(execute()))


@services.command('status')
@configuration
def status(config):
    """Inspect prepared deployments and their persistent supervisors."""
    from .manage import status_service
    def execute():
        _emit([{'model':name,'lifetime':value['lifetime'],
                'observation':'persistent supervisors' if value['lifetime']=='persistent' else 'owned by its active Dataset runtime',
                'components':[status_service(service_directory(value),component)
                              for component in component_names(name,value)] if value['lifetime']=='persistent' else None}
               for name,value in declarations(config)])
    _run(execute)


@services.command('stop')
@configuration
def stop(config):
    """Stop owned persistent services; active users prevent shutdown."""
    from .manage import _stop_service, _directory
    from contextlib import ExitStack
    from .preparation_io import locked
    import fcntl
    def execute():
        results=[]
        for name,value in declarations(config):
            if value['lifetime']!='persistent':
                raise ValueError('Action-owned services close with their action; stop targets persistent deployments')
            # Preflight all users while preventing new acquisition. An active
            # backend must not leave a partially stopped composite deployment.
            components=component_names(name,value); root=service_directory(value)
            with ExitStack() as leases:
                for component in components:
                    directory=_directory(root,component); directory.mkdir(parents=True,exist_ok=True)
                    leases.enter_context(locked(directory/'ensure.lock',value['startup_timeout_s']+30))
                    users=leases.enter_context((directory/'users.lock').open('a'))
                    try: fcntl.flock(users,fcntl.LOCK_EX|fcntl.LOCK_NB)
                    except BlockingIOError as exc: raise RuntimeError('Deployment is in use; stop refused') from exc
                for component in reversed(components):
                    results.append(_stop_service(root,component,timeout_s=value['shutdown_timeout_s']+5))
        _emit(results)
    _run(execute)


@services.command('logs')
@configuration
@click.option('--bytes', 'tail_bytes', default=65536,type=click.IntRange(1,1024*1024))
def logs(config,tail_bytes):
    """Print a bounded tail of each model component's log."""
    def execute():
        from .profiles import component_suffixes
        values=[]
        for name,value in declarations(config):
            directory=service_directory(value)/'deployments'/deployment_id(name,value)
            for suffix in component_suffixes(value):
                path=directory/(suffix+'.log')
                try:
                    with path.open('rb') as stream:
                        stream.seek(0,2); stream.seek(max(0,stream.tell()-tail_bytes))
                        content=stream.read(tail_bytes).decode(errors='replace')
                except FileNotFoundError: content=''
                values.append({'model':name,'component':suffix,'path':str(path),'tail':content})
        _emit(values)
    _run(execute)


@services.command('validate')
@configuration
@click.option('--cases',required=True,type=click.Path(exists=True,dir_okay=False,path_type=Path))
@click.option('--report',required=True,type=click.Path(path_type=Path))
@click.option('--batch-size',default=8,type=click.IntRange(2,64))
@click.option('--parallel',default=4,type=click.IntRange(2,64))
@click.option('--probability-atol',default=1e-6,type=float)
@click.option('--quality-policy',type=click.Path(exists=True,dir_okay=False,path_type=Path))
def validate(config,cases,report,batch_size,parallel,probability_atol,quality_policy):
    """GPU acceptance: launch an action deployment, compare 200+ rows, then stop.

    Run explicitly in a maintenance window. A passing report is scoped to its
    cases, hardware, runtime and tolerance; it does not enable batch by itself.
    """
    import asyncio
    from .quality import benchmark, load_cases
    from .profiles import build_service
    from .prepare import _write
    policy=None
    if quality_policy is not None:
        from .quality import quality_policy as validate_policy
        with quality_policy.open('rb') as stream: raw=stream.read(4097)
        if len(raw)>4096: raise ValueError('Quality policy exceeds 4 KiB')
        policy=validate_policy(json.loads(raw))
    async def execute():
        values=declarations(config)
        if len(values)!=1: raise ValueError('Validate exactly one model at a time')
        name,value=values[0]
        if value['lifetime']!='action' or value['profile'] not in {'pplx','autotrust-jev'} or value.get('replicas',1)!=1:
            raise ValueError('Batch validation requires an action-owned pplx or JEV deployment')
        if value.get('batch_evidence'): raise ValueError('Validation configuration must omit prior batch evidence')
        if 'cuda_graphs' in value['options']:
            raise ValueError('CUDA graph execution does not support native batch validation')
        if value['profile']=='autotrust-jev' and (value['options'].get('max_num_seqs')==1 or value['options'].get('request_pipelining')):
            raise ValueError('Single-sequence pipelining does not perform native batch validation')
        if value['request_concurrency']<parallel: raise ValueError('parallel exceeds request_concurrency')
        data=load_cases(cases)
        if report.exists(): raise ValueError('Report already exists; select a new report path')
        manifest=read_prepared(name,value)
        if await asyncio.to_thread(_versions,manifest['python'],value['profile']) != manifest['runtime']:
            raise ValueError('Runtime differs from prepared manifest')
        owner=build_service(name,value,manifest,validation_batch_size=batch_size).bind()
        try:
            await owner.ensure_ready()
            result=await benchmark(owner.base_url,name,data,max_size=batch_size,parallel=parallel,
                probability_atol=probability_atol,memory_probe=owned_memory_probe(owner),policy=policy)
            report.parent.mkdir(parents=True,exist_ok=True)
            _write(report,result); _emit(result)
            if not result['passed']: raise ValueError('Quality acceptance failed; batch remains disabled')
        finally: await owner.aclose()
    _run(lambda:asyncio.run(execute()))


@services.command('benchmark')
@configuration
@click.option('--cases',required=True,type=click.Path(exists=True,dir_okay=False,path_type=Path))
@click.option('--report',required=True,type=click.Path(path_type=Path))
@click.option('--parallel',default=2,type=click.IntRange(2,64))
def benchmark_singles(config,cases,report,parallel):
    """Compare single/concurrent SystemOne requests. This never certifies batching."""
    import asyncio
    from .quality import benchmark, load_cases
    from .deployment import PROFILES
    from .profiles import build_service
    from .prepare import _write
    async def execute():
        values=declarations(config)
        if len(values)!=1: raise ValueError('Benchmark exactly one model at a time')
        name,value=values[0]
        if value['lifetime']!='action' or PROFILES[value['profile']]!='systemone' or value.get('replicas',1)!=1:
            raise ValueError('Benchmark requires an action-owned single-replica SystemOne deployment')
        if value.get('batch_evidence'): raise ValueError('Single-request benchmark must omit batch evidence')
        if value['request_concurrency']<parallel: raise ValueError('parallel exceeds request_concurrency')
        data=load_cases(cases)
        if report.exists(): raise ValueError('Report already exists; select a new report path')
        manifest=read_prepared(name,value)
        if await asyncio.to_thread(_versions,manifest['python'],value['profile']) != manifest['runtime']:
            raise ValueError('Runtime differs from prepared manifest')
        owner=build_service(name,value,manifest).bind()
        try:
            await owner.ensure_ready()
            result=await benchmark(owner.base_url,name,data,max_size=1,parallel=parallel,
                probability_atol=0,memory_probe=owned_memory_probe(owner))
            report.parent.mkdir(parents=True,exist_ok=True)
            _write(report,result); _emit(result)
            if not result['passed']: raise ValueError('Concurrent execution changed native single-request results')
        finally: await owner.aclose()
    _run(lambda:asyncio.run(execute()))


@services.command('tune')
@configuration
@click.option('--cases',required=True,type=click.Path(exists=True,dir_okay=False,path_type=Path))
@click.option('--objective',type=click.Choice(['throughput','latency']),default='throughput',show_default=True)
@click.option('--max-p95-ms',type=click.IntRange(1,600000),default=2000,show_default=True)
@click.option('--max-candidates',type=click.IntRange(1,16),default=13,show_default=True)
@click.option('--timeout-seconds',type=click.IntRange(30,7200),default=1800,show_default=True)
@click.option('--force',is_flag=True,help='Retune; preserve the previous valid result if this attempt fails.')
def tune_deployment(config,cases,objective,max_p95_ms,max_candidates,timeout_seconds,force):
    """Explicit GPU tuning; requires idle GPUs and 400+ distinct real requests."""
    import asyncio
    from .tuning_runner import tune
    async def execute():
        values=declarations(config)
        if len(values)!=1: raise ValueError('Tune exactly one model at a time')
        name,value=values[0]
        result=await tune(name,value,cases,options=dict(objective=objective,max_p95_ms=max_p95_ms,
            max_candidates=max_candidates,timeout_s=timeout_seconds),force=force)
        report=result['report']
        _emit(dict(name=name,path=result['path'],reused=result['reused'],passed=report['passed'],
                   selected=report['selected']['execution'],measurements=report['selected']['rounds'],
                   search_count=report['search_count'],holdout_count=report['holdout_count']))
    _run(lambda:asyncio.run(execute()))


def owned_memory_probe(owner):
    """Read GPU memory only for owned process trees; no process is modified."""
    from .preparation_io import run_bounded
    import psutil
    def leaves_of(item):
        for child in getattr(item,'owners',[]): yield from leaves_of(child)
        if not getattr(item,'owners',None): yield item
    leaves=list(leaves_of(owner))
    parents=[item._process.pid for item in leaves if getattr(item,'_process',None) is not None]
    def probe():
        try:
            pids=set(parents)
            for pid in parents:
                pids.update(child.pid for child in psutil.Process(pid).children(recursive=True))
            if len(pids)>512: raise ValueError('Deployment process tree exceeds monitoring budget')
            raw=run_bounded(['nvidia-smi','--query-compute-apps=pid,used_memory','--format=csv,noheader,nounits'],
                            timeout_s=5,output_bytes=65536)
            values=[line.decode().split(',') for line in raw.splitlines()]
            selected=[int(memory.strip()) for pid,memory in values if int(pid.strip()) in pids]
            return sum(selected) if selected else None
        except (OSError,RuntimeError,ValueError,TimeoutError,psutil.Error): return None
    return probe


def register(main): main.add_command(services)
