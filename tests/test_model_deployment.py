import asyncio
from copy import deepcopy
import json
import sys

import pytest

from demiflow.operator_llm.model import parse_prompt_model
from demiflow.services.deployment import deployment_config, logical_base, DeploymentService
from demiflow.services import prepare as preparation


def config(tmp_path, **changes):
    weights=tmp_path/'weights'; weights.mkdir(exist_ok=True)
    return deployment_config(dict(profile='pplx',weights=str(weights),revision='r1',gpus=[0],
        python=sys.executable,directory=str(tmp_path/'services'),**changes))


def test_jev_pipeline_uses_owned_single_sequence_engine_only(tmp_path):
    from demiflow.services.profiles import build_service
    cfg=deployment_config({**config(tmp_path),'profile':'autotrust-jev'})
    manifest={'python':sys.executable,'ports':[21001,21002],'id':'fixture',
              'model_contract':{},'runtime':{},'adapter_version':'fixture'}
    for maximum in (1,64):
        spec=deployment_config({**cfg,'options':{'max_num_seqs':maximum}})
        engine,wrapper=build_service('fixture',spec,manifest).services
        assert engine.command[engine.command.index('--max-num-seqs')+1]==str(maximum)
        assert ('--backend-single-sequence' in wrapper.command)==(maximum==1)
    with pytest.raises(ValueError,match='pipelining'):
        deployment_config({**cfg,'options':{'max_num_seqs':1},'batch_evidence':'/proof.json','batch_max_size':2})
    with pytest.raises(ValueError,match='pipelining'):
        build_service('fixture',{**cfg,'options':{'max_num_seqs':1}},manifest,validation_batch_size=2)


def test_declaration_is_inert_and_identity_excludes_placement(tmp_path,monkeypatch):
    cfg=config(tmp_path)
    monkeypatch.setattr(preparation,'prepare_deployment',lambda *a,**k: pytest.fail('implicit prepare'))
    one=parse_prompt_model({'name':'fixture','deployment':cfg},label='model')
    other={**cfg,'gpus':[1],'request_concurrency':32,'directory':str(tmp_path/'elsewhere')}
    two=parse_prompt_model({'name':'fixture','deployment':other},label='model')
    assert one.transport=='systemone' and one.base_url==two.base_url
    assert not (tmp_path/'services').exists()
    assert logical_base('fixture',{**cfg,'revision':'r2'})!=one.base_url
    assert DeploymentService('fixture',cfg).bind(one).owner is None
    with pytest.raises(TypeError): one.deployment['options']['max_length']=1
    import cloudpickle
    assert cloudpickle.loads(cloudpickle.dumps(one))==one
    with pytest.raises(ValueError,match='endpoint'):
        parse_prompt_model({'name':'fixture','base_url':'http://localhost:1/v1','deployment':cfg},label='model')
    with pytest.raises(ValueError,match='supported'):
        deployment_config({**cfg,'options':{'max_num_seqs':2}})


@pytest.mark.parametrize('variable', ['CUBLAS_WORKSPACE_CONFIG', 'CUBLASLT_WORKSPACE_SIZE',
                                     'TORCH_CUBLASLT_UNIFIED_WORKSPACE', 'FLASHINFER_GDN_ENABLE'])
def test_numeric_environment_changes_quality_identity(tmp_path, monkeypatch, variable):
    from demiflow.services.profiles import build_service
    cfg=config(tmp_path)
    manifest={'python':sys.executable,'ports':[21001],'id':'fixture',
              'model_contract':{},'runtime':{},'adapter_version':'fixture'}
    def identity():
        command=build_service('fixture',cfg,manifest).command
        return json.loads(command[command.index('--identity')+1])
    monkeypatch.delenv(variable, raising=False)
    original=identity()
    monkeypatch.setenv(variable, '1')
    assert identity()!=original
    monkeypatch.delenv(variable)
    assert identity()==original


def test_explicit_prepare_is_idempotent_and_reserves_distinct_ports(tmp_path,monkeypatch):
    versions={'python':'3.12','packages':{'pplx-decider':'0.3.0','torch':'fixture'}}
    monkeypatch.setattr(preparation,'_versions',lambda *a,**k:deepcopy(versions))
    cfg=config(tmp_path)
    first=preparation.prepare_deployment('fixture',cfg)
    assert preparation.prepare_deployment('fixture',cfg)==first
    second=preparation.prepare_deployment('fixture',{**cfg,'revision':'r2'})
    assert set(first['ports']).isdisjoint(second['ports'])
    assert first['batch_verified'] is False
    versions['packages']['torch']='changed'
    with pytest.raises(ValueError,match='changed'):
        preparation.prepare_deployment('fixture',cfg)


def test_prepared_model_metadata_change_invalidates_identity(tmp_path,monkeypatch):
    monkeypatch.setattr(preparation,'_versions',lambda *a,**k:{'packages':{'pplx-decider':'0.3.0'}})
    cfg=config(tmp_path)
    preparation.prepare_deployment('fixture',cfg)
    from pathlib import Path
    (Path(cfg['weights'])/'config.json').write_text('{"changed":true}')
    with pytest.raises(RuntimeError,match='artifacts changed'):
        preparation.read_prepared('fixture',cfg)


@pytest.mark.asyncio
async def test_new_action_waits_for_cancelled_callers_cleanup(tmp_path,monkeypatch):
    from demiflow.services import profiles
    from test_service_contracts import FakeService
    cfg=config(tmp_path); events=[]; closing=asyncio.Event(); release=asyncio.Event()
    monkeypatch.setattr(preparation,'read_prepared',lambda *a:{'python':sys.executable,'runtime':{}})
    monkeypatch.setattr(preparation,'_versions',lambda *a:{})
    class SlowClose(FakeService):
        def bind(self,model=None):
            lease=super().bind(model); original=lease.aclose
            async def close():
                closing.set(); await release.wait(); await original()
            lease.aclose=close
            return lease
    monkeypatch.setattr(profiles,'build_service',lambda *a:SlowClose('model',events))
    one=DeploymentService('fixture',cfg).bind(); await one.ensure_ready()
    task=asyncio.create_task(one.aclose()); await asyncio.wait_for(closing.wait(),3); task.cancel()
    with pytest.raises(asyncio.CancelledError): await task
    two=DeploymentService('fixture',cfg).bind(); start=asyncio.create_task(two.ensure_ready())
    await asyncio.sleep(.03)
    assert not start.done() and events==[('start','model')]
    release.set(); await start; await one.aclose(); await two.aclose()
    assert events==[('start','model'),('stop','model'),('start','model'),('stop','model')]


@pytest.mark.asyncio
async def test_action_leases_share_one_owner_and_close_on_last_user(tmp_path,monkeypatch):
    from demiflow.services import profiles
    from test_service_contracts import FakeService
    cfg=config(tmp_path); events=[]
    monkeypatch.setattr(preparation,'read_prepared',lambda *a:{'python':sys.executable,'runtime':{}})
    monkeypatch.setattr(preparation,'_versions',lambda *a:{})
    monkeypatch.setattr(profiles,'build_service',lambda *a:FakeService('model',events))
    one=DeploymentService('fixture',cfg).bind(); two=DeploymentService('fixture',cfg).bind()
    await asyncio.gather(*(one.ensure_ready() for _ in range(5)),two.ensure_ready())
    assert one.owner is two.owner
    await one.aclose()
    assert events==[('start','model')]
    await two.aclose()
    assert events==[('start','model'),('stop','model')]


def test_jev_recipe_preserves_required_settings(tmp_path):
    from demiflow.services.profiles import build_service
    cfg=config(tmp_path)
    cfg=deployment_config({**cfg,'profile':'autotrust-jev','options':{'max_model_len':65536}})
    manifest={'python':sys.executable,'ports':[20001,20002],'id':'fixture',
              'model_contract':{},'runtime':{},'adapter_version':'fixture'}
    group=build_service('fixture',cfg,manifest)
    engine,wrapper=group.services
    command=list(engine.command)
    assert command[command.index('--max-num-batched-tokens')+1]=='65536'
    assert '--no-enable-prefix-caching' in command and '--no-enable-chunked-prefill' in command
    assert engine.env['STARTLUX_PREFIX_BIDIR']=='1'
    assert engine.gpus==(0,) and wrapper.gpus==()


def test_preparation_subprocess_has_real_output_and_time_limits(tmp_path):
    from demiflow.services.preparation_io import run_bounded
    assert run_bounded([sys.executable,'-c','print("ready")'],timeout_s=2)==b'ready\n'
    with pytest.raises(ValueError,match='output'):
        run_bounded([sys.executable,'-c','print("x"*1000000)'],timeout_s=2,output_bytes=1024)
    with pytest.raises(TimeoutError):
        run_bounded([sys.executable,'-c','import time;time.sleep(60)'],timeout_s=.05)


def test_services_cli_reads_the_same_complete_prompt_configuration(tmp_path,monkeypatch):
    from click.testing import CliRunner
    from demiflow.services.cli import services
    from test_prompt_systemone import PACK
    import yaml
    raw=yaml.safe_load(PACK)
    raw['prompts']['match']['model']={'name':'fixture','deployment':config(tmp_path)}
    path=tmp_path/'prompt.yaml'; path.write_text(yaml.safe_dump(raw))
    monkeypatch.setattr(preparation,'_versions',lambda *a,**k:{'python':'fixture',
        'packages':{'pplx-decider':'0.3.0','torch':'fixture'}})
    from demiflow.services import cli
    monkeypatch.setattr(cli,'_versions',preparation._versions)
    runner=CliRunner()
    for action in ('prepare','doctor','status','logs'):
        result=runner.invoke(services,[action,'--config',str(path)])
        assert result.exit_code==0,(result.output,result.exception)
        assert json.loads(result.output)
    result=runner.invoke(services,['start','--config',str(path)])
    assert result.exit_code!=0 and 'persistent' in result.output
    from demiflow.operator_llm.runtime import OperatorLLMRuntime,InProcessOperatorLLMCoordinator
    from demiflow.operator_llm.parser import parse_prompt_pack
    runtime=OperatorLLMRuntime(parse_prompt_pack(path.read_text()),InProcessOperatorLLMCoordinator())
    with pytest.raises(ValueError,match='map_prompt_async'):
        runtime.call('match',{'payload':'no per-row model loading'})


def test_failed_install_is_not_published_and_can_be_prepared_again(tmp_path,monkeypatch):
    from pathlib import Path
    cfg=config(tmp_path); cfg.pop('python')
    monkeypatch.setattr(preparation.shutil,'which',lambda _: '/fake/uv')
    versions={'python':'3.12','packages':{'pplx-decider':'0.3.0','torch':'fixture'}}
    monkeypatch.setattr(preparation,'_versions',lambda *a,**k:versions)
    fail=True; installs=0
    def run(command,**kwargs):
        nonlocal installs
        if command[0]=='/fake/uv':
            target=Path(command[-1])/'bin/python'; target.parent.mkdir(parents=True)
            target.write_text('fixture interpreter; never executed')
        else:
            installs+=1
            if fail: raise RuntimeError('installer failed')
        return b''
    monkeypatch.setattr(preparation,'run_bounded',run)
    with pytest.raises(RuntimeError,match='installer failed'):
        preparation.prepare_deployment('fixture',cfg)
    assert not list((tmp_path/'services').rglob('manifest.json'))
    assert not list((tmp_path/'services').rglob('ready.json'))
    assert not list((tmp_path/'services').rglob('venv'))
    fail=False
    manifest=preparation.prepare_deployment('fixture',cfg)
    assert manifest['runtime']==versions and installs==2


@pytest.mark.asyncio
async def test_runtime_reuses_declaration_but_retains_each_request_admission(tmp_path, monkeypatch):
    from contextlib import asynccontextmanager
    from demiflow.operator_llm.runtime import AsyncOperatorLLMRuntime, InProcessOperatorLLMCoordinator
    from demiflow.services import deployment
    model = parse_prompt_model({'name':'fixture','deployment':config(tmp_path)}, label='model')
    events = []
    class Lease:
        base_url = 'http://fixture/v1'
        async def ensure_ready(self):
            events.append('ready')
            await asyncio.sleep(0)
        @asynccontextmanager
        async def request_slot(self):
            events.append('enter')
            try: yield
            finally: events.append('exit')
        async def aclose(self): events.append('close')
    class Service:
        def __init__(self, *args): events.append('construct')
        def bind(self, model): events.append('bind'); return Lease()
    monkeypatch.setattr(deployment, 'DeploymentService', Service)
    runtime = AsyncOperatorLLMRuntime(None, InProcessOperatorLLMCoordinator())
    async def call():
        async with runtime.transport_context(model) as url:
            assert url == 'http://fixture/v1'
            await asyncio.sleep(0)
    try:
        assert not events
        await asyncio.gather(*(call() for _ in range(16)))
        runtime._service = object()
        with pytest.raises(ValueError, match='cannot be combined'):
            await call()
    finally:
        await runtime.aclose()
    assert events.count('construct') == events.count('bind') == events.count('close') == 1
    assert events.count('ready') == events.count('enter') == events.count('exit') == 16


@pytest.mark.asyncio
@pytest.mark.parametrize('auto_replay',[False,True])
async def test_managed_prompt_uses_physical_url_but_replays_without_a_service(tmp_path,monkeypatch,auto_replay):
    import httpx
    from contextlib import asynccontextmanager
    from demiflow.operator_llm.runtime import AsyncOperatorLLMRuntime, InProcessOperatorLLMCoordinator
    from demiflow.operator_llm.parser import parse_prompt_pack
    from test_prompt_systemone import PACK
    import yaml
    raw=yaml.safe_load(PACK); cfg=config(tmp_path,**({'performance':'optimized'} if auto_replay else {}))
    raw['prompts']['match']['model']={'name':'fixture','deployment':cfg}
    pack=parse_prompt_pack(yaml.safe_dump(raw)); calls=[]; events=[]
    class Lease:
        base_url='http://physical.invalid:1234/v1'
        async def ensure_ready(self): events.append('start')
        @asynccontextmanager
        async def request_slot(self): yield
        async def aclose(self): events.append('stop')
    monkeypatch.setattr(DeploymentService,'bind',lambda *a:Lease())
    def handle(request):
        calls.append(str(request.url))
        return httpx.Response(200,json={'answers':{'match':{'type':'noul','noul':.8}}})
    from demiflow.operator_llm import runtime as runtime_module
    from demiflow.operator_llm.client import AsyncOperatorLLMClient
    def create(model,options=None):
        client=AsyncOperatorLLMClient(model,options)
        client.client=httpx.AsyncClient(transport=httpx.MockTransport(handle))
        client.configure_capacity=lambda _:None
        return client
    monkeypatch.setattr(runtime_module,'create_async_operator_llm_client',create)
    options={'journal_dir':str(tmp_path/'calls')}
    for iteration in range(2):
        if iteration:
            if auto_replay:
                raw['prompts']['match']['model']['deployment']={**cfg,'performance':'auto'}
                pack=parse_prompt_pack(yaml.safe_dump(raw))
            monkeypatch.setattr(DeploymentService,'bind',lambda *a:pytest.fail('replay started a service'))
        runtime=AsyncOperatorLLMRuntime(pack,InProcessOperatorLLMCoordinator(),options)
        try:
            answer=await runtime.call('match',{'payload':{'text':'hello'}})
            assert answer['result']['noul']==.8
        finally: await runtime.aclose()
    assert calls==['http://physical.invalid:1234/v1/systemone'] and events==['start','stop']


@pytest.mark.parametrize('passed',[False,True])
def test_single_benchmark_command_owns_lifecycle_and_never_requests_batch(tmp_path,monkeypatch,passed):
    from click.testing import CliRunner
    from demiflow.services import cli,quality,profiles
    from types import SimpleNamespace
    cfg=deployment_config({**config(tmp_path),'profile':'amos'})
    config_path=tmp_path/'prompt.yaml';config_path.write_text('fixture')
    cases_path=tmp_path/'cases.jsonl';cases_path.write_text('fixture')
    report_path=tmp_path/'report.json';events=[]
    class Owner:
        base_url='http://fixture/v1'
        async def ensure_ready(self): events.append('start')
        async def aclose(self): events.append('close')
    async def benchmark(*args,**kwargs):
        assert kwargs['max_size']==1 and kwargs['probability_atol']==0
        events.append('benchmark')
        return {'passed':passed,'schema_version':'demiflow_single_performance_v1'}
    monkeypatch.setattr(cli,'declarations',lambda _: [('fixture',cfg)])
    monkeypatch.setattr(cli,'read_prepared',lambda *args: {'python':'/python','runtime':{}})
    monkeypatch.setattr(cli,'_versions',lambda *args: {})
    monkeypatch.setattr(cli,'owned_memory_probe',lambda _: lambda:None)
    monkeypatch.setattr(quality,'load_cases',lambda _: [])
    monkeypatch.setattr(quality,'benchmark',benchmark)
    monkeypatch.setattr(profiles,'build_service',lambda *args: SimpleNamespace(bind=lambda:Owner()))
    result=CliRunner().invoke(cli.services,['benchmark','--config',str(config_path),'--cases',str(cases_path),
                                          '--report',str(report_path),'--parallel','2'])
    assert (result.exit_code==0)==passed,(result.output,result.exception)
    assert events==['start','benchmark','close']
    assert json.loads(report_path.read_text())['passed']==passed
