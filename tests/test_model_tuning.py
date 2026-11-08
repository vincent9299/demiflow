import asyncio
from copy import deepcopy
import json
from pathlib import Path
import sys
import threading

import httpx
import pytest

from demiflow.services import tuning, tuning_runner, prepare, profiles
from demiflow.services.deployment import deployment_config, fingerprint, logical_base


def config(tmp_path,profile='pplx'):
    return deployment_config(dict(profile=profile,weights=str(tmp_path/'weights'),revision='r1',
        gpus=[0],python=sys.executable,directory=str(tmp_path/'services'),performance='auto'))


def case(index):
    return dict(model='test',state=str(index),questions={'q':dict(type='choice',instructions='Choose',
        criteria={'a':'First','b':'Second'})})


def cases_file(tmp_path):
    path=tmp_path/'cases.jsonl';path.write_text(''.join(json.dumps(case(i))+'\n' for i in range(400)))
    return path


def hardware():return [dict(uuid='GPU-test',name='Test GPU',driver='1',memory_mib=81920)]


def manifest(cfg):
    from demiflow.services.deployment import semantic_contract
    return dict(id='test',python=sys.executable,model_contract=semantic_contract('test',cfg),
        runtime={'test':'1'},adapter_version='source-v1',weights_signature='weights-v1',ports=[21001])


def metric(rows=200,rps=20,p95=100):
    return dict(rows=rows,wall_s=rows/rps,rps=rps,p50_ms=p95/2,p95_ms=p95,mismatches=0)


def record(execution,budget,rps=20,p95=100):
    state=dict(failed=None,mismatches=0,streams=execution['streams'],
               graph_disabled=execution['mode']=='native',hits=0 if execution['mode']=='native' else 200,
               cache_bytes=0,allocator_peak_bytes=1024,entries=0)
    return dict(execution=execution,passed=True,rounds=[metric(rps=rps,p95=p95) for _ in range(budget['rounds'])],
        warmup=[metric() for _ in range(budget['warmup_rounds'])],execution_status=state)


def report(cfg):
    budget=tuning.limits();values=tuning.candidates(cfg,hardware(),budget);exe=values[1]
    baseline=record(values[0],budget,rps=1)
    baseline['reference']=dict(rows=400,outputs_sha256='d'*64)
    selected=record(exe,budget);validation=deepcopy(selected)
    validation.update(rows=200,mismatches=0,rounds=[metric()])
    value=dict(schema_version=tuning.SCHEMA,quality_policy=tuning.POLICY,passed=True,
        context=tuning.context(manifest(cfg),cfg,hardware()),limits=budget,
        search_count=200,holdout_count=200,cases_sha256='c'*64,search_sha256='a'*64,holdout_sha256='b'*64,
        candidates=[baseline,selected],selected=selected,validation=validation)
    value['sha256']=fingerprint(value)
    return value


@pytest.mark.parametrize('profile',tuning.PROFILES)
def test_auto_is_inert_and_preserves_optimized_logical_identity(tmp_path,profile):
    auto=config(tmp_path,profile)
    optimized=deployment_config({**auto,'performance':'optimized'})
    assert logical_base('test',auto)==logical_base('test',optimized)
    assert not (tmp_path/'services').exists()
    for value in tuning.candidates(auto,hardware(),tuning.limits()):
        resolved=tuning.execution_config(auto,value)
        assert resolved['options']['cuda_graphs']['verify_every']==auto['options']['cuda_graphs']['verify_every']
        assert resolved['options']['cuda_graphs']['max_input_tokens']==auto['options']['cuda_graphs']['max_input_tokens']


def test_candidates_cannot_change_semantics_or_exceed_budgets(tmp_path):
    cfg=config(tmp_path);exe=tuning.candidates(cfg,hardware(),tuning.limits())[1]
    for key,value in [('verify_every',1024),('max_input_tokens',256),('dtype','float16'),
                      ('streams',5),('cpu_threads',9),('memory_limit_mib',70000),('request_concurrency',9)]:
        with pytest.raises(ValueError):tuning.execution_config(cfg,{**exe,key:value})
    native={**exe,'mode':'native','streams':2}
    with pytest.raises(ValueError,match='serial'):tuning.execution_config(cfg,native)


def test_selection_uses_every_round_and_prefers_shorter_tail_near_saturation(tmp_path):
    cfg=config(tmp_path);budget=tuning.limits();values=tuning.candidates(cfg,hardware(),budget)
    slow=record(values[1],budget,rps=24,p95=400)
    balanced=record(values[2],budget,rps=23.9,p95=180)
    spiky=record(values[3],budget,rps=30,p95=100)
    spiky['rounds'][1]['rps']=10
    bad=record(values[4],budget,rps=40,p95=10);bad['rounds'][0]['mismatches']=1
    assert tuning.rank([slow,balanced,spiky,bad],budget)==[balanced,slow,spiky]


@pytest.mark.parametrize('width',[3,4])
def test_known_unsafe_amos_width_cannot_be_published_even_with_passing_metrics(tmp_path,width):
    cfg=config(tmp_path,'amos')
    assert {x['streams'] for x in tuning.candidates(cfg,hardware(),tuning.limits())}=={1,2}
    value=report(cfg)
    for row in (value['selected'],value['validation']):
        row['execution']['streams']=width
        row['execution_status']['streams']=width
    value.pop('sha256');value['sha256']=fingerprint(value)
    with pytest.raises(ValueError,match='not admitted'):
        tuning.validated_report(value,value['context'],cfg)


@pytest.mark.parametrize('mutate',[
    lambda x:x['context'].update(adapter_version='other'),
    lambda x:x['validation'].update(mismatches=1),
    lambda x:x['validation']['rounds'][0].update(p95_ms=10000),
    lambda x:x.update(holdout_sha256=x['search_sha256']),
    lambda x:x['selected']['execution'].update(streams=99),
    lambda x:x['selected']['rounds'].pop(),
    lambda x:x['selected']['warmup'][0].update(mismatches=1),
    lambda x:x['validation']['execution_status'].update(graph_disabled=True),
    lambda x:x['validation']['execution_status'].update(cache_bytes=5*1024**3),
    lambda x:x['validation']['execution_status'].update(allocator_peak_bytes=80*1024**3),
    lambda x:x['candidates'][0].pop('reference'),
    lambda x:x['candidates'][0]['execution_status'].update(hits=1),
])
def test_evidence_rejects_quality_identity_coverage_and_budget_failures(tmp_path,mutate):
    cfg=config(tmp_path);value=report(cfg);expected=deepcopy(value['context'])
    mutate(value);value.pop('sha256');value['sha256']=fingerprint(value)
    with pytest.raises(ValueError):tuning.validated_report(value,expected,cfg)


def test_frozen_execution_changes_command_not_logical_contract(tmp_path):
    cfg=config(tmp_path);value=report(cfg)
    resolved=tuning.validated_report(value,value['context'],cfg)
    prepared=manifest(cfg);prepared['_execution']=value['selected']['execution']
    service=profiles.build_service('test',cfg,prepared)
    identity=json.loads(service.command[service.command.index('--identity')+1])
    assert identity['model_contract']==prepared['model_contract']
    assert identity['execution_configuration']==value['selected']['execution']
    assert service.request_concurrency==resolved['request_concurrency']
    with pytest.raises(ValueError,match='validated frozen'):profiles.build_service('test',cfg,manifest(cfg))


def test_case_split_rejects_duplicates_and_wide_rows(tmp_path):
    path=cases_file(tmp_path);search,holdout=tuning_runner.load_cases(path,'test')
    assert len(search)==len(holdout)==200
    assert not set(map(fingerprint,search))&set(map(fingerprint,holdout))
    path.write_text(path.read_text()+json.dumps(case(0))+'\n')
    with pytest.raises(ValueError,match='distinct'):tuning_runner.load_cases(path,'test')
    path.write_text(' '*65537)
    with pytest.raises(ValueError,match='64 KiB'):tuning_runner.load_cases(path,'test')


def test_request_digest_preserves_criteria_order():
    first=case(1);second=deepcopy(first)
    second['questions']['q']['criteria']={'b':'Second','a':'First'}
    assert fingerprint(first)==fingerprint(second)
    assert tuning_runner.request_digest(first)!=tuning_runner.request_digest(second)


def test_busy_gpu_is_rejected_without_launching_or_stopping(monkeypatch):
    from demiflow.services import preparation_io
    monkeypatch.setattr(preparation_io,'run_bounded',lambda *a,**kw:b'12345\n')
    with pytest.raises(ValueError,match='idle GPUs'):tuning_runner.idle([0])


def test_auto_preparation_is_cpu_only_and_reuse_checks_hardware(tmp_path,monkeypatch):
    cfg=config(tmp_path);Path(cfg['weights']).mkdir()
    monkeypatch.setattr(prepare,'_versions',lambda *a:{'packages':{'pplx-decider':'0.3.0'}})
    monkeypatch.setattr(tuning,'hardware_identity',lambda *a:pytest.fail('prepare queried a GPU'))
    prepared=prepare.prepare_deployment('test',cfg)
    assert prepare.prepare_deployment('test',cfg)==prepared
    with pytest.raises(ValueError,match='requires services tune'):prepare.read_prepared('test',cfg)
    value=report(cfg);value['context']=tuning.context(prepared,cfg,hardware())
    value.pop('sha256');value['sha256']=fingerprint(value)
    prepare._write(tuning.report_path('test',cfg),value)
    monkeypatch.setattr(tuning,'hardware_identity',lambda *a:hardware())
    loaded=prepare.read_prepared('test',cfg)
    assert loaded['_execution']==value['selected']['execution']
    monkeypatch.setattr(tuning,'hardware_identity',lambda *a:[{**hardware()[0],'driver':'changed'}])
    with pytest.raises(ValueError,match='stale'):prepare.read_prepared('test',cfg)


def test_response_validation_is_exact_and_rejects_invalid_native_values():
    sample=case(1)
    value=dict(model='test',answers={'q':dict(type='choice',choice='a',probabilities={'a':.75,'b':.25})},
               usage={'input_tokens':3,'output_tokens':0})
    expected=tuning_runner.response_digest(sample,value)
    changed=deepcopy(value);changed['answers']['q']['probabilities']={'a':.75000001,'b':.24999999}
    assert tuning_runner.response_digest(sample,changed)!=expected
    value['answers']['q']['probabilities']['a']=2
    with pytest.raises(ValueError,match='invalid probabilities'):tuning_runner.response_digest(sample,value)


@pytest.mark.asyncio
async def test_http_measure_has_bounded_workers_and_response_bytes():
    active=peak=0
    async def handler(request):
        nonlocal active,peak
        active+=1;peak=max(active,peak)
        try:
            await asyncio.sleep(.002)
            return httpx.Response(200,json=dict(model='test',answers={'q':dict(type='choice',choice='a',
                probabilities={'a':1.,'b':0.})},usage={'input_tokens':1}))
        finally:active-=1
    async with httpx.AsyncClient(transport=httpx.MockTransport(handler),base_url='http://test/') as client:
        metrics,gold=await tuning_runner.measure(client,[case(i) for i in range(20)],3)
        assert metrics['rows']==20 and len(gold)==20 and peak==3 and active==0
    async with httpx.AsyncClient(transport=httpx.MockTransport(lambda r:httpx.Response(200,content=b'x'*262145)),
                                 base_url='http://test/') as client:
        with pytest.raises(ValueError,match='256 KiB'):await tuning_runner.exchange(client,'models')


@pytest.mark.asyncio
async def test_http_failure_preserves_bounded_backend_detail():
    async with httpx.AsyncClient(transport=httpx.MockTransport(lambda r:httpx.Response(
            503,json={'error':{'detail':'CUDA graph replay failed'}})),base_url='http://test/') as client:
        with pytest.raises(ValueError,match='HTTP 503.*CUDA graph replay failed'):
            await tuning_runner.exchange(client,'models')
    async with httpx.AsyncClient(transport=httpx.MockTransport(lambda r:httpx.Response(
            503,content=b'x'*262145)),base_url='http://test/') as client:
        with pytest.raises(ValueError,match='256 KiB'):
            await tuning_runner.exchange(client,'models')


@pytest.mark.asyncio
async def test_real_http_tuning_reuses_evidence_and_keeps_previous_on_holdout_failure(tmp_path,monkeypatch):
    from demiflow.services.model_worker import make_server
    cfg=config(tmp_path);path=cases_file(tmp_path);prepared=manifest(cfg)
    tuning.report_path('test',cfg).parent.mkdir(parents=True)
    monkeypatch.setattr(prepare,'read_prepared',lambda *a,**kw:deepcopy(prepared))
    monkeypatch.setattr(prepare,'_versions',lambda *a:prepared['runtime'])
    monkeypatch.setattr(tuning,'hardware_identity',lambda *a:hardware())
    monkeypatch.setattr(tuning_runner,'idle',lambda *a:None)
    starts=[];stops=[];fail_holdout=False;block_start=False;entered=asyncio.Event()
    _,held=tuning_runner.load_cases(path,'test');held_states={x['state'] for x in held}
    class Adapter:
        def __init__(self,execution,holdout_run):
            self.execution=execution;self.parallel=execution['streams'];self.calls=0;self.holdout_run=holdout_run
        def single(self,value):
            self.calls+=1
            wrong=fail_holdout and self.holdout_run and value['state'] in held_states
            return dict(answers={'q':dict(type='choice',choice='b' if wrong else 'a',
                probabilities={'a':0. if wrong else 1.,'b':1. if wrong else 0.})},usage={'input_tokens':1})
        def execution_status(self):
            return dict(record(self.execution,tuning.limits())['execution_status'],
                        hits=0 if self.execution['mode']=='native' else self.calls,native=self.calls,checks=0)
    class Owner:
        def __init__(self,manifest):
            self.manifest=manifest;self.server=None
        def bind(self):return self
        async def ensure_ready(self):
            identity={key:self.manifest[key] for key in ('model_contract','runtime','adapter_version','weights_signature')}
            identity.update(hardware={'kind':'cuda'},execution_configuration=self.manifest['_execution'])
            exe=self.manifest['_execution'];adapter=Adapter(exe,Path(self.manifest['_log_directory']).name=='holdout')
            self.server=make_server(adapter,'test',0,exe['request_concurrency'],identity=identity)
            self.base_url=f'http://127.0.0.1:{self.server.server_address[1]}/v1'
            self.thread=threading.Thread(target=lambda:self.server.serve_forever(poll_interval=.01));self.thread.start()
            starts.append(self)
            if block_start:
                entered.set();await asyncio.Event().wait()
        async def aclose(self):
            if self.server:
                await asyncio.to_thread(self.server.shutdown);self.server.server_close();self.thread.join(2)
                assert not self.thread.is_alive();stops.append(self)
    monkeypatch.setattr(profiles,'build_service',lambda name,config,manifest:Owner(manifest))
    budget=dict(max_candidates=2,timeout_s=180,warmup_rounds=1)
    first=await tuning_runner.tune('test',cfg,path,options=budget)
    assert first['report']['passed'] and first['reused'] is False
    tuning.validated_report(first['report'],first['report']['context'],cfg)
    assert len(starts)==len(stops)==3
    again=await tuning_runner.tune('test',cfg,path,options=budget)
    assert again['reused'] and len(starts)==3
    frozen=Path(first['path']).read_bytes();fail_holdout=True
    with pytest.raises(ValueError,match='holdout'):
        await tuning_runner.tune('test',cfg,path,options=budget,force=True)
    assert Path(first['path']).read_bytes()==frozen and len(starts)==len(stops)==6
    block_start=True
    task=asyncio.create_task(tuning_runner.tune('test',cfg,path,options=budget,force=True))
    await asyncio.wait_for(entered.wait(),5);task.cancel()
    with pytest.raises(asyncio.CancelledError):await task
    assert Path(first['path']).read_bytes()==frozen and len(starts)==len(stops)==7
