"""Independent replica selection reuses lifecycle and row HTTP execution."""
import asyncio
from contextlib import asynccontextmanager
from types import SimpleNamespace

import pytest

from demiflow.services.replicas import ReplicaService
from demiflow.services.contracts import ModelServiceError


class Service:
    def __init__(self,index,events,fail=False):
        self.index,self.events,self.fail=index,events,fail
    def bind(self,model=None):
        parent=self
        class Lease:
            base_url=f'http://replica-{parent.index}/v1'
            async def ensure_ready(self):
                if ('start',parent.index) not in parent.events: parent.events.append(('start',parent.index))
                if parent.fail: raise ModelServiceError('failed')
            @asynccontextmanager
            async def request_slot(self):
                parent.events.append(('slot',parent.index)); yield
            async def aclose(self): parent.events.append(('close',parent.index))
            async def rollback(self): parent.events.append(('rollback',parent.index))
        return Lease()


@pytest.mark.asyncio
async def test_legacy_slot_context_values_are_not_treated_as_urls():
    from demiflow.services.contracts import service_request_slot
    class Legacy:
        @asynccontextmanager
        async def request_slot(self): yield self
    async with service_request_slot(Legacy()) as endpoint:
        assert endpoint is None


@pytest.mark.asyncio
async def test_replicas_overlap_without_sharing_or_overwriting_endpoints():
    events=[]; pool=ReplicaService([Service(0,events),Service(1,events)]).bind()
    await pool.ensure_ready(); entered=asyncio.Event(); release=asyncio.Event(); active=set(); seen=[]
    async def call():
        async with pool.request_slot() as url:
            assert url not in active
            active.add(url); seen.append(url)
            if len(active)==2: entered.set()
            await release.wait(); assert url in active; active.remove(url)
    tasks=[asyncio.create_task(call()) for _ in range(4)]
    await asyncio.wait_for(entered.wait(),1)
    assert len(seen)==2 and len(active)==2
    tasks[-1].cancel(); release.set()
    results=await asyncio.gather(*tasks,return_exceptions=True)
    assert sum(isinstance(x,asyncio.CancelledError) for x in results)==1
    assert len(seen)==3 and len(pool.available)==2
    await pool.aclose()


@pytest.mark.asyncio
async def test_replica_close_wakes_waiters_and_failure_rolls_back_all():
    events=[]; pool=ReplicaService([Service(0,events),Service(1,events)]).bind()
    await pool.ensure_ready()
    async with pool.request_slot(),pool.request_slot():
        async def waiting():
            async with pool.request_slot(): pytest.fail('closed pool admitted a waiter')
        waiter=asyncio.create_task(waiting()); await asyncio.sleep(0)
        await pool.aclose()
        with pytest.raises(ModelServiceError,match='closed'): await asyncio.wait_for(waiter,1)
    events=[]; pool=ReplicaService([Service(0,events),Service(1,events,fail=True)]).bind()
    with pytest.raises(ModelServiceError,match='failed'): await pool.ensure_ready()
    assert events[-2:]==[('rollback',1),('rollback',0)]
    await pool.aclose()


def test_replica_configuration_ports_components_and_model_identity(tmp_path,monkeypatch):
    from test_model_deployment import config
    from demiflow.services import prepare,profiles
    from demiflow.services.deployment import deployment_config,logical_base
    from demiflow.services.cli import component_names
    one=config(tmp_path)
    two=deployment_config({**one,'replicas':2,'gpus':[0,1]})
    assert logical_base('fixture',one)==logical_base('fixture',two)
    monkeypatch.setattr(prepare,'_versions',lambda *a,**k:{'packages':{'pplx-decider':'0.3.0'}})
    manifest=prepare.prepare_deployment('fixture',two)
    assert len(manifest['ports'])==2 and prepare.read_prepared('fixture',two)==manifest
    pool=profiles.build_service('fixture',two,manifest)
    assert [service.gpus for service in pool.services]==[(0,),(1,)]
    assert all(service.request_concurrency==1 for service in pool.services)
    assert len(set(component_names('fixture',two)))==2
    with pytest.raises(ValueError,match='batch evidence'):
        deployment_config({**two,'batch_evidence':'/fixture/report.json','batch_max_size':8})
    with pytest.raises(ValueError,match='divide'):
        deployment_config({**two,'replicas':3})
    jev_one=deployment_config({**one,'profile':'autotrust-jev'})
    jev_two=deployment_config({**two,'profile':'autotrust-jev'})
    assert logical_base('fixture',jev_one)==logical_base('fixture',jev_two)
    manifest={**manifest,'ports':[21001,21002,21003,21004]}
    group=profiles.build_service('fixture',jev_two,manifest)
    assert len(group.services)==2
    for replica,index in zip(group.services,range(2)):
        engine,wrapper=replica.services
        assert engine.gpus==(index,) and wrapper.gpus==()
        assert engine.command[engine.command.index('--tensor-parallel-size')+1]=='1'
    assert len(component_names('fixture',jev_two))==4


def test_replica_batch_is_rejected_before_any_service_start(tmp_path):
    from test_model_deployment import config
    from demiflow.operator_llm.model import parse_prompt_model
    from demiflow.operator_llm.client import AsyncOperatorLLMClient
    model=parse_prompt_model({'name':'fixture','deployment':{**config(tmp_path),'gpus':[0,1],'replicas':2}},label='model')
    with pytest.raises(ValueError,match='independent single'):
        AsyncOperatorLLMClient(model,{'systemone_batch':{'max_size':2}})


@pytest.mark.asyncio
async def test_operator_uses_slot_endpoint_and_journals_remain_row_local(tmp_path,monkeypatch):
    import httpx,yaml
    from test_prompt_systemone import PACK
    from demiflow.operator_llm.parser import parse_prompt_pack
    from demiflow.operator_llm.runtime import AsyncOperatorLLMRuntime,InProcessOperatorLLMCoordinator
    from demiflow.operator_llm import runtime as module
    from demiflow.operator_llm.client import AsyncOperatorLLMClient
    from demiflow.services.deployment import DeploymentService
    from test_model_deployment import config
    raw=yaml.safe_load(PACK)
    raw['prompts']['match']['model']={'name':'fixture','deployment':{**config(tmp_path),'replicas':2,'gpus':[0,1]}}
    events=[]; pool=ReplicaService([Service(0,events),Service(1,events)]).bind()
    monkeypatch.setattr(DeploymentService,'bind',lambda *a:pool)
    urls=[]; active=peak=0
    async def handle(request):
        nonlocal active,peak
        active+=1; peak=max(peak,active); urls.append(str(request.url)); await asyncio.sleep(.03); active-=1
        return httpx.Response(200,json={'answers':{'match':{'type':'noul','noul':.75}}})
    def create(model,options):
        client=AsyncOperatorLLMClient(model,options)
        client.client=httpx.AsyncClient(transport=httpx.MockTransport(handle)); client.configure_capacity=lambda _:None
        return client
    monkeypatch.setattr(module,'create_async_operator_llm_client',create)
    runtime=AsyncOperatorLLMRuntime(parse_prompt_pack(yaml.safe_dump(raw)),InProcessOperatorLLMCoordinator(),
        {'journal_dir':str(tmp_path/'journal')})
    try: results=await asyncio.gather(*(runtime.call('match',{'payload':i}) for i in range(6)))
    finally: await runtime.aclose()
    assert len(results)==6 and peak==2
    assert set(urls)=={'http://replica-0/v1/systemone','http://replica-1/v1/systemone'}
    monkeypatch.setattr(DeploymentService,'bind',lambda *a:pytest.fail('replay acquired a replica'))
    runtime=AsyncOperatorLLMRuntime(parse_prompt_pack(yaml.safe_dump(raw)),InProcessOperatorLLMCoordinator(),
        {'journal_dir':str(tmp_path/'journal')})
    try: replay=await asyncio.gather(*(runtime.call('match',{'payload':i}) for i in range(6)))
    finally: await runtime.aclose()
    assert replay==results and len(urls)==6
