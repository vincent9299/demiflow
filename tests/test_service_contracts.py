"""CPU-only lifecycle and admission boundaries, including third-party leases."""
import asyncio
from contextlib import asynccontextmanager
from types import SimpleNamespace

import pytest

from demiflow.services import ManagedHTTPService, ModelServiceError, ServiceGroup
from demiflow.services.contracts import validate_service, service_request_slot
from test_model_service import managed, assert_reaped


def test_systemone_uses_the_same_http_lifecycle(managed):
    prior, _, processes, _, port, _ = managed
    import sys
    spec = ManagedHTTPService([sys.executable, str(prior.root/'server.py')],
        base_url=f'http://127.0.0.1:{port}/v1', root=prior.root, gpus=[], expected_model='fixture',
        request_concurrency=2, startup_timeout_s=5, shutdown_timeout_s=.2, poll_interval_s=.01)
    model = SimpleNamespace(transport='systemone', base_url=spec.base_url, name='fixture')
    owner = spec.bind(model)
    assert not processes
    async def use():
        await asyncio.gather(*(owner.ensure_ready() for _ in range(8)))
        active = peak = 0
        async def call():
            nonlocal active, peak
            async with service_request_slot(owner):
                active += 1; peak = max(peak, active)
                await asyncio.sleep(.01)
                active -= 1
        try:
            await asyncio.gather(*(call() for _ in range(9)))
            assert peak == 2
        finally:
            await owner.aclose()
        with pytest.raises(ModelServiceError, match='closed'):
            await owner.ensure_ready()
    asyncio.run(use())
    assert len(processes) == 1
    assert_reaped(processes)


class FakeService:
    def __init__(self, name, events, *, fail=False, wait=None):
        self.name, self.events, self.fail, self.wait = name, events, fail, wait
        self.owner = None
    def bind(self, model=None):
        parent = self
        class Lease:
            base_url = 'http://127.0.0.1:12345/v1'
            started = closed = False
            async def ensure_ready(self):
                if self.started: return
                self.started = True
                parent.events.append(('start', parent.name))
                if parent.wait is not None: await parent.wait.wait()
                if parent.fail: raise ModelServiceError(parent.name)
            @asynccontextmanager
            async def request_slot(self):
                parent.events.append(('slot', parent.name))
                yield
            async def aclose(self):
                if self.closed: return
                self.closed = True
                parent.events.append(('stop', parent.name))
        self.owner = Lease()
        return self.owner


@pytest.mark.asyncio
async def test_group_starts_once_and_releases_in_reverse_order():
    events = []
    group = ServiceGroup([FakeService('engine', events), FakeService('wrapper', events)])
    owner = group.bind()
    assert events == []
    await asyncio.gather(*(owner.ensure_ready() for _ in range(5)))
    async with owner.request_slot(): pass
    await owner.aclose(); await owner.aclose()
    assert events == [('start','engine'), ('start','wrapper'), ('slot','wrapper'),
                      ('stop','wrapper'), ('stop','engine')]


@pytest.mark.asyncio
async def test_group_start_failure_rolls_back_every_lease():
    events = []
    owner = ServiceGroup([FakeService('engine', events), FakeService('wrapper', events, fail=True)]).bind()
    with pytest.raises(ModelServiceError, match='wrapper'):
        await owner.ensure_ready()
    await owner.aclose()
    assert events[-2:] == [('stop','wrapper'), ('stop','engine')]


@pytest.mark.asyncio
async def test_group_cancel_during_startup_cleans_dependencies():
    events = []
    wait = asyncio.Event()
    owner = ServiceGroup([FakeService('engine', events), FakeService('wrapper', events, wait=wait)]).bind()
    task = asyncio.create_task(owner.ensure_ready())
    async with asyncio.timeout(2):
        while ('start','wrapper') not in events: await asyncio.sleep(.001)
    await owner.aclose()
    with pytest.raises(asyncio.CancelledError): await task
    assert events[-2:] == [('stop','wrapper'), ('stop','engine')]


def test_service_contract_is_structural_and_group_is_bounded():
    assert validate_service(FakeService('custom', [])) is not None
    with pytest.raises(TypeError, match='bind'): validate_service(object())
    for values in ([], [FakeService('custom', [])]*17):
        with pytest.raises(ValueError): ServiceGroup(values)
    with pytest.raises(ValueError, match='Nested'):
        ServiceGroup([ServiceGroup([FakeService('custom', [])])])


@pytest.mark.parametrize('preexisting',[False,True])
@pytest.mark.parametrize('composition',['dependencies','replicas'])
def test_failed_composite_stops_only_new_idle_persistent_dependencies(managed,preexisting,composition):
    import sys
    from demiflow.services.shared_http import SharedHTTPService
    from demiflow.services.manage import start_service,stop_service,status_service
    spec,_,_,_,port,_=managed
    configuration=dict(command=[sys.executable,str(spec.root/'server.py')],
        base_url=f'http://127.0.0.1:{port}/v1',expected_model='fixture',gpus=[],
        startup_timeout_s=5,shutdown_timeout_s=.2,poll_interval_s=.01)
    shared=SharedHTTPService(root=spec.root,name='rollback-fixture',configuration=configuration)
    if preexisting: start_service(spec.root,'rollback-fixture',configuration)
    async def run():
        failing=FakeService('wrapper',[],fail=True)
        if composition=='replicas':
            from demiflow.services.replicas import ReplicaService
            owner=ReplicaService([ServiceGroup([shared]),ServiceGroup([failing])]).bind()
        else: owner=ServiceGroup([shared,failing]).bind()
        try:
            with pytest.raises(ModelServiceError,match='wrapper'): await owner.ensure_ready()
        finally: await owner.aclose()
    try:
        asyncio.run(run())
        assert status_service(spec.root,'rollback-fixture')['alive'] is preexisting
    finally: stop_service(spec.root,'rollback-fixture',timeout_s=5)


@pytest.mark.asyncio
async def test_gpu_cooperation_is_shared_across_project_roots(tmp_path,monkeypatch):
    import sys
    monkeypatch.setenv('DEMIFLOW_RESOURCE_HOME',str(tmp_path/'shared'))
    one=ManagedHTTPService([sys.executable,'-V'],base_url='http://127.0.0.1:30211/v1',
        root=tmp_path/'one',gpus=[9999]).bind()
    two=ManagedHTTPService([sys.executable,'-V'],base_url='http://127.0.0.1:30212/v1',
        root=tmp_path/'two',gpus=[9999]).bind()
    try:
        await one._acquire_resources(30211)
        with pytest.raises(ModelServiceError,match='occupied'): await two._acquire_resources(30212)
    finally:
        await two.aclose(); await one.aclose()


def test_service_process_logs_rotate_with_a_fixed_disk_budget(tmp_path):
    import subprocess,sys
    from demiflow.services.process_log import ProcessLog
    path=tmp_path/'engine.log'
    child=subprocess.Popen([sys.executable,'-c','import sys;sys.stdout.write("x"*20000)'],stdout=subprocess.PIPE)
    pump=ProcessLog(child.stdout,path,1024)
    child.wait(timeout=5); pump.close()
    assert pump.error is None
    assert path.stat().st_size<=1024 and (tmp_path/'engine.log.1').stat().st_size<=1024
