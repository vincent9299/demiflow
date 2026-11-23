"""Original resource journal + real private Ray ownership/binding boundaries."""
from contextlib import contextmanager
import tempfile
import time
import uuid

import pytest

ray = pytest.importorskip('ray')

from demiflow.execution.pipeline.run_context import RunContext
from demiflow.execution.hosts.run_state import RunResourceState
from demiflow.execution.hosts.ray_worker_host import RayControlConnector
from demiflow.resources.manager import ResourceManager
from demiflow.resources.ray import RayResourceProvider
from demiflow.resources.contracts import (
    RESOURCE_PROTOCOL_VERSION, MutationContext, ResourceError, ResourceScope,
    ResourceProfile, ResourcePlan, WorkerRequest, WorkerLaunchSpec,
)


@ray.remote(max_restarts=0, max_task_retries=0)
class OwnedShell:
    def __init__(self, launch, spec, boot):
        self.spec, self.boot = spec, boot
        self.delay = launch.get('identity_delay', 0)

    def identity(self):
        time.sleep(self.delay)
        self.delay = 0
        return {'spec_hash': self.spec, 'boot_id': self.boot}

    def close(self):
        return None


class StrictConnector(RayControlConnector):
    def __init__(self):
        super().__init__()
        self.validations = 0
        self.adoptions = 0
        self.reject = False

    def host_class(self):
        return OwnedShell

    def validate_device_binding(self, allocation_id, identity, **kwargs):
        self.validations += 1
        if self.reject:
            raise ValueError('deliberately rejected actual binding')
        return ()

    def adopt(self, allocation_id, actor):
        self.adoptions += 1
        return super().adopt(allocation_id, actor)


@pytest.fixture(scope='module')
def private_ray():
    assert ray.__file__ and hasattr(ray, 'ObjectRef') and not ray.is_initialized()
    import demiflow.resources.ray as provider_module
    assert provider_module.ray is ray
    try:
        ray.init(address='local', num_cpus=2, num_gpus=0, include_dashboard=False,
                 object_store_memory=100*1024**2,
                 _temp_dir=tempfile.mkdtemp(prefix='demiflow-ray-binding-', dir='/tmp'))
        yield
    finally:
        ray.shutdown()


def _context(run, scope, operation, revision):
    return MutationContext(RESOURCE_PROTOCOL_VERSION, scope, run.attempt, run.fence, operation, revision)


@contextmanager
def allocation(tmp_path, *, timeout=5, delay=0):
    with RunContext.start(tmp_path/'run') as run:
        scope, ident, boot = (uuid.uuid4().hex for _ in range(3))
        state = RunResourceState(run.journal, claim_lease_ms=1)
        connector = StrictConnector()
        provider = RayResourceProvider(connector, provision_timeout_s=timeout,
                                       allocation_reader=state.read_allocation_identity)
        manager = ResourceManager(provider, state)
        state.open_scope(run.fence, ResourceScope(scope, run.ref.run_id, {'provider':'ray_core'},
            'authority', {'cpu_millis':2000, 'memory_bytes':1024**3, 'max_workers':2}))
        launch = {'boot_id':boot, 'identity_delay':delay}
        connector.stage(ident, launch)
        import hashlib
        artifact=tmp_path/'shell.json';artifact.write_text('{"kind":"owned-control-shell"}')
        request = WorkerRequest('worker-'+ident, ident,
            ResourceProfile(requests={'cpu_millis':1000, 'memory_bytes':64*1024**2}),
            WorkerLaunchSpec(artifact_refs={'shell':{'digest':hashlib.sha256(artifact.read_bytes()).hexdigest(),
                            'location':artifact.as_uri()}}, entrypoint='demiflow-worker-actor', argv=('ray-actor',),
                            bootstrap_ref='bootstrap', access_refs={}, runtime_protocol_version=RESOURCE_PROTOCOL_VERSION),
            provision_deadline_ms=30000)
        manager.apply(_context(run,scope,'apply',0), ResourcePlan(scope,(request,),()))
        try:
            yield run,state,manager,connector,provider,scope,ident,boot,request,launch
        finally:
            manager.close(_context(run,scope,'close',1), {'mode':'ABANDON', 'engine_receipt_ref':'test-ended',
                'terminate_deadline_ms':5000, 'reason':'owned test cleanup'})
            manager.reconcile_once(run.fence, time.monotonic()+10,16)
            assert state.read_allocation(scope,ident)['cleanup']=='CONFIRMED'


def _reconcile(manager, run):
    manager.reconcile_once(run.fence,time.monotonic()+10,16)


def test_identity_timeout_retains_same_real_actor_until_verified(tmp_path, private_ray):
    with allocation(tmp_path,timeout=.01,delay=.2) as values:
        run,state,manager,connector,provider,scope,ident,boot,request,launch = values
        _reconcile(manager,run)
        actor_id = provider._actors[ident][0]._actor_id.hex()
        assert state.read_allocation(scope,ident)['phase']=='PENDING'
        assert ident not in provider._validated and connector.adoptions==0
        assert state.read_allocation(scope,ident)['cleanup']=='NONE'
        provider._provision_timeout_s=5
        deadline=time.monotonic()+10
        while state.read_allocation(scope,ident)['phase']!='RUNNING':
            assert time.monotonic()<deadline
            _reconcile(manager,run)
            time.sleep(.01)
        assert state.read_allocation(scope,ident)['phase']=='RUNNING'
        assert provider._actors[ident][0]._actor_id.hex()==actor_id
        assert connector.validations==1 and connector.adoptions==1


def test_reconstructed_provider_finds_original_live_actor_and_proves_exit(tmp_path, private_ray):
    with allocation(tmp_path) as values:
        run,state,manager,connector,provider,scope,ident,boot,request,launch = values
        _reconcile(manager,run)
        original = state.read_allocation_identity(scope,ident)
        actual = provider._actors[ident][0]
        assert original['observation']['placement']['actor_id']==actual._actor_id.hex()
        rebuilt = RayResourceProvider(StrictConnector(), allocation_reader=state.read_allocation_identity)
        assert not rebuilt._actors
        assert rebuilt.ensure_absent({'scope_id':scope,'allocation_id':ident,'boot_id':boot},5000)['cleanup']=='CONFIRMED'
        from ray.exceptions import RayActorError
        with pytest.raises(RayActorError):
            ray.get(actual.identity.remote(),timeout=5)


def test_recovered_cleanup_ownership_never_bypasses_business_binding(tmp_path, private_ray, monkeypatch):
    with allocation(tmp_path) as values:
        run,state,manager,connector,provider,scope,ident,boot,request,launch = values
        _reconcile(manager,run)
        new_connector = StrictConnector();new_connector.stage(ident,launch);new_connector.reject=True
        rebuilt = RayResourceProvider(new_connector,allocation_reader=state.read_allocation_identity)
        with monkeypatch.context() as patch:
            patch.setattr(ray,'kill',lambda *a,**kw:None)  # A lost kill command, not fake exit evidence.
            assert rebuilt.ensure_absent({'scope_id':scope,'allocation_id':ident,'boot_id':boot},20)['cleanup']=='UNCONFIRMED'
        assert ident in rebuilt._owned and ident not in rebuilt._validated
        normalized = rebuilt.describe_and_normalize(None,None,request.to_dict())
        immutable = {'scope_id':scope,'allocation_id':ident,'request':request.to_dict(),
                     'spec_hash':normalized['spec_hash'],'effective':normalized['effective']}
        with pytest.raises(ResourceError,match='binding rejected'):
            rebuilt.ensure_present(ident,immutable)
        assert new_connector.validations==1 and new_connector.adoptions==0
        assert rebuilt.ensure_absent({'scope_id':scope,'allocation_id':ident,'boot_id':boot},5000)['cleanup']=='CONFIRMED'


def test_unknown_creation_is_not_never_created(private_ray):
    provider = RayResourceProvider(StrictConnector())
    result=provider.ensure_absent({'scope_id':'unknown-scope','allocation_id':'unknown-allocation'},10)
    assert result['cleanup']=='UNCONFIRMED'


def test_topology_rejects_outside_uuid_and_insufficient_single_card_memory():
    from demiflow.execution.hosts.ray_devices import RayDeviceTopology
    from demiflow.execution.contracts.dataflow_errors import DataflowError
    topology=RayDeviceTopology({'ray-node':({'device_id':'GPU-owned','uuid':'GPU-owned',
        'memory_bytes':8,'actual_memory_bytes':16},)},(),{'ray-node':('0',)})
    binding={'schema':'demiflow.ray_device_binding.v1','node_id':'node',
        'ray_gpu_ids':['0'],'visible_gpu_ids':['0'],
        'devices':[{'uuid':'GPU-owned','ray_id':'0','memory_bytes':16}]}
    demand=[{'type':'gpu','count':1}]
    assert topology.verify(binding,host_id='ray-node',accelerators=demand,device_memory_bytes=8)==('GPU-owned',)
    with pytest.raises(DataflowError):
        topology.verify(binding,host_id='ray-node',accelerators=demand,device_memory_bytes=9)
    binding['devices'][0]['uuid']='GPU-outside'
    with pytest.raises(DataflowError):
        topology.verify(binding,host_id='ray-node',accelerators=demand)
