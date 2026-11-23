"""Bind actual host providers, capacity probes and runtime memory safeguards."""
from __future__ import annotations

import importlib.util
import os
import time
from demiflow.execution.contracts.dataflow import Bounds, HostDescriptor, RuntimePolicy
from demiflow.execution.contracts.dataflow_errors import DataflowError
from demiflow.execution.dataflow_artifacts import verify_host_domain
from demiflow.execution.storage.objects import RunArtifactStore
from demiflow.execution.recovery_contract import identity
_RAY_AVAILABLE = importlib.util.find_spec('ray') is not None


def resource_scope_binding(resources, policy):
    """Runtime cleanup coordinates, deliberately outside sealed plan identity."""
    binding = {'provider': policy.provider}
    # A sealed local plan continued under another provider has no local pool to name. Record the
    # provider alone, as before these coordinates existed, so that the provider check that follows
    # refuses the continuation with INCOMPATIBLE_HISTORY instead of this lookup failing first.
    pool = getattr(resources.provider, 'pool', None)
    if policy.provider == 'local' and pool is not None:
        binding.update(worker_mode='thread' if policy.worker_mode == 'thread' else 'process',
                       capacity_root=str(pool.root))
    return binding

def driver_resources(action: "RunAction", policy: RuntimePolicy):
    from demiflow.resources.manager import ManagerBounds, ResourceManager
    from demiflow.execution.hosts.run_state import RunResourceState
    run = action.context
    root = run.directory.with_name(run.directory.name + "-driver-resources")
    state = RunResourceState(run.journal)
    from demiflow.execution.hosts.devices import device_configuration
    devices, device_topology = device_configuration(policy.provider)
    job_resources, job_gpu = None, 0
    if policy.provider == "local":
        from demiflow.execution.hosts.capacity import local_capacity
        from demiflow.resources.capacity import LocalCapacityPool
        from demiflow.resources.local import LocalResourceProvider
        from demiflow.resources.process_unit import ManagedProcessUnit
        from demiflow.execution.dataflow_hosts import DataflowLocalConnector
        domain, capacity = local_capacity()
        pool = LocalCapacityPool(domain / "capacity", capacity=capacity)
        unit = ManagedProcessUnit(root / "units")
        job_gpu = len(devices.inventory) if devices is not None else 0
        def job_resources(scope_id):
            provider = LocalResourceProvider(pool, unit, device_pool=devices)
            return ResourceManager(provider, RunResourceState(run.journal, operation_scope=scope_id))
        if policy.worker_mode == "thread":
            from demiflow.resources.thread import ThreadResourceProvider
            from demiflow.execution.hosts.thread import DataflowThreadConnector
            provider = ThreadResourceProvider(pool, root / "thread-units", control=DataflowThreadConnector())
        else:
            provider = LocalResourceProvider(pool, unit, control=DataflowLocalConnector(), device_pool=devices)
    elif policy.provider == "ray_core":
        if not _RAY_AVAILABLE:
            raise DataflowError("UNSUPPORTED_CAPABILITY", "Ray Core provider requires the ray package")
        from demiflow.resources.ray import RayResourceProvider
        from demiflow.execution.dataflow_hosts import DataflowRayConnector
        provider = RayResourceProvider(
            DataflowRayConnector(command_timeout_s=policy.control_timeout_ms / 1000.0,
                                 device_topology=device_topology), device_pool=devices,
            allocation_reader=state.read_allocation_identity)
    else:
        raise DataflowError("INVALID_CONTRACT", "unsupported provider: " + str(policy.provider))
    return ResourceManager(provider, state, bounds=ManagerBounds()), job_resources, job_gpu

def original_job_resources(run, scope_id):
    """Reopen the resource domain recorded by this run, for original-job observation."""
    from demiflow.resources.capacity import LocalCapacityPool
    from demiflow.resources.local import LocalResourceProvider
    from demiflow.resources.manager import ResourceManager
    from demiflow.resources.process_unit import ManagedProcessUnit
    from demiflow.execution.hosts.capacity import local_capacity
    from demiflow.execution.hosts.devices import device_configuration
    from demiflow.execution.hosts.run_state import RunResourceState
    state = RunResourceState(run.journal, operation_scope=scope_id)
    domain, capacity = local_capacity()
    units = run.directory.with_name(run.directory.name + '-driver-resources') / 'units'
    devices, _ = device_configuration('local')
    return ResourceManager(LocalResourceProvider(LocalCapacityPool(domain / 'capacity', capacity=capacity),
                        ManagedProcessUnit(units), device_pool=devices), state)

def driver_hosts(action: "RunAction", policy: RuntimePolicy, store: RunArtifactStore,
                  bounds: Bounds, *, provider=None) -> tuple[HostDescriptor, ...]:
    run = action.context
    revision = run.journal.recovery_identity()["platform_revision"]
    if policy.provider == "local":
        from demiflow.execution.hosts.capacity import local_capacity
        _, capacity = local_capacity()
        from demiflow.execution.hosts.devices import gpu_capacity
        capacity = {**capacity, 'gpu': 0 if policy.worker_mode == 'thread' else gpu_capacity('local')}
        hosts = (HostDescriptor(
            host_id="local", provider="local", runtime_revision=revision,
            store_domains=(policy.store_id,),
            logical_resources={key: float(value) for key, value in capacity.items()},
            isolation_capabilities=("cpu-map", "stream")),)
    elif policy.provider == "ray_core":
        hosts = _ray_hosts(policy, revision, provider=provider)
    else:
        raise DataflowError("INVALID_CONTRACT", "unsupported provider: " + str(policy.provider))
    challenge_bytes = identity({"run_id": run.ref.run_id, "store_id": policy.store_id}).encode()
    challenge = store.put(iter([challenge_bytes]), max_bytes=len(challenge_bytes),
                          media_type="application/octet-stream")
    for host in hosts:
        verify_host_domain(host, store=store, challenge=challenge, bounds=bounds)
    return hosts

def _ray_hosts(policy: RuntimePolicy, revision: str, *, provider=None) -> tuple[HostDescriptor, ...]:
    if not _RAY_AVAILABLE:
        raise DataflowError("UNSUPPORTED_CAPABILITY", "Ray Core provider requires the ray package")
    import ray
    if not ray.is_initialized():
        raise DataflowError("UNSUPPORTED_CAPABILITY", "Ray Core cluster is not initialized")
    hosts = []
    for node in ray.nodes():
        if not node.get("Alive"):
            continue
        host_id = "ray-" + node["NodeID"]
        admitted_gpus = provider.device_host_capacity(host_id) if provider is not None else 0
        hosts.append(HostDescriptor(
            host_id=host_id, provider="ray_core", runtime_revision=revision,
            store_domains=(policy.store_id,),
            logical_resources={"cpu_millis": float(node.get("Resources", {}).get("CPU", 1)) * 1000,
                               "memory_bytes": float(node.get("Resources", {}).get("memory", 1024 ** 3)),
                               "gpu": min(float(node.get("Resources", {}).get("GPU", 0)), admitted_gpus)},
            isolation_capabilities=("cpu-map", "stream")))
    if not hosts:
        raise DataflowError("RESOURCE_INFEASIBLE", "no alive Ray Core host is available")
    return tuple(hosts)


class NativeBudgetGuard:
    """Bounded local sampling; failures stop only this owned execution.

    Threads share their host process, so RSS is a conservative whole-process
    observation. Sampling cannot prevent a third-party allocation between polls.
    """
    def __init__(self, limits):
        self.limits = dict(limits or {})
        self._next = 0.0
        self.peak_rss_bytes = 0

    def check(self):
        if not {'max_rss_bytes', 'cgroup_headroom_bytes'} & self.limits.keys():
            return
        now = time.monotonic()
        if now < self._next:
            return
        self._next = now + .1
        if 'max_rss_bytes' in self.limits:
            from demiflow.resources.process_observation import _read
            fields = _read('/proc/self/stat', maximum=8192).decode().rsplit(')', 1)[1].split()
            rss = int(fields[21]) * os.sysconf('SC_PAGE_SIZE')
            self.peak_rss_bytes = max(self.peak_rss_bytes, rss)
            if rss > self.limits['max_rss_bytes']:
                raise DataflowError('BUDGET_EXCEEDED', 'native host sampled RSS exceeds max_rss_bytes')
        if 'cgroup_headroom_bytes' in self.limits:
            from demiflow.execution.hosts.datafusion import _cgroup_memory
            memory = _cgroup_memory()
            if memory and memory[0] + self.limits['cgroup_headroom_bytes'] >= memory[1]:
                raise DataflowError('RESOURCE_INFEASIBLE', 'native host lacks declared cgroup headroom')
