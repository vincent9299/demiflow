"""Single assembly lane for driver and worker runtime services (H026-H027, C09).

make_driver_services wires the one resources/store/journal/checkpoint/host port
stack in the frozen order RunAction -> DriverArtifactPort -> RunArtifactStore ->
DriverJournalService -> DriverServices, bound to the original action journal. It
never opens a second database and two actions in one process share no mutable
cut state. make_runtime_services assembles the read-only worker layout, artifact
store, journal client, reservation and cancel/clock without opening the driver
database; it verifies the bundle and programs before returning complete services.
Both services values are in-process types and are never serialized.
"""
from __future__ import annotations

import importlib.util
import json
import time
from pathlib import Path
from typing import TYPE_CHECKING

from .contracts.dataflow import (Bounds, DriverServices, HostDescriptor, LoadedProgram,
                                 RuntimePolicy, RuntimeServices, WorkerAssignment)
from .contracts.dataflow_errors import DataflowError
from .dataflow_artifacts import verify_host_domain
from .dataflow_authority import DriverArtifactPort, DriverJournalService, WorkerJournalClient
from .dataflow_seal import load_sealed_dataflow, verify_dataflow_program
from .dataflow_store import RunArtifactStore
from .recovery_contract import identity

if TYPE_CHECKING:
    from .contracts.dataflow import ArtifactRef, ControlTransport
    from .run_context import RunAction

_RAY_AVAILABLE = importlib.util.find_spec("ray") is not None


def _now_ms() -> int:
    return int(time.time() * 1000)


def _driver_resources(action: "RunAction", policy: RuntimePolicy):
    from ..resources.manager import ManagerBounds, ResourceManager
    from .resource_state import RunResourceState
    run = action.context
    root = run.directory.with_name(run.directory.name + "-driver-resources")
    state = RunResourceState(run.journal)
    from .dataflow_devices import device_pool
    devices = device_pool(policy.provider)
    if policy.provider == "local":
        from .dataflow_capacity import local_capacity
        from ..resources.capacity import LocalCapacityPool
        from ..resources.local import LocalResourceProvider
        from ..resources.process_unit import ManagedProcessUnit
        from .dataflow_hosts import DataflowLocalConnector
        domain, capacity = local_capacity()
        pool = LocalCapacityPool(domain / "capacity", capacity=capacity)
        unit = ManagedProcessUnit(root / "units")
        provider = LocalResourceProvider(pool, unit, control=DataflowLocalConnector(), device_pool=devices)
    elif policy.provider == "ray_core":
        if not _RAY_AVAILABLE:
            raise DataflowError("UNSUPPORTED_CAPABILITY", "Ray Core provider requires the ray package")
        from ..resources.ray import RayResourceProvider
        from .dataflow_hosts import DataflowRayConnector
        provider = RayResourceProvider(
            DataflowRayConnector(command_timeout_s=policy.control_timeout_ms / 1000.0), device_pool=devices)
    else:
        raise DataflowError("INVALID_CONTRACT", "unsupported provider: " + str(policy.provider))
    return ResourceManager(provider, state, bounds=ManagerBounds())


def _driver_hosts(action: "RunAction", policy: RuntimePolicy, store: RunArtifactStore,
                  bounds: Bounds) -> tuple[HostDescriptor, ...]:
    run = action.context
    revision = run.journal.recovery_identity()["platform_revision"]
    if policy.provider == "local":
        from .dataflow_capacity import local_capacity
        _, capacity = local_capacity()
        from .dataflow_devices import gpu_capacity
        capacity = {**capacity, 'gpu': gpu_capacity('local')}
        hosts = (HostDescriptor(
            host_id="local", provider="local", runtime_revision=revision,
            store_domains=(policy.store_id,),
            logical_resources={key: float(value) for key, value in capacity.items()},
            isolation_capabilities=("cpu-map", "stream")),)
    elif policy.provider == "ray_core":
        hosts = _ray_hosts(policy, revision)
    else:
        raise DataflowError("INVALID_CONTRACT", "unsupported provider: " + str(policy.provider))
    challenge_bytes = identity({"run_id": run.ref.run_id, "store_id": policy.store_id}).encode()
    challenge = store.put(iter([challenge_bytes]), max_bytes=len(challenge_bytes),
                          media_type="application/octet-stream")
    for host in hosts:
        verify_host_domain(host, store=store, challenge=challenge, bounds=bounds)
    return hosts


def _ray_hosts(policy: RuntimePolicy, revision: str) -> tuple[HostDescriptor, ...]:
    if not _RAY_AVAILABLE:
        raise DataflowError("UNSUPPORTED_CAPABILITY", "Ray Core provider requires the ray package")
    import ray
    if not ray.is_initialized():
        raise DataflowError("UNSUPPORTED_CAPABILITY", "Ray Core cluster is not initialized")
    from .dataflow_devices import gpu_capacity
    admitted_gpus = gpu_capacity('ray_core')
    hosts = []
    for node in ray.nodes():
        if not node.get("Alive"):
            continue
        host_id = "ray-" + node["NodeID"]
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


def make_driver_services(action: "RunAction", *, policy: RuntimePolicy, bounds: Bounds) -> DriverServices:
    if not isinstance(policy, RuntimePolicy):
        raise DataflowError("INVALID_CONTRACT", "policy must be a RuntimePolicy")
    if not isinstance(bounds, Bounds):
        raise DataflowError("INVALID_CONTRACT", "bounds must be a Bounds")
    run = action.context
    port = DriverArtifactPort(action, bounds=bounds)
    store = RunArtifactStore(root=run.directory / ".dataflow-store", store_id=policy.store_id,
                             journal=port, bounds=bounds)
    resources = _driver_resources(action, policy)
    hosts = _driver_hosts(action, policy, store, bounds)
    journal_service = DriverJournalService(action, store=store, bounds=bounds)
    try:
        return DriverServices(action=action, journal_service=journal_service, store=store,
                              resources=resources, policy=policy, bounds=bounds, hosts=hosts,
                              clock_ms=_now_ms)
    except BaseException:
        journal_service.close(deadline_ms=bounds.timeout_ms)
        raise


def _read_constants(ref: "ArtifactRef", *, store: RunArtifactStore, bounds: Bounds) -> dict:
    raw = b"".join(store.read(ref, max_bytes=bounds.max_control_bytes))
    value = json.loads(raw)
    if type(value) is not dict:
        raise DataflowError("INVALID_CONTRACT", "runtime constants must be a JSON object")
    return value


class _VerifiedPrograms(dict):
    """Programs and policy returned from the same authenticated sealed plan."""
    native_limits = None


def _load_programs(assignment: WorkerAssignment, *, store: RunArtifactStore, bounds: Bounds, cache=None) -> dict[str, LoadedProgram]:
    graph, physical, pin = load_sealed_dataflow(assignment.sealed,
        action_id=assignment.action_id, store=store, bounds=bounds)
    if physical.plan_id != assignment.plan_id:
        raise DataflowError("CODE_MISMATCH", "loaded plan_id differs from the assignment")
    if physical.plan_revision != assignment.plan_revision:
        raise DataflowError("FENCED", "loaded plan_revision differs from the assignment")
    from .recovery_identity import platform_identity
    if pin.get("platform_revision") != platform_identity():
        raise DataflowError("CODE_MISMATCH", "Worker platform code differs from the sealed revision")
    programs = _VerifiedPrograms()
    programs.native_limits = physical.policy.native_limits
    owned = {occurrence for stage in assignment.stages for occurrence in stage.occurrences}
    for node in graph.nodes:
        if node.occurrence_id not in owned or node.program_ref is None:
            continue
        programs[node.occurrence_id] = verify_dataflow_program(
            node.program_ref, expected_pin=pin["programs"][node.occurrence_id],
            store=store, bounds=bounds, cache=cache)
    return programs


def make_runtime_services(assignment: WorkerAssignment, *, transport: "ControlTransport",
                          cache: Path) -> RuntimeServices:
    if not isinstance(assignment, WorkerAssignment):
        raise DataflowError("INVALID_CONTRACT", "assignment must be a WorkerAssignment")
    bounds = assignment.bounds
    cache = Path(cache) / assignment.lease.allocation_id
    journal = WorkerJournalClient(transport=transport, lease=assignment.lease, bounds=bounds)
    journal.check_grant()
    shared_store = transport.store
    if shared_store is None:
        raise DataflowError("INVALID_CONTRACT", "transport exposes no authenticated shared store")
    store_id = assignment.sealed.code_pin_ref.store_id
    if store_id != shared_store.store_id:
        raise DataflowError("INVALID_CONTRACT", "assignment artifacts belong to another store domain")
    store = RunArtifactStore(root=shared_store.root, store_id=store_id, journal=journal, bounds=bounds)
    programs = _load_programs(assignment, store=store, bounds=bounds, cache=cache)
    constants = _read_constants(assignment.sealed.constants_ref, store=store, bounds=bounds)
    from .dataflow_native_limits import NativeBudgetGuard, intersect_native_limits
    limits = intersect_native_limits(getattr(programs, "native_limits", None), assignment.native_limits)
    if limits is not None:
        constants['native_limits'] = limits
    guard = NativeBudgetGuard(limits)
    def check_stop():
        guard.check()
        transport.check_stop()
        journal.check_lease()
    return RuntimeServices(store=store, journal=journal, bounds=bounds, assignment=assignment,
                           clock_ms=_now_ms, check_stop=check_stop,
                           constants=constants, programs=programs)
