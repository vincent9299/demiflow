"""U4/S1 POOL Ray evidence: logical GPU placement through Ray Core (controlled).

ray.init(num_gpus=2) declares *logical* GPUs — no physical device is touched and
no real GPU evidence is claimed (real GPU 未取得, U6). The provider maps accelerator
type gpu to the named actor's num_gpus (Ray Core owns physical placement charging,
never LocalCapacityPool), while the device ledger owns memory-coexistence
accounting. Without a ledger the provider keeps the exact U2 rejection defaults.
"""
import time
import uuid

import pytest

ray = pytest.importorskip("ray")

from demiflow.execution.run_context import RunContext
from demiflow.execution.worker.ray_host import RayControlConnector
from demiflow.resources.contracts import (
    RESOURCE_PROTOCOL_VERSION, ResourceError, ResourceProfile, WorkerLaunchSpec, WorkerRequest,
)
from demiflow.resources.device_pool import DevicePool
from demiflow.resources.ray import RayResourceProvider

pytestmark = pytest.mark.ray

GIB = 1024 ** 3


@pytest.fixture(scope="module")
def ray_cluster():
    ray.init(num_cpus=2, num_gpus=2, include_dashboard=False, ignore_reinit_error=True)
    try:
        yield
    finally:
        ray.shutdown()


def _devices(tmp_path):
    return DevicePool(tmp_path / "devices", inventory={"devices": [
        {"device_id": "gpu-%d" % i, "type": "gpu", "memory_bytes": 16 * GIB} for i in range(2)]})


def _launch_spec(run, scope_id, allocation_id, boot_id):
    return {
        "directory": str(run.directory), "run_id": run.ref.run_id, "scope_id": scope_id,
        "allocation_id": allocation_id, "boot_id": boot_id, "controller_epoch": run.attempt,
        "capabilities": ["cpu-map"], "grant": None,
    }


def _request(allocation_id, *, device_type="gpu", device_memory_bytes=4 * GIB):
    launch = WorkerLaunchSpec(
        artifact_refs={"plan": {"digest": "0" * 64, "location": "file:///tmp/plan.pkl"}},
        entrypoint="demiflow-worker-actor", argv=("ray-actor",),
        bootstrap_ref="bootstrap-ref", access_refs={},
        runtime_protocol_version=RESOURCE_PROTOCOL_VERSION)
    return WorkerRequest(
        "w-" + allocation_id, allocation_id,
        ResourceProfile(requests={"cpu_millis": 1000, "memory_bytes": GIB,
                                  "accelerators": [{"type": device_type, "count": 1}]},
                        requirements={"accelerator_memory_bytes": device_memory_bytes}),
        launch, 60000).to_dict()


def _immutable(provider, scope_id, allocation_id, request):
    normalized = provider.describe_and_normalize(None, None, request)
    return {"scope_id": scope_id, "allocation_id": allocation_id, "request": request,
            "spec_hash": normalized["spec_hash"], "effective": normalized["effective"],
            "requirements": None, "binding": {}}


def _wait_gpu_available(expected, timeout=15.0):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if ray.available_resources().get("GPU", 0.0) == expected:
            return
        time.sleep(0.05)
    raise AssertionError("Ray logical GPU availability never reached %r: %r"
                         % (expected, ray.available_resources()))


def test_ray_logical_gpu_actor_placement_and_ledger_lifecycle(tmp_path, ray_cluster):
    devices = _devices(tmp_path)
    connector = RayControlConnector()
    provider = RayResourceProvider(connector, provision_timeout_s=15, device_pool=devices)
    scope_id, allocation_id, boot_id = uuid.uuid4().hex, uuid.uuid4().hex, uuid.uuid4().hex
    with RunContext.start(tmp_path / "run") as run:
        connector.stage(allocation_id, _launch_spec(run, scope_id, allocation_id, boot_id))
        immutable = _immutable(provider, scope_id, allocation_id, _request(allocation_id))
        assert immutable["effective"]["accelerators"] == [{"type": "gpu", "count": 1}]
        observed = provider.ensure_present(allocation_id, immutable)
        assert observed["phase"] == "RUNNING"
        assert observed["boot_id"] == boot_id
        assert observed["placement"]["device_ids"] in (["gpu-0"], ["gpu-1"])
        assert devices.snapshot()["charged"] == {"accelerator_gpu": 1}
        # Ray Core charged the named actor one logical GPU (single_charge: the
        # engine never double-charges Ray placement in a local pool).
        _wait_gpu_available(1.0)
        result = provider.ensure_absent(
            {"scope_id": scope_id, "allocation_id": allocation_id}, 5000)
        assert result["cleanup"] == "CONFIRMED"
        assert result["evidence_ref"].startswith("ray-actor-dead:")
        assert devices.snapshot()["charged"] == {}  # ledger releases only on confirmed death
        _wait_gpu_available(2.0)


def test_ray_device_pending_creates_no_actor_and_stays_retryable(tmp_path, ray_cluster):
    devices = _devices(tmp_path)
    connector = RayControlConnector()
    provider = RayResourceProvider(connector, provision_timeout_s=3, device_pool=devices)
    scope_id, allocation_id = uuid.uuid4().hex, uuid.uuid4().hex
    # Both logical devices are held exclusively by existing reservations.
    for holder in ("holder-0", "holder-1"):
        devices.reserve(holder, "a" * 64, [{"type": "gpu", "count": 1}], deadline_ms=60000)
    try:
        with RunContext.start(tmp_path / "run") as run:
            connector.stage(allocation_id, _launch_spec(run, scope_id, allocation_id,
                                                        uuid.uuid4().hex))
            immutable = _immutable(provider, scope_id, allocation_id, _request(allocation_id))
            observed = provider.ensure_present(allocation_id, immutable)
            assert observed["phase"] == "PENDING"
            assert "capacity_pending" in observed["conditions"]
            assert provider._actors == {}  # device shortfall precedes actor creation
            assert ray.available_resources().get("GPU", 0.0) == 2.0  # no Ray charge either
            # PENDING is retriable on the same allocation: freeing one device admits it.
            devices.release("holder-0", {"confirmed": True, "evidence_ref": "exit://h0",
                                         "identity": None})
            observed = provider.ensure_present(allocation_id, immutable)
            assert observed["phase"] == "RUNNING"
            assert observed["placement"]["device_ids"] == ["gpu-0"]
            result = provider.ensure_absent(
                {"scope_id": scope_id, "allocation_id": allocation_id}, 5000)
            assert result["cleanup"] == "CONFIRMED"
            assert devices.snapshot()["charged"] == {"accelerator_gpu": 1}  # holder-1 still charged
    finally:
        for holder in ("holder-0", "holder-1"):
            if devices.reservation_view(holder) is not None:
                devices.release(holder, {"confirmed": True, "evidence_ref": "exit://" + holder,
                                         "identity": None})


def test_ray_non_gpu_accelerator_type_rejected(tmp_path, ray_cluster):
    devices = _devices(tmp_path)
    provider = RayResourceProvider(RayControlConnector(), device_pool=devices)
    with pytest.raises(ResourceError) as exc:
        provider.describe_and_normalize(None, None, _request("alloc-a", device_type="npu"))
    assert exc.value.code == "UNSUPPORTED_CAPABILITY"
    assert exc.value.reason == "RayCore provider maps accelerator type gpu only"
    assert devices.snapshot()["active"] == []


def test_ray_provider_without_ledger_keeps_exact_rejections(ray_cluster):
    provider = RayResourceProvider(RayControlConnector())
    with pytest.raises(ResourceError) as exc:
        provider.describe_and_normalize(None, None, _request("alloc-a"))
    assert exc.value.code == "UNSUPPORTED_CAPABILITY"
    assert exc.value.reason == "RayCore first batch admits CPU/memory only"
    normalized = provider.describe_and_normalize(None, None, _plain_request("alloc-b"))
    assert set(normalized) == {"effective", "spec", "spec_hash"}  # no additive keys without a ledger
    assert normalized["effective"] == {"cpu_millis": 1000, "memory_bytes": GIB}


def _plain_request(allocation_id):
    launch = WorkerLaunchSpec(
        artifact_refs={"plan": {"digest": "0" * 64, "location": "file:///tmp/plan.pkl"}},
        entrypoint="demiflow-worker-actor", argv=("ray-actor",),
        bootstrap_ref="bootstrap-ref", access_refs={},
        runtime_protocol_version=RESOURCE_PROTOCOL_VERSION)
    return WorkerRequest(
        "w-" + allocation_id, allocation_id,
        ResourceProfile(requests={"cpu_millis": 1000, "memory_bytes": GIB}),
        launch, 60000).to_dict()