"""U4/S1 POOL conformance: logical device ledger + additive provider admission.

Controlled logical evidence only (design D1): the device pool is a declarative
inventory with confirmed-exit accounting; no physical CUDA/UUID binding is claimed
(real GPU evidence 未取得, U6). Without a device ledger the providers keep the
exact U2 rejection defaults and normalized shapes (byte-level protection); with a
ledger they additively admit accelerators, evaluate required/preferred placement,
and charge every non-RELEASED record (queueing, unconfirmed exit, replacement
overlap). Additive budget dimensions are fail-closed: a scope that never
authorized scratch/devices has a zero ceiling for them.
"""
import sys
from contextlib import contextmanager

import pytest

from demiflow.execution.hosts.run_state import RunResourceState
from demiflow.execution.pipeline.run_context import RunContext
from demiflow.resources.capacity import LocalCapacityPool
from demiflow.resources.contracts import (
    RESOURCE_PROTOCOL_VERSION, MutationContext, ResourceError, ResourceProfile,
    ResourceScope, WorkerLaunchSpec, WorkerRequest, negotiate_version, validate_budget,
)
from demiflow.resources.device_pool import DevicePool
from demiflow.resources.local import LocalResourceProvider
from demiflow.resources.manager import ManagerBounds, ResourceManager
from demiflow.resources.process_unit import ManagedProcessUnit

pytestmark = pytest.mark.skipif(not sys.platform.startswith("linux"),
                                reason="Local managed units require Linux")

GIB = 1024 ** 3


@pytest.fixture(autouse=True)
def pidfds_available():
    from demiflow.resources.process_observation import require_pidfds
    try:
        require_pidfds()
    except (OSError, RuntimeError) as exc:
        pytest.skip("pidfd exit evidence unavailable: " + str(exc))


def _inventory(count=2, memory_bytes=16 * GIB, **extra):
    return {"devices": [{"device_id": "gpu-%d" % i, "type": "gpu",
                         "memory_bytes": memory_bytes, **extra} for i in range(count)]}


def _devices(tmp_path, count=2, **extra):
    return DevicePool(tmp_path / "devices", inventory=_inventory(count, **extra))


def _gpu(count=1):
    return [{"type": "gpu", "count": count}]


def _launch(argv):
    return WorkerLaunchSpec(
        artifact_refs={"platform": {"digest": "a" * 64, "location": "file:///tmp/platform.whl"}},
        entrypoint="demiflow-worker", argv=tuple(argv), bootstrap_ref="bootstrap-ref",
        access_refs={}, runtime_protocol_version=RESOURCE_PROTOCOL_VERSION).to_dict()


def _request(allocation_id, *, accelerators=None, device_memory_bytes=None,
             placement=None, scratch_bytes=None, argv=None):
    requests = {"cpu_millis": 1000, "memory_bytes": GIB}
    if accelerators is not None:
        requests["accelerators"] = accelerators
    if scratch_bytes is not None:
        requests["scratch_bytes"] = scratch_bytes
    requirements = ({"accelerator_memory_bytes": device_memory_bytes}
                    if device_memory_bytes is not None else None)
    launch = WorkerLaunchSpec(
        artifact_refs={"platform": {"digest": "a" * 64, "location": "file:///tmp/platform.whl"}},
        entrypoint="demiflow-worker", argv=tuple(argv or _sleep_argv()),
        bootstrap_ref="bootstrap-ref", access_refs={},
        runtime_protocol_version=RESOURCE_PROTOCOL_VERSION)
    return WorkerRequest(
        "w-" + allocation_id, allocation_id,
        ResourceProfile(requests=requests, requirements=requirements),
        launch, 60000, placement=placement).to_dict()


def _provider(tmp_path, device_pool=None, capacity=None):
    pool = LocalCapacityPool(tmp_path / "pool",
                             capacity=capacity or {"cpu_millis": 4000, "memory_bytes": 4 * GIB})
    unit = ManagedProcessUnit(tmp_path / "units")
    return LocalResourceProvider(pool, unit, device_pool=device_pool), pool, unit


def _immutable(provider, scope_id, allocation_id, request):
    normalized = provider.describe_and_normalize(None, None, request)
    return {"scope_id": scope_id, "allocation_id": allocation_id, "request": request,
            "spec_hash": normalized["spec_hash"], "effective": normalized["effective"],
            "requirements": None, "binding": {}}


def _sleep_argv(seconds=30):
    return (sys.executable, "-B", "-c", "import time; time.sleep(%r)" % seconds)


# ---------------- device ledger lifecycle ----------------

def test_device_reserve_release_lifecycle(tmp_path):
    devices = _devices(tmp_path)
    view = devices.reserve("alloc-a", "b" * 64, _gpu(), deadline_ms=60000,
                           device_memory_bytes=4 * GIB)
    assert view["state"] == "RESERVED"
    assert [d["device_id"] for d in view["devices"]] == ["gpu-0"]
    assert devices.reserve("alloc-a", "b" * 64, _gpu(), deadline_ms=60000,
                           device_memory_bytes=4 * GIB) == view  # idempotent
    with pytest.raises(ResourceError) as exc:
        devices.reserve("alloc-a", "c" * 64, _gpu(), deadline_ms=60000)
    assert exc.value.code == "INVALID_SPEC"  # immutable spec
    assert devices.snapshot()["charged"] == {"accelerator_gpu": 1}
    with pytest.raises(ResourceError) as exc:
        devices.release("alloc-a", {"confirmed": False, "evidence_ref": None, "identity": None})
    assert exc.value.code == "CLEANUP_UNCONFIRMED"
    assert devices.snapshot()["charged"] == {"accelerator_gpu": 1}  # unconfirmed keeps charging
    with pytest.raises(ResourceError) as exc:
        devices.release("alloc-a", {"confirmed": True, "evidence_ref": "exit://a",
                                    "identity": {"pid": 1}})
    assert exc.value.code == "INVALID_SPEC"  # logical devices carry no process identity
    released = devices.release("alloc-a", {"confirmed": True, "evidence_ref": "exit://a",
                                           "identity": None})
    assert released["state"] == "RELEASED" and not released["idempotent"]
    assert devices.snapshot()["charged"] == {}
    assert devices.reservation_view("alloc-a") is None
    again = devices.release("alloc-a", {"confirmed": True, "evidence_ref": "exit://a",
                                        "identity": None})
    assert again["idempotent"]  # matching evidence is idempotent
    with pytest.raises(ResourceError) as exc:
        devices.release("alloc-a", {"confirmed": True, "evidence_ref": "exit://other",
                                    "identity": None})
    assert exc.value.code == "CLEANUP_UNCONFIRMED"
    with pytest.raises(ResourceError) as exc:
        devices.reserve("alloc-a", "b" * 64, _gpu(), deadline_ms=60000)
    assert exc.value.code == "INVALID_SPEC"  # released id cannot reserve again


def test_device_inventory_and_spec_validation(tmp_path):
    for inventory in ({"devices": []}, {"devices": "x"}, {},
                      {"devices": [{"device_id": "g", "type": "gpu", "memory_bytes": GIB},
                                   {"device_id": "g", "type": "gpu", "memory_bytes": GIB}]},
                      {"devices": [{"device_id": "g", "type": "gpu", "memory_bytes": GIB,
                                    "uuid": "not-declared"}]}):
        with pytest.raises(ResourceError) as exc:
            DevicePool(tmp_path / "bad", inventory=inventory)
        assert exc.value.code == "INVALID_SPEC"
    devices = _devices(tmp_path)
    with pytest.raises(ResourceError) as exc:
        devices.reserve("alloc-a", "b" * 64, [], deadline_ms=60000)
    assert exc.value.code == "INVALID_SPEC"  # reserve needs accelerator demand
    with pytest.raises(ResourceError) as exc:
        devices.reserve("alloc-a", "short", _gpu(), deadline_ms=60000)
    assert exc.value.code == "INVALID_SPEC"
    with pytest.raises(ResourceError) as exc:
        devices.evaluate(_gpu(), placement={"required": {"device_ids": ["gpu-0"]},
                                             "unknown": {}})
    assert exc.value.code == "INVALID_SPEC"
    with pytest.raises(ResourceError) as exc:
        devices.evaluate([], placement={"required": {"device_ids": ["gpu-0"]}})
    assert exc.value.code == "UNSATISFIABLE_CONSTRAINT"  # required pin without demand


# ---------------- static infeasibility is a preflight failure, never a reservation ----------------

def test_static_infeasible_rejected_before_any_reservation(tmp_path):
    devices = _devices(tmp_path)
    provider, pool, _ = _provider(tmp_path, device_pool=devices)
    cases = [
        (_request("a-x", accelerators=[{"type": "npu", "count": 1}]), None),
        (_request("a-y", accelerators=_gpu(3)), None),  # more devices than inventory
        (_request("a-z", accelerators=_gpu(), device_memory_bytes=32 * GIB), None),  # oversized model
        (_request("a-p", accelerators=_gpu(),
                  placement={"required": {"device_ids": ["gpu-absent"]}}), None),
    ]
    for request, _ in cases:
        with pytest.raises(ResourceError) as exc:
            provider.describe_and_normalize(None, None, request)
        assert exc.value.code == "UNSATISFIABLE_CONSTRAINT"
    assert devices.snapshot()["active"] == []
    assert devices.snapshot()["charged"] == {}
    assert pool.snapshot()["charged"] == {"cpu_millis": 0, "memory_bytes": 0}


# ---------------- coexistence, full accounting, replacement overlap ----------------

def test_device_coexistence_and_full_accounting(tmp_path):
    devices = _devices(tmp_path)  # 2 x 16GiB
    for name in ("a", "b", "c", "d"):
        devices.reserve(name, "b" * 64, _gpu(), deadline_ms=60000, device_memory_bytes=8 * GIB)
    snapshot = devices.snapshot()
    assert snapshot["charged"] == {"accelerator_gpu": 4}
    assert {d["device_id"]: d["charged_memory_bytes"] for d in snapshot["devices"]} == {
        "gpu-0": 16 * GIB, "gpu-1": 16 * GIB}  # a,b coexist on gpu-0; c,d on gpu-1
    assert {d["device_id"]: d["residents"] for d in snapshot["devices"]} == {"gpu-0": 2, "gpu-1": 2}
    with pytest.raises(ResourceError) as exc:
        devices.reserve("e", "b" * 64, _gpu(), deadline_ms=60000, device_memory_bytes=8 * GIB)
    assert exc.value.code == "CAPACITY_PENDING" and exc.value.retriable
    with pytest.raises(ResourceError) as exc:
        devices.release("a", {"confirmed": False, "evidence_ref": None, "identity": None})
    assert exc.value.code == "CLEANUP_UNCONFIRMED"
    assert devices.snapshot()["charged"] == {"accelerator_gpu": 4}  # unconfirmed exit keeps charging
    devices.release("a", {"confirmed": True, "evidence_ref": "exit://a", "identity": None})
    view = devices.reserve("e", "b" * 64, _gpu(), deadline_ms=60000, device_memory_bytes=8 * GIB)
    assert [d["device_id"] for d in view["devices"]] == ["gpu-0"]  # freed share admits the waiter
    # Replacement overlap: b's unconfirmed exit keeps its 8GiB charged, so an
    # exclusive (undeclared-memory) replacement cannot fit anywhere yet.
    with pytest.raises(ResourceError) as exc:
        devices.reserve("b-new", "c" * 64, _gpu(), deadline_ms=60000)
    assert exc.value.code == "CAPACITY_PENDING"
    assert devices.snapshot()["charged"] == {"accelerator_gpu": 4}  # b and e double-count gpu-0
    devices.release("b", {"confirmed": True, "evidence_ref": "exit://b", "identity": None})
    devices.release("e", {"confirmed": True, "evidence_ref": "exit://e", "identity": None})
    view = devices.reserve("b-new", "c" * 64, _gpu(), deadline_ms=60000)
    assert [d["device_id"] for d in view["devices"]] == ["gpu-0"]
    assert view["devices"][0]["memory_bytes"] == 16 * GIB  # undeclared memory = whole device


def test_device_max_residents_caps_coexistence(tmp_path):
    devices = _devices(tmp_path, count=1, max_residents=1)
    devices.reserve("a", "b" * 64, _gpu(), deadline_ms=60000, device_memory_bytes=4 * GIB)
    with pytest.raises(ResourceError) as exc:
        devices.reserve("b", "b" * 64, _gpu(), deadline_ms=60000, device_memory_bytes=4 * GIB)
    assert exc.value.code == "CAPACITY_PENDING"  # 12GiB free but residency is capped
    assert devices.snapshot()["devices"][0]["residents"] == 1


def test_preferred_fallback_and_required_pin(tmp_path):
    devices = _devices(tmp_path)
    devices.reserve("holder", "a" * 64, _gpu(), deadline_ms=60000,
                    device_ids=["gpu-0"], pin_required=True)  # exclusive on gpu-0
    plan = devices.evaluate(_gpu(), placement={"preferred": {"device_ids": ["gpu-0"]}})
    assert plan["warnings"] == [] and plan["device_ids"] == ["gpu-0"] and not plan["pin_required"]
    view = devices.reserve("fallback", "b" * 64, _gpu(), deadline_ms=60000,
                           device_memory_bytes=4 * GIB, device_ids=plan["device_ids"],
                           pin_required=plan["pin_required"])
    assert [d["device_id"] for d in view["devices"]] == ["gpu-1"]  # preferred busy -> feasible fallback
    with pytest.raises(ResourceError) as exc:
        devices.reserve("pinned", "c" * 64, _gpu(), deadline_ms=60000,
                        device_memory_bytes=4 * GIB, device_ids=["gpu-0"], pin_required=True)
    assert exc.value.code == "CAPACITY_PENDING"  # required pin never silently migrates
    devices.release("holder", {"confirmed": True, "evidence_ref": "exit://h", "identity": None})
    view = devices.reserve("prefers", "d" * 64, _gpu(), deadline_ms=60000,
                           device_memory_bytes=4 * GIB, device_ids=["gpu-0"])
    assert [d["device_id"] for d in view["devices"]] == ["gpu-0"]  # preferred tried first


# ---------------- scratch charging with legacy shape protection ----------------

def test_scratch_charging_and_legacy_shape(tmp_path):
    legacy = LocalCapacityPool(tmp_path / "legacy", capacity={"cpu_millis": 4000, "memory_bytes": 4 * GIB})
    effective = {"cpu_millis": 1000, "memory_bytes": GIB}
    legacy.reserve("a", "b" * 64, effective, 60000)
    assert legacy.snapshot()["charged"] == {"cpu_millis": 1000, "memory_bytes": GIB}  # exact legacy shape
    with pytest.raises(ResourceError) as exc:
        legacy.reserve("b", "b" * 64, {**effective, "scratch_bytes": 1}, 60000)
    assert exc.value.code == "UNSATISFIABLE_CONSTRAINT"  # undeclared dimension is rejected, not ignored
    pool = LocalCapacityPool(tmp_path / "scratch",
                             capacity={"cpu_millis": 4000, "memory_bytes": 4 * GIB, "scratch_bytes": GIB})
    pool.reserve("a", "b" * 64, {**effective, "scratch_bytes": 512 * 1024 ** 2}, 60000)
    pool.reserve("b", "b" * 64, {**effective, "scratch_bytes": 512 * 1024 ** 2}, 60000)
    assert pool.snapshot()["charged"]["scratch_bytes"] == GIB
    with pytest.raises(ResourceError) as exc:
        pool.reserve("c", "b" * 64, {**effective, "scratch_bytes": 1}, 60000)
    assert exc.value.code == "CAPACITY_PENDING" and exc.value.retriable
    pool.release("a", {"confirmed": True, "evidence_ref": "exit://a", "identity": None})
    pool.release("b", {"confirmed": True, "evidence_ref": "exit://b", "identity": None})
    assert pool.snapshot()["charged"] == {"cpu_millis": 0, "memory_bytes": 0, "scratch_bytes": 0}


# ---------------- never-spawned reservation withdrawal keeps the id retryable ----------------

def test_capacity_cancel_reservation_keeps_id_retryable(tmp_path):
    pool = LocalCapacityPool(tmp_path / "pool", capacity={"cpu_millis": 4000, "memory_bytes": 4 * GIB})
    effective = {"cpu_millis": 1000, "memory_bytes": GIB}
    pool.reserve("a", "b" * 64, effective, 60000)
    assert pool.cancel_reservation("a")["state"] == "CANCELLED"
    assert pool.snapshot()["charged"] == {"cpu_millis": 0, "memory_bytes": 0}
    pool.reserve("a", "b" * 64, effective, 60000)  # withdrawal, unlike release, keeps the id reusable
    pool.bind_identity("a", {"pid": 123, "start": "x", "unit": {"device": 1, "inode": 1}})
    with pytest.raises(ResourceError) as exc:
        pool.cancel_reservation("a")
    assert exc.value.code == "INVALID_SPEC"  # a bound host needs confirmed exit, never cancel
    pool.release("a", {"confirmed": True, "evidence_ref": "exit://a",
                       "identity": {"pid": 123, "start": "x", "unit": {"device": 1, "inode": 1}}})
    with pytest.raises(ResourceError) as exc:
        pool.cancel_reservation("a")
    assert exc.value.code == "INVALID_SPEC"  # released tombstone stays immutable
    with pytest.raises(ResourceError) as exc:
        pool.cancel_reservation("unknown")
    assert exc.value.code == "INVALID_SPEC"


# ---------------- minor 1.1 additive compatibility ----------------

def test_resource_protocol_minor_additive_compat():
    assert RESOURCE_PROTOCOL_VERSION == {"major": 1, "minor": 1}
    assert validate_budget({"cpu_millis": 1000, "memory_bytes": GIB}) == {
        "cpu_millis": 1000, "memory_bytes": GIB}  # old minor-1.0 document unchanged
    assert validate_budget({"cpu_millis": 1000, "memory_bytes": GIB, "accelerator_gpu": 2})[
        "accelerator_gpu"] == 2
    for key in ("gpu_count", "accelerator_"):
        with pytest.raises(ResourceError) as exc:
            validate_budget({"cpu_millis": 1000, "memory_bytes": GIB, key: 1})
        assert exc.value.code == "INVALID_SPEC"
    assert negotiate_version(RESOURCE_PROTOCOL_VERSION, {"major": 1, "minor": 0}) == {
        "major": 1, "minor": 0}  # min-minor keeps old peers speaking old shape
    WorkerLaunchSpec(
        artifact_refs={"platform": {"digest": "a" * 64, "location": "file:///tmp/platform.whl"}},
        entrypoint="demiflow-worker", argv=("x",), bootstrap_ref="bootstrap-ref",
        access_refs={}, runtime_protocol_version={"major": 1, "minor": 0})  # old launch spec still valid


# ---------------- provider default rejection byte protection (D1) ----------------

def test_local_provider_without_ledger_keeps_exact_rejections(tmp_path):
    provider, _, _ = _provider(tmp_path)
    cases = [
        (_request("a", accelerators=_gpu()), "UNSUPPORTED_CAPABILITY", "Local provider admits CPU/memory only"),
        (_request("a", scratch_bytes=None, accelerators=None,
                  placement={"required": {"device_ids": ["gpu-0"]}}),
         "UNSATISFIABLE_CONSTRAINT", "Local provider has no placement constraints"),
    ]
    custom = _request("a")
    custom["profile"]["requests"]["custom"] = {"fpga": {"amount": 1, "unit": "device"}}
    cases.append((custom, "UNSUPPORTED_CAPABILITY", "Local provider admits CPU/memory only"))
    limits = _request("a")
    limits["profile"]["limits"] = {"cpu_millis": 2000, "memory_bytes": 2 * GIB}
    cases.append((limits, "UNSUPPORTED_CAPABILITY", "Local provider cannot enforce hard physical limits"))
    requirements = _request("a")
    requirements["profile"]["requirements"] = {"accelerator_memory_bytes": GIB}
    cases.append((requirements, "UNSUPPORTED_CAPABILITY", "Local provider cannot verify hardware requirements"))
    for request, code, reason in cases:
        with pytest.raises(ResourceError) as exc:
            provider.describe_and_normalize(None, None, request)
        assert exc.value.code == code and exc.value.reason == reason
    normalized = provider.describe_and_normalize(None, None, _request("plain"))
    assert set(normalized) == {"effective", "spec", "spec_hash"}  # no additive keys without a ledger
    assert normalized["effective"] == {"cpu_millis": 1000, "memory_bytes": GIB}


# ---------------- required/preferred through the real manager receipt ----------------

@contextmanager
def _manager_env(tmp_path, devices, budget):
    with RunContext.start(tmp_path / "run", limits={"max_control_bytes": 16 * 1024 ** 2}) as run:
        provider, pool, unit = _provider(tmp_path, device_pool=devices)
        state = RunResourceState(run.journal, claim_lease_ms=1)
        manager = ResourceManager(provider, state, bounds=ManagerBounds(claim_lease_ms=1))
        scope = ResourceScope("scope-1", "run-1", {"provider": "local"}, "authority-ref", budget)
        state.open_scope(run.fence, scope)
        yield run, provider, pool, unit, state, manager


def _plan(allocation_ids, **request_kwargs):
    return {"scope_id": "scope-1", "desired_workers": [
        {"worker_id": "w-" + a, "allocation_id": a,
         "profile": _request(a, **request_kwargs)["profile"],
         "launch": _launch(_sleep_argv()), "provision_deadline_ms": 60000,
         **({"placement": request_kwargs["placement"]} if request_kwargs.get("placement") else {})}
        for a in allocation_ids], "retirements": []}


def _context(run, operation_id, revision):
    return MutationContext(RESOURCE_PROTOCOL_VERSION, "scope-1", run.attempt, run.fence,
                           operation_id, revision)


def test_preferred_unsatisfiable_returns_mapping_and_warning(tmp_path):
    devices = _devices(tmp_path)
    with _manager_env(tmp_path, devices, {
            "cpu_millis": 8000, "memory_bytes": 8 * GIB,
            "max_workers": 4, "accelerator_gpu": 2}) as (run, provider, pool, unit, state, manager):
        receipt = manager.apply(_context(run, "op-1", 0), _plan(
            ["alloc-a"], accelerators=_gpu(), device_memory_bytes=4 * GIB,
            placement={"preferred": {"device_ids": ["gpu-absent"]}}))
        assert receipt["status"] == "ACCEPTED"
        mapping = receipt["warnings_with_effective_mapping"]["alloc-a"]
        assert mapping["accelerators"] == [{"type": "gpu", "count": 1}]  # effective mapping returned
        warnings = receipt["placement_warnings"]["alloc-a"]
        assert [w["code"] for w in warnings] == ["PREFERRED_UNSATISFIABLE"]
        assert "gpu-absent" in warnings[0]["reason"]
        # apply is intent acceptance only: no reconcile means no spawn/reservation.
        assert pool.snapshot()["charged"] == {"cpu_millis": 0, "memory_bytes": 0}
        assert devices.snapshot()["active"] == []
        assert provider._spawned == {}


def test_required_known_device_has_no_warning_key(tmp_path):
    devices = _devices(tmp_path)
    with _manager_env(tmp_path, devices, {
            "cpu_millis": 8000, "memory_bytes": 8 * GIB,
            "max_workers": 4, "accelerator_gpu": 2}) as (run, provider, pool, unit, state, manager):
        receipt = manager.apply(_context(run, "op-1", 0), _plan(
            ["alloc-a"], accelerators=_gpu(), device_memory_bytes=4 * GIB,
            placement={"required": {"device_ids": ["gpu-0"]}}))
        assert "placement_warnings" not in receipt  # additive key only when non-empty
        with pytest.raises(ResourceError) as exc:
            manager.apply(_context(run, "op-2", 1), _plan(
                ["alloc-b"], accelerators=_gpu(),
                placement={"required": {"device_ids": ["gpu-absent"]}}))
        assert exc.value.code == "UNSATISFIABLE_CONSTRAINT"  # required never silently downgrades
        assert manager.get("scope-1")["allocations"][0]["allocation_id"] == "alloc-a"


# ---------------- additive budget ceilings are fail-closed ----------------

def test_accelerator_budget_ceiling_fail_closed_atomic(tmp_path):
    devices = _devices(tmp_path)
    with _manager_env(tmp_path, devices, {
            "cpu_millis": 8000, "memory_bytes": 8 * GIB,
            "max_workers": 4, "accelerator_gpu": 1}) as (run, provider, pool, unit, state, manager):
        with pytest.raises(ResourceError) as exc:
            manager.apply(_context(run, "op-1", 0), _plan(
                ["alloc-a", "alloc-b"], accelerators=_gpu(), device_memory_bytes=4 * GIB))
        assert exc.value.code == "QUOTA_EXCEEDED" and "accelerator_gpu" in exc.value.reason
        assert manager.get("scope-1")["allocations"] == []  # atomic: no intent rows survive
        with run.journal._connection() as db:
            assert db.execute("SELECT desired_revision FROM resource_scopes "
                              "WHERE scope_id='scope-1'").fetchone()[0] == 0
        plan = _plan(["alloc-a"], accelerators=_gpu(), device_memory_bytes=4 * GIB)
        plan["budgets"] = {"cpu_millis": 8000, "memory_bytes": 8 * GIB, "accelerator_gpu": 2}
        with pytest.raises(ResourceError) as exc:
            manager.apply(_context(run, "op-2", 0), plan)
        assert exc.value.code == "INVALID_SPEC"  # plan budget exceeds authorized scope ceiling


def test_scope_without_additive_budget_rejects_accelerators_and_scratch(tmp_path):
    devices = _devices(tmp_path)
    with _manager_env(tmp_path, devices, {
            "cpu_millis": 8000, "memory_bytes": 8 * GIB,
            "max_workers": 4}) as (run, provider, pool, unit, state, manager):
        with pytest.raises(ResourceError) as exc:
            manager.apply(_context(run, "op-1", 0), _plan(
                ["alloc-a"], accelerators=_gpu(), device_memory_bytes=4 * GIB))
        assert exc.value.code == "QUOTA_EXCEEDED" and "accelerator_gpu" in exc.value.reason
        with pytest.raises(ResourceError) as exc:
            manager.apply(_context(run, "op-2", 0), _plan(["alloc-s"], scratch_bytes=GIB))
        assert exc.value.code == "QUOTA_EXCEEDED" and "scratch_bytes" in exc.value.reason
        assert manager.get("scope-1")["allocations"] == []


# ---------------- device PENDING rolls CPU back; real-process device lifecycle ----------------

def test_device_pending_rolls_back_cpu_then_real_process_lifecycle(tmp_path):
    devices = _devices(tmp_path, count=1)
    provider, pool, unit = _provider(tmp_path, device_pool=devices)
    devices.reserve("holder", "a" * 64, _gpu(), deadline_ms=60000)  # exclusive whole device
    request = _request("alloc-b", accelerators=_gpu(), device_memory_bytes=4 * GIB,
                       argv=_sleep_argv())
    immutable = _immutable(provider, "scope-1", "alloc-b", request)
    try:
        observed = provider.ensure_present("alloc-b", immutable)
        assert observed["phase"] == "PENDING" and "capacity_pending" in observed["conditions"]
        # No hold-and-wait across pools: the CPU reservation was rolled back.
        assert pool.snapshot()["charged"] == {"cpu_millis": 0, "memory_bytes": 0}
        assert provider._spawned == {}
        assert [r["allocation_id"] for r in devices.snapshot()["active"]] == ["holder"]
        devices.release("holder", {"confirmed": True, "evidence_ref": "exit://h", "identity": None})
        observed = provider.ensure_present("alloc-b", immutable)
        assert observed["phase"] == "RUNNING"
        assert observed["placement"] == {"device_ids": ["gpu-0"]}
        assert pool.snapshot()["charged"] == {"cpu_millis": 1000, "memory_bytes": GIB}
        assert devices.snapshot()["charged"] == {"accelerator_gpu": 1}
        result = provider.ensure_absent({"scope_id": "scope-1", "allocation_id": "alloc-b"}, 5000)
        assert result["cleanup"] == "CONFIRMED"
        assert pool.snapshot()["charged"] == {"cpu_millis": 0, "memory_bytes": 0}
        assert devices.snapshot()["charged"] == {}  # both pools release only on confirmed exit
    finally:
        for process in list(provider._spawned.values()):
            if process.poll() is None:
                process.kill()
                process.wait(timeout=5)

def test_elapsed_device_seconds_require_the_original_release(tmp_path, monkeypatch):
    import time
    pool = DevicePool(tmp_path, inventory=_inventory())
    current = [100.]
    monkeypatch.setattr(time, 'time', lambda: current[0])
    spec = 'a' * 64
    pool.reserve('job', spec, [{'type': 'gpu', 'count': 2}], deadline_ms=1000)
    assert pool.released_device_seconds('job', spec, 'gpu') is None
    current[0] += 3.5
    pool.release('job', {'confirmed': True, 'identity': None, 'evidence_ref': 'exit'})
    assert pool.released_device_seconds('job', spec, 'gpu') == 7.
    with pytest.raises(ResourceError, match='original device allocation spec changed'):
        pool.released_device_seconds('job', 'b'*64, 'gpu')
