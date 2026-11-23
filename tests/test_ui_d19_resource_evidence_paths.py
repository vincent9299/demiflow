"""Real Local lifecycle uses the unified bounded, byte-bound evidence contract.

The resolver returns the original JSON document, not a filesystem path. Legacy
absolute release references remain immutable ledger facts, not new read grants.
"""
import json
from pathlib import Path
import sys
import time

import pytest

import demiflow
from demiflow.execution.run_context import RunContext
from demiflow.execution.hosts.run_state import RunResourceState
from demiflow.resources.capacity import LocalCapacityPool
from demiflow.resources.contracts import (
    RESOURCE_PROTOCOL_VERSION, MutationContext, ResourceError, ResourceScope,
    WorkerLaunchSpec,
)
from demiflow.resources.device_pool import DevicePool
from demiflow.resources.local import LocalResourceProvider
from demiflow.resources.manager import ManagerBounds, ResourceManager
from demiflow.resources.process_observation import require_pidfds
from demiflow.resources.process_unit import ManagedProcessUnit


SCOPE = "df-" + "a" * 64
ALLOCATION = "b" * 32
pytestmark = pytest.mark.skipif(not sys.platform.startswith("linux"), reason="Local units require Linux")


@pytest.fixture(params=["short", "long"])
def local_environment(tmp_path, request):
    assert Path(demiflow.__file__).resolve().parents[1] == Path(__file__).resolve().parents[1]
    try:
        require_pidfds()
    except (OSError, RuntimeError) as exc:
        pytest.skip("pidfd exit evidence unavailable: " + str(exc))
    root = tmp_path / "units"
    if request.param == "long":
        root = root / ("workspace-" + "x" * 120)
    unit = ManagedProcessUnit(root)
    process_path = unit.directory(SCOPE, ALLOCATION) / "process.json"
    assert (len(str(process_path)) > 256) == (request.param == "long")
    pool = LocalCapacityPool(tmp_path / "pool", capacity={"cpu_millis": 4000, "memory_bytes": 256 * 1024 ** 2})
    provider = LocalResourceProvider(pool, unit)
    with RunContext.start(tmp_path / "run", limits={"max_control_bytes": 16 * 1024 ** 2}) as run:
        state = RunResourceState(run.journal, claim_lease_ms=1)
        manager = ResourceManager(provider, state, bounds=ManagerBounds(claim_lease_ms=1))
        state.open_scope(run.fence, ResourceScope(
            SCOPE, "evidence-test", {"provider": "local"}, "authority-ref",
            {"cpu_millis": 4000, "memory_bytes": 256 * 1024 ** 2, "max_workers": 1}))
        try:
            yield run, unit, pool, provider, manager
        finally:
            # The RUNNING regression may fail after spawn. Reap only this
            # fixture's child, using its real birth identity and cleanup proof.
            for process in list(provider._spawned.values()):
                try:
                    evidence = unit.stop(SCOPE, ALLOCATION, terminate_deadline_ms=1000, process=process)
                    if evidence["confirmed"]:
                        pool.release(ALLOCATION, evidence)
                finally:
                    if process.poll() is None:
                        process.kill()
                    process.wait(timeout=5)


def _context(run, operation, revision):
    return MutationContext(RESOURCE_PROTOCOL_VERSION, SCOPE, run.attempt, run.fence, operation, revision)


def _reconcile(manager, run):
    manager.reconcile_once(run.fence, time.monotonic() + 10, 16)


def _start(environment):
    run, unit, pool, provider, manager = environment
    launch = WorkerLaunchSpec(
        artifact_refs={"platform": {"digest": "a" * 64, "location": "file:///tmp/platform.whl"}},
        entrypoint="demiflow-worker",
        argv=(sys.executable, "-B", "-c", "import time; time.sleep(30)"),
        bootstrap_ref="bootstrap-ref", access_refs={}, runtime_protocol_version=RESOURCE_PROTOCOL_VERSION)
    manager.apply(_context(run, "start", 0), {
        "scope_id": SCOPE, "desired_workers": [{
            "worker_id": "worker", "allocation_id": ALLOCATION,
            "profile": {"requests": {"cpu_millis": 1000, "memory_bytes": 32 * 1024 ** 2}},
            "launch": launch.to_dict(), "provision_deadline_ms": 60000}], "retirements": []})
    _reconcile(manager, run)
    process = provider._spawned[ALLOCATION]
    assert process.poll() is None
    record = json.loads((unit.directory(SCOPE, ALLOCATION) / "process.json").read_text())
    assert record["identity"]["pid"] == process.pid
    assert pool.snapshot()["charged"]["cpu_millis"] == 1000
    return process, manager.get(SCOPE)["allocations"][0]


def test_real_provider_running_with_bounded_evidence(local_environment):
    _, unit, _, provider, _ = local_environment
    _, observation = _start(local_environment)
    assert observation["phase"] == "RUNNING", observation
    assert 0 < len(observation["evidence_ref"]) <= 256
    # Inventory/adoption must publish the same evidence as initial creation.
    inspected = provider.inspect(SCOPE, None)["allocations"][0]
    assert inspected["phase"] == "RUNNING"
    assert inspected["evidence_ref"] == observation["evidence_ref"]
    evidence_path = unit.directory(SCOPE, ALLOCATION) / "process.json"
    original = json.loads(evidence_path.read_text())
    assert unit.resolve_evidence(SCOPE, ALLOCATION, observation["evidence_ref"]) == original
    recovered = ManagedProcessUnit(unit.root)
    assert recovered.inspect(SCOPE, ALLOCATION)["evidence_ref"] == observation["evidence_ref"]
    assert recovered.resolve_evidence(SCOPE, ALLOCATION, observation["evidence_ref"]) == original


def test_real_provider_cleanup_with_bounded_evidence(local_environment):
    run, unit, pool, _, manager = local_environment
    process, _ = _start(local_environment)
    # Cleanup must close the real spawned allocation even if a prior
    # observation failed validation; it is an independent lifecycle obligation.
    manager.close(_context(run, "close", 1), {
        "mode": "DRAINED", "engine_receipt_ref": "drain-ref",
        "terminate_deadline_ms": 3000, "reason": "test completed"})
    _reconcile(manager, run)
    snapshot = manager.get(SCOPE)
    observation = snapshot["allocations"][0]
    assert observation["cleanup"] == "CONFIRMED"
    assert 0 < len(observation["evidence_ref"]) <= 256
    assert snapshot["cleanup_obligations"] == []
    assert process.poll() is not None
    assert pool.snapshot()["charged"] == {"cpu_millis": 0, "memory_bytes": 0}
    proof = json.loads((unit.directory(SCOPE, ALLOCATION) / "cleanup.json").read_text())
    assert proof["unit_stopped"] and proof["birth_identity_gone"] and proof["descendants_empty"]
    assert unit.resolve_evidence(SCOPE, ALLOCATION, observation["evidence_ref"]) == proof
    with run.journal._connection() as db:
        assert db.execute("SELECT state FROM resource_scopes WHERE scope_id=?", (SCOPE,)).fetchone()[0] == "CLOSED"


@pytest.mark.parametrize("filename", ["identity.json", "process.json", "cleanup.json"])
def test_evidence_resolution_is_allocation_bound(local_environment, filename):
    _, unit, _, _, _ = local_environment
    directory = unit.prepare(SCOPE, ALLOCATION, "c" * 64)
    path = directory / filename
    if not path.exists():
        path.write_text(json.dumps({"fixture_record": filename}))
    original = json.loads(path.read_text())
    reference = unit.evidence_ref(SCOPE, ALLOCATION, filename)
    assert 0 < len(reference) <= 256
    assert reference.startswith("df-resource-evidence:v1:")
    assert unit.resolve_evidence(SCOPE, ALLOCATION, reference) == original
    # An absolute path is not a content/owner-bound reference. Preserving old
    # release ledgers is tested with real cleanup below, without treating those
    # historical strings as permission to open an arbitrary file now.
    with pytest.raises(ResourceError, match="evidence"):
        unit.resolve_evidence(SCOPE, ALLOCATION, str(path))
    for scope, allocation, invalid in (
        ("another-scope", ALLOCATION, reference),
        (SCOPE, "another-allocation", reference),
        (SCOPE, ALLOCATION, str(directory / "host.log")),
        (SCOPE, ALLOCATION, reference + "/../host.log"),
        (SCOPE, ALLOCATION, str(directory / ".." / ALLOCATION / filename)),
    ):
        with pytest.raises(ResourceError, match="evidence"):
            unit.resolve_evidence(scope, allocation, invalid)
    for scope, allocation in (("../another-scope", ALLOCATION), (SCOPE, "../another-allocation")):
        with pytest.raises(ResourceError):
            unit.resolve_evidence(scope, allocation, reference)
    path.write_text(json.dumps({"changed": filename}))
    with pytest.raises(ResourceError) as exc:
        unit.resolve_evidence(SCOPE, ALLOCATION, reference)
    assert exc.value.code == "ARTIFACT_MISMATCH"


@pytest.mark.parametrize("symlink_level", ["record", "allocation", "scope"])
def test_evidence_resolver_rejects_symlink_escape(tmp_path, symlink_level):
    unit = ManagedProcessUnit(tmp_path / "units")
    directory = unit.prepare(SCOPE, ALLOCATION, "c" * 64)
    outside = tmp_path / "outside.json"
    outside.write_text('{"unrelated":true}')
    if symlink_level == "record":
        (directory / "cleanup.json").symlink_to(outside)
    else:
        original = directory if symlink_level == "allocation" else directory.parent
        target = tmp_path / ("outside-" + symlink_level)
        original.rename(target)
        original.symlink_to(target, target_is_directory=True)
        proof_directory = target if symlink_level == "allocation" else target / ALLOCATION
        (proof_directory / "cleanup.json").write_bytes(outside.read_bytes())
    # Reject at reference creation or resolution; neither may turn an outside
    # file into this allocation's authenticated evidence.
    with pytest.raises(ResourceError, match="evidence"):
        reference = unit.evidence_ref(SCOPE, ALLOCATION, "cleanup.json")
        unit.resolve_evidence(SCOPE, ALLOCATION, reference)


@pytest.mark.parametrize("characters", [256, 257])
def test_evidence_ref_preserves_contract_length_boundary(tmp_path, characters):
    # Non-ASCII verifies the frozen contract's character count, not UTF-8 bytes.
    suffix = "/scope/allocation/process.json"
    padding = characters - len(str(tmp_path)) - 1 - len(suffix)
    unit = ManagedProcessUnit(tmp_path / ("界" + "x" * (padding - 1)))
    path = unit.directory("scope", "allocation") / "process.json"
    assert len(str(path)) == characters
    path.parent.mkdir(parents=True)
    original = {"identity": {"pid": 123, "start": "456"}}
    path.write_text(json.dumps(original))
    reference = unit.evidence_ref("scope", "allocation", "process.json")
    assert len(reference) <= 256
    assert reference.startswith("df-resource-evidence:v1:")
    assert unit.resolve_evidence("scope", "allocation", reference) == original


@pytest.mark.parametrize("released_before_retry", ["none", "cpu", "cpu-and-device"])
def test_real_provider_cleanup_retries_preserve_legacy_ledger(tmp_path, released_before_retry):
    """Replay pre-upgrade partial cleanup with real process and both ledgers.

    The device is a declared logical inventory, not a physical GPU claim.
    Previously persisted release evidence must remain byte-for-byte unchanged.
    """
    assert Path(demiflow.__file__).resolve().parents[1] == Path(__file__).resolve().parents[1]
    try:
        require_pidfds()
    except (OSError, RuntimeError) as exc:
        pytest.skip("pidfd exit evidence unavailable: " + str(exc))
    unit = ManagedProcessUnit(tmp_path / ("workspace-" + "x" * 120) / "units")
    cleanup_path = unit.directory(SCOPE, ALLOCATION) / "cleanup.json"
    assert len(str(cleanup_path)) > 256
    pool = LocalCapacityPool(tmp_path / "pool", capacity={"cpu_millis": 2000, "memory_bytes": 128 * 1024 ** 2})
    devices = DevicePool(tmp_path / "devices", inventory={"devices": [
        {"device_id": "logical-gpu", "type": "gpu", "memory_bytes": 64 * 1024 ** 2}]})
    provider = LocalResourceProvider(pool, unit, device_pool=devices)
    launch = WorkerLaunchSpec(
        artifact_refs={"platform": {"digest": "a" * 64, "location": "file:///tmp/platform.whl"}},
        entrypoint="demiflow-worker", argv=(sys.executable, "-B", "-c", "import time; time.sleep(30)"),
        bootstrap_ref="bootstrap-ref", access_refs={}, runtime_protocol_version=RESOURCE_PROTOCOL_VERSION)
    request = {"worker_id": "worker", "allocation_id": ALLOCATION,
               "profile": {"requests": {"cpu_millis": 1000, "memory_bytes": 32 * 1024 ** 2,
                                        "accelerators": [{"type": "gpu", "count": 1}]}},
               "launch": launch.to_dict(), "provision_deadline_ms": 60000}
    normalized = provider.describe_and_normalize({}, None, request)
    immutable = {"scope_id": SCOPE, "allocation_id": ALLOCATION, "request": request,
                 "spec_hash": normalized["spec_hash"], "effective": normalized["effective"],
                 "requirements": None, "binding": {}}
    process = None
    try:
        observation = provider.ensure_present(ALLOCATION, immutable)
        assert observation["phase"] == "RUNNING"
        process = provider._spawned[ALLOCATION]
        assert process.poll() is None
        assert devices.snapshot()["charged"] == {"accelerator_gpu": 1}
        evidence = unit.stop(SCOPE, ALLOCATION, terminate_deadline_ms=1000, process=process)
        assert evidence["confirmed"] and process.poll() is not None
        historical = dict(evidence, evidence_ref=str(cleanup_path))
        historical_device = dict(historical, identity=None)
        cpu_record = pool.root / "allocations" / (ALLOCATION + ".json")
        device_record = devices.root / "allocations" / (ALLOCATION + ".json")
        persisted = {}
        if released_before_retry != "none":
            pool.release(ALLOCATION, historical)
            persisted[cpu_record] = cpu_record.read_bytes()
        if released_before_retry == "cpu-and-device":
            devices.release(ALLOCATION, historical_device)
            persisted[device_record] = device_record.read_bytes()
        recovered = LocalResourceProvider(pool, ManagedProcessUnit(unit.root), device_pool=devices)
        exact = {"scope_id": SCOPE, "allocation_id": ALLOCATION}
        confirmed_bytes = cleanup_path.read_bytes()
        new_release_reference = None
        for _ in range(2):
            result = recovered.ensure_absent(exact, 1000)
            assert result["cleanup"] == "CONFIRMED"
            assert 0 < len(result["evidence_ref"]) <= 256
            assert unit.resolve_evidence(SCOPE, ALLOCATION, result["evidence_ref"]) == json.loads(confirmed_bytes)
            assert cleanup_path.read_bytes() == confirmed_bytes
            if new_release_reference is None:
                new_release_reference = result["evidence_ref"]
            assert result["evidence_ref"] == new_release_reference
            assert pool.snapshot()["charged"] == {"cpu_millis": 0, "memory_bytes": 0}
            assert devices.snapshot()["charged"] == {}
            for path, original in persisted.items():
                assert path.read_bytes() == original
            for path in (cpu_record, device_record):
                expected = str(cleanup_path) if path in persisted else new_release_reference
                assert json.loads(path.read_text())["release_evidence"] == expected
        # Each ledger retries its own original proof. A new release need not
        # fabricate an old path; a prior old-path release must never be changed.
        cpu_evidence = dict(evidence, evidence_ref=json.loads(cpu_record.read_text())["release_evidence"])
        device_evidence = dict(cpu_evidence, identity=None,
            evidence_ref=json.loads(device_record.read_text())["release_evidence"])
        assert pool.release(ALLOCATION, cpu_evidence)["idempotent"]
        assert devices.release(ALLOCATION, device_evidence)["idempotent"]
        # Preserving compatibility must not disable the ledger's exact check.
        for ledger, original in ((pool, cpu_evidence), (devices, device_evidence)):
            with pytest.raises(ResourceError) as exc:
                ledger.release(ALLOCATION, dict(original, evidence_ref="unrelated-evidence"))
            assert exc.value.code == "CLEANUP_UNCONFIRMED"
    finally:
        process = process or provider._spawned.get(ALLOCATION)
        if process is not None:
            if process.poll() is None:
                process.kill()
            process.wait(timeout=5)
