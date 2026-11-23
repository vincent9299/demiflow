"""U2 RES conformance: resource protocol v1 anomaly table against the REAL
RunResourceState + RunJournal native authority, with a declared fake provider.

The fake provider isolates protocol/state-machine semantics from physical
process behaviour; real Local subprocess evidence lives in
test_u2_resources_local.py. These tests never claim GPU/multi-host coverage.
"""
import time

import pytest

from demiflow.execution.run_context import RunContext
from demiflow.execution.hosts.run_state import RunResourceState
from demiflow.resources.contracts import (
    RESOURCE_PROTOCOL_VERSION, MutationContext, ResourceError, ResourceProfile,
    ResourceScope, WorkerLaunchSpec, WorkerRequest,
)
from demiflow.resources.manager import ManagerBounds, ResourceManager


class FakeProvider:
    """Declared test double: scriptable phase/cleanup, records every call."""

    def __init__(self):
        self.present_calls = []
        self.absent_calls = []
        self.phase = "RUNNING"
        self.freshness = "FRESH"
        self.cleanup = "CONFIRMED"
        self.pending = False

    def describe_and_normalize(self, binding, requirements, spec):
        from demiflow.resources.contracts import spec_hash
        requests = spec["profile"]["requests"]
        return {"effective": {"cpu_millis": requests["cpu_millis"],
                              "memory_bytes": requests["memory_bytes"]},
                "spec": spec, "spec_hash": spec_hash(spec)}

    def ensure_present(self, allocation_id, spec):
        self.present_calls.append(allocation_id)
        now = int(time.time() * 1000)
        phase = "PENDING" if self.pending else self.phase
        return {"allocation_id": allocation_id, "provider_uid": "fake-" + allocation_id,
                "boot_id": None, "phase": phase, "observation": self.freshness,
                "observed_at_ms": now, "observation_revision": now, "cleanup": "NONE",
                "effective_resources": spec["effective"], "conditions": [],
                "evidence_ref": "fake://present", "placement": None,
                "ownership": {"scope_owned": [allocation_id], "shared": []}}

    def inspect(self, scope_id, page_token):
        return {"allocations": [], "next_page_token": None,
                "sampled_at_ms": int(time.time() * 1000)}

    def ensure_absent(self, identity, terminate_deadline_ms):
        self.absent_calls.append(identity["allocation_id"])
        return {"allocation_id": identity["allocation_id"], "cleanup": self.cleanup,
                "evidence_ref": "fake://absent", "observed_at_ms": int(time.time() * 1000)}


def launch_spec():
    return WorkerLaunchSpec(
        artifact_refs={"platform": {"digest": "a" * 64, "location": "file:///tmp/platform.whl"}},
        entrypoint="demiflow-worker", argv=("demiflow-worker",),
        bootstrap_ref="bootstrap-ref", access_refs={},
        runtime_protocol_version=RESOURCE_PROTOCOL_VERSION)


def plan_payload(scope_id, allocations, retirements=(), cpu=1000, mem=1024 ** 3, deadline=60000):
    return {"scope_id": scope_id, "desired_workers": [
        {"worker_id": "w-" + a, "allocation_id": a,
         "profile": {"requests": {"cpu_millis": cpu, "memory_bytes": mem}},
         "launch": launch_spec().to_dict(), "provision_deadline_ms": deadline}
        for a in allocations],
        "retirements": list(retirements)}


def release(allocation_id, mode="DRAINED"):
    return {"allocation_id": allocation_id, "mode": mode,
            "engine_receipt_ref": "drain-" + allocation_id,
            "terminate_deadline_ms": 5000, "reason": "scale in"}


@pytest.fixture
def env(tmp_path):
    with RunContext.start(tmp_path / "run", limits={"max_control_bytes": 16 * 1024 ** 2}) as run:
        state = RunResourceState(run.journal, claim_lease_ms=1)
        provider = FakeProvider()
        manager = ResourceManager(provider, state, bounds=ManagerBounds(claim_lease_ms=1))
        scope = ResourceScope("scope-1", "run-1", {"provider": "fake"}, "authority-ref",
                              {"cpu_millis": 8000, "memory_bytes": 8 * 1024 ** 3, "max_workers": 4})
        state.open_scope(run.fence, scope)
        yield run, state, provider, manager


def context(run, operation_id, expected_revision, scope_id="scope-1", fence=None, epoch=None):
    return MutationContext(RESOURCE_PROTOCOL_VERSION, scope_id,
                           run.attempt if epoch is None else epoch,
                           run.fence if fence is None else fence,
                           operation_id, expected_revision)


def reconcile(manager, run):
    time.sleep(0.01)
    return manager.reconcile_once(run.fence, time.monotonic() + 10, 16)


def scope_state(run, scope_id="scope-1"):
    with run.journal._connection() as db:
        return db.execute("SELECT state FROM resource_scopes WHERE scope_id=?", (scope_id,)).fetchone()[0]


def test_res_apply_idempotent_retry_and_payload_conflict(env):
    run, state, provider, manager = env
    payload = plan_payload("scope-1", ["alloc-a"])
    receipt = manager.apply(context(run, "op-1", 0), payload)
    assert receipt["status"] == "ACCEPTED" and receipt["desired_revision"] == 1
    assert manager.apply(context(run, "op-1", 0), payload) == receipt
    with pytest.raises(ResourceError) as exc:
        manager.apply(context(run, "op-1", 0), plan_payload("scope-1", []))
    assert exc.value.code == "IDEMPOTENCY_CONFLICT"
    assert not provider.present_calls  # acceptance alone performs no provider I/O


def test_res_stale_revision_conflict(env):
    run, state, provider, manager = env
    manager.apply(context(run, "op-1", 0), plan_payload("scope-1", ["alloc-a"]))
    with pytest.raises(ResourceError) as exc:
        manager.apply(context(run, "op-2", 0), plan_payload("scope-1", ["alloc-b"]))
    assert exc.value.code == "REVISION_CONFLICT"
    assert exc.value.observed_revision == 1


def test_res_old_epoch_and_old_fence_are_fenced(env):
    run, state, provider, manager = env
    with pytest.raises(ResourceError) as exc:
        manager.apply(context(run, "op-1", 0, epoch=run.attempt + 99), plan_payload("scope-1", ["alloc-a"]))
    assert exc.value.code == "FENCED"
    configuration = run.journal.recovery_identity()
    run.journal.claim_execution(configuration, "replacement-fence")
    with pytest.raises(ResourceError) as exc:
        manager.apply(context(run, "op-2", 0), plan_payload("scope-1", ["alloc-a"]))
    assert exc.value.code == "FENCED"
    with pytest.raises(ResourceError) as exc:
        state.claim_operations(run.fence, int(time.time() * 1000), 16)
    assert exc.value.code == "FENCED"
    # Restore ownership so the RunContext fixture can finish cleanly.
    run.journal.claim_execution(configuration, run.fence)


def test_res_cancel_tombstone_suppresses_late_present(env):
    run, state, provider, manager = env
    manager.apply(context(run, "op-1", 0), plan_payload("scope-1", ["alloc-a"]))
    manager.apply(context(run, "op-2", 1),
                  plan_payload("scope-1", [], retirements=[release("alloc-a", "ABANDON")]))
    reconcile(manager, run)
    assert provider.present_calls == []          # late create suppressed by tombstone
    assert provider.absent_calls == ["alloc-a"]  # cleanup obligation still executed


def test_res_partial_allocation_pending_is_reported_not_failed(env):
    run, state, provider, manager = env
    provider.pending = True
    manager.apply(context(run, "op-1", 0), plan_payload("scope-1", ["alloc-a", "alloc-b"]))
    reconcile(manager, run)
    snapshot = manager.get("scope-1")
    phases = {a["allocation_id"]: a["phase"] for a in snapshot["allocations"]}
    assert phases == {"alloc-a": "PENDING", "alloc-b": "PENDING"}
    assert {p["allocation_id"] for p in snapshot["pending_and_shortfall"]} == {"alloc-a", "alloc-b"}
    assert scope_state(run) == "OPEN"  # partial progress never fabricates failure or completion


def test_res_unknown_observation_does_not_release_capacity(env):
    run, state, provider, manager = env
    provider.freshness = "STALE_UNKNOWN"
    manager.apply(context(run, "op-1", 0), plan_payload("scope-1", ["alloc-a"]))
    reconcile(manager, run)
    snapshot = manager.get("scope-1")
    assert snapshot["allocations"][0]["observation"] == "STALE_UNKNOWN"
    assert snapshot["freshness"]["stale_unknown"] == ["alloc-a"]
    # close cannot reach CLOSED while the host fact is unknown/unconfirmed
    provider.cleanup = "UNCONFIRMED"
    manager.close(context(run, "op-close", 1),
                  {"mode": "ABANDON", "engine_receipt_ref": "abort-ref",
                   "terminate_deadline_ms": 5000, "reason": "lost"})
    reconcile(manager, run)
    assert scope_state(run) == "CLOSING"
    assert manager.get("scope-1")["cleanup_obligations"]


def test_res_unconfirmed_cleanup_keeps_charge_then_confirms(env):
    run, state, provider, manager = env
    manager.apply(context(run, "op-1", 0), plan_payload("scope-1", ["alloc-a"]))
    reconcile(manager, run)
    provider.cleanup = "UNCONFIRMED"
    manager.apply(context(run, "op-2", 1),
                  plan_payload("scope-1", [], retirements=[release("alloc-a")]))
    reconcile(manager, run)
    snapshot = manager.get("scope-1")
    assert snapshot["allocations"][0]["cleanup"] == "UNCONFIRMED"
    assert snapshot["cleanup_obligations"][0]["allocation_id"] == "alloc-a"
    provider.cleanup = "CONFIRMED"
    reconcile(manager, run)
    snapshot = manager.get("scope-1")
    assert snapshot["allocations"][0]["cleanup"] == "CONFIRMED"
    assert snapshot["cleanup_obligations"] == []


def test_res_provision_deadline_cancels_unfinished_request(env):
    run, state, provider, manager = env
    manager.apply(context(run, "op-1", 0), plan_payload("scope-1", ["alloc-a"], deadline=1))
    time.sleep(0.02)
    reconcile(manager, run)
    assert provider.present_calls == []           # never started past its deadline
    assert provider.absent_calls == ["alloc-a"]   # unfinished request cancelled and cleaned
    events = manager.watch("scope-1", 0, 100, 0)["events"]
    assert any(e["kind"] == "provision_deadline_exceeded" for e in events)


def test_res_terminal_phase_never_regresses(env):
    run, state, provider, manager = env
    manager.apply(context(run, "op-1", 0), plan_payload("scope-1", ["alloc-a"]))
    reconcile(manager, run)
    base = int(time.time() * 1000)

    def observation(phase, revision):
        return {"allocation_id": "alloc-a", "provider_uid": "fake-alloc-a", "boot_id": None,
                "phase": phase, "observation": "FRESH", "observed_at_ms": revision,
                "observation_revision": revision, "cleanup": "NONE", "effective_resources": None,
                "conditions": [], "evidence_ref": None, "placement": None,
                "ownership": {"scope_owned": ["alloc-a"], "shared": []}}

    state.record_observations(run.fence, [observation("TERMINATED", base + 1000)])
    assert manager.get("scope-1")["allocations"][0]["phase"] == "TERMINATED"
    # A late RUNNING observation with a higher revision must not revive it.
    state.record_observations(run.fence, [observation("RUNNING", base + 10 ** 9)])
    assert manager.get("scope-1")["allocations"][0]["phase"] == "TERMINATED"


def test_res_close_requires_all_obligations_confirmed(env):
    run, state, provider, manager = env
    manager.apply(context(run, "op-1", 0), plan_payload("scope-1", ["alloc-a", "alloc-b"]))
    reconcile(manager, run)
    provider.cleanup = "UNCONFIRMED"
    receipt = manager.close(context(run, "op-close", 1),
                            {"mode": "DRAINED", "engine_receipt_ref": "drain-ref",
                             "terminate_deadline_ms": 5000, "reason": "run ended"})
    assert receipt["status"] == "CLOSING"
    reconcile(manager, run)
    assert scope_state(run) == "CLOSING"  # empty inventory is not proof; obligations remain
    provider.cleanup = "CONFIRMED"
    reconcile(manager, run)
    reconcile(manager, run)
    assert scope_state(run) == "CLOSED"
    with pytest.raises(ResourceError) as exc:
        manager.apply(context(run, "op-late", 2), plan_payload("scope-1", ["alloc-c"]))
    assert exc.value.code == "SCOPE_CLOSED"


def test_res_watch_cursor_expiry_requires_resync(env):
    run, state, provider, manager = env
    state_small = RunResourceState(run.journal, event_window=4, claim_lease_ms=1)
    manager_small = ResourceManager(provider, state_small, bounds=ManagerBounds(event_window=4, claim_lease_ms=1))
    manager_small.apply(context(run, "op-1", 0), plan_payload("scope-1", ["alloc-a"]))
    reconcile(manager_small, run)
    base = int(time.time() * 1000) + 1000
    for index in range(8):
        state_small.record_observations(run.fence, [
            {"allocation_id": "alloc-a", "observation": "STALE_UNKNOWN",
             "observed_at_ms": base + index, "observation_revision": base + index,
             "conditions": ["probe"]}])
    with pytest.raises(ResourceError) as exc:
        manager_small.watch("scope-1", 0, 4, 0)
    assert exc.value.code == "RESYNC_REQUIRED"
    snapshot = manager_small.get("scope-1")  # rebuild from a consistent snapshot
    assert snapshot["cursor"] > 0


def test_res_operation_timeout_does_not_cancel_accepted_intent(env):
    run, state, provider, manager = env
    # Client never calls reconcile (simulates a timed-out call); the accepted
    # intent stays pending and a later reconcile still fulfils it.
    manager.apply(context(run, "op-1", 0), plan_payload("scope-1", ["alloc-a"]))
    assert provider.present_calls == []
    assert manager.get("scope-1")["desired_revision"] == 1
    reconcile(manager, run)
    assert provider.present_calls == ["alloc-a"]
    assert manager.get("scope-1")["allocations"][0]["phase"] == "RUNNING"


def test_res_budget_ceiling_rejects_overcommit(env):
    run, state, provider, manager = env
    with pytest.raises(ResourceError) as exc:
        manager.apply(context(run, "op-1", 0), plan_payload("scope-1", ["alloc-a"], cpu=9000))
    assert exc.value.code == "QUOTA_EXCEEDED"
    assert manager.get("scope-1")["desired_revision"] == 0  # rejected atomically, no intent


def test_res_live_worker_removal_requires_release_spec(env):
    run, state, provider, manager = env
    manager.apply(context(run, "op-1", 0), plan_payload("scope-1", ["alloc-a"]))
    reconcile(manager, run)
    with pytest.raises(ResourceError) as exc:
        manager.apply(context(run, "op-2", 1), plan_payload("scope-1", []))
    assert exc.value.code == "INVALID_SPEC"