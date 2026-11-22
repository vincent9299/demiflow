"""U2 Local real-process evidence: capacity pool, managed process unit and the
LocalResourceProvider driven through the real ResourceManager + RunResourceState.

These spawn genuine subprocesses (declared minimal test hosts) and verify birth
identity, unit-lock exit evidence, descendant cleanup, capacity charging and
PID-reuse fencing. No GPU/multi-host claims; Ray evidence is a separate suite.
"""
import json
import os
import signal
import sys
import time
from pathlib import Path

import pytest

from demiflow.execution.run_context import RunContext
from demiflow.execution.resource_state import RunResourceState
from demiflow.resources.capacity import (
    ENVELOPE_ENV, LocalCapacityPool, inherited_envelope, write_envelope,
)
from demiflow.resources.contracts import (
    RESOURCE_PROTOCOL_VERSION, MutationContext, ResourceError, ResourceProfile,
    ResourceScope, WorkerLaunchSpec,
)
from demiflow.resources.local import LocalResourceProvider
from demiflow.resources.manager import ManagerBounds, ResourceManager
from demiflow.resources.process_unit import (
    ManagedProcessUnit, _signal_birth, birth_start,
)
from demiflow.resources.process_observation import require_pidfds, unit_stopped

pytestmark = pytest.mark.skipif(not sys.platform.startswith("linux"), reason="Local managed units require Linux")


@pytest.fixture(autouse=True)
def pidfds_available():
    try:
        require_pidfds()
    except (OSError, RuntimeError) as exc:
        pytest.skip("pidfd exit evidence unavailable: " + str(exc))


def sleep_argv(seconds):
    return (sys.executable, "-B", "-c", "import time; time.sleep(%r)" % seconds)


def wait_until(predicate, timeout=5.0):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return True
        time.sleep(0.02)
    return predicate()


def pid_alive(pid):
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    return True


def test_capacity_reserve_bind_release_lifecycle(tmp_path):
    pool = LocalCapacityPool(tmp_path / "pool", capacity={"cpu_millis": 4000, "memory_bytes": 4 * 1024 ** 3})
    effective = {"cpu_millis": 1000, "memory_bytes": 1024 ** 3}
    view = pool.reserve("alloc-a", "b" * 64, effective, 60000)
    assert view["state"] == "RESERVED" and view["identity"] is None
    assert pool.reserve("alloc-a", "b" * 64, effective, 60000) == view  # idempotent
    identity = {"pid": os.getpid(), "start": birth_start(os.getpid()),
                "unit": {"device": 1, "inode": 1}}
    bound = pool.bind_identity("alloc-a", identity)
    assert bound["state"] == "BOUND" and bound["identity"] == identity
    assert pool.snapshot()["charged"] == {"cpu_millis": 1000, "memory_bytes": 1024 ** 3}
    with pytest.raises(ResourceError) as exc:
        pool.release("alloc-a", {"confirmed": False, "evidence_ref": None, "identity": identity})
    assert exc.value.code == "CLEANUP_UNCONFIRMED"
    assert pool.snapshot()["charged"]["cpu_millis"] == 1000  # unknown keeps charging
    released = pool.release("alloc-a", {"confirmed": True, "evidence_ref": "exit://a", "identity": identity})
    assert released["state"] == "RELEASED"
    assert pool.snapshot()["charged"] == {"cpu_millis": 0, "memory_bytes": 0}
    assert pool.release("alloc-a", {"confirmed": True, "evidence_ref": "exit://a", "identity": identity})["idempotent"]


def test_capacity_pending_when_full_and_spec_immutable(tmp_path):
    pool = LocalCapacityPool(tmp_path / "pool", capacity={"cpu_millis": 2000, "memory_bytes": 2 * 1024 ** 3})
    effective = {"cpu_millis": 1000, "memory_bytes": 1024 ** 3}
    pool.reserve("alloc-a", "b" * 64, effective, 60000)
    pool.reserve("alloc-b", "c" * 64, effective, 60000)
    with pytest.raises(ResourceError) as exc:
        pool.reserve("alloc-c", "d" * 64, effective, 60000)
    assert exc.value.code == "CAPACITY_PENDING" and exc.value.retriable
    with pytest.raises(ResourceError) as exc:
        pool.reserve("alloc-a", "e" * 64, effective, 60000)
    assert exc.value.code == "INVALID_SPEC"  # immutable spec
    pool.release("alloc-a", {"confirmed": True, "evidence_ref": "exit://a", "identity": None})
    pool.reserve("alloc-c", "d" * 64, effective, 60000)  # freed capacity admits the waiter
    with pytest.raises(ResourceError) as exc:
        pool.reserve("alloc-a", "b" * 64, effective, 60000)
    assert exc.value.code == "INVALID_SPEC"  # released id cannot reserve again


def test_process_unit_spawn_inspect_stop_confirmed(tmp_path):
    unit = ManagedProcessUnit(tmp_path / "units")
    spawned = unit.spawn("scope-1", "alloc-a", "f" * 64, sleep_argv(30))
    process = spawned["process"]
    identity = spawned["identity"]
    try:
        found = unit.inspect("scope-1", "alloc-a", process=process)
        assert found["phase"] == "RUNNING" and found["observation"] == "FRESH"
        assert pid_alive(identity["pid"])
        lock = unit.directory("scope-1", "alloc-a") / "unit.lock"
        assert not unit_stopped(lock, identity["unit"])
        saved = lock.rename(lock.with_name("original.lock"))
        try:
            lock.touch()
            changed = unit.inspect("scope-1", "alloc-a", process=process)
            assert changed["observation"] == "STALE_UNKNOWN"
            assert not unit_stopped(lock, identity["unit"])
        finally:
            saved.replace(lock)
        evidence = unit.stop("scope-1", "alloc-a", terminate_deadline_ms=3000, process=process)
        assert evidence["confirmed"] is True
        assert evidence["evidence_ref"].endswith("cleanup.json")
        assert not pid_alive(identity["pid"])
        assert process.returncode is not None  # reaped, not a zombie
        after = unit.inspect("scope-1", "alloc-a", process=process)
        assert after["phase"] == "TERMINATED" and after["observation"] == "FRESH"
    finally:
        if process.poll() is None:
            process.kill()
            process.wait(timeout=5)


def test_process_unit_stop_escalates_to_sigkill(tmp_path):
    unit = ManagedProcessUnit(tmp_path / "units")
    argv = (sys.executable, "-B", "-c",
            "import signal,time; signal.signal(signal.SIGTERM, signal.SIG_IGN); time.sleep(30)")
    spawned = unit.spawn("scope-1", "alloc-a", "f" * 64, argv)
    process = spawned["process"]
    try:
        evidence = unit.stop("scope-1", "alloc-a", terminate_deadline_ms=400, process=process)
        assert evidence["confirmed"] is True
        assert process.returncode is not None
        assert not pid_alive(spawned["identity"]["pid"])
    finally:
        if process.poll() is None:
            process.kill()
            process.wait(timeout=5)


def test_process_unit_exit_then_inspect_terminated(tmp_path):
    unit = ManagedProcessUnit(tmp_path / "units")
    spawned = unit.spawn("scope-1", "alloc-a", "f" * 64, sleep_argv(0.2))
    process = spawned["process"]
    process.wait(timeout=5)
    assert wait_until(lambda: unit.inspect("scope-1", "alloc-a", process=process)["phase"] == "TERMINATED")
    evidence = unit.stop("scope-1", "alloc-a", terminate_deadline_ms=1000, process=process)
    assert evidence["confirmed"] is True  # already exited; evidence verified, idempotent release


def test_signal_birth_fences_pid_reuse(tmp_path):
    # A wrong birth identity must never signal an unrelated live process.
    victim = __import__("subprocess").Popen(sleep_argv(30))
    try:
        real_start = birth_start(victim.pid)
        assert real_start is not None
        _signal_birth(victim.pid, "not-the-real-start", signal.SIGKILL)
        time.sleep(0.1)
        assert victim.poll() is None and pid_alive(victim.pid)
    finally:
        victim.kill()
        victim.wait(timeout=5)


def launch_spec(argv):
    return WorkerLaunchSpec(
        artifact_refs={"platform": {"digest": "a" * 64, "location": "file:///tmp/platform.whl"}},
        entrypoint="demiflow-worker", argv=tuple(argv), bootstrap_ref="bootstrap-ref",
        access_refs={}, runtime_protocol_version=RESOURCE_PROTOCOL_VERSION)


def plan_payload(scope_id, allocations, argv, cpu=1000, mem=1024 ** 3):
    return {"scope_id": scope_id, "desired_workers": [
        {"worker_id": "w-" + a, "allocation_id": a,
         "profile": {"requests": {"cpu_millis": cpu, "memory_bytes": mem}},
         "launch": launch_spec(argv).to_dict(), "provision_deadline_ms": 60000}
        for a in allocations], "retirements": []}


def context(run, operation_id, revision):
    return MutationContext(RESOURCE_PROTOCOL_VERSION, "scope-1", run.attempt, run.fence, operation_id, revision)


def reconcile(manager, run):
    return manager.reconcile_once(run.fence, time.monotonic() + 15, 16)


def read_pid(unit, allocation_id):
    record = json.loads((unit.directory("scope-1", allocation_id) / "process.json").read_text())
    return record["identity"]["pid"]


@pytest.fixture
def local_env(tmp_path):
    with RunContext.start(tmp_path / "run", limits={"max_control_bytes": 16 * 1024 ** 2}) as run:
        pool = LocalCapacityPool(tmp_path / "pool", capacity={"cpu_millis": 4000, "memory_bytes": 4 * 1024 ** 3})
        unit = ManagedProcessUnit(tmp_path / "units")
        provider = LocalResourceProvider(pool, unit)
        state = RunResourceState(run.journal, claim_lease_ms=1)
        manager = ResourceManager(provider, state, bounds=ManagerBounds(claim_lease_ms=1))
        scope = ResourceScope("scope-1", "run-1", {"provider": "local"}, "authority-ref",
                              {"cpu_millis": 8000, "memory_bytes": 8 * 1024 ** 3, "max_workers": 4})
        state.open_scope(run.fence, scope)
        yield run, pool, unit, provider, state, manager
        for process in list(provider._spawned.values()):
            if process.poll() is None:
                process.kill()
                process.wait(timeout=5)


def test_local_provider_full_lifecycle_real_process(local_env):
    run, pool, unit, provider, state, manager = local_env
    manager.apply(context(run, "op-1", 0), plan_payload("scope-1", ["alloc-a"], sleep_argv(30)))
    reconcile(manager, run)
    snapshot = manager.get("scope-1")
    assert snapshot["allocations"][0]["phase"] == "RUNNING"
    pid = read_pid(unit, "alloc-a")
    assert pid_alive(pid)
    assert pool.snapshot()["charged"]["cpu_millis"] == 1000
    # The managed worker inherited a parent envelope slice.
    envelope = json.loads((unit.directory("scope-1", "alloc-a") / "envelope.json").read_text())
    assert envelope["allocation_id"] == "alloc-a" and envelope["cpu_millis"] == 1000
    manager.close(context(run, "op-close", 1),
                  {"mode": "DRAINED", "engine_receipt_ref": "drain-ref",
                   "terminate_deadline_ms": 3000, "reason": "run ended"})
    reconcile(manager, run)
    with run.journal._connection() as db:
        assert db.execute("SELECT state FROM resource_scopes WHERE scope_id='scope-1'").fetchone()[0] == "CLOSED"
    assert wait_until(lambda: not pid_alive(pid))
    assert pool.snapshot()["charged"] == {"cpu_millis": 0, "memory_bytes": 0}
    assert manager.get("scope-1")["cleanup_obligations"] == []


def test_local_provider_partial_capacity_pending(local_env):
    run, pool, unit, provider, state, manager = local_env
    manager.apply(context(run, "op-1", 0),
                  plan_payload("scope-1", ["alloc-a", "alloc-b", "alloc-c"], sleep_argv(30), cpu=2000))
    reconcile(manager, run)
    snapshot = manager.get("scope-1")
    phases = {a["allocation_id"]: a["phase"] for a in snapshot["allocations"]}
    assert phases["alloc-a"] == "RUNNING" and phases["alloc-b"] == "RUNNING"
    assert phases["alloc-c"] == "PENDING"  # pool full (4000 millicores), third waits
    assert pool.snapshot()["charged"]["cpu_millis"] == 4000
    assert {p["allocation_id"] for p in snapshot["pending_and_shortfall"]} == {"alloc-c"}


def test_local_provider_never_revives_exited_allocation(local_env):
    run, pool, unit, provider, state, manager = local_env
    manager.apply(context(run, "op-1", 0), plan_payload("scope-1", ["alloc-a"], sleep_argv(0.2)))
    reconcile(manager, run)
    process = provider._spawned.get("alloc-a")
    if process is not None:
        process.wait(timeout=5)
    assert wait_until(lambda: provider.inspect("scope-1", None)["allocations"][0]["phase"] == "TERMINATED")
    # Re-ensure on a terminated allocation reports TERMINATED; it never respawns.
    with run.journal._connection() as db:
        immutable = state._allocation_row(db, "alloc-a")["spec"]
    obs = provider.ensure_present("alloc-a", immutable)
    assert obs["phase"] == "TERMINATED"
    assert "exited_before_present" in obs["conditions"]


def test_envelope_roundtrip_and_fail_closed(tmp_path, monkeypatch):
    path = tmp_path / "envelope.json"
    envelope = {"allocation_id": "alloc-a", "spec_hash": "a" * 64, "cpu_millis": 2000,
                "memory_bytes": 2 * 1024 ** 3, "resource_directory": str(tmp_path / "native")}
    write_envelope(path, envelope)
    monkeypatch.setenv(ENVELOPE_ENV, str(path))
    assert inherited_envelope() == envelope
    monkeypatch.delenv(ENVELOPE_ENV)
    assert inherited_envelope() is None  # independent consumer keeps the root path
    path.write_text('{"allocation_id": "alloc-a"}')  # malformed: missing closed keys
    monkeypatch.setenv(ENVELOPE_ENV, str(path))
    with pytest.raises(ValueError):
        inherited_envelope()  # managed worker must not fall back to whole-machine defaults
