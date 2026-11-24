"""U2 LIF/AUT conformance + Local real worker lifecycle (S3).

LIF: the worker-side command executor consumes the frozen K3 state machine,
fencing, idempotency and capability/artifact gates without redefining them.
AUT: the granted-fence authority attaches against live driver ownership, fenced
writes succeed, and a driver restart that replaces the fence is rejected by the
journal backstop. Local: a real worker process serves the lifecycle over an
inherited socketpair. No GPU/multi-host claims; the data/commit chain is S4.
"""
import time
import uuid
from contextlib import contextmanager
from pathlib import Path
from types import SimpleNamespace

import pytest

from demiflow.errors import ExecutionConflict
from demiflow.execution.storage.artifacts import read
from demiflow.execution.contracts import artifact
from demiflow.execution.storage.artifacts import save_manifest
from demiflow.execution.contracts.envelope import WORKER_PROTOCOL_VERSION
from demiflow.execution.contracts.worker import validate_message
from demiflow.execution.contracts.errors import ProtocolError
from demiflow.execution.contracts.recovery_contract import RecoveryContractError
from demiflow.execution.pipeline.run_context import RunContext, run_scope, current_run
from demiflow.execution.storage.state_artifacts import StateArtifacts
from demiflow.execution.worker.runtime import WorkerAuthority
from demiflow.execution.worker.bootstrap import recv_message, send_message, spawn_local_worker
from demiflow.execution.worker.runtime import WorkerRuntime


def _manifest(run_id):
    return artifact.build_manifest(
        run_id=run_id,
        platform={"source_digest": "a" * 64, "distribution": "demiurge",
                  "version": "1.0", "runtime_identity_ref": "runtime-ref"},
        business={"bundle_digest": "sha256:" + "b" * 64,
                  "entrypoints": [{"name": "main", "module_key": "p_" + "b" * 24,
                                   "definition_ref": {"sha256": "c" * 64}}]},
        config={"resolved_config_digest": "d" * 64, "model_config_refs": []},
        environment={"requirements_sha256": "e" * 64, "lock_digest": "f" * 64,
                     "resolution_digest": "0" * 64},
        protocol_versions={"worker": {"major": 1, "minor": 0},
                           "exchange": {"major": 1, "minor": 0},
                           "checkpoint_schema": 1, "state_schema": 1, "resource_protocol": "1.0"},
        secret_refs=[])


@pytest.fixture
def env(tmp_path):
    with RunContext.start(tmp_path / "run") as run:
        config = run.journal.recovery_identity()
        ids = {k: uuid.uuid4().hex for k in
               ("scope_id", "allocation_id", "boot_id", "grant_id", "worker_id")}
        grant = {"grant_id": ids["grant_id"], "run_id": run.ref.run_id, "attempt": run.attempt,
                 "fence": run.fence, "scope_id": ids["scope_id"], "allocation_id": ids["allocation_id"],
                 "boot_id": ids["boot_id"], "controller_epoch": run.attempt,
                 "expires_at_ms": int(time.time() * 1000) + 120000}
        ref = save_manifest(run.directory, _manifest(run.ref.run_id))
        mref = {"manifest_id": ref["manifest_id"], "sha256": ref["sha256"]}
        launch_spec = {"directory": str(run.directory), "run_id": run.ref.run_id,
                       "scope_id": ids["scope_id"], "allocation_id": ids["allocation_id"],
                       "boot_id": ids["boot_id"], "controller_epoch": run.attempt,
                       "capabilities": ["cpu-map", "stream"], "grant": grant,
                       "artifact_digest_observed": config["platform_revision"]}
        loader = lambda r: read(run.directory / (r["sha256"] + ".json"))
        yield SimpleNamespace(run=run, config=config, grant=grant, mref=mref, ids=ids,
                              launch_spec=launch_spec, loader=loader, tmp_path=tmp_path)


def _runtime(env, **kw):
    return WorkerRuntime(env.launch_spec, manifest_loader=env.loader, **kw)


def _command(env, kind, payload, **over):
    envelope = {"protocol_version": WORKER_PROTOCOL_VERSION, "kind": kind,
                "scope_id": env.ids["scope_id"], "run_id": env.run.ref.run_id,
                "operation_id": uuid.uuid4().hex, "controller_epoch": env.run.attempt,
                "authority_ref": env.ids["grant_id"], "allocation_id": env.ids["allocation_id"],
                "boot_id": env.ids["boot_id"], "payload": payload}
    envelope.update(over)
    return envelope


def _handle(env, rt, kind, payload, **over):
    return rt.handle(_command(env, kind, payload, **over), now_ms=int(time.time() * 1000))


def _assignment(env, *, plan_revision=0, expected=0):
    return {"schema_version": "demiflow.worker_assignment.v1", "assignment_id": uuid.uuid4().hex,
            "operation_id": uuid.uuid4().hex, "scope_id": env.ids["scope_id"],
            "allocation_id": env.ids["allocation_id"], "boot_id": env.ids["boot_id"],
            "worker_id": env.ids["worker_id"], "controller_epoch": env.run.attempt,
            "grant_ref": env.ids["grant_id"], "plan_revision": plan_revision,
            "expected_plan_revision": expected, "artifact_manifest_ref": env.mref,
            "stages": [], "partitions": [], "channels": {"inputs": [], "outputs": []},
            "restore_point": None, "route_epoch": 0, "topology_revision": 0,
            "ready_deadline_ms": 60000}


def _verify(env, rt, caps=("cpu-map",), **over):
    return _handle(env, rt, "VerifyArtifactAndCapabilities",
                   {"artifact_manifest_ref": env.mref, "required_capabilities": list(caps)}, **over)
def _restore(env, rt, state_refs=()):
    return _handle(env, rt, "Restore", {"assignment_ref": uuid.uuid4().hex,
                                        "restore_point": None, "state_refs": list(state_refs)})


def _ready(env, rt):
    """Drive the legal K3 path to READY so Activate is a permitted transition."""
    _verify(env, rt)
    _restore(env, rt)


def _activate(env, rt, **over):
    payload = {"grant_ref": env.ids["grant_id"], "controller_epoch": env.run.attempt,
               "operation_id": uuid.uuid4().hex}
    return _handle(env, rt, "Activate", payload, **over)


@contextmanager
def as_worker():
    """Simulate the worker process context: no driver ContextVar bound while the
    driver keeps live ownership of the run (runtime_owner stays (attempt, fence))."""
    with run_scope(None):
        yield


# ---------------- LIF: lifecycle command executor ----------------

def test_register_envelope_is_valid_k3(env):
    rt = _runtime(env)
    envelope = rt.register()
    validate_message(envelope)
    assert envelope["kind"] == "Register" and rt.state == "REGISTERED"
    assert envelope["payload"]["boot_id"] == env.ids["boot_id"]


def test_verify_transitions_to_verified(env):
    rt = _runtime(env)
    receipt = _verify(env, rt)
    assert receipt == {"kind": "Verified", "state": "VERIFIED"}
    assert rt.state == "VERIFIED"


def test_verify_missing_capability_rejected(env):
    rt = _runtime(env)
    with pytest.raises(ProtocolError) as exc:
        _verify(env, rt, caps=("gpu",))
    assert exc.value.code == "CAPABILITY_REQUIRED_MISSING"
    assert rt.state == "REGISTERED"  # no partial transition


def test_verify_artifact_mismatch_rejected(env):
    rt = _runtime(env)
    # The loader is keyed by sha256 path, so keep the real digest (content loads)
    # and corrupt manifest_id: verify_manifest must reject the id/content bind.
    bad = {"manifest_id": "9" * 64, "sha256": env.mref["sha256"]}
    with pytest.raises(ProtocolError) as exc:
        _handle(env, rt, "VerifyArtifactAndCapabilities",
                {"artifact_manifest_ref": bad, "required_capabilities": ["cpu-map"]})
    assert exc.value.code == "ARTIFACT_MISMATCH"
    assert rt.state == "REGISTERED"


def test_restore_reads_state_artifacts(env):
    ref = StateArtifacts(env.run).put("actor-state", {"count": 7})
    rt = _runtime(env)
    _verify(env, rt)
    receipt = _restore(env, rt, [ref])
    assert receipt["state"] == "READY" and receipt["state_checksums"] == [ref["sha256"]]
    assert rt._restored["actor-state"] == {"count": 7}


def test_restore_rejects_corrupt_state_ref(env):
    ref = StateArtifacts(env.run).put("actor-state", {"count": 7})
    corrupt = dict(ref, sha256="0" * 64)
    rt = _runtime(env)
    _verify(env, rt)
    with pytest.raises(RecoveryContractError):
        _restore(env, rt, [corrupt])


def test_assign_plan_revision_cas(env):
    rt = _runtime(env)
    receipt = _handle(env, rt, "Assign", {"assignment": _assignment(env, plan_revision=1, expected=0)})
    assert receipt == {"kind": "Assigned", "plan_revision": 1}
    assert rt.plan_revision == 1
    with pytest.raises(ProtocolError) as exc:
        _handle(env, rt, "Assign", {"assignment": _assignment(env, plan_revision=2, expected=0)})
    assert exc.value.code == "ASSIGNMENT_PLAN_CONFLICT"
    assert rt.plan_revision == 1  # stale assignment never applied


def test_stale_boot_fenced(env):
    rt = _runtime(env)
    with pytest.raises(ProtocolError) as exc:
        _verify(env, rt, boot_id=uuid.uuid4().hex)
    assert exc.value.code == "BOOT_FENCED"


def test_stale_controller_epoch_fenced(env):
    rt = _runtime(env)
    with pytest.raises(ProtocolError) as exc:
        _verify(env, rt, controller_epoch=env.run.attempt - 1)
    assert exc.value.code == "FENCED"


def test_foreign_scope_fenced(env):
    rt = _runtime(env)
    with pytest.raises(ProtocolError) as exc:
        _verify(env, rt, scope_id=uuid.uuid4().hex)
    assert exc.value.code == "FENCED"


def test_idempotent_receipt_replayed_without_reexecute(env):
    rt = _runtime(env)
    envelope = _command(env, "VerifyArtifactAndCapabilities",
                        {"artifact_manifest_ref": env.mref, "required_capabilities": ["cpu-map"]})
    first = rt.handle(envelope, now_ms=int(time.time() * 1000))
    second = rt.handle(envelope, now_ms=int(time.time() * 1000))
    assert first == second == {"kind": "Verified", "state": "VERIFIED"}


def test_idempotency_conflict_on_payload_change(env):
    rt = _runtime(env)
    envelope = _command(env, "VerifyArtifactAndCapabilities",
                        {"artifact_manifest_ref": env.mref, "required_capabilities": ["cpu-map"]})
    rt.handle(envelope, now_ms=int(time.time() * 1000))
    changed = dict(envelope, payload={"artifact_manifest_ref": env.mref,
                                      "required_capabilities": ["stream"]})
    with pytest.raises(ProtocolError) as exc:
        rt.handle(changed, now_ms=int(time.time() * 1000))
    assert exc.value.code == "IDEMPOTENCY_CONFLICT"


def test_cancel_retires_from_any_state(env):
    rt = _runtime(env)
    receipt = _handle(env, rt, "Cancel", {"operation_id": uuid.uuid4().hex, "reason": "stop"})
    assert receipt["state"] == "RETIRED" and rt.state == "RETIRED"


def test_business_input_gate_closed_before_activate(env):
    rt = _runtime(env)
    with pytest.raises(ProtocolError) as exc:
        rt.admit_business_input(now_ms=int(time.time() * 1000))
    assert exc.value.code == "FENCED"


# ---------------- AUT: granted-fence authority ----------------

def test_activate_attaches_granted_authority(env):
    rt = _runtime(env)
    _verify(env, rt)
    _restore(env, rt)
    with as_worker():
        receipt = _activate(env, rt)
        assert receipt == {"kind": "Activated", "state": "ACTIVATED"}
        assert rt.authority is not None and rt.authority.fence == env.run.fence
        assert current_run() is rt.authority
        assert rt.admit_business_input(now_ms=int(time.time() * 1000)) is True
        rt.close()
    assert rt.authority is None


def test_fenced_write_succeeds_under_grant(env):
    rt = _runtime(env)
    _ready(env, rt)
    with as_worker():
        _activate(env, rt)
        rt.authority.allocate_artifact(env.run.directory / "scratch" / "block.bin", atomic=False)
        with env.run.journal._connection() as db:
            owner = db.execute("SELECT attempt,fence FROM runtime_owner").fetchone()
        assert owner == (env.run.attempt, env.run.fence)  # worker never replaced ownership
        rt.close()


def test_stale_fence_attach_rejected(env):
    env.run.journal.claim_execution(env.config, uuid.uuid4().hex)  # driver restart
    rt = _runtime(env)
    _ready(env, rt)
    try:
        with as_worker():
            with pytest.raises(ExecutionConflict):
                _activate(env, rt)
            assert rt.authority is None and rt.state == "READY"
    finally:
        env.run.journal.claim_execution(env.config, env.run.fence)  # restore for clean driver exit


def test_stale_fence_write_and_poll_rejected(env):
    rt = _runtime(env)
    _ready(env, rt)
    with as_worker():
        _activate(env, rt)
        env.run.journal.claim_execution(env.config, uuid.uuid4().hex)  # restart replaces fence
        with pytest.raises(ExecutionConflict):
            rt.authority.allocate_artifact(env.run.directory / "scratch" / "stale.bin", atomic=False)
        with pytest.raises(ExecutionConflict):
            rt.authority.check_stop()
        rt.close()
    env.run.journal.claim_execution(env.config, env.run.fence)  # restore for clean driver exit


def test_expired_grant_rejected(env):
    env.grant["expires_at_ms"] = int(time.time() * 1000) - 1
    rt = _runtime(env)
    _ready(env, rt)
    with as_worker():
        with pytest.raises(ProtocolError) as exc:
            _activate(env, rt)
        assert exc.value.code == "GRANT_REVOKED"
        assert rt.authority is None


def test_grant_ref_mismatch_rejected(env):
    rt = _runtime(env)
    _ready(env, rt)
    with as_worker():
        with pytest.raises(ProtocolError) as exc:
            _handle(env, rt, "Activate", {"grant_ref": uuid.uuid4().hex,
                                          "controller_epoch": env.run.attempt,
                                          "operation_id": uuid.uuid4().hex})
        assert exc.value.code == "GRANT_REVOKED"


def test_authority_exposes_no_lifecycle_authority(env):
    with as_worker():
        auth = WorkerAuthority(env.run.directory, env.grant)
        auth.__enter__()
        for forbidden in ("transition", "begin_attempt", "claim_execution", "finish_execution"):
            assert not hasattr(auth, forbidden)
        auth.__exit__(None, None, None)


# ---------------- Local: real worker process lifecycle ----------------

def _drive(sock, env, kind, payload):
    op = uuid.uuid4().hex
    send_message(sock, {"protocol_version": WORKER_PROTOCOL_VERSION, "kind": kind,
                        "scope_id": env.ids["scope_id"], "run_id": env.run.ref.run_id,
                        "operation_id": op, "controller_epoch": env.run.attempt,
                        "authority_ref": env.ids["grant_id"], "allocation_id": env.ids["allocation_id"],
                        "boot_id": env.ids["boot_id"], "payload": payload})
    events = []
    while True:
        message = recv_message(sock)
        if message is None:
            raise AssertionError("worker closed before responding to " + kind)
        if message.get("response_to") == op:
            return message, events
        events.append(message)


def test_local_worker_full_lifecycle_real_process(env):
    log = env.tmp_path / "worker.log"
    process, sock, launch_path = spawn_local_worker(env.launch_spec, log_path=log)
    try:
        sock.settimeout(20)
        register = recv_message(sock)
        assert register["kind"] == "Register"
        assert register["payload"]["boot_id"] == env.ids["boot_id"]
        assert register["payload"]["allocation_id"] == env.ids["allocation_id"]

        r, _ = _drive(sock, env, "VerifyArtifactAndCapabilities",
                      {"artifact_manifest_ref": env.mref, "required_capabilities": ["cpu-map"]})
        assert r["receipt"]["state"] == "VERIFIED"
        r, _ = _drive(sock, env, "Restore",
                      {"assignment_ref": uuid.uuid4().hex, "restore_point": None, "state_refs": []})
        assert r["receipt"]["state"] == "READY"
        r, events = _drive(sock, env, "Activate",
                           {"grant_ref": env.ids["grant_id"], "controller_epoch": env.run.attempt,
                            "operation_id": uuid.uuid4().hex})
        assert r["receipt"]["state"] == "ACTIVATED"
        assert any(e["kind"] == "Ready" for e in events)  # worker emitted Ready after Restore
        r, _ = _drive(sock, env, "Assign", {"assignment": _assignment(env)})
        assert r["receipt"]["kind"] == "Assigned"
        r, _ = _drive(sock, env, "Drain", {"deadline_ms": 5000, "reason": "run ended"})
        assert r["receipt"]["state"] == "DRAINING" and r["receipt"]["release_hint"] == "DRAINED"
        process.wait(timeout=10)
        assert process.returncode == 0, log.read_text()
        # The worker attached under the granted fence without replacing ownership.
        with env.run.journal._connection() as db:
            assert db.execute("SELECT attempt,fence FROM runtime_owner").fetchone() == (env.run.attempt, env.run.fence)
    finally:
        if process.poll() is None:
            process.kill()
            process.wait(timeout=5)
        sock.close()
        Path(launch_path).unlink(missing_ok=True)


def test_local_worker_rejects_stale_boot_real_process(env):
    log = env.tmp_path / "worker.log"
    process, sock, launch_path = spawn_local_worker(env.launch_spec, log_path=log)
    try:
        sock.settimeout(20)
        recv_message(sock)  # Register
        op = uuid.uuid4().hex
        send_message(sock, {"protocol_version": WORKER_PROTOCOL_VERSION,
                            "kind": "VerifyArtifactAndCapabilities", "scope_id": env.ids["scope_id"],
                            "run_id": env.run.ref.run_id, "operation_id": op,
                            "controller_epoch": env.run.attempt, "authority_ref": env.ids["grant_id"],
                            "allocation_id": env.ids["allocation_id"], "boot_id": uuid.uuid4().hex,
                            "payload": {"artifact_manifest_ref": env.mref, "required_capabilities": ["cpu-map"]}})
        response = recv_message(sock)
        assert response["response_to"] == op and response["error"]["code"] == "BOOT_FENCED"
        r, _ = _drive(sock, env, "Cancel", {"operation_id": uuid.uuid4().hex, "reason": "done"})
        assert r["receipt"]["state"] == "RETIRED"
        process.wait(timeout=10)
        assert process.returncode == 0, log.read_text()
    finally:
        if process.poll() is None:
            process.kill()
            process.wait(timeout=5)
        sock.close()
        Path(launch_path).unlink(missing_ok=True)

def test_retired_execution_launch_never_enters_a_control_connector(tmp_path):
    from demiflow.execution.worker.bootstrap import LocalControlConnector

    invalid = {'execution': {}}
    with pytest.raises(ProtocolError, match='retired'):
        WorkerRuntime(invalid)
    with pytest.raises(ProtocolError, match='retired'):
        spawn_local_worker(invalid, log_path=tmp_path / 'worker.log')
    assert list(tmp_path.iterdir()) == []
    local = LocalControlConnector()
    with pytest.raises(ProtocolError, match='retired'):
        local.stage('allocation', invalid)
    assert local.driver_socks == {} and local._staged == {} and local._pending == {}


def test_retired_ray_launch_never_enters_provider_staging():
    pytest.importorskip('ray')
    from demiflow.execution.hosts.ray_worker_host import RayControlConnector

    connector = RayControlConnector()
    with pytest.raises(ProtocolError, match='retired'):
        connector.stage('allocation', {'execution': {}})
    assert connector._staged == {} and connector._actors == {}
