"""L016 · run_ray_chain — resource terminal on every exit.

2026-11-16: the five marked Ray cases now exercise the actual v3 compatibility
entry, with the same six input rows, two CPUs, original checkpoint policy,
physical actor death, original journal and resume. The old unmarked AST/fake
layer below is retained pending migration and is NOT claimed as passing.
Historical layer description follows; it is not current implementation evidence.

Ruling (coordinator 61d0e9a3, 2026-11-14): verify_preserve → implement. The
baseline closed the ResourceManager scope only on the success path, so a Ray
failure/cancel before Drain left the journal scope OPEN with an unconfirmed
allocation. ``manager.close`` + ``reconcile_once`` now run in the finally with
DRAINED/ABANDON exactly as ``run_local_chain``, and connector.terminate/close
stay after them.

EVIDENCE, two layers, reported separately:
* Default env (ray absent): the runtime cases inject a test-domain
  provider/connector (authorized by the ruling) while keeping the REAL
  ResourceManager, REAL RunResourceState and REAL run journal, so CLOSED /
  CONFIRMED / orphan-scope assertions read the durable ledger rather than a fake.
  The driver seam is the real one: LocalDatasetExecutor's unified_chain dispatch
  is redirected to run_ray_chain, so source capture, plan sealing, layout
  interpretation and the commit authority all run for real.
* env-ray (ray 2.60.0, selected with -m ray): real single-host local Ray Core,
  num_cpus=2, no cluster and no remote address. These exercise the real provider,
  including its asynchronous-kill window: the single death probe right after
  ray.kill can return UNCONFIRMED (ray-actor-alive:), which F6 correctly keeps
  charged, and the standard protocol then converges to CLOSED + CONFIRMED once
  the intent's claim lease expires. Neither layer is multi-host cluster evidence.
"""
import ast
import inspect
import json
import sys
import time
import types

import pytest

from demiflow.data.dataset import Dataset
from demiflow.data.plan import CallableSpec, LogicalPlan, MapOp, MapStreamOptions
from demiflow.data.sources import ItemsSource
from demiflow.execution import chain
from demiflow.execution.executors.local import LocalDatasetExecutor
from demiflow.execution.recovery_contract import RecoveryContractError
from demiflow.execution.run_context import RunContext
from demiflow.resources.contracts import spec_hash, validate_observation
from demiflow.resources.manager import ResourceManager

# These assertions still describe the retired v2 driver and are intentionally
# not converted into passing v3 evidence here. Keep their readers local so an
# obsolete assertion fails as a test instead of aborting repository collection.
import textwrap


def _function(name):
    return ast.parse(textwrap.dedent(inspect.getsource(getattr(chain, name)))).body[0]

def _dotted(node):
    parts = []
    while isinstance(node, ast.Attribute):
        parts.append(node.attr)
        node = node.value
    if isinstance(node, ast.Name):
        parts.append(node.id)
    return ".".join(reversed(parts))

def _calls(node, name):
    return [item for item in ast.walk(node)
            if isinstance(item, ast.Call) and _dotted(item.func) == name]

def _assigned(node, name):
    for item in ast.walk(node):
        if isinstance(item, ast.Assign) and any(
                isinstance(target, ast.Name) and target.id == name for target in item.targets):
            return item.value
    raise AssertionError("no assignment to " + name)

def _dict_value(value):
    assert isinstance(value, ast.Dict), "not a dict literal"
    return {_dotted_key(key): val for key, val in zip(value.keys, value.values)}

def _dict_literal(node, name):
    return _dict_value(_assigned(node, name))

def _dotted_key(key):
    return key.value if isinstance(key, ast.Constant) else _dotted(key)

def _in_finally(node, call):
    for item in ast.walk(node):
        if isinstance(item, ast.Try):
            for statement in item.finalbody:
                if call in ast.walk(statement):
                    return True
    return False


def test_layout_is_interpreted_from_the_sealed_plan():
    fn = _function("run_ray_chain")
    assert len(_calls(fn, "interpret_physical_plan")) == 1
    assert len(_calls(fn, "load_sealed_plan")) == 1
    assert len(_calls(fn, "load_execution_bindings")) == 1
    placement = ast.unparse(_assigned(fn, "placement"))
    assert "layout.workers" in placement
    assert "group_id == 'chain-group'" in placement


def test_worker_profile_comes_from_the_layout():
    fn = _function("run_ray_chain")
    assert ast.unparse(_assigned(fn, "sealed_profile")) == \
        "ResourceProfile(**to_wire(placement.profile))"
    requests = _calls(fn, "WorkerRequest")
    assert len(requests) == 1
    assert ast.unparse(requests[0].args[2]) == "sealed_profile"


def test_stages_and_partitions_come_from_the_placement():
    fn = _function("run_ray_chain")
    assignment = _dict_literal(fn, "assignment")
    assert ast.unparse(assignment["stages"]) == "list(placement.stage_ids)"
    assert ast.unparse(assignment["partitions"]) == "list(placement.partition_ids)"


def test_program_binding_comes_from_the_sealed_bindings():
    fn = _function("run_ray_chain")
    assert ast.unparse(_assigned(fn, "bindings_ref")) == "bindings['ref']"
    launch = _dict_literal(fn, "launch_spec")
    execution_plan = _dict_value(launch["execution_plan"])
    assert ast.unparse(execution_plan["bindings_ref"]) == "bindings_ref"
    assert ast.unparse(execution_plan["action_id"]) == "action.action_id"


def test_assignment_channels_are_validated_against_the_layout():
    from demiflow.execution import plan_interpreter
    source = inspect.getsource(plan_interpreter.validate_worker_assignment)
    assert "_group_channel_ids(layout, placement)" in source
    assert "assignment input channels differ from the layout" in source


def test_driver_holds_the_only_commit_authority():
    fn = _function("run_ray_chain")
    assert len(_calls(fn, "terminal.commit_cut")) == 1
    assert _calls(fn, "CommitCoordinator") == []
    assert len(_calls(fn, "terminal.finish")) == 1
    assert len(_calls(fn, "DriverStreamTerminal")) == 1


def test_no_local_capacity_double_charge():
    fn = _function("run_ray_chain")
    assert _calls(fn, "LocalCapacityPool") == []
    assert len(_calls(fn, "RayResourceProvider")) == 1


def test_actor_is_terminated_on_every_exit():
    fn = _function("run_ray_chain")
    for name in ("connector.terminate", "connector.close"):
        calls = _calls(fn, name)
        assert len(calls) == 1, name
        assert _in_finally(fn, calls[0]), name + " must run on failure/cancel too"


def test_restore_point_comes_from_the_terminal_journal():
    fn = _function("run_ray_chain")
    assert ast.unparse(_assigned(fn, "restore")) == "terminal.restore_point()"
    assignment = _dict_literal(fn, "assignment")
    assert "restore" in ast.unparse(assignment["restore_point"])


def test_source_capture_is_backend_neutral():
    fn = _function("run_ray_chain")
    # D1: the fixed source is read with the local reader, never Ray Data.
    assert len(_calls(fn, "capture_source")) == 1
    assert len(_calls(fn, "freeze_source")) == 1
    assert "ray.data" not in inspect.getsource(chain.run_ray_chain)


# ---------------- structural: the unified resource terminal (the fix) ----------------

def _close_payload(fn):
    return _calls(fn, "manager.close")[0].args[1]


def test_resource_terminal_runs_in_the_finally():
    fn = _function("run_ray_chain")
    closes = _calls(fn, "manager.close")
    assert len(closes) == 1, "exactly one resource terminal, no second ledger"
    assert _in_finally(fn, closes[0]), "scope close must run on failure/cancel too"
    assert any(_in_finally(fn, call) for call in _calls(fn, "manager.reconcile_once"))


def test_terminal_mode_follows_the_drain_flag():
    fn = _function("run_ray_chain")
    body = next(item for item in ast.walk(fn) if isinstance(item, ast.Try))
    names = [ast.unparse(stmt) for stmt in fn.body]
    # armed before the try, so an early failure can never read it unset
    assert "drained = False" in names
    assert names.index("drained = False") < next(
        i for i, text in enumerate(names) if text.startswith("try:"))
    finishes = _calls(fn, "terminal.finish")
    assert len(finishes) == 1
    index = next(i for i, stmt in enumerate(body.body) if finishes[0] in ast.walk(stmt))
    assert ast.unparse(body.body[index + 1]) == "drained = True"
    payload = _dict_value(_close_payload(fn))
    assert ast.unparse(payload["mode"]) == "'DRAINED' if drained else 'ABANDON'"
    assert ast.unparse(payload["engine_receipt_ref"]) == \
        "allocation_id + ('-drained' if drained else '-abandoned')"
    assert ast.unparse(payload["reason"]) == "'chain drained' if drained else 'chain terminal cleanup'"
    assert ast.unparse(payload["terminate_deadline_ms"]) == "5000"


def test_actor_kill_stays_after_the_resource_terminal():
    fn = _function("run_ray_chain")
    body = next(item for item in ast.walk(fn) if isinstance(item, ast.Try))
    texts = [ast.unparse(stmt) for stmt in body.finalbody]
    terminal = next(i for i, text in enumerate(texts) if "manager.close" in text)
    terminate = next(i for i, text in enumerate(texts) if "connector.terminate" in text)
    close = next(i for i, text in enumerate(texts) if "connector.close" in text)
    assert terminal < terminate < close
    # guarded, so a cleanup failure cannot skip the actor kill
    guarded = body.finalbody[terminal]
    assert isinstance(guarded, ast.Try) and guarded.handlers


def test_local_and_ray_share_one_terminal_protocol():
    ray = _dict_value(_close_payload(_function("run_ray_chain")))
    local = _dict_value(_close_payload(_function("run_local_chain")))
    assert set(ray) == set(local)
    for key in ray:
        assert ast.unparse(ray[key]) == ast.unparse(local[key]), key
    for name in ("run_local_chain", "run_ray_chain"):
        fn = _function(name)
        assert _in_finally(fn, _calls(fn, "manager.close")[0])
        assert len(_calls(fn, "ResourceManager")) == 1
        assert len(_calls(fn, "RunResourceState")) == 1
        assert len(_calls(fn, "state.open_scope")) == 1


# ---------------- runtime: real journal, injected provider/connector ----------------

class _FakeRayConnector:
    """Test-domain stand-in for the actor control transport (no Ray)."""
    log = []
    behaviour = {}
    instances = []

    def __init__(self, command_timeout_s=120):
        self.command_timeout_s = command_timeout_s
        self.staged = {}
        _FakeRayConnector.instances.append(self)

    def stage(self, allocation_id, launch_spec):
        self.log.append(("connector.stage", allocation_id))
        self.staged[allocation_id] = launch_spec

    def handle(self, allocation_id, message):
        self.log.append(("connector.handle", message["kind"]))
        return {"receipt": {"operation_id": message["operation_id"], "kind": message["kind"]}}

    def pull(self, allocation_id):
        self.log.append(("connector.pull", allocation_id))
        mode = self.behaviour.get("pull", "empty")
        if mode == "raise":
            raise RecoveryContractError("injected: ray actor unreachable")
        if mode == "failure":
            return [{"schema_version": chain.WORKER_FAILURE_SCHEMA,
                     "error": {"type": "InjectedActorDeath", "message": "actor died"}}]
        return []

    def deliver_receipt(self, allocation_id, receipt):
        self.log.append(("connector.deliver_receipt", allocation_id))

    def terminate(self, allocation_id):
        self.log.append(("connector.terminate", allocation_id))

    def close(self, allocation_id):
        self.log.append(("connector.close", allocation_id))


class _FakeRayProvider:
    """Test-domain stand-in for Ray Core placement; observations stay contract-valid."""
    log = []
    behaviour = {}
    instances = []

    def __init__(self, control, *, provision_timeout_s=30, device_pool=None):
        self._control = control
        self._alive = {}
        _FakeRayProvider.instances.append(self)

    def describe_and_normalize(self, binding, requirements, spec):
        requests = spec["profile"]["requests"]
        effective = {"cpu_millis": requests["cpu_millis"], "memory_bytes": requests["memory_bytes"]}
        return {"effective": effective, "spec": spec, "spec_hash": spec_hash(spec)}

    def _observation(self, scope_id, allocation_id, phase, *, boot_id=None,
                     cleanup="NONE", effective=None, conditions=None, evidence_ref=None):
        now = int(time.time() * 1000)
        return validate_observation({
            "allocation_id": allocation_id, "provider_uid": "fake-ray-" + allocation_id[:16],
            "boot_id": boot_id, "phase": phase, "observation": "FRESH",
            "observed_at_ms": now, "observation_revision": now, "cleanup": cleanup,
            "effective_resources": effective, "conditions": conditions or [],
            "evidence_ref": evidence_ref, "placement": None,
            "ownership": {"scope_owned": [allocation_id], "shared": []},
        })

    def ensure_present(self, allocation_id, immutable_spec):
        self.log.append(("provider.ensure_present", allocation_id))
        scope_id = immutable_spec["scope_id"]
        requests = immutable_spec["request"]["profile"]["requests"]
        effective = {"cpu_millis": requests["cpu_millis"], "memory_bytes": requests["memory_bytes"]}
        phase = self.behaviour.get("phase", "RUNNING")
        if phase != "RUNNING":
            return self._observation(scope_id, allocation_id, phase, effective=effective,
                                     conditions=["LAUNCH_FAILED", "injected placement failure"])
        boot_id = self._control.staged[allocation_id]["boot_id"]
        self._alive[allocation_id] = boot_id
        return self._observation(scope_id, allocation_id, "RUNNING", boot_id=boot_id,
                                 effective=effective,
                                 evidence_ref="fake-ray-actor:" + allocation_id)

    def ensure_absent(self, exact_identity, terminate_deadline_ms):
        allocation_id = exact_identity["allocation_id"]
        self.log.append(("provider.ensure_absent", allocation_id))
        now = int(time.time() * 1000)
        mode = self.behaviour.get("cleanup", "confirmed")
        if mode == "raise":
            raise RuntimeError("injected: cleanup transport lost")
        if mode == "unconfirmed":
            return {"allocation_id": allocation_id, "cleanup": "UNCONFIRMED",
                    "evidence_ref": "fake-ray-actor-alive:" + allocation_id, "observed_at_ms": now}
        self._alive.pop(allocation_id, None)
        return {"allocation_id": allocation_id, "cleanup": "CONFIRMED",
                "evidence_ref": "fake-ray-actor-dead:" + allocation_id, "observed_at_ms": now}

    def inspect(self, scope_id, page_token):
        return {"allocations": [], "next_page_token": None,
                "sampled_at_ms": int(time.time() * 1000)}


def _install_fake_module(monkeypatch, name, **attrs):
    module = types.ModuleType(name)
    for key, value in attrs.items():
        setattr(module, key, value)
    monkeypatch.setitem(sys.modules, name, module)


@pytest.fixture
def ray_driver(monkeypatch):
    """Redirect the real unified_chain driver seam to run_ray_chain with fakes."""
    _FakeRayConnector.log = []
    _FakeRayConnector.behaviour = {}
    _FakeRayConnector.instances = []
    _FakeRayProvider.log = []
    _FakeRayProvider.behaviour = {}
    _FakeRayProvider.instances = []
    _install_fake_module(monkeypatch, "demiflow.resources.ray",
                         RayResourceProvider=_FakeRayProvider)
    _install_fake_module(monkeypatch, "demiflow.execution.worker.ray_host",
                         RayControlConnector=_FakeRayConnector)
    timeout = {"control_timeout_s": 5.0}

    def seam(executor, dataset, *, source=None, plan=None, host_id="local"):
        return chain.run_ray_chain(executor, dataset, source=source, plan=plan,
                                   host_id=host_id, **timeout)

    monkeypatch.setattr(chain, "run_local_chain", seam)
    return types.SimpleNamespace(connector=_FakeRayConnector, provider=_FakeRayProvider,
                                 timeout=timeout)


def _dataset():
    def double(row):
        return {**row, "x": row["x"] * 2}

    items = tuple({"x": i} for i in range(6))
    plan = LogicalPlan((MapOp(CallableSpec.create(double),
                              stream_options=MapStreamOptions(concurrency=1)),))
    return Dataset(ItemsSource(items), plan, LocalDatasetExecutor(workers=1, unified_chain=True))


LIMITS = {"checkpoint_rows": 2, "checkpoint_interval_ms": 60000}


def _ledger(run):
    with run.journal._connection() as db:
        scopes = [{"scope_id": r[0], "state": r[1],
                   "close_document": json.loads(r[2]) if r[2] else None}
                  for r in db.execute("SELECT scope_id,state,close_document FROM resource_scopes")]
        allocations = [{"allocation_id": r[0], "scope_id": r[1], "cleanup": r[2],
                        "terminal": bool(r[3]), "phase": r[4]}
                       for r in db.execute("SELECT allocation_id,scope_id,cleanup,terminal,phase "
                                           "FROM resource_allocations")]
        outstanding = db.execute("SELECT COUNT(*) FROM resource_intents "
                                 "WHERE status IN ('pending','claimed')").fetchone()[0]
    return scopes, allocations, outstanding


def _fail(tmp_path, name):
    dataset = _dataset()
    with pytest.raises(RecoveryContractError) as caught:
        with RunContext.start(tmp_path / name, limits=LIMITS) as run:
            reference = run.ref
            dataset.materialize().take_all()
    return run, reference, caught.value


def test_failure_before_running_closes_the_scope(tmp_path, ray_driver):
    ray_driver.provider.behaviour["phase"] = "FAILED"
    run, _, error = _fail(tmp_path, "before-running")
    assert "did not reach RUNNING" in str(error)
    scopes, allocations, outstanding = _ledger(run)
    assert [s["state"] for s in scopes] == ["CLOSED"]
    assert [a["cleanup"] for a in allocations] == ["CONFIRMED"]
    assert all(a["terminal"] for a in allocations)
    assert outstanding == 0
    assert scopes[0]["close_document"]["mode"] == "ABANDON"
    assert scopes[0]["close_document"]["engine_receipt_ref"].endswith("-abandoned")


def test_actor_loss_after_running_closes_the_scope(tmp_path, ray_driver):
    ray_driver.connector.behaviour["pull"] = "raise"
    run, _, error = _fail(tmp_path, "actor-loss")
    assert "unreachable" in str(error)
    scopes, allocations, outstanding = _ledger(run)
    assert [s["state"] for s in scopes] == ["CLOSED"]
    assert [a["cleanup"] for a in allocations] == ["CONFIRMED"]
    assert outstanding == 0
    assert scopes[0]["close_document"]["mode"] == "ABANDON"
    allocation_id = allocations[0]["allocation_id"]
    # pull is only reached after the RUNNING check, so the scope really was live
    assert ("connector.pull", allocation_id) in ray_driver.connector.log
    assert ("provider.ensure_present", allocation_id) in ray_driver.provider.log


def test_worker_failure_frame_closes_the_scope(tmp_path, ray_driver):
    ray_driver.connector.behaviour["pull"] = "failure"
    run, _, error = _fail(tmp_path, "worker-failure")
    assert "InjectedActorDeath" in str(error)
    scopes, allocations, outstanding = _ledger(run)
    assert [s["state"] for s in scopes] == ["CLOSED"]
    assert [a["cleanup"] for a in allocations] == ["CONFIRMED"]
    assert outstanding == 0


def test_control_timeout_closes_the_scope(tmp_path, ray_driver):
    ray_driver.timeout["control_timeout_s"] = 0.05
    run, _, error = _fail(tmp_path, "timeout")
    assert "did not emit Register" in str(error)
    scopes, allocations, outstanding = _ledger(run)
    assert [s["state"] for s in scopes] == ["CLOSED"]
    assert [a["cleanup"] for a in allocations] == ["CONFIRMED"]
    assert outstanding == 0


def test_actor_kill_runs_after_the_resource_terminal(tmp_path, ray_driver):
    ray_driver.connector.behaviour["pull"] = "raise"
    run, _, _ = _fail(tmp_path, "ordering")
    allocation_id = _ledger(run)[1][0]["allocation_id"]
    log = ray_driver.provider.log + ray_driver.connector.log
    absent = log.index(("provider.ensure_absent", allocation_id))
    terminate = log.index(("connector.terminate", allocation_id))
    close = log.index(("connector.close", allocation_id))
    assert absent < terminate < close


def test_unconfirmed_exit_keeps_its_charge(tmp_path, ray_driver):
    ray_driver.connector.behaviour["pull"] = "raise"
    ray_driver.provider.behaviour["cleanup"] = "unconfirmed"
    run, _, _ = _fail(tmp_path, "unconfirmed")
    scopes, allocations, _ = _ledger(run)
    # F6: only a CONFIRMED exit releases capacity; the guard is not weakened
    assert [a["cleanup"] for a in allocations] == ["UNCONFIRMED"]
    assert [s["state"] for s in scopes] == ["CLOSING"]
    log = ray_driver.connector.log
    assert ("connector.terminate", allocations[0]["allocation_id"]) in log
    assert ("connector.close", allocations[0]["allocation_id"]) in log


def test_terminal_failure_still_kills_the_actor_and_keeps_the_original_error(
        tmp_path, ray_driver, monkeypatch, caplog):
    ray_driver.connector.behaviour["pull"] = "raise"

    def broken_close(self, context, close_spec):
        raise RuntimeError("injected: resource terminal unavailable")

    monkeypatch.setattr(ResourceManager, "close", broken_close)
    with caplog.at_level("ERROR"):
        run, _, error = _fail(tmp_path, "terminal-failure")
    assert "unreachable" in str(error), "the cleanup failure must not mask the chain error"
    assert "Ray chain resource terminal failed" in caplog.text
    scopes, allocations, _ = _ledger(run)
    assert [s["state"] for s in scopes] == ["OPEN"]
    log = ray_driver.connector.log
    assert ("connector.terminate", allocations[0]["allocation_id"]) in log
    assert ("connector.close", allocations[0]["allocation_id"]) in log


def test_resume_leaves_no_orphan_scope(tmp_path, ray_driver):
    ray_driver.connector.behaviour["pull"] = "raise"
    run, reference, _ = _fail(tmp_path, "resume")
    first_scope = _ledger(run)[0][0]["scope_id"]
    # The materialize action is still unconfirmed, so the resumed run refuses a
    # clean exit. That is the history guard, not a resource leak: the ledger is
    # read inside the block, before any new attempt work.
    with pytest.raises(RecoveryContractError, match="unconfirmed actions"):
        with RunContext.resume(reference) as run2:
            scopes, allocations, outstanding = _ledger(run2)
            assert first_scope in [s["scope_id"] for s in scopes]
            assert all(s["state"] == "CLOSED" for s in scopes), scopes
            assert all(a["cleanup"] == "CONFIRMED" for a in allocations), allocations
            assert outstanding == 0
            assert run2.attempt > run.attempt


def test_second_failure_after_resume_opens_and_closes_its_own_scope(tmp_path, ray_driver):
    ray_driver.connector.behaviour["pull"] = "raise"
    run, reference, _ = _fail(tmp_path, "twice")
    first_scope = _ledger(run)[0][0]["scope_id"]
    dataset = _dataset()
    with pytest.raises(RecoveryContractError):
        with RunContext.resume(reference) as run2:
            dataset.materialize().take_all()
    scopes, allocations, outstanding = _ledger(run2)
    assert len(scopes) == 2, scopes
    assert all(s["state"] == "CLOSED" for s in scopes), scopes
    assert all(a["cleanup"] == "CONFIRMED" for a in allocations), allocations
    assert outstanding == 0
    assert first_scope in [s["scope_id"] for s in scopes]
    assert len({a["allocation_id"] for a in allocations}) == 2


# ---------------- real single-host Ray Core (env-ray only, marked ray) ----------------
#
# These run only under /yzp/zhaozy/yangzepeng/0905/env-ray/bin/python with -m ray;
# the default env deselects them. Local Ray only (num_cpus=2), no cluster, no
# remote address, no paid model. Ray's session dir is redirected under
# /tmp/demiflow-* and ray.shutdown() always runs.

# Each invocation owns its private cluster; never attach to a running service.


@pytest.fixture(scope="module")
def ray_local(tmp_path_factory):
    ray = pytest.importorskip("ray")
    from pathlib import Path
    root = tmp_path_factory.mktemp("ray-chain")
    assert str(root).startswith("/tmp/demiflow-")
    assert not ray.is_initialized(), "this test must own a private 2-CPU cluster"
    ray.init(address="local", num_cpus=2, num_gpus=0, include_dashboard=False,
             object_store_memory=100 * 1024**2, _temp_dir=str(root / "ray"))
    with pytest.MonkeyPatch.context() as patch:
        for key, suffix in (("DEMIFLOW_RESOURCE_ROOT", "pool"),
                            ("DEMIFLOW_RESOURCE_HOME", "home"),
                            ("DEMIFLOW_FORGE_DOMAIN", "forge")):
            patch.setenv(key, str(root / suffix))
        try:
            yield ray
        finally:
            ray.shutdown()


def _ray_dataset(plan, items):
    from demiflow.execution.executors.ray import RayDatasetExecutor
    return Dataset(ItemsSource(items), plan, RayDatasetExecutor(workers=2))


def _double_plan():
    def double(row):
        return {**row, "x": row["x"] * 2}

    items = tuple({"x": i} for i in range(6))
    plan = LogicalPlan((MapOp(CallableSpec.create(double),
                              stream_options=MapStreamOptions(concurrency=1)),))
    return items, plan, [{"x": i * 2} for i in range(6)]


def _ray_intents(run):
    with run.journal._connection() as db:
        return [{"allocation_id": r[0], "kind": r[1], "status": r[2]}
                for r in db.execute("SELECT allocation_id,kind,status FROM resource_intents")]


def _ray_evidence(run):
    with run.journal._connection() as db:
        return {r[0]: json.loads(r[1]).get("evidence_ref")
                for r in db.execute("SELECT allocation_id,document FROM resource_allocations")}


def _assert_terminal_issued(run, mode):
    """Read the same original resource journal after the unified runtime exits.

    V3 drains individual allocations and uses ABANDON to close the remaining
    scope on every path. A success is proved by its original final ACK, not the
    retired driver's receipt suffix. Every allocation still needs physical exit
    evidence, and an unconfirmed exit remains charged until convergence.
    """
    scopes, allocations, _ = _ledger(run)
    assert scopes and allocations
    for scope in scopes:
        close = scope["close_document"]
        assert close is not None, "unified chain failed to issue its resource terminal"
        assert close["mode"] in ("ABANDON", "DRAINED")
        assert close["terminate_deadline_ms"] > 0
        assert scope["state"] in ("CLOSING", "CLOSED")
    intents = _ray_intents(run)
    absent = [item for item in intents if item["kind"] == "ensure_absent"]
    assert {item["allocation_id"] for item in absent} == {
        allocation["allocation_id"] for allocation in allocations}
    assert all(item["status"] in ("claimed", "done") for item in absent)
    evidence = _ray_evidence(run)
    for allocation in allocations:
        assert allocation["terminal"]
        assert allocation["cleanup"] in ("CONFIRMED", "UNCONFIRMED")
        assert evidence[allocation["allocation_id"]], allocation
    commits = run.journal.execution_commits("action-1")
    if mode == "DRAINED":
        assert commits and all(item["status"] == "acked" for item in commits)
    return scopes, allocations


def _materialize_chain(dataset):
    from demiflow.execution.recovery_identity import plan_identity
    from demiflow.execution.run_context import current_run
    run = current_run()
    with run.action(plan_identity("materialize", dataset._source, dataset._plan)) as action:
        result = chain.run_ray_chain(dataset._executor, dataset)
        try:
            rows = list(result.rows())
            action.complete({"kind": "materialized", "rows": len(rows)})
            return rows
        finally:
            result.release()


def _assert_no_residual_actor(ray, allocations):
    assert ray.util.list_named_actors(all_namespaces=True) == []
    for allocation in allocations:
        with pytest.raises(ValueError):
            ray.get_actor("demiflow-worker-" + allocation["allocation_id"])


def _converge_terminal(run, timeout_s=45.0):
    """Drive the standard F6 protocol until the retained charge is released.

    The controller is rebuilt fresh, exactly as a resume would. An UNCONFIRMED
    observation leaves the ensure_absent intent claimed for its full 30s lease,
    so convergence is bounded by that lease rather than by the kill itself.
    """
    from demiflow.execution.resource_state import RunResourceState
    from demiflow.execution.worker.ray_host import RayControlConnector
    from demiflow.resources.manager import ManagerBounds, ResourceManager
    from demiflow.resources.ray import RayResourceProvider

    manager = ResourceManager(RayResourceProvider(RayControlConnector(), provision_timeout_s=5),
                              RunResourceState(run.journal), bounds=ManagerBounds())
    deadline = time.monotonic() + timeout_s
    while True:
        manager.reconcile_once(run.fence, time.monotonic() + 15, 16)
        scopes, allocations, outstanding = _ledger(run)
        if all(s["state"] == "CLOSED" for s in scopes):
            break
        if time.monotonic() > deadline:
            pytest.fail(f"resource terminal did not converge: {scopes} {allocations}")
        time.sleep(1)
    assert all(a["cleanup"] == "CONFIRMED" for a in allocations), allocations
    assert outstanding == 0
    assert all(i["status"] == "done" for i in _ray_intents(run)
               if i["kind"] == "ensure_absent"), _ray_intents(run)


@pytest.mark.ray
def test_real_ray_success_issues_the_drained_terminal(tmp_path, ray_local):
    items, plan, expected = _double_plan()
    dataset = _ray_dataset(plan, items)
    with RunContext.start(tmp_path / "ok", limits=LIMITS) as run:
        assert _materialize_chain(dataset) == expected
        _, allocations = _assert_terminal_issued(run, "DRAINED")
    _assert_no_residual_actor(ray_local, allocations)


def _boom_plan():
    def boom(row):
        if row["x"] == 3:
            raise ValueError("injected business failure")
        return {**row, "x": row["x"] * 2}

    items = tuple({"x": i} for i in range(6))
    return items, LogicalPlan((MapOp(CallableSpec.create(boom),
                                     stream_options=MapStreamOptions(concurrency=1)),))


@pytest.mark.ray
def test_real_ray_worker_failure_issues_the_abandon_terminal(tmp_path, ray_local):
    items, plan = _boom_plan()
    dataset = _ray_dataset(plan, items)
    with pytest.raises(ValueError) as caught:
        with RunContext.start(tmp_path / "boom", limits=LIMITS) as run:
            _materialize_chain(dataset)
    assert "injected business failure" in str(caught.value)
    _, allocations = _assert_terminal_issued(run, "ABANDON")
    _assert_no_residual_actor(ray_local, allocations)


@pytest.mark.ray
def test_real_ray_terminal_converges_to_closed_and_confirmed(tmp_path, ray_local):
    items, plan = _boom_plan()
    dataset = _ray_dataset(plan, items)
    with pytest.raises(RecoveryContractError, match="unconfirmed actions"):
        with RunContext.start(tmp_path / "converge", limits=LIMITS) as run:
            try:
                _materialize_chain(dataset)
            except ValueError:
                pass
            _assert_terminal_issued(run, "ABANDON")
            _converge_terminal(run)
            scopes, allocations, outstanding = _ledger(run)
    assert scopes and all(s["state"] == "CLOSED" for s in scopes), scopes
    assert allocations and all(a["cleanup"] == "CONFIRMED" for a in allocations), allocations
    assert outstanding == 0
    _assert_no_residual_actor(ray_local, allocations)


@pytest.mark.ray
def test_real_ray_actor_death_terminal_and_resume_leaves_no_orphan(tmp_path, ray_local):
    import os
    import signal

    crash_marker = tmp_path / "crashed-once"

    def crash_once(row):
        if row["x"] == 4 and not crash_marker.exists():
            crash_marker.touch()
            os.kill(os.getpid(), signal.SIGKILL)
        return {**row, "x": row["x"] * 2}

    items = tuple({"x": i} for i in range(6))
    plan = LogicalPlan((MapOp(CallableSpec.create(crash_once),
                              stream_options=MapStreamOptions(concurrency=1)),))
    dataset = _ray_dataset(plan, items)
    with pytest.raises(Exception):
        with RunContext.start(tmp_path / "crash", limits=LIMITS) as run:
            reference = run.ref
            _materialize_chain(dataset)
    assert crash_marker.exists(), "the worker process never actually died"
    scopes, allocations = _assert_terminal_issued(run, "ABANDON")
    first_scope = scopes[0]["scope_id"]
    _assert_no_residual_actor(ray_local, allocations)

    with RunContext.resume(reference) as run2:
        assert _materialize_chain(dataset) == [{"x": i * 2} for i in range(6)]
        _converge_terminal(run2)
        scopes2, allocations2, outstanding2 = _ledger(run2)
    assert len(scopes2) == len(scopes) + 1, scopes2
    assert {a["allocation_id"] for a in allocations} < {a["allocation_id"] for a in allocations2}
    assert first_scope in [s["scope_id"] for s in scopes2]
    assert all(s["state"] == "CLOSED" for s in scopes2), scopes2
    assert all(a["cleanup"] == "CONFIRMED" for a in allocations2), allocations2
    assert outstanding2 == 0
    assert run2.attempt > run.attempt
    _assert_no_residual_actor(ray_local, allocations2)


@pytest.mark.ray
def test_real_ray_failure_before_any_progress_leaves_no_residual_actor(tmp_path, ray_local):
    def boom(row):
        raise ValueError("injected failure for residue check")

    items = tuple({"x": i} for i in range(3))
    plan = LogicalPlan((MapOp(CallableSpec.create(boom),
                              stream_options=MapStreamOptions(concurrency=1)),))
    dataset = _ray_dataset(plan, items)
    with pytest.raises(ValueError):
        with RunContext.start(tmp_path / "residue", limits=LIMITS) as run:
            _materialize_chain(dataset)
    _, allocations = _assert_terminal_issued(run, "ABANDON")
    # a named actor left behind would still be adoptable; Ray must report it gone
    _assert_no_residual_actor(ray_local, allocations)