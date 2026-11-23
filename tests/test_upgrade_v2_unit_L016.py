"""L016 obligations on the real unified Ray chain and original resource journal.

All 28 original nodeids remain. Retired v2 AST checks now inspect actual sealed
assignments and releases. The former fake driver fault cases now inject only a
specific fault into the real Ray provider/endpoint, and inspect original ledger
state before any test-owned repair. The five original real cases keep their
six input rows, two CPUs, checkpoint policy, actual actor death and resume.
A missing Ray environment is not a passing result. No Ray Data is permitted.
"""
import json
import time

import pytest

from demiflow.data.dataset import Dataset
from demiflow.data.plan import CallableSpec, LogicalPlan, MapOp, MapStreamOptions
from demiflow.data.sources import ItemsSource
from demiflow.execution.recovery_contract import RecoveryContractError
from demiflow.execution.run_context import RunContext
from demiflow.resources.manager import ResourceManager

@pytest.mark.ray
def test_layout_is_interpreted_from_the_sealed_plan(ray_chain_facts):
    chain_contract.test_layout_is_interpreted_from_the_sealed_plan(ray_chain_facts)


@pytest.mark.ray
def test_worker_profile_comes_from_the_layout(ray_chain_facts):
    chain_contract.test_worker_profile_comes_from_the_layout(ray_chain_facts)


@pytest.mark.ray
def test_stages_and_partitions_come_from_the_placement(ray_chain_facts):
    chain_contract.test_stages_and_partitions_come_from_the_placement(ray_chain_facts)


@pytest.mark.ray
def test_program_binding_comes_from_the_sealed_bindings(ray_chain_facts):
    chain_contract.test_program_binding_comes_from_the_sealed_bindings(ray_chain_facts)


@pytest.mark.ray
def test_assignment_channels_are_validated_against_the_layout(ray_chain_facts):
    chain_contract.test_assignment_channels_are_validated_against_the_layout(ray_chain_facts)


@pytest.mark.ray
def test_driver_holds_the_only_commit_authority(ray_chain_facts):
    chain_contract.test_driver_holds_the_only_commit_authority(ray_chain_facts)


@pytest.mark.ray
def test_no_local_capacity_double_charge(ray_chain_facts):
    from demiflow.resources.ray import RayResourceProvider
    facts = ray_chain_facts
    manager = facts['execution']._services.resources
    assert isinstance(manager.provider, RayResourceProvider)
    assert manager.state.journal is facts['run'].journal
    assert facts['local_reservations'] == []


@pytest.mark.ray
def test_actor_is_terminated_on_every_exit(ray_chain_facts, ray_local):
    scopes, allocations, outstanding = _ledger(ray_chain_facts['run'])
    assert scopes and all(s['state'] == 'CLOSED' for s in scopes)
    assert allocations and all(a['cleanup'] == 'CONFIRMED' for a in allocations)
    assert outstanding == 0
    _assert_no_residual_actor(ray_local, allocations)


@pytest.mark.ray
def test_restore_point_comes_from_the_terminal_journal(tmp_path, ray_local):
    chain_contract.test_restore_point_comes_from_the_terminal_journal(tmp_path)


@pytest.mark.ray
def test_source_capture_is_backend_neutral(ray_chain_facts):
    facts = ray_chain_facts
    assert len(facts['graph'].sources) == 1
    expected = {split.split_id for split in facts['physical'].splits}
    actual = {split.split_id for assignment in facts['assignments'] for split in assignment.source_splits}
    assert actual == expected and actual
    assert facts['ray_data_imports'] == []


# ---------------- structural: the unified resource terminal (the fix) ----------------

@pytest.mark.ray
def test_resource_terminal_runs_in_the_finally(tmp_path, ray_local):
    chain_contract.test_failure_terminal_releases_the_resource_scope(tmp_path)


@pytest.mark.ray
def test_terminal_mode_follows_the_drain_flag(ray_chain_facts):
    facts = ray_chain_facts
    _assert_terminal_issued(facts['run'], 'DRAINED')
    # Stage rotation uses DRAINED retirements. Final execution close may use the
    # scope ABANDON terminal after the worker drained and its final cut ACKed.
    # Check that actual ordering, rather than requiring the retired v2 mode.
    releases = [event for event in facts['terminal_events'] if event[0] == 'release']
    assert all(mode == 'DRAINED' for _, _, mode in releases)
    events = facts['terminal_events']
    terminals = [(i, event) for i, event in enumerate(events) if event[0] == 'scope_close']
    assert terminals and all(event[2] == 'ABANDON' for _, event in terminals)
    for index, _ in terminals:
        assert any(event[0] == 'acked' for event in events[:index])
        assert any(event[0] == 'drained' for event in events[:index])


@pytest.mark.ray
def test_actor_kill_stays_after_the_resource_terminal(ray_chain_facts):
    events = ray_chain_facts['terminal_events']
    killed = [(i, value[1]) for i, value in enumerate(events) if value[0] == 'kill']
    assert killed
    for index, allocation in killed:
        assert ('drained', allocation) in events[:index]
        assert any((value[0] == 'release' and value[1] == allocation)
                   or value[0] == 'scope_close' for value in events[:index])
        assert any(value[0] == 'connector_close' and value[1] == allocation for value in events[index + 1:])


@pytest.mark.ray
def test_local_and_ray_share_one_terminal_protocol(ray_chain_facts):
    from demiflow.execution.action_driver.dataflow import DataflowExecution
    assert type(ray_chain_facts['execution']) is DataflowExecution
    from demiflow.execution.executors.base import DatasetExecutor
    from demiflow.execution.executors.local import LocalDatasetExecutor
    from demiflow.execution.executors.ray import RayDatasetExecutor
    assert LocalDatasetExecutor.execute is RayDatasetExecutor.execute is DatasetExecutor.execute




import test_upgrade_v2_unit_L015 as chain_contract


@pytest.fixture(scope='module')
def ray_chain_facts(ray_local, tmp_path_factory):
    import builtins
    from demiflow.resources.capacity import LocalCapacityPool
    from demiflow.execution.dataflow_hosts import DataflowRayConnector, RayHostEndpoint
    from demiflow.execution.commit import CommitCoordinator
    events, reservations, imports = [], [], []
    original_apply = ResourceManager.apply
    original_terminal = ResourceManager.close
    original_drain, original_commit = RayHostEndpoint.drain, CommitCoordinator.commit
    original_kill, original_close = ray_local.kill, DataflowRayConnector.close
    original_reserve, original_import = LocalCapacityPool.reserve, builtins.__import__
    def apply(manager, context, plan):
        result = original_apply(manager, context, plan)
        for release in plan.retirements:
            events.append(('release', release.allocation_id, release.mode))
        return result
    def terminate(actor, *args, **kwargs):
        events.append(('kill_actor', actor._actor_id.hex()))
        return original_kill(actor, *args, **kwargs)
    def close(connector, allocation_id):
        events.append(('connector_close', allocation_id))
        return original_close(connector, allocation_id)
    def terminal(manager, context, spec):
        result = original_terminal(manager, context, spec)
        events.append(('scope_close', context.scope_id, spec['mode']))
        return result
    def drain(endpoint, *, deadline_ms):
        result = original_drain(endpoint, deadline_ms=deadline_ms)
        events.append(('drained', endpoint._allocation_id))
        return result
    def commit(owner, *args, **kwargs):
        result = original_commit(owner, *args, **kwargs)
        assert result['status'] == 'acked'
        events.append(('acked', result['commit_id']))
        return result
    def reserve(pool, *args, **kwargs):
        reservations.append(args)
        return original_reserve(pool, *args, **kwargs)
    def importing(name, *args, **kwargs):
        if name == 'ray.data' or name.startswith('ray.data.'):
            imports.append(name)
            raise AssertionError('Ray Data is not a unified execution backend')
        return original_import(name, *args, **kwargs)
    with pytest.MonkeyPatch.context() as patch:
        patch.setenv('DEMIFLOW_V3_PROVIDER', 'ray_core')
        patch.setattr(ResourceManager, 'apply', apply)
        patch.setattr(ResourceManager, 'close', terminal)
        patch.setattr(RayHostEndpoint, 'drain', drain)
        patch.setattr(CommitCoordinator, 'commit', commit)
        patch.setattr(ray_local, 'kill', terminate)
        patch.setattr(DataflowRayConnector, 'close', close)
        patch.setattr(LocalCapacityPool, 'reserve', reserve)
        patch.setattr(builtins, '__import__', importing)
        source = chain_contract.actual_chain.__wrapped__(tmp_path_factory)
        try:
            facts = next(source)
        finally:
            source.close()
        with facts['run'].journal._connection() as db:
            actor_ids = {json.loads(raw)['placement']['actor_id']: key for key, raw in
                db.execute('SELECT allocation_id,document FROM resource_allocations')}
        events = [('kill', actor_ids[event[1]]) if event[0] == 'kill_actor' else event for event in events]
        facts.update(terminal_events=events, local_reservations=reservations, ray_data_imports=imports)
    yield facts



# ---------------- real Ray faults, original resource/journal authority ----------------

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


def _repair_owned_resource_terminal(execution):
    """Test cleanup after an injected cleanup failure, under the original fence.

    This never changes a verdict or writes ledger rows directly. Assertions use
    the captured pre-repair observations; reconciliation is the native protocol.
    """
    scheduler = execution._scheduler
    if scheduler is None or not execution._scope_open:
        return
    manager = execution._services.resources
    def scope_state():
        return next(scope['state'] for scope in _ledger(execution._action.context)[0]
                    if scope['scope_id'] == scheduler.scope_id)
    if scope_state() == 'OPEN':
        scheduler.close(deadline_ms=45000)
        return
    deadline = time.monotonic() + 45
    while scope_state() != 'CLOSED':
        assert time.monotonic() < deadline, 'owned failure fixture did not clean up'
        manager.reconcile_once(execution._fence(), deadline, 16)
        time.sleep(.02)


def _ray_failure(tmp_path, ray, *, mode, reference=None):
    """Inject one transport/provision fault; execute the real Ray chain driver."""
    from demiflow.execution.action_driver.dataflow import DataflowExecution
    from demiflow.execution.dataflow_hosts import RayHostEndpoint, DataflowRayConnector
    from demiflow.execution.scheduler.placement import DataflowScheduler
    from demiflow.execution.contracts.dataflow_errors import DataflowError
    from demiflow.resources.contracts import ResourceError
    from demiflow.resources.ray import RayResourceProvider
    original_init = DataflowExecution.__init__
    original_activate = RayHostEndpoint.activate
    original_command = RayHostEndpoint._send_k3
    original_present = RayResourceProvider.ensure_present
    original_absent = RayResourceProvider.ensure_absent
    original_close = ResourceManager.close
    original_scheduler_close = DataflowScheduler.close
    original_kill = ray.kill
    original_connector_close = DataflowRayConnector.close
    executions, events, actor_allocations = [], [], {}
    def created(execution, *args, **kwargs):
        original_init(execution, *args, **kwargs)
        executions.append(execution)
    def present(provider, allocation_id, spec):
        result = original_present(provider, allocation_id, spec)
        if mode == 'before':
            # The real provider owns an unactivated shell and can prove its
            # eventual death; no fake 'never-created' proof is manufactured.
            result = dict(result, phase='FAILED', conditions=['LAUNCH_FAILED', 'injected placement failure'])
        events.append(('present', allocation_id, result['phase']))
        return result
    def activate(endpoint, assignment):
        original_activate(endpoint, assignment)
        events.append(('activated', assignment.lease.allocation_id))
        if mode in ('after', 'unconfirmed', 'terminal_failure'):
            raise ConnectionError('injected: ray actor unreachable')
    def command(endpoint, kind, payload):
        if mode == 'timeout' and kind == 'VerifyArtifactAndCapabilities':
            raise DataflowError('DEADLINE_EXCEEDED', 'injected Ray registration deadline')
        return original_command(endpoint, kind, payload)
    def absent(provider, identity, deadline_ms):
        events.append(('absent', identity['allocation_id']))
        owned = provider._actors.get(identity['allocation_id'])
        if owned is not None:
            actor_allocations[owned[0]._actor_id.hex()] = identity['allocation_id']
        result = original_absent(provider, identity, deadline_ms)
        if mode == 'unconfirmed':
            # Lose only the confirmation. Physical teardown still ran, but its
            # unobserved success cannot release the original logical charge.
            return dict(result, cleanup='UNCONFIRMED',
                        evidence_ref='ray-actor-unconfirmed:' + identity['allocation_id'])
        return result
    def close(manager, context, spec):
        if mode == 'terminal_failure':
            raise RuntimeError('injected: resource terminal unavailable')
        result = original_close(manager, context, spec)
        events.append(('terminal', context.scope_id))
        return result
    def scheduler_close(scheduler, *, deadline_ms):
        return original_scheduler_close(scheduler,
            deadline_ms=min(deadline_ms, 200) if mode == 'unconfirmed' else deadline_ms)
    def kill(actor, *args, **kwargs):
        events.append(('kill', actor_allocations.get(actor._actor_id.hex(), actor._actor_id.hex())))
        return original_kill(actor, *args, **kwargs)
    def connector_close(connector, allocation_id):
        events.append(('connector_close', allocation_id))
        return original_connector_close(connector, allocation_id)
    items, plan = _boom_plan() if mode == 'frame' else _double_plan()[:2]
    dataset = _ray_dataset(plan, items)
    expected = (ValueError if mode == 'frame' else DataflowError
                if mode in ('before', 'timeout') else ConnectionError)
    context = RunContext.start(tmp_path / 'run', limits=LIMITS) if reference is None else RunContext.resume(reference)
    with pytest.raises(expected) as caught:
        with context as run:
            with pytest.MonkeyPatch.context() as patch:
                for owner, name, replacement in (
                    (DataflowExecution, '__init__', created),
                    (RayResourceProvider, 'ensure_present', present),
                    (RayResourceProvider, 'ensure_absent', absent),
                    (RayHostEndpoint, 'activate', activate),
                    (RayHostEndpoint, '_send_k3', command),
                    (ResourceManager, 'close', close),
                    (DataflowScheduler, 'close', scheduler_close),
                    (ray, 'kill', kill),
                    (DataflowRayConnector, 'close', connector_close)):
                    patch.setattr(owner, name, replacement)
                try:
                    _materialize_chain(dataset)
                except expected:
                    snapshot = _ledger(run)
                    actors = tuple(ray.util.list_named_actors(all_namespaces=True))
                    if executions:
                        manager = executions[-1]._services.resources
                        with run.journal._connection() as db:
                            charged = manager.state._charged(db, executions[-1]._scheduler.scope_id)
                    else:
                        charged = None
                    raise
                finally:
                    patch.undo()
                    for execution in executions:
                        _repair_owned_resource_terminal(execution)
    return dict(run=run, reference=run.ref, error=caught.value, snapshot=snapshot,
                actors=actors, charged=charged, events=events, executions=executions)


def _assert_closed_failure(facts):
    scopes, allocations, outstanding = facts['snapshot']
    assert scopes and all(scope['state'] == 'CLOSED' for scope in scopes)
    assert allocations and all(a['cleanup'] == 'CONFIRMED' and a['terminal'] for a in allocations)
    assert outstanding == 0 and facts['actors'] == ()
    assert all(scope['close_document']['mode'] == 'ABANDON' for scope in scopes)


@pytest.mark.ray
def test_failure_before_running_closes_the_scope(tmp_path, ray_local):
    facts = _ray_failure(tmp_path, ray_local, mode='before')
    assert 'did not reach RUNNING' in str(facts['error'])
    assert not any(event[0] == 'activated' for event in facts['events'])
    _assert_closed_failure(facts)
    assert facts['run'].journal.execution_commits('action-1') == []


@pytest.mark.ray
def test_actor_loss_after_running_closes_the_scope(tmp_path, ray_local):
    facts = _ray_failure(tmp_path, ray_local, mode='after')
    assert 'unreachable' in str(facts['error'])
    assert any(event[0] == 'activated' for event in facts['events'])
    _assert_closed_failure(facts)


@pytest.mark.ray
def test_worker_failure_frame_closes_the_scope(tmp_path, ray_local):
    facts = _ray_failure(tmp_path, ray_local, mode='frame')
    assert 'injected business failure' in str(facts['error'])
    _assert_closed_failure(facts)


@pytest.mark.ray
def test_control_timeout_closes_the_scope(tmp_path, ray_local):
    facts = _ray_failure(tmp_path, ray_local, mode='timeout')
    assert facts['error'].code == 'DEADLINE_EXCEEDED'
    assert 'registration deadline' in str(facts['error'])
    _assert_closed_failure(facts)
    assert facts['run'].journal.execution_commits('action-1') == []


@pytest.mark.ray
def test_actor_kill_runs_after_the_resource_terminal(tmp_path, ray_local):
    facts = _ray_failure(tmp_path, ray_local, mode='after')
    _assert_closed_failure(facts)
    events = facts['events']
    terminals = [i for i, event in enumerate(events) if event[0] == 'terminal']
    kills = [(i, event[1]) for i, event in enumerate(events) if event[0] == 'kill']
    assert terminals and kills
    for index, allocation in kills:
        assert min(terminals) < index
        assert any(event == ('absent', allocation) for event in events[:index])
        assert ('connector_close', allocation) in events[index + 1:]


@pytest.mark.ray
def test_unconfirmed_exit_keeps_its_charge(tmp_path, ray_local):
    facts = _ray_failure(tmp_path, ray_local, mode='unconfirmed')
    scopes, allocations, _ = facts['snapshot']
    assert scopes and all(scope['state'] == 'CLOSING' for scope in scopes)
    assert allocations and all(a['cleanup'] == 'UNCONFIRMED' for a in allocations)
    assert facts['charged']['workers'] == len(allocations)
    assert facts['charged']['cpu_millis'] > 0
    assert facts['charged']['memory_bytes'] > 0
    assert facts['actors'] == (), 'unknown receipt must retain charge even after real death'
    assert any('worker exit remains unconfirmed' in note for note in facts['error'].__notes__)
    assert all(scope['state'] == 'CLOSED' for scope in _ledger(facts['run'])[0])


@pytest.mark.ray
def test_terminal_failure_still_kills_the_actor_and_keeps_the_original_error(tmp_path, ray_local):
    facts = _ray_failure(tmp_path, ray_local, mode='terminal_failure')
    assert 'unreachable' in str(facts['error'])
    assert any('resource terminal unavailable' in note for note in facts['error'].__notes__)
    scopes, allocations, _ = facts['snapshot']
    assert scopes and all(scope['state'] == 'OPEN' for scope in scopes)
    assert all(a['cleanup'] == 'CONFIRMED' for a in allocations)
    assert facts['charged']['workers'] == 0
    assert facts['charged']['cpu_millis'] == facts['charged']['memory_bytes'] == 0
    assert facts['actors'] == (), 'a failed ledger close must not leave its owned worker alive'
    assert any(event[0] == 'kill' for event in facts['events'])


@pytest.mark.ray
def test_resume_leaves_no_orphan_scope(tmp_path, ray_local):
    facts = _ray_failure(tmp_path, ray_local, mode='after')
    first = {scope['scope_id'] for scope in facts['snapshot'][0]}
    with pytest.raises(RecoveryContractError, match='unconfirmed actions'):
        with RunContext.resume(facts['reference']) as resumed:
            scopes, allocations, outstanding = _ledger(resumed)
            assert first <= {scope['scope_id'] for scope in scopes}
            assert all(scope['state'] == 'CLOSED' for scope in scopes)
            assert all(a['cleanup'] == 'CONFIRMED' for a in allocations)
            assert outstanding == 0 and resumed.attempt > facts['run'].attempt


@pytest.mark.ray
def test_second_failure_after_resume_opens_and_closes_its_own_scope(tmp_path, ray_local):
    # Resume needs an original committed cut. Both attempts really fail in the
    # worker at row 3, after the first two rows ACK; launch failure alone is not
    # recoverable history and must never be admitted from its sealed files.
    first = _ray_failure(tmp_path, ray_local, mode='frame')
    original_commits = first['run'].journal.execution_commits('action-1')
    assert original_commits and all(item['status'] == 'acked' for item in original_commits)
    second = _ray_failure(tmp_path, ray_local, mode='frame', reference=first['reference'])
    _assert_closed_failure(second)
    assert second['run'].journal.execution_commits('action-1') == original_commits
    first_scopes, first_allocations, _ = first['snapshot']
    scopes, allocations, _ = second['snapshot']
    assert len(scopes) == len(first_scopes) + 1
    assert {s['scope_id'] for s in first_scopes} < {s['scope_id'] for s in scopes}
    assert {a['allocation_id'] for a in first_allocations} < {a['allocation_id'] for a in allocations}
    assert second['run'].attempt > first['run'].attempt


# ---------------- real single-host Ray Core (env-ray only, marked ray) ----------------
#
# These run only under /yzp/zhaozy/yangzepeng/0905/env-ray/bin/python with -m ray;
# the default env deselects them. Local Ray only (num_cpus=2), no cluster, no
# remote address, no paid model. Ray's session dir is redirected under
# /tmp/demiflow-* and ray.shutdown() always runs.

# Each invocation owns its private cluster; never attach to a running service.


@pytest.fixture(scope="module")
def ray_local(tmp_path_factory):
    import tempfile
    ray = pytest.importorskip("ray")
    root = tmp_path_factory.mktemp("ray-chain")
    assert str(root).startswith("/tmp/demiflow-")
    assert not ray.is_initialized(), "this test must own a private 2-CPU cluster"
    # AF_UNIX has a 107-byte path ceiling. Ray appends its timestamp and socket
    # names, so use a short owned directory rather than the long pytest node path.
    with tempfile.TemporaryDirectory(prefix="demiflow-l16-") as ray_root:
        with pytest.MonkeyPatch.context() as patch:
            patch.setenv('DEMIFLOW_V3_PROVIDER', 'ray_core')
            for key, suffix in (("DEMIFLOW_RESOURCE_ROOT", "pool"),
                                ("DEMIFLOW_RESOURCE_HOME", "home"),
                                ("DEMIFLOW_FORGE_DOMAIN", "forge")):
                patch.setenv(key, str(root / suffix))
            try:
                ray.init(address="local", num_cpus=2, num_gpus=0, include_dashboard=False,
                         object_store_memory=100 * 1024**2, _temp_dir=ray_root)
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
    absent_ids = {item["allocation_id"] for item in absent}
    allocation_ids = {item["allocation_id"] for item in allocations}
    assert absent_ids <= allocation_ids
    assert {a["allocation_id"] for a in allocations if a["cleanup"] != "CONFIRMED"} <= absent_ids
    assert all(item["status"] in ("claimed", "done") for item in absent)
    evidence = _ray_evidence(run)
    for allocation in allocations:
        assert allocation["terminal"]
        assert allocation["cleanup"] in ("CONFIRMED", "UNCONFIRMED")
        proof = evidence[allocation["allocation_id"]]
        assert proof and proof.startswith(("ray-actor-dead:", "ray-actor-alive:",
            "ray-actor-unconfirmed:", "never-created:")), allocation
        if allocation["cleanup"] == "CONFIRMED":
            assert proof.startswith(("ray-actor-dead:", "never-created:")), allocation
    commits = run.journal.execution_commits("action-1")
    if mode == "DRAINED":
        assert commits and all(item["status"] == "acked" for item in commits)
    return scopes, allocations


def _materialize_chain(dataset):
    with dataset.materialize() as result:
        return list(result._source.handle.rows())


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
    from demiflow.execution.hosts.run_state import RunResourceState
    from demiflow.execution.hosts.ray_worker_host import RayControlConnector
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
    original_commits = run.journal.execution_commits("action-1")
    assert original_commits and all(item["status"] == "acked" for item in original_commits)
    first_scope = scopes[0]["scope_id"]
    _assert_no_residual_actor(ray_local, allocations)

    with RunContext.resume(reference) as run2:
        assert _materialize_chain(dataset) == [{"x": i * 2} for i in range(6)]
        _converge_terminal(run2)
        scopes2, allocations2, outstanding2 = _ledger(run2)
        assert run2.journal.execution_commits("action-1")[:len(original_commits)] == original_commits
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
