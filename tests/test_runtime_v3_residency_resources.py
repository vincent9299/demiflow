"""Real resource lifecycles remain bounded without deleting original history."""
from dataclasses import replace
from types import SimpleNamespace

import pytest

from test_u2_resources_local import local_env, context, plan_payload, sleep_argv, reconcile
from demiflow.resources.contracts import ResourceError
from demiflow.execution.scheduler.placement import DataflowScheduler
from demiflow.execution.worker.dataflow_execution import DataflowWorkerExecutor


@pytest.fixture
def lifecycle_authority(local_env):
    from demiflow.execution.run_context import RunAction
    from demiflow.execution.dataflow_authority import DriverJournalService
    from demiflow.planning.dataflow_policy import resolve_runtime_policy
    run = local_env[0]
    _, bounds = resolve_runtime_policy({}, run.limits, provider='local')
    authority = DriverJournalService(RunAction(run, 'control-only', {}), store=None,
                                     bounds=replace(bounds, max_partitions=2))
    try:
        yield authority
    finally:
        authority.close(deadline_ms=1000)


def _lease(allocation):
    import time
    from demiflow.execution.contracts.dataflow import ArtifactRef, WorkerLease
    return WorkerLease(allocation, 'boot-'+allocation, 'stage', ('partition',), 'local', allocation,
                       ArtifactRef('a'*64, 0, 'store', 'grant', 'application/json'), int(time.time()*1000)+60000)


def test_seventy_actual_lifecycles_keep_history_and_active_admission_bounded(local_env, lifecycle_authority):
    run, pool, unit, provider, state, manager = local_env
    state.max_allocations = 1
    manager.bounds = replace(manager.bounds, max_retirements=1)
    scope = 'scope-1'
    revision = 0
    for index in range(70):
        allocation = 'phase-%03d' % index
        receipt = manager.apply(context(run, 'start-%d' % index, revision),
            plan_payload(scope, [allocation], sleep_argv(30), cpu=1, mem=1024))
        revision = receipt['desired_revision']
        reconcile(manager, run)
        assert state.read_allocation(scope, allocation)['phase'] == 'RUNNING'
        lease = _lease(allocation)
        lifecycle_authority.register_worker(lease, ('node',))
        assert pool.snapshot()['charged']['cpu_millis'] == 1
        release = {'allocation_id': allocation, 'mode': 'DRAINED',
                   'engine_receipt_ref': 'actual-cut-%d' % index,
                   'terminate_deadline_ms': 3000, 'reason': 'phase_committed'}
        receipt = manager.apply(context(run, 'retire-%d' % index, revision),
            {'scope_id': scope, 'desired_workers': [], 'retirements': [release]})
        revision = receipt['desired_revision']
        reconcile(manager, run)
        assert state.read_allocation(scope, allocation)['cleanup'] == 'CONFIRMED'
        lifecycle_authority.retire_worker(lease)
        assert not lifecycle_authority._leases and not lifecycle_authority._worker_occurrences
        assert pool.snapshot()['charged']['cpu_millis'] == 0
        assert not provider._spawned
        with run.journal._connection() as db:
            assert len(state._scope_allocations(db, scope, active_only=True)) == 0
            assert db.execute('SELECT count(*) FROM resource_allocations').fetchone()[0] == index + 1
    first = manager.get(scope)
    assert len(first['allocations']) == 64 and first['next_page_token']
    second = manager.get(scope, first['next_page_token'])
    assert len(second['allocations']) == 6 and second['next_page_token'] is None
    scheduler = object.__new__(DataflowScheduler)
    scheduler._resources = manager
    scheduler.scope_id = scope
    assert scheduler._observation('phase-069')['cleanup'] == 'CONFIRMED'
    assert scheduler._observation('phase-000')['cleanup'] == 'CONFIRMED'


def test_unknown_exit_still_charges_and_blocks_replacement(local_env, lifecycle_authority, monkeypatch):
    run, pool, unit, provider, state, manager = local_env
    state.max_allocations = 1
    lease = _lease('old')
    lifecycle_authority.register_worker(lease, ('node',))
    manager.apply(context(run, 'start', 0), plan_payload('scope-1', ['old'], sleep_argv(30), cpu=1, mem=1024))
    reconcile(manager, run)
    release = {'allocation_id': 'old', 'mode': 'DRAINED', 'engine_receipt_ref': 'cut',
               'terminate_deadline_ms': 1000, 'reason': 'phase_committed'}
    manager.apply(context(run, 'retire', 1), {'scope_id': 'scope-1', 'desired_workers': [], 'retirements': [release]})
    with monkeypatch.context() as patch:
        patch.setattr(unit, 'stop', lambda *a, **k: {'confirmed': False, 'evidence_ref': 'unknown-exit'})
        reconcile(manager, run)
    assert state.read_allocation('scope-1', 'old')['cleanup'] == 'UNCONFIRMED'
    from demiflow.execution.contracts.dataflow_errors import DataflowError
    with pytest.raises(DataflowError, match='cleanup is not confirmed'):
        lifecycle_authority.retire_worker(lease)
    assert lifecycle_authority._leases
    assert pool.snapshot()['charged']['cpu_millis'] == 1
    with pytest.raises(ResourceError, match='active scope allocation count'):
        manager.apply(context(run, 'replacement', 2), plan_payload('scope-1', ['next'], sleep_argv(30), cpu=1, mem=1024))
    reconcile(manager, run)
    assert state.read_allocation('scope-1', 'old')['cleanup'] == 'CONFIRMED'
    lifecycle_authority._pending_payloads['pending'] = SimpleNamespace(allocation_id='old', boot_id=lease.boot_id)
    with pytest.raises(DataflowError, match='unfinished journal RPCs'):
        lifecycle_authority.retire_worker(lease)
    lifecycle_authority._pending_payloads.clear()
    lifecycle_authority.retire_worker(lease)
    assert not lifecycle_authority._leases
    assert pool.snapshot()['charged']['cpu_millis'] == 0


def test_worker_control_ids_change_on_boot_business_route_identity_does_not():
    from demiflow.execution.exchange.dataflow_channels import route_record
    from demiflow.execution.exchange.dataflow_state import RoutingCounters
    from demiflow.execution.contracts.dataflow import ExchangeSpec, Record
    from demiflow.execution.recovery_contract import identity
    from demiflow.execution.contracts.record_codec import _encode_payload
    def worker(boot, attempt):
        e = object.__new__(DataflowWorkerExecutor)
        e._event_sequence = 0
        e.assignment = SimpleNamespace(action_id='action', attempt=attempt,
            lease=SimpleNamespace(allocation_id='allocation-' + boot, boot_id=boot))
        return e
    first, restart, resumed = worker('first', 1), worker('second', 1), worker('third', 2)
    assert len({e._next_operation_id('idle') for e in (first, restart, resumed)}) == 3
    spec = ExchangeSpec('ch', 'e'*64, ('p',), ('c',), 'durable_incremental', 'forward', (),
                        None, 0, 4, 1024, 8192, 'store', 'test')
    raw = _encode_payload({'n': 1}, max_bytes=1024)
    record = Record('a'*64, {'position': 1}, None, {'n': 1}, len(raw), 1)
    left = route_record(record, spec=spec, state=RoutingCounters(values={'ch': 7}, route_epoch=0))
    right = route_record(record, spec=spec, state=RoutingCounters(values={'ch': 7}, route_epoch=0))
    assert left == right
    assert identity(['producer-delivery-v3', 'p', left[0].delivery_id]) == identity(['producer-delivery-v3', 'p', right[0].delivery_id])


def test_retirement_clears_only_volatile_native_replies_and_fences_old_boot(tmp_path):
    from test_runtime_v3_native_rpc_replay import interrupted_run, service, event, charged
    from demiflow.execution.hosts.run_state import RunResourceState
    from demiflow.resources.capacity import LocalCapacityPool
    from demiflow.resources.process_unit import ManagedProcessUnit
    from demiflow.resources.local import LocalResourceProvider
    from demiflow.resources.manager import ResourceManager, ManagerBounds
    from demiflow.resources.contracts import ResourceScope
    with interrupted_run(tmp_path/'run') as run, service(run) as (authority, store, lease):
        pool = LocalCapacityPool(tmp_path/'pool', capacity={'cpu_millis': 1000, 'memory_bytes': 1024**2})
        unit = ManagedProcessUnit(tmp_path/'units')
        provider = LocalResourceProvider(pool, unit)
        state = RunResourceState(run.journal, claim_lease_ms=1)
        manager = ResourceManager(provider, state, bounds=ManagerBounds(claim_lease_ms=1))
        state.open_scope(run.fence, ResourceScope('scope-1', 'run', {'provider': 'local'}, 'grant',
                         {'cpu_millis': 1000, 'memory_bytes': 1024**2, 'max_workers': 1}))
        try:
            manager.apply(context(run, 'start', 0), plan_payload('scope-1', ['worker-1'], sleep_argv(30), cpu=1, mem=1024))
            reconcile(manager, run)
            request = event(run, store)
            assert authority.dispatch(request).status == 'ok'
            assert len(authority._native_reserve_replies) == 1 and charged(run) == (1, 1)
            manager.close(context(run, 'close', 1), {'mode': 'DRAINED', 'engine_receipt_ref': 'cut',
                           'terminate_deadline_ms': 3000, 'reason': 'parked'})
            reconcile(manager, run)
            authority.retire_worker(lease)
            assert not authority._native_reserve_replies and not authority._native_reserve_owners
            assert charged(run) == (1, 1)  # Original paid request and budget fact survive.
            assert authority.dispatch(request).error_code == 'FENCED'
        finally:
            for process in list(provider._spawned.values()):
                if process.poll() is None:
                    process.kill()
                    process.wait(timeout=5)
