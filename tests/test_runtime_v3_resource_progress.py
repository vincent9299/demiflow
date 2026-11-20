"""Real Dataset admission and lease progress, using private capacity domains."""
import time
from dataclasses import replace

import pytest

from demiflow import data
from demiflow.execution.run_context import RunContext
from demiflow.execution.contracts.dataflow_errors import DataflowError


def test_runs_share_one_capacity_domain(tmp_path, monkeypatch):
    from demiflow.execution.dataflow_capacity import local_capacity
    from demiflow.execution.dataflow_services import _driver_resources
    from demiflow.planning.dataflow_policy import resolve_runtime_policy
    from demiflow.resources.capacity import LocalCapacityPool
    from demiflow.resources.contracts import ResourceError
    monkeypatch.setenv('DEMIFLOW_RESOURCE_ROOT', str(tmp_path / 'domain'))
    root, capacity = local_capacity()
    assert root == tmp_path / 'domain'
    pools = []
    for index in range(2):
        with RunContext.start(tmp_path / str(index)) as run:
            policy, _ = resolve_runtime_policy({}, run.limits, provider='local')
            class Action:
                context = run
            pools.append(_driver_resources(Action(), policy).provider.pool)
    assert pools[0].root == pools[1].root
    pools[0].reserve('occupied', 'a' * 64, capacity, 1000)
    with pytest.raises(ResourceError) as error:
        pools[1].reserve('other', 'b' * 64, {'cpu_millis': 1, 'memory_bytes': 1}, 1000)
    assert error.value.code == 'CAPACITY_PENDING'
    pools[0].cancel_reservation('occupied')
    assert pools[1].snapshot()['charged'] == {'cpu_millis': 0, 'memory_bytes': 0}


def test_pending_admission_retries_same_allocation_then_executes(tmp_path, monkeypatch):
    from demiflow.execution import dataflow_capacity
    from demiflow.resources.capacity import LocalCapacityPool
    from demiflow.resources.local import LocalResourceProvider
    capacity = {'cpu_millis': 3000, 'memory_bytes': 8 * 1024 ** 3}
    domain = tmp_path / 'capacity-domain'
    monkeypatch.setattr(dataflow_capacity, 'local_capacity', lambda: (domain, capacity))
    pool = LocalCapacityPool(domain / 'capacity', capacity=capacity)
    pool.reserve('other-run', 'c' * 64, {'cpu_millis': 1000, 'memory_bytes': 1}, 1000)
    original = LocalResourceProvider.ensure_present
    pending = []
    observed = []
    def place(provider, allocation_id, spec):
        result = original(provider, allocation_id, spec)
        observed.append((allocation_id, result['phase']))
        if result['phase'] == 'PENDING':
            pending.append(allocation_id)
            if len(pending) == 1:
                pool.cancel_reservation('other-run')
        return result
    monkeypatch.setattr(LocalResourceProvider, 'ensure_present', place)
    with RunContext.start(tmp_path / 'run') as run, data.local_execution():
        assert data.from_items([{'x': 1}]).map(lambda row: {'x': row['x'] + 1}).take_all() == [{'x': 2}]
        assert run.journal.execution_commits('action-1')[-1]['status'] == 'acked'
    assert pending and (pending[0], 'RUNNING') in observed
    assert len({allocation for allocation, _ in observed}) == 3
    assert pool.snapshot()['charged'] == {'cpu_millis': 0, 'memory_bytes': 0}


def test_unavailable_capacity_times_out_without_business_effect(tmp_path, monkeypatch):
    from demiflow.execution import dataflow_capacity
    from demiflow.resources.capacity import LocalCapacityPool
    capacity = {'cpu_millis': 3000, 'memory_bytes': 8 * 1024 ** 3}
    domain = tmp_path / 'capacity-domain'
    monkeypatch.setattr(dataflow_capacity, 'local_capacity', lambda: (domain, capacity))
    pool = LocalCapacityPool(domain / 'capacity', capacity=capacity)
    pool.reserve('other-run', 'd' * 64, capacity, 1000)
    # Shorten only the admission policy; the business assertion is unchanged.
    from demiflow.execution.action_driver import dataflow as driver
    original = driver.make_driver_services
    def services(action, *, policy, bounds):
        return original(action, policy=replace(policy, control_timeout_ms=200), bounds=bounds)
    monkeypatch.setattr(driver, 'make_driver_services', services)
    effect = tmp_path / 'effect'
    def call(row):
        effect.write_text('must never execute')
        return row
    before = time.monotonic()
    with pytest.raises(DataflowError) as error:
        with RunContext.start(tmp_path / 'run') as run, data.local_execution():
            data.from_items([{'x': 1}]).map(call).take_all()
    assert error.value.code == 'CAPACITY_PENDING'
    assert not effect.exists()
    assert time.monotonic() - before < 10
    assert run.journal.execution_commits('action-1') == []
    assert pool.snapshot()['charged'] == capacity
    pool.cancel_reservation('other-run')


def test_long_call_renews_same_worker_and_commits(tmp_path, monkeypatch):
    from demiflow.execution.scheduler import placement as dataflow_scheduler
    from demiflow.execution.dataflow_authority import DriverJournalService
    monkeypatch.setattr(dataflow_scheduler, 'lease_ttl_ms', lambda policy: 4000)
    original = DriverJournalService.renew_worker
    renewals = []
    def renew(service, lease):
        old = service._leases[lease.allocation_id]
        original(service, lease)
        renewals.append((lease.allocation_id, old.expires_at_ms, lease.expires_at_ms))
    monkeypatch.setattr(DriverJournalService, 'renew_worker', renew)
    def slow(row):
        import time
        time.sleep(5)
        return row
    with RunContext.start(tmp_path / 'run') as run, data.local_execution():
        assert data.from_items([{'x': 1}]).map(slow).take_all() == [{'x': 1}]
        assert run.journal.execution_commits('action-1')[-1]['status'] == 'acked'
    assert len({allocation for allocation, _, _ in renewals}) == 3
    assert all(new > old for _, old, new in renewals)


def test_renewal_cannot_change_owner_or_resurrect_expiry(tmp_path, monkeypatch):
    import json
    from types import SimpleNamespace
    from test_runtime_v3_real_seams import prepared
    from demiflow.execution import dataflow_authority as authority
    from demiflow.execution.contracts.dataflow import WorkerLease
    with prepared(tmp_path, data.from_items([{'x': 1}])) as (execution, services):
        now = authority._now_ms()
        monkeypatch.setattr(authority, '_now_ms', lambda: now)
        action = services.action
        stage = execution._physical.stages[0]
        doc = {'run_id': action.context.ref.run_id, 'attempt': action.context.attempt,
               'fence': action.context.fence}
        raw = json.dumps(doc).encode()
        ref = services.store.put(iter([raw]), max_bytes=len(raw), media_type='application/json')
        lease = WorkerLease('allocation', 'boot', stage.stage_id, stage.partitions,
                            'local', 'endpoint', ref, now + 1000)
        client = authority.WorkerJournalClient(transport=SimpleNamespace(store=services.store),
                                               lease=lease, bounds=services.bounds)
        services.journal_service.register_worker(lease, stage.occurrences)
        command = dict(doc, kind='renew_lease', allocation_id='allocation', boot_id='boot',
                       expires_at_ms=now + 2000)
        for change in ({'boot_id': 'other'}, {'fence': 'other'}, {'attempt': 99},
                       {'run_id': 'other'}, {'expires_at_ms': now + 500}):
            with pytest.raises(DataflowError, match='FENCED'):
                client.renew_lease(dict(command, **change))
        client.renew_lease(command)
        services.journal_service.renew_worker(replace(lease, expires_at_ms=now + 2000))
        monkeypatch.setattr(authority, '_now_ms', lambda: now + 2001)
        with pytest.raises(DataflowError, match='FENCED'):
            client.check_lease()
        with pytest.raises(DataflowError, match='FENCED'):
            client.renew_lease(dict(command, expires_at_ms=now + 3000))
        with pytest.raises(DataflowError, match='FENCED'):
            services.journal_service.renew_worker(replace(lease, expires_at_ms=now + 3000))
