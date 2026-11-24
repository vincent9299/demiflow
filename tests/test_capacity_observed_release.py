"""Capacity held by units whose driver was killed returns to the next admission.

Real processes and private capacity domains throughout. The ledger releases a
record only on the physical facts of the cleanup contract; everything it cannot
establish keeps its charge, and the original run still settles its own scope.
"""
from concurrent.futures import ProcessPoolExecutor
import json
import multiprocessing
import os
from pathlib import Path
import sqlite3
import subprocess
import sys
import time

import lance
import pytest

from demiflow.execution.journal.run_journal import RunJournal
from demiflow.resources.capacity import OBSERVED_EXIT, LocalCapacityPool, observed_exit_reference
from demiflow.resources.contracts import ResourceError
from demiflow.resources.local import LocalResourceProvider
from demiflow.resources.process_observation import visibility
from demiflow.resources.process_unit import ManagedProcessUnit, process_identity

from test_sdk_capacity_recovery import crash_graph, maintain, pool_for, write_graph
from test_u2_resources_local import sleep_argv, wait_until

pytestmark = pytest.mark.skipif(not sys.platform.startswith('linux'), reason='Local managed units require Linux')

SPEC = 'a' * 64
EFFECTIVE = {'cpu_millis': 1000, 'memory_bytes': 1 << 20}
GONE_DRIVER = {'pid': 2147480000, 'start': 'never-started'}


def _observer(pool_root, capacity, directory):
    """Another run's provider on the same ledger, with exactly `capacity` to admit against."""
    return LocalResourceProvider(LocalCapacityPool(pool_root, capacity=capacity), ManagedProcessUnit(directory))


def _admit(provider, name='next'):
    return provider._reserve(name, 'b' * 64, EFFECTIVE, 1000, provider.unit.directory('other-scope', name))


def _lost_driver_run(tmp_path, monkeypatch, mode='process'):
    domain = tmp_path / 'domain'
    monkeypatch.setenv('DEMIFLOW_RESOURCE_ROOT', str(domain))
    crash_graph(tmp_path, mode)
    pool = pool_for(domain)
    leaked = pool.live_records()
    assert leaked and all(record['state'] == 'BOUND' and record['origin']['owner'] for record in leaked)
    return domain, pool, leaked


@pytest.mark.parametrize('lock', ['kept', 'removed_after_release'])
def test_killed_driver_capacity_admits_the_next_run_and_the_original_run_still_settles(tmp_path, monkeypatch, lock):
    domain, pool, leaked = _lost_driver_run(tmp_path, monkeypatch)
    charged = pool.snapshot()['charged']
    assert charged['cpu_millis'] == sum(record['effective']['cpu_millis'] for record in leaked)
    # The next run sees a domain filled exactly by the lost driver's records.
    observer = _observer(domain / 'capacity', charged, tmp_path / 'other-units')
    _admitted_after_every_unit_exited(observer)
    for record in leaked:
        released = pool.record(record['allocation_id'])
        assert released['state'] == 'RELEASED' and released['identity'] == record['identity']
        assert released['release_evidence'] == observed_exit_reference(pool.root, released)
        assert released['release_evidence'].startswith(OBSERVED_EXIT)
        assert not (pool.root / 'allocations' / (record['allocation_id'] + '.json')).exists()
        if lock == 'removed_after_release':
            (Path(record['origin']['directory']) / 'unit.lock').unlink()
    before = RunJournal(tmp_path / 'run').execution_commits('action-1')
    history = {record['allocation_id']: json.dumps(pool.record(record['allocation_id']), sort_keys=True)
               for record in leaked}
    # The original run resumes: it settles its old scope from the ledger's release.
    assert write_graph(tmp_path, 'process').status == 'committed'
    assert lance.dataset(str(tmp_path / 'out.lance')).to_table()['value'].to_pylist() == [2, None, 2]
    after = RunJournal(tmp_path / 'run').execution_commits('action-1')
    assert after[0]['commit_id'] == before[0]['commit_id'] and after[0]['status'] == 'acked'
    with sqlite3.connect(tmp_path / 'run/run.sqlite') as db:
        assert set(db.execute('SELECT state FROM resource_scopes')) == {('CLOSED',)}
        assert set(db.execute('SELECT cleanup FROM resource_allocations')) == {('CONFIRMED',)}
    assert history == {key: json.dumps(pool.record(key), sort_keys=True) for key in history}   # refunded once
    assert [view['allocation_id'] for view in pool.snapshot()['active']] == ['next']


def _admit_or_pending(provider):
    try:
        _admit(provider)
    except ResourceError as error:
        assert error.code == 'CAPACITY_PENDING'
        provider._observe_after = 0.0   # this test polls faster than one process normally looks
        return False
    return True


def _admitted_after_every_unit_exited(provider):
    """Admit `next`, then keep observing until each worker of the lost driver has really gone."""
    assert wait_until(lambda: _admit_or_pending(provider), timeout=20)
    def only_next():
        provider._observe_after = 0.0
        provider._release_gone()
        return [view['allocation_id'] for view in provider.pool.snapshot()['active']] == ['next']
    assert wait_until(only_next, timeout=20)


def test_stopped_maintenance_accepts_the_ledger_release_without_the_unit_lock(tmp_path, monkeypatch):
    domain, pool, leaked = _lost_driver_run(tmp_path, monkeypatch)
    observer = _observer(domain / 'capacity', pool.snapshot()['charged'], tmp_path / 'other-units')
    _admitted_after_every_unit_exited(observer)
    for record in leaked:
        (Path(record['origin']['directory']) / 'unit.lock').unlink()
    assert maintain(RunJournal(tmp_path / 'run'))['native_work']
    with sqlite3.connect(tmp_path / 'run/run.sqlite') as db:
        assert set(db.execute('SELECT state FROM resource_scopes')) == {('CLOSED',)}
        evidence = {row[0] for row in db.execute(
            "SELECT json_extract(document,'$.evidence_ref') FROM resource_allocations")}
    assert evidence == {pool.record(record['allocation_id'])['release_evidence'] for record in leaked}


def test_a_release_naming_another_physical_unit_is_refused_by_the_original_run(tmp_path, monkeypatch):
    domain, pool, leaked = _lost_driver_run(tmp_path, monkeypatch)
    observer = _observer(domain / 'capacity', pool.snapshot()['charged'], tmp_path / 'other-units')
    _admitted_after_every_unit_exited(observer)
    record = leaked[0]
    process = Path(record['origin']['directory']) / 'process.json'
    spawned = json.loads(process.read_text())
    spawned['identity']['start'] = 'another-birth'
    process.write_text(json.dumps(spawned))
    with pytest.raises(Exception):
        maintain(RunJournal(tmp_path / 'run'), seconds=1)
    with sqlite3.connect(tmp_path / 'run/run.sqlite') as db:
        assert ('CLOSED',) not in set(db.execute('SELECT state FROM resource_scopes'))


def _keep_observing(root, directory, seconds):
    observer = _observer(Path(root), {'cpu_millis': 1, 'memory_bytes': 1}, Path(directory))
    deadline, released = time.monotonic() + seconds, 0
    while time.monotonic() < deadline:
        observer._observe_after = 0.0
        released += observer._release_gone()
    return released


def test_the_original_run_resumes_while_two_other_runs_observe_its_units(tmp_path, monkeypatch):
    domain, pool, leaked = _lost_driver_run(tmp_path, monkeypatch)
    tree = Path(__file__).resolve().parents[1]
    env = {**os.environ, 'PYTHONPATH': os.pathsep.join([str(tree), str(tree / 'tests')]),
           'PYTHONDONTWRITEBYTECODE': '1'}
    with ProcessPoolExecutor(max_workers=2, mp_context=multiprocessing.get_context('spawn')) as observers:
        watching = [observers.submit(_keep_observing, str(domain / 'capacity'), str(tmp_path / ('other-' + str(i))), 6)
                    for i in range(2)]
        resumed = subprocess.run([sys.executable, '-B', '-c',
            'from test_sdk_capacity_recovery import write_graph; import sys; '
            'print(write_graph(sys.argv[1], "process").status)', str(tmp_path)],
            env=env, capture_output=True, text=True, timeout=60)
        [future.result() for future in watching]
    assert resumed.returncode == 0 and resumed.stdout.strip().endswith('committed'), resumed.stdout + resumed.stderr
    assert lance.dataset(str(tmp_path / 'out.lance')).to_table()['value'].to_pylist() == [2, None, 2]
    with sqlite3.connect(tmp_path / 'run/run.sqlite') as db:
        assert set(db.execute('SELECT state FROM resource_scopes')) == {('CLOSED',)}
        assert set(db.execute('SELECT cleanup FROM resource_allocations')) == {('CONFIRMED',)}
    # Whoever released first, each original record was released exactly once and nothing stays charged.
    for record in leaked:
        released = pool.record(record['allocation_id'])
        assert released['state'] == 'RELEASED' and released['identity'] == record['identity']
    assert pool.snapshot()['charged'] == {'cpu_millis': 0, 'memory_bytes': 0}


def test_a_wait_longer_than_a_second_admits_as_soon_as_capacity_returns(tmp_path, monkeypatch):
    import threading
    from demiflow import data
    from demiflow.execution.hosts import capacity as dataflow_capacity
    from demiflow.execution.pipeline.run_context import RunContext
    capacity = {'cpu_millis': 3000, 'memory_bytes': 8 * 1024 ** 3}
    domain = tmp_path / 'capacity-domain'
    monkeypatch.setattr(dataflow_capacity, 'local_capacity', lambda: (domain, capacity))
    pool = LocalCapacityPool(domain / 'capacity', capacity=capacity)
    pool.reserve('other-run', 'd' * 64, capacity, 1000)
    freed = []
    def free():
        pool.cancel_reservation('other-run')
        freed.append(time.monotonic())
    threading.Timer(1.6, free).start()
    with RunContext.start(tmp_path / 'run'), data.local_execution():
        assert data.from_items([{'x': 1}]).map(lambda row: {'x': row['x'] + 1}).take_all() == [{'x': 2}]
    assert freed and time.monotonic() - freed[0] < 5
    assert pool.snapshot()['charged'] == {'cpu_millis': 0, 'memory_bytes': 0}


class _Unit:
    """One real managed unit bound in a ledger, recorded as created by a driver that is gone."""

    def __init__(self, tmp_path, *, argv=None, origin=None, name='held'):
        self.root = tmp_path / 'ledger'
        self.capacity = dict(EFFECTIVE)
        self.pool = LocalCapacityPool(self.root, capacity=self.capacity)
        self.units = ManagedProcessUnit(tmp_path / 'lost-run-driver-resources' / 'units')
        self.name = name
        self.directory = self.units.directory('scope', name)
        self.origin = {'owner': dict(GONE_DRIVER), **visibility(), 'directory': str(self.directory),
                       'accelerators': False, **(origin or {})}
        self.pool.reserve(name, SPEC, EFFECTIVE, 1000, origin=self.origin)
        self.log = tmp_path / 'unit.log'
        spawned = self.units.spawn('scope', name, SPEC, argv or sleep_argv(60), log_path=self.log)
        self.process, self.identity = spawned['process'], spawned['identity']
        self.pool.bind_identity(name, self.identity)
        self.observer = _observer(self.root, self.capacity, tmp_path / 'other-units')

    def exit(self):
        self.process.kill()
        self.process.wait(timeout=5)

    def state(self):
        return self.pool.record(self.name)['state']

    def rewrite(self, change):
        path = self.root / 'allocations' / (self.name + '.json')
        record = json.loads(path.read_text())
        change(record)
        path.write_text(json.dumps(record))


@pytest.fixture
def unit(tmp_path):
    value = _Unit(tmp_path)
    yield value
    if value.process.poll() is None:
        value.exit()


def test_an_exited_unit_of_a_gone_driver_is_released_with_a_reference_bound_to_its_record(unit):
    unit.exit()
    _admit(unit.observer)
    released = unit.pool.record('held')
    assert released['state'] == 'RELEASED' and released['origin'] == unit.origin
    assert released['release_evidence'] == observed_exit_reference(unit.root, released)
    assert unit.observer.resolve_evidence('scope', 'held', released['release_evidence']) == released
    with pytest.raises(ResourceError) as error:
        unit.observer.resolve_evidence('scope', 'held', OBSERVED_EXIT + '0' * 64)
    assert error.value.code == 'ARTIFACT_MISMATCH'
    # The same record in another ledger root, or with any bound field changed, is another reference.
    assert observed_exit_reference(unit.root.with_name('elsewhere'), released) != released['release_evidence']
    assert observed_exit_reference(unit.root, {**released, 'spec_hash': 'c' * 64}) != released['release_evidence']


def _keeps_charge(unit, holders):
    with pytest.raises(ResourceError) as error:
        _admit(unit.observer)
    assert error.value.code == 'CAPACITY_PENDING' and unit.state() == 'BOUND'
    assert unit.observer._holders == 'capacity_holders ' + holders
    pending = unit.observer._capacity_pending('other-scope', 'next', EFFECTIVE)
    assert pending['conditions'] == ['capacity_pending', unit.observer._holders]


def test_a_unit_still_running_keeps_its_charge(unit):
    _keeps_charge(unit, 'running=0/0m orphaned=1/1000m unknown=0/0m')


def test_a_unit_whose_driver_is_running_is_left_to_that_driver(unit):
    unit.rewrite(lambda record: record['origin'].update(owner=process_identity(os.getpid())))
    unit.exit()
    _keeps_charge(unit, 'running=1/1000m orphaned=0/0m unknown=0/0m')


def test_a_reused_pid_is_not_the_original_driver_but_alone_releases_nothing(unit):
    reused = {'pid': os.getpid(), 'start': 'an-earlier-birth'}
    unit.rewrite(lambda record: record['origin'].update(owner=reused))
    _keeps_charge(unit, 'running=0/0m orphaned=1/1000m unknown=0/0m')   # the unit itself still runs


@pytest.mark.parametrize('damage', ['lock_replaced', 'lock_removed'])
def test_a_unit_whose_lock_cannot_prove_its_exit_keeps_its_charge(unit, damage):
    unit.exit()
    lock = unit.directory / 'unit.lock'
    lock.unlink()
    if damage == 'lock_replaced':
        lock.write_bytes(b'')   # a free lock at the same path, on another inode
    _keeps_charge(unit, 'running=0/0m orphaned=1/1000m unknown=0/0m')


@pytest.mark.parametrize('field,value', [('boot_id', 'another-boot'), ('pid_namespace', 'pid:[1]')])
def test_a_unit_outside_the_observers_boot_or_pid_namespace_keeps_its_charge(unit, field, value):
    unit.exit()
    unit.rewrite(lambda record: record['origin'].update({field: value}))
    _keeps_charge(unit, 'running=0/0m orphaned=0/0m unknown=1/1000m')


def test_device_work_keeps_its_charge(unit):
    unit.exit()
    unit.rewrite(lambda record: record['origin'].update(accelerators=True))
    _keeps_charge(unit, 'running=0/0m orphaned=1/1000m unknown=0/0m')


def test_a_record_without_an_origin_keeps_its_charge(unit):
    unit.exit()
    unit.rewrite(lambda record: record.pop('origin'))
    _keeps_charge(unit, 'running=0/0m orphaned=0/0m unknown=1/1000m')


def test_a_thread_unit_keeps_its_charge(unit):
    unit.exit()
    unit.rewrite(lambda record: record['identity'].update(unit='thread-held'))
    _keeps_charge(unit, 'running=0/0m orphaned=1/1000m unknown=0/0m')


def test_an_unbound_reservation_keeps_its_charge(tmp_path):
    pool = LocalCapacityPool(tmp_path / 'ledger', capacity=EFFECTIVE)
    pool.reserve('reserved', SPEC, EFFECTIVE, 1000, origin={
        'owner': dict(GONE_DRIVER), **visibility(), 'directory': str(tmp_path / 'never'), 'accelerators': False})
    observer = _observer(tmp_path / 'ledger', EFFECTIVE, tmp_path / 'other-units')
    with pytest.raises(ResourceError):
        _admit(observer)
    assert pool.record('reserved')['state'] == 'RESERVED'


def test_work_started_through_the_isolation_primitive_keeps_the_unit_running(tmp_path):
    # The unit's process hands its work to run_isolated and is then killed; the
    # isolated child inherited the unit lock and is still doing that work.
    script = ('import os, sys, time\n'
              'from demiflow.execution.hosts.isolation import run_isolated\n'
              'def work(marker):\n'
              '    open(marker + ".started", "w").close()\n'
              '    while not os.path.exists(marker + ".finish"): time.sleep(0.02)\n'
              'run_isolated(work, sys.argv[1])\n')
    marker = str(tmp_path / 'work')
    unit = _Unit(tmp_path, argv=(sys.executable, '-B', '-c', script, marker))
    try:
        assert wait_until(lambda: os.path.exists(marker + '.started'), timeout=20), unit.log.read_text()
        unit.exit()
        _keeps_charge(unit, 'running=0/0m orphaned=1/1000m unknown=0/0m')
        open(marker + '.finish', 'w').close()
        assert wait_until(lambda: _admit_or_pending(unit.observer))
        assert unit.state() == 'RELEASED'
    finally:
        open(marker + '.finish', 'w').close()


def _observe(root, directory, name):
    observer = _observer(Path(root), EFFECTIVE, Path(directory))
    try:
        _admit(observer, name)
        return 'admitted'
    except ResourceError as error:
        return error.code


def test_concurrent_observers_release_once_and_admit_within_capacity(unit, tmp_path):
    unit.exit()
    with ProcessPoolExecutor(max_workers=4, mp_context=multiprocessing.get_context('spawn')) as workers:
        results = list(workers.map(_observe, [str(unit.root)] * 4, [str(tmp_path / 'other-units')] * 4,
                                   ['next-' + str(index) for index in range(4)]))
    assert results.count('admitted') == 1 and results.count('CAPACITY_PENDING') == 3
    released = unit.pool.record('held')
    assert released['state'] == 'RELEASED'
    assert released['release_evidence'] == observed_exit_reference(unit.root, released)
    assert unit.pool.snapshot()['charged'] == {'cpu_millis': 1000, 'memory_bytes': 1 << 20}


@pytest.mark.parametrize('first', ['ledger', 'owner'])
def test_the_first_durable_release_stands_for_the_owner_and_for_the_ledger(unit, first):
    unit.exit()
    observed = {'confirmed': True, 'identity': unit.identity,
                'evidence_ref': observed_exit_reference(unit.root, unit.pool.record('held'))}
    owner = {'confirmed': True, 'identity': unit.identity, 'evidence_ref': 'owner-cleanup-evidence'}
    order = [observed, owner] if first == 'ledger' else [owner, observed]
    assert unit.pool.release('held', order[0]) == {
        'allocation_id': 'held', 'state': 'RELEASED', 'idempotent': False, 'release_evidence': order[0]['evidence_ref']}
    stored = (unit.root / 'allocations' / 'held.json').read_bytes()
    assert unit.pool.release('held', order[1]) == {
        'allocation_id': 'held', 'state': 'RELEASED', 'idempotent': True, 'release_evidence': order[0]['evidence_ref']}
    assert (unit.root / 'allocations' / 'held.json').read_bytes() == stored
    for evidence in ({**order[1], 'identity': {**unit.identity, 'start': 'another-birth'}},
                     {**order[1], 'confirmed': False}):
        with pytest.raises(ResourceError) as error:
            unit.pool.release('held', evidence)
        assert error.value.code == 'CLEANUP_UNCONFIRMED'


def test_an_admission_timeout_says_who_holds_the_domain(tmp_path, monkeypatch):
    from dataclasses import replace
    from demiflow import data
    from demiflow.execution.action_driver import dataflow as driver
    from demiflow.execution.contracts.dataflow_errors import DataflowError
    from demiflow.execution.hosts import capacity as dataflow_capacity
    from demiflow.execution.pipeline.run_context import RunContext
    capacity = {'cpu_millis': 3000, 'memory_bytes': 8 * 1024 ** 3}
    domain = tmp_path / 'capacity-domain'
    monkeypatch.setattr(dataflow_capacity, 'local_capacity', lambda: (domain, capacity))
    pool = LocalCapacityPool(domain / 'capacity', capacity=capacity)
    pool.reserve('other-run', 'd' * 64, capacity, 1000, origin={
        'owner': process_identity(os.getpid()), **visibility(), 'directory': str(tmp_path / 'other'),
        'accelerators': False})
    original = driver.make_driver_services
    monkeypatch.setattr(driver, 'make_driver_services', lambda action, *, policy, bounds: original(
        action, policy=replace(policy, control_timeout_ms=300), bounds=bounds))
    with pytest.raises(DataflowError) as error:
        with RunContext.start(tmp_path / 'run'), data.local_execution():
            data.from_items([{'x': 1}]).map(lambda row: row).take_all()
    assert error.value.code == 'CAPACITY_PENDING'
    assert str(error.value).startswith('CAPACITY_PENDING: Worker admission deadline exceeded before business activation: ')
    assert 'capacity_holders running=1/3000m orphaned=0/0m unknown=0/0m' in str(error.value)
    assert 'capacity_pending' in error.value.context['conditions']
