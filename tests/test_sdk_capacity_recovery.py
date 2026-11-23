"""Crash recovery must settle the original physical allocations before replay."""
import json
import fcntl
import os
from pathlib import Path
import sqlite3
import subprocess
import sys
import time

import lance
import pyarrow as pa
import pytest

from demiflow import data
from demiflow.execution.pipeline.run_context import RunContext
from demiflow.execution.recovery_contract import RunRef
from demiflow.execution.run_journal import RunJournal
from demiflow.resources.capacity import LocalCapacityPool
from demiflow.resources.contracts import RESOURCE_PROTOCOL_VERSION, MutationContext, ResourceError, ResourceScope
from demiflow.resources.manager import ResourceManager
from demiflow.resources.local import LocalResourceProvider
from demiflow.resources.process_unit import ManagedProcessUnit
from demiflow.execution.hosts.run_state import RunResourceState
from demiflow.execution.pipeline.run_context import clean_stopped_execution


def write_graph(root, mode, crash=None):
    root = Path(root)
    directory = root / 'run'
    previous = RunJournal(directory).recovery_identity()
    context = (RunContext.resume(RunRef(str(directory), previous['run_id']))
               if previous else RunContext.start(directory))
    if crash:
        from demiflow.execution import commit
        def fault(point, commit_id):
            if point == crash:
                os._exit(37)
        commit.fault_point = fault
    with context, data.local_execution(workers=2, worker_mode=mode):
        return data.from_items([{'value': 2}, {'value': None}, {'value': 2}]).write_lance(
            str(root / 'out.lance'), mode='create',
            schema=pa.schema([('value', pa.int64())]), return_receipt=True)


def crash_graph(root, mode='process', window='after_sink_commit_before_receipt'):
    tree = Path(__file__).resolve().parents[1]
    env = {**os.environ, 'PYTHONPATH': os.pathsep.join([str(tree), str(tree / 'tests')]),
           'OPENBLAS_NUM_THREADS': '1', 'OMP_NUM_THREADS': '1', 'PYTHONDONTWRITEBYTECODE': '1'}
    child = subprocess.run([sys.executable, '-B', '-c',
        'from test_sdk_capacity_recovery import write_graph; import sys; write_graph(*sys.argv[1:])',
        str(root), mode, window], env=env, capture_output=True, text=True, timeout=45)
    assert child.returncode == 37, child.stdout + child.stderr


def pool_for(domain):
    from demiflow.execution.hosts.capacity import local_capacity
    _, capacity = local_capacity()
    return LocalCapacityPool(domain / 'capacity', capacity=capacity)


@pytest.mark.parametrize('mode', ['process', 'thread'])
@pytest.mark.parametrize('window', ['after_prepared', 'after_sink_commit_before_receipt'])
def test_crash_then_receipt_replay_returns_original_capacity(tmp_path, monkeypatch, mode, window):
    domain = tmp_path / 'domain'
    monkeypatch.setenv('DEMIFLOW_RESOURCE_ROOT', str(domain))
    crash_graph(tmp_path, mode, window)
    pool = pool_for(domain)
    assert pool.snapshot()['charged']['cpu_millis'] > 0
    before = RunJournal(tmp_path / 'run').execution_commits('action-1')
    receipt = write_graph(tmp_path, mode)
    assert receipt.status == 'committed'
    target = lance.dataset(str(tmp_path / 'out.lance'))
    assert target.version == 1
    assert target.to_table()['value'].to_pylist() == [2, None, 2]
    after = RunJournal(tmp_path / 'run').execution_commits('action-1')
    assert after[0]['commit_id'] == before[0]['commit_id'] and after[0]['status'] == 'acked'
    assert pool.snapshot()['charged'] == {'cpu_millis': 0, 'memory_bytes': 0}
    with sqlite3.connect(tmp_path / 'run/run.sqlite') as db:
        assert set(db.execute('SELECT state FROM resource_scopes')) == {('CLOSED',)}
        assert set(db.execute('SELECT cleanup FROM resource_allocations')) == {('CONFIRMED',)}


@pytest.fixture
def original_unit(tmp_path, monkeypatch):
    """A real managed unit whose original driver left no close intent."""
    from test_u2_resources_local import plan_payload, sleep_argv
    domain = tmp_path / 'domain'
    monkeypatch.setenv('DEMIFLOW_RESOURCE_ROOT', str(domain))
    pool = pool_for(domain)
    unit = ManagedProcessUnit(tmp_path / 'run-driver-resources/units')
    provider = LocalResourceProvider(pool, unit)
    with RunContext.start(tmp_path / 'run', limits={'cancel_grace_s': 1}) as run:
        state = RunResourceState(run.journal)
        manager = ResourceManager(provider, state)
        binding = dict(provider='local', worker_mode='process', capacity_root=str(pool.root))
        state.open_scope(run.fence, ResourceScope('scope-1', run.ref.run_id, binding, run.fence,
            {'cpu_millis': 4000, 'memory_bytes': 4 * 1024 ** 3, 'max_workers': 4}))
        context = MutationContext(RESOURCE_PROTOCOL_VERSION, 'scope-1', run.attempt, run.fence, 'start', 0)
        manager.apply(context, plan_payload('scope-1', ['worker'], sleep_argv(30)))
        manager.reconcile_once(run.fence, time.monotonic() + 5, 1)
        assert state.read_allocation('scope-1', 'worker')['phase'] == 'RUNNING'
    try:
        yield run, pool, unit, provider, state, manager
    finally:
        for child in provider._spawned.values():
            if child.poll() is None:
                child.kill()
            child.wait(timeout=5)


def maintain(journal, seconds=3):
    status = journal.execution_status()
    return clean_stopped_execution(journal, expected_attempt=status['attempt'],
        expected_revision=status['revision'], confirm_stopped=lambda _: True,
        deadline=time.monotonic() + seconds)


def owner_rows(journal):
    with journal._connection() as db:
        return db.execute('SELECT * FROM runtime_owner').fetchall()


def test_stopped_maintenance_retires_live_original_unit_without_rewriting_business(original_unit):
    run, pool, unit, provider, state, manager = original_unit
    before = run.journal.execution_status()
    assert provider._spawned['worker'].poll() is None
    # This separate run's reservation must not be refunded by original-run cleanup.
    pool.reserve('another-run', 'a' * 64, {'cpu_millis': 1, 'memory_bytes': 1}, 1000)
    try:
        maintain(run.journal)
        assert run.journal.execution_status() == before
        assert not owner_rows(run.journal)
        assert state.cleanup_complete('scope-1')
        assert provider._spawned['worker'].poll() is not None
        assert pool.snapshot()['charged'] == {'cpu_millis': 1, 'memory_bytes': 1}
    finally:
        pool.cancel_reservation('another-run')


def test_old_capacity_domain_is_not_inferred_from_current_environment(original_unit, tmp_path, monkeypatch):
    run, pool, *_ = original_unit
    charged = pool.snapshot()['charged']
    monkeypatch.setenv('DEMIFLOW_RESOURCE_ROOT', str(tmp_path / 'other'))
    with pytest.raises(ResourceError, match='capacity domain differs'):
        maintain(run.journal)
    assert pool.snapshot()['charged'] == charged and owner_rows(run.journal)


@pytest.mark.parametrize('damage', ['identity', 'spec', 'missing', 'ambiguous'])
def test_unproven_original_evidence_keeps_charge_and_owner(original_unit, damage):
    run, pool, unit, provider, *_ = original_unit
    path = unit.directory('scope-1', 'worker') / 'process.json'
    original = path.read_bytes()
    thread_dir = run.directory.with_name('run-driver-resources') / 'thread-units/scope-1/worker'
    if damage == 'missing':
        path.unlink()
    elif damage == 'ambiguous':
        thread_dir.mkdir(parents=True)
    else:
        value = json.loads(original)
        if damage == 'identity':
            value['identity']['start'] = 'wrong-birth'
        else:
            value['spec_hash'] = 'f' * 64
        path.write_text(json.dumps(value))
    charged = pool.snapshot()['charged']
    try:
        with pytest.raises(ResourceError) as caught:
            maintain(run.journal)
        assert caught.value.code == 'ARTIFACT_MISMATCH'
        assert pool.snapshot()['charged'] == charged and owner_rows(run.journal)
        assert provider._spawned['worker'].poll() is None
    finally:
        path.write_bytes(original)
        if thread_dir.exists():
            thread_dir.rmdir()


def test_legacy_scope_uses_matching_original_provider_evidence(original_unit):
    run, pool, _, _, state, _ = original_unit
    with sqlite3.connect(run.journal.path) as db:
        value = json.loads(db.execute('SELECT document FROM resource_scopes').fetchone()[0])
        value['provider_binding'] = {'provider': 'local'}
        db.execute('UPDATE resource_scopes SET document=?', (json.dumps(value),))
    maintain(run.journal)
    assert state.cleanup_complete('scope-1')
    assert pool.snapshot()['charged'] == {'cpu_millis': 0, 'memory_bytes': 0}


def test_released_unit_observation_can_be_recovered_without_second_refund(original_unit, monkeypatch):
    run, pool, _, provider, state, _ = original_unit
    result = provider.ensure_absent({'scope_id': 'scope-1', 'allocation_id': 'worker'}, 1000)
    assert result['cleanup'] == 'CONFIRMED'
    original = pool.reservation_view('worker')
    def no_second_release(*args, **kwargs):
        raise AssertionError('already refunded capacity must not be refunded again')
    monkeypatch.setattr(LocalCapacityPool, 'release', no_second_release)
    maintain(run.journal)
    assert state.cleanup_complete('scope-1')
    assert pool.reservation_view('worker') == original


def test_capacity_flock_obeys_one_cleanup_deadline(original_unit):
    run, pool, *_ = original_unit
    fd = os.open(pool.root / 'capacity.lock', os.O_RDWR)
    fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
    started = time.monotonic()
    try:
        with pytest.raises(ResourceError, match='deadline'):
            maintain(run.journal, seconds=.2)
        assert time.monotonic() - started < 2
        assert owner_rows(run.journal)
    finally:
        os.close(fd)
    assert pool.snapshot()['charged']['cpu_millis'] == 1000


def test_original_unit_lock_must_confirm_exit(original_unit):
    run, pool, unit, provider, *_ = original_unit
    child = provider._spawned['worker']
    child.kill(); child.wait(timeout=5)
    fd = os.open(unit.directory('scope-1', 'worker') / 'unit.lock', os.O_RDWR)
    fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
    try:
        with pytest.raises(ResourceError, match='cleanup deadline'):
            maintain(run.journal)
        assert pool.snapshot()['charged']['cpu_millis'] == 1000
    finally:
        os.close(fd)
    maintain(run.journal)
    assert pool.snapshot()['charged']['cpu_millis'] == 0


def test_resource_exit_precedes_gc_but_publication_guard_still_checks_gc(original_unit, monkeypatch):
    run, pool, _, _, state, _ = original_unit
    original = RunJournal._reconcile_published_gc
    calls = []
    def gc_after_exit(journal, fence):
        assert state.cleanup_complete('scope-1'), 'GC was attempted before physical resource retirement'
        assert pool.snapshot()['charged']['cpu_millis'] == 0
        calls.append(fence)
        return original(journal, fence)
    monkeypatch.setattr(RunJournal, '_reconcile_published_gc', gc_after_exit)
    maintain(run.journal)
    assert len(calls) >= 2  # Claim and the normal release/publication guard.


def test_cleanup_state_rejects_admission_and_cross_scope_dispatch(original_unit):
    run, _, _, provider, *_ = original_unit
    state = RunResourceState(run.journal, cleanup_scope='scope-1')
    manager = ResourceManager(provider, state)
    with pytest.raises(ResourceError, match='cannot open'):
        state.open_scope(run.fence, None)
    with pytest.raises(ResourceError, match='cannot admit'):
        manager.apply(None, None)
    with pytest.raises(ResourceError, match='cannot admit'):
        state.accept_plan(None, None)
    for operation in [dict(kind='ensure_present', scope_id='scope-1'),
                      dict(kind='ensure_absent', scope_id='another-scope')]:
        with pytest.raises(ResourceError, match='cannot dispatch'):
            manager._reconcile_operation(operation)
    with pytest.raises(ResourceError, match='admission observation'):
        state.record_observations(run.fence, [dict(allocation_id='worker', phase='RUNNING')])


def leave_claimed_cleanup(original_unit):
    run, _, _, _, state, manager = original_unit
    configuration = run.journal.recovery_identity()
    run.journal.claim_execution(configuration, 'setup')
    context = MutationContext(RESOURCE_PROTOCOL_VERSION, 'scope-1', 1, 'setup', 'close', 1)
    manager.close(context, dict(mode='ABANDON', engine_receipt_ref='original-close',
        terminate_deadline_ms=1000, reason='original_driver_stopped'))
    operations = state.claim_operations('setup', int(time.time() * 1000), 1)
    assert len(operations) == 1 and operations[0]['kind'] == 'ensure_absent'
    run.journal.release_execution('setup')


def test_resumed_epoch_reclaims_original_cleanup_without_waiting_crash_lease(original_unit):
    run, pool, _, _, state, _ = original_unit
    leave_claimed_cleanup(original_unit)
    run.journal.transition({'succeeded'}, 'interrupted')
    before = time.monotonic()
    with RunContext.resume(run.ref) as resumed:
        assert resumed.attempt == 2
        assert state.cleanup_complete('scope-1')
        assert pool.snapshot()['charged']['cpu_millis'] == 0
    assert time.monotonic() - before < 3  # Original claim lease is 30 seconds.


def test_current_epoch_claim_is_not_stolen_by_maintenance(original_unit):
    run, pool, *_ = original_unit
    leave_claimed_cleanup(original_unit)
    with pytest.raises(ResourceError, match='cleanup deadline'):
        maintain(run.journal)
    assert pool.snapshot()['charged']['cpu_millis'] == 1000
    assert owner_rows(run.journal)


def test_cleanup_claims_only_its_scope_and_never_ensure_present(original_unit):
    from test_u2_resources_local import plan_payload, sleep_argv
    run, _, _, provider, state, manager = original_unit
    run.journal.claim_execution(run.journal.recovery_identity(), 'setup')
    state.open_scope('setup', ResourceScope('scope-2', run.ref.run_id, {'provider': 'local'}, 'setup',
        {'cpu_millis': 4000, 'memory_bytes': 4 * 1024 ** 3, 'max_workers': 4}))
    manager.apply(MutationContext(RESOURCE_PROTOCOL_VERSION, 'scope-2', 1, 'setup', 'other', 0),
                  plan_payload('scope-2', ['other'], sleep_argv(30)))
    manager.close(MutationContext(RESOURCE_PROTOCOL_VERSION, 'scope-1', 1, 'setup', 'close', 1),
        dict(mode='ABANDON', engine_receipt_ref='original-close', terminate_deadline_ms=1000, reason='test'))
    cleanup = RunResourceState(run.journal, cleanup_scope='scope-1')
    operations = cleanup.claim_operations('setup', int(time.time() * 1000), 64)
    assert [(op['scope_id'], op['kind']) for op in operations] == [('scope-1', 'ensure_absent')]
    result = ResourceManager(provider, cleanup)._reconcile_operation(operations[0])
    cleanup.record_observations('setup', [result])
    assert cleanup.cleanup_complete('scope-1')
    with run.journal._connection() as db:
        assert db.execute("SELECT status FROM resource_intents WHERE scope_id='scope-2'").fetchall() == [('pending',)]
    run.journal.release_execution('setup')


def test_unrecorded_spawn_cannot_release_an_inherited_busy_unit_lock(original_unit):
    run, pool, unit, provider, *_ = original_unit
    process_path = unit.directory('scope-1', 'worker') / 'process.json'
    capacity_path = pool.root / 'allocations/worker.json'
    process_bytes, capacity_bytes = process_path.read_bytes(), capacity_path.read_bytes()
    unbound = json.loads(capacity_bytes)
    unbound.update(state='RESERVED', identity=None)
    capacity_path.write_text(json.dumps(unbound))
    process_path.unlink()  # Crash window after fork but before durable PID binding.
    try:
        with pytest.raises(ResourceError, match='cleanup deadline'):
            maintain(run.journal)
        assert provider._spawned['worker'].poll() is None
        assert pool.snapshot()['charged']['cpu_millis'] == 1000
    finally:
        process_path.write_bytes(process_bytes)
        capacity_path.write_bytes(capacity_bytes)


def test_unrecorded_spawn_rejects_replaced_unit_lock_even_if_new_inode_is_free(original_unit):
    from demiflow.execution.hosts.execution_recovery import _OriginalLocalExit
    run, pool, unit, provider, _, _ = original_unit
    directory = unit.directory('scope-1', 'worker')
    process_path = directory / 'process.json'
    lock, backup = directory / 'unit.lock', directory / 'original-unit.lock'
    capacity_path = pool.root / 'allocations/worker.json'
    process_bytes, capacity_bytes = process_path.read_bytes(), capacity_path.read_bytes()
    unbound = json.loads(capacity_bytes)
    unbound.update(state='RESERVED', identity=None)
    capacity_path.write_text(json.dumps(unbound))
    process_path.unlink()
    lock.rename(backup)  # The live child still holds the original inode.
    lock.touch()        # A free lock at the old path proves nothing about it.
    state = RunResourceState(run.journal, cleanup_scope='scope-1')
    binding = dict(provider='local', worker_mode='process', capacity_root=str(pool.root))
    recovery = _OriginalLocalExit(run.journal, state, binding, time.monotonic() + 1)
    try:
        with pytest.raises(ResourceError) as caught:
            recovery.ensure_absent({'scope_id': 'scope-1', 'allocation_id': 'worker'}, 1000)
        assert caught.value.code == 'CLEANUP_UNCONFIRMED'
        assert provider._spawned['worker'].poll() is None
        assert pool.snapshot()['charged']['cpu_millis'] == 1000
    finally:
        lock.unlink()
        backup.rename(lock)
        process_path.write_bytes(process_bytes)
        capacity_path.write_bytes(capacity_bytes)


@pytest.mark.parametrize('phase', ['REQUESTED', 'RUNNING', 'RELEASED'])
def test_unbound_reservation_without_unit_requires_never_spawned_journal_facts(tmp_path, monkeypatch, phase):
    from test_u2_resources_local import plan_payload, sleep_argv
    domain = tmp_path / 'domain'
    monkeypatch.setenv('DEMIFLOW_RESOURCE_ROOT', str(domain))
    pool = pool_for(domain)
    unit = ManagedProcessUnit(tmp_path / 'run-driver-resources/units')
    provider = LocalResourceProvider(pool, unit)
    with RunContext.start(tmp_path / 'run', limits={'cancel_grace_s': 1}) as run:
        state = RunResourceState(run.journal)
        binding = dict(provider='local', worker_mode='process', capacity_root=str(pool.root))
        state.open_scope(run.fence, ResourceScope('scope-1', run.ref.run_id, binding, run.fence,
            {'cpu_millis': 4000, 'memory_bytes': 4 * 1024 ** 3, 'max_workers': 4}))
        manager = ResourceManager(provider, state)
        manager.apply(
            MutationContext(RESOURCE_PROTOCOL_VERSION, 'scope-1', run.attempt, run.fence, 'start', 0),
            plan_payload('scope-1', ['worker'], sleep_argv(30)))
        row = state.read_cleanup_allocation('scope-1', 'worker')
        pool.reserve('worker', row['spec_hash'], row['effective'], 1000)
    assert not unit.directory('scope-1', 'worker').exists()
    assert pool.reservation_view('worker')['identity'] is None
    if phase == 'RUNNING':
        # Missing physical evidence contradicts a previously observed running
        # unit, even when the remaining reservation has no bound identity.
        observation = state.read_allocation('scope-1', 'worker')
        observation['phase'] = phase
        with sqlite3.connect(run.journal.path) as db:
            db.execute('UPDATE resource_allocations SET phase=?,document=?', (phase, json.dumps(observation)))
        with pytest.raises(ResourceError, match='cleanup deadline'):
            maintain(run.journal)
        assert pool.snapshot()['charged']['cpu_millis'] == 1000
        assert owner_rows(run.journal)
        assert not state.cleanup_complete('scope-1')
    elif phase == 'RELEASED':
        run.journal.claim_execution(run.journal.recovery_identity(), 'setup')
        manager.close(MutationContext(RESOURCE_PROTOCOL_VERSION, 'scope-1', 1, 'setup', 'close', 1),
            dict(mode='ABANDON', engine_receipt_ref='original-close', terminate_deadline_ms=1000, reason='test'))
        def lose_observation(*args, **kwargs):
            raise RuntimeError('crash after release before Journal observation')
        monkeypatch.setattr(state, 'record_observations', lose_observation)
        with pytest.raises(RuntimeError, match='crash after release'):
            manager.reconcile_once('setup', time.monotonic() + 2, 1)
        assert pool.reservation_view('worker')['state'] == 'RELEASED'
        assert state.read_allocation('scope-1', 'worker')['cleanup'] != 'CONFIRMED'
        capacity_path = pool.root / 'allocations/worker.json'
        original_record = capacity_path.read_bytes()
        def reject_second_release(*args, **kwargs):
            raise AssertionError('recovery must not refund an already released allocation')
        monkeypatch.setattr(LocalCapacityPool, 'release', reject_second_release)
        run.journal.release_execution('setup')
        run.journal.transition({'succeeded'}, 'interrupted')
        with RunContext.resume(run.ref):
            assert state.cleanup_complete('scope-1')
        assert capacity_path.read_bytes() == original_record
        assert pool.snapshot()['charged']['cpu_millis'] == 0
    else:
        maintain(run.journal)
        assert pool.snapshot()['charged']['cpu_millis'] == 0
        assert not owner_rows(run.journal)
        assert state.cleanup_complete('scope-1')


def test_full_original_pool_is_freed_before_replay_and_next_normal_run(tmp_path, monkeypatch):
    from demiflow.execution.hosts import capacity as dataflow_capacity
    domain = tmp_path / 'domain'
    monkeypatch.setenv('DEMIFLOW_RESOURCE_ROOT', str(domain))
    crash_graph(tmp_path)
    pool = pool_for(domain)
    charged = pool.snapshot()['charged']
    assert charged['cpu_millis'] == 2000
    # Existing allocations consume the entire explicit CPU ceiling. Recovery
    # needs no spare worker, and subsequent ordinary SDK work can use it again.
    capacity = {'cpu_millis': charged['cpu_millis'], 'memory_bytes': 8 * 1024 ** 3}
    monkeypatch.setattr(dataflow_capacity, 'local_capacity', lambda: (domain, capacity))
    assert write_graph(tmp_path, 'process').status == 'committed'
    assert pool.snapshot()['charged']['cpu_millis'] == 0
    with RunContext.start(tmp_path / 'next'), data.local_execution(workers=2):
        assert data.from_items([{'value': 9}]).take_all() == [{'value': 9}]
    assert pool.snapshot()['charged'] == {'cpu_millis': 0, 'memory_bytes': 0}


def test_legacy_thread_scope_recovers_only_matching_original_evidence(tmp_path, monkeypatch):
    domain = tmp_path / 'domain'
    monkeypatch.setenv('DEMIFLOW_RESOURCE_ROOT', str(domain))
    crash_graph(tmp_path, 'thread')
    with sqlite3.connect(tmp_path / 'run/run.sqlite') as db:
        rows = db.execute('SELECT scope_id,document FROM resource_scopes').fetchall()
        for scope, raw in rows:
            value = json.loads(raw)
            value['provider_binding'] = {'provider': 'local'}
            db.execute('UPDATE resource_scopes SET document=? WHERE scope_id=?', (json.dumps(value), scope))
    assert write_graph(tmp_path, 'thread').status == 'committed'
    assert pool_for(domain).snapshot()['charged'] == {'cpu_millis': 0, 'memory_bytes': 0}


def test_unknown_cleanup_read_retains_owner_and_does_not_mask_original_error(original_unit, monkeypatch):
    run, pool, *_ = original_unit
    from demiflow.execution.hosts import execution_recovery as dataflow_resource_recovery
    def fail(*args, **kwargs):
        raise OSError('original recovery failure')
    def unknown(*args, **kwargs):
        raise OSError('resource fact read unavailable')
    monkeypatch.setattr(dataflow_resource_recovery, 'recover_execution_resources', fail)
    monkeypatch.setattr(RunResourceState, 'unfinished_scopes', unknown)
    with pytest.raises(OSError, match='original recovery failure'):
        maintain(run.journal)
    assert owner_rows(run.journal)
    assert pool.snapshot()['charged']['cpu_millis'] == 1000


@pytest.mark.parametrize('table,column', [
    ('resource_scopes', 'document'), ('resource_scopes', 'close_document'),
    ('resource_allocations', 'spec'), ('resource_allocations', 'effective'),
    ('resource_allocations', 'document'), ('resource_intents', 'document'),
])
def test_cleanup_bounds_journal_bytes_before_json_decode(original_unit, monkeypatch, table, column):
    run, _, _, _, normal, manager = original_unit
    run.journal.claim_execution(run.journal.recovery_identity(), 'setup')
    if table == 'resource_intents':
        manager.close(MutationContext(RESOURCE_PROTOCOL_VERSION, 'scope-1', 1, 'setup', 'close', 1),
            dict(mode='ABANDON', engine_receipt_ref='original-close', terminate_deadline_ms=1000, reason='test'))
    # Multibyte JSON is below the character limit but above the byte limit.
    oversized = json.dumps({'large': '界' * 30000}, ensure_ascii=False)
    with sqlite3.connect(run.journal.path) as db:
        db.execute(f'UPDATE {table} SET {column}=?', (oversized,))
    decode = json.loads
    def bounded_decode(raw, *args, **kwargs):
        assert len(raw if isinstance(raw, bytes) else raw.encode()) <= 65536
        return decode(raw, *args, **kwargs)
    monkeypatch.setattr(json, 'loads', bounded_decode)
    cleanup = RunResourceState(run.journal, cleanup_scope='scope-1')
    with pytest.raises(ResourceError, match='document exceeds byte budget') as caught:
        if table == 'resource_scopes':
            normal.unfinished_scopes(run.ref.run_id)
        elif table == 'resource_allocations':
            cleanup.read_cleanup_allocation('scope-1', 'worker')
        else:
            cleanup.claim_operations('setup', int(time.time() * 1000), 1)
    assert caught.value.code == 'QUOTA_EXCEEDED'


def test_cleanup_scope_close_rolls_back_atomically_after_native_fence(original_unit):
    run, _, _, provider, *_ = original_unit
    run.journal.claim_execution(run.journal.recovery_identity(), 'setup')
    cleanup = RunResourceState(run.journal, cleanup_scope='scope-1')
    with sqlite3.connect(run.journal.path) as db:
        db.execute("CREATE TRIGGER reject_retirement BEFORE INSERT ON resource_intents "
                   "WHEN NEW.kind='ensure_absent' BEGIN SELECT RAISE(ABORT,'retirement fault'); END")
    with pytest.raises(sqlite3.IntegrityError, match='retirement fault'):
        ResourceManager(provider, cleanup).close(
            MutationContext(RESOURCE_PROTOCOL_VERSION, 'scope-1', 1, 'setup', 'close', 1),
            dict(mode='ABANDON', engine_receipt_ref='original-close', terminate_deadline_ms=1000, reason='test'))
    with run.journal._connection() as db:
        assert db.execute('SELECT state FROM resource_scopes').fetchall() == [('OPEN',)]
        assert db.execute('SELECT cleanup FROM resource_allocations').fetchall() == [('NONE',)]
        assert not db.execute("SELECT 1 FROM resource_intents WHERE kind='ensure_absent'").fetchall()


@pytest.mark.parametrize('operation', ['claim', 'observe'])
def test_cleanup_claim_and_observation_keep_fenced_transaction(original_unit, monkeypatch, operation):
    run, _, _, provider, _, manager = original_unit
    run.journal.claim_execution(run.journal.recovery_identity(), 'setup')
    manager.close(MutationContext(RESOURCE_PROTOCOL_VERSION, 'scope-1', 1, 'setup', 'close', 1),
        dict(mode='ABANDON', engine_receipt_ref='original-close', terminate_deadline_ms=1000, reason='test'))
    cleanup = RunResourceState(run.journal, cleanup_scope='scope-1')
    def fail(*args, **kwargs):
        raise RuntimeError('after mutation fault')
    if operation == 'claim':
        monkeypatch.setattr(cleanup, '_advance_observed', fail)
        with pytest.raises(RuntimeError, match='after mutation fault'):
            cleanup.claim_operations('setup', int(time.time() * 1000), 1)
        with run.journal._connection() as db:
            assert db.execute("SELECT status FROM resource_intents WHERE kind='ensure_absent'").fetchall() == [('pending',)]
    else:
        operations = cleanup.claim_operations('setup', int(time.time() * 1000), 1)
        result = ResourceManager(provider, cleanup)._reconcile_operation(operations[0])
        assert result['cleanup'] == 'CONFIRMED'
        monkeypatch.setattr(cleanup, '_emit_event', fail)
        with pytest.raises(RuntimeError, match='after mutation fault'):
            cleanup.record_observations('setup', [result])
        with run.journal._connection() as db:
            assert db.execute('SELECT cleanup FROM resource_allocations').fetchall() == [('REQUESTED',)]
