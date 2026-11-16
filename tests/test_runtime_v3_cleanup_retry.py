"""Original resource authority retries completed cleanup I/O, never guesses exit."""
import time
from dataclasses import replace

import pytest

from demiflow.resources.contracts import ResourceError
from test_u2_resources_local import local_env, context, plan_payload, sleep_argv, reconcile


def prepare_cleanup(local_env):
    run, pool, unit, provider, state, manager = local_env
    state.claim_lease_ms = 30000
    manager.bounds = replace(manager.bounds, claim_lease_ms=30000)
    manager.apply(context(run, 'start', 0),
                  plan_payload('scope-1', ['worker'], sleep_argv(30)))
    reconcile(manager, run)
    assert state.read_allocation('scope-1', 'worker')['phase'] == 'RUNNING'
    manager.close(context(run, 'close', 1), {
        'mode': 'ABANDON', 'engine_receipt_ref': 'original-cut',
        'terminate_deadline_ms': 2000, 'reason': 'test_original_cleanup'})
    return run, pool, unit, provider, state, manager


@pytest.mark.parametrize('result', ['unconfirmed', 'provider_error', 'transient_io_error'])
def test_returned_cleanup_retries_without_refunding_unknown_exit(local_env, monkeypatch, result):
    run, pool, unit, provider, state, manager = prepare_cleanup(local_env)
    calls = []
    original = unit.stop

    def stop(*args, **kwargs):
        calls.append(time.monotonic())
        if len(calls) == 1:
            if result == 'provider_error':
                raise ResourceError('CLEANUP_UNCONFIRMED', 'owned child is still draining')
            if result == 'transient_io_error':
                raise OSError('temporary process observation failure')
            return {'confirmed': False, 'evidence_ref': 'owned-child-still-draining'}
        return original(*args, **kwargs)

    monkeypatch.setattr(unit, 'stop', stop)
    reconcile(manager, run)
    assert len(calls) == 1
    assert state.read_allocation('scope-1', 'worker')['cleanup'] == 'UNCONFIRMED'
    assert pool.reservation_view('worker')['state'] == 'BOUND'
    assert pool.snapshot()['charged']['cpu_millis'] == 1000
    assert provider._spawned['worker'].poll() is None
    with run.journal._connection() as db:
        status, until = db.execute(
            "SELECT status,claimed_until_ms FROM resource_intents WHERE kind='ensure_absent'"
        ).fetchone()
    assert status == 'claimed'
    assert until <= int(time.time() * 1000) + 100
    # No force-release or wall-clock jump: the original provider must observe
    # and stop the real unit within a deadline far shorter than its crash lease.
    deadline = time.monotonic() + 3
    while state.read_allocation('scope-1', 'worker')['cleanup'] != 'CONFIRMED':
        assert time.monotonic() < deadline
        manager.reconcile_once(run.fence, deadline, 16)
        time.sleep(.02)
    assert len(calls) == 2
    assert pool.reservation_view('worker')['state'] == 'RELEASED'
    assert pool.snapshot()['charged'] == {'cpu_millis': 0, 'memory_bytes': 0}
    assert not provider._spawned
    with run.journal._connection() as db:
        assert db.execute('SELECT state FROM resource_scopes').fetchone()[0] == 'CLOSED'


def test_cleanup_without_returned_observation_keeps_original_crash_lease(local_env):
    run, pool, unit, provider, state, manager = prepare_cleanup(local_env)
    now = int(time.time() * 1000)
    operations = state.claim_operations(run.fence, now, 16)
    assert len(operations) == 1 and operations[0]['kind'] == 'ensure_absent'
    assert state.claim_operations(run.fence, now + 1000, 16) == []
    assert pool.reservation_view('worker')['state'] == 'BOUND'
    # Complete the same claimed operation through its real owner, not a fake
    # exit record or an altered lease. Confirmed physical exit is still needed.
    entry = manager._reconcile_operation(operations[0])
    state.record_observations(run.fence, [entry])
    assert entry['cleanup'] == 'CONFIRMED'
    assert pool.reservation_view('worker')['state'] == 'RELEASED'
