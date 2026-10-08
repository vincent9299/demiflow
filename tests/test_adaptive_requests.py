import asyncio
import json
import pytest
from demiflow.execution.adaptive_requests import AdaptiveRequestGate, request_admission_policy
from demiflow.execution.request_limits import ServiceStopped


def test_grow_shrink_drain_and_fatal_state(tmp_path,monkeypatch):
    now=[100.]
    monkeypatch.setattr('demiflow.execution.adaptive_requests.time.monotonic',lambda:now[0])
    gate=AdaptiveRequestGate(dict(initial_concurrency=2,max_concurrency=4,increase_step=1,
        window_s=10,min_samples=2,recovery_s=20),state_path=tmp_path/'gate.json')
    gate.begin_action()
    gate.result(success=True,elapsed_s=2);now[0]=110
    gate.result(success=True,elapsed_s=2)
    assert gate.capacity==3
    now[0]=111;gate.result(transient=True,elapsed_s=2)
    assert gate.capacity==1 and gate.consecutive==1
    now[0]=121;gate.result(success=True,elapsed_s=2);gate.result(success=True,elapsed_s=2)
    assert gate.capacity==1
    now[0]=132;gate.result(success=True,elapsed_s=2);gate.result(success=True,elapsed_s=2)
    assert gate.capacity==2
    assert json.loads((tmp_path/'gate.json').read_text())['capacity']==2
    assert len((tmp_path/'gate.events.jsonl').read_text().splitlines())==3
    with pytest.raises(ServiceStopped):gate.result(fatal='quota exhausted')
    assert json.loads((tmp_path/'gate.json').read_text())['stopped']=='quota exhausted'


def test_latency_bound_and_no_growth_without_elapsed():
    gate=AdaptiveRequestGate(dict(initial_concurrency=4,min_samples=2,window_s=.001,latency_p95_s=10))
    gate.window_started-=1
    gate.result(success=True)
    assert gate.fresh_responses==0
    gate.result(success=True,elapsed_s=15);gate.result(success=True,elapsed_s=15)
    assert gate.capacity==2 and gate.last_reason=='latency_p95'


def test_live_work_is_not_cancelled_on_shrink_and_waiters_remain_bounded():
    async def run():
        gate=AdaptiveRequestGate(dict(initial_concurrency=2,max_concurrency=4))
        release=asyncio.Event();entered=[]
        async def work(i):
            async with gate.enter():
                entered.append(i);await release.wait()
        a,b=asyncio.create_task(work(1)),asyncio.create_task(work(2))
        await asyncio.sleep(.01)
        gate.result(transient=True)
        c=asyncio.create_task(work(3));await asyncio.sleep(.01)
        assert gate.capacity==1 and gate.active==2 and entered==[1,2]
        c.cancel()
        with pytest.raises(asyncio.CancelledError):await c
        release.set();await asyncio.gather(a,b)
        assert gate.active==0 and gate.peak==2
        await work(4)
        assert entered==[1,2,4] and gate.active==0
    asyncio.run(run())


@pytest.mark.parametrize('policy',[{'initial_concurrency':True},{'max_concurrency':-1},
    {'min_concurrency':5},{'increase_step':1.5},{'window_s':float('inf')},
    {'coalesce_inflight_failures':1},{'coalesce_inflight_failures':'true'}])
def test_invalid_policy(policy):
    with pytest.raises(ValueError):request_admission_policy(policy)


def test_old_inflight_failures_do_not_repeat_shrink_or_delay_recovery(tmp_path,monkeypatch):
    now=[100.]
    monkeypatch.setattr('demiflow.execution.adaptive_requests.time.monotonic',lambda:now[0])
    gate=AdaptiveRequestGate(dict(initial_concurrency=32,max_concurrency=40,
        recovery_s=20,coalesce_inflight_failures=True),state_path=tmp_path/'gate.json')
    now[0]=101;gate.result(transient=True,elapsed_s=10)
    assert gate.capacity==16 and gate.recover_at==121
    now[0]=102;gate.result(transient=True,elapsed_s=10)
    now[0]=103;gate.result(success=True,elapsed_s=10)
    now[0]=104;gate.result(transient=True,elapsed_s=12)
    assert gate.capacity==16 and gate.recover_at==121 and gate.latencies==[10]
    assert gate.coalesced_transient_failures==2 and gate.adjustments==1
    # A newly dispatched request failing under the lower limit is new pressure.
    now[0]=110;gate.result(transient=True,elapsed_s=2)
    assert gate.capacity==8 and gate.recover_at==130 and gate.adjustments==2
    assert json.loads((tmp_path/'gate.json').read_text())['coalesced_transient_failures']==2
    gate.begin_action()
    assert gate.capacity==32 and gate.last_decrease_at is None and gate.coalesced_transient_failures==0


def test_coalescing_preserves_fatal_and_consecutive_failure_protection(monkeypatch):
    now=[100.]
    monkeypatch.setattr('demiflow.execution.adaptive_requests.time.monotonic',lambda:now[0])
    gate=AdaptiveRequestGate(dict(initial_concurrency=32,max_concurrency=40,
        coalesce_inflight_failures=True),failures=3)
    gate.result(transient=True,elapsed_s=10)
    now[0]=101;gate.result(transient=True,elapsed_s=10)
    now[0]=102
    with pytest.raises(ServiceStopped,match='service_consecutive_failure_limit'):
        gate.result(transient=True,elapsed_s=10)
    assert gate.capacity==16 and gate.consecutive==3
    gate.begin_action()
    with pytest.raises(ServiceStopped,match='quota exhausted'):
        gate.result(fatal='quota exhausted')


def test_default_and_untimed_failures_keep_existing_decrease_behavior(monkeypatch):
    now=[100.]
    monkeypatch.setattr('demiflow.execution.adaptive_requests.time.monotonic',lambda:now[0])
    default=AdaptiveRequestGate(dict(initial_concurrency=32,max_concurrency=40))
    default.result(transient=True,elapsed_s=10)
    now[0]=101;default.result(transient=True,elapsed_s=10)
    assert default.capacity==8 and default.coalesced_transient_failures==0
    enabled=AdaptiveRequestGate(dict(initial_concurrency=32,max_concurrency=40,
        coalesce_inflight_failures=True))
    enabled.result(transient=True)
    now[0]=102;enabled.result(transient=True)
    assert enabled.capacity==8 and enabled.coalesced_transient_failures==0


def test_latency_decrease_also_covers_old_inflight_failure(monkeypatch):
    now=[100.]
    monkeypatch.setattr('demiflow.execution.adaptive_requests.time.monotonic',lambda:now[0])
    gate=AdaptiveRequestGate(dict(initial_concurrency=8,max_concurrency=16,
        min_samples=2,window_s=10,latency_p95_s=5,coalesce_inflight_failures=True))
    gate.result(success=True,elapsed_s=10)
    now[0]=110;gate.result(success=True,elapsed_s=10)
    assert gate.capacity==4 and gate.last_reason=='latency_p95'
    now[0]=111;gate.result(transient=True,elapsed_s=15)
    assert gate.capacity==4 and gate.coalesced_transient_failures==1


def test_failure_window_ignores_isolated_noise_but_decreases_on_sustained_errors(monkeypatch):
    now=[100.]
    monkeypatch.setattr('demiflow.execution.adaptive_requests.time.monotonic',lambda:now[0])
    gate=AdaptiveRequestGate(dict(initial_concurrency=32,max_concurrency=40,
        transient_min_failures=3,transient_failure_ratio=.1,transient_window_s=60,
        coalesce_inflight_failures=True))
    for _ in range(20):gate.result(success=True,elapsed_s=2)
    original_window=gate.window_started
    now[0]=101;gate.result(transient=True,elapsed_s=2)
    now[0]=102;gate.result(transient=True,elapsed_s=2)
    assert gate.capacity==32 and gate.window_started==original_window
    assert gate.deferred_transient_failures==2
    now[0]=103;gate.result(transient=True,elapsed_s=2)
    assert gate.capacity==16 and gate.adjustments==1
    assert not gate.transient_observations
    recovery=gate.recover_at
    now[0]=104;gate.result(transient=True,elapsed_s=10)
    assert gate.capacity==16 and gate.recover_at==recovery
    assert gate.coalesced_transient_failures==1 and not gate.transient_observations


def test_failure_window_checks_ratio_and_expires_old_events(monkeypatch):
    now=[100.]
    monkeypatch.setattr('demiflow.execution.adaptive_requests.time.monotonic',lambda:now[0])
    gate=AdaptiveRequestGate(dict(initial_concurrency=16,transient_min_failures=3,
        transient_failure_ratio=.5,transient_window_s=10),failures=20)
    for _ in range(20):gate.result(success=True,elapsed_s=2)
    for _ in range(3):gate.result(transient=True,elapsed_s=2)
    assert gate.capacity==16  # Three failures alone do not prove a high rate.
    now[0]=111;gate.result(transient=True,elapsed_s=1)
    assert len(gate.transient_observations)==1 and gate.capacity==16
    gate.result(transient=True,elapsed_s=1)
    gate.result(transient=True,elapsed_s=1)
    assert gate.capacity==8 and gate.adjustments==1


def test_failure_window_has_fixed_memory_and_does_not_disable_hard_stop(monkeypatch):
    now=[100.]
    monkeypatch.setattr('demiflow.execution.adaptive_requests.time.monotonic',lambda:now[0])
    gate=AdaptiveRequestGate(dict(initial_concurrency=16,transient_min_failures=5,
        transient_failure_ratio=.5),failures=3)
    for _ in range(2000):gate.result(success=True,elapsed_s=2)
    assert len(gate.transient_observations)==256
    gate.result(transient=True,elapsed_s=1)
    gate.result(transient=True,elapsed_s=1)
    with pytest.raises(ServiceStopped,match='service_consecutive_failure_limit'):
        gate.result(transient=True,elapsed_s=1)
    assert gate.capacity==16 and gate.deferred_transient_failures==3
    gate.begin_action()
    assert not gate.transient_observations and gate.deferred_transient_failures==0
    with pytest.raises(ServiceStopped,match='authentication'):
        gate.result(fatal='authentication')


@pytest.mark.parametrize('policy',[
    {'transient_min_failures':257},{'transient_min_failures':0},
    {'transient_min_failures':True},{'transient_min_failures':1.5},
    {'transient_failure_ratio':True},{'transient_failure_ratio':-.1},
    {'transient_failure_ratio':1.1},{'transient_failure_ratio':float('nan')},
    {'transient_window_s':0},{'transient_window_s':float('inf')},
])
def test_invalid_failure_window(policy):
    with pytest.raises(ValueError):request_admission_policy(policy)


def test_explicit_backpressure_bypasses_noise_window_and_preserves_inflight_coalescing(monkeypatch):
    now=[100.]
    monkeypatch.setattr('demiflow.execution.adaptive_requests.time.monotonic',lambda:now[0])
    gate=AdaptiveRequestGate(dict(initial_concurrency=32,max_concurrency=40,
        transient_min_failures=3,transient_failure_ratio=.5,coalesce_inflight_failures=True))
    for _ in range(100):gate.result(success=True,elapsed_s=2)
    gate.result(backpressure=True,elapsed_s=2)
    assert gate.capacity==16 and gate.last_reason=='backpressure' and gate.consecutive==1
    now[0]=101;gate.result(backpressure=True,elapsed_s=10)
    assert gate.capacity==16 and gate.coalesced_transient_failures==1
    now[0]=105;gate.result(backpressure=True,elapsed_s=1)
    assert gate.capacity==8 and gate.adjustments==2
