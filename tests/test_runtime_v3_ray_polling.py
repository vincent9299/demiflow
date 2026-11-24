"""Mailbox ownership under delayed external Ray transport (unit carrier only)."""
from collections import deque
from types import SimpleNamespace as NS

import pytest

from demiflow.execution.hosts import endpoints as hosts
from demiflow.execution.contracts.dataflow_errors import DataflowError


class Ref:
    def __init__(self, value=None, ready=False, error=None):
        self.value, self.ready, self.error = value, ready, error


@pytest.fixture
def endpoint(monkeypatch):
    calls, waits = [], []
    ref = Ref()
    class ActorError(Exception):
        pass
    def remote(limit):
        calls.append(limit)
        return ref
    actor = NS(pull_events=NS(remote=remote), handle=NS(remote=lambda command, **options: Ref({'receipt':{}}, True)))
    def wait(refs, timeout):
        waits.append(timeout)
        return [r for r in refs if r.ready], [r for r in refs if not r.ready]
    def get(ref):
        assert ref.ready, 'driver blocked in get instead of checking readiness'
        if ref.error:
            raise ref.error
        return ref.value
    monkeypatch.setattr(hosts, 'ray', NS(wait=wait, get=get), raising=False)
    monkeypatch.setattr(hosts, 'RayActorError', ActorError, raising=False)
    monkeypatch.setattr(hosts, 'GetTimeoutError', TimeoutError, raising=False)
    ep = hosts.RayHostEndpoint.__new__(hosts.RayHostEndpoint)
    ep._pending_pull, ep._pull_limit, ep._pull_started = None, 0, 0.0
    ep._events, ep._allocation_id = deque(), 'owned'
    ep._services = NS(bounds=NS(max_pending_events=4), policy=NS(control_timeout_ms=1000),
                      journal_service=NS(poll_worker_replies=lambda **options: ()))
    ep._boot_id = 'boot'
    ep._connector = NS(_actor=lambda allocation: actor)
    ep._k3_envelope = lambda kind, payload: {'kind':kind, 'payload':payload}
    ep._accept_final_observations = lambda result: None
    ep._early_event = ep._events.append
    return NS(endpoint=ep, ref=ref, actor=actor, calls=calls, waits=waits,
              error=ActorError, get=get, wait=wait)


def test_empty_pending_polls_keep_one_read_without_blocking_get(endpoint):
    ep = endpoint.endpoint
    for _ in range(20):
        assert ep.poll(max_events=4, deadline_ms=100) == ()
    assert endpoint.calls == [4]
    assert endpoint.waits == [0.0] * 20
    endpoint.ref.value, endpoint.ref.ready = ('first', 'second'), True
    assert ep.poll(max_events=4, deadline_ms=100) == ('first', 'second')
    assert ep._pending_pull is None and endpoint.calls == [4]


def test_smaller_pull_window_keeps_already_owned_remainder_in_order(endpoint):
    ep = endpoint.endpoint
    assert ep.poll(max_events=4, deadline_ms=100) == ()
    endpoint.ref.value, endpoint.ref.ready = (1, 2, 3, 4), True
    for expected in (1, 2, 3, 4):
        assert ep.poll(max_events=1, deadline_ms=100) == (expected,)
    assert endpoint.calls == [4] and not ep._events


def test_control_completion_consumes_existing_read_without_second_mailbox_owner(endpoint, monkeypatch):
    ep = endpoint.endpoint
    assert ep.poll(max_events=4, deadline_ms=100) == ()
    def wait(refs, timeout):
        if endpoint.ref in refs and timeout > 0:
            endpoint.ref.value, endpoint.ref.ready = ('final-progress',), True
        return endpoint.wait(refs, timeout)
    monkeypatch.setattr(hosts.ray, 'wait', wait)
    assert ep._send_k3('Drain', {}) == {}
    assert ep._cleanup_completed and ep._pending_pull is None
    assert tuple(ep._events) == ('final-progress',) and endpoint.calls == [4]


def test_original_read_deadline_is_not_extended_by_repeated_polls(endpoint, monkeypatch):
    ep = endpoint.endpoint
    assert ep.poll(max_events=4, deadline_ms=100) == ()
    monkeypatch.setattr(hosts.time, 'monotonic', lambda: ep._pull_started + 1.01)
    with pytest.raises(DataflowError, match='UNKNOWN_EFFECT'):
        ep.poll(max_events=4, deadline_ms=100)
    assert endpoint.calls == [4]


@pytest.mark.parametrize('where', ['submit', 'receive'])
def test_actor_loss_never_retries_or_claims_an_empty_mailbox(endpoint, where):
    ep = endpoint.endpoint
    def fail(limit):
        raise endpoint.error('actual carrier loss')
    if where == 'submit':
        endpoint.actor.pull_events.remote = fail
    else:
        endpoint.ref.ready, endpoint.ref.error = True, endpoint.error('actual carrier loss')
    with pytest.raises(DataflowError, match='UNKNOWN_EFFECT'):
        ep.poll(max_events=4, deadline_ms=100)
    assert len(endpoint.calls) <= 1


def test_oversized_mailbox_reply_is_rejected_before_retention(endpoint):
    endpoint.ref.ready, endpoint.ref.value = True, tuple(range(5))
    with pytest.raises(DataflowError, match='requested event credit'):
        endpoint.endpoint.poll(max_events=4, deadline_ms=100)
    assert not endpoint.endpoint._events
