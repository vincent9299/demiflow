"""Real service threads/journal; readiness is never a control reply authority."""
from contextlib import contextmanager
from dataclasses import replace
import hashlib
import select
import socket
import threading
import time
from types import SimpleNamespace

import pytest

from demiflow.execution.contracts.dataflow import ArtifactRef, WorkerEvent, WorkerLease
from demiflow.execution.dataflow_authority import DriverJournalService
from demiflow.execution.run_context import RunAction, RunContext
from demiflow.planning.dataflow_policy import resolve_runtime_policy


@contextmanager
def service_env(tmp_path):
    _, bounds = resolve_runtime_policy({}, {}, provider='local')
    with RunContext.start(tmp_path / 'run') as run:
        action = RunAction(run, 'action-1', {'kind': 'test'})
        run.journal.bind_execution_action(run.fence, action.action_id, action.declaration)
        service = DriverJournalService(action, store=None, bounds=replace(bounds, max_pending_events=8))
        digest = hashlib.sha256(b'grant').hexdigest()
        service.register_worker(WorkerLease('allocation', 'boot', 'stage', ('p0',), 'host',
            'endpoint', ArtifactRef(digest, 0, 'local', digest, 'application/json'),
            int(time.time() * 1000) + 60000), ())
        try:
            yield SimpleNamespace(run=run, action=action, service=service, bounds=bounds)
        finally:
            service.close(deadline_ms=1000)
            run.journal.complete_execution_action(run.fence, action.action_id, {'done': True})


def event(env, index):
    return WorkerEvent('demiflow.dataflow_event.v3', 'reserve',
        hashlib.sha256(str(index).encode()).hexdigest(), env.run.ref.run_id, env.run.attempt,
        env.run.fence, 'allocation', 'boot', 0, 'scope', env.run.attempt, None,
        {'amounts': {'requests': 1}})


def waitable(service):
    value = getattr(service, 'reply_waitable', lambda: None)()
    assert value is not None, 'completed journal replies must wake the driver selector'
    return value


def wait_dispatched(service):
    with service._queue.all_tasks_done:
        assert service._queue.all_tasks_done.wait_for(
            lambda: service._queue.unfinished_tasks == 0, timeout=2)


def test_real_journal_completion_wakes_after_an_empty_poll(tmp_path):
    with service_env(tmp_path) as env:
        service = env.service
        descriptor = waitable(service)
        assert service.poll_replies(max_replies=8) == ()
        assert not select.select([descriptor], [], [], 0)[0]
        request = event(env, 1)
        assert service.submit(request)
        assert select.select([descriptor], [], [], 2)[0] == [descriptor]
        replies = service.poll_replies(max_replies=8)
        assert len(replies) == 1 and replies[0].reply.operation_id == request.operation_id
        assert replies[0].reply.status == 'ok'
        assert env.run.journal.budget_consumed('requests') == 1
        assert not select.select([descriptor], [], [], 0)[0]


def test_partial_drain_retains_readiness_without_duplicate_results(tmp_path):
    with service_env(tmp_path) as env:
        service = env.service
        descriptor = waitable(service)
        requests = [event(env, index) for index in range(3)]
        for request in requests:
            assert service.submit(request)
        wait_dispatched(service)
        seen = []
        for remaining in (2, 1, 0):
            assert select.select([descriptor], [], [], 0)[0] == [descriptor]
            replies = service.poll_replies(max_replies=1)
            assert len(replies) == 1 and replies[0].reply.status == 'ok'
            seen.append(replies[0].reply.operation_id)
            assert bool(select.select([descriptor], [], [], 0)[0]) == bool(remaining)
        assert set(seen) == {request.operation_id for request in requests} and len(set(seen)) == 3
        assert service.poll_replies(max_replies=8) == ()
        assert env.run.journal.budget_consumed('requests') == 3


def test_append_racing_last_drain_rearms_the_same_waitable(tmp_path, monkeypatch):
    with service_env(tmp_path) as env:
        service = env.service
        descriptor = waitable(service)
        assert service.submit(event(env, 1))
        wait_dispatched(service)
        draining, release, dispatched = threading.Event(), threading.Event(), threading.Event()
        actual_drain, actual_dispatch = service._drain_reply_waitable_locked, service.dispatch

        def gated_drain():
            draining.set()
            assert release.wait(2)
            actual_drain()

        def observed_dispatch(request):
            answer = actual_dispatch(request)
            dispatched.set()
            return answer

        monkeypatch.setattr(service, '_drain_reply_waitable_locked', gated_drain)
        monkeypatch.setattr(service, 'dispatch', observed_dispatch)
        # The second request is admitted before the consumer takes the lock.
        allow_dispatch = threading.Event()
        def gated_dispatch(request):
            assert allow_dispatch.wait(2)
            return observed_dispatch(request)
        monkeypatch.setattr(service, 'dispatch', gated_dispatch)
        assert service.submit(event(env, 2))
        consumed = []
        reader = threading.Thread(target=lambda: consumed.extend(service.poll_replies(max_replies=1)))
        reader.start()
        try:
            assert draining.wait(2)
            allow_dispatch.set()
            assert dispatched.wait(2)  # Append is now contending with the final drain lock.
            release.set()
            reader.join(2)
            assert not reader.is_alive() and len(consumed) == 1
            wait_dispatched(service)
            assert select.select([descriptor], [], [], 0)[0] == [descriptor]
            monkeypatch.setattr(service, '_drain_reply_waitable_locked', actual_drain)
            second = service.poll_replies(max_replies=1)
            assert second[0].reply.operation_id == event(env, 2).operation_id
            assert not select.select([descriptor], [], [], 0)[0]
        finally:
            allow_dispatch.set()
            release.set()
            reader.join(2)


def test_closed_service_discards_late_result_and_closes_only_its_pair(tmp_path, monkeypatch):
    with service_env(tmp_path) as env:
        service = env.service
        sockets = (waitable(service), service._reply_writer)
        finished, release = threading.Event(), threading.Event()
        original = service.dispatch

        def delayed(request):
            reply = original(request)
            finished.set()
            assert release.wait(2)
            return reply

        monkeypatch.setattr(service, 'dispatch', delayed)
        request = event(env, 1)
        assert service.submit(request)
        try:
            assert finished.wait(2)
            service.close(deadline_ms=0)
            assert service.reply_waitable() is None
            assert all(sock.fileno() == -1 for sock in sockets)
            assert not service.physically_stopped()
            replies = service.poll_replies(max_replies=8)
            assert len(replies) == 1 and replies[0].reply.error_code == 'UNKNOWN_EFFECT'
            assert replies[0].reply.operation_id == request.operation_id
            release.set()
            for worker in service._workers:
                worker.join(2)
            assert service.physically_stopped()
            assert service.poll_replies(max_replies=8) == ()
            assert env.run.journal.budget_consumed('requests') == 1
        finally:
            release.set()
            for worker in service._workers:
                worker.join(2)


def test_native_close_failure_still_closes_both_notification_descriptors(tmp_path):
    with service_env(tmp_path) as env:
        service = env.service
        sockets = (waitable(service), service._reply_writer)

        def fail():
            raise ValueError('owned native close failure')

        service.bind_queue(SimpleNamespace(action_id=env.action.action_id, close=fail))
        with pytest.raises(ValueError, match='owned native close failure'):
            service.close(deadline_ms=1000)
        assert all(sock.fileno() == -1 for sock in sockets)
        assert service.reply_waitable() is None
        service.close(deadline_ms=0)


@pytest.mark.parametrize('where', ['configure_socket', 'start_second_thread'])
def test_partial_initialization_releases_owned_descriptors_and_threads(tmp_path, monkeypatch, where):
    import demiflow.execution.dataflow_authority as authority
    pairs, threads = [], []
    original_pair, original_start = socket.socketpair, threading.Thread.start

    class FailedSetup:
        def __init__(self, sock):
            self.sock = sock
        def setblocking(self, flag):
            raise OSError('owned socket setup failure')
        def close(self):
            self.sock.close()

    def capture_pair():
        pair = original_pair()
        pairs.append(pair)
        return (pair[0], FailedSetup(pair[1])) if where == 'configure_socket' else pair

    def start(thread):
        threads.append(thread)
        if len(threads) == 2:
            raise RuntimeError('owned thread startup failure')
        return original_start(thread)

    _, bounds = resolve_runtime_policy({}, {}, provider='local')
    with RunContext.start(tmp_path / 'run') as run:
        action = RunAction(run, 'action-1', {'kind': 'test'})
        with monkeypatch.context() as patch:
            patch.setattr(authority.socket, 'socketpair', capture_pair)
            if where == 'start_second_thread':
                patch.setattr(threading.Thread, 'start', start)
            with pytest.raises((OSError, RuntimeError), match='owned .* failure'):
                DriverJournalService(action, store=None, bounds=bounds)
        assert len(pairs) == 1 and all(sock.fileno() == -1 for sock in pairs[0])
        assert all(not thread.is_alive() for thread in threads)


@pytest.mark.parametrize('provider', ['local', 'ray_core'])
def test_original_pump_waits_for_reply_readiness_only_on_local(tmp_path, monkeypatch, provider):
    from demiflow.execution.dataflow import DataflowExecution
    import demiflow.execution.dataflow as runtime
    with service_env(tmp_path) as env:
        peer, sender = socket.socketpair()
        try:
            # Isolate control-loop wiring; real Dataset/native tests cover the business path.
            endpoint = SimpleNamespace(_sock=peer, poll=lambda **kwargs: (), deliver_reply=lambda reply: None)
            driver = object.__new__(DataflowExecution)
            driver._closed, driver._started = False, True
            driver._check_stop = driver._renew_workers = driver._advance_cut = lambda: None
            driver._checkpoint_progress = lambda: 0
            driver._bounds = env.bounds
            driver._services = SimpleNamespace(journal_service=env.service)
            driver._endpoints = {'allocation': endpoint}
            driver._policy = SimpleNamespace(provider=provider)
            driver._state, driver._confirmed_rows = 'running', 0
            observed = []

            def ready(readers, writers, errors, timeout):
                observed.append(('select', tuple(readers), timeout))
                return [], [], []

            monkeypatch.setattr(select, 'select', ready)
            monkeypatch.setattr(runtime.time, 'sleep', lambda timeout: observed.append(('sleep', (), timeout)))
            driver.pump(deadline_ms=2)
            assert len(observed) == 1 and observed[0][2] == .002
            if provider == 'local':
                assert observed[0] == ('select', (peer, waitable(env.service)), .002)
            else:
                assert observed[0] == ('sleep', (), .002)
        finally:
            peer.close()
            sender.close()
