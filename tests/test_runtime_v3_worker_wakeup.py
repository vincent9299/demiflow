"""Use barriers, not elapsed-time thresholds, to exercise the lost-wake seam."""
from collections import deque
import threading
from types import SimpleNamespace

import pytest

from demiflow.execution.worker.dataflow_graph_runner import WorkerGraphRunner


class ObservedCondition(threading.Condition):
    def __init__(self, cancel):
        super().__init__()
        self.waits = []
        self.cancel = cancel

    def wait(self, timeout=None):
        self.waits.append(timeout)
        # A wait here would lose a notification already delivered by the test.
        # Stop deterministically instead of relying on a noisy 5ms assertion.
        self.cancel.set()
        return False


def runner():
    value = WorkerGraphRunner.__new__(WorkerGraphRunner)
    value.e = SimpleNamespace(_cancel_event=threading.Event())
    value._condition = ObservedCondition(value.e._cancel_event)
    value._wake_generation = 0
    value._queue = deque()
    value.bounds = SimpleNamespace(max_pending_events=64)
    value._idle_sent = False
    value._send = lambda *args, **kwargs: None
    return value


@pytest.mark.parametrize('notification', ['command', 'wake', 'cancel'])
def test_notification_between_empty_step_and_wait_is_not_lost(notification):
    value = runner()
    empty_step = threading.Event(); notification_done = threading.Event()
    calls = []
    command = {'kind':'data', 'sequence':7}
    def step():
        calls.append('step')
        if len(calls) == 1:
            empty_step.set()
            assert notification_done.wait(2)
            return 0
        assert notification == 'wake' or value._queue.popleft() is command
        value.e._cancel_event.set()
        return 1
    value._step_ready = step
    def notify():
        assert empty_step.wait(2)
        if notification == 'command':
            value.receive(command)
        else:
            if notification == 'cancel':
                value.e._cancel_event.set()
            value.wake()
        notification_done.set()
    worker = threading.Thread(target=notify)
    worker.start()
    try:
        value._run()
    finally:
        notification_done.set()
        worker.join(2)
    assert not worker.is_alive()
    assert value._condition.waits == []
    assert len(calls) == (1 if notification == 'cancel' else 2)


def test_truly_idle_worker_keeps_original_bounded_wait_without_spinning():
    value = runner()
    calls = []
    value._step_ready = lambda: calls.append('step') or 0
    value._run()
    assert calls == ['step']
    assert value._condition.waits == [.005]


def test_command_after_wait_started_wakes_original_condition():
    value = runner()
    entered_wait = threading.Event()
    class ReadyCondition(threading.Condition):
        def wait(self, timeout=None):
            entered_wait.set()
            return super().wait(timeout)
    value._condition = ReadyCondition()
    command = {'kind':'data', 'sequence':9}
    handled = []
    def step():
        with value._condition:
            if value._queue:
                handled.append(value._queue.popleft())
                value.e._cancel_event.set()
                return 1
        return 0
    value._step_ready = step
    worker = threading.Thread(target=value._run)
    worker.start()
    try:
        assert entered_wait.wait(2)
        value.receive(command)
        worker.join(2)
        assert not worker.is_alive()
        assert handled == [command]
    finally:
        value.e._cancel_event.set()
        value.wake()
        worker.join(2)


def test_publish_does_not_sleep_over_credit_ack_between_check_and_wait():
    from test_runtime_v3_c import make, bounds
    value = runner()
    exhausted, acknowledged = threading.Event(), threading.Event()
    operations, sent = [], []
    class CreditTable(dict):
        captured = False
        def __len__(self):
            count = super().__len__()
            if count and not self.captured:
                self.captured = True
                exhausted.set()
                assert acknowledged.wait(2)
            # Real concurrency can release credit immediately after len() was
            # evaluated, before the capacity error enters condition.wait().
            return count
    class NoDelayCondition(threading.Condition):
        def __init__(self):
            super().__init__(); self.waits = []
        def wait(self, timeout=None):
            self.waits.append(timeout)
            return False
    value._condition = NoDelayCondition()
    value._credits = CreditTable({'old-delivery': (None, 'old-buffer')})
    value._credit_payloads = {}
    value._credit_limit = 1
    value._park_mode = False
    value.partition = 'partition'
    value.bounds = bounds()
    value.services = SimpleNamespace(check_stop=lambda: None)
    value.e._next_operation_id = lambda kind: 'new-delivery'
    value.e._kernels = {}
    value._peer = SimpleNamespace(
        _bytes=0,
        release=lambda key: operations.append(('release', key)),
        put=lambda raw: operations.append(('put', raw)) or {'key':'new-buffer'})
    value._send = lambda kind,payload: sent.append((kind,payload))
    from demiflow.execution.kernels.dataflow import _row_bytes
    row = {'row':1}
    record = make('Record', inline=row, encoded_bytes=_row_bytes(row, max_bytes=value.bounds.max_row_bytes))
    def ack():
        assert exhausted.wait(2)
        value.receive({'kind':'ack','delivery_id':'old-delivery'})
        acknowledged.set()
    thread = threading.Thread(target=ack); thread.start()
    try:
        value._publish(record)
    finally:
        acknowledged.set()
        thread.join(2)
    assert not thread.is_alive()
    assert value._condition.waits == []
    assert [operation[0] for operation in operations] == ['release','put']
    assert list(value._credits) == ['new-delivery']
    assert sent[0][0] == 'terminal'
    assert sent[0][1]['delivery_id'] == 'new-delivery'
