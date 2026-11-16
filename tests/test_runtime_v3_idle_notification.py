"""A control command arriving during notification must get a fresh idle reply."""
from collections import deque
from types import SimpleNamespace
import threading
import time

import pytest

from demiflow.execution.worker.dataflow_graph_runner import WorkerGraphRunner


def _finished_runner():
    # Exercise the real step/receive state machine without allocating a host.
    runner = object.__new__(WorkerGraphRunner)
    runner.e = SimpleNamespace(_kernels={}, _readers={},
        _node_specs={'terminal': SimpleNamespace(kind='terminal')},
        assignment=SimpleNamespace(source_splits=()),
        accept_receipt=lambda *args, **kwargs: None)
    runner.bounds = SimpleNamespace(max_pending_events=8)
    runner._condition = threading.Condition()
    runner._wake_generation = 0
    runner._queue = deque()
    runner._cut_controls = deque()
    runner._sealed_cut = None
    runner._credits = {}
    runner._paused = True
    runner._idle_sent = False
    runner._done_sent = False
    runner._finished = {'terminal'}
    runner._finished_splits = set()
    runner._consumed_deliveries = set()
    runner._source_rows_since_cut = 0
    runner._cut_credit = {}
    runner._source_quantum = runner._source_park_quantum = 1
    runner._source_live_quantum = 8
    runner._park_mode = False
    runner._cut = None
    runner._metrics_at = time.monotonic()
    runner._flush_metrics = lambda: None
    runner._advance_input_completions = lambda **kwargs: 0
    return runner


def test_live_durable_peers_keep_original_checkpoint_policy_and_parking_cap():
    runner = _finished_runner()
    runner.e._physical = SimpleNamespace(policy=SimpleNamespace(checkpoint_rows=8))
    runner.receive({'kind': 'resume', 'durable_peers_live': True})
    assert runner._source_quantum == 8
    assert runner._source_park_quantum == 1
    runner.receive({'kind': 'park_policy'})
    assert runner._source_quantum == 1
    from demiflow.execution.contracts.dataflow_errors import DataflowError
    with pytest.raises(DataflowError, match='durable peer residency'):
        runner.receive({'kind': 'resume', 'durable_peers_live': True})
    assert runner._source_quantum == 1


@pytest.mark.parametrize('value', [1, None, 'true'])
def test_live_durable_peers_require_an_actual_boolean(value):
    runner = _finished_runner()
    from demiflow.execution.contracts.dataflow_errors import DataflowError
    with pytest.raises(DataflowError, match='durable peer residency'):
        runner.receive({'kind': 'resume', 'durable_peers_live': value})
    assert runner._source_quantum == 1


@pytest.mark.parametrize('command', [
    {'kind': 'pause'}, {'kind': 'resume'}, {'kind': 'park_policy'},
    {'kind': 'receipt', 'commit_id': 'receipt', 'cut_id': 'cut'},
])
@pytest.mark.parametrize('during', ['eoi', 'idle'])
def test_control_during_notification_gets_fresh_idle(command, during):
    runner = _finished_runner()
    entered, received = threading.Event(), threading.Event()
    failures, notifications = [], []

    def control():
        try:
            assert entered.wait(2)
            runner.receive(command)
        except BaseException as exc:
            failures.append(exc)
        finally:
            received.set()

    def send(kind, payload=None, ref=None):
        notifications.append(kind)
        if kind == during and not entered.is_set():
            entered.set()
            # Mailbox backpressure may take time. Control must remain live;
            # holding the runner lock across this send would deadlock here.
            assert received.wait(2)

    runner._send = send
    thread = threading.Thread(target=control)
    thread.start()
    try:
        runner.step(max_records=1, deadline_ms=50)
        assert not failures
        assert not runner._idle_sent, 'old notification overwrote a new control frontier'
        runner.step(max_records=1, deadline_ms=50)
        assert notifications.count('idle') == 2
        assert notifications.count('eoi') == 1
        runner.step(max_records=1, deadline_ms=50)
        assert notifications.count('idle') == 2
    finally:
        entered.set()
        thread.join(3)
        assert not thread.is_alive()
