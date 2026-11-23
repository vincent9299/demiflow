"""A finite user fold owns business time, never a fabricated recovery cursor."""
from collections import deque
from contextlib import nullcontext
from dataclasses import replace
from types import SimpleNamespace as NS
import threading
import time
import json

import pytest

from demiflow import data
from demiflow.execution.contracts.dataflow_errors import DataflowError
from demiflow.execution.action_driver.dataflow import DataflowExecution
from demiflow.execution.pipeline.run_context import RunContext
from demiflow.execution.run_journal import RunJournal
from demiflow.execution.worker.dataflow_graph_runner import WorkerGraphRunner
from test_runtime_v3_ray_terminal_seams import private_ray_provider


def _driver(monkeypatch, *, peers=1):
    clock = [0.0]
    monkeypatch.setattr('demiflow.execution.action_driver.dataflow.time.monotonic', lambda: clock[0])
    driver = object.__new__(DataflowExecution)
    driver._checkpoint_deadline = 60.0
    driver._clear_checkpoint_wait()
    driver._checkpoint_requested = True
    driver._cut = driver._publication_future = None
    driver._stopping = False
    driver._idle = set()
    driver._endpoints = {}
    leases = {}
    for index in range(peers):
        key = 'worker-' + str(index)
        leases[key] = NS(boot_id='boot-' + str(index), partition_ids=('p0',))
        driver._endpoints[key] = NS(_activity_generation=1,
            _assignment=NS(stages=(NS(occurrences=('fold',)),), source_splits=()))
    driver._scheduler = NS(leases=leases, scope_id='scope')
    driver._graph = NS(nodes=(NS(occurrence_id='fold', kernel=NS(kind='reduce',
        inputs=(NS(requirement='finite_complete'),))),))
    driver._physical = NS(plan_revision=0)
    driver._action = NS(context=NS(attempt=1, fence='fence', ref=NS(run_id='run'),
        limits={'checkpoint_timeout_s': 60}))
    return driver, clock


def _wait(driver, *, generation=1, sequence=1, occurrence='fold', worker='worker-0',
          partition='p0', boot=None):
    from demiflow.execution.contracts.dataflow import WorkerEvent
    event = WorkerEvent(schema='demiflow.dataflow_event.v3', kind='checkpoint_wait',
        operation_id='1' * 64, run_id='run', attempt=1, fence='fence',
        allocation_id=worker, boot_id=boot or driver._scheduler.leases[worker].boot_id,
        plan_revision=0, scope_id='scope', controller_epoch=1,
        payload_ref=None,
        payload={'activity_generation': generation, 'sequence': sequence,
                 'partition_id': partition, 'occurrence_id': occurrence})
    driver._update_checkpoint_wait_clock()
    driver._validate_event(event)
    driver._handle_event(event)
    driver._update_checkpoint_wait_clock()
    return event


def test_only_finite_business_is_excluded_without_refilling_coordination_allowance(monkeypatch):
    driver, clock = _driver(monkeypatch)
    clock[0] = 10
    _wait(driver)
    clock[0] = 90
    driver._check_checkpoint_deadline()  # 80s of confirmed business, original 60s unchanged.
    _wait(driver, sequence=2, occurrence=None)
    assert driver._checkpoint_deadline == 140
    clock[0] = 139.99
    driver._check_checkpoint_deadline()
    clock[0] = 140
    with pytest.raises(TimeoutError, match='Checkpoint coordination'):
        driver._check_checkpoint_deadline()


def test_silent_peer_cannot_borrow_another_workers_business_exemption(monkeypatch):
    driver, clock = _driver(monkeypatch, peers=2)
    _wait(driver)
    clock[0] = 60
    with pytest.raises(TimeoutError, match='Checkpoint coordination'):
        driver._check_checkpoint_deadline()
    assert driver._checkpoint_deadline == 60


@pytest.mark.parametrize('state', ['cut', 'publication', 'stopping'])
def test_admitted_cut_publication_and_terminal_stop_keep_original_deadline(monkeypatch, state):
    driver, clock = _driver(monkeypatch)
    setattr(driver, {'cut': '_cut', 'publication': '_publication_future', 'stopping': '_stopping'}[state], True)
    _wait(driver)
    clock[0] = 60
    with pytest.raises(TimeoutError, match='Checkpoint coordination'):
        driver._check_checkpoint_deadline()


def test_finite_business_never_extends_original_run_deadline(monkeypatch):
    from demiflow.execution.recovery_contract import RecoveryContractError
    driver, clock = _driver(monkeypatch)
    run = object.__new__(RunContext)
    run.active, run.started, run.deadline = True, 0.0, float('inf')
    run.limits = {'timeout_s': 120, 'checkpoint_timeout_s': 60}
    run._draining = False
    run.check_stop_callback = None
    run.journal = NS(execution_status=lambda: {'status': 'running'})
    driver._action.context = run
    driver._native_guard = NS(check=lambda: None)
    driver._terminal_stop_deadline = None
    driver._began_at = 0.0
    driver._bounds = NS(timeout_ms=120000)
    driver._checkpoint_finite_wait['worker-0'] = (1, 'boot-0')
    driver._update_checkpoint_wait_clock()
    clock[0] = 119
    driver._check_stop()
    clock[0] = 120.001
    with pytest.raises(RecoveryContractError, match='Execution time budget exhausted'):
        driver._check_stop()


@pytest.mark.parametrize('damage', ['boot', 'partition', 'occurrence', 'kind', 'port', 'generation'])
def test_wait_evidence_is_bound_to_real_sealed_worker(monkeypatch, damage):
    driver, _ = _driver(monkeypatch)
    kwargs = {}
    if damage == 'boot': kwargs['boot'] = 'different-boot'
    if damage == 'partition': kwargs['partition'] = 'different-partition'
    if damage == 'occurrence': kwargs['occurrence'] = 'undeclared'
    if damage == 'kind': driver._graph.nodes[0].kernel.kind = 'MapOp'
    if damage == 'port': driver._graph.nodes[0].kernel.inputs[0].requirement = 'incremental'
    if damage == 'generation': kwargs['generation'] = 2
    with pytest.raises(DataflowError) as error:
        _wait(driver, **kwargs)
    assert error.value.code == 'FENCED'
    assert not driver._checkpoint_finite_wait


def test_delayed_busy_cannot_replace_finished_or_new_generation_frontier(monkeypatch):
    driver, clock = _driver(monkeypatch)
    _wait(driver, sequence=2, occurrence=None)
    _wait(driver, sequence=1)
    assert not driver._checkpoint_finite_wait
    driver._endpoints['worker-0']._activity_generation = 2
    _wait(driver, sequence=3, generation=1)
    assert not driver._checkpoint_finite_wait
    clock[0] = 60
    with pytest.raises(TimeoutError): driver._check_checkpoint_deadline()


def test_idle_and_phase_change_cannot_inherit_finite_business(monkeypatch):
    driver, _ = _driver(monkeypatch)
    event = _wait(driver)
    driver._handle_event(replace(event, kind='idle',
        payload={'activity_generation': 1, 'done': True, 'source_done': True}))
    assert driver._idle == {'worker-0'} and not driver._checkpoint_finite_wait
    _wait(driver, sequence=2)
    assert not driver._idle and driver._checkpoint_finite_wait
    driver._clear_checkpoint_wait()
    assert not driver._checkpoint_finite_wait and not driver._checkpoint_wait_sequence
    assert driver._checkpoint_wait_at is None


def _runner():
    runner = object.__new__(WorkerGraphRunner)
    runner._condition = threading.Condition()
    runner._checkpoint_finishes = set()
    runner._checkpoint_wait_requested = False
    runner._checkpoint_wait_signature = None
    runner._checkpoint_wait_sequence = 0
    runner._activity_generation = 0
    runner._wake_generation = 0
    runner.partition = 'p0'
    runner._paused = runner._idle_sent = False
    runner._queue = deque()
    runner.bounds = NS(max_pending_events=8)
    runner.notices = []
    runner._send = lambda kind, payload: runner.notices.append((kind, payload))
    return runner


@pytest.mark.parametrize('pause_first', [True, False])
def test_actual_finite_iterator_reports_pause_before_or_during_finish(pause_first):
    runner = _runner()
    entered, release = threading.Event(), threading.Event()
    def finish(port, **kwargs):
        entered.set()
        assert release.wait(3)
        yield 'result'
    kernel = NS(_spec=NS(kind='reduce'), finish_replayable=False, finish=finish)
    iterator = runner._finite_finish_iterator('fold', kernel, 'left')
    if pause_first: runner.receive({'kind': 'pause', 'activity_generation': 1})
    received, errors = [], []
    def consume():
        try: received.extend(iterator)
        except BaseException as exc: errors.append(exc)
    worker = threading.Thread(target=consume)
    worker.start()
    try:
        assert entered.wait(3)
        if not pause_first: runner.receive({'kind': 'pause', 'activity_generation': 1})
        assert runner.notices[-1][1]['occurrence_id'] == 'fold'
        assert not runner._idle_sent
        # A current activity watermark invalidates the old proof and receives
        # another bounded response even while the business call is blocked.
        runner.receive({'kind': 'edge_eoi', 'activity_generation': 2})
        assert runner.notices[-1][1]['activity_generation'] == 2
    finally:
        release.set()
        worker.join(3)
    assert not worker.is_alive() and not errors and received == ['result']
    assert runner.notices[-1][1]['occurrence_id'] is None
    assert [payload['sequence'] for _, payload in runner.notices] == [1, 2, 3]


def test_busy_event_uses_original_bounded_wire_and_activation_queue(monkeypatch):
    from demiflow.execution.contracts.dataflow_codec import encode_wire, decode_wire
    from demiflow.execution.hosts.endpoints import _EndpointBase
    from test_runtime_v3_c import bounds
    driver, _ = _driver(monkeypatch)
    event = _wait(driver)
    budget = bounds()
    raw = encode_wire(event, type_name='WorkerEvent', bounds=budget)
    assert decode_wire(raw, type_name='WorkerEvent', bounds=budget) == event
    endpoint = object.__new__(_EndpointBase)
    endpoint._assignment = event
    endpoint._allocation_id, endpoint._boot_id = event.allocation_id, event.boot_id
    endpoint._events = deque()
    endpoint._services = NS(bounds=budget)
    endpoint._early_event(event)
    assert tuple(endpoint._events) == (event,)


def test_failed_fold_keeps_original_error_when_wait_cleanup_send_fails():
    runner = _runner()
    failure = ValueError('original reducer failure')
    def finish(port, **kwargs):
        raise failure
        yield
    kernel = NS(_spec=NS(kind='reduce'), finish_replayable=False, finish=finish)
    runner.receive({'kind': 'pause', 'activity_generation': 1})
    send = runner._send
    def notify(kind, payload):
        if payload['occurrence_id'] is None: raise RuntimeError('closed original control channel')
        send(kind, payload)
    runner._send = notify
    with pytest.raises(ValueError) as caught:
        list(runner._finite_finish_iterator('fold', kernel, 'left'))
    assert caught.value is failure and not runner._checkpoint_finishes
    assert any('closed original control channel' in note for note in failure.__notes__)


def _real_long_finite_fold(tmp_path, monkeypatch, workers, record_property, *, provider):
    marker = tmp_path/'fold-started'
    notices, executions = [], []
    start = DataflowExecution.start
    poll = DataflowExecution._poll_control
    handle = DataflowExecution._handle_event
    requested = []
    def started(execution):
        assert execution._policy.provider == provider
        executions.append(execution)
        return start(execution)
    def observed(execution, event):
        if event.kind == 'checkpoint_wait': notices.append(dict(event.payload))
        return handle(execution, event)
    def polled(execution, *args, **kwargs):
        result = poll(execution, *args, **kwargs)
        if marker.exists() and not requested:
            requested.append(time.monotonic())
            execution.request_checkpoint()
        return result
    monkeypatch.setattr(DataflowExecution, 'start', started)
    monkeypatch.setattr(DataflowExecution, '_poll_control', polled)
    monkeypatch.setattr(DataflowExecution, '_handle_event', observed)
    def fold(acc, row):
        if acc is None:
            marker.write_text('original worker entered finite fold')
            time.sleep(1.8)
        return {'k': row['k'], 'count': (acc or {}).get('count', 0) + 1,
                'sum': (acc or {}).get('sum', 0) + row['v']}
    limits = {'timeout_s': 60, 'checkpoint_timeout_s': 1,
              'checkpoint_rows': 1000, 'checkpoint_interval_ms': 60000}
    execution_context = data.local_execution(workers=workers) if provider == 'local' else nullcontext()
    with RunContext.start(tmp_path/'run', limits=limits) as run, execution_context:
        actual = data.from_items([{'k': 0, 'v': i} for i in range(8)]).reduce_by_key('k', fold).take_all()
        assert actual == [{'k': 0, 'count': 8, 'sum': 28}]
        assert run.limits['checkpoint_timeout_s'] == 1 and run.limits['timeout_s'] == 60
    assert requested and notices and any(x['occurrence_id'] is not None for x in notices)
    commits = RunJournal(tmp_path/'run').execution_commits('action-1')
    assert commits and all(c['status'] == 'acked' for c in commits)
    assert executions[0]._restore_image.cut.final
    assert executions[0]._scheduler._physical_exit_confirmed
    record_property('finite_checkpoint_evidence', json.dumps({
        'run_directory': str(tmp_path/'run'), 'limits': limits,
        'workers': workers, 'provider': provider, 'requested_at': requested, 'notices': notices,
        'commits': [{'commit_id': c['commit_id'], 'sequence': c['sequence'], 'status': c['status']}
                    for c in commits], 'final': True, 'physical_exit_confirmed': True,
        'actual': actual}, sort_keys=True))


@pytest.mark.parametrize('workers', [1, 2])
def test_real_dataset_long_finite_fold_reaches_original_final_ack(tmp_path, monkeypatch, workers, record_property):
    _real_long_finite_fold(tmp_path, monkeypatch, workers, record_property, provider='local')


def test_real_ray_long_finite_fold_reaches_original_final_ack(tmp_path, monkeypatch, record_property,
                                                           private_ray_provider):
    _real_long_finite_fold(tmp_path, monkeypatch, None, record_property, provider='ray_core')
