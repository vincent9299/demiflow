"""Checkpoint row policy counts a logical frontier, independently of graph depth."""
from types import SimpleNamespace as NS
import time

import pytest

from demiflow import data
from demiflow.execution.exchange.deliveries import DeliveryExchange
from demiflow.execution.action_driver.dataflow import DataflowExecution
from demiflow.execution.checkpoint.coordinator import DataflowCheckpointCoordinator
from demiflow.execution.contracts import state_codec
from demiflow.execution.pipeline.run_context import RunContext


def acknowledge_rows(driver, edge, count):
    spec = NS(edge_id=edge, producer_partitions=('p',), consumer_partitions=('c',), mode='memory')
    receiver = NS(send_business=lambda command: None)
    for index in range(count):
        payload = dict(delivery_id=str(index), edge_id=edge, producer_partition='p', target_partition='c')
        driver._exchange.route(payload, spec, None, {'c': receiver}, parking=False, bounds=driver._bounds)
        driver._exchange.acknowledge(str(index), receiver)


def execution(metrics, *, previous=None, source_rows=0, threshold=10):
    """Scheduling component: actual pump/request/cut methods, no worker I/O."""
    driver = object.__new__(DataflowExecution)
    driver._node_metrics = {key: {'rows_input': 0, 'rows_output': value}
                            for key, value in metrics.items()}
    driver._graph = NS(sources=('source',))
    # Match DataflowExecution.__init__ for this row-only scheduling probe.
    driver._fit = None
    driver._waiting_for_fit = False
    driver._checkpoint_source_rows_only = False
    driver._last_checkpoint_rows = source_rows
    driver._last_checkpoint_outputs = dict(previous or {})
    driver._exchange = DeliveryExchange()
    driver._last_edge_acks = {}
    driver._publication_future = driver._publication_thread = None
    driver._publication_resumed = driver._publication_stopping = False
    driver._checkpoint_deadline = None
    driver._source_credit_rotation = 0
    driver._source_done = set()
    driver._source_credit_needs_rotation = False
    driver._cancelled = False
    limits = {'checkpoint_timeout_s': 30,
        'checkpoint_unconfirmed_rows': 4096, 'checkpoint_unconfirmed_bytes': 64*1024**2}
    # As RunContext.start(limits=...) records them: every given limit is declared.
    driver._action = NS(context=NS(limits=limits, explicit_limits=frozenset(limits)))
    driver._checkpoint_at = time.monotonic()
    driver._policy = NS(checkpoint_rows=threshold, checkpoint_interval_ms=60000, provider='thread')
    driver._bounds = NS(max_pending_events=16)
    driver._closed, driver._started = False, True
    driver._state, driver._confirmed_rows = 'running', 0
    driver._check_stop = driver._renew_workers = lambda: None
    driver._services = NS(journal_service=NS(poll_replies=lambda **kwargs: []))
    driver.sent = []
    driver._endpoints = {'worker': NS(poll=lambda **kwargs: [],
        send_business=driver.sent.append, _assignment=NS(source_splits=()))}
    driver._done, driver._idle = set(), set()
    driver._stopping, driver._checkpoint_requested = False, False
    driver._checkpoint_deferred = set()
    driver._durable_residency = driver._cut = None
    driver._advance_cut = lambda: None
    return driver


def test_identity_chain_does_not_divide_row_threshold_by_graph_depth():
    driver = execution({(f'map-{i}', 'p0'): 9 for i in range(40)})
    driver.pump(deadline_ms=1)
    assert not driver._checkpoint_requested and driver.sent == []
    driver._node_metrics[('map-39', 'p0')]['rows_output'] = 10
    driver.pump(deadline_ms=1)
    assert driver._checkpoint_requested and driver.sent == [{'kind': 'pause'}]


@pytest.mark.parametrize('trigger', ['source', 'timer', 'regression'])
def test_graph_source_cut_policy_does_not_count_broadcast_copies(trigger):
    driver = execution({('source', 'p0'): 12,
                        **{('router', f'p{i}'): 12 for i in range(6)},
                        ('terminal', 'p0'): 72}, threshold=60)
    driver._checkpoint_source_rows_only = True
    acknowledge_rows(driver, 'broadcast', 72)
    driver.pump(deadline_ms=1)
    assert driver._checkpoint_progress() == 12
    assert not driver._checkpoint_requested and driver.sent == []
    if trigger == 'source':
        driver._node_metrics[('source', 'p0')]['rows_output'] = 60
    elif trigger == 'timer':
        driver._checkpoint_at -= 61
    else:
        driver._last_checkpoint_rows = 13
    driver.pump(deadline_ms=1)
    assert driver._checkpoint_requested and driver.sent == [{'kind': 'pause'}]


def test_later_stage_is_not_hidden_by_a_large_previously_acked_stage():
    driver = execution({('earlier', 'p0'): 1000000, ('later', 'p0'): 12},
                       previous={'earlier': 1000000, 'later': 2})
    driver.pump(deadline_ms=1)
    assert driver._checkpoint_requested
    # Merely requesting a cut cannot acknowledge the new output frontier.
    assert driver._last_checkpoint_outputs == {'earlier': 1000000, 'later': 2}


def test_all_partitions_of_one_logical_occurrence_count_together():
    driver = execution({('join', 'p0'): 14, ('join', 'p1'): 15}, previous={'join': 20})
    assert driver._checkpoint_frontiers() == (0, {'join': 29})
    driver.pump(deadline_ms=1)
    assert not driver._checkpoint_requested
    driver._node_metrics[('join', 'p1')]['rows_output'] += 1
    driver.pump(deadline_ms=1)
    assert driver._checkpoint_requested
    assert len(driver._checkpoint_frontiers()[1]) == 1


@pytest.mark.parametrize('kind', ['output', 'missing_output', 'source'])
def test_counter_regression_conservatively_requests_a_cut(kind):
    metrics = {('source', 'p0'): 100, ('join', 'p0'): 100}
    if kind == 'missing_output':
        metrics.pop(('join', 'p0'))
    else:
        metrics[('source' if kind == 'source' else 'join', 'p0')] = 1
    driver = execution(metrics, previous={'join': 100}, source_rows=100)
    driver.pump(deadline_ms=1)
    assert driver._checkpoint_requested
    assert driver._last_checkpoint_outputs == {'join': 100}
    assert driver._last_checkpoint_rows == 100


@pytest.mark.parametrize('trigger', ['source_partitions', 'delivery_acks', 'timer'])
def test_original_source_ack_and_timer_triggers_remain(trigger):
    driver = execution({('join', 'p0'): 1})
    if trigger == 'source_partitions':
        driver._node_metrics.update({('source', 'p0'): {'rows_output': 4},
                                     ('source', 'p1'): {'rows_output': 6}})
    elif trigger == 'delivery_acks':
        acknowledge_rows(driver, 'logical-edge', 10)
        assert driver._exchange.snapshot_acks() == (10, {'logical-edge': 10})
        assert driver._exchange.can_cut()
    else:
        driver._checkpoint_at -= 61
    driver.pump(deadline_ms=1)
    assert driver._checkpoint_requested


def test_only_successful_commit_ack_advances_all_frontiers():
    driver = execution({('source', 'p0'): 8, ('join', 'p0'): 9},
                       previous={'join': 2}, source_rows=3)
    acknowledge_rows(driver, 'edge', 4)
    driver._cut = NS(cut_id='cut', participants=('participant',), final=False)
    driver._contributions = {'participant': object()}
    driver._acquisition = None
    driver._sinks = {}
    driver._phase_index, driver._phases = 0, [('phase',)]
    old_time = driver._checkpoint_at

    def fail(*args, **kwargs):
        raise RuntimeError('no commit ACK')

    driver._checkpoint = NS(commit=fail)
    DataflowExecution._advance_cut(driver)
    driver._publication_thread.join(2)
    with pytest.raises(RuntimeError, match='no commit ACK'):
        driver._poll_publication()
    assert (driver._last_checkpoint_rows, driver._last_checkpoint_outputs) == (3, {'join': 2})
    assert driver._exchange.snapshot_acks()[0] == 4 and driver._checkpoint_at == old_time
    assert driver.sent == []
    image = NS(commit_id='acked-commit')
    driver._checkpoint.commit = lambda *args, **kwargs: image
    DataflowExecution._advance_cut(driver)
    driver._publication_thread.join(2)
    driver._poll_publication()
    assert (driver._last_checkpoint_rows, driver._last_checkpoint_outputs) == (8, {'join': 9})
    assert driver._exchange.snapshot_acks()[0] == 0 and driver._checkpoint_progress() == 0
    assert driver.sent == [{'kind': 'receipt', 'cut_id': 'cut', 'commit_id': 'acked-commit'},
                           {'kind': 'resume', 'publishing': False, 'observation_rows': 0,
                            'source_credit': {'rows': 0, 'bytes': 0}}]


def test_finite_join_after_source_eoi_naturally_checkpoints_and_resumes(tmp_path, monkeypatch):
    """No injected request: finite output rows trigger the original native cut."""
    class Interrupted(Exception):
        pass

    original_request = DataflowExecution.request_checkpoint
    original_commit = DataflowCheckpointCoordinator.commit
    observed, boundary = [], {}

    def requested(driver):
        sources, outputs = driver._checkpoint_frontiers()
        delta = max((rows - driver._last_checkpoint_outputs.get(occ, 0)
                     for occ, rows in outputs.items()), default=0)
        if sources == driver._last_checkpoint_rows and delta >= driver._policy.checkpoint_rows:
            observed.append({'outputs': delta, 'acks': driver._exchange.snapshot_acks()[0],
                             'source_eoi': driver._checkpoint.sources_exhausted()})
        return original_request(driver)

    def committed(coordinator, *args, **kwargs):
        image = original_commit(coordinator, *args, **kwargs)
        if not observed:
            return image
        for contribution in image.contributions:
            for ref in contribution.operator_states.values():
                if ref.media_type != 'application/x-demiflow-kernel-image-v3':
                    continue
                state = state_codec.loads(b''.join(coordinator._store.read(ref, max_bytes=ref.size_bytes)),
                                          max_bytes=coordinator._physical.bounds.max_state_bytes)
                cursor = state.get('finish_cursor')
                if state.get('kind') == 'join' and cursor and cursor['emitted']:
                    assert not image.cut.final
                    boundary.update(commit_id=image.commit_id, emitted=cursor['emitted'])
                    raise Interrupted('naturally triggered finite output ACK')
        return image

    def consume(row):
        # A real slow transform keeps the finite iterator live beyond its
        # ordinary metrics refresh. This does not inject a checkpoint request.
        time.sleep(.002)
        return row

    def pipeline():
        return data.from_items([{'k': 0, 'i': i} for i in range(100)]).join(
            data.from_items([{'k': 0, 'j': j} for j in range(10)]), on='k').map(consume)

    with monkeypatch.context() as patch:
        patch.setattr(DataflowExecution, 'request_checkpoint', requested)
        patch.setattr(DataflowCheckpointCoordinator, 'commit', committed)
        with pytest.raises(Interrupted, match='naturally triggered finite output ACK'):
            with RunContext.start(tmp_path/'run', limits={'checkpoint_rows': 64,
                    'checkpoint_interval_ms': 60000, 'timeout_s': 60}) as run, data.local_execution(workers=4):
                run_ref = run.ref
                pipeline().count()
    assert 0 < boundary['emitted'] < 1000
    # The two finite-source handoffs may have real ACKs. They are below the
    # unchanged row threshold and cannot be the cause of this output-driven cut.
    assert observed and any(item['acks'] < 64 and item['source_eoi'] for item in observed)
    with RunContext.resume(run_ref) as run, data.local_execution(workers=4):
        assert pipeline().count() == 1000
        assert any(fact['commit_id'] == boundary['commit_id'] and fact['status'] == 'acked'
                   for fact in run.journal.execution_commits('action-1'))
