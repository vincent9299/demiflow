"""Real publication overlap with finite source credit and original ACK ownership."""
import json
import threading
import time
from pathlib import Path

import pytest

from demiflow import data
from demiflow.execution.checkpoint.commit import CommitCoordinator
from demiflow.execution.action_driver.dataflow import DataflowExecution
from demiflow.execution.pipeline.run_context import RunContext
from demiflow.execution.journal.run_journal import RunJournal


def _lines(path):
    return path.read_text().splitlines() if path.exists() else []


def _capture(monkeypatch):
    executions = []
    original = DataflowExecution._start_publication
    def start(execution):
        executions[:] = [execution]
        return original(execution)
    monkeypatch.setattr(DataflowExecution, '_start_publication', start)
    return executions


def _wait(predicate, message):
    deadline = time.monotonic() + 5
    while not predicate() and time.monotonic() < deadline:
        time.sleep(.005)
    assert predicate(), message


def test_slow_publication_stops_at_actual_unconfirmed_row_credit(tmp_path, monkeypatch):
    observed = tmp_path / 'worker-rows'
    def record(row):
        with observed.open('a') as out:
            out.write(str(row['id']) + '\n')
        return row
    executions = _capture(monkeypatch)
    original = CommitCoordinator._finish
    checked = []
    def slow(coordinator, fact, sinks, acknowledge):
        if fact['sequence'] == 1 and fact['intent']['sinks'] == ['materialize']:
            _wait(lambda: len(_lines(observed)) == 6, 'next generation did not consume the available credit')
            time.sleep(.1)
            assert len(_lines(observed)) == 6, 'unconfirmed source row limit was exceeded'
            assert len(executions[0]._checkpoint._pending) == 1
            assert len(coordinator.journal.execution_commits(coordinator.action.action_id)) == 1
            assert coordinator.journal.execution_commit(fact['commit_id'])['status'] == 'prepared'
            checked.append(True)
        return original(coordinator, fact, sinks, acknowledge)
    monkeypatch.setattr(CommitCoordinator, '_finish', slow)
    with RunContext.start(tmp_path/'run', limits={'timeout_s': 30, 'checkpoint_rows': 4,
            'checkpoint_unconfirmed_rows': 6, 'checkpoint_interval_ms': 60000}):
        rows = data.from_items([{'id': i} for i in range(21)]).map(record).materialize().take_all()
    assert rows == [{'id': i} for i in range(21)]
    assert checked == [True]
    assert _lines(observed) == [str(i) for i in range(21)]
    assert all(f['status'] == 'acked' for f in RunJournal(tmp_path/'run').execution_commits('action-1'))


def test_small_source_credit_rotates_owners_without_losing_union_rows(tmp_path):
    with RunContext.start(tmp_path/'run', limits={'timeout_s': 30, 'checkpoint_rows': 20,
            'checkpoint_unconfirmed_rows': 1, 'checkpoint_interval_ms': 60000}):
        rows = data.from_items([{'id': i} for i in range(3)]).union(
            data.from_items([{'id': i} for i in range(3, 6)])).materialize().take_all()
    assert sorted(rows, key=lambda row: row['id']) == [{'id': i} for i in range(6)]
    facts = RunJournal(tmp_path/'run').execution_commits('action-1')
    assert facts and all(f['status'] == 'acked' for f in facts)


def test_failure_while_publishing_drains_original_ack_and_preserves_error(tmp_path, monkeypatch):
    failed = tmp_path/'worker-failed'
    def fail(row):
        if row['id'] == 6:
            failed.write_text('original business failure')
            raise ValueError('failure after sealed frontier')
        return row
    original = CommitCoordinator._finish
    drained = []
    def slow(coordinator, fact, sinks, acknowledge):
        if fact['sequence'] == 1 and fact['intent']['sinks'] == ['materialize']:
            _wait(failed.exists, 'worker did not run while the old cut published')
            time.sleep(.1)
        result = original(coordinator, fact, sinks, acknowledge)
        drained.append(result['commit_id'])
        return result
    monkeypatch.setattr(CommitCoordinator, '_finish', slow)
    with pytest.raises(ValueError, match='failure after sealed frontier'):
        with RunContext.start(tmp_path/'run', limits={'timeout_s': 30, 'checkpoint_rows': 4,
                'checkpoint_interval_ms': 60000}):
            data.from_items([{'id': i} for i in range(12)]).map(fail).materialize()
    journal = RunJournal(tmp_path/'run')
    assert len(drained) == 1
    assert journal.execution_commit(drained[0])['status'] == 'acked'
    assert journal.execution_action('action-1')['result'] is None
    assert not [t for t in threading.enumerate() if t.name == 'demiflow-checkpoint-publication']


def test_cancel_during_native_publication_drains_without_aborting_acked_cut(tmp_path, monkeypatch):
    entered = threading.Event()
    original_finish = CommitCoordinator._finish
    drained = []
    def slow(coordinator, fact, sinks, acknowledge):
        entered.set()
        time.sleep(.15)
        result = original_finish(coordinator, fact, sinks, acknowledge)
        drained.append(result['commit_id'])
        return result
    monkeypatch.setattr(CommitCoordinator, '_finish', slow)
    original_pump = DataflowExecution._pump_once
    cancelled = []
    def pump(execution, **kwargs):
        status = original_pump(execution, **kwargs)
        if entered.is_set() and execution._publication_future is not None and not cancelled:
            cancelled.append(True)
            execution.cancel('controlled cancellation during publication')
        return status
    monkeypatch.setattr(DataflowExecution, '_pump_once', pump)
    with pytest.raises(Exception, match='cancelled'):
        with RunContext.start(tmp_path/'run', limits={'timeout_s': 30, 'checkpoint_rows': 4,
                'checkpoint_interval_ms': 60000}):
            data.from_items([{'id': i} for i in range(12)]).materialize()
    journal = RunJournal(tmp_path/'run')
    assert cancelled == [True] and len(drained) == 1
    assert journal.execution_commit(drained[0])['status'] == 'acked'
    assert journal.execution_action('action-1')['result'] is None
    assert not [t for t in threading.enumerate() if t.name == 'demiflow-checkpoint-publication']


def test_publication_obeys_encoded_byte_window_before_next_read(tmp_path, monkeypatch):
    observed = tmp_path/'byte-rows'
    rows = [{'id': i, 'value': 'x'*900} for i in range(30)]
    from demiflow.execution.contracts.record_codec import row_bytes as _row_bytes
    sizes = [_row_bytes(row, max_bytes=4096) for row in rows]
    window = sum(sizes[:12])
    def record(row):
        with observed.open('a') as out:
            out.write(str(row['id'])+'\n')
        return row
    original = CommitCoordinator._finish
    checked = []
    def slow(coordinator, fact, sinks, acknowledge):
        if fact['sequence'] == 1 and fact['intent']['sinks'] == ['materialize']:
            _wait(lambda: len(_lines(observed)) > 4, 'byte credit did not permit any overlap')
            time.sleep(.15)
            count = len(_lines(observed))
            assert 4 < count < len(rows)
            assert sum(sizes[:count]) <= window
            assert sum(sizes[:count]) + 4096 > window
            time.sleep(.1)
            assert len(_lines(observed)) == count, 'read advanced after the finite byte window closed'
            checked.append(count)
        return original(coordinator, fact, sinks, acknowledge)
    monkeypatch.setattr(CommitCoordinator, '_finish', slow)
    with RunContext.start(tmp_path/'run', limits={'timeout_s': 45, 'checkpoint_rows': 4,
            'max_row_bytes': 4096, 'checkpoint_unconfirmed_rows': 128,
            'checkpoint_unconfirmed_bytes': window, 'checkpoint_interval_ms': 60000}):
        actual = data.from_items(rows).map(record).materialize().take_all()
    assert actual == rows and len(checked) == 1


def test_each_logical_edge_advances_checkpoint_trigger_without_depth_multiplication():
    from test_runtime_v3_checkpoint_progress import execution
    driver = execution({}, threshold=64)
    from test_runtime_v3_checkpoint_progress import acknowledge_rows
    for edge in ('source-map', 'map-sink'):
        acknowledge_rows(driver, edge, 34)
    assert driver._exchange.snapshot_acks()[0] == 68
    assert driver._checkpoint_progress() == 34
    driver.pump(deadline_ms=1)
    assert not driver._checkpoint_requested
    acknowledge_rows(driver, 'map-sink', 30)
    driver.pump(deadline_ms=1)
    assert driver._checkpoint_requested


def test_native_lance_row_trigger_counts_business_frontier_once(tmp_path, monkeypatch):
    import lance
    from demiflow.execution.outputs.lance_sink import LanceSink
    from demiflow.execution.storage.state_artifacts import StateArtifacts
    observed = tmp_path/'lance-input'
    async def record(row):
        with observed.open('a') as out:
            out.write(str(row['id'])+'\n')
        return row
    from demiflow.execution.outputs.sinks import NativeTerminalSink
    original = NativeTerminalSink.prepare
    native = LanceSink.prepare
    preparation, native_preparation = [], []
    def prepare(sink, manifest, context):
        preparation.append(len(_lines(observed)))
        return original(sink, manifest, context)
    def native_prepare(sink, blocks, context):
        native_preparation.append(len(_lines(observed)))
        return native(sink, blocks, context)
    monkeypatch.setattr(NativeTerminalSink, 'prepare', prepare)
    monkeypatch.setattr(LanceSink, 'prepare', native_prepare)
    with RunContext.start(tmp_path/'run', limits={'timeout_s': 45, 'checkpoint_rows': 8,
            'checkpoint_interval_ms': 60000}) as run:
        data.from_items([{'id': i} for i in range(64)]).map_async(record).write_lance(
            str(tmp_path/'out.lance'), mode='create')
        facts = run.journal.execution_commits('action-1')
        first = StateArtifacts(run).read(facts[0]['prepared']['state']['dataflow-cut-v3'])
        assert sum(c['next_sequence'] for c in first['dataflow.sources.v3']) == 8
        assert all(f['status'] == 'acked' for f in facts)
        assert 2 <= len(facts) <= 9  # Eight row-triggered cuts plus optional true EOF discovery.
    assert preparation[0] == 8 and native_preparation == [64]
    table = lance.dataset(str(tmp_path/'out.lance'))
    assert table.version == 1
    assert table.to_table().to_pylist() == [{'id': i} for i in range(64)]


def test_failed_original_prepare_cleans_workers_without_false_receipt(tmp_path, monkeypatch):
    from demiflow.execution.outputs.sinks import _DataflowArtifactSink
    def fail(sink, *args, **kwargs):
        raise OSError('original sink preparation failed')
    monkeypatch.setattr(_DataflowArtifactSink, 'prepare', fail)
    executions = _capture(monkeypatch)
    with pytest.raises(OSError, match='original sink preparation failed'):
        with RunContext.start(tmp_path/'run', limits={'timeout_s': 20, 'checkpoint_rows': 2}):
            data.from_items([{'id': i} for i in range(8)]).materialize()
    journal = RunJournal(tmp_path/'run')
    facts = journal.execution_commits('action-1')
    assert len(facts) == 1 and facts[0]['status'] == 'pending'
    assert facts[0]['prepared'] is None and facts[0]['receipts'] == {}
    assert journal.execution_action('action-1')['result'] is None
    assert executions[0]._closed and not executions[0]._endpoints
    assert not [t for t in threading.enumerate() if t.name == 'demiflow-checkpoint-publication']


def test_take_during_old_publication_commits_the_new_delivered_prefix(tmp_path, monkeypatch):
    observed = tmp_path/'take-input'
    async def record(row):
        with observed.open('a') as out:
            out.write(str(row['id'])+'\n')
        return row
    executions = _capture(monkeypatch)
    original = CommitCoordinator._finish
    checked = []
    def slow(coordinator, fact, sinks, acknowledge):
        if fact['sequence'] == 1:
            _wait(lambda: len(_lines(observed)) >= 3, 'take did not receive its later prefix during publication')
            _wait(lambda: executions[0]._stopping, 'driver did not observe terminal satisfaction')
            checked.append(fact['commit_id'])
        return original(coordinator, fact, sinks, acknowledge)
    monkeypatch.setattr(CommitCoordinator, '_finish', slow)
    with RunContext.start(tmp_path/'run', limits={'timeout_s': 30, 'checkpoint_rows': 1,
            'checkpoint_interval_ms': 60000}) as run:
        actual = data.from_items([{'id': i} for i in range(20)]).map_async(record).take(3)
        assert actual == [{'id': i} for i in range(3)]
        execution = executions[0]
        assert len(checked) == 1
        assert execution._terminal_result.rows == 3
        assert execution._publication(execution._restore_image)['manifest']['total_rows'] == 3
        assert execution._restore_image.commit_id != checked[0]
        assert not execution._restore_image.cut.final
        facts = run.journal.execution_commits('action-1')
        assert len(facts) == 2 and all(f['status'] == 'acked' for f in facts)


def test_receipt_releases_only_the_sealed_progress_frontier():
    from types import SimpleNamespace as NS
    from test_runtime_v3_checkpoint_progress import execution
    driver = execution({('source', 'p0'): 6, ('map', 'p0'): 6}, source_rows=0)
    driver._publication_frontiers = (4, {'map': 4}, 4, {'edge': 4})
    from test_runtime_v3_checkpoint_progress import acknowledge_rows
    acknowledge_rows(driver, 'edge', 6)
    driver._cut = NS(cut_id='original-cut', final=False)
    driver._contributions = {'worker': object()}
    driver._phase_index, driver._phases = 0, [('phase',)]
    driver._finish_publication(NS(commit_id='original-ack'))
    assert driver._last_checkpoint_rows == 4 and driver._last_checkpoint_outputs == {'map': 4}
    assert driver._exchange.snapshot_acks()[0] == 2 and driver._checkpoint_progress() == 2
    assert driver._last_edge_acks == {'edge': 4}
