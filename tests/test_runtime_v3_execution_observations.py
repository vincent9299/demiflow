"""Public diagnostics bind to real worker events, not planned or legacy tasks."""
import os
import threading
from types import SimpleNamespace

import pytest
from demiflow import data
from demiflow.execution.run_context import RunContext


def identity(row):
    return {**row, 'pid': os.getpid(), 'tid': threading.get_native_id()}


def test_real_worker_identity_stage_counts_and_admitted_bounds(tmp_path):
    with data.local_execution(workers=2) as session:
        plan = data.from_items([{'i': i} for i in range(3)]).map(identity)
        result = plan.take_all()
        facts = plan.execution_metadata().diagnostics
        observed = {(item['worker_process_id'], item['runner_thread_id']) for item in facts['workers']}
        assert {(row['pid'], row['tid']) for row in result} <= observed
        assert all(pid != os.getpid() for pid, _ in observed)
        assert set(session.stats['workers_used']) == observed
        assert facts['engine'] == 'dataflow_v3' and facts['status'] == 'complete'
        assert 0 < facts['peak_active_workers'] <= facts['admitted_bounds']['max_active_workers'] == 2
        maps = [stage for stage in facts['stages'] if stage['name'] == 'MapOp']
        assert len(maps) == 1 and maps[0]['rows_input'] == maps[0]['rows_output'] == 3
        assert 'peak_pending_tasks' not in facts
        assert all('query' not in item for item in facts['stages'])


def test_failed_business_status_is_not_overwritten_by_cleanup_cancel(tmp_path):
    def fail(row):
        raise ValueError('observed business failure')
    with pytest.raises(ValueError, match='observed business failure'):
        with RunContext.start(tmp_path / 'run') as run, data.local_execution() as session:
            plan = data.from_items([{'i': 1}]).map(fail)
            try:
                plan.take_all()
            finally:
                assert session.stats['status'] == 'failed'
                assert session.stats['failure_type'] == 'builtins.ValueError'
                assert not run.journal.execution_commits('action-1')


def test_early_terminal_completion_reports_actual_prefix_stop(tmp_path):
    with data.local_execution() as session:
        assert data.range(10000).map(identity).take(1)[0]['id'] == 0
        assert session.stats['status'] == 'closed_early'
        assert session.stats['state'] == 'succeeded'
        assert session.stats['workers_used']


def test_ready_identity_must_be_stable_and_nonnegative():
    # Explicit event-consumer component boundary; actual process execution above.
    from demiflow.execution.action_driver.dataflow import DataflowExecution
    from demiflow.execution.contracts.dataflow_errors import DataflowError
    execution = DataflowExecution.__new__(DataflowExecution)
    lease = SimpleNamespace(stage_id='s', partition_ids=('p',))
    execution._endpoints = {'a': object()}
    execution._scheduler = SimpleNamespace(leases={'a': lease})
    execution._observed_workers = {}
    execution._current_worker_observations = {}
    execution._observed_worker_count = 0
    execution._worker_detail_bytes = 0
    execution._bounds = SimpleNamespace(max_partitions=1, max_control_bytes=512)
    execution._peak_active_workers = 0
    def ready(pid, tid):
        return SimpleNamespace(kind='ready', allocation_id='a', boot_id='b',
            payload={'worker_process_id': pid, 'runner_thread_id': tid})
    execution._handle_event(ready(7, 9))
    execution._handle_event(ready(7, 9))
    assert execution._peak_active_workers == 0  # ready receipt order is not concurrency
    assert execution._observed_worker_count == len(execution._observed_workers) == 1
    with pytest.raises(DataflowError, match='changed'):
        execution._handle_event(ready(8, 9))
    with pytest.raises(DataflowError, match='positive PID'):
        execution._handle_event(ready(True, 9))


def test_worker_identity_details_are_bounded_without_losing_exact_boot_count():
    from demiflow.execution.action_driver.dataflow import DataflowExecution
    from demiflow.execution.contracts.dataflow_errors import DataflowError
    execution = DataflowExecution.__new__(DataflowExecution)
    lease = SimpleNamespace(stage_id='s', partition_ids=('p',))
    execution._endpoints = {'a': object(), 'b': object()}
    execution._scheduler = SimpleNamespace(leases={'a': lease, 'b': lease})
    execution._observed_workers = {}
    execution._current_worker_observations = {}
    execution._observed_worker_count = 0
    execution._worker_detail_bytes = 0
    execution._bounds = SimpleNamespace(max_partitions=1, max_control_bytes=512)
    def ready(allocation, pid):
        return SimpleNamespace(kind='ready', allocation_id=allocation, boot_id=allocation,
            payload={'worker_process_id': pid, 'runner_thread_id': 10})
    execution._handle_event(ready('a', 7))
    execution._handle_event(ready('b', 8))
    execution._handle_event(ready('b', 8))
    assert execution._observed_worker_count == 2
    assert len(execution._observed_workers) == 1
    assert execution._worker_detail_bytes <= 512
    with pytest.raises(DataflowError, match='changed'):
        execution._handle_event(ready('b', 9))
    # A smaller byte budget also bounds details even when count capacity remains.
    execution._observed_workers.clear()
    execution._current_worker_observations.clear()
    execution._worker_detail_bytes = 0
    execution._bounds = SimpleNamespace(max_partitions=10, max_control_bytes=1)
    execution._handle_event(ready('a', 7))
    assert not execution._observed_workers and execution._worker_detail_bytes == 0
