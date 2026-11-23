"""A durable producer window is not the absent consumer's one-row queue."""

from demiflow.execution.action_driver import services as action_services
import pytest

from demiflow import data
from demiflow.execution.run_context import RunContext


def expand(row):
    return [{'n': row['n'], 'branch': branch} for branch in range(2)]


def consume(row):
    return row


def pipeline():
    return data.from_items([{'n': 0}, {'n': 1}]).flat_map(expand).map(
        consume, concurrency=1, queue_depth=1)


@pytest.mark.parametrize('interrupt', [False, True])
def test_two_outputs_park_and_resume_before_single_row_consumer(tmp_path, monkeypatch, interrupt):
    from dataclasses import replace
    from demiflow.execution.executors import dataflow_api
    from demiflow.execution.action_driver.dataflow import DataflowExecution
    # This is the existing compiler's declared unfused mode, not a replacement
    # kernel or scheduler. A fused pure pipeline would never cross this seam.
    resolve = action_services.resolve_action_runtime
    def unfused(request):
        policy, bounds = resolve(request)
        return replace(policy, fusion=False), bounds
    monkeypatch.setattr(action_services, 'resolve_action_runtime', unfused)
    observed = []
    class Interrupted(Exception):
        pass
    original = DataflowExecution._retire_phase
    def retire(execution, image):
        entries = execution._checkpoint.residency['deliveries']
        pending = [entry for entry in entries.values() if not entry['consumed']]
        if len(pending) >= 2:
            observed.append(image.commit_id)
            if interrupt:
                raise Interrupted('after real expanded producer cut')
        return original(execution, image)
    limits = {'memory_bytes': 256 * 1024**2, 'max_row_bytes': 64 * 1024,
              'max_batch_bytes': 256 * 1024, 'checkpoint_rows': 1,
              'checkpoint_interval_ms': 60000, 'timeout_s': 60}
    expected = [{'n': n, 'branch': branch} for n in range(2) for branch in range(2)]
    previous = []
    try:
        with RunContext.start(tmp_path/'run', limits=limits) as run, data.local_execution(workers=1, worker_mode='process'):
            ref = run.ref
            with monkeypatch.context() as patch:
                patch.setattr(DataflowExecution, '_retire_phase', retire)
                actual = pipeline().take_all()
            previous = run.journal.execution_commits('action-1')
    except Interrupted:
        assert interrupt and observed
        with RunContext.resume(ref) as run, data.local_execution(workers=1, worker_mode='process'):
            previous = run.journal.execution_commits('action-1')
            actual = pipeline().take_all()
            current = run.journal.execution_commits('action-1')
            assert current[:len(previous)] == previous
            assert all(item['status'] == 'acked' for item in current)
    else:
        assert not interrupt, 'fault arm must reach its actual persisted two-row window'
    assert observed and previous and actual == expected
