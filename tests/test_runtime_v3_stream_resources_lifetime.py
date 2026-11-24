"""Real semantic resource admission and independent published-result lifetime."""
import pytest

from demiflow import data
from demiflow.execution.contracts.dataflow_errors import DataflowError
from demiflow.execution.action_driver.dataflow import DataflowExecution
from demiflow.execution.pipeline.run_context import RunContext
from _worker_event_fixtures import emit, events
from runtime_v3_recovery_scenarios import _provider
from runtime_v3_support import _RayDataBlocker


@pytest.mark.parametrize('provider', ['local', 'ray_core'])
def test_stream_cpu_claim_reaches_original_allocation_and_nested_terminal(tmp_path, monkeypatch, provider):
    if provider == 'ray_core':
        pytest.importorskip('ray')
    allocations = []
    original = DataflowExecution._activate
    def activate(execution, lease, stage):
        result = original(execution, lease, stage)
        observation = execution._scheduler._observation(lease.allocation_id)
        allocations.append((dict(stage.profile.requests), observation['effective_resources']))
        return result
    monkeypatch.setattr(DataflowExecution, '_activate', activate)
    with _RayDataBlocker() as blocked, _provider(provider):
        with RunContext.start(tmp_path/'run', limits={'timeout_s': 60}) as run:
            mapped = data.from_items([{'k': 1}, {'k': 2}]).map(lambda row: row,
                execution='inline', backend_options={'ray': {'worker_resources': {'cpu': 2}}})
            result = mapped.join(data.from_items([{'k': 2, 'v': 'kept'}]), on='k').limit(1)
            assert result.take_all() == [{'k': 2, 'v': 'kept'}]
            assert result.run_stream().emitted == 1
            assert all(run.journal.execution_commits(action)[-1]['status'] == 'acked'
                       for action in ('action-1', 'action-2'))
        assert not blocked.attempts
    claimed = [(p, a) for p, a in allocations if p['cpu_millis'] >= 2000]
    assert claimed
    assert all(a['cpu_millis'] >= p['cpu_millis'] for p, a in claimed)


def test_stream_missing_gpu_is_resource_failure_before_source_or_constructor(tmp_path, monkeypatch):
    monkeypatch.delenv('DEMIFLOW_DEVICE_INVENTORY', raising=False)
    path = str(tmp_path/'calls.jsonl')
    class Actor:
        def __init__(self):
            emit(path, 'construct')
        def __call__(self, row):
            return row
    def source():
        emit(path, 'source')
        yield {'k': 1}
    with pytest.raises(DataflowError, match='GPU capacity'):
        with RunContext.start(tmp_path/'run'), data.local_execution():
            data.from_iter(source).map(Actor, execution='inline',
                backend_options={'ray': {'worker_resources': {'gpu': 1}}}).run_stream()
    assert events(path) == []


def test_closed_execution_refuses_lazy_source_but_keeps_published_result(tmp_path):
    path = str(tmp_path/'calls.jsonl')
    def source():
        emit(path, 'read')
        yield {'k': 1}
    with data.local_execution():
        lazy = data.from_iter(source).map(lambda row: row)
        cached = lazy.materialize()
    assert len(events(path)) == 1
    with pytest.raises(RuntimeError, match='closed'):
        lazy.take_all()
    assert len(events(path)) == 1
    assert cached.take_all() == [{'k': 1}]
    assert cached.map(lambda row: {'k': row['k']+1}).take_all() == [{'k': 2}]
    assert len(events(path)) == 1
    cached.release()
    with pytest.raises(RuntimeError, match='released'):
        cached.take_all()
