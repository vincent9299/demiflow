"""Public stream declarations compose through the same Local/Ray Core graph."""
from dataclasses import dataclass

import pytest
from demiflow import data
from demiflow.data.dataset import Dataset
from demiflow.data.plan import LogicalPlan, reject_streaming_map_options
from demiflow.errors import UnsupportedExecutionOptionError
from demiflow.execution.capabilities import validate_stream_plan
from demiflow.execution.contracts.dataflow_errors import DataflowError
from demiflow.execution.run_context import RunContext
from _worker_event_fixtures import emit, events
from runtime_v3_recovery_scenarios import _provider
from runtime_v3_support import _RayDataBlocker, _ExecuteActionRecorder


@pytest.mark.parametrize('provider', ['local', 'ray_core'])
def test_real_shared_and_nested_stream_input_join_limit_and_terminal(tmp_path, provider):
    if provider == 'ray_core':
        pytest.importorskip('ray')
    path = str(tmp_path / 'calls.jsonl')
    def transform(row):
        emit(path, 'call', k=row['k'])
        return {**row, 'value': row['k'] + 10}
    with _RayDataBlocker() as blocked, _provider(provider), _ExecuteActionRecorder() as calls:
        mapped = data.from_items([{'k': i} for i in range(3)]).map(transform, execution='inline')
        shared = mapped.join(mapped, on='k')
        nested = shared.join(data.from_items([{'k': 1, 'tag': 'ok'}]), on='k').limit(1)
        assert events(path) == []
        with RunContext.start(tmp_path / 'run', limits={'timeout_s': 60}) as run:
            assert nested.take_all() == [{'k': 1, 'value': 11, 'value_right': 11, 'tag': 'ok'}]
            first = events(path)
            assert [event['k'] for event in first] == [0, 1, 2]
            assert nested.run_stream().emitted == 1
            assert [event['k'] for event in events(path)[len(first):]] == [0, 1, 2]
            for action_id in ('action-1', 'action-2'):
                assert run.journal.execution_commits(action_id)[-1]['status'] == 'acked'
        assert [call['provider'] for call in calls.calls] == [provider, provider]
        assert [call['terminal_kind'] for call in calls.calls] == ['iter_rows', 'run_stream']
        assert not blocked.attempts


def test_stream_preflight_rejects_unknown_engine_kernel_and_options_before_business(tmp_path):
    path = str(tmp_path / 'side-effects.jsonl')
    class Actor:
        def __init__(self):
            emit(path, 'construct')
        def __call__(self, row):
            return row
    def source():
        emit(path, 'read')
        yield {'k': 1}
    plain = data.from_iter(source)
    mapped = plain.map(Actor, execution='inline')
    with pytest.raises(UnsupportedExecutionOptionError, match='no declared provider'):
        reject_streaming_map_options(mapped._plan, 'missing-engine')
    @dataclass(frozen=True)
    class UnsupportedOp:
        pass
    invalid = Dataset(mapped._source, LogicalPlan(mapped._plan.operations + (UnsupportedOp(),)), mapped._executor)
    with pytest.raises(DataflowError, match='operator kind is not registered'):
        invalid.run_stream()
    with pytest.raises(ValueError, match='explicit map'):
        plain.map(Actor).run_stream()
    invalid_options = plain.map(Actor, backend_options={'ray': {'compute': {'kind': 'task_pool', 'size': 2}}})
    invalid_nested = mapped.join(invalid_options, on='k')
    with pytest.raises(UnsupportedExecutionOptionError, match='backend_options'):
        invalid_nested.run_stream()
    assert events(path) == []


def test_stream_preflight_is_bounded_and_rejects_async_map_callables():
    mapped = data.from_items([]).map(lambda row: row, execution='inline')
    with pytest.raises(DataflowError, match='node budget'):
        validate_stream_plan(LogicalPlan(mapped._plan.operations * 4097), source=mapped._source)
    async def callback(row):
        return row
    with pytest.raises(TypeError, match='synchronous'):
        data.from_items([]).map(callback, execution='inline')


@pytest.mark.parametrize('provider', ['local', 'ray_core'])
def test_public_explain_separates_metadata_from_closed_runtime_policy(provider):
    from demiflow.execution.executors.local import LocalDatasetExecutor
    from demiflow.execution.executors.ray import RayDatasetExecutor
    from demiflow.execution.dataflow_api import explain_dataset
    from demiflow.data.api import DataAPI
    executor = LocalDatasetExecutor() if provider == 'local' else RayDatasetExecutor()
    def forbidden():
        raise AssertionError('explain must not read a source')
        yield
    try:
        plan = DataAPI(executor).from_iter(forbidden).map(lambda row: row, execution='inline')
        result = executor.plan(plan._source, plan._plan, 'take_all', source_parallelism_cap=2,
                               terminal_parallelism_cap=1)
        assert result.supported
        for options, code in (({'action_kind': 'missing'}, 'UNSUPPORTED_CAPABILITY'),
                              ({'terminal_category': 'missing'}, 'INVALID_CONTRACT'),
                              ({'terminal_category': []}, 'INVALID_CONTRACT'),
                              ({'source_parallelism_cap': True}, 'INVALID_CONTRACT'),
                              ({'terminal_parallelism_cap': 0}, 'INVALID_CONTRACT'),
                              ({'silent_typo': 1}, 'INVALID_CONTRACT'),
                              ({'terminal_native_options': {'compute': 'ray-data'}}, 'UNSUPPORTED_CAPABILITY')):
            with pytest.raises(DataflowError) as rejected:
                explain_dataset(plan._source, plan._plan, provider=provider, options=options)
            assert rejected.value.code == code
    finally:
        executor.close()
