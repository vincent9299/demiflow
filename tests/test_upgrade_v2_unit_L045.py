"""L045: the remaining Engine row adapter uses the one native v3 entry.

The retired _apply_plan hook no longer exists. A recording wrapper delegates
unchanged to real execute; the original allocator and original action journal
prove that the borrowed placement actually constrains execution.
"""
from contextlib import ExitStack
from types import SimpleNamespace

import pytest

from demiflow import data
from demiflow.execution import dataset_native
from demiflow.execution.action_driver.dataflow import DataflowExecution
from demiflow.execution.dataset_native import Engine
from demiflow.execution.resource_state import RunResourceState
from demiflow.execution.run_context import RunContext

MiB = 2 ** 20


def _placement():
    return SimpleNamespace(
        execution_id='exec-l045', placement='driver', group_id=None,
        backend='datafusion', stage_ids=('python_substage',),
        profile={'limits': {'cpu_millis': 2000, 'memory_bytes': 32 * MiB}}, programs=(),
        stage_limits={'python_substage': {'inflight_rows': 2, 'inflight_bytes': MiB}},
        recovery='final_commit')


@pytest.fixture
def native_evidence(monkeypatch):
    calls, physicals, charges = [], [], []
    execute = dataset_native.execute
    prepare = DataflowExecution.prepare
    charged = RunResourceState._charged

    def record(executor, source, plan, **kwargs):
        calls.append(kwargs)
        yield from execute(executor, source, plan, **kwargs)

    def prepared(execution):
        result = prepare(execution)
        physicals.append(execution._physical)
        return result

    def charge(state, db, scope):
        result = charged(state, db, scope)
        charges.append(dict(result))
        return result

    monkeypatch.setattr(dataset_native, 'execute', record)
    monkeypatch.setattr(DataflowExecution, 'prepare', prepared)
    monkeypatch.setattr(RunResourceState, '_charged', charge)
    return calls, physicals, charges


def _engine(executor, tmp_path, stats, execution):
    return Engine(executor, ExitStack(), str(tmp_path), stats, execution=execution)


def test_python_rows_forwards_bound_execution_and_real_budget(tmp_path, native_evidence):
    calls, physicals, charges = native_evidence
    sample = _placement()
    with RunContext.start(tmp_path / 'run') as run, data.local_execution(
            workers=2, worker_mode='thread', temp_directory=str(tmp_path)) as session:
        executor = session._executor
        ds = data.from_items([{'x': 1}, {'x': 2}, {'x': 3}]).map(lambda r: {'x': r['x'] * 10})
        stats = {'engine': 'datafusion', 'stages': []}
        rows = list(_engine(executor, tmp_path, stats, sample).python_rows(
            executor, ds._source, ds._plan))
        assert run.position == 1 and run.journal.execution_action('action-2') is None
        assert run.journal.execution_commits('action-1')[-1]['status'] == 'acked'
    assert rows == [{'x': 10}, {'x': 20}, {'x': 30}]
    assert len(calls) == 1 and calls[0]['execution'] is sample
    assert physicals and physicals[0].bounds.max_resident_bytes == 32 * MiB
    assert all(edge.credit_rows <= 2 and edge.credit_bytes <= MiB
               for edge in physicals[0].exchanges)
    assert charges and 1000 <= max(c['cpu_millis'] for c in charges) <= 2000
    assert all(c['memory_bytes'] <= 32 * MiB for c in charges)
    assert 'python_stages' in stats and executor._local_kernel_stats is stats


def test_python_rows_unbound_forwards_none(tmp_path, native_evidence):
    calls, _, _ = native_evidence
    with data.local_execution(workers=2, worker_mode='thread', temp_directory=str(tmp_path)) as session:
        executor = session._executor
        ds = data.from_items([{'x': 5}]).map(lambda r: {'x': r['x'] + 1})
        stats = {'engine': 'datafusion', 'stages': []}
        rows = list(_engine(executor, tmp_path, stats, None).python_rows(
            executor, ds._source, ds._plan))
    assert rows == [{'x': 6}]
    assert len(calls) == 1 and calls[0]['execution'] is None
    assert executor._local_kernel_stats is stats


def test_python_rows_preserves_finally_on_callback_error(tmp_path, native_evidence):
    def boom(row):
        raise ValueError('deliberate python substage failure')

    calls, physicals, charges = native_evidence
    sample = _placement()
    with pytest.raises(ValueError, match='deliberate python substage failure'):
        with RunContext.start(tmp_path / 'failed-run') as run, data.local_execution(
                workers=2, worker_mode='thread', temp_directory=str(tmp_path)) as session:
            executor = session._executor
            ds = data.from_items([{'x': 1}]).map(boom)
            stats = {'engine': 'datafusion', 'stages': []}
            try:
                list(_engine(executor, tmp_path, stats, sample).python_rows(
                    executor, ds._source, ds._plan))
            finally:
                assert calls[0]['execution'] is sample
                assert executor._local_kernel_stats is stats
                assert run.journal.execution_action('action-1')['result'] is None
                commits = run.journal.execution_commits('action-1')
                assert all(not c['prepared']['sinks']['iter_rows']['manifest']['final']
                           for c in commits if c['status'] == 'acked')
    assert physicals and physicals[0].bounds.max_resident_bytes == 32 * MiB
    assert charges and all(c['cpu_millis'] <= 2000 and c['memory_bytes'] <= 32 * MiB
                           for c in charges)
