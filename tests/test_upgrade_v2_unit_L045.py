"""L045 independent test: Engine.python_rows forwards the bound execution into
the _native=False _apply_plan call (the 10th call site) and preserves the
original stats merge + finally.

Callee LocalDatasetExecutor._apply_plan gains `execution=None` in A016 (A-group,
not in this isolated worktree). Per the card we code against that stable
signature: a recording adapter accepts execution and delegates to the REAL
baseline wrapper, so interpreter/journal/commit run for real (not mocked). The
adapter only verifies L045's forwarding responsibility; A016's bound validation
and the combination gate run in the integration tree.
"""
from contextlib import ExitStack
from types import SimpleNamespace

import pytest

from demiflow import data
from demiflow.execution.dataset_native import Engine
from demiflow.execution.executors.local import LocalDatasetExecutor


def _placement():
    return SimpleNamespace(
        execution_id='exec-l045', placement='driver', group_id=None,
        backend='datafusion', stage_ids=('python_substage',),
        profile={'limits': {'memory_bytes': 1 << 30}}, programs=(),
        stage_limits={'python_substage': {'inflight_rows': 64}}, recovery='final_commit')


@pytest.fixture
def apply_plan_spy(monkeypatch):
    """A016 stable-signature adapter recording kwargs, delegating to real baseline."""
    original = LocalDatasetExecutor._apply_plan
    calls = []

    def adapter(self, source, plan, *, action_kind='iter_rows', terminal_category='action',
                terminal_native_options=None, physical_plan=None, _native=True, execution=None):
        calls.append({'_native': _native, 'execution': execution})
        return original(self, source, plan, action_kind=action_kind,
                        terminal_category=terminal_category,
                        terminal_native_options=terminal_native_options,
                        physical_plan=physical_plan, _native=_native)

    monkeypatch.setattr(LocalDatasetExecutor, '_apply_plan', adapter)
    return calls


def _engine(executor, tmp_path, stats, execution):
    return Engine(executor, ExitStack(), str(tmp_path), stats, execution=execution)


def test_python_rows_forwards_bound_execution_and_native_false(tmp_path, apply_plan_spy):
    sample = _placement()
    with data.local_execution(workers=2, worker_mode='thread', temp_directory=str(tmp_path)) as session:
        executor = session._executor
        ds = data.from_items([{'x': 1}, {'x': 2}, {'x': 3}]).map(lambda r: {'x': r['x'] * 10})
        stats = {'engine': 'datafusion', 'stages': []}
        engine = _engine(executor, tmp_path, stats, sample)
        rows = list(engine.python_rows(executor, ds._source, ds._plan))
    assert rows == [{'x': 10}, {'x': 20}, {'x': 30}]
    assert len(apply_plan_spy) == 1
    assert apply_plan_spy[0]['_native'] is False
    # The exact bound placement is forwarded, not re-picked or dropped.
    assert apply_plan_spy[0]['execution'] is sample
    # Original finally stats merge preserved: child stages folded into engine stats.
    assert 'python_stages' in stats
    assert executor._local_kernel_stats is stats


def test_python_rows_unbound_forwards_none(tmp_path, apply_plan_spy):
    with data.local_execution(workers=2, worker_mode='thread', temp_directory=str(tmp_path)) as session:
        executor = session._executor
        ds = data.from_items([{'x': 5}]).map(lambda r: {'x': r['x'] + 1})
        stats = {'engine': 'datafusion', 'stages': []}
        engine = _engine(executor, tmp_path, stats, None)
        rows = list(engine.python_rows(executor, ds._source, ds._plan))
    assert rows == [{'x': 6}]
    assert apply_plan_spy[0]['execution'] is None
    assert apply_plan_spy[0]['_native'] is False


def test_python_rows_preserves_finally_on_callback_error(tmp_path, apply_plan_spy):
    def boom(row):
        raise ValueError('deliberate python substage failure')

    sample = _placement()
    with data.local_execution(workers=2, worker_mode='thread', temp_directory=str(tmp_path)) as session:
        executor = session._executor
        ds = data.from_items([{'x': 1}]).map(boom)
        stats = {'engine': 'datafusion', 'stages': []}
        engine = _engine(executor, tmp_path, stats, sample)
        with pytest.raises(ValueError, match='deliberate python substage failure'):
            list(engine.python_rows(executor, ds._source, ds._plan))
        # finally still ran: the bound execution was forwarded and stats authority restored.
        assert apply_plan_spy[0]['execution'] is sample
        assert executor._local_kernel_stats is stats