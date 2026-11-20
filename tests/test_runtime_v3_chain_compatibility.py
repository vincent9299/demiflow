"""Legacy chain entry borrows the actual action and its bounded v3 runtime."""
from contextlib import contextmanager
import os

import pytest

from demiflow.data.api import DataAPI
from demiflow.execution.contracts.dataflow_errors import DataflowError
from demiflow.execution.dataflow_handles import StoredMaterializedHandle
from demiflow.execution.recovery_identity import plan_identity
from demiflow.execution.run_context import RunContext


@contextmanager
def _entry(tmp_path):
    from demiflow.execution import chain
    if os.environ.get('DEMIFLOW_V3_PROVIDER') == 'ray_core':
        from demiflow.execution.executors.ray import RayDatasetExecutor
        executor, entry = RayDatasetExecutor(workers=2, resource_root=tmp_path), chain.run_ray_chain
    else:
        from demiflow.execution.executors.local import LocalDatasetExecutor
        executor, entry = LocalDatasetExecutor(workers=2, resource_root=tmp_path), chain.run_local_chain
    try:
        yield DataAPI(executor), executor, entry
    finally:
        executor.close()


def _observe(monkeypatch):
    from demiflow.execution.action_driver import dataflow
    from demiflow.execution import dataflow_api
    original = dataflow_api.execute_action
    observed = []
    def execute(request, *, policy, bounds):
        handle = original(request, policy=policy, bounds=bounds)
        observed.append((request, policy, bounds, handle))
        return handle
    monkeypatch.setattr(dataflow, 'execute_action', execute)
    # The facade may already be imported by an earlier suite in the same pytest
    # process. Observe its actual binding as well, not only a future import.
    monkeypatch.setattr(dataflow_api, 'execute_action', execute)
    return observed


@contextmanager
def _cleanup_under_owner(observed):
    try:
        yield
    finally:
        for _, _, _, handle in observed:
            handle.close()


def test_chain_returns_repeatable_materialization_and_closes_its_runtime(tmp_path, monkeypatch):
    observed = _observe(monkeypatch)
    result = None
    try:
        with RunContext.start(tmp_path / 'run', limits={'memory_bytes': 32 * 2**20,
                'max_row_bytes': 4096, 'max_batch_bytes': 16384}) as run, _entry(tmp_path) as (api, executor, entry):
            ds = api.from_items([{'id': 1}, {'id': 2}]).map(lambda row: {'id': row['id'] + 1})
            with run.action(plan_identity('materialize', ds._source, ds._plan)) as action, _cleanup_under_owner(observed):
                result = entry(executor, ds)
                assert isinstance(result, StoredMaterializedHandle)
                assert result.row_count == 2
                assert list(result.rows()) == list(result.rows()) == [{'id': 2}, {'id': 3}]
                request, policy, bounds, handle = observed[0]
                assert request.action is action and len(observed) == 1
                assert policy.max_active_workers == 2
                assert bounds.max_resident_bytes <= 32 * 2**20 and bounds.max_row_bytes == 4096
                assert handle._closed
                assert action.record['result'] is None
                commits = run.journal.execution_commits(action.action_id)
                assert commits and all(item['status'] == 'acked' for item in commits)
                action.complete({'kind': 'materialized', 'rows': result.row_count})
    finally:
        # Red-baseline cleanup is explicit; it cannot make the earlier lifecycle
        # assertion pass. Never leave an old leaking wrapper's worker behind.
        if isinstance(result, StoredMaterializedHandle):
            result.release()


def test_chain_keeps_the_original_row_ceiling(tmp_path, monkeypatch):
    observed = _observe(monkeypatch)
    result = None
    run = None
    try:
        with pytest.raises(DataflowError, match='BUDGET_EXCEEDED'):
            with RunContext.start(tmp_path / 'run', limits={'memory_bytes': 32 * 2**20,
                    'max_row_bytes': 128, 'max_batch_bytes': 2048}) as run, _entry(tmp_path) as (api, executor, entry):
                ds = api.from_items([{'payload': 'x' * 4096}])
                with run.action(plan_identity('materialize', ds._source, ds._plan)) as action, _cleanup_under_owner(observed):
                    result = entry(executor, ds)
                    action.complete({'kind': 'unexpected-unbounded-success'})
        assert not run.journal.execution_commits('action-1')
    finally:
        if isinstance(result, StoredMaterializedHandle):
            result.release()


@pytest.mark.parametrize('mismatch', ['source', 'plan', 'provider'])
def test_chain_rejects_a_foreign_binding_before_starting_workers(tmp_path, monkeypatch, mismatch):
    from demiflow.execution import chain
    observed = _observe(monkeypatch)
    with pytest.raises(DataflowError, match='INVALID_CONTRACT'):
        with RunContext.start(tmp_path / 'run') as run, _entry(tmp_path) as (api, executor, entry):
            declared = api.from_items([{'id': 1}])
            candidate = (api.from_items([{'id': 2}]) if mismatch == 'source' else
                         declared.map(lambda row: {'id': row['id'] + 1}) if mismatch == 'plan' else declared)
            if mismatch == 'provider':
                entry = chain.run_local_chain if entry is chain.run_ray_chain else chain.run_ray_chain
            with run.action(plan_identity('materialize', declared._source, declared._plan)):
                entry(executor, candidate)
    assert observed == []
    assert not run.journal.execution_commits('action-1')
    with run.journal._connection() as db:
        # Rejection precedes even initialization of the resource owner.
        assert db.execute("SELECT name FROM sqlite_master WHERE type='table' "
                          "AND name='resource_allocations'").fetchall() == []
