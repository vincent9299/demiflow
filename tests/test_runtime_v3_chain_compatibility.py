"""P1 materialization opens exactly one action on its configured v3 runtime."""
from contextlib import contextmanager
import os

import pytest

from demiflow.data.api import DataAPI
from demiflow.execution.contracts.dataflow_errors import DataflowError
from demiflow.execution.outputs.dataflow_handles import StoredMaterializedHandle
from demiflow.execution.seal.program_identity import plan_identity
from demiflow.execution.pipeline.run_context import RunContext


@contextmanager
def _entry(tmp_path):
    if os.environ.get('DEMIFLOW_V3_PROVIDER') == 'ray_core':
        from demiflow.execution.executors.ray import RayDatasetExecutor
        executor = RayDatasetExecutor(workers=2, resource_root=tmp_path)
    else:
        from demiflow.execution.executors.local import LocalDatasetExecutor
        executor = LocalDatasetExecutor(workers=2, resource_root=tmp_path)
    try:
        yield DataAPI(executor)
    finally:
        executor.close()


def _observe(monkeypatch):
    from demiflow.execution.action_driver import dataflow
    from demiflow.execution.executors import dataflow_api
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
                'max_row_bytes': 4096, 'max_batch_bytes': 16384}) as run, _entry(tmp_path) as api:
            ds = api.from_items([{'id': 1}, {'id': 2}]).map(lambda row: {'id': row['id'] + 1})
            with _cleanup_under_owner(observed):
                result = ds.materialize()._source.handle
                assert isinstance(result, StoredMaterializedHandle)
                assert result.row_count == 2
                assert list(result.rows()) == list(result.rows()) == [{'id': 2}, {'id': 3}]
                request, policy, bounds, handle = observed[0]
                assert len(observed) == run.position == 1
                assert request.action.action_id == 'action-1'
                assert policy.max_active_workers == 2
                assert bounds.max_resident_bytes <= 32 * 2**20 and bounds.max_row_bytes == 4096
                assert handle._closed and request.action.record['result'] is not None
                commits = run.journal.execution_commits(request.action.action_id)
                assert commits and all(item['status'] == 'acked' for item in commits)
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
                    'max_row_bytes': 128, 'max_batch_bytes': 2048}) as run, _entry(tmp_path) as api:
                ds = api.from_items([{'payload': 'x' * 4096}])
                with _cleanup_under_owner(observed):
                    result = ds.materialize()._source.handle
        assert not run.journal.execution_commits('action-1')
    finally:
        if isinstance(result, StoredMaterializedHandle):
            result.release()


def test_materialize_rejects_a_nested_action_before_starting_workers(tmp_path, monkeypatch):
    from demiflow.execution.contracts.recovery_contract import RecoveryContractError
    observed = _observe(monkeypatch)
    with pytest.raises(RecoveryContractError, match='Nested Dataset actions'):
        with RunContext.start(tmp_path / 'run') as run, _entry(tmp_path) as api:
            ds = api.from_items([{'id': 1}])
            with run.action(plan_identity('materialize', ds._source, ds._plan)):
                ds.materialize()
    assert observed == []
    assert not run.journal.execution_commits('action-1')
    with run.journal._connection() as db:
        # Rejection precedes even initialization of the resource owner.
        assert db.execute("SELECT name FROM sqlite_master WHERE type='table' "
                          "AND name='resource_allocations'").fetchall() == []
