"""A016 · the retained adapter delegates to v3 and rejects invalid sealed bounds."""
import pytest

from demiflow import data
from demiflow.execution.dataset_native import execute
from demiflow.execution.executors.local import LocalDatasetExecutor
from demiflow.execution.recovery_contract import RecoveryContractError
from _runtime_v3_executor_scope import observed_source


def _plain():
    return data.from_items([{'id': i} for i in range(3)]).map(lambda row: row)


def test_unbound_execution_uses_unified_rows():
    ds = _plain()
    assert list(execute(LocalDatasetExecutor(), ds._source, ds._plan)) == [
        {'id': 0}, {'id': 1}, {'id': 2}]


def test_invalid_bound_execution_is_refused():
    ds = _plain()
    with pytest.raises(RecoveryContractError, match='sealed resource profile'):
        list(execute(LocalDatasetExecutor(), ds._source, ds._plan, execution=object()))


def test_invalid_bound_execution_rejected_before_real_source_read(tmp_path):
    ds = _plain()
    source, marker = observed_source(tmp_path)
    with pytest.raises(RecoveryContractError, match='sealed resource profile'):
        list(execute(LocalDatasetExecutor(), source, ds._plan, execution=object()))
    assert not marker.exists()
