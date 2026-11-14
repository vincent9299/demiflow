"""A016 · _apply_plan runs the unbound contract; a sealed execution binding is L-gated."""
import pytest

from demiflow import data
from demiflow.execution.executors.local import LocalDatasetExecutor
from demiflow.execution.plan_interpreter import PlanContractError


def _plain():
    return data.from_items([{"id": i} for i in range(3)]).map(lambda r: r)


def test_unbound_execution_runs_pull():
    ex = LocalDatasetExecutor()
    ds = _plain()
    rows = list(ex._apply_plan(ds._source, ds._plan, action_kind="iter_rows"))
    assert [r["id"] for r in rows] == [0, 1, 2]


def test_bound_execution_rejected_with_plan_contract_error():
    ex = LocalDatasetExecutor()
    ds = _plain()
    with pytest.raises(PlanContractError, match="sealed execution binding") as exc:
        list(ex._apply_plan(ds._source, ds._plan, execution=object()))
    assert exc.value.reason == "invalid_contract"


def test_bound_execution_rejected_before_read(monkeypatch):
    ex = LocalDatasetExecutor()
    ds = _plain()

    def spy(self, source, **kwargs):
        raise AssertionError("source read attempted for a sealed binding")

    monkeypatch.setattr(LocalDatasetExecutor, "_iter_source", spy)
    with pytest.raises(PlanContractError):
        list(ex._apply_plan(ds._source, ds._plan, execution=object()))