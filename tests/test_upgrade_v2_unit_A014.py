"""A014 · iter_stream_rows borrows an outer action; only owner completes."""
import pytest

from demiflow import data
from demiflow.data.plan import CallableSpec, FilterOp
from demiflow.execution.recovery_contract import RecoveryContractError
from demiflow.execution.recovery_identity import plan_identity
from demiflow.execution.run_context import RunContext, current_action
from demiflow.execution.stream_delivery import iter_stream_rows


def _stream_ds():
    return data.from_items([{"id": i} for i in range(5)]).map(
        lambda r: r, execution="inline", concurrency=1)


def test_self_created_delivery_delivers_rows(tmp_path):
    ds = _stream_ds()
    with RunContext.start(tmp_path / "run") as run:
        rows = list(iter_stream_rows(ds._executor, ds._source, ds._plan, limit=5))
        assert [r["id"] for r in rows] == [0, 1, 2, 3, 4]
        assert current_action() is None


def test_zero_limit_yields_nothing(tmp_path):
    ds = _stream_ds()
    with RunContext.start(tmp_path / "run"):
        assert list(iter_stream_rows(ds._executor, ds._source, ds._plan, limit=0)) == []


def test_borrow_explicit_action_does_not_complete(tmp_path):
    ds = _stream_ds()
    with RunContext.start(tmp_path / "run") as run:
        declaration = plan_identity("take", ds._source, ds._plan)
        with run.action(declaration) as outer:
            rows = list(iter_stream_rows(ds._executor, ds._source, ds._plan,
                                         limit=5, action=outer))
            assert [r["id"] for r in rows] == [0, 1, 2, 3, 4]
            assert outer.record["result"] is None
            outer.complete({"kind": "take", "delivery": "completed"})
        assert outer.record["result"] is not None


def test_borrow_via_current_action(tmp_path):
    ds = _stream_ds()
    with RunContext.start(tmp_path / "run") as run:
        with run.action(plan_identity("take", ds._source, ds._plan)) as outer:
            assert current_action() is outer
            rows = list(iter_stream_rows(ds._executor, ds._source, ds._plan, limit=3))
            assert [r["id"] for r in rows] == [0, 1, 2]
            assert outer.record["result"] is None
            outer.complete({"kind": "take", "delivery": "completed"})


def test_borrow_rejects_foreign_source(tmp_path):
    ds = _stream_ds()
    foreign = data.from_items([{"x": 1}])._source
    with RunContext.start(tmp_path / "run") as run:
        with run.action(plan_identity("take", ds._source, ds._plan)) as outer:
            with pytest.raises(RecoveryContractError, match="source differs"):
                list(iter_stream_rows(ds._executor, foreign, ds._plan, action=outer))
            outer.complete({"kind": "take", "delivery": "completed"})


def test_borrow_rejects_foreign_plan(tmp_path):
    ds = _stream_ds()
    foreign_plan = ds._plan.append(FilterOp(CallableSpec.create(lambda r: True)))
    with RunContext.start(tmp_path / "run") as run:
        with run.action(plan_identity("take", ds._source, ds._plan)) as outer:
            with pytest.raises(RecoveryContractError, match="plan differs"):
                list(iter_stream_rows(ds._executor, ds._source, foreign_plan, action=outer))
            outer.complete({"kind": "take", "delivery": "completed"})


def test_borrow_rejects_foreign_run(tmp_path):
    ds = _stream_ds()
    with RunContext.start(tmp_path / "r1") as run1:
        with run1.action(plan_identity("take", ds._source, ds._plan)) as outer:
            outer.complete({"kind": "take", "delivery": "completed"})
    with RunContext.start(tmp_path / "r2"):
        with pytest.raises(RecoveryContractError, match="different run"):
            list(iter_stream_rows(ds._executor, ds._source, ds._plan, action=outer))


def test_invalid_limit_rejected(tmp_path):
    ds = _stream_ds()
    with RunContext.start(tmp_path / "run"):
        with pytest.raises(ValueError, match="nonnegative"):
            list(iter_stream_rows(ds._executor, ds._source, ds._plan, limit=-1))