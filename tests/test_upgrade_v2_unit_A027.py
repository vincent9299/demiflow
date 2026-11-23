"""A027 · Dataset.aggregate validates inputs before one checked.action; streaming uses the same result contract."""
import pytest

from demiflow import data
from demiflow.data.aggregate import Sum
from demiflow.errors import UnsupportedExecutionOptionError
from demiflow.execution.pipeline.run_context import RunContext


def _action_spy(monkeypatch):
    calls = []
    orig = RunContext.action

    def counting(self, declaration, **kw):
        calls.append(declaration.get("kind"))
        return orig(self, declaration, **kw)

    monkeypatch.setattr(RunContext, "action", counting)
    return calls


def _plain():
    return data.from_items([{"id": i} for i in range(5)]).map(lambda r: r)


def _stream():
    return data.from_items([{"id": i} for i in range(5)]).map(
        lambda r: r, execution="thread", concurrency=2)


def test_aggregate_single_action(monkeypatch):
    calls = _action_spy(monkeypatch)
    assert _plain().aggregate(Sum("id")) == {"sum(id)": 10}
    assert calls == ["aggregate"]


def test_aggregate_requires_aggregation_before_action(monkeypatch):
    calls = _action_spy(monkeypatch)
    with pytest.raises(ValueError, match="at least one aggregation"):
        _plain().aggregate()
    assert calls == []


def test_aggregate_rejects_non_aggregate_before_action(monkeypatch):
    calls = _action_spy(monkeypatch)
    with pytest.raises(TypeError, match="AggregateFnV2"):
        _plain().aggregate(object())
    assert calls == []


def test_aggregate_streaming_uses_one_real_action(monkeypatch):
    calls = _action_spy(monkeypatch)
    assert _stream().aggregate(Sum('id')) == {'sum(id)': 10}
    assert calls == ['aggregate']