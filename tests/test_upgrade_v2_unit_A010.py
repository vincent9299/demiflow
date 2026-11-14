"""A010 · Dataset.take_all opens one checked.action and never silently truncates."""
import pytest

from demiflow import data
from demiflow.execution.run_context import RunContext


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


def test_take_all_plain_single_action(monkeypatch):
    calls = _action_spy(monkeypatch)
    assert [r["id"] for r in _plain().take_all()] == [0, 1, 2, 3, 4]
    assert calls == ["take_all"]


def test_take_all_streaming_single_action(monkeypatch):
    calls = _action_spy(monkeypatch)
    assert sorted(r["id"] for r in _stream().take_all()) == [0, 1, 2, 3, 4]
    assert calls == ["take_all"]


def test_take_all_limit_exact_single_action(monkeypatch):
    calls = _action_spy(monkeypatch)
    assert [r["id"] for r in _plain().take_all(5)] == [0, 1, 2, 3, 4]
    assert calls == ["take_all"]


def test_take_all_overflow_raises(monkeypatch):
    _action_spy(monkeypatch)
    with pytest.raises(ValueError, match="more than the take_all limit"):
        _plain().take_all(2)


def test_take_all_negative_rejected_before_action(monkeypatch):
    calls = _action_spy(monkeypatch)
    with pytest.raises(ValueError, match="non-negative"):
        _plain().take_all(-1)
    assert calls == []