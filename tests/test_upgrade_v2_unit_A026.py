"""A026 · Dataset.take_batch opens one checked.action and shapes a bounded batch."""
import pytest

from demiflow import data
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


def test_take_batch_plain_single_action(monkeypatch):
    calls = _action_spy(monkeypatch)
    batch = _plain().take_batch(3, batch_format="pyarrow")
    assert [r["id"] for r in batch.to_pylist()] == [0, 1, 2]
    assert calls == ["take_batch"]


def test_take_batch_streaming_single_action(monkeypatch):
    calls = _action_spy(monkeypatch)
    batch = _stream().take_batch(2, batch_format="pyarrow")
    assert sorted(r["id"] for r in batch.to_pylist()) == [0, 1]
    assert calls == ["take_batch"]


def test_take_batch_non_positive_rejected_before_action(monkeypatch):
    calls = _action_spy(monkeypatch)
    with pytest.raises(ValueError, match="positive"):
        _plain().take_batch(0)
    assert calls == []