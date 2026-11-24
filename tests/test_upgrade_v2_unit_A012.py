"""A012 · Dataset.iter_batches keeps one action open across lazy batch consumption."""
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


def test_iter_batches_lazy_no_action_until_consumed(monkeypatch):
    calls = _action_spy(monkeypatch)
    it = _stream().iter_batches(batch_size=2, batch_format="pyarrow")
    assert calls == []
    total = sum(b.num_rows for b in it)
    assert total == 5
    assert calls == ["iter_batches"]


def test_iter_batches_plain_groups_single_action(monkeypatch):
    calls = _action_spy(monkeypatch)
    batches = list(_plain().iter_batches(batch_size=2, batch_format="pyarrow"))
    assert [b.num_rows for b in batches] == [2, 2, 1]
    assert calls == ["iter_batches"]


def test_iter_batches_early_close_single_action(monkeypatch):
    calls = _action_spy(monkeypatch)
    it = _stream().iter_batches(batch_size=2, batch_format="pyarrow")
    next(it)
    it.close()
    assert calls == ["iter_batches"]


def test_iter_batches_invalid_size_rejected_before_action(monkeypatch):
    calls = _action_spy(monkeypatch)
    with pytest.raises(ValueError, match="positive or None"):
        _plain().iter_batches(batch_size=0)
    assert calls == []