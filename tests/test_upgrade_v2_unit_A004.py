"""A004 · Dataset.count keeps its single action boundary; streaming counts via borrowed delivery."""
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
    return data.from_items([{"id": i} for i in range(6)]).map(lambda r: r)


def _stream():
    return data.from_items([{"id": i} for i in range(6)]).map(
        lambda r: r, execution="thread", concurrency=2)


def test_count_plain_single_action(monkeypatch):
    calls = _action_spy(monkeypatch)
    assert _plain().count() == 6
    assert calls == ["count"]


def test_count_streaming_single_action(monkeypatch):
    calls = _action_spy(monkeypatch)
    assert _stream().count() == 6
    assert calls == ["count"]
