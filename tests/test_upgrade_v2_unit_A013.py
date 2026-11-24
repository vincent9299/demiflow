"""A013 · Dataset.schema reads metadata only when fetch_if_missing=False; real read enters one action."""

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
    return data.from_items([{"id": 0, "name": "x"}]).map(lambda r: r)


def _stream():
    return data.from_items([{"id": 0}]).map(lambda r: r, execution="thread", concurrency=2)


def test_schema_true_single_action(monkeypatch):
    calls = _action_spy(monkeypatch)
    schema = _plain().schema()
    assert set(schema.names) == {"id", "name"}
    assert calls == ["schema"]


def test_schema_streaming_uses_one_real_action(monkeypatch):
    calls = _action_spy(monkeypatch)
    assert _stream().schema().names == ['id']
    assert calls == ['schema']
