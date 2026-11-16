"""A028 · Dataset.size_bytes opens one checked.action; streaming uses the same size semantics."""
import pytest

from demiflow import data
from demiflow.errors import UnsupportedExecutionOptionError
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


def test_size_bytes_single_action(monkeypatch):
    calls = _action_spy(monkeypatch)
    assert _plain().size_bytes() > 0
    assert calls == ["size_bytes"]


def test_size_bytes_streaming_matches_same_plain_rows(monkeypatch):
    expected = _plain().size_bytes()
    calls = _action_spy(monkeypatch)
    assert _stream().size_bytes() == expected and expected > 0
    assert calls == ['size_bytes']