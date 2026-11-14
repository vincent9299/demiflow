"""A003 · Dataset.run_stream preserves the e517110 preflight and one action boundary."""
import pytest

from demiflow import data
from demiflow.execution.recovery_contract import RecoveryContractError
from demiflow.execution.run_context import RunContext


def _action_spy(monkeypatch):
    calls = []
    orig = RunContext.action

    def counting(self, declaration, **kw):
        calls.append(declaration.get("kind"))
        return orig(self, declaration, **kw)

    monkeypatch.setattr(RunContext, "action", counting)
    return calls


def _stream():
    return data.from_items([{"id": i} for i in range(4)]).map(
        lambda r: r, execution="inline", concurrency=1)


def _plain():
    return data.from_items([{"id": i} for i in range(4)]).map(lambda r: r)


def test_run_stream_single_action(monkeypatch):
    calls = _action_spy(monkeypatch)
    stats = _stream().run_stream()
    assert stats.emitted == 4
    assert calls == ["run_stream"]


def test_run_stream_preflight_rejects_plain_before_action(monkeypatch):
    calls = _action_spy(monkeypatch)
    with pytest.raises(ValueError, match="run_stream requires"):
        _plain().run_stream()
    assert calls == []


def test_run_stream_retired_checkpoint_rejected(monkeypatch):
    with pytest.raises(RecoveryContractError):
        _stream().run_stream(checkpoint=object())