"""A009 · Dataset.take opens one checked.action; take(0) does not read the source."""
import pytest

from demiflow import data
from demiflow.errors import UnsupportedExecutionOptionError
from demiflow.execution.executors.local import LocalDatasetExecutor
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


def test_take_plain_single_action(monkeypatch):
    calls = _action_spy(monkeypatch)
    assert [r["id"] for r in _plain().take(2)] == [0, 1]
    assert calls == ["take"]


def test_take_streaming_single_action(monkeypatch):
    calls = _action_spy(monkeypatch)
    assert [r["id"] for r in _stream().take(3)] == [0, 1, 2]
    assert calls == ["take"]


def test_take_zero_no_source_read(monkeypatch):
    calls = _action_spy(monkeypatch)
    reads = []

    def spy(self, source, **kwargs):
        reads.append(source)
        raise AssertionError("take(0) must not read the source")

    monkeypatch.setattr(LocalDatasetExecutor, "_iter_source", spy)
    assert _plain().take(0) == []
    assert reads == []
    assert calls == ["take"]


def test_take_negative_rejected_before_action(monkeypatch):
    calls = _action_spy(monkeypatch)
    with pytest.raises(ValueError, match="non-negative"):
        _plain().take(-1)
    assert calls == []


def test_take_ray_options_rejected(monkeypatch):
    ds = data.from_items([{"id": 0}]).map(lambda r: r, backend_options={"ray": {}})
    with pytest.raises(UnsupportedExecutionOptionError, match="backend_options"):
        ds.take(1)