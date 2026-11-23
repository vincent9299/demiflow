"""A011 · Dataset.iter_rows keeps one action open from first consumption to close/exhaust."""
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


def test_iter_rows_lazy_no_action_until_consumed(monkeypatch):
    calls = _action_spy(monkeypatch)
    it = _stream().iter_rows()
    assert calls == []
    assert sorted(r["id"] for r in it) == [0, 1, 2, 3, 4]
    assert calls == ["iter_rows"]


def test_iter_rows_plain_full_single_action(monkeypatch):
    calls = _action_spy(monkeypatch)
    assert [r["id"] for r in _plain().iter_rows()] == [0, 1, 2, 3, 4]
    assert calls == ["iter_rows"]


def test_iter_rows_early_close_single_action(monkeypatch):
    calls = _action_spy(monkeypatch)
    it = _stream().iter_rows()
    assert next(it)["id"] in range(5)
    it.close()
    assert calls == ["iter_rows"]


def test_iter_rows_partial_then_exhaust_single_action(monkeypatch):
    calls = _action_spy(monkeypatch)
    it = _plain().iter_rows()
    next(it)
    rest = list(it)
    assert len(rest) == 4
    assert calls == ["iter_rows"]