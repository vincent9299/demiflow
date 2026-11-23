"""A009 · Dataset.take opens one checked.action; take(0) does not read the source."""
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


def test_take_plain_single_action(monkeypatch):
    calls = _action_spy(monkeypatch)
    assert [r["id"] for r in _plain().take(2)] == [0, 1]
    assert calls == ["take"]


def test_take_streaming_single_action(monkeypatch):
    calls = _action_spy(monkeypatch)
    assert [r["id"] for r in _stream().take(3)] == [0, 1, 2]
    assert calls == ["take"]


def test_take_zero_no_source_read(monkeypatch, tmp_path):
    calls = _action_spy(monkeypatch)
    marker = tmp_path / 'source-read'
    def rows():
        marker.write_text('read')
        yield {'id': 1}
    assert data.from_iter(rows).map(lambda row: row).take(0) == []
    assert not marker.exists()
    assert calls == ['take']


def test_take_negative_rejected_before_action(monkeypatch):
    calls = _action_spy(monkeypatch)
    with pytest.raises(ValueError, match="non-negative"):
        _plain().take(-1)
    assert calls == []


def test_take_resource_only_backend_options_use_same_action(monkeypatch, tmp_path):
    calls = _action_spy(monkeypatch)
    with RunContext.start(tmp_path / 'run') as run:
        ds = data.from_items([{'id': 0}]).map(lambda row: row, backend_options={'ray': {}})
        assert ds.take(1) == [{'id': 0}]
        assert calls == ['take']
        assert run.journal.source_binding('action-1-dataflow-sealed') is not None
        assert run.journal.execution_commits('action-1')[-1]['status'] == 'acked'


def test_take_retired_pool_options_rejected_before_source_or_user_call(monkeypatch, tmp_path):
    calls = _action_spy(monkeypatch)
    marker, called = tmp_path / 'source-read', tmp_path / 'udf-called'
    def rows():
        marker.write_text('read')
        yield {'id': 0}
    def transform(row):
        called.write_text('called')
        return row
    with pytest.raises(ValueError, match='Ray Data compute/worker ranges'):
        with RunContext.start(tmp_path / 'run') as run:
            ds = data.from_iter(rows).map(transform, backend_options={'ray': {
                'compute': {'kind': 'actor_pool', 'min_size': 2, 'initial_size': 2, 'max_size': 3}}})
            ds.take(1)
    assert not marker.exists() and not called.exists()
    assert calls == ['take']
    assert not run.journal.execution_commits('action-1')
    with run.journal._connection() as connection:
        assert connection.execute("SELECT name FROM sqlite_master WHERE name='resource_allocations'").fetchall() == []
