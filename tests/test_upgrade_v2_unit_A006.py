"""A006 · take admits before reads, honors limit==0, and keeps streaming delivery."""
import pytest

from demiflow import data
from demiflow.data.native_options import NativeOptions
from demiflow.data.plan import CallableSpec, MapOp
from demiflow.errors import UnsupportedExecutionOptionError
from demiflow.execution.executors.local import LocalDatasetExecutor
from demiflow.execution.run_context import RunContext


@pytest.fixture(autouse=True)
def execution_scope(tmp_path):
    # Direct executor adapters require the same original run authority normally
    # supplied by Dataset's public terminal. No alternative engine is restored.
    with RunContext.start(tmp_path / 'execution'):
        yield


def _plain():
    return data.from_items([{"id": i} for i in range(5)]).map(lambda r: r)


def _stream(execution="thread"):
    return data.from_items([{"id": i} for i in range(5)]).map(
        lambda r: r, execution=execution, concurrency=2)


def _read_spy(monkeypatch):
    calls = []

    def spy(self, source, **kwargs):
        calls.append(source)
        raise AssertionError("source read attempted before admission")

    monkeypatch.setattr(LocalDatasetExecutor, "_iter_source", spy)
    return calls


def test_take_zero_returns_empty_without_read(monkeypatch):
    ex = LocalDatasetExecutor()
    ds = _plain()
    calls = _read_spy(monkeypatch)
    assert ex.take(ds._source, ds._plan, 0) == []
    assert calls == []


def test_plain_take_pulls_prefix():
    ex = LocalDatasetExecutor()
    ds = _plain()
    assert [r["id"] for r in ex.take(ds._source, ds._plan, 2)] == [0, 1]


def test_thread_streaming_take_delivers():
    ex = LocalDatasetExecutor()
    ds = _stream("thread")
    assert [r["id"] for r in ex.take(ds._source, ds._plan, 3)] == [0, 1, 2]


def test_inline_streaming_take_delivers():
    ex = LocalDatasetExecutor()
    ds = _stream("inline")
    assert [r["id"] for r in ex.take(ds._source, ds._plan, 4)] == [0, 1, 2, 3]


def test_take_rejects_ray_native_options_before_read(monkeypatch):
    ex = LocalDatasetExecutor()
    ds = _plain()
    plan = ds._plan.append(MapOp(CallableSpec.create(lambda r: r),
                                 native_options=NativeOptions(backend="ray")))
    calls = _read_spy(monkeypatch)
    with pytest.raises(UnsupportedExecutionOptionError, match="backend_options"):
        ex.take(ds._source, plan, 2)
    assert calls == []