"""A006 · take admits before reads, honors limit==0, and keeps streaming delivery."""
import pytest

from demiflow import data
from demiflow.data.native_options import NativeOptions
from demiflow.data.plan import CallableSpec, MapOp
from demiflow.execution.contracts.dataflow_errors import DataflowError
from demiflow.execution.executors.local import LocalDatasetExecutor
from _runtime_v3_executor_scope import call


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
    assert call(ex, 'take', ds._source, ds._plan, 0) == []
    assert calls == []


def test_plain_take_pulls_prefix():
    ex = LocalDatasetExecutor()
    ds = _plain()
    assert [r["id"] for r in call(ex, 'take', ds._source, ds._plan, 2)] == [0, 1]


def test_thread_streaming_take_delivers():
    ex = LocalDatasetExecutor()
    ds = _stream("thread")
    assert [r["id"] for r in call(ex, 'take', ds._source, ds._plan, 3)] == [0, 1, 2]


def test_inline_streaming_take_delivers():
    ex = LocalDatasetExecutor()
    ds = _stream("inline")
    assert [r["id"] for r in call(ex, 'take', ds._source, ds._plan, 4)] == [0, 1, 2, 3]


def test_take_rejects_invalid_native_resources_before_read(monkeypatch):
    ex = LocalDatasetExecutor()
    ds = _plain()
    plan = ds._plan.append(MapOp(CallableSpec.create(lambda r: r),
                                 native_options=NativeOptions(backend="ray", worker_resources={"cpu": 0})))
    calls = _read_spy(monkeypatch)
    with pytest.raises(DataflowError, match="positive CPU"):
        call(ex, 'take', ds._source, plan, 2)
    assert calls == []


def test_take_accepts_resource_only_backend_spelling_on_local():
    ex = LocalDatasetExecutor()
    ds = _plain()
    plan = ds._plan.append(MapOp(CallableSpec.create(lambda r: r),
        native_options=NativeOptions(backend='ray', worker_resources={})))
    assert [row['id'] for row in call(ex, 'take', ds._source, plan, 2)] == [0, 1]
    assert ex.NAME == 'local'
