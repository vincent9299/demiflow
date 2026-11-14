"""A007 · iter_rows routes streaming plans to delivery and plain plans to pull."""
import pytest

from demiflow import data
from demiflow.data.native_options import NativeOptions
from demiflow.data.plan import CallableSpec, MapOp
from demiflow.errors import UnsupportedExecutionOptionError
from demiflow.execution.executors.local import LocalDatasetExecutor


def _plain():
    return data.from_items([{"id": i} for i in range(4)]).map(lambda r: r)


def _stream(execution="thread"):
    return data.from_items([{"id": i} for i in range(4)]).map(
        lambda r: r, execution=execution, concurrency=2)


def test_plain_iter_rows_pulls_all():
    ex = LocalDatasetExecutor()
    ds = _plain()
    assert [r["id"] for r in ex.iter_rows(ds._source, ds._plan)] == [0, 1, 2, 3]


def test_thread_streaming_iter_rows_delivers():
    ex = LocalDatasetExecutor()
    ds = _stream("thread")
    assert [r["id"] for r in ex.iter_rows(ds._source, ds._plan)] == [0, 1, 2, 3]


def test_inline_streaming_iter_rows_delivers():
    ex = LocalDatasetExecutor()
    ds = _stream("inline")
    assert [r["id"] for r in ex.iter_rows(ds._source, ds._plan)] == [0, 1, 2, 3]


def test_iter_rows_rejects_ray_native_options_before_read(monkeypatch):
    ex = LocalDatasetExecutor()
    ds = _plain()
    plan = ds._plan.append(MapOp(CallableSpec.create(lambda r: r),
                                 native_options=NativeOptions(backend="ray")))

    def spy(self, source, **kwargs):
        raise AssertionError("source read attempted before admission")

    monkeypatch.setattr(LocalDatasetExecutor, "_iter_source", spy)
    with pytest.raises(UnsupportedExecutionOptionError, match="backend_options"):
        list(ex.iter_rows(ds._source, plan))