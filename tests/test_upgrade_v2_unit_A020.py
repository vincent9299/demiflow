"""A020 · aggregate admits before reads; streaming plans are rejected at the boundary."""
import pytest

from demiflow import data
from demiflow.data.native_options import NativeOptions
from demiflow.data.plan import CallableSpec, MapOp
from demiflow.errors import UnsupportedExecutionOptionError
from demiflow.execution.executors.local import LocalDatasetExecutor


def _plain():
    return data.from_items([{"id": i} for i in range(3)]).map(lambda r: r)


def _stream():
    return data.from_items([{"id": i} for i in range(3)]).map(
        lambda r: r, execution="thread", concurrency=2)


def _read_spy(monkeypatch):
    calls = []

    def spy(self, source, **kwargs):
        calls.append(source)
        raise AssertionError("source read attempted before admission")

    monkeypatch.setattr(LocalDatasetExecutor, "_iter_source", spy)
    return calls


def test_streaming_aggregate_rejected_before_read(monkeypatch):
    ex = LocalDatasetExecutor()
    ds = _stream()
    calls = _read_spy(monkeypatch)
    with pytest.raises(UnsupportedExecutionOptionError, match="streaming"):
        ex.aggregate(ds._source, ds._plan, ())
    assert calls == []


def test_ray_native_options_aggregate_rejected_before_read(monkeypatch):
    ex = LocalDatasetExecutor()
    ds = _plain()
    plan = ds._plan.append(MapOp(CallableSpec.create(lambda r: r),
                                 native_options=NativeOptions(backend="ray")))
    calls = _read_spy(monkeypatch)
    with pytest.raises(UnsupportedExecutionOptionError, match="backend_options"):
        ex.aggregate(ds._source, plan, ())
    assert calls == []


def test_plain_empty_source_aggregate_admits_and_returns_none():
    ex = LocalDatasetExecutor()
    ds = data.from_items([]).map(lambda r: r)
    assert ex.aggregate(ds._source, ds._plan, ()) is None