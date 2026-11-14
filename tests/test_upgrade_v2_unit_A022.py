"""A022 · size_bytes admits before reads; streaming plans are rejected at the boundary."""
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


def test_plain_size_bytes_positive():
    ex = LocalDatasetExecutor()
    ds = _plain()
    assert ex.size_bytes(ds._source, ds._plan) > 0


def test_streaming_size_bytes_rejected_before_read(monkeypatch):
    ex = LocalDatasetExecutor()
    ds = _stream()
    calls = _read_spy(monkeypatch)
    with pytest.raises(UnsupportedExecutionOptionError, match="streaming"):
        ex.size_bytes(ds._source, ds._plan)
    assert calls == []


def test_ray_native_options_size_bytes_rejected_before_read(monkeypatch):
    ex = LocalDatasetExecutor()
    ds = _plain()
    plan = ds._plan.append(MapOp(CallableSpec.create(lambda r: r),
                                 native_options=NativeOptions(backend="ray")))
    calls = _read_spy(monkeypatch)
    with pytest.raises(UnsupportedExecutionOptionError, match="backend_options"):
        ex.size_bytes(ds._source, plan)
    assert calls == []