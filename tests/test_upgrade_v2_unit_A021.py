"""A021 · schema short-circuits metadata, admits before reads, rejects streaming."""
import pytest

from demiflow import data
from demiflow.data.native_options import NativeOptions
from demiflow.data.plan import CallableSpec, MapOp
from demiflow.errors import UnsupportedExecutionOptionError
from demiflow.execution.executors.local import LocalDatasetExecutor


def _plain():
    return data.from_items([{"id": 0, "name": "x"}]).map(lambda r: r)


def _stream():
    return data.from_items([{"id": 0}]).map(lambda r: r, execution="thread", concurrency=2)


def _read_spy(monkeypatch):
    calls = []

    def spy(self, source, **kwargs):
        calls.append(source)
        raise AssertionError("source read attempted before admission")

    monkeypatch.setattr(LocalDatasetExecutor, "_iter_source", spy)
    return calls


def test_fetch_if_missing_false_returns_none_without_read(monkeypatch):
    ex = LocalDatasetExecutor()
    ds = _plain()
    calls = _read_spy(monkeypatch)
    assert ex.schema(ds._source, ds._plan, fetch_if_missing=False) is None
    assert calls == []


def test_plain_schema_infers_fields():
    ex = LocalDatasetExecutor()
    ds = _plain()
    schema = ex.schema(ds._source, ds._plan)
    assert set(schema.names) == {"id", "name"}


def test_streaming_schema_rejected_before_read(monkeypatch):
    ex = LocalDatasetExecutor()
    ds = _stream()
    calls = _read_spy(monkeypatch)
    with pytest.raises(UnsupportedExecutionOptionError, match="streaming"):
        ex.schema(ds._source, ds._plan)
    assert calls == []


def test_ray_native_options_schema_rejected_before_read(monkeypatch):
    ex = LocalDatasetExecutor()
    ds = _plain()
    plan = ds._plan.append(MapOp(CallableSpec.create(lambda r: r),
                                 native_options=NativeOptions(backend="ray")))
    calls = _read_spy(monkeypatch)
    with pytest.raises(UnsupportedExecutionOptionError, match="backend_options"):
        ex.schema(ds._source, plan)
    assert calls == []