"""A006 · take admits before reads, honors limit==0, and keeps streaming delivery."""
from demiflow.data.dataset import Dataset
import pytest

from demiflow import data
from demiflow.data.native_options import NativeOptions
from demiflow.data.plan import CallableSpec, MapOp
from demiflow.execution.contracts.dataflow_errors import DataflowError
from demiflow.execution.executors.local import LocalDatasetExecutor
from _runtime_v3_executor_scope import observed_source


def _plain():
    return data.from_items([{"id": i} for i in range(5)]).map(lambda r: r)


def _stream(execution="thread"):
    return data.from_items([{"id": i} for i in range(5)]).map(
        lambda r: r, execution=execution, concurrency=2)


def test_take_zero_returns_empty_without_read(tmp_path):
    ex = LocalDatasetExecutor()
    ds = _plain()
    source, marker = observed_source(tmp_path)
    assert Dataset(source, ds._plan, ex).take(0) == []
    assert not marker.exists()


def test_plain_take_pulls_prefix():
    ex = LocalDatasetExecutor()
    ds = _plain()
    assert [r["id"] for r in Dataset(ds._source, ds._plan, ex).take(2)] == [0, 1]


def test_thread_streaming_take_delivers():
    ex = LocalDatasetExecutor()
    ds = _stream("thread")
    assert [r["id"] for r in Dataset(ds._source, ds._plan, ex).take(3)] == [0, 1, 2]


def test_inline_streaming_take_delivers():
    ex = LocalDatasetExecutor()
    ds = _stream("inline")
    assert [r["id"] for r in Dataset(ds._source, ds._plan, ex).take(4)] == [0, 1, 2, 3]


def test_take_rejects_invalid_native_resources_before_read(tmp_path):
    ex = LocalDatasetExecutor()
    ds = _plain()
    plan = ds._plan.append(MapOp(CallableSpec.create(lambda r: r),
                                 native_options=NativeOptions(backend="ray", worker_resources={"cpu": 0})))
    source, marker = observed_source(tmp_path)
    with pytest.raises(DataflowError, match="positive CPU"):
        Dataset(source, plan, ex).take(2)
    assert not marker.exists()


def test_take_accepts_resource_only_backend_spelling_on_local():
    ex = LocalDatasetExecutor()
    ds = _plain()
    plan = ds._plan.append(MapOp(CallableSpec.create(lambda r: r),
        native_options=NativeOptions(backend='ray', worker_resources={})))
    assert [row['id'] for row in Dataset(ds._source, plan, ex).take(2)] == [0, 1]
    assert ex.NAME == 'local'
