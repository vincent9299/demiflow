"""A007 · iter_rows routes streaming plans to delivery and plain plans to pull."""
from demiflow.data.dataset import Dataset
import pytest

from demiflow import data
from demiflow.data.native_options import NativeOptions
from demiflow.data.plan import CallableSpec, MapOp
from demiflow.execution.contracts.dataflow_errors import DataflowError
from demiflow.execution.executors.local import LocalDatasetExecutor
from _runtime_v3_executor_scope import observed_source


def _plain():
    return data.from_items([{"id": i} for i in range(4)]).map(lambda r: r)


def _stream(execution="thread"):
    return data.from_items([{"id": i} for i in range(4)]).map(
        lambda r: r, execution=execution, concurrency=2)


def test_plain_iter_rows_pulls_all():
    ex = LocalDatasetExecutor()
    ds = _plain()
    assert [r["id"] for r in Dataset(ds._source, ds._plan, ex).iter_rows()] == [0, 1, 2, 3]


def test_thread_streaming_iter_rows_delivers():
    ex = LocalDatasetExecutor()
    ds = _stream("thread")
    assert [r["id"] for r in Dataset(ds._source, ds._plan, ex).iter_rows()] == [0, 1, 2, 3]


def test_inline_streaming_iter_rows_delivers():
    ex = LocalDatasetExecutor()
    ds = _stream("inline")
    assert [r["id"] for r in Dataset(ds._source, ds._plan, ex).iter_rows()] == [0, 1, 2, 3]


def test_iter_rows_rejects_invalid_native_resources_before_read(tmp_path):
    ex = LocalDatasetExecutor()
    ds = _plain()
    plan = ds._plan.append(MapOp(CallableSpec.create(lambda r: r),
                                 native_options=NativeOptions(backend="ray", worker_resources={"cpu": 0})))

    source, marker = observed_source(tmp_path)
    with pytest.raises(DataflowError, match="positive CPU"):
        list(Dataset(source, plan, ex).iter_rows())
    assert not marker.exists()
