"""A017 · iter_batches batches both pull and streaming-delivery routes."""
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


def _stream():
    return data.from_items([{"id": i} for i in range(5)]).map(
        lambda r: r, execution="thread", concurrency=2)


def _rows(batch):
    if isinstance(batch, list):
        return batch
    return batch.to_pylist()


def test_plain_iter_batches_groups_rows():
    ex = LocalDatasetExecutor()
    ds = _plain()
    batches = list(Dataset(ds._source, ds._plan, ex).iter_batches(batch_size=2, batch_format='pyarrow'))
    flat = [r["id"] for b in batches for r in _rows(b)]
    assert flat == [0, 1, 2, 3, 4]
    assert all(len(_rows(b)) <= 2 for b in batches)


def test_streaming_iter_batches_groups_rows():
    ex = LocalDatasetExecutor()
    ds = _stream()
    batches = list(Dataset(ds._source, ds._plan, ex).iter_batches(batch_size=2, batch_format='pyarrow'))
    flat = [r["id"] for b in batches for r in _rows(b)]
    assert sorted(flat) == [0, 1, 2, 3, 4]


def test_iter_batches_rejects_invalid_native_resources(tmp_path):
    ex = LocalDatasetExecutor()
    ds = _plain()
    plan = ds._plan.append(MapOp(CallableSpec.create(lambda r: r),
                                 native_options=NativeOptions(backend="ray", worker_resources={"cpu": 0})))
    source, marker = observed_source(tmp_path)
    with pytest.raises(DataflowError, match="positive CPU"):
        list(Dataset(source, plan, ex).iter_batches(batch_size=2))
    assert not marker.exists()
