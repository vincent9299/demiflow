"""A017 · iter_batches batches both pull and streaming-delivery routes."""
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
    batches = list(ex.iter_batches(ds._source, ds._plan, batch_size=2, batch_format="list"))
    flat = [r["id"] for b in batches for r in _rows(b)]
    assert flat == [0, 1, 2, 3, 4]
    assert all(len(_rows(b)) <= 2 for b in batches)


def test_streaming_iter_batches_groups_rows():
    ex = LocalDatasetExecutor()
    ds = _stream()
    batches = list(ex.iter_batches(ds._source, ds._plan, batch_size=2, batch_format="list"))
    flat = [r["id"] for b in batches for r in _rows(b)]
    assert sorted(flat) == [0, 1, 2, 3, 4]


def test_iter_batches_rejects_ray_native_options():
    ex = LocalDatasetExecutor()
    ds = _plain()
    plan = ds._plan.append(MapOp(CallableSpec.create(lambda r: r),
                                 native_options=NativeOptions(backend="ray")))
    with pytest.raises(UnsupportedExecutionOptionError, match="backend_options"):
        list(ex.iter_batches(ds._source, plan, batch_size=2))