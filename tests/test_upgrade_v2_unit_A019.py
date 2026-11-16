"""A019 · take_batch delegates to take (which admits) and shapes the batch."""
from demiflow import data
from demiflow.execution.executors.local import LocalDatasetExecutor
from demiflow.execution.run_context import RunContext
import pytest


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


def test_plain_take_batch_returns_prefix():
    ex = LocalDatasetExecutor()
    ds = _plain()
    batch = ex.take_batch(ds._source, ds._plan, 3, batch_format="pyarrow")
    assert [r["id"] for r in batch.to_pylist()] == [0, 1, 2]


def test_streaming_take_batch_returns_prefix():
    ex = LocalDatasetExecutor()
    ds = _stream()
    batch = ex.take_batch(ds._source, ds._plan, 2, batch_format="pyarrow")
    assert sorted(r["id"] for r in batch.to_pylist()) == [0, 1]


def test_take_batch_zero_is_empty():
    ex = LocalDatasetExecutor()
    ds = _plain()
    batch = ex.take_batch(ds._source, ds._plan, 0, batch_format="pyarrow")
    assert batch.to_pylist() == []