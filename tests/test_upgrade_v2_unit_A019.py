"""A019 · take_batch delegates to take (which admits) and shapes the batch."""
from demiflow.data.dataset import Dataset
import pytest

from demiflow import data
from demiflow.execution.executors.local import LocalDatasetExecutor
from _runtime_v3_executor_scope import observed_source


def _plain():
    return data.from_items([{"id": i} for i in range(5)]).map(lambda r: r)


def _stream():
    return data.from_items([{"id": i} for i in range(5)]).map(
        lambda r: r, execution="thread", concurrency=2)


def test_plain_take_batch_returns_prefix():
    ex = LocalDatasetExecutor()
    ds = _plain()
    batch = Dataset(ds._source, ds._plan, ex).take_batch(3, batch_format='pyarrow')
    assert [r["id"] for r in batch.to_pylist()] == [0, 1, 2]


def test_streaming_take_batch_returns_prefix():
    ex = LocalDatasetExecutor()
    ds = _stream()
    batch = Dataset(ds._source, ds._plan, ex).take_batch(2, batch_format='pyarrow')
    assert sorted(r["id"] for r in batch.to_pylist()) == [0, 1]


def test_take_batch_zero_rejected_before_source_read(tmp_path):
    ex = LocalDatasetExecutor()
    ds = _plain()
    source, marker = observed_source(tmp_path)
    with pytest.raises(ValueError, match='batch_size must be positive'):
        Dataset(source, ds._plan, ex).take_batch(0, batch_format='pyarrow')
    assert not marker.exists()
