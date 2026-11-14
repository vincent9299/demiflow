"""A018 · count uses metadata for pinned materialized sources, admits otherwise."""
from demiflow import data
from demiflow.data.plan import LogicalPlan
from demiflow.data.sources import MaterializedSource
from demiflow.execution.executors.local import LocalDatasetExecutor, _LocalMaterializedHandle


def _plain():
    return data.from_items([{"id": i} for i in range(6)]).map(lambda r: r)


def _stream():
    return data.from_items([{"id": i} for i in range(6)]).map(
        lambda r: r, execution="thread", concurrency=2)


def test_materialized_empty_plan_uses_row_count_without_read(monkeypatch):
    ex = LocalDatasetExecutor()
    handle = _LocalMaterializedHandle(blocks=(), row_count=7)
    source = MaterializedSource(handle, 7)

    def spy(self, source, **kwargs):
        raise AssertionError("metadata count must not read rows")

    monkeypatch.setattr(LocalDatasetExecutor, "_iter_source", spy)
    assert ex.count(source, LogicalPlan()) == 7


def test_plain_count_counts_rows():
    ex = LocalDatasetExecutor()
    ds = _plain()
    assert ex.count(ds._source, ds._plan) == 6


def test_streaming_count_counts_rows():
    ex = LocalDatasetExecutor()
    ds = _stream()
    assert ex.count(ds._source, ds._plan) == 6