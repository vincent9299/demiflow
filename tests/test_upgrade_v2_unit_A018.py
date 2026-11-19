"""A018 · count preserves actual published data without rerunning its upstream."""
from demiflow.data.dataset import Dataset
from demiflow import data
from demiflow.data.plan import LogicalPlan
from demiflow.data.sources import MaterializedSource
from demiflow.execution.executors.local import LocalDatasetExecutor


def _plain():
    return data.from_items([{"id": i} for i in range(6)]).map(lambda r: r)


def _stream():
    return data.from_items([{"id": i} for i in range(6)]).map(
        lambda r: r, execution="thread", concurrency=2)


def test_materialized_count_preserves_real_published_count_without_recomputing(tmp_path):
    from pathlib import Path
    marker = tmp_path / 'upstream'
    def record(row):
        with marker.open('a') as stream:
            stream.write('read\n')
        return row
    with data.local_execution():
        materialized = data.from_items([{'id': i} for i in range(7)]).map(record).materialize()
        try:
            assert materialized.count() == 7
            assert materialized.count() == 7
            assert marker.read_text().splitlines() == ['read'] * 7
        finally:
            materialized.release()


def test_plain_count_counts_rows():
    ex = LocalDatasetExecutor()
    ds = _plain()
    assert Dataset(ds._source, ds._plan, ex).count() == 6


def test_streaming_count_counts_rows():
    ex = LocalDatasetExecutor()
    ds = _stream()
    assert Dataset(ds._source, ds._plan, ex).count() == 6
