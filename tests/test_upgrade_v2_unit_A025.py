"""A025 · write_file shares the original action owner for ordinary and streaming maps."""
import json

import pytest

from demiflow import data


@pytest.fixture
def receipts(monkeypatch):
    """Observe actual engine receipts without changing public writer returns."""
    from demiflow.execution.executors import dataflow_api
    original = dataflow_api.run_dataset_action
    observed = []
    def run(request):
        result = original(request)
        observed.append(result)
        return result
    monkeypatch.setattr(dataflow_api, 'run_dataset_action', run)
    return observed


def _plain():
    return data.from_items([{"id": i} for i in range(3)]).map(lambda r: r)


def _stream():
    return data.from_items([{"id": i} for i in range(3)]).map(
        lambda r: r, execution="thread", concurrency=2)


def test_write_json_self_created_action(tmp_path, receipts):
    ds = _plain()
    target = tmp_path / "out.json"
    result = ds.write_json(str(target))
    assert result is None
    assert len(receipts) == 1 and receipts[0].written_rows == 3
    lines = target.read_text().splitlines()
    assert [json.loads(line)["id"] for line in lines] == [0, 1, 2]


def test_write_csv_self_created_action(tmp_path, receipts):
    ds = _plain()
    target = tmp_path / "out.csv"
    result = ds.write_csv(str(target))
    assert result is None
    assert len(receipts) == 1 and receipts[0].written_rows == 3
    assert target.read_text().splitlines()[0] == "id"


def test_write_parquet_self_created_action(tmp_path, receipts):
    import pyarrow.parquet as pq
    ds = _plain()
    target = tmp_path / "out.parquet"
    result = ds.write_parquet(str(target))
    assert result is None
    assert len(receipts) == 1 and receipts[0].written_rows == 3
    assert pq.read_table(target).num_rows == 3


def test_streaming_write_file_preserves_real_rows(tmp_path, receipts):
    ds = _stream()
    target = tmp_path / 'stream.json'
    result = ds.write_json(str(target))
    assert result is None
    assert len(receipts) == 1 and receipts[0].written_rows == 3
    assert [json.loads(line)['id'] for line in target.read_text().splitlines()] == [0, 1, 2]
