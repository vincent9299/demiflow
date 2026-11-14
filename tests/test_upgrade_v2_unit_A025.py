"""A025 · write_file opens one action (borrow or self-create) and rejects streaming."""
import json

import pytest

from demiflow import data
from demiflow.errors import UnsupportedExecutionOptionError
from demiflow.execution.executors.local import LocalDatasetExecutor
from demiflow.execution.recovery_identity import plan_identity
from demiflow.execution.run_context import RunContext


def _plain():
    return data.from_items([{"id": i} for i in range(3)]).map(lambda r: r)


def _stream():
    return data.from_items([{"id": i} for i in range(3)]).map(
        lambda r: r, execution="thread", concurrency=2)


def test_write_json_self_created_action(tmp_path):
    ex = LocalDatasetExecutor()
    ds = _plain()
    target = tmp_path / "out.json"
    result = ex.write_file(ds._source, ds._plan, "json", str(target), {})
    assert result.written_rows == 3
    lines = target.read_text().splitlines()
    assert [json.loads(line)["id"] for line in lines] == [0, 1, 2]


def test_write_csv_self_created_action(tmp_path):
    ex = LocalDatasetExecutor()
    ds = _plain()
    target = tmp_path / "out.csv"
    result = ex.write_file(ds._source, ds._plan, "csv", str(target), {})
    assert result.written_rows == 3
    assert target.read_text().splitlines()[0] == "id"


def test_write_parquet_self_created_action(tmp_path):
    import pyarrow.parquet as pq
    ex = LocalDatasetExecutor()
    ds = _plain()
    target = tmp_path / "out.parquet"
    result = ex.write_file(ds._source, ds._plan, "parquet", str(target), {})
    assert result.written_rows == 3
    assert pq.read_table(target).num_rows == 3


def test_write_file_borrows_outer_action_without_completing(tmp_path):
    ex = LocalDatasetExecutor()
    ds = _plain()
    target = tmp_path / "out.json"
    with RunContext.start(tmp_path / "run") as run:
        with run.action(plan_identity("write_file", ds._source, ds._plan)) as outer:
            result = ex.write_file(ds._source, ds._plan, "json", str(target), {})
            assert result.written_rows == 3
            assert outer.record["result"] is None
            outer.complete({"kind": "write_file", "delivery": "completed"})
        assert outer.record["result"] is not None


def test_streaming_write_file_rejected_before_read(tmp_path, monkeypatch):
    ex = LocalDatasetExecutor()
    ds = _stream()
    calls = []

    def spy(self, source, **kwargs):
        calls.append(source)
        raise AssertionError("source read attempted before admission")

    monkeypatch.setattr(LocalDatasetExecutor, "_iter_source", spy)
    with pytest.raises(UnsupportedExecutionOptionError, match="streaming"):
        ex.write_file(ds._source, ds._plan, "json", str(tmp_path / "x.json"), {})
    assert calls == []