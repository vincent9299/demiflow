"""A025 · write_file shares the original action owner for ordinary and streaming maps."""
import json

import pytest

from demiflow import data
from demiflow.errors import UnsupportedExecutionOptionError
from demiflow.execution.executors.local import LocalDatasetExecutor
from _runtime_v3_executor_scope import call
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
    result = call(ex, 'write_file', ds._source, ds._plan, 'json', str(target), {})
    assert result.written_rows == 3
    lines = target.read_text().splitlines()
    assert [json.loads(line)["id"] for line in lines] == [0, 1, 2]


def test_write_csv_self_created_action(tmp_path):
    ex = LocalDatasetExecutor()
    ds = _plain()
    target = tmp_path / "out.csv"
    result = call(ex, 'write_file', ds._source, ds._plan, 'csv', str(target), {})
    assert result.written_rows == 3
    assert target.read_text().splitlines()[0] == "id"


def test_write_parquet_self_created_action(tmp_path):
    import pyarrow.parquet as pq
    ex = LocalDatasetExecutor()
    ds = _plain()
    target = tmp_path / "out.parquet"
    result = call(ex, 'write_file', ds._source, ds._plan, 'parquet', str(target), {})
    assert result.written_rows == 3
    assert pq.read_table(target).num_rows == 3


def test_write_file_borrows_outer_action_without_completing(tmp_path):
    ex = LocalDatasetExecutor()
    ds = _plain()
    target = tmp_path / "out.json"
    with RunContext.start(tmp_path / "run") as run:
        with run.action(plan_identity("write_file", ds._source, ds._plan)) as outer:
            result = ex.write_file(ds._source, ds._plan, 'json', str(target), {})
            assert result.written_rows == 3
            assert outer.record["result"] is None
            outer.complete({"kind": "write_file", "delivery": "completed"})
        assert outer.record["result"] is not None


def test_streaming_write_file_preserves_real_rows(tmp_path):
    ex = LocalDatasetExecutor()
    ds = _stream()
    target = tmp_path / 'stream.json'
    result = call(ex, 'write_file', ds._source, ds._plan, 'json', str(target), {})
    assert result.written_rows == 3
    assert [json.loads(line)['id'] for line in target.read_text().splitlines()] == [0, 1, 2]
