"""A029 · Dataset._write_file is the single checked.action confluence for parquet/json/csv."""
import json

import pytest

from demiflow import data
from demiflow.errors import UnsupportedExecutionOptionError
from demiflow.execution.run_context import RunContext


def _action_spy(monkeypatch):
    calls = []
    orig = RunContext.action

    def counting(self, declaration, **kw):
        calls.append(declaration.get("kind"))
        return orig(self, declaration, **kw)

    monkeypatch.setattr(RunContext, "action", counting)
    return calls


def _plain():
    return data.from_items([{"id": i} for i in range(3)]).map(lambda r: r)


def _stream():
    return data.from_items([{"id": i} for i in range(3)]).map(
        lambda r: r, execution="thread", concurrency=2)


def test_write_json_single_action_and_none(tmp_path, monkeypatch):
    calls = _action_spy(monkeypatch)
    target = tmp_path / "o.json"
    assert _plain().write_json(str(target)) is None
    assert calls == ["write_file"]
    assert [json.loads(line)["id"] for line in target.read_text().splitlines()] == [0, 1, 2]


def test_write_csv_single_action(tmp_path, monkeypatch):
    calls = _action_spy(monkeypatch)
    target = tmp_path / "o.csv"
    assert _plain().write_csv(str(target)) is None
    assert calls == ["write_file"]
    assert target.read_text().splitlines()[0] == "id"


def test_write_parquet_single_action(tmp_path, monkeypatch):
    import pyarrow.parquet as pq
    calls = _action_spy(monkeypatch)
    target = tmp_path / "o.parquet"
    assert _plain().write_parquet(str(target)) is None
    assert calls == ["write_file"]
    assert pq.read_table(target).num_rows == 3


def test_write_file_streaming_uses_one_action_and_preserves_rows(tmp_path, monkeypatch):
    calls = _action_spy(monkeypatch)
    target = tmp_path / 's.json'
    assert _stream().write_json(str(target)) is None
    assert [json.loads(line)['id'] for line in target.read_text().splitlines()] == [0, 1, 2]
    assert calls == ['write_file']