"""Native v3 seal time is metadata, never a regenerated historical identity."""
import json
from pathlib import Path
import sqlite3
import time

import pytest

import demiflow
from demiflow import data
from demiflow.execution.seal import plan as seal
from demiflow.execution.contracts.dataflow_codec import compute_graph_digest, compute_physical_digest
from demiflow.execution.contracts.dataflow_errors import DataflowError
from demiflow.execution.journal.dataflow_history import load_execution_version
from demiflow.execution.recovery_contract import document
from demiflow.workbench.runs.facts import read_sealed_plan
from test_runtime_v3_real_seams import prepared


@pytest.fixture
def original_seal(tmp_path):
    assert Path(demiflow.__file__).resolve().parents[1] == Path(__file__).resolve().parents[1]
    started_ms = time.time_ns() // 1_000_000
    with prepared(tmp_path, data.from_items([{"x": 1}]).map(lambda row: row)) as (execution, services):
        yield execution, services, started_ms


def _raw(services):
    journal = services.action.context.journal
    with journal._connection() as db:
        return db.execute("SELECT document FROM runtime_sources WHERE action_id=?",
                          (services.action.action_id + "-dataflow-sealed",)).fetchone()[0]


def _replace_fixture_marker(services, marker):
    # Only this /tmp fixture is altered to represent old or corrupt persisted
    # history. Production bind_source remains immutable throughout these tests.
    journal = services.action.context.journal
    with sqlite3.connect(journal.path) as db:
        db.execute("UPDATE runtime_sources SET document=? WHERE action_id=?",
                   (document(marker).decode(), services.action.action_id + "-dataflow-sealed"))


def _reload(execution, services):
    graph, physical, sealed = seal.load_dataflow(services.action.context.directory,
        services.action.action_id, store=services.store, bounds=services.bounds)
    assert graph == execution._graph and physical == execution._physical and sealed == execution._sealed
    assert graph.dag_id == compute_graph_digest(graph)
    assert physical.plan_id == compute_physical_digest(physical)
    binding = load_execution_version(services.action.context.directory, services.action.action_id,
                                     max_bytes=services.bounds.max_state_bytes)
    assert binding.compatible and binding.schema_version == seal.SEAL_SCHEMA
    assert binding.original_plan_ref == sealed.physical_ref
    assert binding.original_code_pin_ref == sealed.code_pin_ref
    result = read_sealed_plan(services.action.context.directory, services.action.action_id)
    assert result["available"] and result["version"] == "v3", result
    return result


def _reseal(execution, services):
    return seal.seal_dataflow(services.action, execution._graph, execution._physical, store=services.store)


def test_first_v3_seal_time_is_stable_and_does_not_change_ids(original_seal, monkeypatch):
    execution, services, started_ms = original_seal
    original = _raw(services)
    marker = json.loads(original)
    assert type(marker.get("sealed_at")) is int
    assert started_ms <= marker["sealed_at"] <= time.time_ns() // 1_000_000
    assert marker["schema"] == "demiflow.dataflow_seal.v3"
    assert _reload(execution, services)["sealed_at"] == marker["sealed_at"]
    # A later wall clock cannot change the first seal's timestamp or identity.
    monkeypatch.setattr(seal, "time_ns", lambda: (marker["sealed_at"] + 60_000) * 1_000_000, raising=False)
    for _ in range(2):
        assert _reseal(execution, services) == execution._sealed
        assert _raw(services) == original
        assert _reload(execution, services)["sealed_at"] == marker["sealed_at"]


def test_old_v3_marker_stays_without_time_and_keeps_original_bytes(original_seal, monkeypatch):
    execution, services, _ = original_seal
    marker = json.loads(_raw(services))
    marker.pop("sealed_at", None)
    _replace_fixture_marker(services, marker)
    original = _raw(services)
    def forbidden_clock():
        raise AssertionError("old seal consulted current time")
    monkeypatch.setattr(seal, "time_ns", forbidden_clock, raising=False)
    for _ in range(2):
        assert _reload(execution, services)["sealed_at"] is None
        assert _reseal(execution, services) == execution._sealed
        assert _raw(services) == original
        assert "sealed_at" not in json.loads(_raw(services))


@pytest.mark.parametrize("timestamp", [0, 2**63 - 1])
def test_v3_seal_time_integer_boundaries_preserve_history(original_seal, timestamp):
    execution, services, _ = original_seal
    marker = dict(json.loads(_raw(services)), sealed_at=timestamp)
    _replace_fixture_marker(services, marker)
    original = _raw(services)
    assert _reload(execution, services)["sealed_at"] == timestamp
    assert _reseal(execution, services) == execution._sealed
    assert _raw(services) == original


@pytest.mark.parametrize("timestamp", [True, -1, 2**63, 1.5, "1700000000000", None])
def test_invalid_v3_seal_time_is_rejected_on_load_and_reseal(original_seal, timestamp):
    execution, services, _ = original_seal
    marker = dict(json.loads(_raw(services)), sealed_at=timestamp)
    _replace_fixture_marker(services, marker)
    original = _raw(services)
    for operation in (
        lambda: seal.load_dataflow(services.action.context.directory, services.action.action_id,
                                  store=services.store, bounds=services.bounds),
        lambda: load_execution_version(services.action.context.directory, services.action.action_id,
                                       max_bytes=services.bounds.max_state_bytes),
        lambda: _reseal(execution, services),
    ):
        with pytest.raises(DataflowError) as exc:
            operation()
        assert exc.value.code == "INVALID_CONTRACT"
        assert _raw(services) == original


@pytest.mark.parametrize("with_time", [False, True])
def test_v3_seal_time_does_not_open_marker_to_unknown_fields(original_seal, with_time):
    execution, services, _ = original_seal
    marker = json.loads(_raw(services))
    marker.pop("sealed_at", None)
    if with_time:
        marker["sealed_at"] = 1700000000000
    marker["unknown_metadata"] = "must remain rejected"
    _replace_fixture_marker(services, marker)
    original = _raw(services)
    for operation in (
        lambda: seal.load_dataflow(services.action.context.directory, services.action.action_id,
                                  store=services.store, bounds=services.bounds),
        lambda: load_execution_version(services.action.context.directory, services.action.action_id,
                                       max_bytes=services.bounds.max_state_bytes),
        lambda: _reseal(execution, services),
    ):
        with pytest.raises(DataflowError) as exc:
            operation()
        assert exc.value.code == "INVALID_CONTRACT"
        assert _raw(services) == original
