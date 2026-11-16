"""A023 · materialize uses one action, the actual dataflow, and pre-read admission."""
import pytest

from demiflow import data
from demiflow.data.native_options import NativeOptions
from demiflow.data.plan import CallableSpec, MapOp
from demiflow.errors import UnsupportedExecutionOptionError
from demiflow.execution.contracts.dataflow_errors import DataflowError
from demiflow.execution.executors.local import LocalDatasetExecutor
from _runtime_v3_executor_scope import call, observed_source
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
    return data.from_items([{"id": i} for i in range(6)]).map(lambda r: r)


def _stream():
    return data.from_items([{"id": i} for i in range(6)]).map(
        lambda r: r, execution="thread", concurrency=2)


def test_plain_materialize_single_action(monkeypatch):
    calls = _action_spy(monkeypatch)
    mat = _plain().materialize()
    assert calls == ["materialize"]
    assert mat.take_all() == [{"id": i} for i in range(6)]


def test_empty_input_materialize_single_action(monkeypatch):
    calls = _action_spy(monkeypatch)
    mat = data.from_items([]).map(lambda r: r).materialize()
    assert calls == ["materialize"]
    assert mat.take_all() == []


def test_plain_materialize_requires_action_context():
    ex = LocalDatasetExecutor()
    ds = _plain()
    with pytest.raises(DataflowError, match="execution scope"):
        ex.materialize(ds._source, ds._plan)


def test_streaming_materialize_admits_then_requires_action_context():
    ex = LocalDatasetExecutor()
    ds = _stream()
    # The thin host adapter requires the original execution scope for both kinds.
    with pytest.raises(DataflowError, match="execution scope"):
        ex.materialize(ds._source, ds._plan)


def test_ray_native_options_materialize_rejected_before_read(tmp_path):
    ex = LocalDatasetExecutor()
    ds = _plain()
    plan = ds._plan.append(MapOp(CallableSpec.create(lambda r: r),
                                 native_options=NativeOptions(backend="ray", worker_resources={"cpu": 0})))
    source, marker = observed_source(tmp_path)
    with pytest.raises(DataflowError, match="positive CPU"):
        call(ex, 'materialize', source, plan)
    assert not marker.exists()
