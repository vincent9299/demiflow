"""L009 unit: execute_native_plan verifies then dispatches the sealed backend.

The layout is built by the REAL interpreter from the fixture v2 bundle; the action
authority is the real RunContext._ACTION contextvar seam. The backend entrypoints
(L010 dataset_native.execute / L011 arrow_batch_sink.execute) do not yet accept the
sealed ``execution=`` placement, so they are injected at the boundary as recording
generator doubles, and the source-inclusive code-pin (L042) is injected as a double.
This proves dispatch-by-sealed-backend, verify-before-execute, and finally-release
on normal/early-close/error. It is a wiring/unit test, NOT an integration pass.
"""
import contextlib
import dataclasses

import pytest

from demiflow.execution import arrow_batch_sink, chain, dataset_native, run_context
from demiflow.execution.native_plan import execute_native_plan
from demiflow.execution.plan_interpreter import PlanContractError, interpret_physical_plan
from demiflow.execution.recovery_contract import RecoveryContractError, identity
from demiflow.execution.recovery_identity import describe
from demiflow.execution.run_context import RunAction
from test_execution_layout import PROGRAM_REF
import _v2_native_fixtures as fx

ACTION = "native-action"
ROWS = [{"x": 1, "a": 2, "b": 4}, {"x": 2, "a": 3, "b": 6}]


def _fake_pin(plan, *, source=None):
    return {"ops": len(plan.operations), "src": describe(source)}


@contextlib.contextmanager
def _action_authority(action_id=ACTION):
    action = RunAction(None, action_id, {"kind": "chain-materialize"})
    token = run_context._ACTION.set(action)
    try:
        yield action
    finally:
        run_context._ACTION.reset(token)


def _layout(monkeypatch, *, backend="local_stream", matching_pin=True):
    source, plan = fx.native_source_plan()
    program_ref = ({**PROGRAM_REF, "code_pin_digest": identity(_fake_pin(plan, source=source))}
                   if matching_pin else None)
    n = fx.native_v2_doc(ACTION, backend=backend, program_ref=program_ref)
    monkeypatch.setattr(chain, "plan_code_pin", _fake_pin)
    layout = interpret_physical_plan(n.ld, n.pp, bindings=n.bundle, manifest_ref=n.mref)
    return source, plan, layout, n


def _install_backends(monkeypatch, rows=ROWS):
    dataset_calls, arrow_calls = [], []

    def make(record):
        def execute(executor, source, plan, *args, **kwargs):
            entry = {"args": args, "kwargs": kwargs, "closed": False}
            record.append(entry)
            try:
                yield from rows
            finally:
                entry["closed"] = True
        return execute

    monkeypatch.setattr(dataset_native, "execute", make(dataset_calls))
    monkeypatch.setattr(arrow_batch_sink, "execute", make(arrow_calls))
    return dataset_calls, arrow_calls


def test_dispatches_sealed_backend_and_yields_rows(monkeypatch):
    source, plan, layout, n = _layout(monkeypatch, backend="local_stream")
    dataset_calls, arrow_calls = _install_backends(monkeypatch)
    sentinel = object()
    with _action_authority():
        out = list(execute_native_plan(object(), source, plan, layout=layout,
                                       execution_id=n.execution_id, arrow=True, sink_schema=sentinel))
    assert out == ROWS
    assert arrow_calls == [] and len(dataset_calls) == 1
    call = dataset_calls[0]
    assert call["kwargs"]["execution"] is layout.executions[0]
    assert call["kwargs"]["arrow"] is True
    assert call["kwargs"]["sink_schema"] is sentinel


def test_arrow_batch_backend_dispatch(monkeypatch):
    source, plan, layout, n = _layout(monkeypatch, backend="arrow_batch")
    dataset_calls, arrow_calls = _install_backends(monkeypatch)
    sentinel = object()
    with _action_authority():
        out = list(execute_native_plan(object(), source, plan, layout=layout,
                                       execution_id=n.execution_id, sink_schema=sentinel))
    assert out == ROWS
    assert dataset_calls == [] and len(arrow_calls) == 1
    call = arrow_calls[0]
    # arrow entrypoint takes schema positionally plus the sealed execution
    assert call["args"] == (sentinel,)
    assert call["kwargs"]["execution"] is layout.executions[0]


def test_finally_closes_backend_on_normal_completion(monkeypatch):
    source, plan, layout, n = _layout(monkeypatch)
    dataset_calls, _ = _install_backends(monkeypatch)
    with _action_authority():
        list(execute_native_plan(object(), source, plan, layout=layout, execution_id=n.execution_id))
    assert dataset_calls[0]["closed"] is True


def test_finally_closes_backend_on_early_close(monkeypatch):
    source, plan, layout, n = _layout(monkeypatch)
    dataset_calls, _ = _install_backends(monkeypatch)
    with _action_authority():
        gen = execute_native_plan(object(), source, plan, layout=layout, execution_id=n.execution_id)
        assert next(gen) == ROWS[0]
        gen.close()
    assert dataset_calls[0]["closed"] is True


def test_finally_closes_backend_on_error(monkeypatch):
    source, plan, layout, n = _layout(monkeypatch)
    calls = []

    def failing(executor, source, plan, *args, **kwargs):
        entry = {"closed": False}
        calls.append(entry)
        try:
            yield ROWS[0]
            raise RuntimeError("backend blew up")
        finally:
            entry["closed"] = True

    monkeypatch.setattr(dataset_native, "execute", failing)
    with _action_authority():
        gen = execute_native_plan(object(), source, plan, layout=layout, execution_id=n.execution_id)
        assert next(gen) == ROWS[0]
        with pytest.raises(RuntimeError, match="backend blew up"):
            next(gen)
    assert calls[0]["closed"] is True


def test_unknown_execution_id_rejected(monkeypatch):
    source, plan, layout, n = _layout(monkeypatch)
    dataset_calls, arrow_calls = _install_backends(monkeypatch)
    with _action_authority():
        with pytest.raises(PlanContractError) as exc:
            list(execute_native_plan(object(), source, plan, layout=layout, execution_id="absent"))
    assert exc.value.reason == "invalid_contract"
    assert dataset_calls == [] and arrow_calls == []


def test_no_active_action_rejected(monkeypatch):
    source, plan, layout, n = _layout(monkeypatch)
    dataset_calls, arrow_calls = _install_backends(monkeypatch)
    # no _action_authority: current_action() is None
    with pytest.raises(PlanContractError) as exc:
        list(execute_native_plan(object(), source, plan, layout=layout, execution_id=n.execution_id))
    assert exc.value.reason == "invalid_contract"
    assert dataset_calls == [] and arrow_calls == []


def test_verify_failure_blocks_dispatch(monkeypatch):
    # default PROGRAM_REF digest does not match the recomputed source-inclusive pin
    source, plan, layout, n = _layout(monkeypatch, matching_pin=False)
    dataset_calls, arrow_calls = _install_backends(monkeypatch)
    with _action_authority():
        with pytest.raises(RecoveryContractError):
            list(execute_native_plan(object(), source, plan, layout=layout, execution_id=n.execution_id))
    assert dataset_calls == [] and arrow_calls == []


def test_worker_placement_refused(monkeypatch):
    source, plan, layout, n = _layout(monkeypatch)
    dataset_calls, arrow_calls = _install_backends(monkeypatch)
    worker = dataclasses.replace(layout.executions[0], placement="worker", group_id="g1")
    layout = dataclasses.replace(layout, executions=(worker,))
    with _action_authority():
        with pytest.raises(PlanContractError) as exc:
            list(execute_native_plan(object(), source, plan, layout=layout,
                                     execution_id=worker.execution_id))
    assert exc.value.reason == "unsupported_layout"
    assert dataset_calls == [] and arrow_calls == []