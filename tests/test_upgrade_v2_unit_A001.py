"""A001 · admit_action_plan pure static admission and C03 routing."""
import dataclasses

import pytest

from demiflow import data
from demiflow.data.native_options import NativeOptions
from demiflow.data.plan import (
    AsyncMapOp, CallableSpec, FilterOp, LimitOp, LogicalPlan, MapBatchesOp,
    MapOp, MapStreamOptions,
)
from demiflow.data.sources import DatasourceSource
from demiflow.errors import UnsupportedExecutionOptionError
from demiflow.execution.capabilities import ActionDecision, admit_action_plan
from demiflow.execution.queue_source import QueueDatasource


def _spec():
    return CallableSpec.create(lambda row: row)


def _plan(*ops):
    return LogicalPlan(tuple(ops))


def _plain_map():
    return MapOp(_spec())


def _stream_map(execution="thread"):
    return MapOp(_spec(), stream_options=MapStreamOptions(concurrency=2, execution=execution))


def _source():
    return data.from_items([{"id": 0}])._source


LOCAL = ("local_pull", "local_stream")


def test_decision_is_frozen_with_declared_fields():
    decision = admit_action_plan(_source(), _plan(_plain_map()), action_kind="take",
                                 available_backends=LOCAL)
    assert isinstance(decision, ActionDecision)
    assert {f.name for f in dataclasses.fields(decision)} == {
        "action_kind", "backend", "route", "recovery"}
    with pytest.raises(dataclasses.FrozenInstanceError):
        decision.route = "pull"


def test_plain_pull_terminals_keep_lazy_pull_route():
    plan = _plan(_plain_map(), FilterOp(_spec()))
    for kind in ("take", "take_all", "take_batch", "iter_rows", "iter_batches",
                 "count", "aggregate", "schema", "size_bytes"):
        decision = admit_action_plan(_source(), plan, action_kind=kind,
                                     available_backends=LOCAL)
        assert decision == ActionDecision(kind, "local_pull", "pull", "attempt_only"), kind


def test_thread_and_inline_streaming_map_deliver():
    for execution in ("thread", "inline"):
        plan = _plan(_stream_map(execution))
        for kind in ("take", "take_all", "take_batch", "iter_rows", "iter_batches", "count"):
            decision = admit_action_plan(_source(), plan, action_kind=kind,
                                         available_backends=LOCAL)
            assert decision == ActionDecision(kind, "local_stream", "stream_delivery",
                                              "delivery_only"), (execution, kind)


def test_streaming_materialize_termals_use_native_checkpoint():
    plan = _plan(_stream_map())
    for kind in ("materialize", "write_lance", "write_datasink"):
        decision = admit_action_plan(_source(), plan, action_kind=kind,
                                     available_backends=LOCAL)
        assert decision == ActionDecision(kind, "local_stream", "stream_materialize",
                                          "native_checkpoint"), kind


def test_run_stream_non_queue_is_delivery_only():
    decision = admit_action_plan(_source(), _plan(_stream_map()), action_kind="run_stream",
                                 available_backends=LOCAL)
    assert decision == ActionDecision("run_stream", "local_stream", "stream_delivery",
                                      "delivery_only")


def test_run_stream_queue_source_selects_queue_stream():
    source = DatasourceSource(datasource=object.__new__(QueueDatasource))
    decision = admit_action_plan(source, _plan(_stream_map()), action_kind="run_stream",
                                 available_backends=LOCAL)
    assert decision.route == "queue_stream"
    assert decision.recovery == "native_checkpoint"


def test_async_map_is_streaming_for_delivery():
    plan = _plan(AsyncMapOp(_spec()))
    decision = admit_action_plan(_source(), plan, action_kind="take", available_backends=LOCAL)
    assert decision.route == "stream_delivery"


def test_write_lance_backend_selection_follows_declared_capability():
    plan = _plan(_plain_map())
    cases = [
        (("lance_scan",), "lance_scan", "lance_scan"),
        (("datafusion",), "datafusion", "native_relational"),
        ((), "local_pull", "pull"),
    ]
    for available, backend, route in cases:
        decision = admit_action_plan(_source(), plan, action_kind="write_lance",
                                     available_backends=LOCAL + available)
        assert decision == ActionDecision("write_lance", backend, route, "final_commit"), available


def test_write_lance_arrow_requires_schema():
    plan = _plan(_plain_map())

    class _Spec:
        schema = object()

    decision = admit_action_plan(_source(), plan, action_kind="write_lance",
                                 sink_spec=_Spec(), available_backends=LOCAL + ("arrow_batch",))
    assert decision == ActionDecision("write_lance", "arrow_batch", "arrow_batch", "final_commit")
    with pytest.raises(ValueError, match="schema"):
        admit_action_plan(_source(), plan, action_kind="write_lance",
                          available_backends=LOCAL + ("arrow_batch",))


def test_relational_non_write_uses_datafusion_when_declared():
    decision = admit_action_plan(_source(), _plan(_plain_map()), action_kind="take",
                                 available_backends=LOCAL + ("datafusion",))
    assert decision == ActionDecision("take", "datafusion", "native_relational", "attempt_only")


def test_write_terminals_recovery():
    plan = _plan(_plain_map())
    assert admit_action_plan(_source(), plan, action_kind="write_datasink",
                             available_backends=LOCAL).recovery == "final_commit"
    assert admit_action_plan(_source(), plan, action_kind="write_file",
                             available_backends=LOCAL).recovery == "delivery_only"


def test_unknown_action_kind_rejected():
    with pytest.raises(ValueError, match="Unknown public action kind"):
        admit_action_plan(_source(), _plan(_plain_map()), action_kind="_private",
                          available_backends=LOCAL)


def test_sink_spec_only_for_write_lance():
    with pytest.raises(ValueError, match="sink_spec"):
        admit_action_plan(_source(), _plan(_plain_map()), action_kind="take",
                          sink_spec=object(), available_backends=LOCAL)


def test_streaming_non_stream_terminal_rejected_before_reads():
    plan = _plan(_stream_map())
    for kind in ("aggregate", "schema", "size_bytes", "write_file"):
        with pytest.raises(UnsupportedExecutionOptionError, match="streaming"):
            admit_action_plan(_source(), plan, action_kind=kind, available_backends=LOCAL)


def test_local_rejects_ray_native_options():
    plan = _plan(MapOp(_spec(), native_options=NativeOptions(backend="ray")))
    with pytest.raises(UnsupportedExecutionOptionError, match="backend_options"):
        admit_action_plan(_source(), plan, action_kind="take", available_backends=LOCAL)


def test_retired_or_private_kind_not_admissible():
    with pytest.raises(ValueError, match="Unknown public action kind"):
        admit_action_plan(_source(), _plan(_plain_map()), action_kind="checkpoint",
                          available_backends=LOCAL)


def test_non_local_backend_rejected():
    with pytest.raises(UnsupportedExecutionOptionError):
        admit_action_plan(_source(), _plan(_plain_map()), action_kind="take",
                          backend="ray", available_backends=LOCAL)


def test_empty_available_backends_selects_plain_local_only():
    plan = _plan(_plain_map())
    decision = admit_action_plan(_source(), plan, action_kind="write_lance")
    assert decision == ActionDecision("write_lance", "local_pull", "pull", "final_commit")
    assert admit_action_plan(_source(), plan, action_kind="take").route == "pull"


def test_pull_composition_rejects_unsupported_operation():
    plan = _plan(MapBatchesOp(_spec()), LimitOp(1))
    decision = admit_action_plan(_source(), plan, action_kind="take", available_backends=LOCAL)
    assert decision.route == "pull"


def test_admission_is_pure_and_does_not_read_source():
    class _ExplodingSource:
        def __getattribute__(self, name):
            if name in ("__class__",):
                return object.__getattribute__(self, name)
            raise AssertionError("admission must not touch the source")

    decision = admit_action_plan(_ExplodingSource(), _plan(_plain_map()),
                                 action_kind="take", available_backends=LOCAL)
    assert decision.route == "pull"
