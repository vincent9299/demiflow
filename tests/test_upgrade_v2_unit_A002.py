"""A002 · validate_stream_plan whole-graph preflight via unified capability."""
import pytest

from demiflow.data.native_options import NativeOptions
from demiflow.data.plan import (
    AsyncMapOp, CallableSpec, FilterOp, LogicalPlan, MapBatchesOp, MapOp,
    MapStreamOptions,
)
from demiflow.planning.dataflow_compile import validate_stream_plan
from demiflow.execution.contracts.dataflow_errors import DataflowError
from demiflow.planning.dataflow_policy import resolve_runtime_policy


def _spec(fn=None):
    return CallableSpec.create(fn or (lambda row: row))


def _plan(*ops):
    return LogicalPlan(tuple(ops))


def _stream_map(**kwargs):
    return MapOp(_spec(), stream_options=MapStreamOptions(concurrency=2, **kwargs))


def test_streaming_map_plan_passes():
    assert validate_stream_plan(_plan(_stream_map(execution="thread"))) is None
    assert validate_stream_plan(_plan(_stream_map(execution="inline"))) is None


def test_async_map_plan_passes():
    assert validate_stream_plan(_plan(AsyncMapOp(_spec()))) is None


def test_synchronous_prefix_before_stream_passes():
    plan = _plan(MapOp(_spec()), FilterOp(_spec()), _stream_map())
    assert validate_stream_plan(plan) is None


def test_no_stream_operation_rejected():
    with pytest.raises(ValueError, match="run_stream requires"):
        validate_stream_plan(_plan(MapOp(_spec())))
    with pytest.raises(ValueError, match="run_stream requires"):
        validate_stream_plan(_plan())


def test_registered_batch_after_stream_passes_and_unknown_kind_is_rejected():
    assert validate_stream_plan(_plan(_stream_map(), MapBatchesOp(_spec()))) is None
    class UnknownOperation:
        pass
    with pytest.raises(DataflowError, match='UNSUPPORTED_CAPABILITY'):
        validate_stream_plan(_plan(_stream_map(), UnknownOperation()))


def test_stream_resource_options_pass_but_ray_data_pool_options_are_rejected():
    plan = _plan(MapOp(_spec(), native_options=NativeOptions(backend="ray"),
                       stream_options=MapStreamOptions()))
    assert validate_stream_plan(plan) is None
    invalid = _plan(MapOp(_spec(), native_options=NativeOptions(backend='ray',
        compute_kind='actor_pool'), stream_options=MapStreamOptions()))
    with pytest.raises(ValueError, match="backend_options"):
        validate_stream_plan(invalid)


def test_worker_scope_requires_callable_class():
    with pytest.raises(TypeError, match="callable class"):
        validate_stream_plan(_plan(_stream_map(callable_scope="worker")))


def test_stream_map_rejects_async_callable():
    async def fn(row):
        return row

    plan = _plan(MapOp(_spec(fn), stream_options=MapStreamOptions()))
    with pytest.raises(TypeError, match="synchronous callable"):
        validate_stream_plan(plan)


def test_composition_limit_enforced():
    _, bounds = resolve_runtime_policy({}, {}, provider='local')
    plan = _plan(*[_stream_map() for _ in range(bounds.max_nodes)])
    with pytest.raises(DataflowError, match='BUDGET_EXCEEDED.*node budget'):
        validate_stream_plan(plan)