"""A002 · validate_stream_plan whole-graph preflight via unified capability."""
import pytest

from demiflow.data.native_options import NativeOptions
from demiflow.data.plan import (
    AsyncMapOp, CallableSpec, FilterOp, LogicalPlan, MapBatchesOp, MapOp,
    MapStreamOptions,
)
from demiflow.execution.capabilities import validate_stream_plan


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


def test_unsupported_stream_operation_rejected():
    with pytest.raises(ValueError):
        validate_stream_plan(_plan(_stream_map(), MapBatchesOp(_spec())))


def test_stream_backend_options_rejected():
    plan = _plan(MapOp(_spec(), native_options=NativeOptions(backend="ray"),
                       stream_options=MapStreamOptions()))
    with pytest.raises(ValueError, match="backend_options"):
        validate_stream_plan(plan)


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
    plan = _plan(*[_stream_map() for _ in range(1025)])
    with pytest.raises(ValueError, match="1024"):
        validate_stream_plan(plan)