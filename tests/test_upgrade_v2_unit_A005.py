"""A005 · reject_streaming_map_options only at non-streaming boundaries."""
import pytest

from demiflow.data.plan import (
    BoundMapOp, CallableSpec, LogicalPlan, MapOp, MapStreamOptions,
)
from demiflow.data.plan import reject_streaming_map_options
from demiflow.errors import UnsupportedExecutionOptionError


def _spec():
    return CallableSpec.create(lambda row: row)


def _plan(*ops):
    return LogicalPlan(tuple(ops))


def _stream_map():
    return MapOp(_spec(), stream_options=MapStreamOptions(execution="thread"))


def _stream_bound_map():
    return BoundMapOp(_spec(), inputs={"x": "x"},
                      stream_options=MapStreamOptions(execution="inline"))


def test_local_streaming_terminals_not_pre_rejected():
    assert reject_streaming_map_options(_plan(_stream_map()), "local") is None
    assert reject_streaming_map_options(_plan(_stream_bound_map()), "local") is None


def test_local_plain_plan_passes():
    assert reject_streaming_map_options(_plan(MapOp(_spec())), "local") is None
    assert reject_streaming_map_options(_plan(), "local") is None


@pytest.mark.parametrize("backend", ["local relational", "local union", "Ray"])
def test_non_streaming_boundaries_reject_streaming_map(backend):
    with pytest.raises(UnsupportedExecutionOptionError, match="streaming options"):
        reject_streaming_map_options(_plan(_stream_map()), backend)


@pytest.mark.parametrize("backend", ["local relational", "local union", "Ray"])
def test_non_streaming_boundaries_reject_bound_streaming_map(backend):
    with pytest.raises(UnsupportedExecutionOptionError):
        reject_streaming_map_options(_plan(_stream_bound_map()), backend)


@pytest.mark.parametrize("backend", ["local relational", "local union", "Ray"])
def test_non_streaming_boundaries_allow_plain_plan(backend):
    assert reject_streaming_map_options(_plan(MapOp(_spec())), backend) is None