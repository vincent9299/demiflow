"""A005 · unified providers accept streaming maps; unknown providers fail closed."""
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


@pytest.mark.parametrize("backend", ["local union", "Ray"])
def test_non_streaming_boundaries_reject_streaming_map(backend):
    with pytest.raises(UnsupportedExecutionOptionError, match="streaming options"):
        reject_streaming_map_options(_plan(_stream_map()), backend)


@pytest.mark.parametrize("backend", ["local union", "Ray"])
def test_non_streaming_boundaries_reject_bound_streaming_map(backend):
    with pytest.raises(UnsupportedExecutionOptionError):
        reject_streaming_map_options(_plan(_stream_bound_map()), backend)


@pytest.mark.parametrize("backend", ["local relational", "local union", "Ray"])
def test_non_streaming_boundaries_allow_plain_plan(backend):
    assert reject_streaming_map_options(_plan(MapOp(_spec())), backend) is None

@pytest.mark.parametrize("bound", [False, True])
def test_local_relational_streaming_map_uses_real_unified_execution(tmp_path, bound):
    from demiflow import data
    from demiflow.execution.run_context import RunContext
    for provider in ('local', 'local relational', 'ray', 'ray_core'):
        assert reject_streaming_map_options(_plan(_stream_bound_map() if bound else _stream_map()), provider) is None
    with RunContext.start(tmp_path / 'run') as run, data.local_execution():
        source = data.from_items([{'x': 1}]).union(data.from_items([{'x': 2}]))
        result = (source.map(lambda x: x + 1, inputs={'x': 'x'}, output='y', execution='thread')
                  if bound else source.map(lambda row: {'x': row['x'] + 1}, execution='thread'))
        expected = [{'x': 1, 'y': 2}, {'x': 2, 'y': 3}] if bound else [{'x': 2}, {'x': 3}]
        assert sorted(result.take_all(), key=lambda row: row['x']) == expected
        assert run.journal.source_binding('action-1-dataflow-sealed') is not None
        commits = run.journal.execution_commits('action-1')
        assert commits and all(item['status'] == 'acked' for item in commits)
