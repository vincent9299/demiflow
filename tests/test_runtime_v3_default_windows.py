"""Implicit streaming defaults fit real concurrency without raising run budgets."""
from dataclasses import replace
import pytest
from demiflow.data.api import DataAPI
from demiflow.execution.executors.local import LocalDatasetExecutor
from demiflow.execution.executors.dataflow_api import make_action_request
from demiflow.execution.action_driver.services import resolve_action_runtime
from demiflow.execution.action_driver.dataflow import DataflowExecution
from demiflow.execution.pipeline.run_context import RunContext
from demiflow.planning.dataflow_compile import lower_action
from demiflow.planning.dataflow_place import partition_stages
from demiflow.planning.dataflow_policy import resolve_runtime_policy
from test_runtime_v3_real_seams import PreparationOnly


class MustNotConstruct:
    def __init__(self):
        raise AssertionError('empty stage constructed an actor')
    def __call__(self, row):
        return row


def _request(executor):
    api = DataAPI(executor)
    ds = api.from_items([{'n':1}]).filter(lambda row:False).map(MustNotConstruct,
        concurrency=3, callable_scope='worker', execution='thread')
    return make_action_request(executor, ds._source, ds._plan, kind='take_all', arguments={})


def test_default_profile_windows_fit_without_constructing_actor(tmp_path):
    with pytest.raises(PreparationOnly):
        with RunContext.start(tmp_path/'run') as run:
            request = _request(LocalDatasetExecutor(workers=2))
            policy, old = resolve_runtime_policy({'max_active_workers':2},run.limits,provider='local')
            from test_runtime_v3_codec_profile_groups import physical
            policy, bounds = resolve_action_runtime(request)
            graph = lower_action(request, bounds=bounds).graph
            selected = physical(graph, policy, bounds)
            assert selected.bounds == old
            assert selected.bounds.max_state_bytes == run.limits['max_state_bytes']
            raise PreparationOnly()


def test_unconfigured_public_stream_uses_adjusted_profile_and_zero_actor_instances(tmp_path):
    # A checkpointable class factory requires no constructor for empty/filtered
    # input. Thread/live-registry placement is checked separately after merge.
    executor = LocalDatasetExecutor(workers=4)
    api = DataAPI(executor)
    try:
        with RunContext.start(tmp_path/'run'):
            result = api.from_items([{'n':1}]).filter(lambda r:False).map(
                MustNotConstruct,concurrency=3,callable_scope='worker',execution='inline').run_stream()
            assert result.emitted == 0
    finally:
        executor.close()


def test_finite_inputs_still_fit_the_downstream_streaming_component():
    from demiflow import data
    from test_runtime_v3_codec_profile_groups import request
    from test_runtime_v3_codec_profile_groups import physical
    from demiflow.execution.scheduler.dataflow_publication_budget import PublicationReservation

    def source():
        raise AssertionError('planning accessed the live iterator')
        yield {'id': 1}

    dataset = (data.from_iter(source).map(lambda row: row, concurrency=2, execution='thread')
               .reduce_by_key('id', lambda acc, row: row))
    action = request(dataset)
    policy, original = resolve_runtime_policy({'max_active_workers': 2}, {}, provider='local')
    graph = lower_action(action, bounds=original).graph
    selected = physical(graph, policy, original)
    assert selected.bounds == original
    assert any(port.requirement == 'finite_complete' for node in graph.nodes for port in node.kernel.inputs)
    stages = {stage.stage_id: stage for stage in selected.stages}
    reservation = PublicationReservation(graph, selected.stages, original,
                                         declared=selected.residency.control_reserved_bytes)
    for ids in selected.residency.simultaneous_groups:
        resident = sum(max(stages[i].resident_bytes, stages[i].profile.requests['memory_bytes'])
                       * stages[i].replica_target for i in ids)
        assert resident + reservation.reserved(ids) <= original.max_resident_bytes
