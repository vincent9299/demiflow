"""Implicit streaming defaults fit real concurrency without raising run budgets."""
from dataclasses import replace
import pytest
from demiflow.data.api import DataAPI
from demiflow.execution.executors.local import LocalDatasetExecutor
from demiflow.execution.dataflow_api import make_action_request, resolve_action_runtime
from demiflow.execution.action_driver.dataflow import DataflowExecution
from demiflow.execution.run_context import RunContext
from demiflow.planning.dataflow_compile import lower_action
from demiflow.planning.dataflow_place import partition_stages
from demiflow.planning.dataflow_policy import fit_default_stream_windows, resolve_runtime_policy
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
            resolved = fit_default_stream_windows(request,policy=policy,bounds=old)
            assert resolved.max_resident_bytes == old.max_resident_bytes
            assert resolved.max_row_bytes < old.max_row_bytes
            graph = lower_action(request,bounds=resolved).graph
            stages = partition_stages(graph,policy=policy,bounds=resolved)
            resident = sum(max(s.resident_bytes,s.profile.requests['memory_bytes']) for s in stages)
            assert resident <= resolved.max_resident_bytes - min(resolved.max_control_bytes,resolved.max_resident_bytes//8)
            assert fit_default_stream_windows(request,policy=policy,bounds=old,
                explicit_limits=('max_row_bytes','max_batch_bytes','max_state_bytes')) == old
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
    from demiflow.planning.dataflow_budget import incremental_components
    from demiflow.execution.dataflow_publication_budget import PublicationReservation

    def source():
        raise AssertionError('planning accessed the live iterator')
        yield {'id': 1}

    dataset = (data.from_iter(source).map(lambda row: row, concurrency=2, execution='thread')
               .reduce_by_key('id', lambda acc, row: row))
    action = request(dataset)
    policy, original = resolve_runtime_policy({'max_active_workers': 2}, {}, provider='local')
    bounds = fit_default_stream_windows(action, policy=policy, bounds=original)
    assert bounds.max_row_bytes < original.max_row_bytes
    assert bounds.max_resident_bytes == original.max_resident_bytes
    assert fit_default_stream_windows(action, policy=policy, bounds=original,
        explicit_limits=('max_row_bytes', 'max_batch_bytes', 'max_state_bytes')) == original
    graph = lower_action(action, bounds=bounds).graph
    assert any(port.requirement == 'finite_complete' for node in graph.nodes for port in node.kernel.inputs)
    stages = partition_stages(graph, policy=policy, bounds=bounds)
    owners = {stage.occurrences[0]: stage for stage in stages}
    indices = {node.occurrence_id: i for i, node in enumerate(graph.nodes)}
    groups = incremental_components(graph.nodes, [(indices[edge.producer], edge.output_port,
        indices[edge.consumer], edge.input_port) for edge in graph.edges])
    reservation = PublicationReservation(graph, stages, bounds)
    for group in groups.values():
        members = [owners[graph.nodes[i].occurrence_id] for i in group]
        resident = sum(max(stage.resident_bytes, stage.profile.requests['memory_bytes'])
                       * len(stage.partitions) for stage in members)
        assert resident + reservation.reserved(stage.stage_id for stage in members) <= bounds.max_resident_bytes
