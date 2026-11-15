"""Implicit streaming defaults fit real concurrency without raising run budgets."""
from dataclasses import replace
import pytest
from demiflow.data.api import DataAPI
from demiflow.execution.executors.local import LocalDatasetExecutor
from demiflow.execution.dataflow_api import make_action_request, resolve_action_runtime
from demiflow.execution.dataflow import DataflowExecution
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
