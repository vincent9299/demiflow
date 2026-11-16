"""One native CPU suffix contract for Local and Ray, with original grants."""
from dataclasses import replace

import pytest
from demiflow import data
from demiflow.execution.contracts.dataflow import ActionRequest, HostDescriptor
from demiflow.execution.dataflow_api import _CompileContext
from demiflow.execution.run_context import RunAction, RunContext
from demiflow.planning.dataflow_compile import lower_action
from demiflow.planning.dataflow_place import partition_stages
from demiflow.planning.dataflow_policy import resolve_runtime_policy
from demiflow.planning.dataflow_thread_fusion import fuse_thread_suffix
from demiflow.resources.contracts import ResourceProfile
from test_runtime_v3_ray_terminal_seams import private_ray_provider


async def _add(row):
    return {'id': row['id'] + 1}


def _identity(row):
    return row


async def _twice(row):
    return {'id': row['id'] * 2}


def _pipeline(count):
    return data.range(count).map_async(_add, concurrency=1).map(_identity).map_async(_twice, concurrency=1)


def _compiled():
    dataset = _pipeline(8)
    request = ActionRequest(kind='take_all', source=dataset._source, plan=dataset._plan,
        arguments={}, sink=None, programs=(), provider='ray_core',
        action=RunAction(_CompileContext(), 'ray-async-suffix', {}))
    policy, bounds = resolve_runtime_policy({'max_active_workers': 4}, {}, provider='ray_core')
    graph = lower_action(request, bounds=bounds).graph
    stages = partition_stages(graph, policy=policy, bounds=bounds)
    host = HostDescriptor(host_id='one-real-host', provider='ray_core', runtime_revision='test',
        store_domains=(), logical_resources={'cpu_millis': 8000, 'memory_bytes': 2**30},
        isolation_capabilities=())
    return graph, stages, policy, bounds, host


def test_ray_cpu_async_suffix_uses_same_owner_and_summed_grant_as_local():
    graph, stages, policy, bounds, host = _compiled()
    assert len(stages) == 5
    merged = fuse_thread_suffix(graph, stages, policy=policy, bounds=bounds, hosts=(host,))
    assert len(merged) == 2
    source = next(s for s in merged if graph.sources[0] in s.occurrences)
    assert source.occurrences == graph.sources
    suffix = next(s for s in merged if graph.terminals[0] in s.occurrences)
    members = [s for s in stages if s.occurrences[0] not in graph.sources]
    assert suffix.occurrences == tuple(s.occurrences[0] for s in members)
    assert suffix.profile.requests['cpu_millis'] == sum(s.profile.requests['cpu_millis'] for s in members)
    assert suffix.resident_bytes == suffix.profile.requests['memory_bytes'] == sum(
        max(s.resident_bytes, s.profile.requests['memory_bytes']) for s in members)
    assert suffix.state_bytes == sum(s.state_bytes for s in members)
    assert fuse_thread_suffix(graph, stages, policy=replace(policy, provider='local'),
        bounds=bounds, hosts=(host,)) == merged


@pytest.mark.parametrize('constraint', ['fusion', 'host_cpu', 'host_memory', 'limits', 'isolation', 'gpu'])
def test_ray_async_colocation_keeps_actual_host_and_explicit_profile_constraints(constraint):
    graph, stages, policy, bounds, host = _compiled()
    if constraint == 'fusion':
        assert fuse_thread_suffix(graph, stages, policy=replace(policy, fusion=False),
            bounds=bounds, hosts=(host,)) == stages
        return
    if constraint.startswith('host_'):
        key = 'cpu_millis' if constraint == 'host_cpu' else 'memory_bytes'
        small = replace(host, logical_resources={**host.logical_resources, key: 1})
        # Aggregate cluster capacity cannot serve as one worker's grant.
        assert fuse_thread_suffix(graph, stages, policy=policy, bounds=bounds,
            hosts=tuple(replace(small, host_id=str(i)) for i in range(8))) == stages
        return
    # The terminal is part of the candidate and must obey the same restrictions.
    original = stages[-1]
    profile = original.profile.to_dict()
    if constraint == 'limits':
        profile['limits'] = {'memory_bytes': 2**28}
    elif constraint == 'isolation':
        profile['requirements'] = {'isolation': 'process'}
    else:
        profile['requests']['accelerators'] = [{'type': 'gpu', 'count': 1}]
    changed = (*stages[:-1], replace(original, profile=ResourceProfile(**profile)))
    assert fuse_thread_suffix(graph, changed, policy=policy, bounds=bounds, hosts=(host,)) == changed


def test_real_ray_async_pipeline_uses_original_result_ack_and_cleanup(tmp_path, monkeypatch, private_ray_provider):
    from demiflow.execution.dataflow import DataflowExecution
    from demiflow.data.api import _use_executor
    from demiflow.execution.executors.ray import RayDatasetExecutor
    captured = []
    original = DataflowExecution.start
    def capture(execution):
        assert execution._policy.provider == 'ray_core'
        captured.append(execution)
        return original(execution)
    monkeypatch.setattr(DataflowExecution, 'start', capture)
    executor = RayDatasetExecutor(workers=4)
    try:
        with RunContext.start(tmp_path / 'run') as run, _use_executor(executor):
            assert list(_pipeline(8).iter_rows()) == [{'id': i * 2} for i in range(1, 9)]
            assert run.journal.execution_commits('action-1')[-1]['status'] == 'acked'
        execution, = captured
        assert len(execution._physical.stages) == 2
        assert not execution._endpoints and not execution._active_worker_allocations
    finally:
        executor.close()
