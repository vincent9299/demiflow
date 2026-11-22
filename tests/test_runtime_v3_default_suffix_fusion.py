"""Default narrow pipelines keep the source independent without parking each map."""
from dataclasses import replace
import time

import pytest

from demiflow import data
from demiflow.execution.contracts.dataflow import ActionRequest, HostDescriptor
from demiflow.execution.action_driver.dataflow import DataflowExecution
from demiflow.execution.dataflow_api import _CompileContext
from demiflow.execution.run_context import RunAction, RunContext
from demiflow.planning.dataflow_compile import lower_action
from demiflow.planning.dataflow_place import partition_stages
from demiflow.planning.dataflow_policy import resolve_runtime_policy
from demiflow.planning.dataflow_thread_fusion import fuse_thread_suffix
from demiflow.resources.contracts import ResourceProfile

HOSTS = (HostDescriptor(host_id='local', provider='local', runtime_revision='test',
    store_domains=(), logical_resources={'cpu_millis': 16000, 'memory_bytes': 2**30},
    isolation_capabilities=()),)


def pipeline():
    return (data.range(31).map(lambda row: {'a': row['id']})
        .flat_map(lambda row: [row, {'a': row['a'] + 100}])
        .filter(lambda row: row['a'] % 2 == 0)
        .map_batches(lambda batch: [{'n': len(batch), 'v': int(batch['a'].sum())}],
                     batch_size=5, batch_format='pandas').limit(3))


def compiled(workers=4, mode='process', *, streaming=False, dataset=None):
    dataset = pipeline() if dataset is None else dataset
    if streaming:
        dataset = dataset.map_async(lambda row: row, concurrency=1)
    action = RunAction(_CompileContext(), 'default-suffix', {})
    request = ActionRequest(kind='take_all', source=dataset._source, plan=dataset._plan,
        arguments={}, sink=None, programs=(), provider='local', action=action)
    policy, bounds = resolve_runtime_policy({'max_active_workers': workers, 'worker_mode': mode}, {}, provider='local')
    from demiflow.planning.dataflow_budget import minimum_workspace_graph
    graph = minimum_workspace_graph(lower_action(request, bounds=bounds).graph)
    return graph, partition_stages(graph, policy=policy, bounds=bounds), policy, bounds


def fused(graph, stages, policy, bounds, *, hosts=HOSTS):
    return fuse_thread_suffix(graph, stages, policy=policy, bounds=bounds, hosts=hosts)


@pytest.mark.parametrize('workers', [2, 3, 4, 6])
@pytest.mark.parametrize('mode', ['process', 'thread'])
def test_insufficient_worker_budget_uses_one_independent_source_and_original_suffix(workers, mode):
    graph, stages, policy, bounds = compiled(workers, mode)
    assert len(stages) == 7
    result = fused(graph, stages, policy, bounds)
    assert len(result) == 2
    source = next(stage for stage in result if graph.sources[0] in stage.occurrences)
    assert source.occurrences == graph.sources
    suffix = next(stage for stage in result if graph.terminals[0] in stage.occurrences)
    members = [stage for stage in stages if stage is not source]
    assert suffix.occurrences == tuple(occ for stage in members for occ in stage.occurrences)
    assert suffix.profile.requests['cpu_millis'] == sum(stage.profile.requests['cpu_millis'] for stage in members)
    edges = sum(edge.producer in suffix.occurrences and edge.consumer in suffix.occurrences
                for edge in graph.edges)
    assert suffix.resident_bytes == suffix.profile.requests['memory_bytes'] == sum(
        max(stage.resident_bytes, stage.profile.requests['memory_bytes']) for stage in members) - edges * bounds.max_row_bytes
    assert suffix.state_bytes == sum(stage.state_bytes for stage in members)
    assert suffix.kernel_refs == tuple(dict.fromkeys(ref for stage in members for ref in stage.kernel_refs))
    # Co-location changes no logical node/edge or original task/batch declaration.
    assert {occ for stage in result for occ in stage.occurrences} == {node.occurrence_id for node in graph.nodes}


def test_optional_fusion_preserves_individual_host_placement_and_explicit_grants():
    graph, stages, policy, bounds = compiled()
    hosts = tuple(replace(HOSTS[0], host_id=f'host-{i}', provider='ray_core',
        logical_resources={'cpu_millis': 2000, 'memory_bytes': 2**30}) for i in range(4))
    assert fused(graph, stages, policy, bounds, hosts=hosts) == stages
    small = tuple(replace(host, logical_resources={'cpu_millis': 16000,
        'memory_bytes': max(stage.resident_bytes for stage in stages)}) for host in hosts)
    assert fused(graph, stages, policy, bounds, hosts=small) == stages
    for offset in (-1, -2):  # Protect both the terminal and a user transform.
        protected = list(stages)
        protected[offset] = replace(protected[offset], profile=ResourceProfile(
            requests=dict(protected[offset].profile.requests), requirements={'isolation': 'process'}))
        result = fused(graph, tuple(protected), policy, bounds)
        assert protected[offset] in result
        assert all(not set(protected[offset].occurrences) & set(stage.occurrences)
                   for stage in result if stage != protected[offset])
    assert fused(graph, stages, replace(policy, fusion=False), bounds) == stages
    assert fused(graph, stages, replace(policy, max_active_workers=7), bounds) == stages
    assert fused(graph, stages, policy, replace(bounds, max_resident_bytes=2**20)) == stages


def test_ray_policy_keeps_independently_placeable_stages():
    graph, stages, policy, bounds = compiled()
    policy = replace(policy, provider='ray_core')
    assert fused(graph, stages, policy, bounds) == stages
    hosts = tuple(replace(HOSTS[0], host_id=f'ray-{i}', provider='ray_core',
        logical_resources={'cpu_millis': 2000, 'memory_bytes': 2**30}) for i in range(4))
    assert fused(graph, stages, policy, bounds, hosts=hosts) == stages



def test_new_default_rule_does_not_absorb_shared_sources_or_gpu_stages():
    shared = data.range(2)
    branch = shared.map(lambda row: row).union(shared.map(lambda row: row))
    for _ in range(3):
        branch = branch.map(lambda row: row)
    graph, stages, policy, bounds = compiled(dataset=branch)
    assert len(stages) > policy.max_active_workers
    result = fused(graph, stages, policy, bounds)
    source = next(stage for stage in result if graph.sources[0] in stage.occurrences)
    assert source.occurrences == graph.sources  # The shared source still has one independent owner.
    suffix = next(stage for stage in result if graph.terminals[0] in stage.occurrences)
    assert len(result) == 2
    assert len(set(occ for stage in result for occ in stage.occurrences)) == len(graph.nodes)
    assert sum(len(stage.occurrences) for stage in result) == len(graph.nodes)
    members = [stage for stage in stages if stage is not source]
    assert suffix.profile.requests['cpu_millis'] == max(stage.profile.requests['cpu_millis'] for stage in members)
    assert suffix.profile.requests['memory_bytes'] < sum(
        max(stage.resident_bytes, stage.profile.requests['memory_bytes']) for stage in members)
    assert fused(graph, stages, replace(policy, fusion=False), bounds) == stages
    gpu = pipeline().map(lambda row: row, backend_options={'ray': {'worker_resources': {'gpu': 1}}})
    graph, stages, policy, bounds = compiled(dataset=gpu)
    original = next(stage for stage in stages if any(
        node.kernel.resource_role == 'gpu' and node.occurrence_id in stage.occurrences
        for node in graph.nodes))
    assert original.profile.requests['accelerators']
    assert fused(graph, stages, policy, bounds) == stages

def test_default_public_pipeline_avoids_per_stage_parking_with_real_workers(tmp_path, monkeypatch):
    plans, allocations = [], []
    prepare, launch = DataflowExecution.prepare, DataflowExecution._prepare_launch
    def preparing(execution):
        value = prepare(execution)
        plans.append((execution._graph, execution._physical))
        return value
    def launching(execution, *args, **kwargs):
        value = launch(execution, *args, **kwargs)
        allocations.append(value)
        return value
    monkeypatch.setattr(DataflowExecution, 'prepare', preparing)
    monkeypatch.setattr(DataflowExecution, '_prepare_launch', launching)
    started = time.monotonic()
    with RunContext.start(tmp_path / 'run') as run:
        dataset = pipeline()  # No local_execution worker-count workaround.
        assert dataset.take_all() == [{'n': 5, 'v': 208}, {'n': 5, 'v': 332}, {'n': 5, 'v': 258}]
        assert run.journal.execution_commits('action-1')[-1]['status'] == 'acked'
    graph, physical = plans[0]
    assert physical.policy.max_active_workers == 4
    assert len(allocations) == len(physical.stages) == 2
    assert all(edge.mode == 'memory_incremental' for edge in physical.exchanges)
    assert next(stage for stage in physical.stages if graph.sources[0] in stage.occurrences).occurrences == graph.sources
    # Structural assertions lock the cause; report duration without a noisy wall-clock oracle.
    (tmp_path / 'performance.txt').write_text(f'{time.monotonic() - started:.3f}s; allocations={len(allocations)}\n')


def test_default_fused_actor_retains_original_task_reconstruction(tmp_path):
    from test_runtime_v3_task_scope import Number
    from test_dataset_native import sealed_plan
    from _worker_event_fixtures import events
    path = str(tmp_path / 'actor-calls')
    limits = {'max_row_bytes': 65536, 'max_batch_bytes': 65536}
    with RunContext.start(tmp_path / 'run', limits=limits), data.local_execution(batch_rows=3):
        dataset = (data.range(7).map(lambda row: {'i': row['id']})
            .map(Number, fn_constructor_args=[path]).map(lambda row: row)
            .map(lambda row: row).limit(7))
        assert [row['within_task'] for row in dataset.take_all()] == [1, 2, 3, 1, 2, 3, 1]
        graph, physical, _ = sealed_plan(dataset)
        assert physical.policy.max_active_workers == 4 and len(physical.stages) == 2
        assert next(stage for stage in physical.stages if graph.sources[0] in stage.occurrences).occurrences == graph.sources
    observed = events(path)
    assert sum(item['event'] == 'construct' for item in observed) == 3
    assert sum(item['event'] == 'close' for item in observed) == 3
    assert [item['row'] for item in observed if item['event'] == 'call'] == list(range(7))
