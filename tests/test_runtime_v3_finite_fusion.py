"""Finite CPU handoff can be local without erasing logical or resource owners."""
from dataclasses import replace

from demiflow import data
from demiflow.execution.contracts.dataflow import ActionRequest, HostDescriptor
from demiflow.execution.dataflow_api import _CompileContext
from demiflow.execution.run_context import RunAction
from demiflow.planning.dataflow_compile import lower_action
from demiflow.planning.dataflow_finite_fusion import fuse_finite_suffix
from demiflow.planning.dataflow_place import partition_stages
from demiflow.planning.dataflow_policy import resolve_runtime_policy
from demiflow.resources.contracts import ResourceProfile

HOSTS = (HostDescriptor(host_id='local', provider='local', runtime_revision='test',
    store_domains=(), logical_resources={'cpu_millis': 8000, 'memory_bytes': 2**30},
    isolation_capabilities=()),)


def compiled(*, asynchronous=False):
    ds = data.from_items([{'k': 1}]).join(data.from_items([{'k': 1, 'v': b'wide'}]), on='k')
    ds = ds.map_async(lambda row: row, concurrency=1) if asynchronous else ds.map(lambda row: row)
    action = RunAction(_CompileContext(), 'finite-fusion', {})
    request = ActionRequest(kind='take_all', source=ds._source, plan=ds._plan,
        arguments={}, sink=None, programs=(), provider='local', action=action)
    policy, bounds = resolve_runtime_policy({}, {}, provider='local')
    from demiflow.planning.dataflow_budget import minimum_workspace_graph
    graph = minimum_workspace_graph(lower_action(request, bounds=bounds).graph)
    return graph, partition_stages(graph, policy=policy, bounds=bounds), policy, bounds


def test_wide_finite_handoff_keeps_original_inputs_and_summed_grants():
    graph, stages, policy, bounds = compiled()
    fused = fuse_finite_suffix(graph, stages, policy=policy, bounds=bounds, hosts=HOSTS)
    by_occurrence = {occ: stage for stage in stages for occ in stage.occurrences}
    join = next(node.occurrence_id for node in graph.nodes if node.kernel.kind == 'join')
    stage = next(stage for stage in fused if join in stage.occurrences)
    assert len(stage.occurrences) == 3  # Original join, map and terminal kernels.
    assert graph.terminals[0] in stage.occurrences
    assert not set(graph.sources) & set(stage.occurrences)
    assert len(fused) == 3
    assert stage.profile.requests['cpu_millis'] == sum(
        by_occurrence[occ].profile.requests['cpu_millis'] for occ in stage.occurrences)
    assert stage.profile.requests['memory_bytes'] == sum(
        max(by_occurrence[occ].resident_bytes, by_occurrence[occ].profile.requests['memory_bytes'])
        for occ in stage.occurrences)
    assert {occ for stage in fused for occ in stage.occurrences} == set(by_occurrence)


def test_finite_fusion_respects_disable_budget_and_isolation():
    graph, stages, policy, bounds = compiled()
    assert fuse_finite_suffix(graph, stages, policy=replace(policy, fusion=False), bounds=bounds, hosts=HOSTS) == stages
    assert fuse_finite_suffix(graph, stages, policy=policy,
        bounds=replace(bounds, max_resident_bytes=128 * 1024), hosts=HOSTS) == stages
    join = next(node.occurrence_id for node in graph.nodes if node.kernel.kind == 'join')
    protected = tuple(replace(stage, profile=ResourceProfile(requests=dict(stage.profile.requests),
        requirements={'isolation': 'process'})) if join in stage.occurrences else stage for stage in stages)
    assert fuse_finite_suffix(graph, protected, policy=policy, bounds=bounds, hosts=HOSTS) == protected


def test_finite_fusion_does_not_absorb_asynchronous_consumers():
    graph, stages, policy, bounds = compiled(asynchronous=True)
    assert fuse_finite_suffix(graph, stages, policy=policy, bounds=bounds, hosts=HOSTS) == stages


def test_finite_fusion_preserves_distributed_placement_when_no_host_fits():
    graph, stages, policy, bounds = compiled()
    hosts = tuple(replace(HOSTS[0], host_id=f'host-{i}', provider='ray_core',
        logical_resources={'cpu_millis': 2000, 'memory_bytes': 2**30}) for i in range(2))
    # Each original 1 CPU stage fits, and the cluster has enough CPUs, but
    # the fused join/map/terminal would require 3 CPUs on one host.
    assert fuse_finite_suffix(graph, stages, policy=policy, bounds=bounds, hosts=hosts) == stages
    small_memory = tuple(replace(host, logical_resources={'cpu_millis': 8000,
        'memory_bytes': max(stage.resident_bytes for stage in stages)}) for host in hosts)
    assert fuse_finite_suffix(graph, stages, policy=policy, bounds=bounds, hosts=small_memory) == stages
