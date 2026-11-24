"""Default codec slack must coexist without changing fixed payload contracts."""
from dataclasses import replace

import pytest

from demiflow import data
from demiflow.data.native_options import NativeOptions
from demiflow.data.plan import CallableSpec, LogicalPlan, MapOp
from demiflow.execution.contracts.dataflow import ActionRequest
from demiflow.execution.contracts.dataflow_errors import DataflowError
from demiflow.execution.executors.dataflow_api import _CompileContext
from demiflow.execution.pipeline.run_context import RunAction
from demiflow.planning.dataflow_compile import lower_action
from demiflow.planning.dataflow_policy import resolve_runtime_policy


def request(dataset):
    return ActionRequest(kind='take_all', source=dataset._source, plan=dataset._plan,
        arguments={}, sink=None, programs=(), provider='local',
        action=RunAction(_CompileContext(), 'codec-components', {}))


def physical(graph, policy, bounds):
    from demiflow.execution.contracts.dataflow import SourceSnapshot
    from demiflow.planning.dataflow_place import compile_physical_dataflow
    from test_runtime_v3_p import _constants, _host
    snapshots = tuple(SourceSnapshot(occ, 'items', {}, True, 'seek', None) for occ in graph.sources)
    return compile_physical_dataflow(graph, snapshots, (), policy=policy, bounds=bounds,
                                     hosts=(_host(),), constants_ref=_constants())


@pytest.mark.parametrize('memory,row,batch', [(192,8,32),(256,16,32),(80,8,20)])
def test_fixed_payloads_and_terminal_metadata_fit_the_same_stream_group(memory,row,batch):
    policy,bounds=resolve_runtime_policy({}, {'memory_bytes':memory*1024,
        'max_row_bytes':row*1024,'max_batch_bytes':batch*1024}, provider='local')
    action=request(data.from_items([{'value':1}]))
    graph = lower_action(action, bounds=bounds).graph
    selected = physical(graph, policy, bounds)
    assert selected.bounds == bounds
    control=min(bounds.max_control_bytes,bounds.max_resident_bytes//8)
    assert len(selected.residency.simultaneous_groups) == 1
    assert sum(s.profile.requests['memory_bytes'] for s in selected.stages) <= memory*1024-control
    terminal=next(n.kernel for n in graph.nodes if n.kind=='terminal')
    contract=terminal.configuration['terminal_memory']
    assert contract['metadata_bytes']>=3072
    assert contract['payload_bytes']>=bounds.max_batch_bytes+bounds.max_row_bytes
    assert sum(contract.values())==terminal.profile.requests['memory_bytes']


def test_finite_boundary_does_not_charge_all_phases_or_activate_generator():
    def forbidden_source():
        raise AssertionError('compile activated iterable')
        yield {}
    dataset=data.from_iter(forbidden_source).limit(2).sort('n').sort('n')
    policy,bounds=resolve_runtime_policy({}, {'memory_bytes':80*1024,'max_row_bytes':8*1024,
        'max_batch_bytes':16*1024,'max_state_bytes':128*1024}, provider='local')
    graph=lower_action(request(dataset),bounds=bounds).graph
    selected = physical(graph, policy, bounds)
    stages = {s.stage_id: s for s in selected.stages}
    assert len(selected.residency.simultaneous_groups) >= 2
    for group in selected.residency.simultaneous_groups:
        assert sum(stages[name].profile.requests['memory_bytes'] for name in group) <= 70*1024
    assert sum(s.profile.requests['memory_bytes'] for s in stages.values()) > bounds.max_resident_bytes
    assert sum(p.requirement=='finite_complete' for n in graph.nodes for p in n.kernel.inputs)==2


def test_explicit_worker_memory_and_too_small_state_are_never_increased_or_redistributed():
    dataset=data.from_items([{'n':1}])
    operation=MapOp(CallableSpec.create(lambda row:row),
        native_options=NativeOptions('ray',worker_resources={'memory_bytes':128*1024}))
    action=replace(request(dataset),plan=LogicalPlan((operation,)))
    _,bounds=resolve_runtime_policy({}, {'memory_bytes':192*1024,'max_row_bytes':8*1024,
        'max_batch_bytes':32*1024}, provider='local')
    graph=lower_action(action,bounds=bounds).graph
    kernel=next(n.kernel for n in graph.nodes if n.kind=='transform')
    assert kernel.profile.requests['memory_bytes']==128*1024
    with pytest.raises(DataflowError,match='terminal state budget'):
        lower_action(action,bounds=replace(bounds,max_state_bytes=3071))


def test_optional_codec_slack_preserves_placeable_multi_host_group():
    from types import SimpleNamespace
    from demiflow.execution.contracts.dataflow import SourceSnapshot
    from demiflow.execution.scheduler.placement import DataflowScheduler
    from demiflow.planning.dataflow_place import compile_physical_dataflow
    from test_runtime_v3_p import _host, _constants

    policy, bounds = resolve_runtime_policy({'fusion': False}, {'memory_bytes': 2**20,
        'max_row_bytes': 8192, 'max_batch_bytes': 24576}, provider='local')
    graph = lower_action(request(data.from_items([{'n': 1}]).map(lambda row: row)), bounds=bounds).graph
    snapshots = tuple(SourceSnapshot(occ, 'items', {}, True, 'seek', None) for occ in graph.sources)
    hosts = tuple(replace(_host(name), logical_resources={'cpu_millis': 16000,
        'memory_bytes': 65536, 'gpu': 0}) for name in ('first', 'second'))
    selected = compile_physical_dataflow(graph, snapshots, (), policy=policy, bounds=bounds,
                                         hosts=hosts, constants_ref=_constants())
    scheduler = DataflowScheduler(selected, resources=SimpleNamespace(), hosts=hosts)
    assert len(selected.residency.simultaneous_groups) == 1
    placed = scheduler._place_demands([(stage.stage_id, stage.profile, stage.stage_id, stage.resident_bytes)
                                      for stage in selected.stages])
    assert set(placed.values()) == {'first', 'second'}
