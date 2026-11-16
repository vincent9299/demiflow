"""Default codec slack must coexist without changing fixed payload contracts."""
from dataclasses import replace

import pytest

from demiflow import data
from demiflow.data.native_options import NativeOptions
from demiflow.data.plan import CallableSpec, LogicalPlan, MapOp
from demiflow.execution.contracts.dataflow import ActionRequest
from demiflow.execution.contracts.dataflow_errors import DataflowError
from demiflow.execution.dataflow_api import _CompileContext
from demiflow.execution.run_context import RunAction
from demiflow.planning.dataflow_compile import lower_action
from demiflow.planning.dataflow_policy import fit_default_stream_windows, resolve_runtime_policy


def request(dataset):
    return ActionRequest(kind='take_all', source=dataset._source, plan=dataset._plan,
        arguments={}, sink=None, programs=(), provider='local',
        action=RunAction(_CompileContext(), 'codec-components', {}))


@pytest.mark.parametrize('memory,row,batch', [(192,8,32),(256,16,32)])
def test_fixed_payloads_and_terminal_metadata_fit_the_same_stream_group(memory,row,batch):
    policy,bounds=resolve_runtime_policy({}, {'memory_bytes':memory*1024,
        'max_row_bytes':row*1024,'max_batch_bytes':batch*1024}, provider='local')
    action=request(data.from_items([{'value':1}]))
    fitted=fit_default_stream_windows(action,policy=policy,bounds=bounds,
        explicit_limits=('max_row_bytes','max_batch_bytes'))
    assert fitted == bounds  # No state-shrinking loop can make room for two fixed codec targets.
    graph=lower_action(action,bounds=fitted).graph
    control=min(bounds.max_control_bytes,bounds.max_resident_bytes//8)
    assert sum(n.kernel.profile.requests['memory_bytes'] for n in graph.nodes)<=memory*1024-control
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
    _,bounds=resolve_runtime_policy({}, {'memory_bytes':80*1024,'max_row_bytes':8*1024,
        'max_batch_bytes':16*1024,'max_state_bytes':128*1024}, provider='local')
    graph=lower_action(request(dataset),bounds=bounds).graph
    source=next(n for n in graph.nodes if n.kind=='source')
    limit=next(n for n in graph.nodes if n.kernel.kind=='LimitOp')
    assert source.kernel.profile.requests['memory_bytes']+limit.kernel.profile.requests['memory_bytes']<=70*1024
    assert sum(n.kernel.profile.requests['memory_bytes'] for n in graph.nodes)>bounds.max_resident_bytes
    assert all(n.kernel.profile.requests['memory_bytes']>=24*1024 for n in graph.nodes)
    assert sum(p.requirement=='finite_complete' for n in graph.nodes for p in n.kernel.inputs)==2


def test_explicit_worker_memory_and_too_small_state_are_never_increased_or_redistributed():
    dataset=data.from_items([{'n':1}])
    operation=MapOp(CallableSpec.create(lambda row:row),
        native_options=NativeOptions('local',worker_resources={'memory_bytes':128*1024}))
    action=replace(request(dataset),plan=LogicalPlan((operation,)))
    _,bounds=resolve_runtime_policy({}, {'memory_bytes':192*1024,'max_row_bytes':8*1024,
        'max_batch_bytes':32*1024}, provider='local')
    graph=lower_action(action,bounds=bounds).graph
    kernel=next(n.kernel for n in graph.nodes if n.kind=='transform')
    assert kernel.profile.requests['memory_bytes']==128*1024
    with pytest.raises(DataflowError,match='terminal state budget'):
        lower_action(action,bounds=replace(bounds,max_state_bytes=3071))
