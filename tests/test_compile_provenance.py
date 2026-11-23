"""Provenance is a bounded observation of the one final compiler result."""
from dataclasses import replace
from types import SimpleNamespace
import sys

import pytest

from demiflow import data
from demiflow.data.dataset import Dataset
from demiflow.execution.contracts.dataflow import ActionRequest
from demiflow.execution.contracts.dataflow_codec import encode_wire
from demiflow.execution.compiler_support.dataflow_compile_inputs import CompileInputs, ReportBindings
from demiflow.execution.plan_capture import capture_pipeline, current_capture
from demiflow.planning import dataflow_compile, dataflow_provenance
from test_dataflow_compile_inputs import _request, BOUNDS
from test_plan_capture import no_execution, forbidden
from test_workbench_operator_contracts import compile_code
import compile_contract_corpus as corpus


def test_observation_never_enters_compiled_identity_or_report(no_execution):
    rows = data.range(2).map(forbidden).filter(forbidden)
    request = _request(rows)
    inputs = CompileInputs(report=ReportBindings(corpus.report_for(rows._source, rows._plan, request.action.action_id)))
    plain = dataflow_compile.lower_action(request, bounds=BOUNDS, inputs=inputs)
    events = []
    observed = dataflow_compile.lower_action(request, bounds=BOUNDS, inputs=inputs, on_compiled=events.append)
    assert observed == plain
    assert encode_wire(observed.graph, type_name='DataflowGraph', bounds=BOUNDS) == encode_wire(
        plain.graph, type_name='DataflowGraph', bounds=BOUNDS)
    assert len(events) == 1 and events[0].unavailable is None
    event = events[0]
    assert (event.action_id, event.dag_id) == (observed.graph.action_id, observed.graph.dag_id)
    assert event.terminals == observed.graph.terminals
    assert {b.occurrence_id for b in event.bindings} == {n.occurrence_id for n in observed.graph.nodes[:-1]}


def test_no_observer_does_not_construct_provenance(no_execution, monkeypatch):
    monkeypatch.setattr(dataflow_provenance, 'compiled_provenance', forbidden)
    captured = capture_pipeline(lambda: data.range(1).take_all(), {})
    assert len(captured.compiled.graph.nodes) == 2


def test_compiler_local_rename_cannot_remove_real_sdk_links(tmp_path, no_execution, monkeypatch):
    code = '''from demiflow import data
def unused(row): raise AssertionError('UDF must not execute')
def run():
    return data.range(2).map(unused).take_all()
'''
    original, before, _ = compile_code(tmp_path, code)
    function = dataflow_compile.capture_graph
    renamed = function.__code__.replace(co_varnames=tuple(
        'compiler_state' if name == 'capture' else name for name in function.__code__.co_varnames))
    monkeypatch.setattr(function, '__code__', renamed)
    captured, details, _ = compile_code(tmp_path, code)
    assert captured.compiled.graph == original.compiled.graph
    assert [d['callsite']['text'] for d in details.values()] == [d['callsite']['text'] for d in before.values()]
    assert all(d['callsite']['availability'] == 'available' for d in details.values())
    assert details[captured.compiled.graph.terminals[0]]['callsite']['metadata']['api'] == 'Dataset.take_all'


def test_one_operation_object_keeps_all_real_occurrences(no_execution):
    first = data.range(2).map(forbidden)
    other = data.range(3)
    second = Dataset(other._source, first._plan, other._executor)
    events = []
    captured = capture_pipeline(lambda: first.union(second).take_all(), {}, on_compiled=events.append)
    links = [b for b in events[0].bindings if b.object_id == id(first._plan.operations[0])]
    assert len(links) == 2 and len({b.occurrence_id for b in links}) == 2
    assert {b.occurrence_id for b in links} == {
        n.occurrence_id for n in captured.compiled.graph.nodes if n.kernel.kind == 'MapOp'}


class PassiveSharedActor:
    def __call__(self, row):
        raise AssertionError('shared actor must not execute')


def test_shared_actor_keeps_final_identity_and_each_occurrence(no_execution):
    actor = PassiveSharedActor()
    rows = data.range(1).map(actor).map(actor)
    request = _request(rows)
    events = []
    compiled = dataflow_compile.lower_action(request, bounds=BOUNDS, on_compiled=events.append)
    shared = [n for n in compiled.graph.nodes if 'shared_actor' in n.kernel.configuration]
    assert len(shared) == 2
    ids = {n.occurrence_id for n in shared}
    assert all(set(n.kernel.configuration['shared_actor']['members']) == ids for n in shared)
    assert ids <= {b.occurrence_id for b in events[0].bindings}
    assert compiled == dataflow_compile.lower_action(request, bounds=BOUNDS)


def test_observer_runs_after_graph_decoration_shared_actor_binding_and_validation(no_execution, monkeypatch):
    from demiflow.planning import dataflow_graph_adapter, dataflow_shared
    finished = []
    for module, name in ((dataflow_graph_adapter, 'decorate_graph_capture'),
                         (dataflow_shared, 'bind_shared_actors'), (dataflow_compile, 'validate_graph')):
        original = getattr(module, name)
        def wrapper(*args, _original=original, _name=name, **kwargs):
            result = _original(*args, **kwargs)
            finished.append(_name)
            return result
        monkeypatch.setattr(module, name, wrapper)
    built = corpus._graph(SimpleNamespace(rows=lambda: data.from_items([{'x': 1}])), 'broadcast')
    request = ActionRequest(kind=built['kind'], source=built['source'], plan=built['plan'],
        arguments=built['arguments'], sink=None, programs=(), provider='local',
        action=SimpleNamespace(action_id='provenance-adapter'))
    events = []
    def observe(event):
        assert finished == ['decorate_graph_capture', 'bind_shared_actors', 'validate_graph']
        events.append(event)
    compiled = dataflow_compile.lower_action(request, bounds=BOUNDS, on_compiled=observe)
    event = events[0]
    assert event.unavailable is None and event.terminals == compiled.graph.terminals
    assert event.dag_id == compiled.graph.dag_id
    assert all(b.occurrence_id in {n.occurrence_id for n in compiled.graph.nodes} for b in event.bindings)
    assert any('graph_result' in n.kernel.configuration for n in compiled.graph.nodes)


@pytest.mark.parametrize('nodes, aliases', [(257, 0), (2, 1025)])
def test_budget_is_checked_before_alias_iteration_or_copy(nodes, aliases):
    class NoIteration:
        def __len__(self):
            return aliases
        def items(self):
            pytest.fail('oversized aliases were traversed')
    graph = SimpleNamespace(action_id='preview', dag_id='a' * 64, nodes=[None] * nodes, terminals=())
    event = dataflow_provenance.compiled_provenance(graph, sources=NoIteration(), operations={})
    assert event.unavailable and not event.bindings and not event.terminals


def test_provenance_budget_does_not_reject_or_truncate_the_compiled_graph(no_execution, monkeypatch):
    rows = data.range(2).map(forbidden)
    request = _request(rows)
    plain = dataflow_compile.lower_action(request, bounds=BOUNDS)
    monkeypatch.setattr(dataflow_provenance, 'MAX_BINDINGS', 1)
    events = []
    result = dataflow_compile.lower_action(request, bounds=BOUNDS, on_compiled=events.append)
    assert result == plain and len(result.graph.nodes) == 3
    assert events[0].unavailable and events[0].bindings == ()


def test_observer_failure_cleans_capture_and_profile_and_never_executes(tmp_path, no_execution):
    from demiflow.workbench.graph.plan_callsite import PlanCallsites
    def failed(event):
        raise RuntimeError('observer failed')
    with pytest.raises(RuntimeError, match='observer failed'), PlanCallsites(tmp_path, {}):
        capture_pipeline(lambda: data.range(1).take_all(), {}, on_compiled=failed)
    assert current_capture() is None and sys.getprofile() is None
    events = []
    with pytest.raises(Exception):
        capture_pipeline(lambda: None, {}, on_compiled=events.append)
    assert events == [] and current_capture() is None


def test_dead_weak_origin_and_other_graph_are_not_used(tmp_path, no_execution):
    from demiflow.workbench.graph.plan_callsite import PlanCallsites
    from demiflow.workbench.graph.plan_details import build_plan_details
    class Origin:
        pass
    origin = Origin()
    calls = PlanCallsites(tmp_path, {})
    calls._remember(origin, ('flow.py', (1, 1, 0, 4), 'DataAPI.range', []))
    key = id(origin)
    del origin
    captured = capture_pipeline(lambda: data.range(1).take_all(), {})
    graph = captured.compiled.graph
    event = dataflow_provenance.CompilationProvenance(graph.action_id, graph.dag_id,
        (dataflow_provenance.OriginBinding('source', key, graph.sources[0]),), graph.terminals)
    calls.bind_compilation(event)
    assert calls.detail(graph.sources[0])['availability'] == 'unavailable'
    other = replace(captured.compiled, graph=replace(graph, dag_id='0' * 64))
    with pytest.raises(ValueError, match='this compiled graph'):
        build_plan_details(other, root=tmp_path, sources={}, callsites=calls)


def test_existing_profile_is_preserved_without_inventing_call_locations(tmp_path, no_execution):
    from demiflow.workbench.graph.plan_callsite import PlanCallsites
    profile = lambda *args: None
    previous = sys.getprofile()
    sys.setprofile(profile)
    try:
        with PlanCallsites(tmp_path, {}) as calls:
            captured = capture_pipeline(lambda: data.range(1).take_all(), {}, on_compiled=calls.bind_compilation)
        assert sys.getprofile() is profile
        assert all(calls.detail(n.occurrence_id)['availability'] == 'unavailable' for n in captured.compiled.graph.nodes)
    finally:
        sys.setprofile(previous)
