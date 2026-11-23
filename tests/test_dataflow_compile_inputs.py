"""The compiler's inputs are explicit: request, bounds and CompileInputs.

``compile_inputs(request)`` is the one place that reads the action's executor
and the report context. ``lower_action`` resolves it once at entry when the
caller passes none, and uses exactly what it is given when the caller passes one.
"""
from demiflow import data
from demiflow.execution import checked
from demiflow.execution.contracts.dataflow import ActionRequest
from demiflow.execution.contracts.dataflow_codec import encode_wire
from demiflow.execution.compiler_support.dataflow_compile_inputs import CompileDefaults, CompileInputs, ReportBindings, compile_inputs, executor_defaults
from demiflow.planning.dataflow_compile import lower_action
from demiflow.planning.dataflow_policy import resolve_runtime_policy

import compile_contract_corpus as corpus
from compile_contract_functions import batch, identity, keep

BOUNDS = resolve_runtime_policy({}, {}, provider='local')[1]


def _request(dataset, kind='take', arguments=None, executor=None, action_id='inputs'):
    action = type('Action', (), {'action_id': action_id})()
    if executor is not None:
        action._dataflow_executor = executor
    return ActionRequest(kind=kind, source=dataset._source, plan=dataset._plan,
                         arguments={'limit': 2} if arguments is None else arguments, sink=None, programs=(),
                         provider='local', action=action)


def _bytes(compiled):
    return encode_wire(compiled.graph, type_name='DataflowGraph', bounds=BOUNDS)


def _configuration(compiled, kind):
    return next(node.kernel.configuration for node in compiled.graph.nodes if node.kernel.kind == kind)


def test_executor_defaults_are_three_attribute_reads():
    assert executor_defaults(None) == CompileDefaults() == CompileDefaults(None, None, None)
    assert executor_defaults(object()) == CompileDefaults()
    assert executor_defaults(corpus.EXECUTORS['local']()) == CompileDefaults(batch_rows=512, partitions=3)
    assert executor_defaults(corpus.EXECUTORS['block_only']()) == CompileDefaults(block_size=128)
    assert executor_defaults(corpus.EXECUTORS['local_without_rows']()) == CompileDefaults(block_size=64)


def test_fallbacks_are_the_ones_the_compiler_applied_before():
    assert CompileDefaults().map_batch_size is None and CompileDefaults().task_batch_rows == 8192
    assert CompileDefaults(batch_rows=512, block_size=64).map_batch_size == 512
    assert CompileDefaults(batch_rows=0, block_size=64).map_batch_size == 64      # zero falls through, as 'or' did
    assert CompileDefaults(batch_rows=None, block_size=64).task_batch_rows == 8192  # the block size never sets task rows
    assert CompileDefaults(batch_rows=512).task_batch_rows == 512


def test_resolution_reads_the_action_executor_and_the_report_context():
    dataset = data.from_items([{'x': 1}]).map_batches(batch)
    assert compile_inputs(_request(dataset)) == CompileInputs()
    resolved = compile_inputs(_request(dataset, executor=corpus.EXECUTORS['local']()))
    assert resolved.defaults == CompileDefaults(batch_rows=512, partitions=3) and resolved.report is None
    token = checked._CURRENT.set(corpus.report_for(dataset._source, dataset._plan, 'inputs'))
    try:
        assert isinstance(compile_inputs(_request(dataset)).report, ReportBindings)
    finally:
        checked._CURRENT.reset(token)
    assert compile_inputs(_request(dataset)).report is None


def test_source_schema_is_lazy_and_shared_leaf_is_inspected_once(monkeypatch):
    import pyarrow as pa
    import demiflow.planning.dataflow_compile as compiler
    from demiflow.execution.sources.materialized_input import literal_schema
    source = data.from_arrow(pa.table({'x': [1, 2]}))
    request = _request(source.union(source))
    expected = _bytes(lower_action(request, bounds=BOUNDS))
    assert CompileInputs().source_schema is literal_schema
    assert compile_inputs(request).source_schema is literal_schema
    events = []
    semantic = compiler._source_semantic_facts
    def facts(value):
        events.append(('semantic', value))
        return semantic(value)
    def schema(value, *, bounds):
        events.append(('schema', value))
        return literal_schema(value, bounds=bounds)
    def no_ambient_context():
        raise AssertionError('explicit compile inputs must not read ambient context')
    monkeypatch.setattr(compiler, '_source_semantic_facts', facts)
    monkeypatch.setattr(checked, 'current', no_ambient_context)
    inputs = CompileInputs(source_schema=schema)
    assert events == []
    compiled = lower_action(request, bounds=BOUNDS, inputs=inputs)
    assert events == [('semantic', source._source), ('schema', source._source),
                      ('semantic', request.source)]
    assert _bytes(compiled) == expected


def test_empty_take_and_invalid_terminal_never_inspect_original_source():
    from dataclasses import replace
    import pytest
    from demiflow.execution.contracts.dataflow_errors import DataflowError
    from demiflow.execution.sources.materialized_input import literal_schema
    source = data.from_items([{'x': 1}])
    seen = []
    def schema(value, *, bounds):
        seen.append(value)
        assert value is not source._source
        return literal_schema(value, bounds=bounds)
    inputs = CompileInputs(source_schema=schema)
    request = _request(source, arguments={'limit': 0})
    lower_action(request, bounds=BOUNDS, inputs=inputs)
    assert len(seen) == 1
    seen.clear()
    with pytest.raises(DataflowError, match='registered sink declaration'):
        lower_action(replace(request, kind='write_datasink'), bounds=BOUNDS, inputs=inputs)
    assert seen == []


def test_explicit_inputs_are_used_instead_of_the_surroundings():
    dataset = data.from_items([{'x': 1}]).map_batches(batch).random_shuffle(seed=1).map(identity)
    ambient = _request(dataset, executor=corpus.EXECUTORS['local']())
    plain = _request(dataset)
    # Omitted: resolved from the request's action.
    resolved = lower_action(ambient, bounds=BOUNDS)
    assert _configuration(resolved, 'MapBatchesOp')['default_batch_size'] == 512
    assert _configuration(resolved, 'RandomShuffleOp')['num_blocks'] == 3
    assert _configuration(resolved, 'MapOp')['task_batch_rows'] == 512
    # Given: the executor on the action is not consulted.
    given = lower_action(ambient, bounds=BOUNDS, inputs=CompileInputs(CompileDefaults(batch_rows=7, partitions=2)))
    assert _configuration(given, 'MapBatchesOp')['default_batch_size'] == 7
    assert _configuration(given, 'RandomShuffleOp')['num_blocks'] == 2
    assert _configuration(given, 'MapOp')['task_batch_rows'] == 7
    empty = lower_action(ambient, bounds=BOUNDS, inputs=CompileInputs())
    assert 'default_batch_size' not in _configuration(empty, 'MapBatchesOp')
    assert _configuration(empty, 'RandomShuffleOp')['num_blocks'] is None
    assert _configuration(empty, 'MapOp')['task_batch_rows'] == 8192
    assert _bytes(empty) == _bytes(lower_action(plain, bounds=BOUNDS))
    # The same value, resolved or given, compiles to the same bytes.
    assert _bytes(lower_action(plain, bounds=BOUNDS, inputs=compile_inputs(ambient))) == _bytes(resolved)


def test_report_bindings_come_from_the_given_inputs_only():
    dataset = data.from_items([{'x': 1}]).map(identity).filter(keep)
    request = _request(dataset)
    report = ReportBindings(corpus.report_for(dataset._source, dataset._plan, 'inputs'))
    assert lower_action(request, bounds=BOUNDS).report_bindings == ()
    bound = lower_action(request, bounds=BOUNDS, inputs=CompileInputs(report=report))
    assert [b['report_node_id'] for b in bound.report_bindings] == [
        'report-source-0', 'report-operation-1', 'report-operation-2', 'inputs']
    assert [b['mode'] for b in bound.report_bindings] == ['execute', 'reuse', 'execute', 'execute']
    occurrences = [node.occurrence_id for node in bound.graph.nodes]
    assert [b['occurrence_id'] for b in bound.report_bindings] == occurrences
    # An active report context is ignored when the caller says there is no report.
    token = checked._CURRENT.set(corpus.report_for(dataset._source, dataset._plan, 'inputs'))
    try:
        assert lower_action(request, bounds=BOUNDS, inputs=CompileInputs()).report_bindings == ()
        assert len(lower_action(request, bounds=BOUNDS).report_bindings) == 4
    finally:
        checked._CURRENT.reset(token)
    # The report never changes the graph.
    assert _bytes(bound) == _bytes(lower_action(request, bounds=BOUNDS))


def test_report_questions_read_the_context_lazily():
    """A context is only asked what the plan needs, when it needs it; nothing is copied up front."""
    class Context:
        def __init__(self):
            self.asked = []

        def __getattr__(self, name):
            self.asked.append(name)
            return {}
    context = Context()
    bindings = ReportBindings(context)
    assert context.asked == []
    assert bindings.source(object()) is None and context.asked == ['source_bindings']
    assert bindings.operation(object()) is None and context.asked[-1] == 'operation_bindings'
    assert bindings.observed(('a', 'a', 0)) is False and bindings.reused(('a', 'n', 0)) is False
    assert context.asked == ['source_bindings', 'operation_bindings', 'observations', 'observations']
    live = {'state': 'running'}
    context.observations = {('a', 'n', 0): live}
    assert bindings.reused(['a', 'n', 0]) is False
    live['state'] = 'reused'                       # a later change is seen: the view is not a snapshot
    assert bindings.reused(['a', 'n', 0]) is True
