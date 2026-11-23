"""P1 lifecycle regression seams, separate from physical engine acceptance."""
from contextlib import contextmanager
from types import SimpleNamespace

import pytest

from demiflow.data import execution_contract as contracts
from demiflow.data.aggregate import Sum
from demiflow.data.dataset import Dataset, MaterializedDataset
from demiflow.data.datasink import WriteResult
from demiflow.data.plan import LogicalPlan
from demiflow.data.sources import ItemsSource
from demiflow.execution import checked, dataflow_api
from demiflow.execution.executors import dataset_runtime


@pytest.fixture
def boundary(monkeypatch):
    original_source, bound_source = ItemsSource([{'x': 1}]), ItemsSource([{'x': 2}])
    original_plan, bound_plan = LogicalPlan(), LogicalPlan()
    state = SimpleNamespace(events=[], checks=[], requests=[], observers=[], result=None,
                            original_source=original_source, bound_source=bound_source,
                            original_plan=original_plan, bound_plan=bound_plan,
                            receipt_error=None)

    def completion(kind, value):
        state.events.append((kind, value))
        if kind == 'receipt' and state.receipt_error:
            raise state.receipt_error

    @contextmanager
    def action(kind, source, plan, spec=None, *, arguments=None):
        state.checks.append((kind, source, plan, spec, arguments))
        state.events.append(('enter', kind))
        try:
            yield SimpleNamespace(source=bound_source, plan=bound_plan,
                                  value=lambda v: completion('value', v),
                                  materialized=lambda v: completion('materialized', v),
                                  streamed=lambda v: completion('streamed', v),
                                  receipt=lambda v: completion('receipt', v))
        finally:
            state.events.append(('exit', kind))

    @contextmanager
    def observe(kind, source, plan, executor, **initial):
        item = SimpleNamespace(kind=kind, source=source, plan=plan, initial=initial,
                               rows=0, batches=0, completed=[])
        item.complete = lambda **fields: item.completed.append(fields)
        state.observers.append(item)
        yield item

    def request(executor, source, plan, **values):
        item = SimpleNamespace(source=source, plan=plan, **values)
        state.requests.append(item)
        state.events.append(('request', item.kind))
        return item

    def run(request):
        state.events.append(('execute', request.kind))
        return state.result

    class Host:
        def execute(self, request):
            return dataset_runtime.execute_action(self, request)

        def schema(self, source, plan, *, fetch_if_missing=True):
            assert fetch_if_missing is False
            return None

        def columns(self, source, plan, *, fetch_if_missing):
            assert fetch_if_missing is False
            return None

    state.host = Host()
    state.dataset = Dataset(original_source, original_plan, state.host)
    monkeypatch.setattr(checked, 'action', action)
    monkeypatch.setattr(dataflow_api, 'make_action_request', request)
    monkeypatch.setattr(dataflow_api, 'run_dataset_action', run)
    monkeypatch.setattr(dataset_runtime, 'observe_action', observe)
    return state


@pytest.mark.parametrize('method,args,result,arguments,completion', [
    ('count', (), 3, {}, 'value'),
    ('take', (1,), [{'x': 1}], {'limit': 1}, None),
    ('take_all', ('3',), [{'x': 1}], {'limit': '3'}, None),
    ('take_batch', (2,), [{'x': 1}], {'batch_size': 2, 'batch_format': 'default'}, None),
    ('size_bytes', (), 16, {}, None),
    ('columns', (), ['x'], {'fetch_if_missing': True}, None),
    ('schema', (), object(), {'fetch_if_missing': True}, None),
    ('aggregate', (Sum('x'),), {'sum(x)': 1}, {'aggregates': ['sum(x)']}, None),
])
def test_action_identity_binding_and_no_added_completion(boundary, method, args, result, arguments, completion):
    state = boundary
    state.result = result
    assert getattr(state.dataset, method)(*args) is result
    kind, source, plan, _, actual_arguments = state.checks[0]
    assert kind == method
    assert source is state.original_source and plan is state.original_plan
    assert actual_arguments == arguments
    request = state.requests[0]
    assert request.source is state.bound_source and request.plan is state.bound_plan
    completed = [(kind, value) for kind, value in state.events if kind in {'value', 'materialized', 'receipt', 'streamed'}]
    assert completed == ([(completion, result)] if completion else [])
    if state.observers:
        observer = state.observers[0]
        assert observer.source is state.original_source and observer.plan is state.original_plan
    if method == 'take_all':
        assert request.arguments == {'limit': 3}
        assert state.observers[0].completed == [{'limit': '3'}]
    if method == 'aggregate':
        assert request.arguments == {'names': ['sum(x)']}
        assert request.programs[0].value is args[0]


def test_materialize_keeps_none_declaration_and_wraps_original_handle(boundary):
    state = boundary
    state.result = SimpleNamespace(row_count=4, blocks=('a', 'b'))
    result = state.dataset.materialize()
    assert isinstance(result, MaterializedDataset)
    assert result._source.handle is state.result
    assert result._executor is state.host
    assert state.checks[0][4] is None
    assert state.requests[0].arguments == {}
    assert ('materialized', state.result) in state.events
    observer = state.observers[0]
    assert (observer.rows, observer.batches, observer.completed) == (4, 2, [{'materialized': True}])


def test_nonfetch_metadata_does_not_open_an_action(boundary):
    assert boundary.dataset.schema(False) is None
    assert boundary.dataset.columns(False) is None
    assert boundary.events == []


def test_require_columns_one_checked_action_and_bounded_summary_closes_on_error(boundary):
    state = boundary
    state.result = ['x']
    assert state.dataset.require_columns(['x']) is None
    assert [item[0] for item in state.checks] == ['require_columns']
    assert [request.kind for request in state.requests] == ['columns']
    assert state.events[-2] == ('value', None)
    closed = []

    def rows():
        try:
            yield {'x': 1}
            yield {'x': 2}
        finally:
            closed.append(True)

    state.result = rows()
    with pytest.raises(ValueError, match='Summary row budget exceeded'):
        state.dataset.collect_summary(max_rows=1)
    assert closed == [True]
    assert state.observers == []
    assert not any(kind == 'value' and isinstance(value, list) for kind, value in state.events)


@pytest.mark.parametrize('method', ['iter_rows', 'iter_batches'])
def test_delivery_scope_starts_on_first_pull_and_closes_with_iterator(boundary, monkeypatch, method):
    state = boundary
    closed = []
    observed = []

    def rows():
        try:
            yield {'x': 1}
            yield {'x': 2}
        finally:
            closed.append(True)

    def observe(kind, values, source, plan, executor):
        observed.append((kind, source, plan))
        yield from values

    monkeypatch.setattr(dataset_runtime, 'observe_rows' if method == 'iter_rows' else 'observe_batches', observe)
    state.result = rows()
    kwargs = {} if method == 'iter_rows' else dict(batch_size=2, batch_format=None, prefetch_batches=7)
    iterator = getattr(state.dataset, method)(**kwargs)
    assert state.events == []
    assert next(iterator) == {'x': 1}
    assert observed == [(method, state.bound_source, state.bound_plan)]
    assert ('exit', method) not in state.events
    iterator.close()
    assert closed == [True]
    assert state.events[-1] == ('exit', method)
    assert not any(kind == 'value' for kind, _ in state.events)
    if method == 'iter_batches':
        assert state.checks[0][4] == dict(batch_size=2, batch_format=None, drop_last=False)
        assert state.requests[0].arguments == dict(batch_size=2, batch_format='default', drop_last=False, prefetch_batches=7)


def test_take_batch_format_error_still_occurs_inside_checked_scope(boundary):
    with pytest.raises(ValueError, match='batch_format must be'):
        boundary.dataset.take_batch(batch_format='invalid')
    assert boundary.events == [('enter', 'take_batch'), ('exit', 'take_batch')]
    assert boundary.requests == []


def test_datasink_preserves_original_source_and_file_uses_checked_source(boundary):
    state = boundary
    sink = SimpleNamespace(recovery_identity=lambda: {'destination': 'example'})
    state.result = WriteResult(written_rows=2, blocks_written=1, target='committed', metadata={'worker': 'actual'})
    assert state.dataset.write_datasink(sink) is None
    assert state.requests[0].source is state.original_source
    assert state.requests[0].plan is state.original_plan
    assert state.checks[0][4] == {'sink': {'destination': 'example'}}
    assert state.observers[0].completed == [{'failed_rows': 0, 'result_target': 'committed'}]
    assert state.dataset.write_json('/tmp/p1-port-unused', compression=None) is None
    assert state.requests[1].source is state.bound_source
    assert state.observers[1].completed == [{'failed_rows': 0, 'result_target': 'committed', 'metadata': {'worker': 'actual'}}]
    assert not any(kind == 'value' for kind, _ in state.events)


def test_stream_controls_keep_objects_outside_checked_arguments(boundary, monkeypatch):
    from demiflow.execution import dataflow_stream
    state = boundary
    controls = [object() for _ in range(4)]
    progress, drain, cancel, stop_file = controls
    state.result = object()
    seen = []

    def run(request, **kwargs):
        seen.append(kwargs)
        return state.result

    monkeypatch.setattr(dataflow_stream, 'run_stream_action', run)
    result = state.host.execute(contracts.RunStream(state.original_source, state.original_plan,
                               progress, drain, 5, cancel, 6.0, stop_file, 8))
    assert result is state.result
    assert state.checks[0][4] == dict(log_every=5, stall_timeout=6.0, stop_file=str(stop_file), source_batch_size=8)
    assert seen == [dict(on_progress=progress, on_drain=drain, cancellation=cancel,
                         stop_file=stop_file, log_every=5, stall_timeout=6.0)]
    assert ('streamed', result) in state.events


def _receipt(status='committed'):
    from demiflow.lance.model import LanceWriteReceipt
    from demiflow.errors import make_error
    return LanceWriteReceipt(
        request_hash='request', uri='/tmp/p1-receipt-unused.lance', expected_version=None,
        committed_version=1 if status == 'committed' else None, input_rows=1,
        written_rows=1 if status == 'committed' else None, schema_hash='schema', status=status,
        error=None if status == 'committed' else make_error(module='test', type_name='UnknownCommit', message='unresolved'),
    )


def test_lance_binding_consumed_before_nested_callback_and_limits_retained(boundary):
    from demiflow.execution.contracts.dataflow import EXECUTION_LIMITS
    state = boundary
    state.result = _receipt()
    callbacks = []

    def before(action, spec):
        callbacks.append(spec)
        assert state.requests == []
        assert dataset_runtime._lance_action_binding() is None
        state.dataset.write_lance('/tmp/p1-nested-unused.lance')

    with dataset_runtime.bind_lance_action(before, max_rows_per_batch=7):
        result = state.dataset.write_lance('/tmp/p1-outer-unused.lance', return_receipt=True)
    assert result is state.result
    assert len(callbacks) == 1
    assert len(state.requests) == 2
    assert EXECUTION_LIMITS not in state.requests[0].arguments
    assert state.requests[1].arguments[EXECUTION_LIMITS] == {'inflight_rows': 7}
    assert all(item[0] == 'write_table' and item[4] is None for item in state.checks)
    assert dataset_runtime._lance_action_binding() is None


def test_lance_callback_error_never_constructs_request_and_binding_resets(boundary):
    def before(action, spec):
        raise RuntimeError('registration rejected')

    with pytest.raises(RuntimeError, match='registration rejected'):
        with dataset_runtime.bind_lance_action(before, max_rows_per_batch=7):
            boundary.dataset.write_lance('/tmp/p1-error-unused.lance')
    assert boundary.requests == []
    assert dataset_runtime._lance_action_binding() is None


@pytest.mark.parametrize('managed', [False, True])
def test_lance_indeterminate_registers_receipt_before_original_error(boundary, managed):
    from demiflow.data.runtime_contract import RuntimeContractError
    from demiflow.errors import LanceWriteError
    state = boundary
    state.result = _receipt('indeterminate')
    if managed:
        state.receipt_error = RuntimeContractError('not committed')
    with pytest.raises(RuntimeContractError if managed else LanceWriteError):
        state.dataset.write_lance('/tmp/p1-indeterminate-unused.lance')
    assert ('receipt', state.result) in state.events
    assert state.observers[0].completed == []


def test_lance_capture_skips_callback_and_propagates_baseexception(boundary, monkeypatch):
    from demiflow.execution import plan_capture
    state = boundary
    callbacks = []
    monkeypatch.setattr(plan_capture, 'current_capture', lambda: object())

    def capture(request):
        raise plan_capture._Captured()

    monkeypatch.setattr(dataflow_api, 'run_dataset_action', capture)
    with pytest.raises(plan_capture._Captured):
        with dataset_runtime.bind_lance_action(lambda *args: callbacks.append(args), max_rows_per_batch=3):
            state.dataset.write_lance('/tmp/p1-capture-unused.lance')
    assert callbacks == []
    assert not any(kind == 'receipt' for kind, _ in state.events)
    assert dataset_runtime._lance_action_binding() is None


def test_guard_has_no_host_and_only_registers_success(boundary):
    dataset_runtime.execute_guard(True, 'ok')
    assert boundary.checks[0] == ('require', None, None, None, {'condition': True, 'message': 'ok'})
    assert boundary.requests == []
    with pytest.raises(ValueError, match='failed'):
        dataset_runtime.execute_guard(False, 'failed')
    assert [event for event in boundary.events if event[0] == 'value'] == [('value', None)]
