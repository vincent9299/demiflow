"""Dataset runtime implementation."""
from __future__ import annotations

from contextlib import contextmanager
from contextvars import ContextVar

from typing import Any, Mapping
from demiflow.data import execution_contract as actions
from demiflow.data.execution_contract import normalize_batch_format as _batch_format
from demiflow.data.datasink import validate_write_result
from demiflow.observability import observe_action, observe_batches, observe_rows


def execute_action(executor: actions.ExecutionPort, request: actions.ActionSpec) -> actions.ActionResult:
    """Interpret the closed SDK request without retaining additional state."""
    if isinstance(request, actions.Count):
        return _count(executor, request)
    if isinstance(request, actions.Materialize):
        return _materialize(executor, request)
    if isinstance(request, actions.Take):
        return _take(executor, request)
    if isinstance(request, actions.TakeAll):
        return _take_all(executor, request)
    if isinstance(request, actions.TakeBatch):
        return _take_batch(executor, request)
    if isinstance(request, actions.RequireColumns):
        return _require_columns(executor, request)
    if isinstance(request, actions.CollectSummary):
        return _collect_summary(executor, request)
    if isinstance(request, actions.Aggregate):
        return _aggregate(executor, request)
    if isinstance(request, actions.IterRows):
        return _iter_rows(executor, request)
    if isinstance(request, actions.IterBatches):
        return _iter_batches(executor, request)
    if isinstance(request, actions.Schema):
        return _schema(executor, request)
    if isinstance(request, actions.Columns):
        return _columns(executor, request)
    if isinstance(request, actions.SizeBytes):
        return _size_bytes(executor, request)
    if isinstance(request, actions.WriteDatasink):
        return _write_datasink(executor, request)
    if isinstance(request, actions.WriteFile):
        return _write_file(executor, request)
    if isinstance(request, actions.RunStream):
        return _run_stream(executor, request)
    if isinstance(request, actions.WriteLance):
        return _write_lance(executor, request)
    raise TypeError(f'Unknown Dataset action request: {type(request).__name__}')


def _count(executor: actions.ExecutionPort, spec: actions.Count):
    from demiflow.execution.checked import action
    from demiflow.execution.dataflow_api import make_action_request, run_dataset_action
    with action('count', spec.source, spec.plan, arguments={}) as checked, observe_action("count", spec.source, spec.plan, executor) as observer:
        request = make_action_request(executor, checked.source, checked.plan,
                                      kind="count", arguments={})
        count = run_dataset_action(request)
        checked.value(count)
        observer.rows = int(count)
        observer.complete()
    return count


def _materialize(executor: actions.ExecutionPort, spec: actions.Materialize):
    from demiflow.execution.checked import action
    from demiflow.execution.dataflow_api import make_action_request, run_dataset_action
    with action("materialize", spec.source, spec.plan) as checked, observe_action("materialize", spec.source, spec.plan, executor) as observer:
        request = make_action_request(executor, checked.source, checked.plan,
                                      kind="materialize", arguments={})
        handle = run_dataset_action(request)
        checked.materialized(handle)
        known_row_count = getattr(handle, "row_count", None)
        row_count = int(known_row_count or 0)
        block_count = len(getattr(handle, "blocks", ()) or ())
        observer.rows = row_count
        observer.batches = block_count
        observer.complete(materialized=True)
    return handle


def _take(executor: actions.ExecutionPort, spec: actions.Take):
    maximum = spec.limit
    from demiflow.execution.checked import action
    from demiflow.execution.dataflow_api import make_action_request, run_dataset_action
    with action("take", spec.source, spec.plan, arguments={"limit": maximum}) as checked, \
            observe_action("take", spec.source, spec.plan, executor) as observer:
        request = make_action_request(executor, checked.source, checked.plan,
                                      kind="take", arguments={"limit": maximum})
        rows = run_dataset_action(request)
        observer.rows = len(rows)
        observer.complete(limit=maximum, early_stopped=len(rows) == maximum)
    return rows


def _take_all(executor: actions.ExecutionPort, spec: actions.TakeAll):
    maximum, limit = spec.limit, spec.original_limit
    from demiflow.execution.checked import action
    from demiflow.execution.dataflow_api import make_action_request, run_dataset_action
    with action("take_all", spec.source, spec.plan, arguments={"limit": limit}) as checked, \
            observe_action("take_all", spec.source, spec.plan, executor) as observer:
        request = make_action_request(executor, checked.source, checked.plan,
                                      kind="take_all", arguments={"limit": maximum})
        rows = run_dataset_action(request)
        observer.rows = len(rows)
        observer.complete(limit=limit)
    return rows


def _take_batch(executor: actions.ExecutionPort, spec: actions.TakeBatch):
    size, batch_format = spec.batch_size, spec.batch_format
    from demiflow.execution.checked import action
    from demiflow.execution.dataflow_api import make_action_request, run_dataset_action
    with action("take_batch", spec.source, spec.plan,
                arguments={"batch_size": size, "batch_format": batch_format}) as checked, \
            observe_action("take_batch", spec.source, spec.plan, executor) as observer:
        request = make_action_request(executor, checked.source, checked.plan,
                                      kind="take_batch",
                                      arguments={"batch_size": size,
                                                 "batch_format": _batch_format(batch_format)})
        batch = run_dataset_action(request)
        observer.rows = _batch_row_count(batch)
        observer.batches = 1
        observer.complete(batch_size=size, batch_format=batch_format)
    return batch


def _require_columns(executor: actions.ExecutionPort, spec: actions.RequireColumns):
    columns, exact = spec.columns, spec.exact
    from demiflow.execution.checked import action
    from demiflow.execution.dataflow_api import make_action_request, run_dataset_action
    arguments = dict(columns=columns, exact=exact)
    with action('require_columns', spec.source, spec.plan, arguments=arguments) as checked:
        request = make_action_request(executor, checked.source, checked.plan,
                                      kind="columns", arguments={"fetch_if_missing": True})
        actual = run_dataset_action(request)
        if actual is None or len(actual) > 2048:
            raise ValueError('Dataset columns are unknown or exceed metadata budget')
        expected = set(arguments['columns'])
        if not expected <= set(actual) or exact and expected != set(actual):
            raise ValueError('Dataset columns differ from the explicit prerequisite')
        checked.value(None)


def _collect_summary(executor: actions.ExecutionPort, spec: actions.CollectSummary):
    max_rows, max_bytes = spec.max_rows, spec.max_bytes
    from demiflow.data.runtime_contract import check_size
    from demiflow.execution.checked import action
    from demiflow.execution.dataflow_api import make_action_request, run_dataset_action
    arguments = dict(max_rows=max_rows, max_bytes=max_bytes)
    with action('collect_summary', spec.source, spec.plan, arguments=arguments) as checked:
        result, used = [], 0
        request = make_action_request(executor, checked.source, checked.plan,
                                      kind="iter_rows", arguments={})
        rows = iter(run_dataset_action(request))
        try:
            for row in rows:
                if len(result) >= max_rows:
                    raise ValueError('Summary row budget exceeded')
                used += check_size(row, max_bytes)
                if used > max_bytes:
                    raise ValueError('Summary byte budget exceeded')
                result.append(row)
            check_size(result, max_bytes)
            import json
            raw = json.dumps(result, ensure_ascii=False, sort_keys=True, indent=2, allow_nan=False).encode() + b'\n'
            if len(raw) > max_bytes:
                raise ValueError('Summary encoded byte budget exceeded')
            checked.value(result)
            return result
        finally:
            close = getattr(rows, 'close', None)
            if close is not None:
                close()


def _aggregate(executor: actions.ExecutionPort, spec: actions.Aggregate):
    aggs = spec.aggregates
    names = [aggregate.name for aggregate in aggs]
    from demiflow.execution.checked import action
    from demiflow.execution.dataflow_api import make_action_request, run_dataset_action
    from demiflow.execution.contracts.dataflow import ActionProgram
    with action("aggregate", spec.source, spec.plan, arguments={"aggregates": names}) as checked, \
            observe_action(
                "aggregate", spec.source, spec.plan, executor,
                aggregate_count=len(aggs),
            ) as observer:
        programs = tuple(ActionProgram(name="aggregate-" + str(index), kind="aggregate", value=aggregate)
                         for index, aggregate in enumerate(aggs))
        request = make_action_request(executor, checked.source, checked.plan,
                                      kind="aggregate", arguments={"names": names},
                                      programs=programs)
        result = run_dataset_action(request)
        observer.complete()
    return result


def _iter_rows(executor: actions.ExecutionPort, spec: actions.IterRows):
    from demiflow.execution.checked import action
    from demiflow.execution.dataflow_api import make_action_request, run_dataset_action

    def _scoped():
        with action("iter_rows", spec.source, spec.plan, arguments={}) as checked:
            request = make_action_request(executor, checked.source, checked.plan,
                                          kind="iter_rows", arguments={})
            yield from observe_rows(
                "iter_rows", run_dataset_action(request),
                checked.source, checked.plan, executor,
            )

    return _scoped()


def _iter_batches(executor: actions.ExecutionPort, spec: actions.IterBatches):
    size, batch_format = spec.batch_size, spec.batch_format
    normalized_format = spec.normalized_format
    prefetch_batches, drop_last = spec.prefetch_batches, spec.drop_last
    from demiflow.execution.checked import action
    from demiflow.execution.dataflow_api import make_action_request, run_dataset_action

    def _scoped():
        with action("iter_batches", spec.source, spec.plan,
                    arguments={"batch_size": size, "batch_format": batch_format,
                               "drop_last": drop_last}) as checked:
            request = make_action_request(executor, checked.source, checked.plan,
                                          kind="iter_batches",
                                          arguments={"prefetch_batches": prefetch_batches,
                                                     "batch_size": size,
                                                     "batch_format": normalized_format,
                                                     "drop_last": drop_last})
            batches = run_dataset_action(request)
            yield from observe_batches(
                "iter_batches", batches, checked.source, checked.plan, executor,
            )

    return _scoped()


def _schema(executor: actions.ExecutionPort, spec: actions.Schema):
    fetch_if_missing = spec.fetch_if_missing
    from demiflow.execution.checked import action
    from demiflow.execution.dataflow_api import make_action_request, run_dataset_action
    with action("schema", spec.source, spec.plan,
                arguments={"fetch_if_missing": fetch_if_missing}) as checked:
        request = make_action_request(executor, checked.source, checked.plan,
                                      kind="schema",
                                      arguments={"fetch_if_missing": fetch_if_missing})
        return run_dataset_action(request)


def _columns(executor: actions.ExecutionPort, spec: actions.Columns):
    fetch_if_missing = spec.fetch_if_missing
    from demiflow.execution.checked import action
    from demiflow.execution.dataflow_api import make_action_request, run_dataset_action
    with action("columns", spec.source, spec.plan,
                arguments={"fetch_if_missing": fetch_if_missing}) as checked:
        request = make_action_request(executor, checked.source, checked.plan,
                                      kind="columns",
                                      arguments={"fetch_if_missing": fetch_if_missing})
        return run_dataset_action(request)


def _size_bytes(executor: actions.ExecutionPort, spec: actions.SizeBytes):
    from demiflow.execution.checked import action
    from demiflow.execution.dataflow_api import make_action_request, run_dataset_action
    with action("size_bytes", spec.source, spec.plan, arguments={}) as checked, \
            observe_action("size_bytes", spec.source, spec.plan, executor) as observer:
        request = make_action_request(executor, checked.source, checked.plan,
                                      kind="size_bytes", arguments={})
        value = run_dataset_action(request)
        observer.complete(size_bytes=value)
    return value


def _write_datasink(executor: actions.ExecutionPort, spec: actions.WriteDatasink):
    datasink = spec.sink
    target = ""
    from dataclasses import asdict
    from demiflow.execution.checked import action
    from demiflow.execution.dataflow_api import make_action_request, run_dataset_action
    native = spec.native_options
    arguments = {} if native is None else {"native_options": asdict(native)}
    with action('write_datasink', spec.source, spec.plan,
                arguments={'sink': datasink.recovery_identity()}), observe_action(
        "write_datasink", spec.source, spec.plan, executor,
        sink_type=type(datasink).__name__, sink_target=target,
    ) as observer:
        request = make_action_request(executor, spec.source, spec.plan,
                                      kind='write_datasink', arguments=arguments,
                                      sink=datasink)
        result = validate_write_result(run_dataset_action(request))
        observer.rows = int(result.written_rows or 0)
        observer.batches = int(result.blocks_written or 0)
        observer.complete(
            failed_rows=result.failed_rows,
            result_target=str(result.target or target),
        )
    return None


def _write_file(executor: actions.ExecutionPort, spec: actions.WriteFile):
    format_name, path = spec.format, spec.path
    from dataclasses import asdict
    from demiflow.execution.checked import action
    from demiflow.execution.dataflow_api import make_action_request, run_dataset_action
    options = dict(spec.options)
    native = spec.native_options
    arguments = {"format": format_name, "path": str(path), "options": options}
    if native is not None:
        arguments["native_options"] = asdict(native)
    with action("write_file", spec.source, spec.plan,
                arguments={"format": format_name, "path": str(path),
                           "options": dict(options)}) as checked, \
            observe_action(
                f"write_{format_name}", spec.source, spec.plan, executor,
                sink_target=str(path),
            ) as observer:
        request = make_action_request(executor, checked.source, checked.plan,
                                      kind='write_file', arguments=arguments)
        result = validate_write_result(run_dataset_action(request))
        observer.rows = int(result.written_rows or 0)
        observer.batches = int(result.blocks_written or 0)
        observer.complete(
            failed_rows=result.failed_rows,
            result_target=str(result.target or path),
            metadata=dict(result.metadata),
        )


def _run_stream(executor: actions.ExecutionPort, spec: actions.RunStream):
    on_progress, on_drain = spec.on_progress, spec.on_drain
    log_every, cancellation = spec.log_every, spec.cancellation
    stall_timeout, stop_file = spec.stall_timeout, spec.stop_file
    source_batch_size = spec.source_batch_size
    from demiflow.execution.checked import action
    from demiflow.execution.dataflow_api import make_action_request
    from demiflow.execution.dataflow_stream import run_stream_action
    arguments = dict(log_every=log_every, stall_timeout=stall_timeout,
                     stop_file=str(stop_file) if stop_file is not None else None,
                     source_batch_size=source_batch_size)
    with action("run_stream", spec.source, spec.plan, arguments=arguments) as checked:
        request = make_action_request(executor, checked.source, checked.plan,
                                      kind="run_stream", arguments=arguments)
        stats = run_stream_action(request, on_progress=on_progress, on_drain=on_drain,
            log_every=log_every, cancellation=cancellation, stall_timeout=stall_timeout, stop_file=stop_file)
        checked.streamed(stats)
        return stats


def _write_lance(executor: actions.ExecutionPort, request_spec: actions.WriteLance):
    spec, native_options = request_spec.spec, request_spec.native_options
    from dataclasses import asdict
    from demiflow.execution.checked import action
    from demiflow.execution.run_context import current_action
    from demiflow.execution.dataflow_api import make_action_request, run_dataset_action
    from demiflow.execution.contracts.dataflow import ActionProgram
    arguments = {} if native_options is None else {'native_options': asdict(native_options)}
    binding = _lance_action_binding()
    if binding is not None:
        from demiflow.execution.contracts.dataflow import EXECUTION_LIMITS
        arguments[EXECUTION_LIMITS] = {'inflight_rows': binding[1]}
    with action('write_table', request_spec.source, request_spec.plan, spec) as checked, observe_action(
        'write_lance', request_spec.source, request_spec.plan, executor,
        sink_type='LanceWrite', sink_target=spec.uri,
    ) as observer:
        from demiflow.execution.plan_capture import current_capture
        if binding is not None and current_capture() is None:
            _consume_lance_action_binding(binding, current_action(), spec)
        request = make_action_request(executor, checked.source, checked.plan,
            kind='write_lance', arguments=arguments,
            programs=(ActionProgram(name='write_lance', kind='sink', value=spec),))
        receipt = run_dataset_action(request)
        checked.receipt(receipt)
        if receipt.status == 'indeterminate':
            from demiflow.errors import LanceWriteError
            raise LanceWriteError(receipt)
        observer.rows = int(receipt.written_rows or 0)
        observer.complete(result_target=spec.uri, write_receipt=receipt.to_dict(),
                          reconciliation_required=False)
    return receipt


def _batch_row_count(batch: Any) -> int:
    rows = getattr(batch, "num_rows", None)
    if rows is not None:
        return int(rows)
    if isinstance(batch, Mapping):
        if not batch:
            return 0
        try:
            return len(next(iter(batch.values())))
        except TypeError:
            return 0
    try:
        return len(batch)
    except TypeError:
        return 0



def execute_guard(condition: bool, message: str) -> None:
    """Execute the public prerequisite without selecting a Dataset host."""
    from demiflow.data.flow_contract import normalize_guard
    from demiflow.execution.checked import action
    arguments = normalize_guard({'condition': condition, 'message': message})
    if type(condition) is not bool:
        raise TypeError('require condition must be a bool')
    with action('require', None, None, arguments=arguments) as checked:
        if not condition:
            raise ValueError(message)
        checked.value(None)


_BINDING = ContextVar('demiflow_internal_lance_action_binding', default=None)


@contextmanager
def bind_lance_action(before_execute, *, max_rows_per_batch):
    """Internal registration hook, before any worker/source activation.

    No public Dataset option, new action, scheduler or commit authority lives
    here. The hook receives the already-created original RunAction once.
    """
    if _BINDING.get() is not None:
        raise RuntimeError('Nested Lance registration binding')
    token = _BINDING.set((before_execute, max_rows_per_batch))
    try:
        yield
    finally:
        _BINDING.reset(token)


def _lance_action_binding():
    """Read the existing registration without exposing it through SDK requests."""
    return _BINDING.get()


def _consume_lance_action_binding(binding, action, spec):
    # Clear before the callback: a nested write must not bind a second action.
    _BINDING.set(None)
    binding[0](action, spec)
