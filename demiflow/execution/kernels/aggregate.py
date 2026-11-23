"""Bounded global aggregation kernels (lane K): partial, merge, finalize.

K013/K014/K015 reuse the platform ``AggregateFnV2`` lifecycle (aggregate_partial
→ merge_states → finalize_state). State is bounded by ``max_state_bytes`` with a
pre-merge coexistence check (an oversized partial is rejected before ``combine``
copies it). Only mergeable kernels decompose; partials merge in a deterministic
digest order. Empty input yields count 0 and a None result, never a fabricated
zero; duplicate aggregate names are rejected up front.

``finalize_aggregate`` has no ``services`` parameter in the frozen signature, so
the owning OperatorKernel binds the runtime services through ``bind_finalize``
for the store/program reads it needs (documented C06 interpretation).
"""
from __future__ import annotations

import pickle
from collections.abc import Iterator, Mapping
from contextlib import contextmanager
from contextvars import ContextVar
from typing import Any

from demiflow.execution.contracts.dataflow import (
    AggregateState,
    KernelSpec,
    Record,
    RuntimeServices,
    TerminalResult,
)

from .dataflow import (
    _canonical,
    _check_stop,
    _cfg,
    _err,
    _materialize_row,
)

AGGREGATE_STATE_VERSION = "demiflow-aggregate-state-v3"
AGGREGATE_STATE_MEDIA_TYPE = "application/x-demiflow-aggregate-state-v3"
AGGREGATE_RESULT_MEDIA_TYPE = "application/x-demiflow-aggregate-result-v3"

_FINALIZE_SERVICES: ContextVar = ContextVar("demiflow_kernel_finalize_services", default=None)


@contextmanager
def bind_finalize(services: RuntimeServices) -> Iterator[None]:
    token = _FINALIZE_SERVICES.set(services)
    try:
        yield
    finally:
        _FINALIZE_SERVICES.reset(token)


def _finalize_services() -> RuntimeServices:
    services = _FINALIZE_SERVICES.get()
    if services is None:
        raise _err("INVALID_CONTRACT", "finalize_aggregate requires bound runtime services")
    return services


def _builtin_aggregate(entry: Mapping[str, Any]) -> Any:
    from demiflow.data import aggregate as agg

    kind = entry.get("builtin")
    on = entry.get("on")
    ignore_nulls = entry.get("ignore_nulls", kind != "count")
    alias_name = entry.get("alias_name")
    if kind == "count":
        return agg.Count(on=on, ignore_nulls=ignore_nulls, alias_name=alias_name)
    if kind == "sum":
        return agg.Sum(on=on, ignore_nulls=ignore_nulls, alias_name=alias_name)
    if kind == "min":
        return agg.Min(on=on, ignore_nulls=ignore_nulls, alias_name=alias_name)
    if kind == "max":
        return agg.Max(on=on, ignore_nulls=ignore_nulls, alias_name=alias_name)
    if kind == "mean":
        return agg.Mean(on=on, ignore_nulls=ignore_nulls, alias_name=alias_name)
    if kind == "std":
        return agg.Std(on=on, ddof=entry.get("ddof", 1), ignore_nulls=ignore_nulls, alias_name=alias_name)
    if kind == "abs_max":
        return agg.AbsMax(on=on, ignore_nulls=ignore_nulls, alias_name=alias_name)
    raise _err("INVALID_CONTRACT", "unknown builtin aggregate", builtin=str(kind))


def _resolve_aggregates(spec: KernelSpec, services: RuntimeServices) -> tuple[Any, ...]:
    from demiflow.data.aggregate import AggregateFnV2

    entries = _cfg(spec, "aggregates")
    if not isinstance(entries, (list, tuple)) or not entries:
        raise _err("INVALID_CONTRACT", "aggregates must be a non-empty list")
    aggregates = []
    for entry in entries:
        if not isinstance(entry, Mapping):
            raise _err("INVALID_CONTRACT", "aggregate entry must be a mapping")
        if "program" in entry:
            program = services.programs.get(entry["program"])
            if program is None or program.kind != "aggregate":
                raise _err("INVALID_CONTRACT", "aggregate program not staged", program=str(entry["program"]))
            value = program.value
        else:
            value = _builtin_aggregate(entry)
        if not isinstance(value, AggregateFnV2):
            raise _err("INVALID_CONTRACT", "aggregate must be an AggregateFnV2")
        aggregates.append(value)
    names = [aggregate.name for aggregate in aggregates]
    if len(names) != len(set(names)):
        raise _err("INVALID_CONTRACT", "duplicate aggregate names", names=names)
    return tuple(aggregates)


def _state_size(state: Any, max_state: int) -> int:
    raw = pickle.dumps(state, protocol=pickle.HIGHEST_PROTOCOL)
    if max_state > 0 and len(raw) > max_state:
        raise _err("BUDGET_EXCEEDED", "aggregate state exceeds max_state_bytes", size=len(raw))
    return len(raw)


def _store_states(states: list[Any], count: int, services: RuntimeServices) -> AggregateState:
    raw = pickle.dumps(states, protocol=pickle.HIGHEST_PROTOCOL)
    ref = services.store.put(
        iter([raw]), max_bytes=len(raw), media_type=AGGREGATE_STATE_MEDIA_TYPE
    )
    return AggregateState(
        kind="aggregate", schema_version=AGGREGATE_STATE_VERSION, state_ref=ref, count=count
    )


def _load_states(state: AggregateState, services: RuntimeServices) -> list[Any]:
    if state.kind != "aggregate" or state.schema_version != AGGREGATE_STATE_VERSION:
        raise _err("INCOMPATIBLE_HISTORY", "aggregate state schema mismatch", version=state.schema_version)
    ref = state.state_ref
    limit = services.bounds.max_state_bytes
    data = b"".join(services.store.read(ref, max_bytes=limit if limit > 0 else ref.size_bytes))
    return pickle.loads(data)


def _block_rows(services: RuntimeServices) -> int:
    bounds = services.bounds
    return min(8192, max(1, bounds.max_batch_bytes // max(1, bounds.max_row_bytes)))


def accumulate_partial(
    records: Iterator[Record], *, spec: KernelSpec, services: RuntimeServices
) -> AggregateState:
    """K013: bounded block accumulation with pre-merge state budget protection."""
    aggregates = _resolve_aggregates(spec, services)
    max_state = services.bounds.max_state_bytes
    states = [aggregate.initial_state() for aggregate in aggregates]
    sizes = [_state_size(state, max_state) for state in states]
    count = 0
    block_rows = _block_rows(services)
    block: list[dict[str, Any]] = []

    def consume() -> None:
        nonlocal count
        rows = tuple(block)
        for index, aggregate in enumerate(aggregates):
            partial = aggregate.aggregate_partial(rows)
            partial_size = _state_size(partial, max_state)
            if max_state > 0 and sizes[index] + partial_size > max_state:
                raise _err("BUDGET_EXCEEDED", "aggregate merge coexistence exceeds max_state_bytes")
            merged = aggregate.merge_states(states[index], partial)
            sizes[index] = _state_size(merged, max_state)
            states[index] = merged
        count += len(rows)

    for record in records:
        _check_stop(services)
        block.append(_materialize_row(record, services))
        if len(block) >= block_rows:
            consume()
            block = []
    if block:
        consume()
    return _store_states(states, count, services)


def merge_partials(
    partials: Iterator[AggregateState], *, spec: KernelSpec, services: RuntimeServices
) -> AggregateState:
    """K014: deterministic-order associative merge of mergeable partial states."""
    if not spec.mergeable:
        raise _err("UNSUPPORTED_CAPABILITY", "non-mergeable kernel cannot decompose into partials")
    aggregates = _resolve_aggregates(spec, services)
    max_state = services.bounds.max_state_bytes
    collected = list(partials)
    collected.sort(key=lambda state: state.state_ref.digest)
    states = [aggregate.initial_state() for aggregate in aggregates]
    sizes = [_state_size(state, max_state) for state in states]
    count = 0
    for partial in collected:
        _check_stop(services)
        incoming = _load_states(partial, services)
        if len(incoming) != len(aggregates):
            raise _err("INCOMPATIBLE_HISTORY", "partial aggregate arity mismatch")
        for index, aggregate in enumerate(aggregates):
            new = incoming[index]
            new_size = _state_size(new, max_state)
            if max_state > 0 and sizes[index] + new_size > max_state:
                raise _err("BUDGET_EXCEEDED", "aggregate merge coexistence exceeds max_state_bytes")
            merged = aggregate.merge_states(states[index], new)
            sizes[index] = _state_size(merged, max_state)
            states[index] = merged
        count += partial.count
    return _store_states(states, count, services)


def finalize_aggregate(state: AggregateState, *, spec: KernelSpec) -> TerminalResult:
    """K015: restore aggregate names and sum/min/max/mean/std return semantics."""
    services = _finalize_services()
    aggregates = _resolve_aggregates(spec, services)
    states = _load_states(state, services)
    if len(states) != len(aggregates):
        raise _err("INCOMPATIBLE_HISTORY", "aggregate state arity mismatch")
    if state.count == 0:
        return TerminalResult(
            kind="aggregate", rows=0, scalar=None, result_ref=None,
            write_result=None, complete=True, completion="source_exhausted",
        )
    result = {}
    for aggregate, accumulator in zip(aggregates, states):
        result[aggregate.name] = aggregate.finalize_state(accumulator)
    raw = pickle.dumps(result, protocol=pickle.HIGHEST_PROTOCOL)
    result_ref = services.store.put(
        iter([raw]), max_bytes=len(raw), media_type=AGGREGATE_RESULT_MEDIA_TYPE
    )
    try:
        _canonical(result)
        scalar = result
    except (ValueError, TypeError):
        scalar = None
    return TerminalResult(
        kind="aggregate", rows=state.count, scalar=scalar, result_ref=result_ref,
        write_result=None, complete=True, completion="source_exhausted",
    )


def _merge_block(aggregates, states, rows, *, max_bytes, workspace):
    """Charge each partial together with the current state before user merge code runs."""
    from . import state_codec

    for index, aggregate in enumerate(aggregates):
        partial = aggregate.aggregate_partial(rows)
        state_codec.measure([states, partial], max_bytes=max_bytes, workspace_bytes=workspace)
        state_codec.dumps([states, partial], max_bytes=max_bytes)
        states[index] = aggregate.merge_states(states[index], partial)
        del partial
        state_bytes, _ = state_codec.measure(states, max_bytes=max_bytes, workspace_bytes=workspace)
        state_codec.dumps(states, max_bytes=max_bytes)
    return state_bytes


def _integer_block_columns(aggregates, states):
    """Inspect the native lifecycle without depending on its instance layout.

    These classes are pinned by the original program seal. A subclass or an
    instance shadowing a class attribute can run arbitrary callback code.
    """
    from demiflow.data.aggregate import Count, Sum
    columns, sums = [], []
    for aggregate, state in zip(aggregates, states):
        kind = type(aggregate)
        if kind not in (Count, Sum) or any(hasattr(kind, name) for name in vars(aggregate)):
            return None
        if type(aggregate.ignore_nulls) is not bool:
            return None
        if state is not None and (type(state) is not int or state.bit_length() > 64):
            return None
        column = aggregate.get_target_column()
        if column is not None:
            if type(column) is not str:
                return None
            columns.append(column)
            if kind is Sum:
                sums.append(column)
    return columns, sums


def _exact_integer_row(row, columns, sums):
    if not isinstance(row, Mapping) or any(column not in row for column in columns):
        return False
    for column in sums:
        value = row[column]
        if value is not None and (type(value) not in (int, bool) or value.bit_length() > 64):
            return False
    return True


def aggregate_records(program, read_records, *, services, workspace, batch_eligible):
    """Reduce the durable input in order under its original worker grant.

    Only exact, unmodified Count/Sum implementations may batch. Integers are
    associative; after a float (or any other non-integral summand), even a run
    of integers must fold against the previous state one row at a time.
    """
    from . import state_codec
    from ..contracts.dataflow_errors import DataflowError
    from contextlib import closing

    aggregates = program if isinstance(program, tuple) else (program,)
    states = [a.initial_state() for a in aggregates]
    bounds = services.bounds
    state_bytes, _ = state_codec.measure(states, max_bytes=bounds.max_state_bytes, workspace_bytes=workspace)
    max_rows = min((services.constants.get('native_limits') or {}).get('batch_rows', 1),
                   bounds.max_pending_events, bounds.max_output_rows)
    eligible = _integer_block_columns(aggregates, states) if batch_eligible and max_rows > 1 else None
    batchable = eligible is not None
    columns, sums = eligible if batchable else ((), ())
    state_room = state_bytes
    if batchable:
        # Any prefix (and a whole partial) sums no more than max_output_rows
        # signed 64-bit magnitudes plus its initial integer. Size the larger
        # representation with the original codec before buffering input.
        worst = -(1 << (65 + bounds.max_output_rows.bit_length()))
        try:
            # Match initial_states' list capacity as well as its length.
            _, state_room = state_codec.measure([[worst for _ in states], worst],
                max_bytes=bounds.max_state_bytes, workspace_bytes=workspace)
        except DataflowError as error:
            if error.code != 'BUDGET_EXCEEDED':
                raise
            batchable = False
    block = []
    payload = retained = 0

    def flush():
        nonlocal payload, retained, state_bytes
        if block:
            state_bytes = _merge_block(aggregates, states, tuple(block),
                                       max_bytes=bounds.max_state_bytes, workspace=state_room)
            block.clear()
        payload = retained = 0

    from ..dataflow_record_frames import resident_limit
    grant = resident_limit(bounds, services)

    def codec_call(function, *args, retained_input=0, **kwargs):
        # Every admission below is side-effect free until the checked
        # allocation. If the pending block occupies the last room, fold it
        # exactly once, then retry the same input/offset.
        nonlocal batchable
        reserve = state_room if batchable else state_bytes
        try:
            return function(*args, services=services, retained=grant - workspace + retained + reserve + retained_input, **kwargs)
        except DataflowError as error:
            if not batchable or error.code != 'BUDGET_EXCEEDED' or 'codec_workspace_bytes' not in error.context:
                raise
            peak = error.context['codec_workspace_bytes'] - (grant - workspace + retained + reserve)
        flush()
        if peak > workspace - state_room:
            # An indivisible frame can reclaim the optional block scratch too.
            batchable = False
        reserve = state_room if batchable else state_bytes
        return function(*args, services=services, retained=grant - workspace + reserve + retained_input, **kwargs)

    with closing(read_records(codec_call=codec_call)) as records:
        for row, size, record_room, reader_room in records:
            if batchable and not _exact_integer_row(row, columns, sums):
                flush()
                batchable = False
            if batchable:
                # The already admitted decode workspace bounds the thawed
                # row. List growth and both temporary Sum column lists coexist.
                charge = record_room + 64
                if block and (payload + size > bounds.max_batch_bytes
                              or reader_room + retained + charge + state_room > workspace):
                    flush()
                if reader_room + charge + state_room <= workspace:
                    block.append(row)
                    retained += charge
                    payload += size
                    if len(block) == max_rows:
                        flush()
                    del row
                    continue
            # The public singleton callback sees the same thawed value and
            # order as older cuts; it may observe row boundaries or mutate it.
            state_bytes = _merge_block(aggregates, states, (row,), max_bytes=bounds.max_state_bytes,
                                       workspace=workspace - reader_room - record_room)
            del row
    flush()
    values = {a.name: a.finalize_state(value) for a, value in zip(aggregates, states)}
    state_codec.measure(values, max_bytes=bounds.max_state_bytes,
                        workspace_bytes=workspace - state_bytes)
    return values
