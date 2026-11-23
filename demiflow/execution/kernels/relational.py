"""Bounded relational kernels (lane K): join, reduce, union, groups.

K005 uses a bounded key/reference hash index over one journal-owned scratch
file. Business payloads are encoded once and hydrated only for matches; spilled
hash repartitioning relinks offsets and does not copy payloads. Duplicate-key
multiplicity and null-no-match semantics are preserved without global sorting.

K006/K008 group via the bounded external sort in ``ordering`` (contiguous groups
are required by reduce/group_batches, unlike ordinary join output order), fold a
user reducer in provenance order without assuming associativity, and keep large
groups on disk. K007 concatenates inputs in port order preserving multiplicity.
"""
from __future__ import annotations

from demiflow.operators.relational import merge_join_rows
import copy
import itertools
import pickle
from collections.abc import Iterator, Mapping
from dataclasses import replace
from typing import Any

from demiflow.execution.contracts.dataflow import (
    KernelSpec,
    Record,
    RuntimeServices,
)

from .dataflow import (
    RECORD_DOMAIN,
    _check_stop,
    _cfg,
    _err,
    _group_key,
    _hash_id,
    _loads_row,
    _dumps_row,
    _materialize_row,
    _occurrence,
    _output_record,
    _OutputBudget,
)
from .ordering import build_sort_runs, merge_sort_runs


def _program(services: RuntimeServices, key: str, kind: str) -> Any:
    program = services.programs.get(key)
    if program is None:
        raise _err("INVALID_CONTRACT", "required program is not staged", program=key)
    if program.kind != kind:
        raise _err("INVALID_CONTRACT", "program kind mismatch", program=key, expected=kind)
    return program.value



def join_partitions(
    left: Iterator[Record],
    right: Iterator[Record],
    *,
    spec: KernelSpec,
    services: RuntimeServices,
    retain=None,
) -> Iterator[Record]:
    """K005: bounded key/reference hash join, no global business ordering."""
    if spec.kind == 'exclude_keys':
        from .lance_keys import exclude_keys
        yield from exclude_keys(left, right, spec=spec, services=services)
        return
    from .join_spool import join_pairs
    lk = tuple(_cfg(spec, "on"))
    rk = tuple(_cfg(spec, "right_on", list(lk)))
    how = "anti" if spec.kind == "exclude_keys" else _cfg(spec, "how", "inner")
    suffix = _cfg(spec, "suffix", "_right")
    if not lk or len(lk) != len(rk):
        raise _err("INVALID_CONTRACT", "join keys must have equal nonzero length")
    if how not in ("inner", "left", "semi", "anti"):
        raise _err("INVALID_CONTRACT", "unsupported join type", how=how)
    occurrence, budget = _occurrence(spec), _OutputBudget(spec, services, streaming=True)
    from ..dataflow_publication_budget import REF_CARRIER
    stored = spec.configuration.get('output_carrier') == REF_CARRIER
    from .operator import _SpoolRecords
    if type(left) is _SpoolRecords:
        left = left.for_join(lk)
    if type(right) is _SpoolRecords:
        right = right.for_join(rk)
    key_bytes = [0]
    pairs = join_pairs(iter(left), iter(right), left_keys=lk, right_keys=rk,
                       how=how, services=services, bounded=stored,
                       try_small_right=spec.configuration.get('small_right_index') is True,
                       retained_key=lambda size: key_bytes.__setitem__(0, size))
    try:
        for sequence, (left_record, right_record) in enumerate(pairs):
            _check_stop(services)
            left_row = _materialize_row(left_record, services)
            out_row = (left_row if right_record is None else merge_join_rows(left_row,
                _materialize_row(right_record, services), lk, rk, suffix))
            record_id = _hash_id([RECORD_DOMAIN, occurrence, "join", sequence])
            if stored:
                from ..dataflow_record_frames import _typed_shape, admit_workspace
                from ..exchange.dataflow_bridge import _encode_payload
                # One actual left/right pair, bounded key index, and encoded
                # output coexist. No payload-sized transport batch is built.
                native = sum(_typed_shape(r.inline, services.bounds.max_row_bytes)[1]
                             for r in (left_record, right_record) if r is not None)
                size, row_native, tree = _typed_shape(out_row, services.bounds.max_row_bytes)
                from .join_spool import _resident_limit
                held = native + row_native + key_bytes[0] + _resident_limit(services) // 16 + 16384
                admit_workspace(tree + 2 * size, services.bounds, services=services, retained=held)
                raw = _encode_payload(out_row, max_bytes=services.bounds.max_row_bytes)
                ref = services.store.put(iter((raw,)), max_bytes=services.bounds.max_row_bytes,
                                         media_type='application/octet-stream')
                out = Record(record_id, {'join_index':sequence}, ref, None, len(raw), sequence,
                    left_record.schema_ref if how in ('semi', 'anti') else None)
                del raw
                if retain is not None:
                    retain(held)
            else:
                out = _output_record(record_id, {"join_index": sequence}, out_row, sequence,
                    services, schema_ref=left_record.schema_ref if how in ('semi', 'anti') else None)
            budget.charge(out)
            yield out
            del out, out_row, left_row, left_record, right_record
            if retain is not None:
                retain(0)
    finally:
        pairs.close()
        if retain is not None:
            retain(0)


def _group_sort_spec(spec: KernelSpec, on: tuple[str, ...]) -> KernelSpec:
    from ..contracts.dataflow_codec import _thaw_json
    configuration = _thaw_json(spec.configuration)
    configuration["keys"] = list(on)
    configuration["descending"] = False
    return replace(spec, kind="SortOp", configuration=configuration)


def _ordered_group_rows(
    records: Iterator[Record], on: tuple[str, ...], spec: KernelSpec, services: RuntimeServices, *, memory=None
) -> Iterator[tuple[str, dict[str, Any], Record]]:
    sort_spec = _group_sort_spec(spec, on)
    def keyed_records():
        for record in records:
            _group_key(_materialize_row(record, services), on)
            yield record
    from .reducer_memory import sort_window
    from demiflow.execution.contracts.relational_region import reducer_contract as contract
    window = sort_window(services.bounds,key_only=False) if memory is not None or contract(spec) is not None else None
    runs = build_sort_runs(keyed_records(), spec=sort_spec, services=services,window=window)
    for record in merge_sort_runs(runs, spec=sort_spec, services=services,window=window):
        row = _materialize_row(record, services)
        yield _group_key(row, on), row, record
        del row, record


def _initial_state(spec: KernelSpec, services: RuntimeServices) -> Any:
    program_key = spec.configuration.get("initial_program")
    if program_key is not None:
        return copy.deepcopy(_program(services, program_key, "callable"))
    return copy.deepcopy(spec.configuration.get("initial"))


def reduce_partition(
    records: Iterator[Record], *, spec: KernelSpec, services: RuntimeServices, retain=None
) -> Iterator[Record]:
    """K006: external grouping, provenance-order fold, no associativity assumed."""
    on = tuple(_cfg(spec, "on"))
    memory = admit_reducer(spec,services)
    yield from reduce_ordered_groups(_ordered_group_rows(records, on, spec, services,memory=memory),
                                     spec=spec, services=services, retain=retain)


def admit_reducer(spec, services, *, join_depth=None):
    from .reducer_memory import declare, check_initial, check_grant
    from demiflow.execution.contracts.relational_region import reducer_contract as contract
    from ...data.plan import CallableSpec
    memory = contract(spec)
    if memory is not None or getattr(services.assignment,'stages',()):
        program = _program(services,_cfg(spec,'reducer_program'),'callable')
        if isinstance(program,CallableSpec):
            initial = program.call_args[0]
        else:
            ref=spec.configuration.get('initial_program')
            initial=_program(services,ref,'callable') if ref is not None else spec.configuration.get('initial')
        if memory is None:
            # Legacy sealed plans keep their original identity and grant. The
            # transient measurement can only prove that grant sufficient or
            # reject before input/copy/callback/pickle; it never edits the seal.
            memory=declare(initial,services.bounds)
        check_grant(memory,services.bounds,services.assignment,join_depth=join_depth)
        check_initial(initial,memory)
    return memory


def reduce_ordered_groups(ordered, *, spec: KernelSpec, services: RuntimeServices, memory_join_depth=None, retain=None):
    """One original sequential fold; caller proves complete ordered key groups."""
    from .reducer_memory import check_initial,check_state,yielded_peers
    memory = admit_reducer(spec,services,join_depth=memory_join_depth)
    on = tuple(_cfg(spec, "on"))
    if not on:
        raise _err("INVALID_CONTRACT", "empty group keys")
    program = _program(services, _cfg(spec, "reducer_program"), "callable")
    from ...data.plan import CallableSpec
    from ...data.runtime_contract import validator
    if isinstance(program, CallableSpec):
        if len(program.call_args) != 1:
            raise _err('INVALID_CONTRACT', 'Reducer program must bind one initial accumulator')
        reducer = program.target
        initial = program.call_args[0]
        fn_kwargs = dict(program.call_kwargs)
        validation = validator(reducer, program.runtime_contract) if program.runtime_contract is not None else None
    else:
        # Preserve the explicit low-level kernel fixture/legacy program form.
        initial_program = spec.configuration.get('initial_program')
        reducer = program
        initial = (_program(services,initial_program,'callable') if initial_program is not None
                   else spec.configuration.get('initial'))
        fn_kwargs = dict(spec.configuration.get("reducer_kwargs") or {})
        validation = None
    if not callable(reducer):
        raise _err("INVALID_CONTRACT", "reducer program must be callable")
    occurrence = _occurrence(spec)
    budget = _OutputBudget(spec, services, streaming=True)
    max_state = services.bounds.max_state_bytes
    sequence = 0

    def output(state,key,sequence):
        record_id = _hash_id([RECORD_DOMAIN, occurrence, "reduce", key])
        out = _output_record(record_id, {"group_key": key}, state, sequence, services)
        budget.charge(out)
        return out

    missing = object()
    current_key, state = missing, None
    iterator = iter(ordered)
    try:
        # Do not let itertools.groupby keep the previous row alive while the
        # producer hydrates the next one. Only the group key and state survive.
        for key,row,_record in iterator:
            _check_stop(services)
            if current_key is missing or key != current_key:
                if current_key is not missing and state is not None:
                    out=output(state,current_key,sequence)
                    sequence+=1
                    if memory is not None and retain is not None:
                        retain(yielded_peers(initial,state,out,memory,services.bounds,
                                            pending=(row,current_key,key)))
                    try:
                        yield out
                    finally:
                        if retain is not None:
                            retain(0)
                    del out
                state=None  # previous group cannot coexist with the next deepcopy
                current_key=key
                if memory is not None:
                    check_initial(initial,memory)
                state=copy.deepcopy(initial)
            if validation is not None:
                validation.inputs((state,row),fn_kwargs)
            state=reducer(state,row,**fn_kwargs)
            if validation is not None:
                state=validation.output(state)
            if memory is not None:
                check_state(state,memory,serialized_limit=max_state)
            elif max_state>0 and len(pickle.dumps(state,protocol=pickle.HIGHEST_PROTOCOL))>max_state:
                raise _err('BUDGET_EXCEEDED','reducer state exceeds max_state_bytes')
            del row,_record
        if current_key is not missing and state is not None:
            out=output(state,current_key,sequence)
            if memory is not None and retain is not None:
                retain(yielded_peers(initial,state,out,memory,services.bounds))
            yield out
    finally:
        if retain is not None:
            retain(0)
        close=getattr(iterator,'close',None)
        if close is not None:
            close()


def union_inputs(
    inputs: tuple[Iterator[Record], ...], *, spec: KernelSpec, services: RuntimeServices
) -> Iterator[Record]:
    """K007: port-order concatenation; shared reference is not business dedup."""
    budget = _OutputBudget(spec, services, streaming=True)
    for source in inputs:
        iterator = iter(source)
        try:
            for record in iterator:
                _check_stop(services)
                budget.charge(record)
                yield record
        finally:
            close = getattr(iterator, "close", None)
            if close is not None:
                close()


def emit_groups(
    records: Iterator[Record], *, spec: KernelSpec, services: RuntimeServices
) -> Iterator[Record]:
    """K008: finite complete groups, type-faithful keys, bounded batch output."""
    on = tuple(_cfg(spec, "on"))
    ordered = _ordered_group_rows(records, on, spec, services)
    try:
        yield from batch_ordered_groups(ordered, spec=spec, services=services)
    finally:
        ordered.close()


def batch_ordered_groups(ordered, *, spec, services, retained_limit=None):
    """One bounded Dataset group implementation for both ordered inputs."""
    on = tuple(_cfg(spec, "on"))
    max_rows = _cfg(spec, "max_rows", 32)
    output = _cfg(spec, "output", "items")
    if not on or max_rows < 1 or output in on or output in ("group_index", "group_last"):
        raise _err("INVALID_CONTRACT", "invalid grouping fields or max_rows")
    occurrence = _occurrence(spec)
    budget = _OutputBudget(spec, services, streaming=True)
    sequence = 0

    for key, items in itertools.groupby(ordered, key=lambda item: item[0]):
        _check_stop(services)
        batch: list[dict[str, Any]] = []
        base: dict[str, Any] | None = None
        index = 0
        retained = encoded = 0
        for _, row, _record in items:
            if base is None:
                base = {field: row[field] for field in on}
            if len(batch) == max_rows:
                out = _emit_batch(base, output, batch, index, False, occurrence, sequence, services, budget)
                sequence += 1
                yield out
                batch = []
                index += 1
                retained = encoded = 0
            if retained_limit is not None:
                from ..dataflow_record_frames import _typed_shape as typed_shape
                size, resident, _ = typed_shape(row, services.bounds.max_row_bytes)
                encoded += size
                retained += resident + 16  # one slot and its shallow mapping
                if encoded > services.bounds.max_row_bytes:
                    raise _err('BUDGET_EXCEEDED', 'grouped rows exceed the output row ceiling')
                if retained > retained_limit:
                    raise _err('BUDGET_EXCEEDED', 'grouped rows exceed the admitted resident window')
            batch.append(dict(row))
        if batch:
            out = _emit_batch(base, output, batch, index, True, occurrence, sequence, services, budget)
            sequence += 1
            yield out


def _emit_batch(
    base: dict[str, Any],
    output: str,
    batch: list[dict[str, Any]],
    index: int,
    last: bool,
    occurrence: str,
    sequence: int,
    services: RuntimeServices,
    budget: _OutputBudget,
) -> Record:
    row = {**base, output: batch, "group_index": index, "group_last": last}
    record_id = _hash_id([RECORD_DOMAIN, occurrence, "groups", sequence])
    out = _output_record(record_id, {"group_index": index}, row, sequence, services)
    budget.charge(out)
    return out
