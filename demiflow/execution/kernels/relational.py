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
from demiflow.execution.contracts.dataflow import stage_resident_bytes

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

from demiflow.execution.kernels.dataflow import RECORD_DOMAIN, _check_stop, _cfg, _err, _loads_row, _dumps_row, _materialize_row, _occurrence, _output_record, _OutputBudget
from demiflow.operators.relational import group_key as _group_key
from demiflow.execution.contracts.record_codec import record_identity as _hash_id
from demiflow.execution.kernels.ordering import build_sort_runs, merge_sort_runs


def _program(services: RuntimeServices, key: str, kind: str) -> Any:
    program = services.programs.get(key)
    if program is None:
        raise _err("INVALID_CONTRACT", "required program is not staged", program=key)
    if program.kind != kind:
        raise _err("INVALID_CONTRACT", "program kind mismatch", program=key, expected=kind)
    return program.value



def typed_join_pairs(left, right, *, spec, services, retained_key):
    """Compute with raw declared key columns; finite ports keep the fallback input."""
    from demiflow.execution.contracts.relational_region import TYPED_JOIN_SUFFIX, SQL_CHILD_RSS, SQL_PARENT_WINDOW
    from demiflow.execution.contracts.record_codec import ARROW_MEDIA_TYPES
    from demiflow.execution.storage.arrow_payload import batch_cache
    from demiflow.operators.relational_sql import typed_join_query, json_key_type
    if not any(s.stage_id.endswith(TYPED_JOIN_SUFFIX) for s in services.assignment.stages):
        return None
    if services.query_region_offsets is None:
        raise _err('UNSUPPORTED_CAPABILITY', 'sealed typed join requires its SQL host')
    import pyarrow as pa
    window = min(SQL_PARENT_WINDOW // 4, SQL_CHILD_RSS // 16)
    columns = (tuple(spec.configuration['on']), tuple(spec.configuration.get('right_on') or spec.configuration['on']))
    tables, inputs, types = {}, [], None
    retained = 4096
    cache = batch_cache(services.store, services.bounds, services=services)
    from demiflow.execution.storage.record_schema import RecordSchemas
    schemas = RecordSchemas(services)
    for name, port, fields in zip(('lkeys', 'rkeys'), (left, right), columns):
        scan = port.within_offset_budget(window // 2048)
        if scan is None:
            return None
        records, chunks, seen = [], [[] for _ in fields], set()
        previous = None
        first = run = 0
        batch = None
        def flush():
            if batch is not None:
                for parts, field in zip(chunks, fields):
                    parts.append(batch.column(batch.schema.get_field_index(field)).slice(first, run))
        try:
            for record in scan:
                services.check_stop()
                if record.payload_ref is not None and record.payload_ref.media_type in ARROW_MEDIA_TYPES:
                    current, index = cache.locate(record)
                    schema = current.schema
                    if any(field not in schema.names for field in fields):
                        return None
                    declared = tuple(schema.field(field).type for field in fields)
                    retained += 2048
                    if record.payload_ref not in seen:
                        retained += current.get_total_buffer_size() + 1024 * len(fields)
                        seen.add(record.payload_ref)
                    if retained > window:
                        return None
                    if previous != record.payload_ref or index != first + run:
                        flush()
                        batch, first, run = current, index, 0
                        previous = record.payload_ref
                    run += 1
                else:
                    schema = schemas.read(record.schema_ref)
                    if schema is None or any(field not in schema.names for field in fields):
                        return None
                    declared = tuple(schema.field(field).type for field in fields)
                    retained += 2048 + 8 * record.encoded_bytes
                    if retained > window:
                        return None
                    flush()
                    previous, batch, run = None, None, 0
                    row = _materialize_row(record, services)
                    for parts, field, dtype in zip(chunks, fields, declared):
                        parts.append(pa.array([row[field]], type=dtype))
                    del row
                if types is None:
                    types = declared
                if declared != types or not all(json_key_type(t) for t in declared):
                    return None
                records.append(record)
            flush()
        finally:
            scan.close()
        inputs.append(records)
        if not records:
            # Keep empty-source error/typing behavior on the original path.
            return None
        arrays = [pa.chunked_array(parts, type=dtype) for parts, dtype in zip(chunks, types)]
        arrays.append(pa.chunked_array([pa.array(range(len(records)), type=pa.int64())]))
        tables[name] = pa.table(arrays, names=[*('k' + str(i) for i in range(len(fields))), 'off'])
    how = spec.configuration.get('how', 'inner')
    # Prove a complete result bound before starting SQL. Once started, errors
    # and cancellation propagate; there is no retry through another algorithm.
    upper = len(inputs[0]) if how in ('semi', 'anti') else len(inputs[0]) * max(1, len(inputs[1]))
    if upper * 256 + retained > SQL_PARENT_WINDOW or upper * 224 > SQL_CHILD_RSS // 16:
        return None
    sql, functions = typed_join_query(types, how)
    pairs = services.query_region_offsets(sql, tables=tables, max_pairs=upper, rss_bytes=SQL_CHILD_RSS,
                                         check_stop=services.check_stop, key_functions=functions)
    for li, ri in pairs:
        if type(li) is not int or not 0 <= li < len(inputs[0]) or (
                ri is not None and (type(ri) is not int or not 0 <= ri < len(inputs[1]))):
            raise _err('INTERNAL_PROTOCOL', 'typed join returned invalid input offsets')
    # Recovery replays an emitted prefix. Order only these pre-admitted offset
    # pairs, never the business keys or full input rows. The 256 bytes/pair
    # above include tuple/int/list storage, sort keys and Timsort scratch.
    pairs.sort(key=lambda pair: (pair[0], -1 if pair[1] is None else pair[1]))
    services.check_stop()
    retained_key(retained + len(pairs) * 256)
    def output():
        try:
            for li, ri in pairs:
                yield inputs[0][li], inputs[1][ri] if ri is not None else None
        finally:
            retained_key(0)
    return output()


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
        from demiflow.execution.kernels.lance_keys import exclude_keys
        yield from exclude_keys(left, right, spec=spec, services=services)
        return
    from demiflow.execution.kernels.join_spool import join_pairs
    lk = tuple(_cfg(spec, "on"))
    rk = tuple(_cfg(spec, "right_on", list(lk)))
    how = "anti" if spec.kind == "exclude_keys" else _cfg(spec, "how", "inner")
    suffix = _cfg(spec, "suffix", "_right")
    if not lk or len(lk) != len(rk):
        raise _err("INVALID_CONTRACT", "join keys must have equal nonzero length")
    if how not in ("inner", "left", "semi", "anti"):
        raise _err("INVALID_CONTRACT", "unsupported join type", how=how)
    occurrence, budget = _occurrence(spec), _OutputBudget(spec, services, streaming=True)
    from demiflow.execution.scheduler.dataflow_publication_budget import REF_CARRIER
    stored = spec.configuration.get('output_carrier') == REF_CARRIER
    from demiflow.execution.kernels.operator import _SpoolRecords
    key_bytes = [0]
    retain_key = lambda size: key_bytes.__setitem__(0, size)
    pairs = (typed_join_pairs(left, right, spec=spec, services=services, retained_key=retain_key)
             if type(left) is _SpoolRecords and type(right) is _SpoolRecords else None)
    if pairs is None:
        if type(left) is _SpoolRecords:
            left = left.for_join(lk)
        if type(right) is _SpoolRecords:
            right = right.for_join(rk)
        pairs = join_pairs(iter(left), iter(right), left_keys=lk, right_keys=rk,
                           how=how, services=services, bounded=stored,
                           try_small_right=spec.configuration.get('small_right_index') is True,
                           retained_key=retain_key)
    try:
        for sequence, (left_record, right_record) in enumerate(pairs):
            _check_stop(services)
            left_row = _materialize_row(left_record, services)
            out_row = (left_row if right_record is None else merge_join_rows(left_row,
                _materialize_row(right_record, services), lk, rk, suffix))
            record_id = _hash_id([RECORD_DOMAIN, occurrence, "join", sequence])
            if stored:
                from demiflow.execution.contracts.record_codec import _typed_shape, admit_workspace
                from demiflow.execution.contracts.record_codec import _encode_payload
                # One actual left/right pair, bounded key index, and encoded
                # output coexist. No payload-sized transport batch is built.
                native = sum(_typed_shape(r.inline, services.bounds.max_row_bytes)[1]
                             for r in (left_record, right_record) if r is not None)
                size, row_native, tree = _typed_shape(out_row, services.bounds.max_row_bytes)
                from demiflow.execution.kernels.join_spool import _resident_limit
                held = native + row_native + key_bytes[0] + _resident_limit(services) // 16 + 16384
                admit_workspace(tree + 2 * size, services.bounds, resident_bytes=stage_resident_bytes(getattr(services, 'assignment', None)), retained=held)
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
    from demiflow.execution.contracts.dataflow_codec import _thaw_json
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
    from demiflow.execution.kernels.reducer_memory import sort_window
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
    from demiflow.execution.kernels.reducer_memory import declare, check_initial, check_grant
    from demiflow.execution.contracts.relational_region import reducer_contract as contract
    from demiflow.data.plan import CallableSpec
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
    from demiflow.execution.kernels.reducer_memory import check_initial, check_state, yielded_peers
    memory = admit_reducer(spec,services,join_depth=memory_join_depth)
    on = tuple(_cfg(spec, "on"))
    if not on:
        raise _err("INVALID_CONTRACT", "empty group keys")
    program = _program(services, _cfg(spec, "reducer_program"), "callable")
    from demiflow.data.plan import CallableSpec
    from demiflow.data.runtime_contract import validator
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
                from demiflow.execution.contracts.record_codec import _typed_shape as typed_shape
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
