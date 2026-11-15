"""Bounded relational kernels (lane K): join, reduce, union, groups.

K005 joins same-key partitions through a restricted DataFusion hash join over a
streaming Arrow reader: a type-tagged key token column preserves exact typed
equality and SQL NULL gives the platform's null-no-match rule, while a pickle
payload column carries the full business row. Duplicate-key multiplicity is kept
(no dedup) and skew cartesian amplification fails on the output budget. No extra
global sort is added (C06 ruling).

K006/K008 group via the bounded external sort in ``ordering`` (contiguous groups
are required by reduce/group_batches, unlike ordinary join output order), fold a
user reducer in provenance order without assuming associativity, and keep large
groups on disk. K007 concatenates inputs in port order preserving multiplicity.
"""
from __future__ import annotations

import copy
import itertools
import pickle
import tempfile
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


def _merge_row(
    left: Mapping[str, Any],
    right: Mapping[str, Any],
    lk: tuple[str, ...],
    rk: tuple[str, ...],
    suffix: str,
) -> dict[str, Any]:
    out = dict(left)
    for key, value in right.items():
        if key in lk and key in rk and key in out and out[key] == value:
            continue
        dest = key if key not in out else key + suffix
        if dest in out:
            raise _err("INVALID_CONTRACT", "join column collision", column=dest)
        out[dest] = value
    return out


def _join_reader(
    records: Iterator[Record],
    fields: tuple[str, ...],
    services: RuntimeServices,
    batch_rows: int,
):
    import pyarrow as pa

    schema = pa.schema([("k", pa.string()), ("p", pa.binary())])

    def batches() -> Iterator[Any]:
        batch: list[dict[str, Any]] = []
        for record in records:
            _check_stop(services)
            row = _materialize_row(record, services)
            null_key = any(row.get(field) is None for field in fields)
            token = None if null_key else _group_key(row, fields)
            batch.append({"k": token, "p": _dumps_row(row)})
            if len(batch) >= batch_rows:
                yield pa.RecordBatch.from_pylist(batch, schema=schema)
                batch = []
        if batch:
            yield pa.RecordBatch.from_pylist(batch, schema=schema)

    return pa.RecordBatchReader.from_batches(schema, batches())


def join_partitions(
    left: Iterator[Record],
    right: Iterator[Record],
    *,
    spec: KernelSpec,
    services: RuntimeServices,
) -> Iterator[Record]:
    """K005: restricted DataFusion join; null/duplicate/skew semantics, no sort."""
    import pyarrow.dataset as ds
    from datafusion import RuntimeEnvBuilder, SessionConfig, SessionContext

    lk = tuple(_cfg(spec, "on"))
    rk = tuple(_cfg(spec, "right_on", list(lk)))
    how = "anti" if spec.kind == "exclude_keys" else _cfg(spec, "how", "inner")
    suffix = _cfg(spec, "suffix", "_right")
    if not lk or len(lk) != len(rk):
        raise _err("INVALID_CONTRACT", "join keys must have equal nonzero length")
    if how not in ("inner", "left", "semi", "anti"):
        raise _err("INVALID_CONTRACT", "unsupported join type", how=how)

    bounds = services.bounds
    batch_rows = min(8192, max(1, bounds.max_batch_bytes // max(1, bounds.max_row_bytes)))
    occurrence = _occurrence(spec)
    budget = _OutputBudget(spec, services)
    sequence = 0

    with tempfile.TemporaryDirectory(prefix="demiflow-join-") as spill:
        config = (
            SessionConfig()
            .with_target_partitions(1)
            .with_batch_size(batch_rows)
            .set("datafusion.optimizer.prefer_hash_join", "true")
        )
        context = SessionContext(
            config,
            RuntimeEnvBuilder()
            .with_fair_spill_pool(bounds.max_resident_bytes)
            .with_disk_manager_specified(spill),
        )
        left_reader = _join_reader(left, lk, services, batch_rows)
        right_reader = _join_reader(right, rk, services, batch_rows)
        context.register_dataset("l", ds.dataset(left_reader, schema=left_reader.schema))
        context.register_dataset("r", ds.dataset(right_reader, schema=right_reader.schema))
        if how == "inner":
            sql = "SELECT l.p AS lp, r.p AS rp FROM l JOIN r ON l.k = r.k"
        elif how == "left":
            sql = "SELECT l.p AS lp, r.p AS rp FROM l LEFT JOIN r ON l.k = r.k"
        elif how == "semi":
            sql = "SELECT l.p AS lp FROM l LEFT SEMI JOIN r ON l.k = r.k"
        else:
            sql = "SELECT l.p AS lp FROM l LEFT ANTI JOIN r ON l.k = r.k"

        for native in context.sql(sql).execute_stream():
            table = native.to_pyarrow().to_pydict()
            lp = table["lp"]
            rp = table.get("rp")
            for index in range(len(lp)):
                _check_stop(services)
                left_row = _loads_row(lp[index])
                if how in ("semi", "anti"):
                    out_row = left_row
                else:
                    right_payload = rp[index] if rp is not None else None
                    if right_payload is None:
                        out_row = left_row
                    else:
                        out_row = _merge_row(left_row, _loads_row(right_payload), lk, rk, suffix)
                record_id = _hash_id([RECORD_DOMAIN, occurrence, "join", sequence])
                out = _output_record(record_id, {"join_index": sequence}, out_row, sequence, services)
                budget.charge(out)
                sequence += 1
                yield out


def _group_sort_spec(spec: KernelSpec, on: tuple[str, ...]) -> KernelSpec:
    from ..contracts.dataflow_codec import _thaw_json
    configuration = _thaw_json(spec.configuration)
    configuration["keys"] = list(on)
    configuration["descending"] = False
    return replace(spec, kind="SortOp", configuration=configuration)


def _ordered_group_rows(
    records: Iterator[Record], on: tuple[str, ...], spec: KernelSpec, services: RuntimeServices
) -> Iterator[tuple[str, dict[str, Any], Record]]:
    sort_spec = _group_sort_spec(spec, on)
    runs = build_sort_runs(records, spec=sort_spec, services=services)
    for record in merge_sort_runs(runs, spec=sort_spec, services=services):
        row = _materialize_row(record, services)
        yield _group_key(row, on), row, record


def _initial_state(spec: KernelSpec, services: RuntimeServices) -> Any:
    program_key = spec.configuration.get("initial_program")
    if program_key is not None:
        return copy.deepcopy(_program(services, program_key, "callable"))
    return copy.deepcopy(spec.configuration.get("initial"))


def reduce_partition(
    records: Iterator[Record], *, spec: KernelSpec, services: RuntimeServices
) -> Iterator[Record]:
    """K006: external grouping, provenance-order fold, no associativity assumed."""
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
        reducer, initial = program, _initial_state(spec, services)
        fn_kwargs = dict(spec.configuration.get("reducer_kwargs") or {})
        validation = None
    if not callable(reducer):
        raise _err("INVALID_CONTRACT", "reducer program must be callable")
    occurrence = _occurrence(spec)
    budget = _OutputBudget(spec, services)
    max_state = services.bounds.max_state_bytes
    sequence = 0

    ordered = _ordered_group_rows(records, on, spec, services)
    for key, items in itertools.groupby(ordered, key=lambda item: item[0]):
        _check_stop(services)
        state = copy.deepcopy(initial)
        for _, row, _record in items:
            if validation is not None:
                validation.inputs((state, row), fn_kwargs)
            state = reducer(state, row, **fn_kwargs)
            if validation is not None:
                state = validation.output(state)
            if max_state > 0 and len(pickle.dumps(state, protocol=pickle.HIGHEST_PROTOCOL)) > max_state:
                raise _err("BUDGET_EXCEEDED", "reducer state exceeds max_state_bytes")
        if state is None:
            continue
        if not isinstance(state, Mapping):
            raise _err("INVALID_CONTRACT", "reducer must return a mapping row or None")
        record_id = _hash_id([RECORD_DOMAIN, occurrence, "reduce", key])
        out = _output_record(record_id, {"group_key": key}, state, sequence, services)
        budget.charge(out)
        sequence += 1
        yield out


def union_inputs(
    inputs: tuple[Iterator[Record], ...], *, spec: KernelSpec, services: RuntimeServices
) -> Iterator[Record]:
    """K007: port-order concatenation; shared reference is not business dedup."""
    budget = _OutputBudget(spec, services)
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
    max_rows = _cfg(spec, "max_rows", 32)
    output = _cfg(spec, "output", "items")
    if not on or max_rows < 1 or output in on or output in ("group_index", "group_last"):
        raise _err("INVALID_CONTRACT", "invalid grouping fields or max_rows")
    occurrence = _occurrence(spec)
    budget = _OutputBudget(spec, services)
    sequence = 0

    ordered = _ordered_group_rows(records, on, spec, services)
    for key, items in itertools.groupby(ordered, key=lambda item: item[0]):
        _check_stop(services)
        batch: list[dict[str, Any]] = []
        base: dict[str, Any] | None = None
        index = 0
        for _, row, _record in items:
            if base is None:
                base = {field: row[field] for field in on}
            if len(batch) == max_rows:
                out = _emit_batch(base, output, batch, index, False, occurrence, sequence, services, budget)
                sequence += 1
                yield out
                batch = []
                index += 1
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
