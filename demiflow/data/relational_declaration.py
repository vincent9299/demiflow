"""Pure relational source assembly using the existing graph declarations.

The SDK wraps these sources in Dataset. Execution consumes the original source
classes at their historic paths; no data, worker or runtime state is constructed.
"""
from __future__ import annotations

from typing import Literal, Sequence

from .plan import reject_streaming_map_options
from ..execution.local_kernel import PlanInput, KeyedSource, UnionSource
from ..planning.dataflow_declaration import validate_relational_inputs


def keys(value):
    """Normalize a key spelling; shared with the original relational runtime."""
    return [value] if isinstance(value, str) else list(value)


def compose_keyed_source(
    left: PlanInput,
    kind: Literal["join", "reduce", "groups", "exclude_keys"],
    on,
    *,
    right: PlanInput | None = None,
    right_on=None,
    how="inner",
    suffix="_right",
    reducer=None,
    initial=None,
    chunk_bytes=32 * 1024 * 1024,
    max_rows=32,
    output="items",
    reducer_kwargs=None,
) -> KeyedSource:
    reject_streaming_map_options(left.plan, 'local relational')
    if right is not None:
        reject_streaming_map_options(right.plan, 'local relational')
    fields = tuple(keys(on))
    rfields = tuple(keys(right_on or on))
    if not fields or any(not isinstance(k, str) or not k for k in fields):
        raise ValueError("nonempty key field names are required")
    if chunk_bytes < 1:
        raise ValueError("chunk_bytes must be positive")
    if right is not None:
        if how not in {"inner", "left", "semi", "anti"} or len(fields) != len(rfields):
            raise ValueError("invalid join type or key arity")
    if kind == "groups" and (max_rows < 1 or output in fields or output in {"group_index", "group_last"}):
        raise ValueError("invalid grouping fields or max_rows")
    if kind == "reduce" and not callable(reducer):
        raise TypeError("reducer must be callable")
    source = KeyedSource(kind, left, fields, right,
        rfields, how, suffix, reducer, initial, chunk_bytes, max_rows, output,
        reducer_kwargs=dict(reducer_kwargs or {}))
    validate_relational_inputs(source, expected_inputs=2 if right is not None else 1)
    return source


def compose_union_sources(inputs: Sequence[PlanInput]) -> UnionSource:
    """Flatten only an untransformed union, preserving input order and identity."""
    flattened = []
    for item in inputs:
        if isinstance(item.source, UnionSource) and not item.plan.operations:
            flattened.extend(item.source.inputs)
        else:
            flattened.append(item)
    return UnionSource(tuple(flattened))
