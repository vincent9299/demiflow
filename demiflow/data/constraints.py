"""Backend-neutral physical constraints derived from a logical Dataset plan.

Constraints are conservative: only operations that are source-equivalent may
be exposed to a Datasource. Backends and adapters consume this typed contract;
they do not inspect each other's implementation details.
"""
from __future__ import annotations

from dataclasses import dataclass

from .plan import (
    AddColumnOp, BoundMapOp, DropColumnsOp, FilterOp, FlatMapOp, LimitOp,
    LogicalPlan, MapBatchesOp, MapOp, OperatorLLMMapOp, RandomSampleOp,
    RandomShuffleOp, RandomizeBlockOrderOp, RenameColumnsOp, RepartitionOp,
    SelectColumnsOp, SortOp,
)
from .sources import ItemsSource, LanceSource, MaterializedSource, RangeSource, SourcePlan


@dataclass(frozen=True)
class SourceReadConstraints:
    row_limit: int | None = None


def analyze_source_constraints(plan: LogicalPlan) -> SourceReadConstraints:
    """Extract limits that are safe before any source row transformation.

    Consecutive leading limits compose by taking their minimum. Analysis stops
    at the first non-limit operation because a later limit may constrain mapped
    or filtered rows rather than physical source rows.
    """
    row_limit: int | None = None
    for operation in plan.operations:
        if not isinstance(operation, LimitOp):
            break
        row_limit = (
            operation.limit
            if row_limit is None
            else min(row_limit, operation.limit)
        )
    return SourceReadConstraints(row_limit=row_limit)


def analyze_stage_work_units(
    source: SourcePlan, plan: LogicalPlan,
) -> tuple[int | None, ...]:
    """Return conservative row-count upper bounds for source and op outputs.

    Bounds are planning hints only. They are propagated only through operations
    that cannot increase row cardinality and become unknown after an operation
    that may expand it.
    """
    bound = _source_upper_bound(source)
    values: list[int | None] = [bound]
    non_expanding = (
        MapOp, BoundMapOp, OperatorLLMMapOp, FilterOp, SelectColumnsOp,
        DropColumnsOp, RenameColumnsOp, AddColumnOp, RandomSampleOp, SortOp,
        RepartitionOp, RandomShuffleOp, RandomizeBlockOrderOp,
    )
    for operation in plan.operations:
        if isinstance(operation, LimitOp):
            bound = operation.limit if bound is None else min(bound, operation.limit)
        elif isinstance(operation, non_expanding):
            pass
        elif isinstance(operation, (FlatMapOp, MapBatchesOp)):
            bound = None
        else:
            bound = None
        values.append(bound)
    return tuple(values)


def _source_upper_bound(source: SourcePlan) -> int | None:
    if isinstance(source, ItemsSource):
        return len(source.items)
    if isinstance(source, RangeSource):
        return source.count
    if isinstance(source, MaterializedSource):
        return source.row_count
    if isinstance(source, LanceSource):
        query = source.query
        limit = getattr(query, "limit", None)
        if limit is not None:
            return int(limit)
        top_k = getattr(query, "top_k", None)
        if top_k is not None:
            return int(top_k)
    return None


def stage_parallelism_caps(
    plan: LogicalPlan, *, source_cap: int | None = None,
    terminal_cap: int | None = None,
) -> tuple[int | None, ...]:
    """Align protocol worker caps with source, transforms, and terminal."""
    for label, value in (("source_cap", source_cap), ("terminal_cap", terminal_cap)):
        if value is not None and (
            isinstance(value, bool) or not isinstance(value, int) or value <= 0
        ):
            raise ValueError(f"{label} must be positive")
    return (source_cap, *(None for _ in plan.operations), terminal_cap)


__all__ = [
    "SourceReadConstraints", "analyze_source_constraints",
    "analyze_stage_work_units", "stage_parallelism_caps",
]
