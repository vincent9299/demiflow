"""Backend-neutral, bounded global aggregation contracts.

The public lifecycle follows Ray Data's AggregateFnV2: aggregate one block,
combine associative partial states, then finalize a bounded Driver result.
"""
from __future__ import annotations

import math
from abc import ABC, abstractmethod
from typing import Any, Callable, Generic, Iterable, Mapping, Optional, TypeVar

Block = tuple[Mapping[str, Any], ...]
Accumulator = TypeVar("Accumulator")
Output = TypeVar("Output")


def _is_null(value: Any) -> bool:
    if value is None:
        return True
    return isinstance(value, float) and math.isnan(value)


def _rows(block: Any) -> Iterable[Mapping[str, Any]]:
    if hasattr(block, "to_pylist"):
        return block.to_pylist()
    if hasattr(block, "to_dict"):
        return block.to_dict(orient="records")
    if isinstance(block, Mapping):
        return (block,)
    return block


def normalize_block(block: Any) -> Block:
    """Normalize a backend-native block at the worker boundary."""
    return tuple(dict(row) for row in _rows(block))


def _zero_int() -> int:
    return 0


def _zero_mean() -> list[int]:
    return [0, 0]


def _zero_std() -> list[float]:
    return [0.0, 0.0, 0]


def _positive_infinity() -> float:
    return float("inf")


def _negative_infinity() -> float:
    return float("-inf")


class AggregateFnV2(ABC, Generic[Accumulator, Output]):
    """A mergeable, bounded block aggregation compatible with Ray's lifecycle."""

    def __init__(
        self,
        name: str,
        zero_factory: Callable[[], Accumulator],
        *,
        on: Optional[str],
        ignore_nulls: bool,
    ) -> None:
        normalized = str(name or "").strip()
        if not normalized:
            raise ValueError("AggregateFnV2 requires a non-empty name")
        if on is not None and (not isinstance(on, str) or not on):
            raise ValueError(f"Column to aggregate on must be a non-empty string (got {on!r})")
        if not callable(zero_factory):
            raise TypeError("zero_factory must be callable")
        self.name = normalized
        self._zero_factory = zero_factory
        self._target_col_name = on
        self._ignore_nulls = bool(ignore_nulls)

    def get_target_column(self) -> Optional[str]:
        """Return the target column name, or None for row-count style aggregation."""
        return self._target_col_name

    @property
    def ignore_nulls(self) -> bool:
        return self._ignore_nulls

    def create_zero(self) -> Accumulator:
        """Create a fresh bounded, serializable zero accumulator."""
        return self._zero_factory()

    def initial_state(self) -> Optional[Accumulator]:
        """Create the initial merge state with configured null semantics."""
        # None is the identity for an empty sequence when nulls are ignored.
        return None if self._ignore_nulls else self._zero_factory()

    def aggregate_partial(self, block: Block) -> Optional[Accumulator]:
        """Aggregate one normalized block into an optional bounded partial state."""
        value = self.aggregate_block(block)
        return None if self._ignore_nulls and _is_null(value) else value

    def merge_states(
        self, current: Optional[Accumulator], new: Optional[Accumulator],
    ) -> Optional[Accumulator]:
        """Associatively merge two optional partial states with configured null semantics."""
        if self._ignore_nulls:
            if _is_null(current):
                return new
            if _is_null(new):
                return current
        else:
            if _is_null(current) or _is_null(new):
                return None
        return self.combine(current, new) # type: ignore[arg-type]

    def finalize_state(self, accumulator: Optional[Accumulator]) -> Optional[Output]:
        """Convert an optional merged accumulator into the final bounded result."""
        return accumulator if _is_null(accumulator) else self.finalize(accumulator) # type: ignore[arg-type]

    @abstractmethod
    def aggregate_block(self, block: Block) -> Optional[Accumulator]:
        """Aggregate one normalized block into a bounded partial accumulator."""
        raise NotImplementedError

    @abstractmethod
    def combine(
        self, current_accumulator: Accumulator, new: Accumulator,
    ) -> Accumulator:
        """Associatively combine two bounded accumulators."""
        raise NotImplementedError

    def finalize(self, accumulator: Accumulator) -> Output:
        """Convert the merged accumulator into the public aggregate result."""
        return accumulator # type: ignore[return-value]


class Count(AggregateFnV2[int, int]):
    def __init__(self, on: Optional[str] = None, ignore_nulls: bool = False, alias_name: Optional[str] = None) -> None:
        super().__init__(alias_name or f"count({on or ''})", _zero_int, on=on, ignore_nulls=ignore_nulls)

    def aggregate_block(self, block: Any) -> int:
        """Count rows or non-null target-column values in one block."""
        rows = _rows(block)
        if self._target_col_name is None:
            try:
                return len(rows) # type: ignore[arg-type]
            except TypeError:
                return sum(1 for _ in rows)
        column = self._target_col_name
        if self._ignore_nulls:
            return sum(not _is_null(row[column]) for row in rows)
        count = 0
        for row in rows:
            row[column]
            count += 1
        return count

    def combine(self, current_accumulator: int, new: int) -> int:
        """Combine block counts by addition."""
        return current_accumulator + new


class _ColumnAggregate(AggregateFnV2[Accumulator, Output], ABC):
    def __init__(self, name: str, zero_factory: Callable[[], Accumulator], *, on: Optional[str], ignore_nulls: bool) -> None:
        if on is None:
            raise ValueError(f"Column to aggregate on has to be provided (got {on})")
        super().__init__(name, zero_factory, on=on, ignore_nulls=ignore_nulls)

    def _values(self, block: Any) -> list[Any]:
        values = [row[self._target_col_name] for row in _rows(block)]
        if self._ignore_nulls:
            return [value for value in values if not _is_null(value)]
        return values


class Sum(_ColumnAggregate[Any, Any]):
    def __init__(self, on: Optional[str] = None, ignore_nulls: bool = True, alias_name: Optional[str] = None) -> None:
        super().__init__(alias_name or f"sum({on})", _zero_int, on=on, ignore_nulls=ignore_nulls)

    def aggregate_block(self, block: Any) -> Any:
        """Compute one block-local sum under configured null handling."""
        values = self._values(block)
        if not values:
            return None if self._ignore_nulls else 0
        if any(_is_null(value) for value in values):
            return None
        return sum(values)

    def combine(self, current_accumulator: Any, new: Any) -> Any:
        """Combine partial sums by addition."""
        return current_accumulator + new


class Min(_ColumnAggregate[Any, Any]):
    def __init__(self, on: Optional[str] = None, ignore_nulls: bool = True, alias_name: Optional[str] = None, zero_factory: Callable[[], Any] = _positive_infinity) -> None:
        super().__init__(alias_name or f"min({on})", zero_factory, on=on, ignore_nulls=ignore_nulls)

    def aggregate_block(self, block: Any) -> Any:
        """Compute one block-local minimum under configured null handling."""
        values = self._values(block)
        return None if not values or any(_is_null(value) for value in values) else min(values)

    def combine(self, current_accumulator: Any, new: Any) -> Any:
        """Combine partial minima."""
        return min(current_accumulator, new)


class Max(_ColumnAggregate[Any, Any]):
    def __init__(self, on: Optional[str] = None, ignore_nulls: bool = True, alias_name: Optional[str] = None, zero_factory: Callable[[], Any] = _negative_infinity) -> None:
        super().__init__(alias_name or f"max({on})", zero_factory, on=on, ignore_nulls=ignore_nulls)

    def aggregate_block(self, block: Any) -> Any:
        """Compute one block-local maximum under configured null handling."""
        values = self._values(block)
        return None if not values or any(_is_null(value) for value in values) else max(values)

    def combine(self, current_accumulator: Any, new: Any) -> Any:
        """Combine partial maxima."""
        return max(current_accumulator, new)


class Mean(_ColumnAggregate[list[Any], float]):
    def __init__(self, on: Optional[str] = None, ignore_nulls: bool = True, alias_name: Optional[str] = None) -> None:
        super().__init__(alias_name or f"mean({on})", _zero_mean, on=on, ignore_nulls=ignore_nulls)

    def aggregate_block(self, block: Any) -> Optional[list[Any]]:
        """Compute one block-local sum-and-count accumulator."""
        values = self._values(block)
        if not values or any(_is_null(value) for value in values):
            return None
        return [sum(values), len(values)]

    def combine(self, current_accumulator: list[Any], new: list[Any]) -> list[Any]:
        """Combine sum-and-count accumulators associatively."""
        return [current_accumulator[0] + new[0], current_accumulator[1] + new[1]]

    def finalize(self, accumulator: list[Any]) -> float:
        """Divide the merged sum by the merged count."""
        return accumulator[0] / accumulator[1]


class Std(_ColumnAggregate[list[float], float]):
    def __init__(self, on: Optional[str] = None, ddof: int = 1, ignore_nulls: bool = True, alias_name: Optional[str] = None) -> None:
        self._ddof = int(ddof)
        super().__init__(alias_name or f"std({on})", _zero_std, on=on, ignore_nulls=ignore_nulls)

    def aggregate_block(self, block: Any) -> Optional[list[float]]:
        """Compute one block-local bounded variance accumulator."""
        values = self._values(block)
        if not values or any(_is_null(value) for value in values):
            return None
        count = len(values)
        mean = sum(values) / count
        m2 = sum((value - mean) ** 2 for value in values)
        return [m2, mean, count]

    def combine(self, current_accumulator: list[float], new: list[float]) -> list[float]:
        """Combine variance accumulators with the parallel variance formula."""
        m2_a, mean_a, count_a = current_accumulator
        m2_b, mean_b, count_b = new
        count = count_a + count_b
        delta = mean_b - mean_a
        mean = (mean_a * count_a + mean_b * count_b) / count
        m2 = m2_a + m2_b + delta * delta * count_a * count_b / count
        return [m2, mean, count]

    def finalize(self, accumulator: list[float]) -> float:
        """Return standard deviation using the configured delta degrees of freedom."""
        m2, _, count = accumulator
        return math.nan if count - self._ddof <= 0 else math.sqrt(m2 / (count - self._ddof))


class AbsMax(_ColumnAggregate[Any, Any]):
    def __init__(self, on: Optional[str] = None, ignore_nulls: bool = True, alias_name: Optional[str] = None, zero_factory: Callable[[], Any] = _zero_int) -> None:
        super().__init__(alias_name or f"abs_max({on})", zero_factory, on=on, ignore_nulls=ignore_nulls)

    def aggregate_block(self, block: Any) -> Any:
        """Compute one block-local maximum absolute value."""
        values = self._values(block)
        return None if not values or any(_is_null(value) for value in values) else max(abs(value) for value in values)

    def combine(self, current_accumulator: Any, new: Any) -> Any:
        """Combine partial absolute maxima."""
        return max(current_accumulator, new)


__all__ = ["AbsMax", "AggregateFnV2", "Block", "Count", "Max", "Mean", "Min", "Std", "Sum"]
