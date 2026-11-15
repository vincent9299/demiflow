"""Neutral input handles for already-materialized bounded driver data (F001-F005).

These handles wrap driver-created values (row lists, Arrow tables, NumPy arrays,
pandas frames) without creating a run or executing an input iterator. They expose
the frozen ``MaterializedHandle`` protocol (``num_blocks``/``rows``/``release``)
so the unified source reader can bind and snapshot them across workers.
"""
from __future__ import annotations

import copy
import sys
from collections.abc import Iterator
from typing import TYPE_CHECKING, Any

from demiflow.execution.contracts.dataflow_errors import DataflowError

if TYPE_CHECKING:
    from demiflow.execution.contracts.dataflow import Bounds, MaterializedHandle

_FORMATS = ("items", "arrow", "numpy", "pandas")
_DEFAULT_INPUT_MAX_BYTES = 256 * 1024 * 1024


def build_input_handle(
    value: object, *, format: str, bounds: "Bounds | None" = None,
) -> "MaterializedHandle":
    """Wrap already-materialized bounded driver data as a neutral input handle."""
    if format not in _FORMATS:
        raise DataflowError("INVALID_CONTRACT", f"unsupported input format: {format!r}")
    max_bytes = _DEFAULT_INPUT_MAX_BYTES if bounds is None else int(bounds.max_resident_bytes)
    return InputMaterializedHandle(value, format=format, max_bytes=max_bytes)


class InputMaterializedHandle:
    """Read-only, bounded view over one materialized driver value."""

    def __init__(self, value: object, *, format: str, max_bytes: int) -> None:
        if format not in _FORMATS:
            raise DataflowError("INVALID_CONTRACT", f"unsupported input format: {format!r}")
        if max_bytes is None or int(max_bytes) <= 0:
            raise DataflowError("INVALID_CONTRACT", "input max_bytes must be positive")
        self._format = format
        self._max_bytes = int(max_bytes)
        self._released = False

        objects = _normalize(value, format)
        row_count = _row_count(objects, format)
        size_bytes = _size_bytes(objects, format)
        if size_bytes > self._max_bytes:
            raise DataflowError(
                "BUDGET_EXCEEDED",
                f"input {size_bytes} bytes exceeds resident budget {self._max_bytes}",
            )
        self._value = _safe_copy(objects, format)
        self._row_count = row_count
        self._num_blocks = 0 if row_count == 0 else 1

    def rows(self) -> Iterator[Any]:
        self._check_open()
        return self._iter_rows()

    def num_blocks(self) -> int:
        self._check_open()
        return self._num_blocks

    def release(self) -> None:
        if self._released:
            return
        self._released = True
        self._value = None

    def _check_open(self) -> None:
        if self._released:
            raise DataflowError("FENCED", "input materialized handle was released")

    def _iter_rows(self) -> Iterator[Any]:
        value = self._value
        if value is None:
            raise DataflowError("FENCED", "input materialized handle was released")
        fmt = self._format
        if fmt == "items":
            for row in value:
                yield row
        elif fmt == "arrow":
            for table in value:
                for batch in table.to_batches():
                    for row in batch.to_pylist():
                        yield row
        elif fmt == "numpy":
            for array in value:
                for item in array:
                    yield {"data": item}
        elif fmt == "pandas":
            for frame in value:
                for row in frame.to_dict(orient="records"):
                    yield row


def _normalize(value: object, format: str) -> Any:
    if format == "items":
        if isinstance(value, Iterator) or not isinstance(value, list):
            raise DataflowError("INVALID_CONTRACT", "from_items requires a materialized list")
        return value
    if format == "arrow":
        import pyarrow as pa

        if isinstance(value, bytes):
            value = [pa.ipc.open_stream(pa.BufferReader(value)).read_all()]
        elif isinstance(value, pa.Table):
            value = [value]
        elif isinstance(value, (list, tuple)):
            value = list(value)
        else:
            raise DataflowError("INVALID_CONTRACT", "from_arrow requires Arrow table data")
        tables = []
        for table in value:
            if isinstance(table, bytes):
                table = pa.ipc.open_stream(pa.BufferReader(table)).read_all()
            if not isinstance(table, pa.Table):
                raise DataflowError("INVALID_CONTRACT", "from_arrow requires Arrow tables")
            tables.append(table)
        return tables
    if format == "numpy":
        import numpy as np

        values = value if isinstance(value, (list, tuple)) else [value]
        arrays = []
        for array in values:
            if not isinstance(array, np.ndarray):
                raise DataflowError("INVALID_CONTRACT", "from_numpy requires NumPy arrays")
            arrays.append(array)
        return arrays
    if format == "pandas":
        import pandas as pd

        values = value if isinstance(value, (list, tuple)) else [value]
        frames = []
        for frame in values:
            if not isinstance(frame, pd.DataFrame):
                raise DataflowError("INVALID_CONTRACT", "from_pandas requires pandas frames")
            frames.append(frame)
        return frames
    raise DataflowError("INVALID_CONTRACT", f"unsupported input format: {format!r}")


def _row_count(objects: Any, format: str) -> int:
    if format == "items":
        return len(objects)
    if format == "arrow":
        return sum(int(table.num_rows) for table in objects)
    if format == "numpy":
        return sum(int(array.shape[0]) if array.ndim else 1 for array in objects)
    if format == "pandas":
        return sum(int(len(frame)) for frame in objects)
    raise DataflowError("INVALID_CONTRACT", f"unsupported input format: {format!r}")


def _size_bytes(objects: Any, format: str) -> int:
    if format == "items":
        return sys.getsizeof(objects) + sum(sys.getsizeof(row) for row in objects)
    if format == "arrow":
        return sum(int(table.nbytes) for table in objects)
    if format == "numpy":
        return sum(int(array.nbytes) for array in objects)
    if format == "pandas":
        return sum(int(frame.memory_usage(deep=True).sum()) for frame in objects)
    raise DataflowError("INVALID_CONTRACT", f"unsupported input format: {format!r}")


def _safe_copy(objects: Any, format: str) -> Any:
    if format == "items":
        return copy.deepcopy(objects)
    if format == "arrow":
        return tuple(objects)
    if format == "numpy":
        return [array.copy() for array in objects]
    if format == "pandas":
        return [frame.copy(deep=True) for frame in objects]
    raise DataflowError("INVALID_CONTRACT", f"unsupported input format: {format!r}")