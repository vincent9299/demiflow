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
    override_num_blocks: int | None = None,
) -> "MaterializedHandle":
    """Wrap already-materialized bounded driver data as a neutral input handle."""
    if format not in _FORMATS:
        raise DataflowError("INVALID_CONTRACT", f"unsupported input format: {format!r}")
    max_bytes = _DEFAULT_INPUT_MAX_BYTES if bounds is None else int(bounds.max_resident_bytes)
    return InputMaterializedHandle(value, format=format, max_bytes=max_bytes,
                                   override_num_blocks=override_num_blocks)


class InputMaterializedHandle:
    """Read-only, bounded view over one materialized driver value."""

    def __init__(self, value: object, *, format: str, max_bytes: int,
                 override_num_blocks: int | None = None) -> None:
        if format not in _FORMATS:
            raise DataflowError("INVALID_CONTRACT", f"unsupported input format: {format!r}")
        if max_bytes is None or int(max_bytes) <= 0:
            raise DataflowError("INVALID_CONTRACT", "input max_bytes must be positive")
        self._format = format
        self._max_bytes = int(max_bytes)
        self._released = False
        self._block_count = max(1, int(override_num_blocks or 1))
        self._blocks = None

        objects = _normalize(value, format)
        row_count = _row_count(objects, format)
        size_bytes = _size_bytes(objects, format, maximum=self._max_bytes)
        peak = size_bytes * (1 if format == "arrow" else 2)
        if peak > self._max_bytes:
            raise DataflowError(
                "BUDGET_EXCEEDED",
                f"input and copy {peak} bytes exceeds resident budget {self._max_bytes}",
            )
        self._value = _safe_copy(objects, format)
        self._row_count = row_count
        block_size = max(1, (row_count + self._block_count - 1) // self._block_count)
        self._num_blocks = (row_count + block_size - 1) // block_size

    @property
    def row_count(self) -> int:
        self._check_open()
        return self._row_count

    @property
    def blocks(self) -> tuple:
        """Preserve the literal materialization identity used before v3.

        Do not introduce a recovery_reference for in-memory literals: that
        selects a different historical identity branch. Partition boundaries
        are also part of that old identity and must honor override_num_blocks.
        """
        self._check_open()
        if self._blocks is None:
            from ..data.runtime_contract import check_size, RuntimeContractError
            size = max(1, (self._row_count + self._block_count - 1) // self._block_count)
            blocks, block, total = [], [], 0
            for row in self._iter_rows():
                try:
                    if self._format == 'numpy':
                        # The existing typed record codec preserves primitive
                        # NumPy scalar/array dtype and shape. Do not convert to
                        # Python/list values merely to appease the JSON checker.
                        # A row view can retain its backing buffer; input plus
                        # copy were admitted above, and this counts each row
                        # conservatively again for the identity frontier.
                        item = row['data']
                        total += (sys.getsizeof(row) + sys.getsizeof('data')
                                  + sys.getsizeof(item) + int(item.nbytes))
                    else:
                        total += check_size(row, self._max_bytes)
                except RuntimeContractError as exc:
                    code = ('INVALID_CONTRACT' if str(exc).startswith('Unsupported runtime value type:')
                            else 'BUDGET_EXCEEDED')
                    raise DataflowError(code, 'literal identity row refused: ' + str(exc)) from exc
                if total > self._max_bytes:
                    raise DataflowError("BUDGET_EXCEEDED", "literal identity exceeds resident budget")
                block.append(row)
                if len(block) == size:
                    blocks.append(tuple(block))
                    block = []
            if block:
                blocks.append(tuple(block))
            self._blocks = tuple(blocks)
        return self._blocks

    def check_open(self) -> None:
        self._check_open()

    def rows(self) -> Iterator[Any]:
        self._check_open()
        return self._iter_rows()

    def typed_rows(self):
        """Internal reader seam; public rows remain ordinary business values."""
        self._check_open()
        if self._format == 'arrow':
            for table in self._value:
                for batch in table.to_batches(max_chunksize=1):
                    yield batch.to_pylist()[0], table.schema
        elif self._format == 'pandas':
            import pyarrow as pa
            for frame in self._value:
                schema = pa.Schema.from_pandas(frame, preserve_index=False)
                for index in range(len(frame)):
                    yield frame.iloc[index:index + 1].to_dict(orient='records')[0], schema
        else:
            for row in self._iter_rows():
                yield row, None

    def num_blocks(self) -> int:
        self._check_open()
        return self._num_blocks

    def release(self) -> None:
        if self._released:
            return
        self._released = True
        self._value = None
        self._blocks = ()

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
                for batch in table.to_batches(max_chunksize=1):
                    for row in batch.to_pylist():
                        yield row
        elif fmt == "numpy":
            for array in value:
                for item in (array[()],) if array.ndim == 0 else array:
                    yield {"data": item}
        elif fmt == "pandas":
            for frame in value:
                for index in range(len(frame)):
                    yield frame.iloc[index:index + 1].to_dict(orient="records")[0]


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
            from .contracts.array_values import validate_array
            try:
                validate_array(array)
            except ValueError as exc:
                raise DataflowError('INVALID_CONTRACT', str(exc)) from exc
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


def _size_bytes(objects: Any, format: str, *, maximum=_DEFAULT_INPUT_MAX_BYTES) -> int:
    if format == "items":
        from ..data.runtime_contract import check_size, RuntimeContractError
        try:
            return check_size(objects, maximum)
        except RuntimeContractError as exc:
            invalid = str(exc).startswith('Unsupported runtime value type:')
            raise DataflowError('INVALID_CONTRACT' if invalid else 'BUDGET_EXCEEDED',
                                'input refused: ' + str(exc)) from exc
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
