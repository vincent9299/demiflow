"""Arrow batches implementation."""
from __future__ import annotations

from demiflow.data import plan as ops

from collections.abc import Iterator, Mapping
from typing import Any
from demiflow.execution.contracts.dataflow_errors import DataflowError


def single_row_batch(
    row: Mapping[str, Any], batch_format: str, bounds
) -> tuple[Any, int]:
    from demiflow.execution.contracts.record_codec import admit_batch_conversion
    admit_batch_conversion([row], 'pandas' if batch_format == 'default' else batch_format,
        schema=None, bounds=bounds)
    if batch_format in ("default", "pandas"):
        import pandas as pd

        frame = pd.DataFrame([dict(row)])
        return frame, int(frame.memory_usage(deep=True).sum())
    if batch_format == "pyarrow":
        import pyarrow as pa

        table = pa.Table.from_pylist([dict(row)])
        return table, table.nbytes
    raise DataflowError("UNSUPPORTED_CAPABILITY", "unknown batch_format", context={"format": batch_format})

def batch_values(result: Any) -> list[Any]:
    if hasattr(result, "to_pylist"):
        return list(result.to_pylist())
    return list(result)

def rows_to_batch(
    rows: list[dict[str, Any]], batch_format: str, bounds, *, schema=None
) -> tuple[Any, int]:
    from demiflow.execution.contracts.record_codec import admit_batch_conversion
    admit_batch_conversion(rows, batch_format, schema=schema, bounds=bounds)
    if batch_format in ("default", "numpy"):
        import numpy as np
        columns = rows[0].keys() if rows else ()
        batch = {name: np.asarray([row[name] for row in rows]) for name in columns}
        return batch, sum(v.nbytes for v in batch.values())
    if batch_format == "pandas":
        import pandas as pd

        if schema is None:
            frame = pd.DataFrame(rows)
        else:
            import pyarrow as pa
            frame = pa.Table.from_pylist(rows, schema=schema).to_pandas()
        return frame, int(frame.memory_usage(deep=True).sum())
    if batch_format == "pyarrow":
        import pyarrow as pa

        table = pa.Table.from_pylist(rows, schema=schema)
        return table, table.nbytes
    raise DataflowError("UNSUPPORTED_CAPABILITY", "unknown batch_format", context={"format": batch_format})

def batch_to_rows(result: Any, batch_format: str, *, on_schema=None) -> Iterator[dict[str, Any]]:
    if isinstance(result, (list, tuple)):
        if on_schema is not None:
            on_schema(None)
        # Public map_batches accepts row lists for every input batch format.
        # Iterator outputs remain a lazy stream of batches below.
        for row in result:
            if not isinstance(row, Mapping):
                raise DataflowError('INVALID_CONTRACT', 'batch row lists must contain mappings')
            yield dict(row)
    elif isinstance(result, Mapping):
        if on_schema is not None:
            on_schema(None)
        lengths = {len(column) for column in result.values()}
        if len(lengths) > 1:
            raise DataflowError("INVALID_CONTRACT", "batch columns must have equal lengths")
        import numpy as np
        def value(item):
            return item.item() if isinstance(item, np.generic) else item.tolist() if isinstance(item, np.ndarray) else item
        for i in range(next(iter(lengths), 0)):
            yield {name: value(column[i]) for name, column in result.items()}
    elif hasattr(result, "to_pylist"):
        if on_schema is not None:
            on_schema(result.schema)
        for batch in result.to_batches(max_chunksize=1) if hasattr(result, 'to_batches') else (result,):
            for index in range(batch.num_rows):
                yield batch.slice(index, 1).to_pylist()[0]
    elif hasattr(result, "iloc"):
        if on_schema is not None:
            import pyarrow as pa
            on_schema(pa.Schema.from_pandas(result, preserve_index=False))
        for index in range(len(result)):
            yield result.iloc[index:index + 1].to_dict(orient='records')[0]
    else:
        iterator = iter(result)
        try:
            for row in iterator:
                if batch_format == 'rows':
                    if on_schema is not None:
                        on_schema(None)
                    yield dict(row)
                else:
                    yield from batch_to_rows(row, batch_format, on_schema=on_schema)
        finally:
            close = getattr(iterator, 'close', None)
            if close is not None:
                close()


def _select(rows, columns):
    for row in rows:
        yield {k: row[k] for k in columns}

def _apply(rows, operations):
    """Apply the narrow operations embedded in a sealed native worker batch."""
    for op in operations:
        if isinstance(op, ops.MapOp):
            rows = map(ops.StandardCallable(op.callable), rows)
        elif isinstance(op, ops.BoundMapOp):
            rows = map(ops.BoundCallable(op), rows)
        elif isinstance(op, ops.FilterOp):
            rows = filter(ops.StandardCallable(op.callable), rows)
        elif isinstance(op, ops.SelectColumnsOp):
            rows = _select(rows, op.columns)
        elif isinstance(op, ops.DropColumnsOp):
            def drop(source=rows, columns=op.columns):
                for row in source:
                    missing = set(columns) - row.keys()
                    if missing:
                        raise KeyError(f"columns not found: {sorted(missing)}")
                    yield {k: v for k, v in row.items() if k not in columns}
            rows = drop()
        elif isinstance(op, ops.RenameColumnsOp):
            def rename(source=rows, names=op.names):
                for row in source:
                    if isinstance(names, Mapping):
                        targets = [names.get(k, k) for k in row]
                    else:
                        targets = names
                        if len(targets) != len(row):
                            raise ValueError("rename_columns list length must match schema")
                    if len(set(targets)) != len(targets):
                        raise ValueError("rename_columns produces duplicate column names")
                    yield dict(zip(targets, row.values()))
            rows = rename()
        else:
            raise NotImplementedError(f"partition kernel cannot execute {type(op).__name__}")
    yield from rows

def finish_typed_batch(batch, *, columns, encoded_operations, schema):
    """Partition-local callbacks, one instance per configured input task batch."""
    import pyarrow as pa
    import cloudpickle
    operations = cloudpickle.loads(encoded_operations)
    rows = ({c.name: row[c.value] for c in columns if row[c.present]} for row in batch.to_pylist())
    return pa.RecordBatch.from_pylist(list(_apply(rows, operations)), schema=schema)
