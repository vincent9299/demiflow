"""Narrow callback evaluator loaded by historical native worker programs."""
from collections.abc import Mapping

from ..data import plan as ops


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
