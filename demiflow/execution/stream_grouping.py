"""Bounded keyed microbatches for the streaming executor; no global sort."""
import math
import sys


def row_size(row, limit):
    """Conservative Python-object estimate before retaining another row.

    Counts container allocations and nested scalar objects, not process RSS or
    transport encoding. Shared objects across different rows count repeatedly.
    """
    pending, seen, size = [row], set(), 0
    while pending:
        value = pending.pop()
        if id(value) in seen:
            continue
        seen.add(id(value))
        size += sys.getsizeof(value)
        if size > limit or len(seen) > 100000:
            raise ValueError('Streaming group row exceeds chunk_bytes or object-count bound')
        if isinstance(value, dict):
            pending.extend(value.keys()); pending.extend(value.values())
        elif isinstance(value, (list, tuple)):
            pending.extend(value)
        elif not isinstance(value, (str, bytes, int, float, bool, type(None))):
            raise TypeError('Streaming grouping accepts nested mappings/sequences and scalar payloads')
    return size


def group_key(row, on):
    values = [row[k] for k in on]
    if any(not isinstance(v, (str, int, float, bool, type(None))) or
           isinstance(v, float) and not math.isfinite(v) for v in values):
        raise ValueError('Streaming grouping keys must be finite scalar values')
    return tuple((type(v), v) for v in values)


def pack_group(rows, *, on, output):
    return [{**{k: rows[0][k] for k in on}, output: rows}]
