"""Payload limits implementation."""
from __future__ import annotations


def stream_limits(facts, limits, source_batch_size=1, *, retained_rows=0,
                  adjustable_fields=('max_row_bytes', 'max_batch_bytes')):
    """Admit one existing graph using its queue/worker/reader residency model.

    Declared row/batch limits are ceilings, not guaranteed per-slot allocations.
    Resolve a smaller effective ceiling when simultaneous residency requires it.
    This calculation is deterministic from the frozen graph and run budgets.
    """
    from demiflow.planning.dataflow_policy import stream_residency
    if type(source_batch_size) is not int or not 1 <= source_batch_size <= 256:
        raise ValueError('Stream source prefetch exceeds its run memory budget')
    if type(retained_rows) is not int or not 0 <= retained_rows <= limits['max_output_rows']:
        raise ValueError('Bounded delivery row count exceeds run output budget')
    facts = list(facts)
    adjustable_fields = frozenset(adjustable_fields)
    if not adjustable_fields <= {'max_row_bytes', 'max_batch_bytes'}:
        raise ValueError('Only default payload ceilings can be adjusted')

    def candidate(cap):
        result = dict(limits)
        result.update({name: min(limits[name], cap) for name in adjustable_fields})
        if result['max_row_bytes'] > result['max_batch_bytes']:
            return None
        try:
            reservation = stream_residency(facts, result)
        except ValueError as exc:
            if not str(exc).startswith('Stream payload reservation '):
                raise
            return None
        if reservation['reserved_bytes'] + (source_batch_size + retained_rows) * result['max_row_bytes'] > result['memory_bytes']:
            return None
        return result

    maximum = max(limits['max_row_bytes'], limits['max_batch_bytes'])
    admitted = candidate(maximum)
    if admitted is not None:
        return admitted
    # Residency is monotone only over legal row <= batch candidates. When
    # the row ceiling is explicit, batch-only search must not descend into
    # cap < row and mistake that invalid region for an oversized candidate.
    minimum = (limits['max_row_bytes'] if 'max_batch_bytes' in adjustable_fields
               and 'max_row_bytes' not in adjustable_fields else 1)
    low, high, admitted = minimum, maximum - 1, None
    while low <= high:
        middle = (low + high) // 2
        value = candidate(middle)
        if value is None:
            high = middle - 1
        else:
            admitted, low = value, middle + 1
    if admitted is None:
        raise ValueError('Stream fixed residency exceeds its run memory budget')
    return admitted
