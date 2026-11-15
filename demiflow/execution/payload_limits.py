"""One finite payload contract for native packing, streaming and durable sinks.

Eight MiB is a batching target. Hard row/batch ceilings come from admitted
memory, RSS headroom and caller ceilings; no indivisible row gets an exception
to those ceilings. Logical Arrow bytes and retained Python buffers are distinct.
"""
from contextlib import contextmanager
from contextvars import ContextVar

BATCH_TARGET_BYTES = 8 * 1024**2
MAX_NATIVE_ROW_BYTES = 128 * 1024**2
_ACTIVE = ContextVar('demiflow_payload_limits', default=None)
PAYLOAD_FIELDS = ('memory_bytes', 'max_rss_bytes', 'max_row_bytes', 'max_batch_bytes', 'batch_rows')


def native_row_limit(pool, rss, ceiling=MAX_NATIVE_ROW_BYTES):
    """Reuse native Dataset's pool/16 and RSS/64 resident-copy allowance."""
    estimate = max(BATCH_TARGET_BYTES, min(MAX_NATIVE_ROW_BYTES, pool // 16, rss // 64))
    return max(1, min(ceiling, estimate, pool, max(1, rss // 2)))


def run_row_limit(limits):
    # The driver and one native worker share the run RSS allowance, as in Forge.
    native_rss = max(1, limits['max_rss_bytes'] // 2)
    pool = min(limits['memory_bytes'], max(1, native_rss // 2))
    return native_row_limit(pool, native_rss)


def resolve_payload_bounds(limits, overrides=None, *, inherited_explicit=(), ceiling=None):
    """Default estimates never replace an explicitly declared finite ceiling."""
    explicit = set(overrides or ()) | set(inherited_explicit)
    derived = run_row_limit(limits)
    row = limits['max_row_bytes'] if 'max_row_bytes' in explicit else derived
    row = min(row, MAX_NATIVE_ROW_BYTES, limits['memory_bytes'], max(1, limits['max_rss_bytes'] // 4))
    batch = limits['max_batch_bytes'] if 'max_batch_bytes' in explicit else max(derived, row)
    batch = min(batch, limits['memory_bytes'])
    if ceiling is not None:
        row, batch = min(row, ceiling['max_row_bytes']), min(batch, ceiling['max_batch_bytes'])
    return dict(max_row_bytes=min(row, batch), max_batch_bytes=batch)


def batch_target(limits):
    return min(BATCH_TARGET_BYTES, limits['max_batch_bytes'], limits['max_row_bytes'])


def active_limits(run, base):
    active = _ACTIVE.get()
    return active[1] if active is not None and active[0] is run else base


def stream_limits(facts, limits, source_batch_size=1, *, retained_rows=0,
                  adjustable_fields=('max_row_bytes', 'max_batch_bytes')):
    """Admit one existing graph using its queue/worker/reader residency model.

    Declared row/batch limits are ceilings, not guaranteed per-slot allocations.
    Resolve a smaller effective ceiling when simultaneous residency requires it.
    This calculation is deterministic from the frozen graph and run budgets.
    """
    from .capabilities import stream_residency
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


@contextmanager
def payload_scope(run, plan, *, owner_limits=None, arguments=None, source=None, physical_dataflow=False):
    limits = dict(run.limits)
    if owner_limits is not None:
        for name in PAYLOAD_FIELDS:
            if name in owner_limits:
                limits[name] = min(limits[name], owner_limits[name])
    if source is not None:
        from .queue_execution import queue_source
        queued=queue_source(source)
        if queued is not None:
            ceiling=min(queued.config.limits.max_payload_bytes,queued.config.limits.max_result_bytes)
            limits['max_row_bytes']=min(limits['max_row_bytes'],ceiling)
            limits['max_batch_bytes']=min(limits['max_batch_bytes'],ceiling)
    # The unified physical plan admits each worker/edge profile. Applying the
    # former folded single-stream residency model here both invents a second
    # topology and rejects valid combinations (e.g. async model -> sort).
    if plan is not None and not physical_dataflow:
        from ..data.plan import is_stream_operation
        if any(is_stream_operation(op) for op in plan.operations):
            from .plan_facts import operation_facts
            from .recovery_contract import RecoveryContractError
            try:
                limits = stream_limits([operation_facts(op) for op in plan.operations], limits,
                                       (arguments or {}).get('source_batch_size') or 1,
                                       retained_rows=(arguments or {}).get('_retained_rows',0))
            except ValueError as exc:
                raise RecoveryContractError(str(exc)) from exc
    token = _ACTIVE.set((run, limits))
    try:
        yield limits
    finally:
        _ACTIVE.reset(token)


def arrow_batches(batches, limits):
    """Split borrowed Arrow batches under one logical row/batch contract.

    Borrowed slice offsets are legal. Their retained parent buffers count against
    the memory allowance, rather than being mistaken for logical batch bytes.
    No payload copy is performed by this admission helper.
    """
    import pyarrow as pa
    from ..lance.arrow_batches import bounded_record_batches
    from .recovery_contract import RecoveryContractError
    for value in batches:
        if not isinstance(value, (pa.RecordBatch, pa.Table)):
            raise RecoveryContractError('Lance staging requires native Arrow batches')
        if value.get_total_buffer_size() > limits['memory_bytes']:
            raise RecoveryContractError('Arrow retained buffers exceed memory budget')
        incoming = value.to_batches() if isinstance(value, pa.Table) else (value,)
        yield from bounded_record_batches(incoming, value.schema,
            batch_rows=limits['batch_rows'], batch_bytes=batch_target(limits),
            max_row_bytes=limits['max_row_bytes'])
