"""Versioned seeded shuffle over the existing bounded external sorter.

Equality is equality of the existing typed row codec bytes, including mapping
insertion order. Neither the codec, Record identities nor historical shuffle
keys are changed. A first sort makes identical row occurrences adjacent; the
second hashes the seed, row digest and per-row duplicate ordinal. Only the last
typed key is retained, never a table-sized counter dictionary.
"""
from demiflow.execution.contracts.dataflow import stage_resident_bytes
import sys

from demiflow.execution.kernels.dataflow import _err, _seed
from demiflow.execution.storage.streams import materialize_row as _materialize_row
from demiflow.execution.contracts.record_codec import input_row_bytes as _input_row_bytes
from demiflow.execution.kernels.ordering import OrderingServices, build_sort_runs, merge_sort_runs, _release_runs, _provenance
from demiflow.operators.ordering import SortWindow
from demiflow.execution.contracts.record_codec import resident_limit, raw_workspace, _typed_shape, _json_shape, _record_metadata
from demiflow.execution.storage.streams import read_document
from demiflow.execution.contracts.record_codec import _encode_payload, _decode_payload

from demiflow.execution.contracts.dataflow_kinds import SHUFFLE_ORDER_FIELD, SHUFFLE_ORDER_VERSION


def shuffle_windows(services):
    from demiflow.operators.shuffle_order import shuffle_windows as windows
    grant = resident_limit(services.bounds, stage_resident_bytes(getattr(services, 'assignment', None)))
    return windows(services.bounds, grant)


class _TypedKeys:
    def __init__(self, services, window, *, seed=None):
        self.services, self.window, self.seed = services, window, seed
        from demiflow.operators.shuffle_order import TypedShuffleKey
        self.key = None if seed is None else TypedShuffleKey(seed)

    def __call__(self, record):
        services, window = self.services, self.window
        if 3 * _input_row_bytes(record) + 1024 > window.frame_bytes:
            raise _err('BUDGET_EXCEEDED', 'shuffle row exceeds its merge frame subwindow')
        # Frozen rows are directly understood by the original codec. Avoid a
        # second whole-row thaw merely to compute a key.
        if record.inline is not None:
            row = record.inline
        else:
            scoped = OrderingServices(services, window.codec_bytes)
            from demiflow.execution.contracts.record_codec import is_arrow_record
            if is_arrow_record(record):
                row = _materialize_row(record, store=scoped.store, bounds=scoped.bounds, assignment=getattr(scoped, 'assignment', None))
            else:
                payload = read_document(record.payload_ref, services.store, scoped.bounds,
                    resident_bytes=stage_resident_bytes(getattr(scoped, 'assignment', None)), _maximum=services.bounds.max_row_bytes)
                if raw_workspace(payload) > window.codec_bytes:
                    raise _err('BUDGET_EXCEEDED', 'shuffle payload decode exceeds its codec subwindow')
                row = _decode_payload(payload, max_bytes=services.bounds.max_row_bytes)
                del payload
        size, native, tree = _typed_shape(row, services.bounds.max_row_bytes)
        meta_size, meta_memory = _json_shape(_record_metadata(record), services.bounds.max_control_bytes)
        from demiflow.operators.shuffle_order import encode_shuffle_row
        raw = encode_shuffle_row(row, (size, native, tree), (meta_size, meta_memory),
            window=window, max_bytes=services.bounds.max_row_bytes, encode_payload=_encode_payload)
        if self.seed is None:
            # Value equality alone does not identify a duplicate occurrence.
            # Replays of the same original Records must assign each identity
            # the same ordinal even when producer delivery order changes.
            # Across actions these hidden ties can change only equal values.
            return (raw, record.record_id, _provenance(record))
        return self.key(raw)


def seeded_shuffle(records, *, spec, services, windows=None, retain=None):
    """One owner for both passes; all input/merge iterators close on every exit.

    The existing store accounts all simultaneously published runs and staging
    writes against the original spill budget. Release unpins only our own run
    references; physical deletion/refunds remain with the original store GC.
    """
    if spec.configuration.get(SHUFFLE_ORDER_FIELD) != SHUFFLE_ORDER_VERSION:
        raise _err('INVALID_CONTRACT', 'unknown seeded shuffle ordering version')
    windows = shuffle_windows(services) if windows is None else windows
    window = windows.sort
    first = second = ()
    canonical = ordered = None
    try:
        first = build_sort_runs(records, spec=spec, services=services, window=window,
            key_for_record=_TypedKeys(services, window))
        close = getattr(records, 'close', None)
        if close is not None:
            close()
        canonical = merge_sort_runs(first, spec=spec, services=services, window=window)
        keys = _TypedKeys(services, window, seed=_seed(spec, services))
        second = build_sort_runs(canonical, spec=spec, services=services, window=window,
            key_for_record=keys)
        canonical.close()
        canonical = None
        keys.key.clear()
        _release_runs(first, services)
        first = ()
        ordered = merge_sort_runs(second, spec=spec, services=services, window=window)
        if retain is not None:
            retain(windows.output_retained_bytes)
        yield from ordered
    finally:
        active = sys.exc_info()[1]
        failure = None
        try:
            for iterator in (ordered, canonical, records):
                close = getattr(iterator, 'close', None)
                if close is not None:
                    try:
                        close()
                    except BaseException as exc:
                        failure = exc if failure is None else failure
        finally:
            try:
                _release_runs((*first, *second), services)
            finally:
                if retain is not None:
                    retain(0)
        if failure is not None:
            if active is not None:
                active.add_note('Shuffle iterator cleanup also failed: ' + str(failure))
            else:
                raise failure
