"""Versioned seeded shuffle over the existing bounded external sorter.

Equality is equality of the existing typed row codec bytes, including mapping
insertion order. Neither the codec, Record identities nor historical shuffle
keys are changed. A first sort makes identical row occurrences adjacent; the
second hashes the seed, row digest and per-row duplicate ordinal. Only the last
typed key is retained, never a table-sized counter dictionary.
"""
from demiflow.execution.contracts.dataflow import stage_resident_bytes
from dataclasses import dataclass, replace
import hashlib
import sys

from demiflow.execution.kernels.dataflow import _err, _seed, _materialize_row, _input_row_bytes
from demiflow.execution.kernels.ordering import OrderingServices, build_sort_runs, merge_sort_runs, _release_runs, _provenance
from demiflow.operators.ordering import SortWindow
from demiflow.execution.contracts.record_codec import resident_limit, raw_workspace, _typed_shape, _json_shape, _record_metadata
from demiflow.execution.storage.streams import read_document
from demiflow.execution.contracts.record_codec import _encode_payload, _decode_payload

FIELD = 'shuffle_order_version'
VERSION = 'typed-multiset-v1'
_DOMAIN = b'demiflow-shuffle-typed-multiset-v1\x00'


@dataclass(frozen=True)
class ShuffleWindows:
    resident_bytes: int
    input_bytes: int
    metadata_bytes: int
    sort: SortWindow
    input_metadata_bytes: int = 0

    @property
    def output_retained_bytes(self):
        return self.sort.merge_resident_bytes + self.metadata_bytes + self.input_metadata_bytes


def shuffle_windows(services):
    """Subdivide the existing grant; do not alter the graph's resource request.

    First build: input 3G/8 + run G/4 + codec G/4 + metadata G/16.
    Second build: first merge 3G/8 + run G/4 + codec G/4 + metadata G/16.
    The input's original descriptors may remain after its iterator closes;
    they are capped at G/16, making the second build total at most G.
    The merge 3G/8 includes G/8 of record decode room;
    the remaining G/4 covers two reader frames, heap and writer temporaries.
    Final output retains merge + sort/input metadata (at most G/2);
    downstream codecs see that charge.
    """
    grant = resident_limit(services.bounds, stage_resident_bytes(getattr(services, 'assignment', None)))
    metadata = grant // 16
    # Each run reserves 4 KiB for both passes' immutable references and the
    # current/owned/merged list slots. No unbounded list of tiny runs is allowed.
    maximum_runs = metadata // 4096
    if maximum_runs < 1:
        raise _err('RESOURCE_INFEASIBLE', 'seeded shuffle lacks its finite metadata/codec windows')
    frame = grant // 16
    window = SortWindow(resident_bytes=grant // 2, run_bytes=grant // 4,
        frame_bytes=frame, read_chunk_bytes=min(4096, services.bounds.max_row_bytes, max(1, frame // 8)),
        merge_fan_in=2, merge_resident_bytes=3 * grant // 8,
        codec_bytes=grant // 4, merge_codec_bytes=grant // 8,
        merge_frame_multiplier=6, max_runs=maximum_runs, merge_reader_bytes=grant // 4)
    window.validate(services.bounds)
    return ShuffleWindows(grant, 3 * grant // 8, metadata, window)


class _TypedKeys:
    def __init__(self, services, window, *, seed=None):
        self.services, self.window, self.seed = services, window, seed
        self.previous = None
        self.duplicate = 0

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
                row = _materialize_row(record, scoped)
            else:
                payload = read_document(record.payload_ref, services.store, scoped.bounds,
                    resident_bytes=stage_resident_bytes(getattr(scoped, 'assignment', None)), _maximum=services.bounds.max_row_bytes)
                if raw_workspace(payload) > window.codec_bytes:
                    raise _err('BUDGET_EXCEEDED', 'shuffle payload decode exceeds its codec subwindow')
                row = _decode_payload(payload, max_bytes=services.bounds.max_row_bytes)
                del payload
        size, native, tree = _typed_shape(row, services.bounds.max_row_bytes)
        meta_size, meta_memory = _json_shape(_record_metadata(record), services.bounds.max_control_bytes)
        # Row + typed tree, canonical key, record document, byte-tagged outer
        # frame and their encodings coexist. Include the previous duplicate key.
        peak = 2 * native + tree + 12 * (size + meta_size) + 2 * meta_memory + 8192
        if peak > window.codec_bytes:
            raise _err('BUDGET_EXCEEDED', 'shuffle row codec exceeds its reserved subwindow')
        # Base64 wrappers in the unchanged sort frame expand both key and
        # document. Check before constructing those full encoded copies.
        if 3 * (size + meta_size) + 1024 > window.frame_bytes:
            raise _err('BUDGET_EXCEEDED', 'shuffle row exceeds its merge frame subwindow')
        raw = _encode_payload(row, max_bytes=services.bounds.max_row_bytes)
        if self.seed is None:
            # Value equality alone does not identify a duplicate occurrence.
            # Replays of the same original Records must assign each identity
            # the same ordinal even when producer delivery order changes.
            # Across actions these hidden ties can change only equal values.
            return (raw, record.record_id, _provenance(record))
        self.duplicate = self.duplicate + 1 if raw == self.previous else 0
        self.previous = raw
        digest = hashlib.sha256(raw).digest()
        seed = str(self.seed).encode('ascii')
        ordinal = str(self.duplicate).encode('ascii')
        rank = hashlib.sha256(_DOMAIN + seed + b'\x00' + digest + b'\x00' + ordinal).digest()
        # Full bytes plus ordinal make even rank/digest collisions deterministic.
        return (rank, raw, self.duplicate)


def seeded_shuffle(records, *, spec, services, windows=None, retain=None):
    """One owner for both passes; all input/merge iterators close on every exit.

    The existing store accounts all simultaneously published runs and staging
    writes against the original spill budget. Release unpins only our own run
    references; physical deletion/refunds remain with the original store GC.
    """
    if spec.configuration.get(FIELD) != VERSION:
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
        keys.previous = None
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
