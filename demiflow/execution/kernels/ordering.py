"""Partition binding for bounded external ordering (lane K).

K009/K010 implement run-sort + finite fan-in external merge over durable
artifacts. Comparison, shuffle and merge algorithms are owned by operators;
channel routing is owned by exchange.

Sort convention matches the public Arrow/Ray default: nulls last in both
directions, per-key descending, multi-key lexicographic. Equal business keys use
a deterministic record-provenance tie break which is *not* claimed as business
order (C06). Spill runs are immutable artifacts; only intermediates created here
are released on failure, never caller-owned input runs.
"""
from __future__ import annotations
from demiflow.execution.contracts.dataflow import stage_resident_bytes

import hashlib
import sys
from contextlib import closing, ExitStack
from dataclasses import replace
from collections.abc import Iterator, Mapping
from typing import Any

from demiflow.execution.contracts.dataflow import (
    ArtifactRef,
    Record,
    RuntimeServices,
    KernelSpec,
)

from demiflow.execution.contracts.record_codec import _typed_shape, _encode_payload, _decode_payload, _encode_record_document, _decode_record_document
from demiflow.execution.contracts.dataflow import thaw_record_value

from demiflow.execution.kernels.dataflow import RECORD_MEDIA_TYPE, _check_stop, _cfg, _err, _occurrence, _seed
from demiflow.execution.contracts.record_codec import input_row_bytes as _input_row_bytes
from demiflow.execution.storage.streams import materialize_row as _materialize_row
from demiflow.operators.ordering import shuffle_key_bytes as _shuffle_key_bytes

from demiflow.operators.ordering import MAX_SORT_MERGE_FAN_IN as _MERGE_FAN_IN, SortWindow
from demiflow.operators.ordering import (
    retained_frame_bytes as _retained_frame_bytes,
    sort_value_token as _value_token,
    encode_sort_frame as _frame_bytes, MergeReadBudget as _MergeReadBudget,
    SortFrameReader as _FrameReader,
)


class OrderingServices:
    """Borrow existing service ports, including the finite-stop wrapper.

    Only bounds narrow; rebuilding RuntimeServices would copy its mappings and
    would discard the real worker's finite computation cancellation view.
    """
    __slots__ = ('_original', 'bounds')

    def __init__(self, original, resident_bytes):
        self._original = original
        self.bounds = replace(original.bounds, max_resident_bytes=resident_bytes)

    def __getattr__(self, name):
        return getattr(self._original, name)


def _provenance(record: Record) -> tuple[bytes, int, str]:
    # Source offsets are numbers, not JSON text (2 must precede 10). The same
    # closed typed token order handles nested expansion/source coordinates.
    position = thaw_record_value(record.source_position)
    # Callback task boundaries are execution metadata, not source order.
    # A filtered/expanded task must not move rows relative to source offsets.
    position.pop('dataflow_task', None)
    return (_value_token(position), record.sequence, record.record_id)


def _record_sort_key(
    record: Record, row: Mapping[str, Any], spec: KernelSpec, services: RuntimeServices, *, ordinal=None
) -> tuple:
    kind = spec.kind
    if kind == 'RepartitionOp':
        from demiflow.operators.ordering import repartition_sort_key
        return repartition_sort_key(row, spec.configuration)
    if kind == 'SortOp':
        from demiflow.operators.ordering import field_sort_key
        keys = tuple(_cfg(spec, 'keys'))
        return field_sort_key(row, keys, spec.configuration.get('descending', False))
    seed = _seed(spec, services)
    occurrence = _occurrence(spec)
    if kind == "RandomShuffleOp":
        return ((_shuffle_key_bytes(record.record_id, seed=seed, occurrence_id=occurrence),),)
    if kind == "RandomizeBlockOrderOp":
        from demiflow.execution.kernels.block_partition import block_identity
        block_id = block_identity(record)
        block_key = _shuffle_key_bytes(block_id, seed=seed, occurrence_id=occurrence)
        return ((block_key, record.sequence if ordinal is None else ordinal),)
    raise _err("INVALID_CONTRACT", "kind is not an ordering kernel", kind=kind)


def _encode_frame(key: tuple, prov: tuple, record: Record, *, max_bytes: int, frame_limit=None) -> bytes:
    raw = _encode_record_document(record, max_bytes=max_bytes)
    encoded = _encode_payload({"record_id": record.record_id, "sequence": record.sequence,
                               "document": raw}, max_bytes=max_bytes * 4 + 1024)
    return _frame_bytes(key, prov, encoded, frame_limit=frame_limit, encode_payload=_encode_payload, typed_shape=_typed_shape)


def _run_frames(ref: ArtifactRef, services: RuntimeServices, window=None, *, budget=None, slot=0) -> Iterator[tuple]:
    kwargs = {} if window is None else {'chunk_bytes': window.read_chunk_bytes}
    chunks = services.store.read(ref, max_bytes=ref.size_bytes, **kwargs)
    reader = _FrameReader(chunks, max_frame_bytes=(services.bounds.max_resident_bytes
                          if window is None else window.frame_bytes), budget=budget, slot=slot,
                          chunk_bytes=0 if window is None else window.read_chunk_bytes, decode_payload=_decode_payload)
    try:
        while True:
            frame = reader.next_frame()
            if frame is None:
                return
            yield frame
    finally:
        close = getattr(chunks, 'close', None)
        if close is not None:
            close()
        if budget is not None:
            budget.close(slot)


def _write_run(
    frames: list[tuple], services: RuntimeServices
) -> ArtifactRef:
    return services.store.put(
        (frame[2] for frame in frames), max_bytes=sum(len(frame[2]) for frame in frames),
        media_type=RECORD_MEDIA_TYPE
    )


def _release_runs(refs, services):
    """Attempt every owned unpin even when one cleanup reply fails."""
    active = sys.exc_info()[1]
    failure = None
    for ref in refs:
        try:
            services.store.release(ref, owner='kernel-sort')
        except BaseException as exc:
            failure = exc if failure is None else failure
    if failure is not None:
        if active is not None:
            active.add_note('Sort run cleanup also failed: ' + str(failure))
        else:
            raise failure


def sort_windows(services):
    from demiflow.execution.contracts.record_codec import resident_limit
    from demiflow.operators.ordering import sort_windows as windows
    grant = resident_limit(services.bounds, stage_resident_bytes(getattr(services, 'assignment', None)))
    return windows(services.bounds, grant)


def sort_with_body_references(records, *, spec, services, rows, window, retain, metadata_bytes):
    """Only this sort owns its body extent. Recovery reopens the closed input port."""
    from demiflow.execution.contracts.record_codec import _json_shape, _typed_shape, admit_workspace, decode_document, encode_document, raw_workspace, resident_limit
    from demiflow.execution.storage.streams import read_document
    from demiflow.execution.contracts.record_codec import is_arrow_record
    from demiflow.execution.storage.objects import RunArtifactStore

    from demiflow.operators.ordering import wide_sort_body_reference, measured_merge_window

    bounds, store = services.bounds, services.store
    grant = resident_limit(bounds, stage_resident_bytes(getattr(services, 'assignment', None)))
    codec = OrderingServices(services, window.codec_bytes)
    room = not isinstance(store, RunArtifactStore) or store.available_file_slots(maximum=bounds.max_open_files) >= (
        store.SCRATCH_FILES + store.PUT_FILES + 2 * store.READ_FILES)
    extent = None
    largest = 0

    def keys(record):
        row = record.inline
        retained = 0
        if row is None:
            if is_arrow_record(record):
                row = _materialize_row(record, store=codec.store, bounds=codec.bounds, assignment=getattr(codec, 'assignment', None))
            else:
                raw = read_document(record.payload_ref, store, codec.bounds, resident_bytes=stage_resident_bytes(getattr(codec, 'assignment', None)),
                                    _maximum=bounds.max_row_bytes)
                admit_workspace(raw_workspace(raw), codec.bounds, resident_bytes=stage_resident_bytes(getattr(codec, 'assignment', None)))
                row = _decode_payload(raw, max_bytes=bounds.max_row_bytes)
                del raw
            retained = _typed_shape(row, bounds.max_row_bytes)[1]
        # Thaw only the key fields. Lists, tuples and typed nested values retain
        # the existing comparator semantics; a large non-key column stays frozen.
        key_row = {}
        position_bytes, position_native = _json_shape(record.source_position, bounds.max_control_bytes)
        memory = retained + 3 * position_bytes + 2 * position_native
        for name in spec.configuration['keys']:
            if name not in row:
                raise _err('INVALID_CONTRACT', 'sort key field missing', field=name)
            size, native, tree = _typed_shape(row[name], bounds.max_row_bytes)
            memory += 3 * size + 2 * native + tree
            admit_workspace(memory, codec.bounds, resident_bytes=stage_resident_bytes(getattr(codec, 'assignment', None)))
            key_row[name] = thaw_record_value(row[name])
        return _record_sort_key(record, key_row, spec, codec), _provenance(record)

    with ExitStack() as scope:
        close = getattr(records, 'close', None)
        if close is not None:
            scope.callback(close)

        def frame(key, provenance, record, published):
            nonlocal extent, largest
            retained = _retained_frame_bytes(key, provenance, b'')
            raw = encode_document(record, codec.bounds, resident_bytes=stage_resident_bytes(getattr(codec, 'assignment', None)), retained=retained)
            saved = {'record_id': record.record_id, 'sequence': record.sequence}
            if (room and record.inline is not None
                    and wide_sort_body_reference(record.encoded_bytes, rows, grant)):
                size = extent.size if extent is not None else 0
                if size + len(raw) + published > bounds.max_spill_bytes:
                    raise _err('BUDGET_EXCEEDED', 'wide sort bodies and runs exceed max_spill_bytes')
                if extent is None:
                    extent = scope.enter_context(store.scratch(max_bytes=bounds.max_spill_bytes,
                        max_io_bytes=codec.bounds.max_resident_bytes))
                saved.update(offset=extent.append(raw), length=len(raw), sha256=hashlib.sha256(raw).digest())
            else:
                saved['document'] = raw
            del raw
            size, native, tree = _typed_shape(saved, window.frame_bytes)
            admit_workspace(native + tree + 2 * size, codec.bounds, resident_bytes=stage_resident_bytes(getattr(codec, 'assignment', None)), retained=retained)
            encoded = _encode_payload(saved, max_bytes=window.frame_bytes)
            packed = _frame_bytes(key, provenance, encoded, frame_limit=window.frame_bytes,
                admit=lambda size: admit_workspace(size, codec.bounds, resident_bytes=stage_resident_bytes(getattr(codec, 'assignment', None)),
                                                   retained=retained + native + len(encoded)), encode_payload=_encode_payload, typed_shape=_typed_shape)
            largest = max(largest, _retained_frame_bytes(key, provenance, packed))
            return packed

        def restore(saved):
            if 'document' in saved:
                raw = saved['document']
            else:
                if (extent is None or set(saved) != {'record_id', 'sequence', 'offset', 'length', 'sha256'}
                        or type(saved['offset']) is not int or saved['offset'] < 0
                        or type(saved['length']) is not int or not 0 < saved['length'] <= codec.bounds.max_resident_bytes
                        or type(saved['sha256']) is not bytes or len(saved['sha256']) != 32):
                    raise _err('CORRUPT_ARTIFACT', 'sort reference has no matching body extent')
                raw = extent.read_at(saved['offset'], saved['length'])
                if hashlib.sha256(raw).digest() != saved['sha256']:
                    raise _err('CORRUPT_ARTIFACT', 'sort body differs from its authenticated reference')
            return decode_document(raw, record_id=saved['record_id'], sequence=saved['sequence'],
                                   bounds=codec.bounds, resident_bytes=stage_resident_bytes(getattr(codec, 'assignment', None)))

        def body_bytes():
            return 0 if extent is None else extent.size

        runs = build_sort_runs(records, spec=spec, services=services, window=window,
            sort_keys=keys, frame_for_record=frame, body_bytes=body_bytes)
        scope.callback(_release_runs, runs, services)
        if close is not None:
            close()
        # The measured largest retained head and read buffer limit the number
        # of simultaneous readers. _MergeReadBudget admits every decode too.
        window = measured_merge_window(window, largest)
        retain(window.merge_reader_bytes + metadata_bytes)
        scope.callback(retain, 0)
        yield from merge_sort_runs(runs, spec=spec, services=services, window=window,
                                   restore_record=restore, body_bytes=body_bytes())


def build_sort_runs(records, *, spec, services, window=None, key_for_record=None,
                    sort_keys=None, frame_for_record=None, body_bytes=None):
    """Bind partition values/codecs/storage to the original run-sort algorithm."""
    bounds = services.bounds
    def prepare_frame(record, ordinal, published):
        if sort_keys is not None:
            key, prov = sort_keys(record)
        elif key_for_record is None:
            row = _materialize_row(record, store=services.store, bounds=services.bounds, assignment=getattr(services, 'assignment', None))
            key = _record_sort_key(record, row, spec, services, ordinal=ordinal)
            del row
            prov = _provenance(record)
        else:
            key = key_for_record(record)
            # New shuffle keys already include their complete typed tie
            # break. Record identity is carried unchanged inside the frame.
            prov = ()
        saved_record = record
        if window is not None and window.key_only:
            # Only the region's key/offset carrier opts in. Offset,
            # provenance and record identity survive; the key is already
            # encoded in the comparator frame and need not be stored twice.
            saved_record = replace(record, payload_ref=None, inline={},
                encoded_bytes=len(_encode_payload({}, max_bytes=bounds.max_row_bytes)), schema_ref=None)
        if frame_for_record is None:
            encoded = _encode_frame(key, prov, saved_record, max_bytes=bounds.max_row_bytes,
                                    frame_limit=None if window is None else window.frame_bytes)
        else:
            encoded = frame_for_record(key, prov, saved_record, published)
        return key,prov,encoded,saved_record

    from demiflow.operators.ordering import build_sort_runs as sort_runs
    return sort_runs(records,bounds=bounds,window=window,input_size=_input_row_bytes,
        prepare_frame=prepare_frame,check_stop=services.check_stop,
        write_run=lambda frames:_write_run(frames,services),
        release_runs=lambda refs:_release_runs(refs,services),body_bytes=body_bytes)


def _merge_into(
    sources: tuple[ArtifactRef, ...], services: RuntimeServices, window=None, *, budget=None,
) -> Iterator[tuple]:
    if budget is None and window is not None and window.merge_reader_bytes is not None:
        budget = _MergeReadBudget(window, len(sources))
    readers = [_run_frames(ref, services, window, budget=budget, slot=index)
               for index, ref in enumerate(sources)]
    from demiflow.operators.ordering import merge_sorted_frames
    yield from merge_sorted_frames(readers,check_stop=services.check_stop,budget=budget)


def merge_sort_runs(
    runs: tuple[ArtifactRef, ...], *, spec: KernelSpec, services: RuntimeServices,
    window: SortWindow | None = None, restore_record=None, body_bytes=0,
) -> Iterator[Record]:
    """K010: finite fan-in multi-pass merge; releases only owned intermediates."""
    if window is not None:
        window.validate(services.bounds)
    if not runs:
        return
    from demiflow.execution.storage.objects import RunArtifactStore
    if isinstance(services.store, RunArtifactStore):
        store = services.store
        available = store.available_file_slots(maximum=services.bounds.max_open_files)
        # Readers remain open across yield. The immediate downstream consumer
        # may publish through this same store before requesting another row, so
        # the final merge needs the same output window as intermediate spills.
        final_fan_in = fan_in = min(_MERGE_FAN_IN,max(0,(available - store.PUT_FILES) // store.READ_FILES))
    else:
        # In-memory contract stores have no physical files or lease overhead.
        final_fan_in = fan_in = min(_MERGE_FAN_IN,services.bounds.max_open_files)
    if window is not None:
        final_fan_in = fan_in = min(fan_in, window.merge_fan_in)
    def merge_frames(group, window):
        budget = (_MergeReadBudget(window, len(group))
                  if window is not None and window.merge_reader_bytes is not None else None)
        with closing(_merge_into(group, services, window, budget=budget)) as source_frames:
            for key, prov, rec_bytes in source_frames:
                yield _frame_bytes(key, prov, rec_bytes,
                    frame_limit=None if window is None else window.frame_bytes,
                    admit=None if budget is None else budget.external,
                    encode_payload=_encode_payload, typed_shape=_typed_shape)

    from demiflow.operators.ordering import merge_sort_runs as merge_runs
    owned: list[ArtifactRef] = []
    frames = merge_runs(runs, bounds=services.bounds, fan_in=fan_in,
        final_fan_in=final_fan_in, window=window, body_bytes=body_bytes,
        check_stop=services.check_stop, merge_frames=merge_frames,
        put_frames=lambda frames, remaining: services.store.put(frames,
            max_bytes=remaining, media_type=RECORD_MEDIA_TYPE),
        release_run=lambda ref: services.store.release(ref, owner='kernel-sort'),
        owned=owned, ref_size=lambda ref: ref.size_bytes,
        read_frames=lambda refs, window: _merge_into(refs, services, window))
    try:
        with closing(frames) as source_frames:
            for key, prov, rec_bytes in source_frames:
                _check_stop(services)
                if window is not None and window.codec_bytes is not None:
                    from demiflow.execution.contracts.record_codec import raw_workspace
                    codec_window = window.codec_bytes if window.merge_codec_bytes is None else window.merge_codec_bytes
                    if raw_workspace(rec_bytes) > codec_window:
                        raise _err('BUDGET_EXCEEDED', 'sort record decode exceeds its codec subwindow')
                saved = _decode_payload(rec_bytes, max_bytes=(services.bounds.max_resident_bytes
                                       if window is None else window.frame_bytes))
                if restore_record is not None:
                    record = restore_record(saved)
                    del saved
                    yield record
                    del record
                    continue
                if window is not None and window.codec_bytes is not None:
                    if raw_workspace(saved['document']) + 2 * len(rec_bytes) > codec_window:
                        raise _err('BUDGET_EXCEEDED', 'sort document decode exceeds its codec subwindow')
                document = saved["document"]
                yield _decode_record_document(document, record_id=saved["record_id"],
                    sequence=saved["sequence"], max_bytes=services.bounds.max_row_bytes)
    finally:
        _release_runs(owned, services)
