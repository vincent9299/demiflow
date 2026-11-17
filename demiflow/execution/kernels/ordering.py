"""Bounded external ordering and stable routing (lane K).

K009/K010 implement run-sort + finite fan-in external merge over durable
artifacts; K011 is the versioned global shuffle key; K012 routes records through
one stable hash/rebalance/target/broadcast path with a resumable ordinal.

Sort convention matches the public Arrow/Ray default: nulls last in both
directions, per-key descending, multi-key lexicographic. Equal business keys use
a deterministic record-provenance tie break which is *not* claimed as business
order (C06). Spill runs are immutable artifacts; only intermediates created here
are released on failure, never caller-owned input runs.
"""
from __future__ import annotations

import hashlib
import heapq
import struct
import sys
from contextlib import closing
from dataclasses import dataclass, replace
from collections.abc import Iterator, Mapping
from typing import Any

from demiflow.execution.contracts.dataflow import (
    ArtifactRef,
    ExchangeSpec,
    Record,
    RoutedRecord,
    RuntimeServices,
    KernelSpec,
)
from demiflow.execution.contracts.dataflow_errors import DataflowError

from ..exchange.dataflow_bridge import (
    _encode_payload, _decode_payload, _encode_record_document, _decode_record_document,
)
from ..contracts.dataflow import thaw_record_value

from .dataflow import (
    RECORD_MEDIA_TYPE,
    _canonical,
    _check_stop,
    _cfg,
    _err,
    _hash_id,
    _input_row_bytes,
    _materialize_row,
    _occurrence,
    _seed,
    _shuffle_key_bytes,
)

_MERGE_FAN_IN = 32
_FRAME_OVERHEAD = 256


@dataclass(frozen=True)
class SortWindow:
    """Private subwindow of an already admitted worker, never a new grant.

    Each merge reader holds a frame, its decoded comparison key and a buffer;
    two more frame windows cover heap/output/codec temporaries. Build reserves
    six row windows for the current key/record/codec in addition to its run.
    key_only is for internal key/offset carriers, never public business rows.
    """
    resident_bytes: int
    run_bytes: int
    frame_bytes: int
    read_chunk_bytes: int
    merge_fan_in: int = 2
    key_only: bool = False
    merge_resident_bytes: int | None = None
    # New private callers may admit the actual typed codec tree instead of
    # reserving six maximum rows. Legacy windows retain the original rule.
    codec_bytes: int | None = None
    merge_codec_bytes: int | None = None
    merge_frame_multiplier: int = 3
    max_runs: int | None = None

    def validate(self, bounds):
        values = (self.resident_bytes, self.run_bytes, self.frame_bytes,
                  self.read_chunk_bytes, self.merge_fan_in)
        if any(type(value) is not int or value < 1 for value in values) or type(self.key_only) is not bool:
            raise _err('INVALID_CONTRACT', 'sort subwindow values must be positive integers')
        merge_window = self.resident_bytes if self.merge_resident_bytes is None else self.merge_resident_bytes
        codec = 6 * bounds.max_row_bytes if self.codec_bytes is None else self.codec_bytes
        merge_codec = codec if self.merge_codec_bytes is None else self.merge_codec_bytes
        if type(merge_window) is not int or not 1 <= merge_window <= self.resident_bytes:
            raise _err('INVALID_CONTRACT', 'invalid sort merge subwindow')
        if (type(codec) is not int or codec < 1
                or type(merge_codec) is not int or not 1 <= merge_codec <= codec
                or type(self.merge_frame_multiplier) is not int or self.merge_frame_multiplier < 3
                or self.max_runs is not None and (type(self.max_runs) is not int or self.max_runs < 1)):
            raise _err('INVALID_CONTRACT', 'invalid sort codec/metadata subwindow')
        if (self.resident_bytes > bounds.max_resident_bytes
                or self.frame_bytes > self.run_bytes
                or self.read_chunk_bytes > min(1 << 20, bounds.max_row_bytes)
                or self.merge_fan_in > min(_MERGE_FAN_IN, bounds.max_open_files)
                or self.run_bytes + codec > self.resident_bytes
                or self.merge_fan_in * (self.merge_frame_multiplier * self.frame_bytes + self.read_chunk_bytes)
                   + 2 * self.frame_bytes + (merge_codec if self.codec_bytes is not None else 0) > merge_window):
            raise _err('RESOURCE_INFEASIBLE', 'sort subwindow exceeds its admitted live-object window')


def _retained_frame_bytes(key, provenance, encoded):
    # Encoded bytes and decoded comparison tokens coexist in the retained run.
    # Count the actual closed builtin tree (including container allocation),
    # without invoking user methods or pretending encoding is its only copy.
    def size(value):
        if type(value) is tuple:
            return sys.getsizeof(value) + sum(size(item) for item in value)
        if type(value) in (bytes, str, int):
            return sys.getsizeof(value)
        raise _err('INTERNAL_PROTOCOL', 'unexpected sort comparison token')
    return size(key) + size(provenance) + sys.getsizeof(encoded) + _FRAME_OVERHEAD


def shuffle_key(record_id: str, *, seed: int, occurrence_id: str) -> bytes:
    """K011: host/attempt/partition-count independent global random sort key."""
    if not isinstance(record_id, str) or not record_id:
        raise _err("INVALID_CONTRACT", "record_id must be a non-empty string")
    if isinstance(seed, bool) or not isinstance(seed, int):
        raise _err("INVALID_CONTRACT", "seed must be an integer")
    return _shuffle_key_bytes(record_id, seed=seed, occurrence_id=occurrence_id)


def _int_token(value: int) -> bytes:
    negative = value < 0
    magnitude = abs(value)
    mag = magnitude.to_bytes((magnitude.bit_length() + 7) // 8 or 1, "big")
    length = struct.pack(">Q", len(mag))
    if negative:
        return b"\x00" + bytes(b ^ 0xFF for b in length + mag)
    return b"\x01" + length + mag


def _float_token(value: float) -> bytes:
    raw = struct.pack(">d", value)
    if raw[0] & 0x80:
        return bytes(b ^ 0xFF for b in raw)
    return bytes([raw[0] ^ 0x80]) + raw[1:]


def _value_token(value: Any) -> bytes:
    if value is None:
        return b"\x00"
    if isinstance(value, bool):
        return b"\x01" + (b"\x01" if value else b"\x00")
    if isinstance(value, int):
        return b"\x02" + _int_token(value)
    if isinstance(value, float):
        return b"\x03" + _float_token(value)
    if isinstance(value, str):
        return b"\x04" + value.encode("utf-8")
    if isinstance(value, bytes):
        return b"\x05" + value
    if isinstance(value, (tuple, list)):
        tag = b"\x06" if isinstance(value, tuple) else b"\x07"
        parts = []
        for item in value:
            token = _value_token(item)
            parts.append(struct.pack(">Q", len(token)) + token)
        return tag + b"".join(parts)
    if isinstance(value, Mapping):
        items = sorted(value.items(), key=lambda kv: _value_token(kv[0]))
        parts = []
        for key, item in items:
            kt = _value_token(key)
            vt = _value_token(item)
            parts.append(struct.pack(">Q", len(kt)) + kt + struct.pack(">Q", len(vt)) + vt)
        return b"\x08" + b"".join(parts)
    raise _err("INVALID_CONTRACT", "unsupported sort value type", type=type(value).__name__)


def _field_token(value: Any, descending: bool) -> tuple[int, bytes]:
    if value is None:
        return (1, b"")
    token = _value_token(value)
    if descending:
        token = bytes(b ^ 0xFF for b in token)
    return (0, token)


def _provenance(record: Record) -> tuple[bytes, int, str]:
    # Source offsets are numbers, not JSON text (2 must precede 10). The same
    # closed typed token order handles nested expansion/source coordinates.
    position = thaw_record_value(record.source_position)
    # Callback task boundaries are execution metadata, not source order.
    # A filtered/expanded task must not move rows relative to source offsets.
    position.pop('dataflow_task', None)
    return (_value_token(position), record.sequence, record.record_id)


def _descending_flags(spec: KernelSpec, count: int) -> tuple[bool, ...]:
    value = spec.configuration.get("descending", False)
    if isinstance(value, bool):
        return (value,) * count
    flags = tuple(value)
    if len(flags) != count or any(not isinstance(flag, bool) for flag in flags):
        raise _err("INVALID_CONTRACT", "descending must be a bool or one bool per key")
    return flags


def _record_sort_key(
    record: Record, row: Mapping[str, Any], spec: KernelSpec, services: RuntimeServices, *, ordinal=None
) -> tuple:
    kind = spec.kind
    if kind == 'RepartitionOp':
        from .dataflow import _group_key
        keys = tuple(spec.configuration.get('keys') or ())
        bucket = int.from_bytes(hashlib.sha256(_group_key(row, keys).encode()).digest()[:8], 'big') % spec.configuration['num_blocks']
        fields = tuple(_field_token(row[key], False) for key in keys) if spec.configuration.get('sort') else ()
        return ((0, bucket), *fields)
    if kind == "SortOp":
        keys = tuple(_cfg(spec, "keys"))
        flags = _descending_flags(spec, len(keys))
        try:
            fields = tuple(_field_token(row[key], desc) for key, desc in zip(keys, flags))
        except KeyError as exc:
            raise _err("INVALID_CONTRACT", "sort key field missing", field=str(exc)) from exc
        return fields
    seed = _seed(spec, services)
    occurrence = _occurrence(spec)
    if kind == "RandomShuffleOp":
        return ((_shuffle_key_bytes(record.record_id, seed=seed, occurrence_id=occurrence),),)
    if kind == "RandomizeBlockOrderOp":
        from .block_partition import block_identity
        block_id = block_identity(record)
        block_key = _shuffle_key_bytes(block_id, seed=seed, occurrence_id=occurrence)
        return ((block_key, record.sequence if ordinal is None else ordinal),)
    raise _err("INVALID_CONTRACT", "kind is not an ordering kernel", kind=kind)


def _frame_bytes(key: tuple, prov: tuple, rec_bytes: bytes, *, frame_limit=None) -> bytes:
    limit = len(rec_bytes) * 4 + 1024 if frame_limit is None else frame_limit
    key_bytes = _encode_payload({"value": key}, max_bytes=limit)
    prov_bytes = _encode_payload({"value": prov}, max_bytes=limit)
    if frame_limit is not None and len(key_bytes)+len(prov_bytes)+len(rec_bytes)+12+_FRAME_OVERHEAD > frame_limit:
        raise _err('BUDGET_EXCEEDED', 'single sort frame exceeds the resident budget')
    return (
        struct.pack(">I", len(key_bytes)) + key_bytes
        + struct.pack(">I", len(prov_bytes)) + prov_bytes
        + struct.pack(">I", len(rec_bytes)) + rec_bytes
    )


def _encode_frame(key: tuple, prov: tuple, record: Record, *, max_bytes: int, frame_limit=None) -> bytes:
    raw = _encode_record_document(record, max_bytes=max_bytes)
    encoded = _encode_payload({"record_id": record.record_id, "sequence": record.sequence,
                               "document": raw}, max_bytes=max_bytes * 4 + 1024)
    return _frame_bytes(key, prov, encoded, frame_limit=frame_limit)


class _FrameReader:
    """Streaming length-prefixed frame reader over bounded store chunks."""

    def __init__(self, chunks: Iterator[bytes], *, max_frame_bytes: int) -> None:
        self._max_frame_bytes = max_frame_bytes
        self._chunks = iter(chunks)
        self._buffer = bytearray()
        self._eof = False

    def _fill(self, needed: int) -> None:
        while len(self._buffer) < needed and not self._eof:
            try:
                chunk = next(self._chunks)
            except StopIteration:
                self._eof = True
                break
            self._buffer.extend(chunk)

    def _read_exact(self, count: int) -> bytes:
        self._fill(count)
        if len(self._buffer) < count:
            raise _err("CORRUPT_ARTIFACT", "truncated sort run frame")
        data = bytes(self._buffer[:count])
        del self._buffer[:count]
        return data

    def next_frame(self) -> tuple | None:
        self._fill(4)
        if not self._buffer:
            return None
        (key_len,) = struct.unpack(">I", self._read_exact(4))
        if key_len > self._max_frame_bytes:
            raise _err("BUDGET_EXCEEDED", "sort key frame exceeds the resident budget")
        key = _decode_payload(self._read_exact(key_len), max_bytes=self._max_frame_bytes)["value"]
        (prov_len,) = struct.unpack(">I", self._read_exact(4))
        if key_len + prov_len + 12 > self._max_frame_bytes:
            raise _err("BUDGET_EXCEEDED", "sort provenance frame exceeds the resident budget")
        prov = _decode_payload(self._read_exact(prov_len), max_bytes=self._max_frame_bytes)["value"]
        (rec_len,) = struct.unpack(">I", self._read_exact(4))
        if key_len + prov_len + rec_len + 12 > self._max_frame_bytes:
            raise _err("BUDGET_EXCEEDED", "sort record frame exceeds the resident budget")
        rec_bytes = self._read_exact(rec_len)
        return (key, prov, rec_bytes)


def _run_frames(ref: ArtifactRef, services: RuntimeServices, window=None) -> Iterator[tuple]:
    kwargs = {} if window is None else {'chunk_bytes': window.read_chunk_bytes}
    chunks = services.store.read(ref, max_bytes=ref.size_bytes, **kwargs)
    reader = _FrameReader(chunks, max_frame_bytes=(services.bounds.max_resident_bytes
                          if window is None else window.frame_bytes))
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


def build_sort_runs(
    records: Iterator[Record], *, spec: KernelSpec, services: RuntimeServices,
    window: SortWindow | None = None,
) -> tuple[ArtifactRef, ...]:
    """K009: bounded stable runs, disk reserved before write, single-row rejection."""
    return _build_sort_runs(records, spec=spec, services=services, window=window)


def _build_sort_runs(records, *, spec, services, window=None, key_for_record=None):
    """Private key hook; public sort/shuffle signatures and legacy keys stay fixed."""
    bounds = services.bounds
    if window is not None:
        window.validate(bounds)
    run_budget = bounds.max_resident_bytes if window is None else window.run_bytes
    if run_budget < 1:
        raise _err("RESOURCE_INFEASIBLE", "max_resident_bytes must be positive")
    refs: list[ArtifactRef] = []
    frames: list[tuple] = []
    used = 0
    spill_total = 0

    def flush() -> None:
        nonlocal frames, used, spill_total
        if not frames:
            return
        if window is not None and window.max_runs is not None and len(refs) >= window.max_runs:
            raise _err('BUDGET_EXCEEDED', 'sort run references exceed the metadata window')
        frames.sort(key=lambda item: (item[0], item[1]))
        total = used if window is None else sum(len(frame[2]) for frame in frames)
        if spill_total + total > bounds.max_spill_bytes:
            raise _err("BUDGET_EXCEEDED", "sort spill exceeds max_spill_bytes", bytes=spill_total + total)
        refs.append(_write_run(frames, services))
        spill_total += total
        frames = []
        used = 0

    try:
        for ordinal, record in enumerate(records):
            _check_stop(services)
            size = _input_row_bytes(record)
            if size > bounds.max_row_bytes:
                raise _err("BUDGET_EXCEEDED", "single row exceeds max_row_bytes", size=size)
            if key_for_record is None:
                row = _materialize_row(record, services)
                key = _record_sort_key(record, row, spec, services, ordinal=ordinal)
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
            encoded = _encode_frame(key, prov, saved_record, max_bytes=bounds.max_row_bytes,
                                    frame_limit=None if window is None else window.frame_bytes)
            frame_bytes = len(encoded) + _FRAME_OVERHEAD
            if frame_bytes > (run_budget if window is None else window.frame_bytes):
                raise _err("BUDGET_EXCEEDED", "single sort frame exceeds the resident budget")
            resident = frame_bytes if window is None else _retained_frame_bytes(key, prov, encoded)
            if resident > run_budget:
                raise _err('BUDGET_EXCEEDED', 'single retained sort frame exceeds the resident budget')
            if frames and used + resident > run_budget:
                flush()
            frames.append((key, prov, encoded))
            used += resident
        flush()
    except BaseException:
        _release_runs(refs, services)
        raise
    return tuple(refs)


def _merge_into(
    sources: tuple[ArtifactRef, ...], services: RuntimeServices, window=None,
) -> Iterator[tuple]:
    readers = [_run_frames(ref, services, window) for ref in sources]
    try:
        for frame in heapq.merge(*readers, key=lambda item: (item[0], item[1])):
            _check_stop(services)
            yield frame
    finally:
        for reader in readers:
            reader.close()


def merge_sort_runs(
    runs: tuple[ArtifactRef, ...], *, spec: KernelSpec, services: RuntimeServices,
    window: SortWindow | None = None,
) -> Iterator[Record]:
    """K010: finite fan-in multi-pass merge; releases only owned intermediates."""
    if window is not None:
        window.validate(services.bounds)
    if not runs:
        return
    from ..dataflow_store import RunArtifactStore
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
    if final_fan_in < 1 or (len(runs)>1 and final_fan_in<2) or (len(runs)>final_fan_in and fan_in<2):
        raise _err("BUDGET_EXCEEDED", "sort merge requires at least two input file slots plus owned leases/output")
    max_spill = services.bounds.max_spill_bytes
    owned: list[ArtifactRef] = []
    current = tuple(runs)
    spill_total = 0
    try:
        while len(current) > final_fan_in:
            merged: list[ArtifactRef] = []
            for start in range(0, len(current), fan_in):
                _check_stop(services)
                group = current[start:start + fan_in]
                remaining = max_spill - spill_total
                if remaining < 1:
                    raise _err("BUDGET_EXCEEDED", "merge spill exceeds max_spill_bytes")

                def frames() -> Iterator[bytes]:
                    with closing(_merge_into(group, services, window)) as source_frames:
                        for key, prov, rec_bytes in source_frames:
                            yield _frame_bytes(key, prov, rec_bytes,
                                               frame_limit=None if window is None else window.frame_bytes)

                with closing(frames()) as output_frames:
                    ref = services.store.put(output_frames, max_bytes=remaining, media_type=RECORD_MEDIA_TYPE)
                owned.append(ref)
                merged.append(ref)
                spill_total += ref.size_bytes
                for source in group:
                    if source in owned:
                        services.store.release(source, owner="kernel-sort")
                        owned.remove(source)
            current = tuple(merged)
        with closing(_merge_into(current, services, window)) as source_frames:
            for key, prov, rec_bytes in source_frames:
                _check_stop(services)
                if window is not None and window.codec_bytes is not None:
                    from ..dataflow_record_frames import raw_workspace
                    codec_window = window.codec_bytes if window.merge_codec_bytes is None else window.merge_codec_bytes
                    if raw_workspace(rec_bytes) > codec_window:
                        raise _err('BUDGET_EXCEEDED', 'sort record decode exceeds its codec subwindow')
                saved = _decode_payload(rec_bytes, max_bytes=(services.bounds.max_resident_bytes
                                       if window is None else window.frame_bytes))
                if window is not None and window.codec_bytes is not None:
                    if raw_workspace(saved['document']) + 2 * len(rec_bytes) > codec_window:
                        raise _err('BUDGET_EXCEEDED', 'sort document decode exceeds its codec subwindow')
                yield _decode_record_document(saved["document"], record_id=saved["record_id"],
                    sequence=saved["sequence"], max_bytes=services.bounds.max_row_bytes)
    finally:
        _release_runs(owned, services)


class _RepartitionIterator:
    """Iterator[RoutedRecord] with a resumable rebalance ordinal (C05)."""

    def __init__(
        self,
        records: Iterator[Record],
        exchange: ExchangeSpec,
        services: RuntimeServices,
    ) -> None:
        self._records = iter(records)
        self._exchange = exchange
        self._services = services
        self._ordinal = 0
        self._consumers = tuple(exchange.consumer_partitions)
        if not self._consumers:
            raise _err("INVALID_CONTRACT", "exchange has no consumer partitions")
        self._routing = exchange.routing
        if self._routing not in ("forward", "hash", "rebalance", "broadcast", "target"):
            raise _err("INVALID_CONTRACT", "unknown routing", routing=self._routing)
        if self._routing == "hash" and not exchange.keys:
            raise _err("INVALID_CONTRACT", "hash routing requires keys")
        if self._routing == "target" and not exchange.keys:
            raise _err("INVALID_CONTRACT", "target routing requires a target key")

    @property
    def ordinal(self) -> int:
        return self._ordinal

    def restore_ordinal(self, value: int) -> None:
        if not isinstance(value, int) or value < 0:
            raise _err("INVALID_CONTRACT", "ordinal must be a non-negative integer")
        self._ordinal = value

    def __iter__(self) -> "_RepartitionIterator":
        return self

    def __next__(self) -> RoutedRecord:
        record = next(self._records)
        _check_stop(self._services)
        if _input_row_bytes(record) > self._services.bounds.max_row_bytes:
            raise _err("BUDGET_EXCEEDED", "single row exceeds max_row_bytes")
        edge = self._exchange.edge_id
        reader = self._target(record)
        self._ordinal += 1
        delivery = _hash_id(["dataflow-delivery-v3", record.record_id, edge, reader])
        return RoutedRecord(reader_id=reader, delivery_id=delivery, record=record)

    def _target(self, record: Record) -> str:
        consumers = self._consumers
        count = len(consumers)
        if self._routing == "forward":
            return consumers[0]
        if self._routing == "rebalance":
            return consumers[self._ordinal % count]
        if self._routing == "hash":
            row = _materialize_row(record, self._services)
            from .dataflow import _group_key

            key = _group_key(row, tuple(self._exchange.keys))
            digest = hashlib.sha256(key.encode("utf-8")).digest()
            return consumers[int.from_bytes(digest[:8], "big") % count]
        if self._routing == "target":
            row = _materialize_row(record, self._services)
            field = self._exchange.keys[0]
            if field not in row:
                raise _err("INVALID_CONTRACT", "target field missing", field=field)
            target = row[field]
            if target not in consumers:
                raise _err("INVALID_CONTRACT", "target is not a consumer partition", target=str(target))
            return target
        raise _err("INTERNAL_PROTOCOL", "unhandled routing")

    def close(self) -> None:
        close = getattr(self._records, "close", None)
        if close is not None:
            close()


def repartition_records(
    records: Iterator[Record], *, exchange: ExchangeSpec, services: RuntimeServices
) -> Iterator[RoutedRecord]:
    """K012: stable hash/rebalance/target/forward routing; broadcast fans out."""
    if exchange.routing != "broadcast":
        return _RepartitionIterator(records, exchange, services)

    consumers = tuple(exchange.consumer_partitions)
    if not consumers:
        raise _err("INVALID_CONTRACT", "exchange has no consumer partitions")

    def broadcast() -> Iterator[RoutedRecord]:
        source = iter(records)
        try:
            for record in source:
                _check_stop(services)
                if _input_row_bytes(record) > services.bounds.max_row_bytes:
                    raise _err("BUDGET_EXCEEDED", "single row exceeds max_row_bytes")
                for reader in consumers:
                    delivery = _hash_id(
                        ["dataflow-delivery-v3", record.record_id, exchange.edge_id, reader]
                    )
                    yield RoutedRecord(reader_id=reader, delivery_id=delivery, record=record)
        finally:
            close = getattr(source, "close", None)
            if close is not None:
                close()

    return broadcast()
