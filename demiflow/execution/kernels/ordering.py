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
    record: Record, row: Mapping[str, Any], spec: KernelSpec, services: RuntimeServices
) -> tuple:
    kind = spec.kind
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
        block = record.source_position.get("split_id") or record.source_position.get("partition")
        block_id = _canonical(block) if block is not None else record.record_id
        block_key = _shuffle_key_bytes(block_id, seed=seed, occurrence_id=occurrence)
        return ((block_key, record.sequence),)
    raise _err("INVALID_CONTRACT", "kind is not an ordering kernel", kind=kind)


def _frame_bytes(key: tuple, prov: tuple, rec_bytes: bytes) -> bytes:
    key_bytes = _encode_payload({"value": key}, max_bytes=len(rec_bytes) * 4 + 1024)
    prov_bytes = _encode_payload({"value": prov}, max_bytes=len(rec_bytes) * 4 + 1024)
    return (
        struct.pack(">I", len(key_bytes)) + key_bytes
        + struct.pack(">I", len(prov_bytes)) + prov_bytes
        + struct.pack(">I", len(rec_bytes)) + rec_bytes
    )


def _encode_frame(key: tuple, prov: tuple, record: Record, *, max_bytes: int) -> bytes:
    raw = _encode_record_document(record, max_bytes=max_bytes)
    encoded = _encode_payload({"record_id": record.record_id, "sequence": record.sequence,
                               "document": raw}, max_bytes=max_bytes * 4 + 1024)
    return _frame_bytes(key, prov, encoded)


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


def _run_frames(ref: ArtifactRef, services: RuntimeServices) -> Iterator[tuple]:
    reader = _FrameReader(services.store.read(ref, max_bytes=ref.size_bytes),
                          max_frame_bytes=services.bounds.max_resident_bytes)
    while True:
        frame = reader.next_frame()
        if frame is None:
            return
        yield frame


def _write_run(
    frames: list[tuple], services: RuntimeServices
) -> ArtifactRef:
    return services.store.put(
        (frame[2] for frame in frames), max_bytes=sum(len(frame[2]) for frame in frames),
        media_type=RECORD_MEDIA_TYPE
    )


def build_sort_runs(
    records: Iterator[Record], *, spec: KernelSpec, services: RuntimeServices
) -> tuple[ArtifactRef, ...]:
    """K009: bounded stable runs, disk reserved before write, single-row rejection."""
    bounds = services.bounds
    run_budget = bounds.max_resident_bytes
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
        frames.sort(key=lambda item: (item[0], item[1]))
        total = used
        if spill_total + total > bounds.max_spill_bytes:
            raise _err("BUDGET_EXCEEDED", "sort spill exceeds max_spill_bytes", bytes=spill_total + total)
        refs.append(_write_run(frames, services))
        spill_total += total
        frames = []
        used = 0

    try:
        for record in records:
            _check_stop(services)
            size = _input_row_bytes(record)
            if size > bounds.max_row_bytes:
                raise _err("BUDGET_EXCEEDED", "single row exceeds max_row_bytes", size=size)
            row = _materialize_row(record, services)
            key = _record_sort_key(record, row, spec, services)
            prov = _provenance(record)
            encoded = _encode_frame(key, prov, record, max_bytes=bounds.max_row_bytes)
            frame_bytes = len(encoded) + _FRAME_OVERHEAD
            if frame_bytes > run_budget:
                raise _err("BUDGET_EXCEEDED", "single sort frame exceeds the resident budget")
            if frames and used + frame_bytes > run_budget:
                flush()
            frames.append((key, prov, encoded))
            used += frame_bytes
        flush()
    except BaseException:
        for ref in refs:
            services.store.release(ref, owner="kernel-sort")
        raise
    return tuple(refs)


def _merge_into(
    sources: tuple[ArtifactRef, ...], services: RuntimeServices
) -> Iterator[tuple]:
    readers = [_run_frames(ref, services) for ref in sources]
    try:
        for frame in heapq.merge(*readers, key=lambda item: (item[0], item[1])):
            yield frame
    finally:
        for reader in readers:
            reader.close()


def merge_sort_runs(
    runs: tuple[ArtifactRef, ...], *, spec: KernelSpec, services: RuntimeServices
) -> Iterator[Record]:
    """K010: finite fan-in multi-pass merge; releases only owned intermediates."""
    if not runs:
        return
    if len(runs) > 1 and services.bounds.max_open_files < 2:
        raise _err("BUDGET_EXCEEDED", "sort merge requires at least two input file slots")
    fan_in = min(_MERGE_FAN_IN, services.bounds.max_open_files)
    max_spill = services.bounds.max_spill_bytes
    owned: list[ArtifactRef] = []
    current = tuple(runs)
    spill_total = 0
    try:
        while len(current) > fan_in:
            merged: list[ArtifactRef] = []
            for start in range(0, len(current), fan_in):
                _check_stop(services)
                group = current[start:start + fan_in]
                remaining = max_spill - spill_total
                if remaining < 1:
                    raise _err("BUDGET_EXCEEDED", "merge spill exceeds max_spill_bytes")

                def frames() -> Iterator[bytes]:
                    for key, prov, rec_bytes in _merge_into(group, services):
                        yield _frame_bytes(key, prov, rec_bytes)

                ref = services.store.put(frames(), max_bytes=remaining, media_type=RECORD_MEDIA_TYPE)
                owned.append(ref)
                merged.append(ref)
                spill_total += ref.size_bytes
                for source in group:
                    if source in owned:
                        services.store.release(source, owner="kernel-sort")
                        owned.remove(source)
            current = tuple(merged)
        for key, prov, rec_bytes in _merge_into(current, services):
            _check_stop(services)
            saved = _decode_payload(rec_bytes, max_bytes=services.bounds.max_resident_bytes)
            yield _decode_record_document(saved["document"], record_id=saved["record_id"],
                sequence=saved["sequence"], max_bytes=services.bounds.max_row_bytes)
    finally:
        for ref in owned:
            services.store.release(ref, owner="kernel-sort")


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
