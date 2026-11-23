"""Ordering implementation."""
from __future__ import annotations

import hashlib
import heapq
from demiflow.execution.contracts.dataflow import ArtifactRef
import struct
import sys
from collections.abc import Iterator, Mapping
from typing import Any
from dataclasses import dataclass
from demiflow.execution.contracts.dataflow_errors import DataflowError
from demiflow.execution.contracts.record_codec import canonical_record_json as _canonical


SHUFFLE_DOMAIN = "demiflow-shuffle-v3"

def shuffle_key_bytes(record_id: str, *, seed: int, occurrence_id: str) -> bytes:
    # Single versioned formula; ordering.shuffle_key is the public wrapper.
    return hashlib.sha256(
        _canonical([SHUFFLE_DOMAIN, seed, occurrence_id, record_id]).encode("utf-8")
    ).digest()


MAX_SORT_MERGE_FAN_IN = 32


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
    merge_reader_bytes: int | None = None

    def validate(self, bounds):
        values = (self.resident_bytes, self.run_bytes, self.frame_bytes,
                  self.read_chunk_bytes, self.merge_fan_in)
        if any(type(value) is not int or value < 1 for value in values) or type(self.key_only) is not bool:
            raise DataflowError('INVALID_CONTRACT', 'sort subwindow values must be positive integers')
        merge_window = self.resident_bytes if self.merge_resident_bytes is None else self.merge_resident_bytes
        codec = 6 * bounds.max_row_bytes if self.codec_bytes is None else self.codec_bytes
        merge_codec = codec if self.merge_codec_bytes is None else self.merge_codec_bytes
        if type(merge_window) is not int or not 1 <= merge_window <= self.resident_bytes:
            raise DataflowError('INVALID_CONTRACT', 'invalid sort merge subwindow')
        if (type(codec) is not int or codec < 1
                or type(merge_codec) is not int or not 1 <= merge_codec <= codec
                or type(self.merge_frame_multiplier) is not int or self.merge_frame_multiplier < 3
                or self.max_runs is not None and (type(self.max_runs) is not int or self.max_runs < 1)):
            raise DataflowError('INVALID_CONTRACT', 'invalid sort codec/metadata subwindow')
        reader = (self.merge_fan_in * (self.merge_frame_multiplier * self.frame_bytes + self.read_chunk_bytes)
                  + 2 * self.frame_bytes if self.merge_reader_bytes is None else self.merge_reader_bytes)
        if type(reader) is not int or reader < self.frame_bytes:
            raise DataflowError('INVALID_CONTRACT', 'invalid measured merge reader subwindow')
        if (self.resident_bytes > bounds.max_resident_bytes
                or self.frame_bytes > self.run_bytes
                or self.read_chunk_bytes > min(1 << 20, bounds.max_row_bytes)
                or self.merge_fan_in > min(MAX_SORT_MERGE_FAN_IN, bounds.max_open_files)
                or self.run_bytes + codec > self.resident_bytes
                or reader + (merge_codec if self.codec_bytes is not None else 0) > merge_window):
            raise DataflowError('RESOURCE_INFEASIBLE', 'sort subwindow exceeds its admitted live-object window')


def reducer_sort_window(bounds, *, key_only):
    row = bounds.max_row_bytes
    chunk = min(65536, max(1, row // 64))
    return SortWindow(resident_bytes=8 * row, run_bytes=2 * row,
        frame_bytes=max(1,(7 * row - 2 * chunk)//8), read_chunk_bytes=chunk,
        merge_fan_in=2, key_only=key_only, merge_resident_bytes=7 * row)


SORT_FRAME_OVERHEAD = 256


def _ordering_error(code, message, **context):
    return DataflowError(code, message, context=context)


def retained_frame_bytes(key, provenance, encoded):
    # Encoded bytes and decoded comparison tokens coexist in the retained run.
    # Count the actual closed builtin tree (including container allocation),
    # without invoking user methods or pretending encoding is its only copy.
    def size(value):
        if type(value) is tuple:
            return sys.getsizeof(value) + sum(size(item) for item in value)
        if type(value) in (bytes, str, int):
            return sys.getsizeof(value)
        raise _ordering_error('INTERNAL_PROTOCOL', 'unexpected sort comparison token')
    return size(key) + size(provenance) + sys.getsizeof(encoded) + SORT_FRAME_OVERHEAD


def shuffle_key(record_id: str, *, seed: int, occurrence_id: str) -> bytes:
    """K011: host/attempt/partition-count independent global random sort key."""
    if not isinstance(record_id, str) or not record_id:
        raise _ordering_error("INVALID_CONTRACT", "record_id must be a non-empty string")
    if isinstance(seed, bool) or not isinstance(seed, int):
        raise _ordering_error("INVALID_CONTRACT", "seed must be an integer")
    return shuffle_key_bytes(record_id, seed=seed, occurrence_id=occurrence_id)


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


def sort_value_token(value: Any) -> bytes:
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
            token = sort_value_token(item)
            parts.append(struct.pack(">Q", len(token)) + token)
        return tag + b"".join(parts)
    if isinstance(value, Mapping):
        items = sorted(value.items(), key=lambda kv: sort_value_token(kv[0]))
        parts = []
        for key, item in items:
            kt = sort_value_token(key)
            vt = sort_value_token(item)
            parts.append(struct.pack(">Q", len(kt)) + kt + struct.pack(">Q", len(vt)) + vt)
        return b"\x08" + b"".join(parts)
    raise _ordering_error("INVALID_CONTRACT", "unsupported sort value type", type=type(value).__name__)


def sort_field_token(value: Any, descending: bool) -> tuple[int, bytes]:
    if value is None:
        return (1, b"")
    token = sort_value_token(value)
    if descending:
        token = bytes(b ^ 0xFF for b in token)
    return (0, token)


def encode_sort_frame(key: tuple, prov: tuple, rec_bytes: bytes, *, encode_payload, typed_shape, frame_limit=None, admit=None) -> bytes:
    limit = len(rec_bytes) * 4 + 1024 if frame_limit is None else frame_limit
    if admit is not None:
        ks, _, kt = typed_shape({'value': key}, limit)
        ps, _, pt = typed_shape({'value': prov}, limit)
        # Existing key/provenance/record bytes are retained by the merge.
        # Admit the tagged trees, JSON/string bytes and final joined frame
        # before encoding. These three phases do not coexist with each other.
        admit(max(kt + 2 * ks, ks + pt + 2 * ps,
                  2 * (ks + ps) + len(rec_bytes) + 12) + 512)
    key_bytes = encode_payload({"value": key}, max_bytes=limit)
    prov_bytes = encode_payload({"value": prov}, max_bytes=limit)
    if frame_limit is not None and len(key_bytes)+len(prov_bytes)+len(rec_bytes)+12+SORT_FRAME_OVERHEAD > frame_limit:
        raise _ordering_error('BUDGET_EXCEEDED', 'single sort frame exceeds the resident budget')
    return b''.join((struct.pack(">I", len(key_bytes)), key_bytes,
        struct.pack(">I", len(prov_bytes)), prov_bytes,
        struct.pack(">I", len(rec_bytes)), rec_bytes))


class MergeReadBudget:
    """Actual held frames/buffers inside one already admitted merge window."""
    def __init__(self, window, count):
        self.limit = window.merge_reader_bytes
        self.total_limit = window.merge_resident_bytes or window.resident_bytes
        self.frames = [0] * count
        self.buffers = [0] * count
        self.working = [0] * count

    def check(self, extra=0, *, total=False):
        # Fixed readers/heap/slot tables are small and bounded by fan-in.
        used = 4096 + sum(self.frames) + sum(self.buffers) + sum(self.working) + extra
        if used > (self.total_limit if total else self.limit):
            raise _ordering_error('BUDGET_EXCEEDED', 'sort merge frames exceed their measured resident subwindow')

    def external(self, extra):
        self.check(extra, total=True)

    def finish(self, slot, size):
        # The old heap frame and the newly decoded frame coexist until next()
        # returns. Check both before replacing the slot's retained charge.
        self.check(size)
        self.frames[slot] = size
        self.working[slot] = 0

    def close(self, slot):
        self.frames[slot] = self.buffers[slot] = self.working[slot] = 0


class SortFrameReader:
    """Streaming length-prefixed frame reader over bounded store chunks."""

    def __init__(self, chunks: Iterator[bytes], *, decode_payload, max_frame_bytes: int,
                 budget=None, slot=0, chunk_bytes=0) -> None:
        self._decode_payload = decode_payload
        self._max_frame_bytes = max_frame_bytes
        self._chunks = iter(chunks)
        self._buffer = bytearray()
        self._eof = False
        self._budget, self._slot, self._chunk_bytes = budget, slot, chunk_bytes
        if budget is not None:
            budget.buffers[slot] = sys.getsizeof(self._buffer)
            budget.check()

    def _fill(self, needed: int) -> None:
        while len(self._buffer) < needed and not self._eof:
            if self._budget is not None:
                # read()'s next chunk, bytearray growth, and an old chunk can
                # overlap. The configured read chunk is a hard upper bound.
                self._budget.check(sys.getsizeof(self._buffer) + 3 * self._chunk_bytes + 256)
            try:
                chunk = next(self._chunks)
            except StopIteration:
                self._eof = True
                break
            self._buffer.extend(chunk)
            if self._budget is not None:
                self._budget.buffers[self._slot] = sys.getsizeof(self._buffer)

    def _read_exact(self, count: int) -> bytes:
        self._fill(count)
        if len(self._buffer) < count:
            raise _ordering_error("CORRUPT_ARTIFACT", "truncated sort run frame")
        if self._budget is not None:
            self._budget.check(2 * count + 256)
        data = bytes(self._buffer[:count])
        del self._buffer[:count]
        if self._budget is not None:
            self._budget.buffers[self._slot] = sys.getsizeof(self._buffer)
        return data

    def _decode(self, size):
        raw = self._read_exact(size)
        if self._budget is not None:
            from demiflow.execution.contracts.record_codec import raw_workspace
            self._budget.check(raw_workspace(raw))
        return self._decode_payload(raw, max_bytes=self._max_frame_bytes)['value']

    def next_frame(self) -> tuple | None:
        self._fill(4)
        if not self._buffer:
            if self._budget is not None:
                self._budget.finish(self._slot, 0)
            return None
        (key_len,) = struct.unpack(">I", self._read_exact(4))
        if key_len > self._max_frame_bytes:
            raise _ordering_error("BUDGET_EXCEEDED", "sort key frame exceeds the resident budget")
        key = self._decode(key_len)
        if self._budget is not None:
            self._budget.working[self._slot] = retained_frame_bytes(key, (), b'')
        (prov_len,) = struct.unpack(">I", self._read_exact(4))
        if key_len + prov_len + 12 > self._max_frame_bytes:
            raise _ordering_error("BUDGET_EXCEEDED", "sort provenance frame exceeds the resident budget")
        prov = self._decode(prov_len)
        if self._budget is not None:
            self._budget.working[self._slot] = retained_frame_bytes(key, prov, b'')
        (rec_len,) = struct.unpack(">I", self._read_exact(4))
        if key_len + prov_len + rec_len + 12 > self._max_frame_bytes:
            raise _ordering_error("BUDGET_EXCEEDED", "sort record frame exceeds the resident budget")
        rec_bytes = self._read_exact(rec_len)
        if self._budget is not None:
            self._budget.finish(self._slot, retained_frame_bytes(key, prov, rec_bytes))
        return (key, prov, rec_bytes)


def build_sort_runs(records, *, bounds, window=None, input_size, prepare_frame,
                    check_stop, write_run, release_runs, body_bytes=None):
    """Original bounded run-sort over prepared frames and bound storage calls.

    The opaque retained input returned by prepare_frame stays alive until the
    original end-of-iteration release. No Record or runtime service is inspected.
    body_bytes observes the current shared extent after frame preparation.
    """
    if window is not None:
        window.validate(bounds)
    run_budget = bounds.max_resident_bytes if window is None else window.run_bytes
    if run_budget < 1:
        raise _ordering_error("RESOURCE_INFEASIBLE", "max_resident_bytes must be positive")
    refs: list[ArtifactRef] = []
    frames: list[tuple] = []
    used = 0
    spill_total = 0

    def flush() -> None:
        nonlocal frames, used, spill_total
        if not frames:
            return
        if window is not None and window.max_runs is not None and len(refs) >= window.max_runs:
            raise _ordering_error('BUDGET_EXCEEDED', 'sort run references exceed the metadata window')
        frames.sort(key=lambda item: (item[0], item[1]))
        total = sum(len(frame[2]) for frame in frames)
        if spill_total + total + (body_bytes() if body_bytes is not None else 0) > bounds.max_spill_bytes:
            raise _ordering_error("BUDGET_EXCEEDED", "sort spill exceeds max_spill_bytes", bytes=spill_total + total)
        refs.append(write_run(frames))
        spill_total += total
        frames = []
        used = 0

    ordinal = 0
    try:
        for record in records:
            check_stop()
            size = input_size(record)
            if size > bounds.max_row_bytes:
                raise _ordering_error("BUDGET_EXCEEDED", "single row exceeds max_row_bytes", size=size)
            key, prov, encoded, saved_record = prepare_frame(record, ordinal, spill_total)
            frame_bytes = len(encoded) + SORT_FRAME_OVERHEAD
            if frame_bytes > (run_budget if window is None else window.frame_bytes):
                raise _ordering_error("BUDGET_EXCEEDED", "single sort frame exceeds the resident budget")
            resident = frame_bytes if window is None else retained_frame_bytes(key, prov, encoded)
            if resident > run_budget:
                raise _ordering_error('BUDGET_EXCEEDED', 'single retained sort frame exceeds the resident budget')
            if frames and used + resident > run_budget:
                flush()
            frames.append((key, prov, encoded))
            used += resident
            ordinal += 1
            # The input reader charges a single decoded row. Drop this one
            # before requesting the next row from the port.
            del saved_record, record, key, prov, encoded
        flush()
    except BaseException:
        release_runs(refs)
        raise
    return tuple(refs)


def merge_sorted_frames(readers, *, check_stop, budget=None):
    """Original heap merge; bound readers keep their original lazy open/close."""
    try:
        if budget is None:
            for frame in heapq.merge(*readers, key=lambda item: (item[0], item[1])):
                check_stop()
                yield frame
        else:
            # heapq.merge's final-reader shortcut keeps its first value and
            # exhausted heap entry alive while yielding the remaining reader.
            # These are no longer the frames charged to the reader slots.
            # Advance explicitly so every retained frame has one live slot.
            heap = []
            for index, reader in enumerate(readers):
                frame = next(reader, None)
                if frame is not None:
                    heap.append((frame[0], frame[1], index, frame))
                del frame
            heapq.heapify(heap)
            while heap:
                entry = heapq.heappop(heap)
                index, frame = entry[2], entry[3]
                del entry
                check_stop()
                yield frame
                del frame
                frame = next(readers[index], None)
                if frame is not None:
                    heapq.heappush(heap, (frame[0], frame[1], index, frame))
                del frame
    finally:
        for reader in readers:
            reader.close()
