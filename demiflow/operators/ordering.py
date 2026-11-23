"""Ordering implementation."""
from __future__ import annotations

import hashlib
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
