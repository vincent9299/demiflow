"""Shuffle order implementation."""
from __future__ import annotations

import hashlib
from dataclasses import dataclass
from demiflow.operators.ordering import SortWindow
from demiflow.execution.contracts.dataflow_errors import DataflowError

_DOMAIN = b'demiflow-shuffle-typed-multiset-v1\x00'

def _shuffle_error(code, message):
    return DataflowError(code, message)


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

def shuffle_windows(bounds, grant):
    """Original build/merge/metadata subwindows; never a new grant."""
    metadata = grant // 16
    # Each run reserves 4 KiB for both passes' immutable references and the
    # current/owned/merged list slots. No unbounded list of tiny runs is allowed.
    maximum_runs = metadata // 4096
    if maximum_runs < 1:
        raise _shuffle_error('RESOURCE_INFEASIBLE', 'seeded shuffle lacks its finite metadata/codec windows')
    frame = grant // 16
    window = SortWindow(resident_bytes=grant // 2, run_bytes=grant // 4,
        frame_bytes=frame, read_chunk_bytes=min(4096, bounds.max_row_bytes, max(1, frame // 8)),
        merge_fan_in=2, merge_resident_bytes=3 * grant // 8,
        codec_bytes=grant // 4, merge_codec_bytes=grant // 8,
        merge_frame_multiplier=6, max_runs=maximum_runs, merge_reader_bytes=grant // 4)
    window.validate(bounds)
    return ShuffleWindows(grant, 3 * grant // 8, metadata, window)

def encode_shuffle_row(row, row_shape, metadata_shape, *, window, max_bytes, encode_payload):
    """Original typed-key allocation checks and encoding, without record I/O."""
    size, native, tree = row_shape
    meta_size, meta_memory = metadata_shape
    # Row + typed tree, canonical key, record document, byte-tagged outer
    # frame and their encodings coexist. Include the previous duplicate key.
    peak = 2 * native + tree + 12 * (size + meta_size) + 2 * meta_memory + 8192
    if peak > window.codec_bytes:
        raise _shuffle_error('BUDGET_EXCEEDED', 'shuffle row codec exceeds its reserved subwindow')
    # Base64 wrappers in the unchanged sort frame expand both key and
    # document. Check before constructing those full encoded copies.
    if 3 * (size + meta_size) + 1024 > window.frame_bytes:
        raise _shuffle_error('BUDGET_EXCEEDED', 'shuffle row exceeds its merge frame subwindow')
    raw = encode_payload(row, max_bytes=max_bytes)
    return raw


class TypedShuffleKey:
    """Versioned duplicate ordinal/rank over canonical typed row bytes."""
    def __init__(self, seed):
        self.seed = seed
        self.previous = None
        self.duplicate = 0

    def __call__(self, raw):
        self.duplicate = self.duplicate + 1 if raw == self.previous else 0
        self.previous = raw
        digest = hashlib.sha256(raw).digest()
        seed = str(self.seed).encode('ascii')
        ordinal = str(self.duplicate).encode('ascii')
        rank = hashlib.sha256(_DOMAIN + seed + b'\x00' + digest + b'\x00' + ordinal).digest()
        # Full bytes plus ordinal make even rank/digest collisions deterministic.
        return (rank, raw, self.duplicate)

    def clear(self):
        self.previous = None

