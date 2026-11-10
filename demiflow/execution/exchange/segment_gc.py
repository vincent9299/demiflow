"""Bounded incremental segment index + three-factor GC safe point (U3/S4d).

Design §5.3: the safe point per physical channel is the minimum of the reader's
consumed cursor and the latest acked checkpoint's consumer offset; pinned
state/effect refs retain their segments regardless. A segment is reclaimed only
once every channel that references it has advanced its safe point past the
sequence at which the segment was sent AND it carries no pin. Reclaim unlinks the
segment files (Local) and records evidence (segment id + channel sequence range +
time + reason); Ray object release is the S5 carriage.

The index holds only LIVE segments: registration is incremental and reclaim drops
entries as it goes, so collection never scans full history. ``unsubscribe``
releases a reader's pin (its channel stops holding segments back); ``reclaim_all``
is the terminate path (drain/cancel/fault) that frees every remaining segment once
the run no longer needs the channel data plane (committed output and the fixed
source capture live in BlockSink, not here).
"""
import json
import time
from pathlib import Path

from ..recovery_contract import identifier
from ..state_artifacts import atomic_bytes


class SegmentIndex:
    """Live channel-segment index with a monotonic per-channel GC safe point."""

    def __init__(self, directory, *, evidence_name="gc-evidence.jsonl"):
        self._directory = Path(directory).resolve()
        self._segments = {}
        self._cursors = {}
        self._acked = {}
        self._released = set()
        self._evidence_path = self._directory / evidence_name
        self.reclaimed = []

    @property
    def live_count(self):
        return len(self._segments)

    def register(self, segment_id, paths, channel_id, sequence):
        """Record that a live segment is referenced by (channel, sequence).

        One segment may ride several channels (broadcast/fan-out replicates the
        same DataRef); each reference is tracked so reclaim waits for every reader.
        """
        identifier(segment_id)
        identifier(channel_id)
        if type(sequence) is not int or sequence < 0:
            raise ValueError("segment sequence must be a nonnegative int")
        entry = self._segments.setdefault(
            segment_id, {"paths": [], "channels": {}, "pinned": False})
        for relative in paths:
            if relative not in entry["paths"]:
                entry["paths"].append(relative)
        entry["channels"].setdefault(channel_id, set()).add(sequence)

    def note_cursor(self, channel_id, cursor):
        """Reader's last consumed data sequence (-1 before any consumption)."""
        identifier(channel_id)
        self._cursors[channel_id] = max(self._cursors.get(channel_id, -1), cursor)

    def note_acked(self, channel_id, consumer_offset):
        """Latest acked checkpoint consumer offset (next-to-consume bound)."""
        identifier(channel_id)
        self._acked[channel_id] = max(self._acked.get(channel_id, 0), consumer_offset)

    def safe_point(self, channel_id):
        """min(reader cursor offset, acked consumer offset); 0 until both advance."""
        cursor_offset = self._cursors.get(channel_id, -1) + 1
        acked_offset = self._acked.get(channel_id, 0)
        if channel_id in self._released:
            return float("inf")
        return min(cursor_offset, acked_offset)

    def pin(self, segment_id):
        """A state/effect ref pins this segment against reclaim below the safe point."""
        if segment_id in self._segments:
            self._segments[segment_id]["pinned"] = True

    def unsubscribe(self, channel_id):
        """Explicit reader pin release: the channel no longer holds segments back."""
        identifier(channel_id)
        self._released.add(channel_id)

    def _needed(self, entry):
        for channel_id, sequences in entry["channels"].items():
            safe = self.safe_point(channel_id)
            if any(sequence >= safe for sequence in sequences):
                return True
        return False

    def _remove(self, segment_id, entry, reason):
        for relative in entry["paths"]:
            path = self._directory / relative
            try:
                path.unlink()
            except FileNotFoundError:
                pass
        ranges = {ch: [min(seqs), max(seqs)] for ch, seqs in entry["channels"].items()}
        record = {"segment_id": segment_id, "channel_ranges": ranges,
                  "paths": list(entry["paths"]), "reason": reason,
                  "time_ms": int(time.time() * 1000)}
        self.reclaimed.append(record)
        del self._segments[segment_id]
        self._write_evidence(record)

    def _write_evidence(self, record):
        self._directory.mkdir(parents=True, exist_ok=True)
        line = json.dumps(record, sort_keys=True) + "\n"
        existing = self._evidence_path.read_bytes() if self._evidence_path.exists() else b""
        atomic_bytes(self._evidence_path, existing + line.encode())

    def reclaim(self):
        """Free unpinned live segments every referencing channel has consumed past.

        Iterates only the live index (bounded by outstanding segments), never the
        full history; reclaimed entries are dropped so the index stays bounded.
        """
        freed = []
        for segment_id in list(self._segments):
            entry = self._segments[segment_id]
            if entry["pinned"] or self._needed(entry):
                continue
            self._remove(segment_id, entry, "safe-point")
            freed.append(segment_id)
        return freed

    def reclaim_all(self, reason="terminated"):
        """Terminate path: free every remaining segment (drain/cancel/fault)."""
        freed = []
        for segment_id in list(self._segments):
            self._remove(segment_id, self._segments[segment_id], reason)
            freed.append(segment_id)
        return freed