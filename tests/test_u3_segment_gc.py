"""U3 segment-GC conformance (S4d, design §5.3 / §7 GC group).

Proves the GC state machine over real segment files: the three-factor safe point
(min of reader cursor and acked consumer offset, with pinned state/effect refs
retained), bounded incremental index (no full-history scan), multi-reader broadcast
retention, explicit unsubscribe pin release, terminate full reclaim, and reclaim
evidence on disk (segment id + channel sequence range + time + reason).
"""
import json

from demiflow.execution.exchange.segment_gc import SegmentIndex


def _segment(tmp_path, segment_id, payload=b"x"):
    relative = "segments/" + segment_id + ".pkl"
    path = tmp_path / relative
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(payload)
    return relative


def _index(tmp_path):
    return SegmentIndex(tmp_path)


def test_gc_safe_point_is_min_of_cursor_and_acked_offset(tmp_path):
    index = _index(tmp_path)
    paths = [_segment(tmp_path, "s0"), _segment(tmp_path, "s1"), _segment(tmp_path, "s2")]
    for seq, relative in enumerate(paths):
        index.register("seg-" + str(seq), [relative], "ch-a", seq)
    # Reader consumed through sequence 2 (cursor 2 -> offset 3) but only sequence 1
    # is acked (offset 1): the safe point is the min, so nothing past the ack is freed.
    index.note_cursor("ch-a", 2)
    index.note_acked("ch-a", 1)
    assert index.safe_point("ch-a") == 1
    assert index.reclaim() == ["seg-0"]  # sequence 0 < safe point 1
    assert not (tmp_path / paths[0]).exists()
    assert (tmp_path / paths[1]).exists() and (tmp_path / paths[2]).exists()
    # The ack advances to offset 3: sequences 1 and 2 now fall below the safe point.
    index.note_acked("ch-a", 3)
    assert index.safe_point("ch-a") == 3
    assert sorted(index.reclaim()) == ["seg-1", "seg-2"]
    assert index.live_count == 0


def test_gc_pinned_state_ref_retains_segment_below_safe_point(tmp_path):
    index = _index(tmp_path)
    pinned_path = _segment(tmp_path, "pinned")
    free_path = _segment(tmp_path, "free")
    index.register("seg-pinned", [pinned_path], "ch-a", 0)
    index.register("seg-free", [free_path], "ch-a", 1)
    index.pin("seg-pinned")  # a state/effect ref still needs this segment
    index.note_cursor("ch-a", 5)
    index.note_acked("ch-a", 6)
    assert index.reclaim() == ["seg-free"]
    assert (tmp_path / pinned_path).exists()  # pin holds it despite safe point 6
    assert not (tmp_path / free_path).exists()
    assert index.live_count == 1


def test_gc_broadcast_segment_waits_for_every_reader(tmp_path):
    index = _index(tmp_path)
    shared = _segment(tmp_path, "shared")
    # One broadcast segment rides two channels at sequence 0 (egress replication).
    index.register("seg-shared", [shared], "ch-a", 0)
    index.register("seg-shared", [shared], "ch-b", 0)
    index.note_cursor("ch-a", 0)
    index.note_acked("ch-a", 1)
    # ch-a is past it, but ch-b has not consumed/acked: the segment is still needed.
    assert index.reclaim() == []
    assert (tmp_path / shared).exists()
    index.note_cursor("ch-b", 0)
    index.note_acked("ch-b", 1)
    assert index.reclaim() == ["seg-shared"]
    assert not (tmp_path / shared).exists()


def test_gc_unsubscribe_releases_reader_pin(tmp_path):
    index = _index(tmp_path)
    relative = _segment(tmp_path, "held")
    index.register("seg-held", [relative], "ch-a", 0)
    index.register("seg-held", [relative], "ch-b", 0)
    index.note_cursor("ch-a", 0)
    index.note_acked("ch-a", 1)
    # ch-b never advances (a partial cancel): unsubscribe releases its pin so the
    # segment ch-a already consumed past is freed rather than held forever.
    index.unsubscribe("ch-b")
    assert index.safe_point("ch-b") == float("inf")
    assert index.reclaim() == ["seg-held"]
    assert not (tmp_path / relative).exists()


def test_gc_terminate_reclaims_all_remaining_segments(tmp_path):
    index = _index(tmp_path)
    paths = [_segment(tmp_path, "t" + str(i)) for i in range(4)]
    for seq, relative in enumerate(paths):
        index.register("seg-" + str(seq), [relative], "ch-a", seq)
    index.pin("seg-0")  # even a pinned segment is freed once the run terminates
    index.note_cursor("ch-a", 0)
    index.note_acked("ch-a", 1)
    freed = index.reclaim_all(reason="drained")
    assert sorted(freed) == ["seg-0", "seg-1", "seg-2", "seg-3"]
    assert index.live_count == 0
    assert all(not (tmp_path / p).exists() for p in paths)


def test_gc_index_is_bounded_incremental_no_history_scan(tmp_path):
    index = _index(tmp_path)
    # Register and reclaim many generations; the live index never accumulates
    # history, so reclaim cost is bounded by outstanding segments, not total sent.
    for seq in range(200):
        relative = _segment(tmp_path, "g" + str(seq))
        index.register("seg-" + str(seq), [relative], "ch-a", seq)
        index.note_cursor("ch-a", seq)
        index.note_acked("ch-a", seq + 1)
        assert index.reclaim() == ["seg-" + str(seq)]
        assert index.live_count == 0  # nothing accumulates across generations
    assert len(index.reclaimed) == 200


def test_gc_reclaim_evidence_on_disk(tmp_path):
    index = _index(tmp_path)
    relative = _segment(tmp_path, "ev")
    index.register("seg-ev", [relative], "ch-a", 3)
    index.register("seg-ev", [relative], "ch-b", 5)
    index.note_cursor("ch-a", 3)
    index.note_acked("ch-a", 4)
    index.note_cursor("ch-b", 5)
    index.note_acked("ch-b", 6)
    index.reclaim()
    index.reclaim_all(reason="drained")  # no-op, already freed
    evidence_path = tmp_path / "gc-evidence.jsonl"
    assert evidence_path.exists()
    records = [json.loads(line) for line in evidence_path.read_text().splitlines()]
    assert len(records) == 1
    record = records[0]
    assert record["segment_id"] == "seg-ev"
    assert record["channel_ranges"] == {"ch-a": [3, 3], "ch-b": [5, 5]}
    assert record["paths"] == [relative]
    assert record["reason"] == "safe-point"
    assert type(record["time_ms"]) is int and record["time_ms"] > 0