"""Q004 independent unit tests: stream_limits admission (C06, BOUNDS/STREAM).

Uses the real stream_residency authority (capabilities.py) with fixed-schema
operation-fact samples. Effective limits only tighten; fixed Actor residency and
dynamic reader/queue slots are accounted separately; admission is deterministic
and fails before any source read when fixed residency cannot fit; an explicit
max_row ceiling is never exceeded.
"""
import pytest

from demiflow.execution.payload_limits import stream_limits

MB = 1024 ** 2


def _limits(memory=1024 * MB, row=8 * MB, batch=8 * MB):
    return {"memory_bytes": memory, "max_row_bytes": row, "max_batch_bytes": batch,
            "max_output_rows": 1_000_000, "batch_rows": 256, "max_state_bytes": 16 * MB}


def _facts(kind="map", concurrency=1, depth=1, max_batch=1):
    return [{"kind": kind, "arguments": {"concurrency": concurrency, "queue_depth": depth,
                                         "max_batch": max_batch}}]


def test_generous_budget_returns_unchanged_and_only_tightens():
    limits = _limits()
    result = stream_limits(_facts(), limits)
    assert result == limits  # candidate(maximum) admits -> no tightening needed
    for name in ("max_row_bytes", "max_batch_bytes"):
        assert result[name] <= limits[name]


def test_deterministic_for_same_frozen_inputs():
    limits = _limits(memory=256 * MB)
    assert stream_limits(_facts(), limits, 256) == stream_limits(_facts(), limits, 256)


def test_fixed_residency_exceeds_budget_fails_before_read():
    limits = _limits(memory=1 * MB)  # fixed ~80MiB cannot fit
    with pytest.raises(ValueError, match="fixed residency"):
        stream_limits(_facts(), limits)


def test_dynamic_prefetch_slots_tighten_effective_row_ceiling():
    limits = _limits(memory=256 * MB)
    small = stream_limits(_facts(), limits, 1)
    large = stream_limits(_facts(), limits, 256)
    assert small["max_row_bytes"] == 8 * MB  # full ceiling fits with one prefetch row
    assert large["max_row_bytes"] < small["max_row_bytes"]  # 256 slots force a tighter cap
    assert large["max_batch_bytes"] <= limits["max_batch_bytes"]


def test_retained_rows_count_against_dynamic_residency():
    limits = _limits(memory=256 * MB)
    base = stream_limits(_facts(), limits, 1, retained_rows=0)
    held = stream_limits(_facts(), limits, 1, retained_rows=64)
    assert held["max_row_bytes"] <= base["max_row_bytes"]


def test_explicit_max_row_ceiling_preserved():
    limits = _limits(row=4096, batch=8192)
    result = stream_limits(_facts(), limits)
    assert result["max_row_bytes"] <= 4096
    assert result["max_batch_bytes"] <= 8192


def test_input_limits_not_mutated():
    limits = _limits(memory=256 * MB)
    snapshot = dict(limits)
    stream_limits(_facts(), limits, 256)
    assert limits == snapshot


@pytest.mark.parametrize("bad", [0, 257, -1, "1", 1.5])
def test_invalid_source_batch_size_rejected(bad):
    with pytest.raises(ValueError):
        stream_limits(_facts(), _limits(), bad)


@pytest.mark.parametrize("bad", [-1, 1_000_001, "0", 1.5])
def test_invalid_retained_rows_rejected(bad):
    with pytest.raises(ValueError):
        stream_limits(_facts(), _limits(), retained_rows=bad)