"""Q007 independent unit tests: derive_prefetch fail-closed floor (C06, BOUNDS/GRAPH/RECOVERY).

Pure function. The admitted inflight ceiling is min(inflight_slots, legacy
max_pending); a protocol floor above that ceiling is rejected (never forced past
the grant). None keeps the exact legacy default path. Mandated reversal of the
old fail-open max() per RESPONSE.md #6 / Q43 and CRITICAL-REVISION.md §6.
"""
import pytest

from demiflow.planning.dataflow_graph_adapter import LEGACY_PREFETCH, derive_prefetch
from demiflow.execution.contracts.errors import ReservationError


def test_none_default_path_is_legacy_exact():
    assert derive_prefetch(None) == LEGACY_PREFETCH
    assert LEGACY_PREFETCH == {"append_slots": 64, "source_batch_size": 256,
                               "queue_depth": 4096, "max_pending": 1024}


def test_caps_to_admitted_budget_never_above_legacy():
    assert derive_prefetch(8) == {"append_slots": 8, "source_batch_size": 8,
                                  "queue_depth": 8, "max_pending": 8}
    assert derive_prefetch(100) == {"append_slots": 64, "source_batch_size": 100,
                                    "queue_depth": 100, "max_pending": 100}
    assert derive_prefetch(10 ** 9) == LEGACY_PREFETCH


def test_floor_within_ceiling_keeps_ceiling():
    assert derive_prefetch(4)["max_pending"] == 4  # default floor 1
    assert derive_prefetch(200, max_pending_floor=64)["max_pending"] == 200
    assert derive_prefetch(4, max_pending_floor=4)["max_pending"] == 4


def test_floor_above_ceiling_rejected_fail_closed():
    # Exact acceptance: credit floor=2, grant ceiling=1 -> reject, no expansion.
    with pytest.raises(ReservationError) as exc:
        derive_prefetch(1, max_pending_floor=2)
    assert exc.value.code == "INVALID_SPEC"
    with pytest.raises(ReservationError) as exc:
        derive_prefetch(4, max_pending_floor=64)
    assert exc.value.code == "INVALID_SPEC"
    # legacy max_pending ceiling also binds even with huge slot budget
    with pytest.raises(ReservationError):
        derive_prefetch(10 ** 9, max_pending_floor=LEGACY_PREFETCH["max_pending"] + 1)


@pytest.mark.parametrize("bad", [0, -1, "8", 1.5, True])
def test_invalid_inflight_slots_rejected(bad):
    with pytest.raises(ReservationError) as exc:
        derive_prefetch(bad)
    assert exc.value.code == "INVALID_SPEC"


@pytest.mark.parametrize("bad", [0, -1, "4", 1.5])
def test_invalid_floor_rejected(bad):
    with pytest.raises(ReservationError) as exc:
        derive_prefetch(8, max_pending_floor=bad)
    assert exc.value.code == "INVALID_SPEC"