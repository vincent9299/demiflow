"""Q008 independent unit tests: resolve_prefetch_credit joint solve (C06, BOUNDS/GRAPH/RECOVERY).

Pure/deterministic. Slot grant + byte grant are checked together against the
actual channel credit before seal. Default credit tightens to one consistent
smaller grant (shared by seal/router/InputGate); explicit credit the grant cannot
honor is rejected before any source read. The floor never expands the grant; no
second grant is requested; recovery re-solve is identical to the original seal.
"""
import pytest

from demiflow.execution.reservation import (
    LEGACY_PREFETCH, ReservationError, derive_prefetch, resolve_prefetch_credit,
)

MB = 1024 ** 2


def test_explicit_rows_above_ceiling_rejected_before_read():
    # Exact acceptance: credit floor=2, grant ceiling=1 -> reject.
    with pytest.raises(ReservationError) as exc:
        resolve_prefetch_credit(1, inflight_bytes=MB, credit_budget={"rows": 2, "bytes": 8},
                                credit_explicit=True)
    assert exc.value.code == "BUDGET_EXCEEDED" and exc.value.retriable is False


def test_explicit_bytes_above_grant_rejected():
    with pytest.raises(ReservationError) as exc:
        resolve_prefetch_credit(64, inflight_bytes=100, credit_budget={"rows": 8, "bytes": 200},
                                credit_explicit=True)
    assert exc.value.code == "BUDGET_EXCEEDED"


def test_explicit_within_grant_kept_unchanged():
    result = resolve_prefetch_credit(64, inflight_bytes=8 * MB,
                                     credit_budget={"rows": 64, "bytes": 8 * MB}, credit_explicit=True)
    assert result["credit_budget"] == {"rows": 64, "bytes": 8 * MB}
    assert result["prefetch"] == derive_prefetch(64, max_pending_floor=64)


def test_default_credit_tightens_to_grant_consistently():
    result = resolve_prefetch_credit(4, inflight_bytes=1024,
                                     credit_budget={"rows": 64, "bytes": 8 * MB}, credit_explicit=False)
    assert result["credit_budget"] == {"rows": 4, "bytes": 1024}
    # one consistent result: alignment buffer holds the full tightened grant
    assert result["prefetch"] == derive_prefetch(4, max_pending_floor=4)
    assert result["prefetch"]["max_pending"] >= result["credit_budget"]["rows"]


def test_default_small_legal_budget_still_advances():
    result = resolve_prefetch_credit(1, inflight_bytes=64,
                                     credit_budget={"rows": 64, "bytes": 8 * MB}, credit_explicit=False)
    assert result["credit_budget"] == {"rows": 1, "bytes": 64}
    assert result["prefetch"]["max_pending"] == 1  # buffer == one full grant, no deadlock


def test_default_never_expands_grant():
    result = resolve_prefetch_credit(4, inflight_bytes=1024,
                                     credit_budget={"rows": 2, "bytes": 512}, credit_explicit=False)
    assert result["credit_budget"] == {"rows": 2, "bytes": 512}  # already fits, unchanged
    assert result["prefetch"]["max_pending"] <= LEGACY_PREFETCH["max_pending"]


def test_deterministic_recovery_resolve_and_no_input_mutation():
    credit = {"rows": 64, "bytes": 999}
    snapshot = dict(credit)
    first = resolve_prefetch_credit(4, inflight_bytes=1024, credit_budget=credit, credit_explicit=False)
    again = resolve_prefetch_credit(4, inflight_bytes=1024, credit_budget=credit, credit_explicit=False)
    assert first == again  # recovery re-solve reproduces the sealed credit
    assert credit == snapshot  # inputs not modified
    assert first["credit_budget"] is not credit  # all-new bounded values


def test_none_unbound_path_keeps_legacy_and_credit():
    result = resolve_prefetch_credit(None, inflight_bytes=1,
                                     credit_budget={"rows": 64, "bytes": 8}, credit_explicit=False)
    assert result["prefetch"] == LEGACY_PREFETCH
    assert result["credit_budget"] == {"rows": 64, "bytes": 8}


@pytest.mark.parametrize("credit", [
    {"rows": 0, "bytes": 8}, {"rows": 1, "bytes": 0}, {"rows": -1, "bytes": 8},
    {"rows": 1, "bytes": -8}, {"rows": 1}, {"bytes": 8},
    {"rows": 1, "bytes": 8, "extra": 2}, {"rows": "1", "bytes": 8}, {"rows": 1, "bytes": 1.5},
])
def test_invalid_credit_budget_rejected(credit):
    with pytest.raises(ReservationError) as exc:
        resolve_prefetch_credit(4, inflight_bytes=8, credit_budget=credit, credit_explicit=False)
    assert exc.value.code == "INVALID_SPEC"


@pytest.mark.parametrize("slots,byte_grant", [
    (0, 8), (-1, 8), ("4", 8), (1.5, 8), (4, 0), (4, -1), (4, "8"), (4, 1.5),
])
def test_invalid_grant_rejected(slots, byte_grant):
    with pytest.raises(ReservationError) as exc:
        resolve_prefetch_credit(slots, inflight_bytes=byte_grant,
                                credit_budget={"rows": 1, "bytes": 8}, credit_explicit=False)
    assert exc.value.code == "INVALID_SPEC"


def test_invalid_credit_explicit_rejected():
    with pytest.raises(ReservationError) as exc:
        resolve_prefetch_credit(4, inflight_bytes=8, credit_budget={"rows": 1, "bytes": 8},
                                credit_explicit="yes")
    assert exc.value.code == "INVALID_SPEC"


def test_solved_credit_always_satisfies_derive_prefetch_floor():
    # floor <= ceiling guaranteed for every admitted slot budget (no second raise)
    for slots in (1, 2, 4, 64, 256, 1024, 4096, 10 ** 9):
        result = resolve_prefetch_credit(slots, inflight_bytes=8 * MB,
                                         credit_budget={"rows": 4096, "bytes": 8 * MB},
                                         credit_explicit=False)
        assert result["prefetch"] == derive_prefetch(slots, max_pending_floor=result["credit_budget"]["rows"])
        assert result["credit_budget"]["rows"] <= result["prefetch"]["max_pending"]