"""U4/S2 RSV conformance: engine-side four-dimension pre-call reservation.

ReserveGate reserves input bytes / inflight slots / output bytes / effect units
BEFORE a paid API/GPU call (an unreserved call must not happen), rejects an
undeclared/unbounded output as a precheck defect, releases only on a confirmed
outcome, and keeps unconfirmed reservations charged across controller restarts
(durable reconciliation; recovery overlap re-reserved explicitly). Control-plane /
downstream-commit / lease capacity is held back off the top (v1 §07
deadlock_prevention). Prefetch bounds derive from the admitted inflight budget;
the default path injects no key and stays byte-identical (U3). The effect dimension
composes with the existing LLM PromptJournal (D5): known-completed reuse consumes
no new budget; request-without-response is unknown and stays charged fail-closed.
"""
import sys

import pytest

from demiflow.execution.recovery_contract import RecoveryContractError
from demiflow.execution.reservation import MAX_ACTIVE_RECORDS, CallBudget, ReserveGate, usable_budget
from demiflow.planning.dataflow_graph_adapter import LEGACY_PREFETCH, derive_prefetch
from demiflow.execution.contracts.errors import ReservationError
from demiflow.operator_llm.journal import PromptJournal, UncertainPromptCall, request_key


def _budget(input_bytes=1000, inflight_slots=4, output_bytes=1000, effect_units=4):
    return CallBudget(input_bytes=input_bytes, inflight_slots=inflight_slots,
                      output_bytes=output_bytes, effect_units=effect_units)


def _gate(tmp_path, **kw):
    return ReserveGate(tmp_path / "gate", budget=_budget(**kw))


# ---------------- four-dimension pre-call reservation ----------------

def test_reserve_charges_all_four_dimensions(tmp_path):
    gate = _gate(tmp_path)
    view = gate.reserve("c1", input_bytes=100, output_bytes=200, effect_units=2)
    assert view["state"] == "RESERVED"
    snap = gate.snapshot()
    assert snap["charged"] == {"input_bytes": 100, "inflight_slots": 1,
                               "output_bytes": 200, "effect_units": 2}
    assert snap["budget"] == {"input_bytes": 1000, "inflight_slots": 4,
                              "output_bytes": 1000, "effect_units": 4}


def test_no_call_without_reservation(tmp_path):
    gate = _gate(tmp_path, inflight_slots=1, effect_units=1)
    calls = []

    def guarded_call(call_id):
        # The reservation is the precondition: the paid fn runs only after it.
        gate.reserve(call_id, input_bytes=10, output_bytes=10, effect_units=1)
        try:
            calls.append(call_id)
            return "ok"
        finally:
            gate.release(call_id, outcome="completed")

    assert guarded_call("a") == "ok"
    gate.reserve("hold", input_bytes=10, output_bytes=10, effect_units=1)  # saturate the slot
    with pytest.raises(ReservationError) as exc:
        guarded_call("b")
    assert exc.value.code == "BUDGET_EXCEEDED"
    assert calls == ["a"]  # the unreserved call never happened


@pytest.mark.parametrize("kwargs", [
    {"input_bytes": 10, "output_bytes": 0},
    {"input_bytes": 10, "output_bytes": -5},
    {"input_bytes": 10, "output_bytes": "100"},
    {"input_bytes": 0, "output_bytes": 10},
    {"input_bytes": 10, "output_bytes": 10, "effect_units": 0},
])
def test_unbounded_or_invalid_spec_rejected_precheck(tmp_path, kwargs):
    gate = _gate(tmp_path)
    with pytest.raises(ReservationError) as exc:
        gate.reserve("c1", **kwargs)
    assert exc.value.code == "INVALID_SPEC"
    assert gate.snapshot()["charged"]["inflight_slots"] == 0  # nothing charged, no call


def test_long_output_declared_bounded_maximum(tmp_path):
    # A non-deterministic long output is admitted only against a declared maximum
    # (bounded chunk/spill ceiling); the declared maximum is what stays charged.
    gate = _gate(tmp_path, output_bytes=1024)
    gate.reserve("chunked", input_bytes=8, output_bytes=1024, effect_units=1)
    assert gate.snapshot()["charged"]["output_bytes"] == 1024
    with pytest.raises(ReservationError) as exc:
        gate.reserve("next", input_bytes=8, output_bytes=1, effect_units=1)
    assert exc.value.code == "BUDGET_EXCEEDED"  # no room to save another output


@pytest.mark.parametrize("dimension,budget_kw,first_kw,second_kw", [
    ("input_bytes", {"input_bytes": 100}, {"input_bytes": 90}, {"input_bytes": 20}),
    ("inflight_slots", {"inflight_slots": 1}, {}, {}),
    ("output_bytes", {"output_bytes": 100}, {"output_bytes": 90}, {"output_bytes": 20}),
    ("effect_units", {"effect_units": 1}, {"effect_units": 1}, {"effect_units": 1}),
])
def test_budget_exceeded_per_dimension_is_retriable(tmp_path, dimension, budget_kw, first_kw, second_kw):
    base = dict(input_bytes=1000, inflight_slots=4, output_bytes=1000, effect_units=4)
    gate = _gate(tmp_path, **{**base, **budget_kw})
    unit = dict(input_bytes=1, output_bytes=1, effect_units=1)
    gate.reserve("first", **{**unit, **first_kw})
    with pytest.raises(ReservationError) as exc:
        gate.reserve("second", **{**unit, **second_kw})
    assert exc.value.code == "BUDGET_EXCEEDED"
    assert exc.value.retriable is True
    assert dimension in str(exc.value)


def test_backpressure_by_bytes_not_only_slots(tmp_path):
    # Plenty of inflight slots, but the byte budget binds first: backpressure
    # responds to actual declared bytes, not just the slot count.
    gate = _gate(tmp_path, inflight_slots=100, input_bytes=100, output_bytes=100)
    gate.reserve("big", input_bytes=90, output_bytes=90, effect_units=1)
    with pytest.raises(ReservationError) as exc:
        gate.reserve("small", input_bytes=20, output_bytes=20, effect_units=1)
    assert exc.value.code == "BUDGET_EXCEEDED"
    assert gate.snapshot()["charged"]["inflight_slots"] == 1  # slots were NOT the limit


# ---------------- release / unknown fail-closed ----------------

@pytest.mark.parametrize("outcome", ["completed", "cancelled"])
def test_confirmed_outcome_releases_and_is_idempotent(tmp_path, outcome):
    gate = _gate(tmp_path)
    gate.reserve("c1", input_bytes=10, output_bytes=10)
    first = gate.release("c1", outcome=outcome)
    assert first == {"call_id": "c1", "state": "RELEASED", "idempotent": False}
    assert gate.snapshot()["charged"]["inflight_slots"] == 0
    again = gate.release("c1", outcome=outcome)
    assert again["idempotent"] is True
    with pytest.raises(ReservationError) as exc:
        gate.release("c1", outcome="cancelled" if outcome == "completed" else "completed")
    assert exc.value.code == "INVALID_SPEC"  # a released outcome is immutable


def test_unknown_outcome_stays_charged_fail_closed(tmp_path):
    gate = _gate(tmp_path)
    gate.reserve("c1", input_bytes=10, output_bytes=10, effect_units=1)
    with pytest.raises(ReservationError) as exc:
        gate.release("c1", outcome="unknown")
    assert exc.value.code == "UNKNOWN_EFFECT"
    snap = gate.snapshot()
    assert snap["charged"]["effect_units"] == 1
    assert [r["call_id"] for r in gate.outstanding()] == ["c1"]


def test_immutable_spec_and_released_cannot_rereserve(tmp_path):
    gate = _gate(tmp_path)
    gate.reserve("c1", input_bytes=10, output_bytes=10)
    assert gate.reserve("c1", input_bytes=10, output_bytes=10)["call_id"] == "c1"  # idempotent
    with pytest.raises(ReservationError) as exc:
        gate.reserve("c1", input_bytes=11, output_bytes=10)
    assert exc.value.code == "INVALID_SPEC"  # spec immutable
    gate.release("c1", outcome="completed")
    with pytest.raises(ReservationError) as exc:
        gate.reserve("c1", input_bytes=10, output_bytes=10)
    assert exc.value.code == "INVALID_SPEC"  # released cannot reserve again
    with pytest.raises(ReservationError):
        gate.release("never-reserved", outcome="completed")


# ---------------- durable reconciliation / recovery overlap ----------------

def test_durable_reconciliation_across_restart(tmp_path):
    root = tmp_path / "gate"
    gate = ReserveGate(root, budget=_budget())
    gate.reserve("live", input_bytes=10, output_bytes=10)
    gate.reserve("done", input_bytes=10, output_bytes=10)
    gate.release("done", outcome="completed")
    # Controller restart: a fresh gate re-charges only the non-released facts.
    reborn = ReserveGate(root, budget=_budget())
    assert [r["call_id"] for r in reborn.outstanding()] == ["live"]
    assert reborn.snapshot()["charged"]["inflight_slots"] == 1


def test_recovery_overlap_explicit_reserve(tmp_path):
    root = tmp_path / "gate"
    gate = ReserveGate(root, budget=_budget())
    gate.reserve("overlap", input_bytes=10, output_bytes=10, effect_units=1)
    reborn = ReserveGate(root, budget=_budget())
    # Same call re-reserved explicitly: idempotent, never double-charged.
    reborn.reserve("overlap", input_bytes=10, output_bytes=10, effect_units=1)
    assert reborn.snapshot()["charged"]["effect_units"] == 1
    # A different call attempting the same effect is conservatively double-charged.
    reborn.reserve("overlap-retry", input_bytes=10, output_bytes=10, effect_units=1)
    assert reborn.snapshot()["charged"]["effect_units"] == 2


# ---------------- control/downstream/lease holdback ----------------

def test_control_downstream_lease_holdback_off_the_top():
    total = {"input_bytes": 1000, "inflight_slots": 10, "output_bytes": 1000, "effect_units": 10}
    reserved = {"inflight_slots": 3, "output_bytes": 200, "effect_units": 2}
    budget = usable_budget(total, reserved=reserved)
    assert budget == CallBudget(input_bytes=1000, inflight_slots=7, output_bytes=800, effect_units=8)


def test_data_calls_cannot_consume_holdback(tmp_path):
    total = {"input_bytes": 1000, "inflight_slots": 4, "output_bytes": 1000, "effect_units": 4}
    # Reserve 1 slot / 1 effect unit for control-plane progress + downstream commit.
    budget = usable_budget(total, reserved={"inflight_slots": 1, "effect_units": 1})
    gate = ReserveGate(tmp_path / "gate", budget=budget)
    for i in range(3):
        gate.reserve("d%d" % i, input_bytes=1, output_bytes=1, effect_units=1)
    with pytest.raises(ReservationError) as exc:
        gate.reserve("d3", input_bytes=1, output_bytes=1, effect_units=1)
    assert exc.value.code == "BUDGET_EXCEEDED"  # the held-back slot stays available for control


def test_holdback_infeasible_rejected_before_any_call():
    total = {"input_bytes": 100, "inflight_slots": 2, "output_bytes": 100, "effect_units": 2}
    with pytest.raises(ReservationError) as exc:
        usable_budget(total, reserved={"inflight_slots": 2})
    assert exc.value.code == "INVALID_SPEC"
    with pytest.raises(ReservationError):
        usable_budget({"input_bytes": 100}, reserved={})
    with pytest.raises(ReservationError):
        usable_budget(total, reserved={"unknown_dim": 1})


# ---------------- record bounds ----------------

def test_active_record_quota_enforced(tmp_path):
    gate = _gate(tmp_path, inflight_slots=MAX_ACTIVE_RECORDS + 10,
                 input_bytes=10 ** 9, output_bytes=10 ** 9, effect_units=10 ** 9)
    for i in range(MAX_ACTIVE_RECORDS):
        gate.reserve("c%d" % i, input_bytes=1, output_bytes=1, effect_units=1)
    with pytest.raises(ReservationError) as exc:
        gate.reserve("overflow", input_bytes=1, output_bytes=1, effect_units=1)
    assert exc.value.code == "QUOTA_EXCEEDED"


def test_invalid_call_identity_rejected(tmp_path):
    gate = _gate(tmp_path)
    for bad in ("", "x" * 129, "has space", "slash/id", 123):
        with pytest.raises(RecoveryContractError):
            gate.reserve(bad, input_bytes=1, output_bytes=1)


# ---------------- prefetch derivation ----------------

def test_derive_prefetch_default_equivalence():
    assert derive_prefetch(None) == LEGACY_PREFETCH
    assert LEGACY_PREFETCH == {"append_slots": 64, "source_batch_size": 256,
                               "queue_depth": 4096, "max_pending": 1024}
    assert derive_prefetch(10 ** 9) == LEGACY_PREFETCH  # never above the legacy ceiling


def test_derive_prefetch_caps_to_admitted_budget():
    assert derive_prefetch(8) == {"append_slots": 8, "source_batch_size": 8,
                                  "queue_depth": 8, "max_pending": 8}
    assert derive_prefetch(100) == {"append_slots": 64, "source_batch_size": 100,
                                    "queue_depth": 100, "max_pending": 100}
    for bad in (0, -1, "8", 1.5):
        with pytest.raises(ReservationError) as exc:
            derive_prefetch(bad)
        assert exc.value.code == "INVALID_SPEC"


def test_derive_prefetch_max_pending_protocol_floor():
    # A protocol floor larger than admission must be rejected before sealing.
    with pytest.raises(ReservationError, match="floor exceeds"):
        derive_prefetch(4, max_pending_floor=64)
    assert derive_prefetch(4)["max_pending"] == 4  # default floor is 1
    assert derive_prefetch(200, max_pending_floor=64)["max_pending"] == 200
    with pytest.raises(ReservationError):
        derive_prefetch(4, max_pending_floor=0)


# ---------------- effect composition with the real LLM journal (D5) ----------------

def _request(tag):
    return {"protocol": "openai-chat", "model_contract": {"model": "m"}, "body": {"prompt": tag}}


def test_effect_known_completed_reuse_consumes_no_new_budget(tmp_path):
    gate = _gate(tmp_path, effect_units=2)
    journal = PromptJournal(tmp_path / "journal")
    request = _request("hello")
    call_id = request_key(request)
    assert journal.lookup(request) is None
    gate.reserve(call_id, input_bytes=10, output_bytes=10, effect_units=1)
    assert journal.reserve(request) is True
    result = {"text": "world"}
    journal.response(request, result)
    gate.release(call_id, outcome="completed")
    assert gate.snapshot()["charged"]["effect_units"] == 0
    # Known-completed reuse: the journal returns the recorded response, no new
    # reservation and no new call — the effect budget stays free.
    assert journal.lookup(request) == result
    assert journal.reserve(request) is False
    assert gate.snapshot()["charged"]["effect_units"] == 0


def test_effect_uncertain_stays_charged_no_blind_retry(tmp_path):
    root = tmp_path / "gate"
    gate = ReserveGate(root, budget=_budget(effect_units=1))
    journal = PromptJournal(tmp_path / "journal")
    request = _request("uncertain")
    call_id = request_key(request)
    gate.reserve(call_id, input_bytes=10, output_bytes=10, effect_units=1)
    assert journal.reserve(request) is True
    # Crash: the request is durable but no response was recorded.
    reborn = ReserveGate(root, budget=_budget(effect_units=1))
    assert reborn.snapshot()["charged"]["effect_units"] == 1
    with pytest.raises(UncertainPromptCall):
        journal.lookup(request)
    # Recovery surfaces UNKNOWN fail-closed: the reservation is NOT freed.
    with pytest.raises(ReservationError) as exc:
        reborn.release(call_id, outcome="unknown")
    assert exc.value.code == "UNKNOWN_EFFECT"
    assert reborn.snapshot()["charged"]["effect_units"] == 1
    # No blind retry: re-reserving the same call is idempotent (still one charge),
    # and a fresh attempt is budget-blocked while the unconfirmed effect stands.
    reborn.reserve(call_id, input_bytes=10, output_bytes=10, effect_units=1)
    assert reborn.snapshot()["charged"]["effect_units"] == 1
    with pytest.raises(ReservationError) as exc:
        reborn.reserve(request_key(_request("blind")), input_bytes=10, output_bytes=10, effect_units=1)
    assert exc.value.code == "BUDGET_EXCEEDED"


def test_no_journal_cross_restart_effect_unknown_fail_closed(tmp_path):
    # D5①: with no effect journal, a cross-restart effect is unprovable. Recovery
    # must mark it UNKNOWN fail-closed (stays charged), never blind-retry.
    root = tmp_path / "gate"
    gate = ReserveGate(root, budget=_budget(effect_units=1))
    gate.reserve("embed-1", input_bytes=10, output_bytes=10, effect_units=1)
    # Crash before any release; no journal exists to prove completion.
    reborn = ReserveGate(root, budget=_budget(effect_units=1))
    assert [r["call_id"] for r in reborn.outstanding()] == ["embed-1"]
    assert reborn.snapshot()["charged"]["effect_units"] == 1
    with pytest.raises(ReservationError) as exc:
        reborn.release("embed-1", outcome="unknown")
    assert exc.value.code == "UNKNOWN_EFFECT"
    assert reborn.snapshot()["charged"]["effect_units"] == 1  # unprovable, still charged
    with pytest.raises(ReservationError) as exc:
        reborn.reserve("embed-2", input_bytes=10, output_bytes=10, effect_units=1)
    assert exc.value.code == "BUDGET_EXCEEDED"  # no blind new attempt while unconfirmed


# ---------------- configured prefetch end-to-end (Local graph) ----------------

pytestmark_linux = pytest.mark.skipif(not sys.platform.startswith("linux"),
                                      reason="Local managed units require Linux")


def _mini_graph_spec(prefetch_slots=None):
    from demiflow.data.plan import CallableSpec, LogicalPlan, MapOp, MapStreamOptions
    from demiflow.data.sources import ItemsSource

    def double(row):
        return {**row, "x": row["x"] * 2}

    def passthrough(row):
        return row

    def plan(fn):
        return LogicalPlan((MapOp(CallableSpec.create(fn),
                                  stream_options=MapStreamOptions(concurrency=1)),))

    n = 24
    readers = ["c0", "c1"]
    spec = {
        "source": ItemsSource(tuple({"x": i} for i in range(n))), "source_id": "rsv-source",
        "ancestor": {"plan": plan(double), "logical_node_id": "shared", "occurrence_id": "shared-p0"},
        "targets": [{"plan": plan(passthrough), "logical_node_id": "node-" + rid,
                     "occurrence_id": rid + "-p0", "reader_id": rid} for rid in readers],
        "routing": "broadcast", "exchange_mode": "memory_incremental",
        "credit_budget": {"rows": 64, "bytes": 8 * 1024 ** 2}, "queue_depth": 4096,
    }
    if prefetch_slots is not None:
        spec["prefetch_slots"] = prefetch_slots
    return spec, readers, n


def _run_graph(tmp_path, spec):
    from demiflow.execution.executors.local import LocalDatasetExecutor
    from demiflow.execution.run_context import RunContext
    executor = LocalDatasetExecutor(workers=1)
    with RunContext.start(tmp_path / "run",
                          limits={"checkpoint_rows": 8, "checkpoint_interval_ms": 60000}) as run:
        with run.action({"kind": "materialize", "source": {"fixture": "rsv"}}) as execution:
            result = executor.materialize_graph(spec)
            execution.complete(dict(kind="graph-materialized", workers=result["workers"]))
        from demiflow.execution.sealed_plans import load_sealed_plan
        sealed = load_sealed_plan(run.directory, execution.action_id, version='v3')
    router = next(node.occurrence_id for node in sealed['graph'].nodes if 'graph_route' in node.kernel.configuration)
    routed = {edge.edge_id for edge in sealed['graph'].edges if router in (edge.producer, edge.consumer)}
    return result, [exchange for exchange in sealed['physical'].exchanges if exchange.edge_id in routed]


@pytest.fixture
def pidfds_available():
    from demiflow.resources.process_observation import require_pidfds
    try:
        require_pidfds()
    except (OSError, RuntimeError) as exc:
        pytest.skip("pidfd exit evidence unavailable: " + str(exc))


@pytestmark_linux
def test_graph_configured_prefetch_binds_published_credit(tmp_path, pidfds_available):
    spec, readers, n = _mini_graph_spec(prefetch_slots=4)
    with pytest.raises(ReservationError, match="explicit credit rows exceed"):
        _run_graph(tmp_path / "invalid", spec)
    del spec["credit_budget"]
    result, exchanges = _run_graph(tmp_path / "admitted", spec)
    assert result["egress_rows"] == n
    assert result["target_rows"] == dict.fromkeys(readers, n)
    assert exchanges and all(0 < exchange.credit_rows <= 4 for exchange in exchanges)


@pytestmark_linux
def test_graph_default_explicit_credit_visible_in_published_plan(tmp_path, pidfds_available):
    spec, readers, n = _mini_graph_spec()
    result, exchanges = _run_graph(tmp_path, spec)
    assert result["target_rows"] == dict.fromkeys(readers, n)
    assert exchanges and all(0 < exchange.credit_rows <= spec["credit_budget"]["rows"] for exchange in exchanges)
