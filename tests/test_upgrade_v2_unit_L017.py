"""L017 unit: run_local_graph prefetch credit wiring (Q008 seam) + recovery guard.

The bound path joint-solves slot+byte credit through resolve_prefetch_credit
before sealing; the default path injects no prefetch key. Q008's callee is not yet
merged into this worktree, so a contract-sample callee is injected to execute the
REAL wiring block and assert it derives inflight_bytes = max(max_row_bytes+256,
max_batch_bytes), passes credit_explicit correctly, and consumes both returned
fields. End-to-end bound-path execution against the real Q008 callee is an
integration gate. AST checks pin the wiring and the recovery byte-grant guard.
"""
import ast
import pathlib
import types

import pytest

import demiflow

assert pathlib.Path(demiflow.__file__).resolve().is_relative_to(
    pathlib.Path(__file__).resolve().parents[1]), demiflow.__file__

from demiflow.execution import graph, operator_precheck, reservation
from demiflow.execution.reservation import LEGACY_PREFETCH, ReservationError, derive_prefetch

_SRC = pathlib.Path(graph.__file__).read_text()


class _ReachedFreeze(Exception):
    pass


def _sample(calls):
    # Faithful test-domain mirror of Q008 resolve_prefetch_credit @ f921605: joint
    # slot+byte solve, default credit tightens to the admitted ceiling, explicit
    # credit the grant cannot honor is rejected fail-closed (no max-expansion).
    def resolve(inflight_slots, *, inflight_bytes, credit_budget, credit_explicit):
        calls.append(dict(inflight_slots=inflight_slots, inflight_bytes=inflight_bytes,
                          credit_budget=credit_budget, credit_explicit=credit_explicit))
        rows, byte_credit = credit_budget["rows"], credit_budget["bytes"]
        ceiling = min(inflight_slots, LEGACY_PREFETCH["max_pending"])
        if credit_explicit:
            if rows > ceiling:
                raise ReservationError("BUDGET_EXCEEDED",
                                       "explicit credit rows exceed the admitted prefetch ceiling; cannot expand grant")
            if byte_credit > inflight_bytes:
                raise ReservationError("BUDGET_EXCEEDED",
                                       "explicit credit bytes exceed the admitted inflight byte grant")
            solved_rows, solved_bytes = rows, byte_credit
        else:
            solved_rows, solved_bytes = min(rows, ceiling), min(byte_credit, inflight_bytes)
        return {"prefetch": derive_prefetch(inflight_slots, max_pending_floor=solved_rows),
                "credit_budget": {"rows": solved_rows, "bytes": solved_bytes}}
    return resolve


def _drive(monkeypatch, graph_spec, sample, limits):
    action = types.SimpleNamespace(context=types.SimpleNamespace(limits=limits))
    monkeypatch.setattr(graph, "current_action", lambda: action)
    monkeypatch.setattr(operator_precheck, "precheck_plan_state", lambda plan: None)

    def freeze(action, source):
        raise _ReachedFreeze()

    monkeypatch.setattr(graph, "freeze_source", freeze)
    monkeypatch.setattr(reservation, "resolve_prefetch_credit", sample, raising=False)
    with pytest.raises(_ReachedFreeze):
        graph.run_local_graph(graph_spec)


def _spec(**extra):
    spec = {"source": object(), "source_id": "s1", "ancestor": {"plan": object()},
            "targets": [{"plan": object()}]}
    spec.update(extra)
    return spec


@pytest.mark.parametrize("limits,expected", [
    ({"max_row_bytes": 1000, "max_batch_bytes": 4096}, 4096),
    ({"max_row_bytes": 10000, "max_batch_bytes": 4096}, 10256),
])
def test_bound_path_derives_inflight_bytes_and_explicit_credit(monkeypatch, limits, expected):
    calls = []
    budget = {"rows": 4, "bytes": 4096}   # within the admitted ceiling (slots=4)
    _drive(monkeypatch, _spec(prefetch_slots=4, credit_budget=budget), _sample(calls), limits)
    assert len(calls) == 1
    assert calls[0]["inflight_slots"] == 4
    assert calls[0]["inflight_bytes"] == expected
    assert calls[0]["credit_budget"] == budget
    assert calls[0]["credit_explicit"] is True


def test_bound_path_without_explicit_credit_marks_default(monkeypatch):
    calls = []
    _drive(monkeypatch, _spec(prefetch_slots=4), _sample(calls),
           {"max_row_bytes": 1000, "max_batch_bytes": 4096})
    assert len(calls) == 1
    assert calls[0]["credit_explicit"] is False
    assert calls[0]["credit_budget"] == {"rows": 64, "bytes": 8 * 1024 ** 2}


def test_default_path_injects_no_prefetch_and_never_calls_seam(monkeypatch):
    calls = []
    _drive(monkeypatch, _spec(), _sample(calls),
           {"max_row_bytes": 1000, "max_batch_bytes": 4096})
    assert calls == []


def test_explicit_credit_rejection_propagates_before_source_read(monkeypatch):
    action = types.SimpleNamespace(context=types.SimpleNamespace(
        limits={"max_row_bytes": 1000, "max_batch_bytes": 4096}))
    monkeypatch.setattr(graph, "current_action", lambda: action)
    monkeypatch.setattr(operator_precheck, "precheck_plan_state", lambda plan: None)
    reached = []
    monkeypatch.setattr(graph, "freeze_source",
                        lambda action, source: reached.append(True))
    monkeypatch.setattr(reservation, "resolve_prefetch_credit", _sample([]), raising=False)
    # Explicit credit rows=64 exceeds the admitted ceiling (slots=4): fail-closed,
    # no max-expansion (RESPONSE.md #6/Q43, CRITICAL-REVISION.md:130/136).
    with pytest.raises(ReservationError) as exc:
        graph.run_local_graph(_spec(prefetch_slots=4, credit_budget={"rows": 64, "bytes": 8 * 1024 ** 2}))
    assert exc.value.code == "BUDGET_EXCEEDED"
    assert reached == []   # rejected before any source read


def test_default_credit_tightens_to_admitted_grant():
    # The contract the wiring relies on: default credit 64 tightens to the slot
    # ceiling 4, then derive_prefetch(4, max_pending_floor=4) -- never expands.
    calls = []
    resolve = _sample(calls)
    resolved = resolve(4, inflight_bytes=4096, credit_budget={"rows": 64, "bytes": 8 * 1024 ** 2},
                       credit_explicit=False)
    assert resolved["credit_budget"]["rows"] == 4
    assert resolved["prefetch"] == derive_prefetch(4, max_pending_floor=4)


def _func(name):
    tree = ast.parse(_SRC)
    return next(n for n in tree.body if isinstance(n, ast.FunctionDef) and n.name == name)


def test_wiring_consumes_both_resolved_fields_and_lazy_imports():
    fn = _func("run_local_graph")
    bound = next(s for s in fn.body if isinstance(s, ast.If)
                 and isinstance(s.test, ast.Compare)
                 and isinstance(s.test.left, ast.Name) and s.test.left.id == "prefetch_slots")
    imported = [n for n in ast.walk(bound) if isinstance(n, ast.ImportFrom)
                and any(a.name == "resolve_prefetch_credit" for a in n.names)]
    assert imported, "bound path must lazy-import resolve_prefetch_credit"
    assigned = {t.id for n in ast.walk(bound) if isinstance(n, ast.Assign)
                for t in n.targets if isinstance(t, ast.Name)}
    assert {"prefetch", "budget"} <= assigned, "both resolved fields must be consumed"


def test_recovery_byte_grant_guard_is_present():
    fn = _func("run_local_graph")
    guard = [s for s in fn.body if isinstance(s, ast.If)
             and isinstance(s.test, ast.Compare)
             and isinstance(s.test.left, ast.Name) and s.test.left.id == "restore"
             and isinstance(s.test.ops[0], ast.IsNot)]
    assert guard, "recovery must verify the sealed worker inflight byte grant"
    text = ast.dump(guard[0])
    assert "inflight_bytes" in text and "stage_owners" in text
