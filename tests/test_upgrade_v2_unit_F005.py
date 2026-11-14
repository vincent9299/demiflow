"""F005 verify_preserve: compile_graph_logical_dag static fan-out identity.

Body byte-identical to baseline bbd93ff (SHA
05ef5fd8b231b7566174ba04d97055e99ef25c531f3f828757b916592a17c1e3). Locks: the
program mapping keys (group_id/role/reader_id/operators), a shared ancestor kept
once, same-function targets keeping distinct execution instances, and a dag_id
that depends only on semantic declarations (never physical layout).
"""
from __future__ import annotations

import ast
import hashlib
from pathlib import Path

import demiflow
from demiflow.data.plan import CallableSpec, LogicalPlan, MapOp
from demiflow.data.sources import ItemsSource
from demiflow.planning.dag import dag_id_for, validate_logical_dag
from demiflow.planning.graph_dag import compile_graph_logical_dag

_BASELINE_SHA = "05ef5fd8b231b7566174ba04d97055e99ef25c531f3f828757b916592a17c1e3"
_PROGRAM_KEYS = {"group_id", "role", "reader_id", "operators"}
_OPERATOR_KEYS = {"program_index", "logical_node_id", "occurrence_id"}


def _fn(row):
    return row


def _plan(n=1):
    return LogicalPlan(tuple(MapOp(CallableSpec.create(_fn)) for _ in range(n)))


def _source():
    return ItemsSource(items=({"id": 1}, {"id": 2}))


def _compile(ancestor_plan, targets, *, action_id="act"):
    return compile_graph_logical_dag(
        action_id=action_id, source=_source(),
        ancestor={"plan": ancestor_plan}, targets=targets)


def test_module_source_is_worktree():
    assert Path(demiflow.__file__).resolve().is_relative_to(Path(__file__).resolve().parents[1])


def test_function_body_matches_baseline_sha():
    path = Path(demiflow.__file__).parent / "planning" / "graph_dag.py"
    source = path.read_text()
    node = next(n for n in ast.walk(ast.parse(source))
                if isinstance(n, ast.FunctionDef) and n.name == "compile_graph_logical_dag")
    assert hashlib.sha256(ast.get_source_segment(source, node).encode()).hexdigest() == _BASELINE_SHA


def test_program_mapping_keys_and_roles():
    plan = _plan(2)
    targets = [{"plan": _plan(1), "reader_id": "t1"}, {"plan": _plan(1), "reader_id": "t2"}]
    dag, programs = _compile(plan, targets)
    assert isinstance(programs, list) and len(programs) == 3
    for prog in programs:
        assert set(prog) == _PROGRAM_KEYS
        for op in prog["operators"]:
            assert set(op) == _OPERATOR_KEYS
    assert programs[0]["group_id"] == "ancestor"
    assert programs[0]["role"] == "ancestor"
    assert programs[0]["reader_id"] is None
    assert programs[1]["group_id"] == "target-t1"
    assert programs[1]["role"] == "target"
    assert programs[1]["reader_id"] == "t1"
    assert programs[2]["group_id"] == "target-t2"


def test_operator_counts_match_local_plans():
    plan = _plan(3)
    targets = [{"plan": _plan(2), "reader_id": "t1"}]
    _, programs = _compile(plan, targets)
    assert len(programs[0]["operators"]) == 3
    assert len(programs[1]["operators"]) == 2
    assert [op["program_index"] for op in programs[0]["operators"]] == [0, 1, 2]


def test_same_function_distinct_targets_not_collapsed():
    plan = _plan(1)
    targets = [{"plan": plan, "reader_id": "t1"}, {"plan": plan, "reader_id": "t2"}]
    dag, programs = _compile(plan, targets)
    t1 = programs[1]["operators"][0]
    t2 = programs[2]["operators"][0]
    assert t1["logical_node_id"] != t2["logical_node_id"]
    assert t1["occurrence_id"] != t2["occurrence_id"]


def test_shared_ancestor_executed_once():
    plan = _plan(2)
    targets = [{"plan": _plan(1), "reader_id": "t1"}, {"plan": _plan(1), "reader_id": "t2"}]
    dag, programs = _compile(plan, targets)
    assert len(programs[0]["operators"]) == 2
    occ = [n["occurrence_id"] for n in dag["nodes"]]
    assert len(occ) == len(set(occ))
    assert dag["shared_ancestors"]


def test_dag_id_stable_for_same_semantic_plan():
    plan = _plan(2)
    targets = [{"plan": _plan(1), "reader_id": "t1"}, {"plan": _plan(1), "reader_id": "t2"}]
    d1, p1 = _compile(plan, targets)
    d2, p2 = _compile(plan, targets)
    assert d1["dag_id"] == d2["dag_id"]
    assert d1 == d2
    assert [p["group_id"] for p in p1] == [p["group_id"] for p in p2]


def test_dag_id_independent_of_physical_layout():
    # The compiler takes only semantic declarations; worker counts/budgets are
    # downstream physical facts and cannot drift the logical dag_id.
    plan = _plan(2)
    targets = [{"plan": _plan(1), "reader_id": "t1"}]
    baseline, _ = _compile(plan, targets)
    again, _ = _compile(plan, targets)
    assert baseline["dag_id"] == again["dag_id"] == dag_id_for(baseline)


def test_logical_dag_is_valid_v1():
    plan = _plan(2)
    targets = [{"plan": _plan(1), "reader_id": "t1"}, {"plan": _plan(1), "reader_id": "t2"}]
    dag, _ = _compile(plan, targets)
    assert dag["schema_version"] == "demiflow.logical_dag.v1"
    assert dag["dag_id"] == dag_id_for(dag)
    validate_logical_dag(dag)


def test_single_target_no_shared_ancestor():
    plan = _plan(1)
    targets = [{"plan": _plan(1), "reader_id": "only"}]
    dag, programs = _compile(plan, targets)
    assert len(programs) == 2
    assert dag["shared_ancestors"] == []


def test_empty_target_plan_keeps_terminal():
    plan = _plan(1)
    targets = [{"plan": LogicalPlan(), "reader_id": "t1"}]
    dag, programs = _compile(plan, targets)
    assert programs[1]["operators"] == []
    assert len(dag["terminals"]) == 1
    validate_logical_dag(dag)