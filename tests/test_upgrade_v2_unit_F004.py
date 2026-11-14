"""F004 unit tests: compile_dataset_logical_dag recursive PlanInput expansion.

Locks the C08 contract: complete input_path, stable program_id, local sealed
identities for verify_loaded_plan, full-graph occurrence bindings, no collapse of
a shared object at two positions, and structural cycle/budget pre-checks. Real
producers only (RangeSource/ItemsSource + MapOp); no business read, no mock.
"""
from __future__ import annotations
from pathlib import Path

import demiflow
from demiflow.data.plan import CallableSpec, LogicalPlan, MapOp
from demiflow.data.sources import ItemsSource, RangeSource
from demiflow.execution.local_kernel import KeyedSource, PlanInput, UnionSource
from demiflow.execution.plan_interpreter import PlanContractError
from demiflow.execution.recovery_contract import RecoveryContractError, identity
from demiflow.planning.dag import _operation_semantics, compile_logical_dag
from demiflow.planning.dataset_dag import ProgramSlice, compile_dataset_logical_dag


def _op(factor=2):
    def fn(row):
        return {"v": row["id"] * factor}
    return MapOp(CallableSpec.create(fn))


def _plan(*ops):
    return LogicalPlan(tuple(ops))


def _compile(source, plan, *, action_id="act", terminal_kind="take_all", sink_ids=("out",)):
    return compile_dataset_logical_dag(action_id=action_id, source=source, plan=plan,
                                       terminal_kind=terminal_kind, sink_ids=sink_ids)


def test_module_source_is_worktree():
    assert Path(demiflow.__file__).resolve().is_relative_to(Path(__file__).resolve().parents[1])


def test_linear_program_structure_and_local_identities():
    plan = _plan(_op(), _op(3))
    dag, slices = _compile(RangeSource(10), plan)
    assert len(slices) == 1
    s = slices[0]
    assert isinstance(s, ProgramSlice)
    assert s.input_path == ()
    assert s.program_id == identity(["act", []])
    # source + 2 ops + terminal
    assert len(dag["nodes"]) == 4
    assert len(dag["edges"]) == 3
    assert len(dag["sources"]) == 1 and len(dag["terminals"]) == 1
    # sealed_operators are the LOCAL linear compile identities, in order
    recomputed = [identity([_operation_semantics(o)[0], i]) for i, o in enumerate(plan.operations)]
    assert [n["logical_node_id"] for n in s.sealed_operators] == recomputed
    assert len(s.operator_bindings) == 2
    occ = {n["occurrence_id"] for n in dag["nodes"]}
    for j, b in enumerate(s.operator_bindings):
        assert b["program_index"] == j
        assert b["occurrence_id"] in occ


def test_sealed_operators_satisfy_verify_loaded_plan_recomputation():
    # The exact check verify_loaded_plan performs against sealed logical_node_ids.
    plan = _plan(_op(), _op(5), _op(7))
    _, slices = _compile(RangeSource(4), plan)
    sealed = [n["logical_node_id"] for n in slices[0].sealed_operators]
    recomputed = [identity([_operation_semantics(op)[0], index])
                  for index, op in enumerate(plan.operations)]
    assert recomputed == sealed


def test_linear_graph_matches_frozen_compiler_shape():
    plan = _plan(_op())
    src = RangeSource(6)
    dag, _ = _compile(src, plan)
    frozen = compile_logical_dag(action_id="act", source=src, operations=plan.operations,
                                 terminal_kind="take_all", sink_ids=("out",))
    assert len(dag["nodes"]) == len(frozen["nodes"])
    assert len(dag["edges"]) == len(frozen["edges"])
    assert [n["op_kind"] for n in dag["nodes"]] == [n["op_kind"] for n in frozen["nodes"]]


def test_join_paths_sources_and_fan_in_edges():
    left = PlanInput(RangeSource(5), _plan(_op()))
    right = PlanInput(RangeSource(7), _plan())
    keyed = KeyedSource("join", left, ("id",), right, ("id",))
    dag, slices = _compile(keyed, _plan(_op()))
    paths = {s.input_path for s in slices}
    assert paths == {(), ("source", "left"), ("source", "right")}
    # two leaf data sources; the keyed root is a composite, not a data source
    assert len(dag["sources"]) == 2
    by_path = {s.input_path: s for s in slices}
    assert by_path[("source", "left")].program_id == identity(["act", ["source", "left"]])
    assert by_path[("source", "right")].program_id == identity(["act", ["source", "right"]])
    # the keyed source node consumes both child tails (fan-in)
    root_source = next(n for n in dag["nodes"] if n["op_kind"] == "KeyedSource")
    assert root_source["input_ports"] == ["in"]
    incoming = [e for e in dag["edges"] if e["to"]["logical_node_id"] == root_source["logical_node_id"]]
    assert len(incoming) == 2


def test_union_paths_use_real_indices():
    u = UnionSource((PlanInput(RangeSource(3), _plan(_op())),
                     PlanInput(RangeSource(4), _plan()),
                     PlanInput(ItemsSource([{"id": 1}]), _plan(_op()))))
    dag, slices = _compile(u, _plan())
    paths = [s.input_path for s in slices]
    assert paths == [(), ("source", "inputs", 0), ("source", "inputs", 1), ("source", "inputs", 2)]
    assert len(dag["sources"]) == 3
    for i in range(3):
        s = next(x for x in slices if x.input_path == ("source", "inputs", i))
        assert s.program_id == identity(["act", ["source", "inputs", i]])


def test_nested_composite_paths():
    left = PlanInput(RangeSource(2), _plan(_op()))
    right = PlanInput(RangeSource(3), _plan())
    keyed = KeyedSource("join", left, ("id",), right, ("id",))
    nested = UnionSource((PlanInput(keyed, _plan(_op())), PlanInput(RangeSource(4), _plan())))
    dag, slices = _compile(nested, _plan())
    paths = {s.input_path for s in slices}
    assert ("source", "inputs", 0, "source", "left") in paths
    assert ("source", "inputs", 0, "source", "right") in paths
    assert ("source", "inputs", 1) in paths
    assert len(slices) == 5


def test_keyed_without_right_has_only_left_child():
    left = PlanInput(RangeSource(9), _plan(_op()))
    grouped = KeyedSource("groups", left, ("id",))
    dag, slices = _compile(grouped, _plan())
    paths = {s.input_path for s in slices}
    assert paths == {(), ("source", "left")}
    assert len(dag["sources"]) == 1


def test_determinism_same_plan_same_identities():
    src = RangeSource(10)
    plan = _plan(_op())
    d1, s1 = _compile(src, plan, action_id="stable")
    d2, s2 = _compile(src, plan, action_id="stable")
    assert d1["dag_id"] == d2["dag_id"]
    assert [x.program_id for x in s1] == [x.program_id for x in s2]
    assert d1 == d2


def test_action_id_changes_program_id():
    src = RangeSource(3)
    _, a = _compile(src, _plan(), action_id="a1")
    _, b = _compile(src, _plan(), action_id="a2")
    assert a[0].program_id != b[0].program_id


def test_shared_object_two_positions_not_collapsed():
    shared = PlanInput(RangeSource(3), _plan(_op()))
    u = UnionSource((shared, shared))
    dag, slices = _compile(u, _plan(), action_id="dup")
    assert len(slices) == 3
    assert len({s.program_id for s in slices}) == 3
    # identical local sealed identities, distinct full-graph occurrences
    assert slices[1].sealed_operators == slices[2].sealed_operators
    assert (slices[1].operator_bindings[0]["occurrence_id"]
            != slices[2].operator_bindings[0]["occurrence_id"])
    nids = [n["logical_node_id"] for n in dag["nodes"]]
    oids = [n["occurrence_id"] for n in dag["nodes"]]
    assert len(nids) == len(set(nids))
    assert len(oids) == len(set(oids))


def test_operator_bindings_reference_real_full_graph_nodes():
    left = PlanInput(RangeSource(2), _plan(_op(), _op(3)))
    right = PlanInput(RangeSource(3), _plan(_op()))
    keyed = KeyedSource("join", left, ("id",), right, ("id",))
    dag, slices = _compile(keyed, _plan(_op()))
    occ = {n["occurrence_id"] for n in dag["nodes"]}
    for s in slices:
        for j, b in enumerate(s.operator_bindings):
            assert b["program_index"] == j
            assert b["occurrence_id"] in occ
        # sealed local ids must NOT be the renumbered full-graph ids
        full = {b["occurrence_id"] for b in s.operator_bindings}
        local_occ = {n["occurrence_id"] for n in s.sealed_operators}
        assert full.isdisjoint(local_occ)


def test_empty_root_plan():
    dag, slices = _compile(RangeSource(5), _plan())
    assert len(slices) == 1
    assert slices[0].sealed_operators == ()
    assert slices[0].operator_bindings == ()
    # source -> terminal
    assert len(dag["nodes"]) == 2 and len(dag["edges"]) == 1


def test_empty_subprogram_feeds_composite_source():
    u = UnionSource((PlanInput(RangeSource(2), _plan()), PlanInput(RangeSource(3), _plan())))
    dag, slices = _compile(u, _plan())
    assert all(s.sealed_operators == () for s in slices)
    assert len(dag["sources"]) == 2


def test_borrowed_source_and_plan_are_the_actual_declarations():
    src = RangeSource(8)
    plan = _plan(_op())
    _, slices = _compile(src, plan)
    assert slices[0].source is src
    assert slices[0].plan is plan


def test_leaf_and_composite_source_ports():
    left = PlanInput(RangeSource(2), _plan())
    keyed = KeyedSource("groups", left, ("id",))
    dag, _ = _compile(keyed, _plan())
    root_node = next(n for n in dag["nodes"] if n["op_kind"] == "KeyedSource")
    leaf_node = next(n for n in dag["nodes"] if n["op_kind"] == "RangeSource")
    assert root_node["input_ports"] == ["in"]      # composite consumes children
    assert leaf_node["input_ports"] == []          # true data source


def test_cycle_rejected_before_compilation():
    cyc = UnionSource(())
    object.__setattr__(cyc, "inputs", (PlanInput(cyc, _plan()),))
    try:
        _compile(cyc, _plan())
    except PlanContractError as exc:
        assert exc.reason == "invalid_contract"
    else:
        raise AssertionError("cycle was not rejected")


def test_keyed_cycle_rejected():
    left = PlanInput(RangeSource(2), _plan())
    keyed = KeyedSource("join", left, ("id",))
    object.__setattr__(left, "source", keyed)   # left.source -> keyed -> left ...
    try:
        _compile(keyed, _plan())
    except PlanContractError as exc:
        assert exc.reason == "invalid_contract"
    else:
        raise AssertionError("keyed cycle was not rejected")


def test_program_budget_rejected():
    big = UnionSource(tuple(PlanInput(RangeSource(1), _plan()) for _ in range(5000)))
    try:
        _compile(big, _plan())
    except PlanContractError as exc:
        assert exc.reason == "invalid_contract"
    else:
        raise AssertionError("program budget was not enforced")


def test_depth_budget_rejected():
    deep = RangeSource(1)
    for _ in range(40):
        deep = UnionSource((PlanInput(deep, _plan()),))
    try:
        _compile(deep, _plan())
    except PlanContractError as exc:
        assert exc.reason == "invalid_contract"
    else:
        raise AssertionError("depth budget was not enforced")


def test_invalid_source_type_rejected():
    try:
        _compile(object(), _plan())
    except PlanContractError as exc:
        assert exc.reason == "invalid_contract"
    else:
        raise AssertionError("non-SourcePlan was accepted")


def test_invalid_plan_type_rejected():
    try:
        _compile(RangeSource(2), object())
    except PlanContractError as exc:
        assert exc.reason == "invalid_contract"
    else:
        raise AssertionError("non-LogicalPlan was accepted")


def test_invalid_action_id_rejected():
    try:
        _compile(RangeSource(2), _plan(), action_id="bad id!")
    except RecoveryContractError:
        pass
    else:
        raise AssertionError("invalid action_id was accepted")


def test_logical_dag_is_valid_and_binds_dag_id():
    from demiflow.planning.dag import dag_id_for, validate_logical_dag
    left = PlanInput(RangeSource(2), _plan(_op()))
    keyed = KeyedSource("join", left, ("id",), PlanInput(RangeSource(3), _plan()), ("id",))
    dag, _ = _compile(keyed, _plan(_op()))
    assert dag["schema_version"] == "demiflow.logical_dag.v1"
    assert dag["dag_id"] == dag_id_for(dag)
    validate_logical_dag(dag)   # raises on any structural violation
