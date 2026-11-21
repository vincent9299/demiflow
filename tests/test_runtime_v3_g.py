"""Lane G regression gate against the integrated v3 contracts.

DTO construction uses the delivered closed contracts. Adapter tests patch only
explicit cross-module call boundaries for their unit assertions. Source and
native queue tests retain their real readers, journal/HTTP/SQLite paths where
originally exercised. Carrier doubles are unit evidence, not Local/Ray or
end-to-end acceptance evidence.
"""
from __future__ import annotations

import os
import subprocess
import sys
from dataclasses import dataclass, replace
from pathlib import Path
from types import MappingProxyType

import pytest

_TREE = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(_TREE / "tests"))
from _runtime_v3_g_fakes import assert_tree_binding
from demiflow.execution.contracts.dataflow import ActionProgram, ActionRequest, Bounds
from demiflow.execution.contracts.dataflow_errors import DataflowError
from demiflow.planning.dataflow_policy import resolve_runtime_policy

def Bounds(**overrides):
    return replace(resolve_runtime_policy({}, {}, provider="local")[1], **overrides)

from demiflow.data import plan as ops  # noqa: E402
from demiflow.data.dataflow_operation import NativeOperation  # noqa: E402
from demiflow.data.plan import CallableSpec, LogicalOp, LogicalPlan  # noqa: E402
from demiflow.data.sources import ItemsSource, RangeSource  # noqa: E402
from demiflow.execution.local_kernel import KeyedSource, PlanInput, UnionSource  # noqa: E402
from demiflow.execution.plan_facts import (  # noqa: E402
    execution_instances,
    operation_capability,
    operation_facts,
)
from demiflow.execution.recovery_contract import RuntimeContractError  # noqa: E402
from demiflow.planning.dataflow_compile import (  # noqa: E402
    _LiteralEmptySource,
    _operation_semantic_facts,
    _source_kind,
    _source_semantic_facts,
    bind_occurrences,
    capture_graph,
    compile_kernel_spec,
    expand_operation_inputs,
    expand_relational_inputs,
    lower_action,
    validate_graph,
)
from demiflow.planning.dataflow_registry import (  # noqa: E402
    operator_registry,
    source_registry,
    terminal_registry,
)


def _bind():
    return assert_tree_binding()


def _fn(row):
    return row


def _pred(row):
    return True


def _map_plan():
    return LogicalPlan((ops.MapOp(callable=CallableSpec.create(_fn)),))


def _req(source, plan, kind="take", args=None, action_id="act", sink=None):
    return ActionRequest(
        kind=kind, source=source, plan=plan,
        arguments=args if args is not None else {"limit": 2},
        sink=sink, programs=(), provider="local",
        action=type("Action", (), {"action_id": action_id})(),
    )


def _capture(source, plan, **kw):
    return capture_graph(_req(source, plan, **kw), bounds=Bounds()).graph


def _expect_error(code, fn):
    with pytest.raises(DataflowError) as exc:
        fn()
    assert exc.value.code == code, (exc.value.code, exc.value.message)


def test_same_named_prompt_uses_generic_semantic_fields():
    @dataclass(frozen=True)
    class OperatorLLMMapOp(LogicalOp):
        text: str
        node_id: str

    op = OperatorLLMMapOp("row value", "execution-coordinate")
    assert _operation_semantic_facts(op) == {
        "type": f"{type(op).__module__}.{type(op).__qualname__}",
        "fields": {"text": "row value"},
    }


@pytest.mark.parametrize("project, expected", [
    (_source_kind, "literal_empty"),
    (_source_semantic_facts, {"kind": "literal_empty"}),
])
def test_literal_empty_source_requires_the_compiler_type(project, expected):
    assert project(_LiteralEmptySource()) == expected
    same_named_subclass = type("_LiteralEmptySource", (_LiteralEmptySource,), {})
    _expect_error("INVALID_CONTRACT", lambda: project(same_named_subclass()))


def _enumeration_1025():
    nodes = [{"id": "root", "role": "stage"}] + [
        {"id": str(i), "role": "stage"} for i in range(1025)]
    edges = [{"from": str(i), "to": "root", "port": str(i)} for i in range(1025)]
    return nodes, edges, ["root"]


# ---------------------------------------------------------------------------
# G001 capture_graph
# ---------------------------------------------------------------------------
def test_g001():
    _bind()
    shared = ItemsSource(items=({"x": 1},))
    diamond = UnionSource(inputs=(PlanInput(shared, _map_plan()), PlanInput(shared, _map_plan())))
    g = _capture(diamond, LogicalPlan())
    assert len([n for n in g.nodes if n.kind == "source"]) == 1  # shared prefix once
    assert len([n for n in g.nodes if n.kind == "transform"]) == 2  # two independent maps

    independent = UnionSource(inputs=(
        PlanInput(ItemsSource(items=({"x": 1},)), _map_plan()),
        PlanInput(ItemsSource(items=({"x": 2},)), _map_plan()),
    ))
    g2 = _capture(independent, LogicalPlan())
    assert len([n for n in g2.nodes if n.kind == "transform"]) == 2  # same fn, still two

    cyclic = UnionSource(inputs=())
    object.__setattr__(cyclic, "inputs", (PlanInput(cyclic, LogicalPlan()),))
    _expect_error("INVALID_CONTRACT", lambda: _capture(cyclic, LogicalPlan()))


# ---------------------------------------------------------------------------
# G002 bind_occurrences
# ---------------------------------------------------------------------------
_HASHSEED_PROGRAM = r"""
import sys
sys.path.insert(0, "tests")
from demiflow.execution.contracts.dataflow import ActionRequest, ActionProgram
from demiflow.planning.dataflow_policy import resolve_runtime_policy
def Bounds(): return resolve_runtime_policy({}, {}, provider="local")[1]
from demiflow.data.sources import ItemsSource
from demiflow.data.plan import LogicalPlan, MapOp, CallableSpec
from demiflow.planning.dataflow_compile import capture_graph
def fn(row): return row
src = ItemsSource(items=({"x": 1},))
plan = LogicalPlan((MapOp(callable=CallableSpec.create(fn)),))
req = ActionRequest(kind="take", source=src, plan=plan, arguments={"limit": 1},
                    sink=None, programs=(), provider="local",
                    action=type("A", (), {"action_id": "fixed"})())
g = capture_graph(req, bounds=Bounds()).graph
print(g.dag_id)
print(",".join(n.occurrence_id for n in g.nodes))
"""


def test_g002():
    _bind()
    left_a, right_a = ItemsSource(items=({"k": 1},)), ItemsSource(items=({"k": 2},))
    join1 = KeyedSource(kind="join", left=PlanInput(left_a, LogicalPlan()), on=("k",),
                        right=PlanInput(right_a, LogicalPlan()), right_on=("k",))
    join2 = KeyedSource(kind="join", left=PlanInput(right_a, LogicalPlan()), on=("k",),
                        right=PlanInput(left_a, LogicalPlan()), right_on=("k",))
    d1 = _capture(join1, LogicalPlan()).dag_id
    d2 = _capture(join2, LogicalPlan()).dag_id
    assert d1 != d2  # left/right swap changes identity

    env = dict(os.environ, PYTHONPATH=str(_TREE))
    outs = []
    for seed in ("1", "2"):
        proc = subprocess.run(
            [sys.executable, "-B", "-c", _HASHSEED_PROGRAM],
            cwd=str(_TREE), env=dict(env, PYTHONHASHSEED=seed),
            capture_output=True, text=True,
        )
        assert proc.returncode == 0, proc.stderr
        outs.append(proc.stdout.strip())
    assert outs[0] == outs[1]  # serialization rebuild + hashseed independent identity

    g = _capture(join1, LogicalPlan())
    rebinding = bind_occurrences(g.nodes, g.edges, action_id="act")
    assert tuple(n.occurrence_id for n in rebinding) == tuple(n.occurrence_id for n in g.nodes)


# ---------------------------------------------------------------------------
# G003 compile_kernel_spec
# ---------------------------------------------------------------------------
def test_g003():
    _bind()
    constructions = []

    class SideEffectActor:
        def __init__(self):
            constructions.append(1)

        def __call__(self, row):
            return row

    spec = compile_kernel_spec(
        ops.MapOp(callable=CallableSpec.create(SideEffectActor)),
        program_ref=None, bounds=Bounds(),
    )
    assert constructions == []  # side-effect constructor count stays zero
    assert spec.kind == "MapOp"
    assert spec.state == "auto_json" and spec.effect == "pure"  # class factories own per-instance state

    class UnregisteredOp(LogicalOp):
        pass

    _expect_error("UNSUPPORTED_CAPABILITY",
                  lambda: compile_kernel_spec(UnregisteredOp(), program_ref=None, bounds=Bounds()))


# ---------------------------------------------------------------------------
# G004 expand_relational_inputs
# ---------------------------------------------------------------------------
def test_g004():
    _bind()
    same = ItemsSource(items=({"k": 1},))
    join = KeyedSource(kind="join", left=PlanInput(same, LogicalPlan()), on=("k",),
                       right=PlanInput(same, LogicalPlan()), right_on=("k",))
    bindings = expand_relational_inputs(join, path=(), bounds=Bounds())
    assert tuple(b.port for b in bindings) == ("left", "right")  # same schema, distinct ports

    union = UnionSource(inputs=tuple(PlanInput(same, LogicalPlan()) for _ in range(3)))
    ub = expand_relational_inputs(union, path=(), bounds=Bounds())
    assert tuple(b.port for b in ub) == ("0", "1", "2")  # multiplicity preserved
    assert len(ub) == 3


# ---------------------------------------------------------------------------
# G005 validate_graph
# ---------------------------------------------------------------------------
def test_g005():
    _bind()
    g = _capture(ItemsSource(items=({"x": 1},)), _map_plan())
    validate_graph(g, bounds=Bounds())  # valid baseline

    dangling = replace(g, edges=g.edges + (
        replace(g.edges[0], producer="f" * 64),))
    _expect_error("INVALID_CONTRACT", lambda: validate_graph(dangling, bounds=Bounds()))

    dup = replace(g, nodes=g.nodes + (g.nodes[0],))
    _expect_error("INVALID_CONTRACT", lambda: validate_graph(dup, bounds=Bounds()))

    no_source = replace(g, sources=())
    _expect_error("INVALID_CONTRACT", lambda: validate_graph(no_source, bounds=Bounds()))

    deep_plan = LogicalPlan(tuple(ops.MapOp(callable=CallableSpec.create(_fn)) for _ in range(8)))
    deep = _capture(ItemsSource(items=({"x": 1},)), deep_plan)
    _expect_error("BUDGET_EXCEEDED", lambda: validate_graph(deep, bounds=Bounds(max_depth=2)))


# ---------------------------------------------------------------------------
# G006 lower_action
# ---------------------------------------------------------------------------
def test_g006():
    _bind()
    zero_compiled = lower_action(_req(ItemsSource(items=({"x": 1},)), _map_plan(), args={"limit": 0}),
                        bounds=Bounds())
    zero = zero_compiled.graph
    source_declarations = [d.source_spec for d in zero_compiled.declarations if d.source_spec is not None]
    assert len(source_declarations) == 1 and source_declarations[0].items == ()
    assert not [n for n in zero.nodes if n.kind == "transform"]
    assert not [n for n in zero.nodes if n.kernel.kind in {"prompt", "embedding", "agent"}]

    from demiflow import data
    def unread_source():
        raise AssertionError("take(0) must never invoke the source")
        yield {"x": 1}
    with data.local_execution(worker_mode="thread"):
        assert data.from_iter(unread_source).take(0) == []

    schema = lower_action(_req(ItemsSource(items=({"x": 1},)), LogicalPlan(), kind="schema",
                               args={"fetch_if_missing": False}), bounds=Bounds()).graph
    assert schema.terminals  # schema compiles without executing the source

    _expect_error("INVALID_CONTRACT",
                  lambda: lower_action(_req(ItemsSource(items=({"x": 1},)), LogicalPlan(),
                                            kind="write_datasink", sink=None), bounds=Bounds()))


# ---------------------------------------------------------------------------
# G007 operator_registry
# ---------------------------------------------------------------------------
def test_g007():
    _bind()
    registry = operator_registry()
    assert isinstance(registry, MappingProxyType)
    public_ops = [name for name in dir(ops)
                  if isinstance(getattr(ops, name), type)
                  and issubclass(getattr(ops, name), LogicalOp)
                  and getattr(ops, name) is not LogicalOp
                  and getattr(ops, name).__module__ == ops.__name__]
    for name in public_ops:
        assert name in registry, f"public op missing from registry: {name}"
    with pytest.raises(TypeError):
        registry["MapOp"] = None  # no runtime dynamic registration bypass
    assert operator_registry() is registry  # single immutable singleton
    _expect_error("UNSUPPORTED_CAPABILITY",
                  lambda: compile_kernel_spec(NativeOperation(
                      kind="not_registered_kind", configuration={}, bindings={},
                      factory=CallableSpec.create(_fn)), program_ref=None, bounds=Bounds()))


# ---------------------------------------------------------------------------
# G008 source_registry
# ---------------------------------------------------------------------------
def test_g008():
    _bind()
    registry = source_registry()
    assert isinstance(registry, MappingProxyType)
    expected = {"items", "range", "file", "lance", "sql", "materialized", "iterable",
                "datasource", "queue", "queue_history", "join", "reduce", "groups", "union"}
    assert expected.issubset(registry.keys())
    for kind in ("items", "range", "file", "lance", "sql", "materialized", "iterable",
                 "datasource", "queue", "queue_history", "join", "reduce", "groups", "union"):
        assert registry[kind].reader_symbol  # every read/from entry has a reader kind


# ---------------------------------------------------------------------------
# G009 terminal_registry
# ---------------------------------------------------------------------------
def test_g009():
    _bind()
    registry = terminal_registry()
    assert isinstance(registry, MappingProxyType)
    assert registry["write_datasink"].result_kind == "write_result"
    assert registry["take"].result_kind == "rows"
    assert registry["write_datasink"].adapter_symbol != registry["take"].adapter_symbol
    _expect_error("INVALID_CONTRACT",
                  lambda: lower_action(_req(ItemsSource(items=({"x": 1},)), LogicalPlan(),
                                            kind="unknown_terminal_xyz"), bounds=Bounds()))


# ---------------------------------------------------------------------------
# G010 expand_operation_inputs
# ---------------------------------------------------------------------------
def test_g010():
    _bind()
    prefix = ItemsSource(items=({"p": 1},))
    dataset = type("Dataset", (), {"_source": prefix, "_plan": LogicalPlan()})()
    op = ops.StreamPrependOp(callable=CallableSpec.create(_fn), source_dataset=dataset,
                             max_rows=7, max_row_bytes=1024)
    bindings = expand_operation_inputs(op, path=(), bounds=Bounds())
    assert tuple(b.port for b in bindings) == ("prefix",)
    assert bindings[0].source is prefix  # prefix is a real port, not a nested worker action
    assert op.max_rows == 7 and op.max_row_bytes == 1024  # limits preserved, not truncated

    shared = UnionSource(inputs=(PlanInput(prefix, LogicalPlan((op,)),),
                                 PlanInput(prefix, LogicalPlan((op,)),)))
    g = _capture(shared, LogicalPlan())
    assert len([n for n in g.nodes if n.kind == "source"]) == 1  # shared prefix reused once

    _expect_error("BUDGET_EXCEEDED",
                  lambda: expand_operation_inputs(op, path=tuple(str(i) for i in range(70)),
                                                  bounds=Bounds(max_depth=64)))


# ---------------------------------------------------------------------------
# G011 operation_facts
# ---------------------------------------------------------------------------
def test_g011():
    _bind()
    legacy = operation_facts(ops.MapOp(callable=CallableSpec.create(_fn)))
    assert legacy["kind"] == "map"
    assert legacy["arguments"]["inputs"] is None

    for direction in (False, True, (False, True)):
        keys = ("x", "y")
        facts = operation_facts(ops.SortOp(keys=keys, descending=direction))
        expected = [direction] * len(keys) if type(direction) is bool else list(direction)
        assert facts["kind"] == "sort"
        assert facts["arguments"] == {"key": list(keys), "descending": expected, "boundaries": None}
    class UnknownOp(LogicalOp):
        pass
    with pytest.raises(RuntimeContractError):
        operation_facts(UnknownOp())  # only registered capabilities acquire facts
    for direction in ((True,), (True, 1)):
        with pytest.raises(RuntimeContractError):
            operation_facts(ops.SortOp(keys=("x", "y"), descending=direction))

    nodes, edges, actions = _enumeration_1025()
    with pytest.raises(ValueError):
        execution_instances(nodes, edges, actions)  # 1025 enumeration budget preserved

    native = NativeOperation(kind="raw_fetch", configuration={"url": "u", "nested": {"a": [1, 2]}},
                             bindings={"out": "o"}, factory=CallableSpec.create(_fn))
    facts = operation_facts(native)
    assert facts["kind"] == "raw_fetch"
    assert facts["native"] is True
    assert facts["arguments"]["configuration"] == {"url": "u", "nested": {"a": [1, 2]}}
    assert type(facts["arguments"]["configuration"]["nested"]) is dict  # deep-thawed, not proxy
    assert facts["arguments"]["bindings"] == {"out": "o"}
    assert "factory" in facts["code_identity"]


# ---------------------------------------------------------------------------
# G012 operation_capability
# ---------------------------------------------------------------------------
def test_g012():
    _bind()
    legacy = operation_capability(ops.MapOp(callable=CallableSpec.create(_fn)))
    assert legacy["kind"] == "map" and legacy["driver"] == "pull"

    native = NativeOperation(kind="prompt", configuration={}, bindings={},
                             factory=CallableSpec.create(_fn))
    cap = operation_capability(native)
    assert cap["kind"] == "prompt"
    assert cap["driver"] == "stream" and cap["native"] is True
    assert operator_registry()["prompt"].kind == cap["kind"]  # same registry source

    nodes, edges, actions = _enumeration_1025()
    with pytest.raises(ValueError):
        execution_instances(nodes, edges, actions)


# ---------------------------------------------------------------------------
# G013 NativeOperation
# ---------------------------------------------------------------------------
def test_g013():
    _bind()
    factory = CallableSpec.create(_fn)
    op = NativeOperation(kind="raw_fetch", configuration={"a": {"b": 1}},
                         bindings={"x": [1, 2]}, factory=factory)
    assert isinstance(op.configuration, MappingProxyType)
    assert isinstance(op.configuration["a"], MappingProxyType)
    with pytest.raises(TypeError):
        op.configuration["a"]["b"] = 2  # inner immutable
    with pytest.raises(TypeError):
        NativeOperation(kind="k", configuration={}, bindings={}, factory=factory, extra=1)

    empty = NativeOperation(kind="k", configuration={}, bindings={}, factory=factory)
    assert empty.configuration == {}

    with pytest.raises(ValueError):
        NativeOperation(kind="", configuration={}, bindings={}, factory=factory)
    with pytest.raises(ValueError):
        NativeOperation(kind="x" * 129, configuration={}, bindings={}, factory=factory)
    with pytest.raises(TypeError):
        NativeOperation(kind="k", configuration={}, bindings={}, factory=_fn)
    with pytest.raises(ValueError):
        NativeOperation(kind="k", configuration={"t": (1, 2)}, bindings={}, factory=factory)
    with pytest.raises(ValueError):
        NativeOperation(kind="k", configuration={"n": float("nan")}, bindings={}, factory=factory)
    deep = {"level": 0}
    cursor = deep
    for _ in range(40):
        cursor["level"] = {}
        cursor = cursor["level"]
    with pytest.raises(ValueError):
        NativeOperation(kind="k", configuration=deep, bindings={}, factory=factory)