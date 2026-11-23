"""L040 · source_callable_roots — native source callable roots for the code pin.

Binds the source side of gap④: ``describe(source)`` records a reducer's own
source/bytecode/closure but never ``__globals__``, so the business helper a
reducer imports escaped the code pin. These tests use the REAL public producers
(``reduce_by_key`` / ``join`` / ``union`` / ``read_sql`` / ``from_iterator``) and
the REAL native source dataclasses — no stand-in source type, no stub.

Scope: root collection only (order, boundedness, passivity, fail-closed). The
module BFS itself is L041's; the pin/archive consequences are L042/L043's.
"""
import functools
import sys

import pytest

import demiflow.data as data
from demiflow.data.plan import CallableSpec, LogicalPlan, MapOp
from demiflow.data.sources import (
    DatasourceSource, FileSource, ItemsSource, IterableSource, LanceSource,
    MaterializedSource, RangeSource, SqlSource,
)
from demiflow.execution.seal.business_sources import source_callable_roots
from demiflow.execution.pipeline.local_kernel import KeyedSource, PlanInput, UnionSource
from demiflow.execution.recovery_contract import RecoveryContractError


@pytest.fixture(autouse=True)
def local_executor():
    """Real public local-execution scope; declaration only, no action is run."""
    with data.local_execution(workers=1, worker_mode="thread", partitions=1):
        yield


def _local(items=({"k": "a", "v": 1}, {"k": "b", "v": 2})):
    return data.from_items(list(items))


def _reducer(acc, row):
    return acc + row["v"]


def _other(acc, row):
    return acc


def _factory():
    return iter(())


def _connection():
    raise AssertionError("connection_factory must never be invoked by the pin")


def _plan_input(items=()):
    return PlanInput(ItemsSource(items=tuple(items)), LogicalPlan())


def test_reduce_by_key_reducer_is_a_root():
    graph = _local().reduce_by_key("k", _reducer)
    assert type(graph._source) is KeyedSource
    assert source_callable_roots(graph._source) == (_reducer,)


def test_reducer_arguments_are_roots_in_sorted_key_order():
    graph = _local().reduce_by_key("k", _reducer, initial=0,
                                   fn_kwargs={"zeta": _other, "alpha": _factory, "scalar": 7})
    roots = source_callable_roots(graph._source)
    assert roots == (_reducer, _factory, _other)


def test_initial_callable_is_a_root_and_plain_data_is_not():
    graph = _local().reduce_by_key("k", _reducer, initial=_other)
    assert source_callable_roots(graph._source) == (_reducer, _other)
    plain = _local().reduce_by_key("k", _reducer, initial={"rows": [1, 2]})
    assert source_callable_roots(plain._source) == (_reducer,)


def test_join_walks_left_then_right_without_a_reducer():
    left = _local().reduce_by_key("k", _reducer)
    right = _local({"k": "a", "v": 9}).reduce_by_key("k", _other)
    joined = left.join(right, on="k")
    assert type(joined._source) is KeyedSource
    assert source_callable_roots(joined._source) == (_reducer, _other)


def test_union_preserves_input_index_order():
    first = _local().reduce_by_key("k", _reducer)
    second = _local().reduce_by_key("k", _other)
    union = first.union(second, first)
    assert type(union._source) is UnionSource
    # Shared substructure is visited per position: no implicit folding (C08).
    assert source_callable_roots(union._source) == (_reducer, _other, _reducer)


def test_nested_union_order_is_depth_first_input_path():
    inner = _local().reduce_by_key("k", _reducer).union(
        _local().reduce_by_key("k", _other))
    outer = inner.union(_local().reduce_by_key("k", _factory))
    assert source_callable_roots(outer._source) == (_reducer, _other, _factory)


def test_sql_and_iterable_factories_are_roots():
    sql = data.read_sql("select 1", _connection)
    assert type(sql._source) is SqlSource
    assert source_callable_roots(sql._source) == (_connection,)
    from demiflow.data.api import DataAPI
    iterable = DataAPI().from_iter(_factory)
    assert type(iterable._source) is IterableSource
    assert source_callable_roots(iterable._source) == (_factory,)


def test_partial_and_bound_method_reduce_to_the_real_function():
    partial = _local().reduce_by_key("k", functools.partial(_reducer))
    assert source_callable_roots(partial._source) == (_reducer,)

    class Owner:
        def fold(self, acc, row):
            return acc

    bound = _local().reduce_by_key("k", Owner().fold)
    assert source_callable_roots(bound._source) == (Owner.fold,)


def test_callable_spec_target_is_unwrapped():
    source = KeyedSource("reduce", _plan_input(), ("k",), reducer=CallableSpec.create(_reducer))
    assert source_callable_roots(source) == (_reducer,)


@pytest.mark.parametrize("source", [
    ItemsSource(items=({"a": 1},)),
    RangeSource(count=3),
    FileSource(format="csv", paths=("/tmp/demiflow-l-chain/absent.csv",)),
    MaterializedSource(handle=object(), row_count=0),
    LogicalPlan(),
])
def test_sources_without_callables_have_no_roots(source):
    assert source_callable_roots(source) == ()


def test_lance_and_datasource_sources_have_no_roots(tmp_path):
    from demiflow.lance.model import LanceScanSpec
    from demiflow.data.datasource import Datasource
    lance = LanceSource(query=LanceScanSpec(uri=str(tmp_path / "absent.lance")))
    assert source_callable_roots(lance) == ()

    class inert(Datasource):
        def get_read_tasks(self, parallelism, per_task_row_limit=None, data_context=None):
            raise AssertionError("datasource must not be read by the pin")

    assert source_callable_roots(DatasourceSource(datasource=inert())) == ()


def test_subplan_operations_are_not_source_roots():
    """A nested PlanInput.plan belongs to that slice's own plan argument (C08)."""
    def subplan_udf(row):
        return row

    nested = PlanInput(ItemsSource(items=()), LogicalPlan((MapOp(CallableSpec.create(subplan_udf)),)))
    source = KeyedSource("reduce", nested, ("k",), reducer=_reducer)
    assert source_callable_roots(source) == (_reducer,)


def test_none_source_is_empty():
    assert source_callable_roots(None) == ()


def test_cycle_fails_closed():
    source = KeyedSource("reduce", _plan_input(), ("k",), reducer=_reducer)
    object.__setattr__(source, "left", PlanInput(source, LogicalPlan()))
    with pytest.raises(RecoveryContractError, match="cyclic"):
        source_callable_roots(source)


def test_shared_substructure_is_not_a_cycle():
    shared = _plan_input()
    source = UnionSource(inputs=(shared, shared))
    assert source_callable_roots(source) == ()


def test_over_budget_structure_fails_closed():
    from demiflow.execution.seal.business_sources import _DEP_MAX_NODES
    inputs = tuple(_plan_input() for _ in range(_DEP_MAX_NODES))
    with pytest.raises(RecoveryContractError, match="value budget"):
        source_callable_roots(UnionSource(inputs=inputs))


def test_unpinnable_callable_fails_closed():
    class CallableInstance:
        def __call__(self, acc, row):
            return acc

    source = KeyedSource("reduce", _plan_input(), ("k",), reducer=CallableInstance())
    with pytest.raises(RecoveryContractError, match="not a pinnable function"):
        source_callable_roots(source)


def test_traversal_is_passive():
    """No user property, __getattr__ or callable may run during collection."""
    touched = []

    class Hostile:
        @property
        def reducer(self):
            touched.append("property")
            raise AssertionError("property must not be read")

        def __getattr__(self, name):
            touched.append(name)
            raise AssertionError("attribute must not be read")

    # An unknown source type is not walked at all: explicit native types only.
    assert source_callable_roots(Hostile()) == ()
    assert touched == []
    assert source_callable_roots(SqlSource("select 1", _connection)) == (_connection,)
    assert touched == []


def test_root_order_is_stable_across_repeated_calls():
    graph = _local().reduce_by_key("k", _reducer, fn_kwargs={"b": _other, "a": _factory})
    first = source_callable_roots(graph._source)
    for _ in range(5):
        assert source_callable_roots(graph._source) == first