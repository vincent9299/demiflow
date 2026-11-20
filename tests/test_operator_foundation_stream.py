"""Native stream lifecycle and validation, including actual await boundaries."""
from dataclasses import replace

import pytest

from demiflow.data.api import DataAPI
from demiflow.data.dataset import Dataset
from demiflow.data.flow_contract import field, scalar, union_type
from demiflow.data.plan import LogicalPlan
from demiflow.data.runtime_contract import RuntimeContractError
from demiflow.execution.executors.local import LocalDatasetExecutor


@pytest.fixture
def data():
    executor = LocalDatasetExecutor(workers=1, block_size=2, materialize_memory_limit=128)
    try:
        yield DataAPI(executor)
    finally:
        executor.close()


@pytest.mark.parametrize("form", ["instance", "bound_method", "class"])
@pytest.mark.parametrize("start_failure", [False, True])
def test_actor_ownership_without_policy_attributes(data, form, start_failure, tmp_path):
    events = tmp_path/'events'
    def record(name):
        with events.open('a') as stream:
            stream.write(name+'\n')
    def recorded():
        return events.read_text().splitlines() if events.exists() else []

    class Actor:
        def __init__(self):
            record("construct")

        def snapshot_state(self):
            return {}  # No semantic state; resource lifecycle remains local.

        def restore_state(self, state):
            assert state == {}

        async def astart(self):
            record("start")
            if start_failure:
                raise ValueError("start failed")

        async def __call__(self, row):
            assert recorded().count("start") == 1
            return {"x": row["x"] + 1}

        async def astop(self):
            record("stop")

        async def aclose(self):
            record("close")

    actor = Actor if form == "class" else Actor()
    fn = actor.__call__ if form == "bound_method" else actor
    ds = data.from_items([{"x": 1}]).map_async(fn, catch=(Exception,))
    if form != "class":
        ds = ds.map_async(fn)  # same owner at two distinct logical stages
    if start_failure:
        with pytest.raises(ValueError, match="start failed"):
            ds.materialize()
    else:
        rows = ds.materialize().take_all()
        assert rows == [{"x": 2 if form == "class" else 3}]
        assert recorded().count("stop") == 1
    assert recorded().count("construct") == recorded().count("start") == recorded().count("close") == 1


def _checked(dataset, returns):
    op = dataset._plan.operations[-1]
    row = {**scalar("record"), "fields": {"x": field(scalar("int"))}}
    contract = {"label": "fixture-node", "parameters": [{"name": "row", "type": row}],
                "returns": returns, "max_row_bytes": 4096, "awaitable": True}
    op = replace(op, callable=replace(op.callable, runtime_contract=contract))
    return Dataset(dataset._source, LogicalPlan((op,)), dataset._executor, dataset._stages)


def test_async_actual_return_checked_after_await_and_outside_business_catch(data, tmp_path):
    calls = tmp_path/'calls'

    async def malformed(row):
        with calls.open("a") as stream:
            stream.write(str(row["x"])+"\n")
        return {"x": "wrong"}

    row_type = {**scalar("record"), "fields": {"x": field(scalar("int"))}}
    ds = _checked(data.from_items([{"x": 1}]).map_async(malformed, catch=(Exception,)), row_type)
    with pytest.raises(RuntimeContractError, match="expected int"):
        ds.materialize()
    assert calls.read_text().splitlines() == ["1"]


def test_async_none_list_and_business_miss_preserve_native_semantics(data):
    async def expand(row):
        if row["x"] == 0:
            return None
        if row["x"] == 1:
            raise ValueError("business miss")
        return [{"x": row["x"]}, {"x": row["x"] + 10}]

    row_type = {**scalar("record"), "fields": {"x": field(scalar("int"))}}
    returns = union_type(row_type, {**scalar("list"), "item": row_type}, scalar("none"))
    ds = _checked(data.from_items([{"x": i} for i in range(3)]).map_async(expand, catch=(ValueError,)), returns)
    assert ds.materialize().take_all() == [{"x": 2}, {"x": 12}]


def test_materialize_consumer_does_not_append_business_operator(data, monkeypatch):
    ds = data.from_items([{"x": 1}]).map_async(lambda row: row)
    from demiflow.execution.action_driver.dataflow import DataflowExecution
    original = DataflowExecution.prepare
    observed = []

    def prepare(execution):
        result = original(execution)
        if execution._request.kind == 'materialize':
            observed.append(([node.kernel.kind for node in execution._graph.nodes
                              if node.kind == 'transform'],
                             [node.kernel.kind for node in execution._graph.nodes
                              if node.kind == 'terminal']))
        return result

    monkeypatch.setattr(DataflowExecution, 'prepare', prepare)
    assert ds.materialize().take_all() == [{"x": 1}]
    assert observed == [(['AsyncMapOp'], ['materialize'])]
