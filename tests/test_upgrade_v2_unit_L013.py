"""L013 · _WriteSeal.layout — derive from the sealed placement, never a fake worker.

Binds: a v1 chain seal still returns the worker reader's ``(checkpoint_policy,
stage_limits)`` unchanged; a v2 native driver seal returns the sealed layout's
checkpoint and a program-operation-index keyed budget map derived solely from the
driver placement's ``stage_limits`` through the root program's operator_bindings.
The driver route never loads a worker group that does not exist, never substitutes
current run defaults for the sealed budgets, and fails closed on a missing stage
budget. Only the root program is indexed: the streamed stage machine is built from
the root plan's operations, while sub-program budgets are consumed by the native
backend from the same placement.

The v1 cases use the real sealer/journal/reader. The v2 cases feed a C08 contract
sample placement: layout() reads only that bound object, so no interpreter,
journal or commit authority is mocked.
"""
import ast
import inspect
import textwrap
from types import MappingProxyType, SimpleNamespace

import pytest

import demiflow.data as data
from demiflow.data.plan import CallableSpec, LogicalPlan, MapOp
from demiflow.data.sources import ItemsSource
from demiflow.execution.chain import _WriteSeal, seal_write_topology
from demiflow.execution.plan_interpreter import CheckpointPolicy
from demiflow.execution.recovery_contract import RecoveryContractError, identity
from demiflow.execution.run_context import RunContext, RunAction
from demiflow.execution.worker import layout as worker_layout

from _v2_write_seal_fixtures import (
    SampleExecution, SampleLayout, freeze_programs, program_binding,
)

CHECKPOINT = CheckpointPolicy(checkpoint_rows=8, interval_ms=1000,
                              unconfirmed_rows=4, unconfirmed_bytes=4096)
SEALED_BUDGETS = {"inflight_rows": 3, "inflight_bytes": 4096, "output_bytes": 512}
OTHER_BUDGETS = {"inflight_rows": 7, "inflight_bytes": 8192, "output_bytes": 1024}
TOPOLOGY = {"bindings_ref": {"path": "b", "sha256": "s"}, "operators": []}


def _double(row):
    return {"k": row["k"], "v": row["v"] * 2}


def _triple(row):
    return {"k": row["k"], "v": row["v"] * 3}


def _plan(*functions):
    return LogicalPlan(tuple(MapOp(CallableSpec.create(f)) for f in functions))


@pytest.fixture
def sealed(tmp_path):
    with RunContext.start(tmp_path / "run") as run:
        declaration = {"kind": "write-lance", "source": {"source_id": "fixed"}}
        run.journal.bind_execution_action(run.fence, "write-action", declaration)
        action = RunAction(run, "write-action", declaration)
        with data.local_execution(workers=1, worker_mode="thread", partitions=1):
            try:
                yield SimpleNamespace(run=run, action=action)
            finally:
                record = run.journal.execution_action("write-action")
                if record is not None and record["result"] is None:
                    run.journal.complete_execution_action(
                        run.fence, "write-action", {"kind": "test-complete"})


@pytest.fixture
def source():
    return ItemsSource(items=({"k": "a", "v": 1},))


def _native_seal(action, source, programs, stage_limits, *, checkpoint=CHECKPOINT):
    placement = SampleExecution(execution_id="exec-1", placement="driver",
                                programs=freeze_programs(programs),
                                stage_limits=MappingProxyType(dict(stage_limits)))
    layout = SampleLayout(checkpoint=checkpoint, executions=(placement,))
    return _WriteSeal(action, None, TOPOLOGY, source=source,
                      native_layout=layout, native_execution_id="exec-1")


def _root(action_id, bindings):
    return program_binding(program_id=identity([action_id, []]), input_path=[],
                           operator_bindings=bindings)


# --- v1: the original worker reader, unchanged --------------------------------

def test_v1_layout_returns_the_worker_reader_result(sealed, source):
    seal = seal_write_topology(sealed.action, source, _plan(_double))
    checkpoint, stage_limits = seal.layout()
    assert isinstance(checkpoint, CheckpointPolicy)
    assert set(stage_limits) == {0}
    assert set(stage_limits[0]) == {"inflight_rows", "inflight_bytes", "output_bytes"}


def test_v1_layout_indexes_every_operation(sealed, source):
    seal = seal_write_topology(sealed.action, source, _plan(_double, _triple))
    _, stage_limits = seal.layout()
    assert sorted(stage_limits) == [0, 1]


def test_v1_layout_still_uses_load_worker_layout(sealed, source, monkeypatch):
    seal = seal_write_topology(sealed.action, source, _plan(_double))
    calls = []
    original = worker_layout.load_worker_layout

    def spy(launch_spec, authority, assignment):
        calls.append(launch_spec)
        return original(launch_spec, authority, assignment)

    monkeypatch.setattr(worker_layout, "load_worker_layout", spy)
    seal.layout()
    assert calls == [{"execution_plan": {"action_id": "write-action",
                                         "group_id": "chain-group",
                                         "bindings_ref": seal.bindings_ref}}]


def test_v1_layout_matches_the_sealed_physical_budgets(sealed, source):
    from demiflow.execution.sealed_plans import load_execution_bindings, load_sealed_plan
    seal = seal_write_topology(sealed.action, source, _plan(_double, _triple))
    checkpoint, stage_limits = seal.layout()
    bundle = load_sealed_plan(sealed.run.directory, "write-action")
    doc = load_execution_bindings(sealed.run.directory, "write-action",
                                  sealed_plan=bundle)["document"]
    physical = bundle["physical_plan"]
    stage_by_id = {stage["stage_id"]: stage for stage in physical["stages"]}
    expected = {binding["program_index"]: dict(stage_by_id[binding["stage_id"]]["budgets"])
                for binding in doc["programs"][0]["operator_bindings"]}
    assert stage_limits == expected
    assert sorted(stage_limits) == [0, 1]
    recovery = physical["recovery"]
    assert checkpoint == CheckpointPolicy(
        checkpoint_rows=doc["checkpoint_rows"],
        interval_ms=recovery["checkpoint_interval_ms"],
        unconfirmed_rows=recovery["unconfirmed_rows"],
        unconfirmed_bytes=recovery["unconfirmed_bytes"])


# --- v2: derived from the sealed driver placement -----------------------------

def test_v2_layout_maps_program_index_to_sealed_budgets(sealed, source):
    programs = [_root(sealed.action.action_id,
                      [{"program_index": 0, "stage_id": "stage-a"},
                       {"program_index": 1, "stage_id": "stage-b"}])]
    seal = _native_seal(sealed.action, source, programs,
                        {"stage-a": SEALED_BUDGETS, "stage-b": OTHER_BUDGETS})
    checkpoint, stage_limits = seal.layout()
    assert checkpoint is CHECKPOINT
    assert stage_limits == {0: SEALED_BUDGETS, 1: OTHER_BUDGETS}


def test_v2_layout_never_loads_a_worker(sealed, source, monkeypatch):
    def forbidden(*args, **kwargs):
        raise AssertionError("the driver route must not load a worker group")

    monkeypatch.setattr(worker_layout, "load_worker_layout", forbidden)
    programs = [_root(sealed.action.action_id, [{"program_index": 0, "stage_id": "stage-a"}])]
    seal = _native_seal(sealed.action, source, programs, {"stage-a": SEALED_BUDGETS})
    assert seal.layout() == (CHECKPOINT, {0: SEALED_BUDGETS})


def test_v2_layout_does_not_substitute_run_defaults(sealed, source):
    programs = [_root(sealed.action.action_id, [{"program_index": 0, "stage_id": "stage-a"}])]
    seal = _native_seal(sealed.action, source, programs, {"stage-a": SEALED_BUDGETS})
    _, stage_limits = seal.layout()
    limits = sealed.run.limits
    assert stage_limits[0]["inflight_rows"] == SEALED_BUDGETS["inflight_rows"]
    assert stage_limits[0]["inflight_rows"] != limits["batch_rows"]
    assert stage_limits[0]["output_bytes"] != limits["max_row_bytes"] + 256


def test_v2_layout_checkpoint_is_the_sealed_policy(sealed, source):
    other = CheckpointPolicy(checkpoint_rows=99, interval_ms=7,
                             unconfirmed_rows=1, unconfirmed_bytes=2)
    seal = _native_seal(sealed.action, source, [], {}, checkpoint=other)
    checkpoint, stage_limits = seal.layout()
    assert checkpoint is other
    assert stage_limits == {}


def test_v2_layout_empty_programs_yield_no_budgets(sealed, source):
    seal = _native_seal(sealed.action, source, [], {"stage-a": SEALED_BUDGETS})
    assert seal.layout() == (CHECKPOINT, {})


def test_v2_layout_ignores_an_operation_free_sub_program(sealed, source):
    action_id = sealed.action.action_id
    programs = [_root(action_id, [{"program_index": 0, "stage_id": "stage-a"}]),
                program_binding(program_id=identity([action_id, ["source", "left"]]),
                                input_path=["source", "left"])]
    seal = _native_seal(sealed.action, source, programs, {"stage-a": SEALED_BUDGETS})
    assert seal.layout() == (CHECKPOINT, {0: SEALED_BUDGETS})


def test_v2_layout_indexes_only_the_root_program(sealed, source):
    action_id = sealed.action.action_id
    programs = [_root(action_id, [{"program_index": 0, "stage_id": "stage-a"}]),
                program_binding(program_id=identity([action_id, ["source", "left"]]),
                                input_path=["source", "left"],
                                operator_bindings=[{"program_index": 0, "stage_id": "stage-b"},
                                                   {"program_index": 1, "stage_id": "stage-c"}])]
    seal = _native_seal(sealed.action, source, programs,
                        {"stage-a": SEALED_BUDGETS, "stage-b": OTHER_BUDGETS,
                         "stage-c": OTHER_BUDGETS})
    assert seal.layout() == (CHECKPOINT, {0: SEALED_BUDGETS})


def test_v2_layout_refuses_a_missing_stage_budget(sealed, source):
    programs = [_root(sealed.action.action_id, [{"program_index": 0, "stage_id": "stage-a"}])]
    seal = _native_seal(sealed.action, source, programs, {"stage-other": SEALED_BUDGETS})
    with pytest.raises(RecoveryContractError, match="stage budget is missing"):
        seal.layout()


def test_v2_layout_returns_an_independent_map(sealed, source):
    programs = [_root(sealed.action.action_id, [{"program_index": 0, "stage_id": "stage-a"}])]
    seal = _native_seal(sealed.action, source, programs, {"stage-a": SEALED_BUDGETS})
    _, stage_limits = seal.layout()
    stage_limits[0]["inflight_rows"] = 999
    stage_limits[7] = {}
    assert seal.layout() == (CHECKPOINT, {0: SEALED_BUDGETS})


def test_v2_layout_is_read_only(sealed, source):
    programs = [_root(sealed.action.action_id, [{"program_index": 0, "stage_id": "stage-a"}])]
    seal = _native_seal(sealed.action, source, programs, {"stage-a": SEALED_BUDGETS})
    journal = sealed.run.journal.path
    before = journal.read_bytes()
    seal.layout()
    assert journal.read_bytes() == before


def test_v2_branch_returns_before_the_worker_reader_import():
    tree = ast.parse(textwrap.dedent(inspect.getsource(_WriteSeal.layout)))
    body = tree.body[0].body
    assert isinstance(body[0], ast.If)
    assert ast.unparse(body[0].test) == "self._native_placement is not None"
    assert isinstance(body[0].body[-1], ast.Return)
    assert ast.unparse(body[0].body[-1].value) == "(self._native_layout.checkpoint, stage_limits)"
    assert isinstance(body[1], ast.ImportFrom)
    assert body[1].module == "worker.layout"