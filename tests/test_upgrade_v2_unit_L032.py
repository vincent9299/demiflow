"""L032 · _WriteSeal.__init__ — bind only an already-verified native reference.

Binds: the four original fields are unchanged; the v1 defaults leave source /
native_layout / native_execution_id / resolved placement as None; a v2 binding
requires a real driver placement located by id inside the supplied layout, plus
the fixed source; a half-supplied native reference, an absent/ambiguous id and a
worker placement all fail closed. Construction performs no implicit load or
compile and never recomputes a historical identity.

Real RunContext/RunAction journal. The v2 layout is a C08 contract sample
(tests/_v2_write_seal_fixtures.py) because ExecutionPlacement is an L-contract
deliverable; __init__ reads only the supplied object, so no authority is mocked.
"""
import sys

import pytest

import demiflow.data as data
from demiflow.data.sources import ItemsSource
from demiflow.execution.chain import _WriteSeal
from demiflow.execution.plan_interpreter import CheckpointPolicy
from demiflow.execution.recovery_contract import RecoveryContractError, identity
from demiflow.execution.run_context import RunContext, RunAction

from _v2_write_seal_fixtures import (
    SampleExecution, SampleLayout, freeze_programs, program_binding,
)

PLAN_REF = {"path": "p.json", "sha256": "abc", "code_pin_digest": "pin",
            "code_sources": [], "format": "code-pin-v1"}
TOPOLOGY = {"run_id": "r", "action_id": "write-action", "operators": [],
            "sink_id": "lance", "bindings_ref": {"path": "sealed.json", "sha256": "ref"}}
CHECKPOINT = CheckpointPolicy(checkpoint_rows=8, interval_ms=1000,
                              unconfirmed_rows=4, unconfirmed_bytes=4096)


@pytest.fixture
def action(tmp_path):
    with RunContext.start(tmp_path / "run") as run:
        declaration = {"kind": "write-lance", "source": {"source_id": "fixed"}}
        run.journal.bind_execution_action(run.fence, "write-action", declaration)
        run_action = RunAction(run, "write-action", declaration)
        with data.local_execution(workers=1, worker_mode="thread", partitions=1):
            try:
                yield run_action
            finally:
                record = run.journal.execution_action("write-action")
                if record is not None and record["result"] is None:
                    run.journal.complete_execution_action(
                        run.fence, "write-action", {"kind": "test-complete"})


@pytest.fixture
def source():
    return ItemsSource(items=({"k": "a", "v": 1},))


def _layout(executions):
    return SampleLayout(checkpoint=CHECKPOINT, executions=tuple(executions))


def _driver(execution_id="exec-1", programs=()):
    return SampleExecution(execution_id=execution_id, placement="driver",
                           programs=freeze_programs(programs))


def test_v1_defaults_keep_the_original_four_fields(action):
    seal = _WriteSeal(action, PLAN_REF, TOPOLOGY)
    assert seal._action is action
    assert seal._plan_ref is PLAN_REF
    assert seal.topology is TOPOLOGY
    assert seal.bindings_ref is TOPOLOGY["bindings_ref"]
    assert seal._source is None
    assert seal._native_layout is None
    assert seal._native_execution_id is None
    assert seal._native_placement is None


def test_v1_accepts_a_source_without_becoming_native(action, source):
    seal = _WriteSeal(action, PLAN_REF, TOPOLOGY, source=source)
    assert seal._source is source
    assert seal._native_placement is None


def test_execution_id_without_layout_is_refused(action):
    with pytest.raises(RecoveryContractError, match="requires its sealed layout"):
        _WriteSeal(action, PLAN_REF, TOPOLOGY, native_execution_id="exec-1")


def test_layout_without_execution_id_is_refused(action, source):
    with pytest.raises(RecoveryContractError, match="requires its sealed execution id"):
        _WriteSeal(action, PLAN_REF, TOPOLOGY, source=source,
                   native_layout=_layout([_driver()]))


def test_layout_without_source_is_refused(action):
    with pytest.raises(RecoveryContractError, match="requires the fixed source"):
        _WriteSeal(action, PLAN_REF, TOPOLOGY,
                   native_layout=_layout([_driver()]), native_execution_id="exec-1")


def test_absent_execution_id_is_refused(action, source):
    with pytest.raises(RecoveryContractError, match="absent or ambiguous"):
        _WriteSeal(action, PLAN_REF, TOPOLOGY, source=source,
                   native_layout=_layout([_driver("other")]), native_execution_id="exec-1")


def test_empty_executions_are_refused(action, source):
    with pytest.raises(RecoveryContractError, match="absent or ambiguous"):
        _WriteSeal(action, PLAN_REF, TOPOLOGY, source=source,
                   native_layout=_layout([]), native_execution_id="exec-1")


def test_ambiguous_execution_id_is_refused(action, source):
    with pytest.raises(RecoveryContractError, match="absent or ambiguous"):
        _WriteSeal(action, PLAN_REF, TOPOLOGY, source=source,
                   native_layout=_layout([_driver(), _driver()]),
                   native_execution_id="exec-1")


def test_worker_placement_is_refused(action, source):
    worker = SampleExecution(execution_id="exec-1", placement="worker",
                             group_id="chain-group")
    with pytest.raises(RecoveryContractError, match="driver executions only"):
        _WriteSeal(action, PLAN_REF, TOPOLOGY, source=source,
                   native_layout=_layout([worker]), native_execution_id="exec-1")


def test_driver_placement_is_bound_by_identity(action, source):
    wanted = _driver("exec-2", [program_binding(program_id="p", input_path=[])])
    layout = _layout([_driver("exec-1"), wanted])
    seal = _WriteSeal(action, PLAN_REF, TOPOLOGY, source=source,
                      native_layout=layout, native_execution_id="exec-2")
    assert seal._native_placement is wanted
    assert seal._native_layout is layout
    assert seal._native_execution_id == "exec-2"
    assert seal._source is source
    assert seal.bindings_ref is TOPOLOGY["bindings_ref"]


def test_construction_does_not_load_or_compile(action, source, monkeypatch):
    monkeypatch.delitem(sys.modules, "demiflow.execution.native_plan", raising=False)
    monkeypatch.delitem(sys.modules, "demiflow.planning.dataset_dag", raising=False)
    before = action.context.journal.source_binding("write-action-sealed-plan")
    layout = _layout([_driver(programs=[program_binding(program_id="p", input_path=[])])])
    seal = _WriteSeal(action, PLAN_REF, TOPOLOGY, source=source,
                      native_layout=layout, native_execution_id="exec-1")
    assert seal._native_placement is layout.executions[0]
    assert "demiflow.execution.native_plan" not in sys.modules
    assert "demiflow.planning.dataset_dag" not in sys.modules
    assert action.context.journal.source_binding("write-action-sealed-plan") == before


def test_supplied_layout_is_not_copied_or_reinterpreted(action, source):
    programs = freeze_programs([program_binding(program_id="p", input_path=[])])
    wanted = SampleExecution(execution_id="exec-1", programs=programs,
                             stage_limits={"stage-0": {"inflight_rows": 3}})
    seal = _WriteSeal(action, PLAN_REF, TOPOLOGY, source=source,
                      native_layout=_layout([wanted]), native_execution_id="exec-1")
    assert seal._native_placement.programs is programs
    assert seal._native_placement.stage_limits is wanted.stage_limits
    assert seal.topology is TOPOLOGY


def test_native_binding_keeps_the_declared_execution_id_formula(action, source):
    execution_id = identity([action.action_id, "driver", "native_relational"])
    layout = _layout([_driver(execution_id)])
    seal = _WriteSeal(action, PLAN_REF, TOPOLOGY, source=source,
                      native_layout=layout, native_execution_id=execution_id)
    assert seal._native_execution_id == execution_id
    assert seal._native_placement.execution_id == execution_id


def test_empty_programs_are_a_valid_driver_binding(action, source):
    seal = _WriteSeal(action, None, TOPOLOGY, source=source,
                      native_layout=_layout([_driver(programs=())]),
                      native_execution_id="exec-1")
    assert seal._native_placement.programs == ()
    assert seal._plan_ref is None