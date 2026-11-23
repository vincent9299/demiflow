"""L008 unit: load_native_execution reads the same sealed inputs, locates one id.

Real v2 seal -> load roundtrip (no F004 needed: the fixture compiles ld/pp with
the frozen planners). The sealed layout is the authority; an absent/ambiguous
execution_id is refused and a v1 run exposes no native execution (no auto-upgrade).
"""
import shutil

import pytest

from demiflow.execution.seal.plan import load_native_execution
from demiflow.execution.seal.plan_interpreter import ExecutionPlacement
from demiflow.execution.contracts.errors import PlanContractError
from demiflow.execution.pipeline.run_context import RunContext
from test_execution_layout import chain_fixture
import _v2_native_fixtures as fx


@pytest.fixture
def workdir():
    path = fx.make_workdir()
    try:
        yield path
    finally:
        shutil.rmtree(path, ignore_errors=True)


def test_load_native_execution_locates_placement(workdir, legacy_seal_writer):
    n = fx.native_v2_doc(backend="datafusion", recovery="final_commit")
    sealed = fx.seal_existing_format(legacy_seal_writer, workdir / "run", n, native=True)
    run = RunContext(sealed.root)
    layout, placement = load_native_execution(run, sealed.action_id, execution_id=n.execution_id)
    assert layout.supported_slice == "native_driver_v1"
    assert isinstance(placement, ExecutionPlacement)
    assert placement.execution_id == n.execution_id
    assert placement.backend == "datafusion"
    assert placement.recovery == "final_commit"
    assert list(placement.stage_ids) == n.op_stages
    assert placement.stage_limits == {stage["stage_id"]: stage["budgets"]
                                      for stage in n.pp["stages"] if stage["stage_id"] in n.op_stages}
    assert layout.checkpoint.checkpoint_rows == n.doc["checkpoint_rows"]
    recovery = n.pp["recovery"]
    assert (layout.checkpoint.interval_ms, layout.checkpoint.unconfirmed_rows,
            layout.checkpoint.unconfirmed_bytes) == (
        recovery["checkpoint_interval_ms"], recovery["unconfirmed_rows"], recovery["unconfirmed_bytes"])
    assert tuple(op["stage_id"] for op in placement.programs[0]["operator_bindings"]) == tuple(n.op_stages)


def test_unknown_execution_id_rejected(workdir, legacy_seal_writer):
    n = fx.native_v2_doc()
    sealed = fx.seal_existing_format(legacy_seal_writer, workdir / "run", n, native=True)
    run = RunContext(sealed.root)
    with pytest.raises(PlanContractError) as exc:
        load_native_execution(run, sealed.action_id, execution_id="absent-execution")
    assert exc.value.reason == "invalid_contract"


def test_v1_run_exposes_no_native_execution(workdir, legacy_seal_writer):
    sealed = fx.seal_existing_format(legacy_seal_writer, workdir / "run", chain_fixture(), native=False)
    run = RunContext(sealed.root)
    with pytest.raises(PlanContractError) as exc:
        load_native_execution(run, sealed.action_id, execution_id="any")
    assert exc.value.reason == "invalid_contract"