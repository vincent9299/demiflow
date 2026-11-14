"""L008 unit: load_native_execution reads the same sealed inputs, locates one id.

Real v2 seal -> load roundtrip (no F004 needed: the fixture compiles ld/pp with
the frozen planners). The sealed layout is the authority; an absent/ambiguous
execution_id is refused and a v1 run exposes no native execution (no auto-upgrade).
"""
import shutil

import pytest

from demiflow.execution.native_plan import load_native_execution
from demiflow.execution.plan_interpreter import ExecutionPlacement, PlanContractError
from demiflow.execution.run_context import RunContext
from test_execution_layout import chain_fixture
import _v2_native_fixtures as fx


@pytest.fixture
def workdir():
    path = fx.make_workdir()
    try:
        yield path
    finally:
        shutil.rmtree(path, ignore_errors=True)


def test_load_native_execution_locates_placement(workdir):
    n = fx.native_v2_doc(backend="datafusion", recovery="final_commit")
    sealed = fx.seal_native_run(workdir / "run", n)
    run = RunContext(sealed.root)
    layout, placement = load_native_execution(run, sealed.action_id, execution_id=n.execution_id)
    assert layout.supported_slice == "native_driver_v1"
    assert isinstance(placement, ExecutionPlacement)
    assert placement.execution_id == n.execution_id
    assert placement.backend == "datafusion"
    assert placement.recovery == "final_commit"
    assert list(placement.stage_ids) == n.op_stages


def test_unknown_execution_id_rejected(workdir):
    n = fx.native_v2_doc()
    sealed = fx.seal_native_run(workdir / "run", n)
    run = RunContext(sealed.root)
    with pytest.raises(PlanContractError) as exc:
        load_native_execution(run, sealed.action_id, execution_id="absent-execution")
    assert exc.value.reason == "invalid_contract"


def test_v1_run_exposes_no_native_execution(workdir):
    sealed = fx.seal_v1_run(workdir / "run", chain_fixture())
    run = RunContext(sealed.root)
    with pytest.raises(PlanContractError) as exc:
        load_native_execution(run, sealed.action_id, execution_id="any")
    assert exc.value.reason == "invalid_contract"