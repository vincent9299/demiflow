"""L006 unit (verify_preserve): load_sealed_plan is correct and unchanged.

Evidence that the F1 reader is schema-agnostic: it returns the same sealed
facts/logical_dag/physical_plan for v1 and v2 runs, reads even without execution
bindings, and fail-closes on a missing plan. No production change was required.
"""
import shutil

import pytest

from demiflow.execution.plan_interpreter import PlanContractError
from demiflow.execution.sealed_plans import SEALED_PLAN_FACTS_SCHEMA, load_sealed_plan
from test_execution_layout import chain_fixture
import _v2_native_fixtures as fx


@pytest.fixture
def workdir():
    path = fx.make_workdir()
    try:
        yield path
    finally:
        shutil.rmtree(path, ignore_errors=True)


def test_load_sealed_plan_v2_run(workdir):
    n = fx.native_v2_doc()
    sealed = fx.seal_native_run(workdir / "run", n)
    sp = load_sealed_plan(sealed.root, sealed.action_id)
    assert set(sp) == {"facts", "logical_dag", "physical_plan"}
    assert sp["facts"]["schema"] == SEALED_PLAN_FACTS_SCHEMA
    assert sp["facts"]["run_id"] == sealed.run_id
    assert sp["facts"]["dag_id"] == n.ld["dag_id"]
    assert sp["facts"]["plan_id"] == n.pp["plan_id"]
    assert sp["logical_dag"] == n.ld
    assert sp["physical_plan"] == n.pp


def test_load_sealed_plan_v1_run(workdir):
    v1 = chain_fixture()
    sealed = fx.seal_v1_run(workdir / "run", v1)
    sp = load_sealed_plan(sealed.root, sealed.action_id)
    assert sp["logical_dag"] == v1.ld
    assert sp["physical_plan"] == v1.pp


def test_load_sealed_plan_without_bindings(workdir):
    n = fx.native_v2_doc()
    sealed = fx.seal_native_run(workdir / "run", n, with_bindings=False)
    sp = load_sealed_plan(sealed.root, sealed.action_id)
    assert sp["facts"]["action_id"] == sealed.action_id


def test_missing_plan_rejected(workdir):
    root = workdir / "run"
    root.mkdir(parents=True)
    with pytest.raises(PlanContractError) as exc:
        load_sealed_plan(root, "absent-action")
    assert exc.value.reason == "missing_plan"