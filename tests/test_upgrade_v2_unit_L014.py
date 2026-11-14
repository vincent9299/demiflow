"""L014 · verify_loaded_plan — verify_preserve evidence (no production change).

The baseline implementation is kept byte-for-byte. These tests bind what the
card requires of it: the code pin AND every operator are recomputed from the
loaded plan alone, a driver-supplied digest is never trusted, and the check is
read-only. The plan_ref and sealed operators come from the REAL sealers
(stage_plan_artifact / compile_chain_topology), not a hand-built stand-in.

Honest boundary: this function binds ``logical_node_id`` only. ``occurrence_id``
and the full-DAG stage mapping are bound by the identity bridge /
operator_bindings, not here, so no claim is made about them.
"""
import sys
from types import SimpleNamespace

import pytest

import demiflow.data as data
from demiflow.data.plan import CallableSpec, LogicalPlan, MapOp
from demiflow.data.sources import ItemsSource
from demiflow.execution.chain import (
    compile_chain_topology, stage_plan_artifact, verify_loaded_plan,
)
from demiflow.execution.recovery_contract import RecoveryContractError, identity
from demiflow.execution.run_context import RunContext, RunAction


@pytest.fixture
def sealed(tmp_path):
    with RunContext.start(tmp_path / "run") as run:
        declaration = {"kind": "chain-materialize", "source": {"source_id": "fixed"}}
        run.journal.bind_execution_action(run.fence, "main-action", declaration)
        action = RunAction(run, "main-action", declaration)
        with data.local_execution(workers=1, worker_mode="thread", partitions=1):
            try:
                yield SimpleNamespace(run=run, action=action)
            finally:
                record = run.journal.execution_action("main-action")
                if record is not None and record["result"] is None:
                    run.journal.complete_execution_action(
                        run.fence, "main-action", {"kind": "test-complete"})


def _plan(*functions):
    return LogicalPlan(tuple(MapOp(CallableSpec.create(f)) for f in functions))


def _seal(sealed, plan):
    plan_ref = stage_plan_artifact(sealed.action, plan)
    source = ItemsSource(items=({"x": 1}, {"x": 2}))
    topology, _, _ = compile_chain_topology(
        sealed.action, source, plan, {"source_id": identity(["test-source"]), "data": {}},
        output_child_id="main-action-chain", host_id="local", plan_ref=plan_ref)
    return plan_ref, topology["operators"]


def _first(row):
    return {**row, "a": row["x"] + 1}


def _second(row):
    return {**row, "b": row["a"] * 2}


def _third(row):
    return {**row, "c": row["b"] - 1}


def test_genuine_loaded_plan_passes(sealed):
    plan = _plan(_first, _second, _third)
    plan_ref, operators = _seal(sealed, plan)
    assert verify_loaded_plan(plan, plan_ref, operators) is None


def test_edited_business_code_fail_closes(sealed):
    plan = _plan(_first, _second)
    plan_ref, operators = _seal(sealed, plan)

    def edited(row):
        return {**row, "a": row["x"] + 9999}

    with pytest.raises(RecoveryContractError, match="code-pin differs"):
        verify_loaded_plan(_plan(edited, _second), plan_ref, operators)


def test_driver_supplied_digest_is_not_trusted(sealed):
    plan = _plan(_first, _second)
    plan_ref, operators = _seal(sealed, plan)
    # A driver that hands over the digest of some OTHER plan must not pass: the
    # worker recomputes the pin from the loaded plan alone.
    forged = dict(plan_ref, code_pin_digest=identity({"operations": [], "dependencies": {}}))
    with pytest.raises(RecoveryContractError, match="code-pin differs"):
        verify_loaded_plan(plan, forged, operators)


def test_edited_middle_operator_is_refused(sealed):
    plan = _plan(_first, _second, _third)
    plan_ref, operators = _seal(sealed, plan)

    def middle_edit(row):
        return {**row, "b": row["a"] * 3}

    # The code-pin gate runs first, so an edited body is caught there.
    with pytest.raises(RecoveryContractError, match="code-pin differs"):
        verify_loaded_plan(_plan(_first, middle_edit, _third), plan_ref, operators)


def test_tampered_middle_operator_id_fail_closes(sealed):
    plan = _plan(_first, _second, _third)
    plan_ref, operators = _seal(sealed, plan)
    # Every index is compared, not just the first: tamper only the middle entry.
    tampered = [dict(item) for item in operators]
    tampered[1]["logical_node_id"] = identity(["forged-middle-node"])
    with pytest.raises(RecoveryContractError, match="operator identity differs"):
        verify_loaded_plan(plan, plan_ref, tampered)


def test_tampered_last_operator_id_fail_closes(sealed):
    plan = _plan(_first, _second, _third)
    plan_ref, operators = _seal(sealed, plan)
    tampered = [dict(item) for item in operators]
    tampered[-1]["logical_node_id"] = identity(["forged-last-node"])
    with pytest.raises(RecoveryContractError, match="operator identity differs"):
        verify_loaded_plan(plan, plan_ref, tampered)


def test_truncated_operator_list_fail_closes(sealed):
    plan = _plan(_first, _second, _third)
    plan_ref, operators = _seal(sealed, plan)
    with pytest.raises(RecoveryContractError, match="operator identity differs"):
        verify_loaded_plan(plan, plan_ref, operators[:-1])


def test_extra_sealed_operator_fail_closes(sealed):
    plan = _plan(_first, _second)
    plan_ref, operators = _seal(sealed, plan)
    with pytest.raises(RecoveryContractError, match="operator identity differs"):
        verify_loaded_plan(plan, plan_ref, operators + [operators[-1]])


def test_extra_loaded_operator_is_refused(sealed):
    plan = _plan(_first, _second)
    plan_ref, operators = _seal(sealed, plan)
    with pytest.raises(RecoveryContractError, match="code-pin differs"):
        verify_loaded_plan(_plan(_first, _second, _third), plan_ref, operators)


def test_reordered_operators_fail_closes(sealed):
    plan = _plan(_first, _second, _third)
    plan_ref, operators = _seal(sealed, plan)
    swapped = [operators[1], operators[0], operators[2]]
    with pytest.raises(RecoveryContractError, match="operator identity differs"):
        verify_loaded_plan(plan, plan_ref, swapped)


def test_none_operators_still_checks_the_code_pin(sealed):
    plan = _plan(_first, _second)
    plan_ref, _ = _seal(sealed, plan)
    assert verify_loaded_plan(plan, plan_ref, None) is None

    def edited(row):
        return {**row, "a": row["x"] + 9999}

    with pytest.raises(RecoveryContractError, match="code-pin differs"):
        verify_loaded_plan(_plan(edited, _second), plan_ref, None)


def test_empty_plan_passes_and_stays_read_only(sealed, tmp_path):
    plan = LogicalPlan()
    plan_ref, operators = _seal(sealed, plan)
    entries = set(tmp_path.rglob("*"))
    assert verify_loaded_plan(plan, plan_ref, operators) is None
    assert set(tmp_path.rglob("*")) == entries


def test_native_operator_policy_is_pinned(sealed):
    from demiflow.data.plan import AsyncMapOp
    plan = LogicalPlan((AsyncMapOp(CallableSpec.create(_first), concurrency=2),))
    plan_ref, operators = _seal(sealed, plan)
    assert verify_loaded_plan(plan, plan_ref, operators) is None
    changed = LogicalPlan((AsyncMapOp(CallableSpec.create(_first), concurrency=3),))
    with pytest.raises(RecoveryContractError, match="code-pin differs"):
        verify_loaded_plan(changed, plan_ref, operators)