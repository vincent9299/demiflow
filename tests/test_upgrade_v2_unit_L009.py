"""The original v2 placement can be read and checked without a native dispatch path."""
import hashlib
import sqlite3

import pytest

from demiflow.execution.dataflow_history import load_execution_version, validate_resume_compatibility
from demiflow.execution.seal.plan_interpreter import PlanContractError
from demiflow.execution.run_context import RunContext
from demiflow.execution.seal.plan import load_native_execution
from demiflow.execution.seal.sealed_plans import load_execution_bindings, load_sealed_plan
from runtime_v3_history_scenarios import OriginalCarrierReader
from test_runtime_v3_sealed_version_selection import dual_seal


def test_read_published_native_placement_without_executing_or_replacing_it(dual_seal):
    root, action_id = dual_seal
    run = RunContext(root)
    seal = load_sealed_plan(root, action_id, version="v2")
    bindings = load_execution_bindings(root, action_id, sealed_plan=seal)
    expected = bindings["document"]["executions"][0]
    layout, placement = load_native_execution(run, action_id, execution_id=expected["execution_id"])
    assert placement in layout.executions
    assert (placement.backend, placement.recovery) == (expected["backend"], expected["recovery"])
    assert not run.journal.execution_commits(action_id)
    with pytest.raises(PlanContractError, match="execution_id is absent"):
        load_native_execution(run, action_id, execution_id="unknown")


def test_native_only_published_layout_has_no_automatic_resume_adapter(dual_seal):
    root, action_id = dual_seal
    # This test's original writer produced both seals for independent selection.
    # Leave the published v2 carriers untouched and remove only its v3 marker.
    with sqlite3.connect(root / "run.sqlite") as db:
        db.execute("DELETE FROM runtime_sources WHERE action_id=?", (action_id + "-dataflow-sealed",))
    seal = load_sealed_plan(root, action_id, version="v2")
    carrier = root / seal["facts"]["physical_plan_ref"]["path"]
    before = hashlib.sha256(carrier.read_bytes()).digest()
    binding = load_execution_version(root, action_id, max_bytes=4 * 1024**2)
    report = validate_resume_compatibility(binding, store=OriginalCarrierReader(root))
    assert not report.supported
    assert any("adapter" in reason for reason in report.reasons)
    assert hashlib.sha256(carrier.read_bytes()).digest() == before
    assert not RunContext(root).journal.execution_commits(action_id)
