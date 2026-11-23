"""Verify v1 and v2 carriers produced and re-sealed by the published writer."""
import json
import shutil

import pytest

from demiflow.execution.plan_interpreter import (
    EXECUTION_BINDINGS_SCHEMA, EXECUTION_BINDINGS_SCHEMA_V2, PlanContractError,
)
from demiflow.execution.sealed_plans import load_execution_bindings, load_sealed_plan
from test_execution_layout import chain_fixture
import _v2_native_fixtures as fx


@pytest.fixture
def workdir():
    path = fx.make_workdir()
    try:
        yield path
    finally:
        shutil.rmtree(path, ignore_errors=True)


def _carrier_doc(root, ref):
    return json.loads((root / ref["path"]).read_bytes())


def test_v2_seal_returns_content_addressed_carrier(workdir, legacy_seal_writer):
    n = fx.native_v2_doc()
    root = workdir / "run"
    sealed = fx.seal_existing_format(legacy_seal_writer, root, n, native=True)
    ref = sealed.ref
    assert set(ref) == {"path", "sha256", "bytes"}
    assert (root / ref["path"]).is_file()
    doc = _carrier_doc(root, ref)
    assert doc["schema"] == EXECUTION_BINDINGS_SCHEMA_V2
    assert doc["supported_slice"] == "native_driver_v1"
    assert "executions" in doc and "programs" not in doc


def test_v2_rejects_mixed_program_owners(workdir, legacy_seal_writer):
    sealed = fx.seal_existing_format(legacy_seal_writer, workdir / "run", fx.native_v2_doc(),
                                     native=True)
    plan = load_sealed_plan(sealed.root, sealed.action_id)
    doc = _carrier_doc(sealed.root, sealed.ref)
    doc["programs"] = [{"group_id": "g0"}]
    fx.retarget_history_bindings(sealed.root, sealed.action_id, doc)
    with pytest.raises(PlanContractError) as exc:
        load_execution_bindings(sealed.root, sealed.action_id, sealed_plan=plan)
    assert exc.value.reason == "invalid_contract"


def test_v1_program_bindings_stay_readable(workdir, legacy_seal_writer):
    sealed = fx.seal_existing_format(legacy_seal_writer, workdir / "run", chain_fixture(),
                                     native=False)
    plan = load_sealed_plan(sealed.root, sealed.action_id)
    loaded = load_execution_bindings(sealed.root, sealed.action_id, sealed_plan=plan)
    assert loaded["ref"] == sealed.ref
    assert loaded["document"]["schema"] == EXECUTION_BINDINGS_SCHEMA
    assert "programs" in loaded["document"] and "executions" not in loaded["document"]
