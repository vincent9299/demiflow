"""L005 unit: load_execution_bindings dispatches v1/v2 and refuses unknown schema.

Real durable carriers. v1 and v2 both read back bound to the same sealed plan;
an unknown schema and identity drift fail closed on .reason. Carrier retargeting
mirrors the existing tamper technique (rewrite the content-addressed carrier and
the immutable journal ref) so the digest gate still passes before the schema gate.
"""
import copy
import hashlib
import json
import shutil
import sqlite3

import pytest

from demiflow.execution.plan_interpreter import (
    EXECUTION_BINDINGS_SCHEMA, EXECUTION_BINDINGS_SCHEMA_V2, PlanContractError,
)
from demiflow.execution.recovery_contract import document, identity
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


def _retarget(root, action_id, new_doc):
    raw = document(new_doc)
    digest = hashlib.sha256(raw).hexdigest()
    rel = "scratch/chain-execution-bindings/" + digest + ".json"
    path = root / rel
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(raw)
    ref = {"path": rel, "sha256": digest, "bytes": len(raw)}
    key = action_id + "-execution-bindings"
    with sqlite3.connect(root / "run.sqlite") as db:
        db.execute("UPDATE runtime_sources SET document=? WHERE action_id=?", (json.dumps(ref), key))
    return ref


def _load(root, action_id):
    sealed = load_sealed_plan(root, action_id)
    return load_execution_bindings(root, action_id, sealed_plan=sealed)


def test_load_v2_bindings(workdir):
    n = fx.native_v2_doc()
    sealed = fx.seal_native_run(workdir / "run", n)
    loaded = _load(sealed.root, sealed.action_id)
    assert set(loaded) == {"ref", "document"}
    assert loaded["document"]["schema"] == EXECUTION_BINDINGS_SCHEMA_V2
    assert loaded["ref"] == sealed.ref


def test_load_v1_bindings(workdir):
    sealed = fx.seal_v1_run(workdir / "run", chain_fixture())
    loaded = _load(sealed.root, sealed.action_id)
    assert loaded["document"]["schema"] == EXECUTION_BINDINGS_SCHEMA


def test_unknown_schema_rejected(workdir):
    n = fx.native_v2_doc()
    sealed = fx.seal_native_run(workdir / "run", n)
    bad = copy.deepcopy(n.doc)
    bad["schema"] = "demiflow.execution_bindings.v999"
    _retarget(sealed.root, sealed.action_id, bad)
    with pytest.raises(PlanContractError) as exc:
        _load(sealed.root, sealed.action_id)
    assert exc.value.reason == "invalid_contract"


def test_identity_drift_rejected(workdir):
    n = fx.native_v2_doc()
    sealed = fx.seal_native_run(workdir / "run", n)
    bad = copy.deepcopy(n.doc)
    bad["dag_id"] = identity(["wrong-dag"])
    _retarget(sealed.root, sealed.action_id, bad)
    with pytest.raises(PlanContractError) as exc:
        _load(sealed.root, sealed.action_id)
    assert exc.value.reason == "identity_mismatch"