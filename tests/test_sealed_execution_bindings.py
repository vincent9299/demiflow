"""B-package sealed reading: durable sealing + bounded, safe, run-relative reads.

These exercise the real RunContext/RunJournal lifecycle: seal F1 via the existing
chain.seal_plan_facts seam and the new execution-bindings carrier, then read back
post-run through the run root (=target). All scratch is confined to /tmp/demiflow-b-*
(the workdir fixture); no business worker is spawned, so nothing external is killed.

Positive: F1/carrier roundtrip, bindings bound to the same sealed run/action/dag/
plan, seal->interpret equality, idempotent re-seal, post-run durability + read-only.
Negative: missing plan/bindings, corrupt/oversize/unsafe carriers, identity mismatch
and invalid contracts all fail closed on .reason. read_plan_artifact is also unit-
tested directly against fixed carriers.
"""
import copy
import hashlib
import json
import shutil
import sqlite3
import sys
import tempfile
from pathlib import Path
from types import SimpleNamespace

import pytest

from demiflow.execution import chain, sealed_plans
from demiflow.execution.plan_interpreter import (
    PlanContractError, interpret_physical_plan, to_wire, validate_worker_assignment,
)
from demiflow.execution.recovery_contract import document, identity
from demiflow.execution.run_context import RunAction, RunContext
from demiflow.execution.run_journal import RunJournal
from demiflow.execution.sealed_plans import (
    MAX_BINDINGS_BYTES, MAX_SEALED_PLAN_BYTES, SEALED_PLAN_FACTS_SCHEMA,
    load_execution_bindings, load_sealed_plan, read_plan_artifact, seal_execution_bindings,
)
from test_execution_layout import (
    MANIFEST_REF, PROFILE, PROFILE_REF, assignment_for, bundle_for, chain_fixture,
    expected_channel_ids, fanout_fixture, stage_id,
)

pytestmark = pytest.mark.skipif(not sys.platform.startswith("linux"),
                                reason="Local managed units require Linux")


@pytest.fixture
def workdir():
    path = Path(tempfile.mkdtemp(prefix="demiflow-b-", dir="/tmp"))
    try:
        yield path
    finally:
        shutil.rmtree(path, ignore_errors=True)


def _seal_f1(run, action, fx):
    sealed_at = int(run.journal.recovery_identity()["started_at"] * 1000)
    return chain.seal_plan_facts(run, action, fx.ld, fx.pp, fx.mref, sealed_at)


def _seal_bindings(run, action, fx):
    return seal_execution_bindings(
        run, action, logical_dag=fx.ld, physical_plan=fx.pp,
        profiles=fx.doc["profiles"], programs=fx.doc["programs"],
        driver_stage_ids=fx.doc["driver_stage_ids"], source_bindings=fx.doc["source_bindings"],
        channel_options=fx.doc["channel_options"], checkpoint_rows=fx.doc["checkpoint_rows"],
        supported_slice=fx.doc["supported_slice"])


def _bind(run, action_id):
    declaration = {"kind": "chain-materialize", "source": {"source_id": "fixed"}}
    run.journal.bind_execution_action(run.fence, action_id, declaration)
    return RunAction(run, action_id, declaration)


def _sealed(workdir, fx, action_id, *, with_bindings=True):
    root = workdir / "run"
    with RunContext.start(root) as run:
        action = _bind(run, action_id)
        _seal_f1(run, action, fx)
        if with_bindings:
            _seal_bindings(run, action, fx)
        run_id = run.ref.run_id
        run.journal.complete_execution_action(run.fence, action_id, {"kind": "test-complete"})
    return SimpleNamespace(root=root, run_id=run_id, fx=fx, action_id=action_id)


@pytest.fixture
def sealed_chain(workdir):
    return _sealed(workdir, chain_fixture(), "main-action")


@pytest.fixture
def sealed_fanout(workdir):
    return _sealed(workdir, fanout_fixture(), "fanout-action")


def _tamper_binding(root, key, mutate):
    with sqlite3.connect(root / "run.sqlite") as db:
        row = db.execute("SELECT document FROM runtime_sources WHERE action_id=?", (key,)).fetchone()
        doc = json.loads(row[0])
        mutate(doc)
        db.execute("UPDATE runtime_sources SET document=? WHERE action_id=?", (json.dumps(doc), key))


def _carrier(workdir, name, value):
    raw = document(value)
    (workdir / name).write_bytes(raw)
    return {"path": name, "sha256": hashlib.sha256(raw).hexdigest(), "bytes": len(raw)}


def _reason(exc):
    assert isinstance(exc.value, PlanContractError)
    return exc.value.reason


# --- positive: F1 + bindings roundtrip ---

def test_load_sealed_plan_returns_f1_and_carriers(sealed_chain):
    sc = sealed_chain
    sp = load_sealed_plan(sc.root, sc.action_id)
    assert set(sp) == {"facts", "logical_dag", "physical_plan"}
    facts = sp["facts"]
    assert facts["schema"] == SEALED_PLAN_FACTS_SCHEMA
    assert facts["run_id"] == sc.run_id
    assert facts["action_id"] == sc.action_id
    assert facts["dag_id"] == sc.fx.ld["dag_id"]
    assert facts["plan_id"] == sc.fx.pp["plan_id"]
    assert facts["artifact_manifest_ref"] == MANIFEST_REF
    assert sp["logical_dag"] == sc.fx.ld
    assert sp["physical_plan"] == sc.fx.pp


def test_load_execution_bindings_binds_same_sealed_plan(sealed_chain):
    sc = sealed_chain
    sp = load_sealed_plan(sc.root, sc.action_id)
    bindings = load_execution_bindings(sc.root, sc.action_id, sealed_plan=sp)
    assert set(bindings) == {"ref", "document"}
    expected_doc = {**sc.fx.doc, "run_id": sc.run_id}
    assert bindings["document"] == expected_doc
    assert bindings["ref"] == bundle_for(expected_doc)["ref"]


def test_seal_then_interpret_roundtrip_equals_in_memory(sealed_chain):
    sc = sealed_chain
    sp = load_sealed_plan(sc.root, sc.action_id)
    bindings = load_execution_bindings(sc.root, sc.action_id, sealed_plan=sp)
    layout = interpret_physical_plan(sp["logical_dag"], sp["physical_plan"], bindings=bindings,
                                     manifest_ref=sp["facts"]["artifact_manifest_ref"])
    expected_doc = {**sc.fx.doc, "run_id": sc.run_id}
    expected = interpret_physical_plan(sc.fx.ld, sc.fx.pp, bindings=bundle_for(expected_doc),
                                       manifest_ref=sc.fx.mref)
    assert layout == expected
    assert to_wire(layout.bindings_ref) == bindings["ref"]
    # the sealed layout still validates a real worker assignment
    worker = layout.workers[0]
    inputs, outputs = expected_channel_ids(layout, worker)
    validate_worker_assignment(layout, assignment_for(layout, worker, sorted(inputs), sorted(outputs)),
                               group_id=worker.group_id)


def test_fanout_sealed_roundtrip(sealed_fanout):
    sc = sealed_fanout
    sp = load_sealed_plan(sc.root, sc.action_id)
    bindings = load_execution_bindings(sc.root, sc.action_id, sealed_plan=sp)
    layout = interpret_physical_plan(sp["logical_dag"], sp["physical_plan"], bindings=bindings,
                                     manifest_ref=sp["facts"]["artifact_manifest_ref"])
    assert layout.supported_slice == "static_fanout_v1"
    assert len(layout.workers) == 3
    for worker in layout.workers:
        inputs, outputs = expected_channel_ids(layout, worker)
        validate_worker_assignment(layout, assignment_for(layout, worker, sorted(inputs), sorted(outputs)),
                                   group_id=worker.group_id)


def test_carriers_survive_teardown_and_are_content_addressed(sealed_chain):
    sc = sealed_chain
    sp = load_sealed_plan(sc.root, sc.action_id)
    for ref in (sp["facts"]["logical_dag_ref"], sp["facts"]["physical_plan_ref"]):
        path = sc.root / ref["path"]
        assert path.is_file()
        assert ref["path"].startswith("scratch/chain-sealed-plans/")
        raw = path.read_bytes()
        assert hashlib.sha256(raw).hexdigest() == ref["sha256"]
        assert len(raw) == ref["bytes"]
    bindings = load_execution_bindings(sc.root, sc.action_id, sealed_plan=sp)
    path = sc.root / bindings["ref"]["path"]
    assert path.is_file()
    assert bindings["ref"]["path"].startswith("scratch/chain-execution-bindings/")
    assert not path.with_name(path.name + ".staging").exists()


def test_post_run_read_is_read_only(sealed_chain):
    sc = sealed_chain
    journal = sc.root / "run.sqlite"
    before = journal.stat().st_mtime_ns
    sp = load_sealed_plan(sc.root, sc.action_id)
    load_execution_bindings(sc.root, sc.action_id, sealed_plan=sp)
    reader = RunJournal(sc.root)
    assert reader.source_binding(sc.action_id + "-sealed-plan") == sp["facts"]
    assert journal.stat().st_mtime_ns == before


def test_seal_idempotent_reseal_identical(workdir):
    fx = chain_fixture()
    root = workdir / "run"
    with RunContext.start(root) as run:
        action = _bind(run, "main-action")
        _seal_f1(run, action, fx)
        first = _seal_bindings(run, action, fx)
        carrier = root / first["path"]
        mtime = carrier.stat().st_mtime_ns
        again = _seal_bindings(run, action, fx)
        assert again == first
        assert carrier.stat().st_mtime_ns == mtime
        assert run.journal.source_binding("main-action-execution-bindings") == first
        run.journal.complete_execution_action(run.fence, "main-action", {"kind": "test-complete"})


def test_load_sealed_plan_without_bindings(workdir):
    sc = _sealed(workdir, chain_fixture(), "main-action", with_bindings=False)
    sp = load_sealed_plan(sc.root, sc.action_id)  # F1 readable even without bindings
    assert sp["facts"]["schema"] == SEALED_PLAN_FACTS_SCHEMA
    with pytest.raises(PlanContractError) as exc:
        load_execution_bindings(sc.root, sc.action_id, sealed_plan=sp)
    assert _reason(exc) == "missing_bindings"


# --- negative: missing / corrupt / oversize / unsafe ---

def test_missing_plan_no_journal(workdir):
    with pytest.raises(PlanContractError) as exc:
        load_sealed_plan(workdir / "absent", "main-action")
    assert _reason(exc) == "missing_plan"


def test_missing_plan_no_binding(workdir):
    root = workdir / "nobinding"
    with RunContext.start(root) as run:
        action = _bind(run, "main-action")
        run.journal.complete_execution_action(run.fence, "main-action", {"kind": "done"})
    with pytest.raises(PlanContractError) as exc:
        load_sealed_plan(root, "main-action")
    assert _reason(exc) == "missing_plan"


def test_corrupt_carrier_digest(sealed_chain):
    sc = sealed_chain
    sp = load_sealed_plan(sc.root, sc.action_id)
    carrier = sc.root / sp["facts"]["logical_dag_ref"]["path"]
    carrier.write_bytes(carrier.read_bytes() + b" ")
    with pytest.raises(PlanContractError) as exc:
        load_sealed_plan(sc.root, sc.action_id)
    assert _reason(exc) == "corrupt_artifact"


def test_corrupt_carrier_missing(sealed_chain):
    sc = sealed_chain
    sp = load_sealed_plan(sc.root, sc.action_id)
    (sc.root / sp["facts"]["physical_plan_ref"]["path"]).unlink()
    with pytest.raises(PlanContractError) as exc:
        load_sealed_plan(sc.root, sc.action_id)
    assert _reason(exc) == "corrupt_artifact"


def test_artifact_too_large_budget(sealed_chain, monkeypatch):
    sc = sealed_chain
    monkeypatch.setattr(sealed_plans, "MAX_SEALED_PLAN_BYTES", 16)
    with pytest.raises(PlanContractError) as exc:
        load_sealed_plan(sc.root, sc.action_id)
    assert _reason(exc) == "artifact_too_large"


def test_unsafe_artifact_symlink(sealed_chain, workdir):
    sc = sealed_chain
    sp = load_sealed_plan(sc.root, sc.action_id)
    carrier = sc.root / sp["facts"]["logical_dag_ref"]["path"]
    target = workdir / "outside.json"
    target.write_bytes(carrier.read_bytes())
    carrier.unlink()
    carrier.symlink_to(target)
    with pytest.raises(PlanContractError) as exc:
        load_sealed_plan(sc.root, sc.action_id)
    assert _reason(exc) == "unsafe_artifact"


def test_identity_mismatch_f1_run_id(sealed_chain):
    sc = sealed_chain
    _tamper_binding(sc.root, sc.action_id + "-sealed-plan", lambda d: d.update(run_id="other-run"))
    with pytest.raises(PlanContractError) as exc:
        load_sealed_plan(sc.root, sc.action_id)
    assert _reason(exc) == "identity_mismatch"


def test_invalid_contract_f1_schema(sealed_chain):
    sc = sealed_chain
    _tamper_binding(sc.root, sc.action_id + "-sealed-plan", lambda d: d.update(schema="bogus"))
    with pytest.raises(PlanContractError) as exc:
        load_sealed_plan(sc.root, sc.action_id)
    assert _reason(exc) == "invalid_contract"


@pytest.mark.parametrize("field,value", [
    ("run_id", "other-run"), ("action_id", "other-action"), ("dag_id", "f" * 64),
    ("plan_id", "e" * 64), ("artifact_manifest_ref", {"manifest_id": "other", "sha256": "ab" * 32}),
])
def test_bindings_cross_check_identity_mismatch(sealed_chain, field, value):
    sc = sealed_chain
    sp = load_sealed_plan(sc.root, sc.action_id)
    bad = {**sp, "facts": {**sp["facts"], field: value}}
    with pytest.raises(PlanContractError) as exc:
        load_execution_bindings(sc.root, sc.action_id, sealed_plan=bad)
    assert _reason(exc) == "identity_mismatch"


def test_bindings_bad_bundle_shape(sealed_chain):
    sc = sealed_chain
    sp = load_sealed_plan(sc.root, sc.action_id)
    with pytest.raises(PlanContractError) as exc:
        load_execution_bindings(sc.root, sc.action_id, sealed_plan={"facts": sp["facts"]})
    assert _reason(exc) == "invalid_contract"


# --- read_plan_artifact direct unit tests (fixed carriers under workdir) ---

def test_read_plan_artifact_ok(workdir):
    ref = _carrier(workdir, "ok.json", {"hello": "world"})
    assert read_plan_artifact(workdir, ref, max_bytes=4096) == {"hello": "world"}


def test_read_plan_artifact_declared_oversize(workdir):
    ref = _carrier(workdir, "big.json", {"x": 1})
    ref = {**ref, "bytes": 4097}
    with pytest.raises(PlanContractError) as exc:
        read_plan_artifact(workdir, ref, max_bytes=4096)
    assert _reason(exc) == "artifact_too_large"


def test_read_plan_artifact_actual_oversize(workdir):
    ref = _carrier(workdir, "big.json", {"x": "y" * 8192})
    with pytest.raises(PlanContractError) as exc:
        read_plan_artifact(workdir, ref, max_bytes=16)
    assert _reason(exc) == "artifact_too_large"


def test_read_plan_artifact_symlink(workdir):
    ref = _carrier(workdir, "real.json", {"x": 1})
    link = workdir / "link.json"
    link.symlink_to(workdir / "real.json")
    with pytest.raises(PlanContractError) as exc:
        read_plan_artifact(workdir, {**ref, "path": "link.json"}, max_bytes=4096)
    assert _reason(exc) == "unsafe_artifact"


def test_read_plan_artifact_escape_rejected(workdir):
    ref = _carrier(workdir, "ok.json", {"x": 1})
    with pytest.raises(PlanContractError) as exc:
        read_plan_artifact(workdir, {**ref, "path": "../escape.json"}, max_bytes=4096)
    assert _reason(exc) == "invalid_contract"


def test_read_plan_artifact_absolute_rejected(workdir):
    ref = _carrier(workdir, "ok.json", {"x": 1})
    with pytest.raises(PlanContractError) as exc:
        read_plan_artifact(workdir, {**ref, "path": "/etc/passwd"}, max_bytes=4096)
    assert _reason(exc) == "invalid_contract"


def test_read_plan_artifact_missing(workdir):
    ref = _carrier(workdir, "ok.json", {"x": 1})
    (workdir / "ok.json").unlink()
    with pytest.raises(PlanContractError) as exc:
        read_plan_artifact(workdir, ref, max_bytes=4096)
    assert _reason(exc) == "corrupt_artifact"


def test_read_plan_artifact_digest_mismatch(workdir):
    ref = _carrier(workdir, "ok.json", {"x": 1})
    with pytest.raises(PlanContractError) as exc:
        read_plan_artifact(workdir, {**ref, "sha256": "0" * 64}, max_bytes=4096)
    assert _reason(exc) == "corrupt_artifact"


def test_read_plan_artifact_length_mismatch(workdir):
    ref = _carrier(workdir, "ok.json", {"x": 1})
    with pytest.raises(PlanContractError) as exc:
        read_plan_artifact(workdir, {**ref, "bytes": ref["bytes"] + 1}, max_bytes=4096)
    assert _reason(exc) == "corrupt_artifact"


def test_read_plan_artifact_non_dict_json(workdir):
    raw = document([1, 2, 3])
    (workdir / "arr.json").write_bytes(raw)
    ref = {"path": "arr.json", "sha256": hashlib.sha256(raw).hexdigest(), "bytes": len(raw)}
    with pytest.raises(PlanContractError) as exc:
        read_plan_artifact(workdir, ref, max_bytes=4096)
    assert _reason(exc) == "invalid_contract"


def test_read_plan_artifact_bad_json(workdir):
    raw = b"{not json"
    (workdir / "bad.json").write_bytes(raw)
    ref = {"path": "bad.json", "sha256": hashlib.sha256(raw).hexdigest(), "bytes": len(raw)}
    with pytest.raises(PlanContractError) as exc:
        read_plan_artifact(workdir, ref, max_bytes=4096)
    assert _reason(exc) == "corrupt_artifact"


def test_read_plan_artifact_non_regular_file(workdir):
    (workdir / "subdir").mkdir()
    ref = {"path": "subdir", "sha256": "0" * 64, "bytes": 0}
    with pytest.raises(PlanContractError) as exc:
        read_plan_artifact(workdir, ref, max_bytes=4096)
    assert _reason(exc) == "unsafe_artifact"


def test_read_plan_artifact_bad_ref_shape(workdir):
    with pytest.raises(PlanContractError) as exc:
        read_plan_artifact(workdir, {"path": "x.json"}, max_bytes=4096)
    assert _reason(exc) == "invalid_contract"


# --- seal-time fail-closed ---

def test_seal_rejects_invalid_bindings(workdir):
    fx = chain_fixture()
    root = workdir / "run"
    with RunContext.start(root) as run:
        action = _bind(run, "main-action")
        _seal_f1(run, action, fx)
        with pytest.raises(PlanContractError) as exc:
            seal_execution_bindings(
                run, action, logical_dag=fx.ld, physical_plan=fx.pp,
                profiles=fx.doc["profiles"], programs=fx.doc["programs"],
                driver_stage_ids=fx.doc["driver_stage_ids"],
                source_bindings=fx.doc["source_bindings"],
                channel_options=fx.doc["channel_options"], checkpoint_rows=0,  # invalid
                supported_slice=fx.doc["supported_slice"])
        assert _reason(exc) == "invalid_contract"
        run.journal.complete_execution_action(run.fence, "main-action", {"kind": "done"})


def test_seal_rejects_unsupported_slice(workdir):
    fx = chain_fixture()
    root = workdir / "run"
    with RunContext.start(root) as run:
        action = _bind(run, "main-action")
        _seal_f1(run, action, fx)
        with pytest.raises(PlanContractError) as exc:
            seal_execution_bindings(
                run, action, logical_dag=fx.ld, physical_plan=fx.pp,
                profiles=fx.doc["profiles"], programs=fx.doc["programs"],
                driver_stage_ids=fx.doc["driver_stage_ids"],
                source_bindings=fx.doc["source_bindings"],
                channel_options=fx.doc["channel_options"], checkpoint_rows=2,
                supported_slice="dynamic_v9")
        assert _reason(exc) == "unsupported_layout"
        run.journal.complete_execution_action(run.fence, "main-action", {"kind": "done"})