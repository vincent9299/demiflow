"""Sealing and bounded safe reading of sealed execution inputs.

This module owns the durable side: it seals the execution-bindings document into
a content-addressed in-run carrier referenced through the existing journal
``<action_id>-execution-bindings`` source-binding slot, and reads back the F1
sealed plan plus its bindings. It imports the interpreter one-way for the shared
contract types and validation; the interpreter never imports this reader.

Reading is engine-neutral: no workbench/forge import, no business Python, no
plan pickle. Every carrier read is run-relative, symlink-refusing, length- and
digest-gated and bounded before the bytes are consumed.
"""
from __future__ import annotations

import hashlib
import json
from pathlib import Path

from .recovery_contract import RecoveryContractError, document, identifier, identity
from .plan_interpreter import (
    EXECUTION_BINDINGS_SCHEMA, EXECUTION_BINDINGS_SCHEMA_V2, PlanContractError,
    _validate_artifact_ref, _validate_bindings, _validate_manifest_ref,
)
from .run_journal import RunJournal
from .state_artifacts import atomic_bytes
from ..planning.dag import validate_logical_dag, validate_physical_execution_plan

SEALED_PLAN_FACTS_SCHEMA = "demiflow.sealed_plan_facts.v1"
MAX_SEALED_PLAN_BYTES = 4 * 1024 ** 2   # mirrors chain.MAX_SEALED_PLAN_BYTES
MAX_BINDINGS_BYTES = 4 * 1024 ** 2

_F1_KEYS = frozenset({"schema", "run_id", "action_id", "dag_id", "plan_id",
                      "artifact_manifest_ref", "logical_dag_ref", "physical_plan_ref",
                      "sealed_at"})
_SEALED_PLAN_KEY = "-sealed-plan"
_BINDINGS_KEY = "-execution-bindings"
_BINDINGS_DIR = ("scratch", "chain-execution-bindings")


def _require(condition, reason, detail):
    if not condition:
        raise PlanContractError(reason, detail)


def _check_action_id(action_id):
    try:
        identifier(action_id)
    except (RecoveryContractError, ValueError) as exc:
        raise PlanContractError("invalid_contract", "action_id invalid: " + str(exc))


def read_plan_artifact(directory, ref, *, max_bytes) -> dict:
    """Bounded, run-relative, symlink-refusing, digest-gated carrier read."""
    _validate_artifact_ref(ref, "artifact")
    _require(type(max_bytes) is int and max_bytes >= 0,
             "invalid_contract", "max_bytes must be a nonnegative int")
    if ref["bytes"] > max_bytes:
        raise PlanContractError("artifact_too_large", "declared artifact bytes exceed the read budget")
    root = Path(directory)
    path = root / Path(ref["path"])
    if any(item.is_symlink() for item in (path, *path.parents)):
        raise PlanContractError("unsafe_artifact", "artifact path contains a symlink")
    resolved, root_resolved = path.resolve(), root.resolve()
    if resolved != root_resolved and root_resolved not in resolved.parents:
        raise PlanContractError("unsafe_artifact", "artifact path escapes the run directory")
    if not path.exists():
        raise PlanContractError("corrupt_artifact", "artifact carrier is missing")
    if not path.is_file():
        raise PlanContractError("unsafe_artifact", "artifact carrier is not a regular file")
    actual = path.stat().st_size
    if actual > max_bytes:
        raise PlanContractError("artifact_too_large", "artifact carrier exceeds the read budget")
    if actual != ref["bytes"]:
        raise PlanContractError("corrupt_artifact", "artifact carrier length differs")
    with path.open("rb") as stream:
        raw = stream.read(max_bytes + 1)
    if len(raw) > max_bytes:
        raise PlanContractError("artifact_too_large", "artifact carrier exceeds the read budget")
    if len(raw) != ref["bytes"]:
        raise PlanContractError("corrupt_artifact", "artifact carrier changed while reading")
    if hashlib.sha256(raw).hexdigest() != ref["sha256"]:
        raise PlanContractError("corrupt_artifact", "artifact carrier digest differs")
    try:
        value = json.loads(raw)
    except ValueError:
        raise PlanContractError("corrupt_artifact", "artifact carrier is not valid JSON")
    _require(type(value) is dict, "invalid_contract", "artifact carrier is not a JSON object")
    return value


def _validate_f1(facts, journal, action_id):
    _require(type(facts) is dict and set(facts) == _F1_KEYS,
             "invalid_contract", "sealed plan facts key set is unsupported")
    _require(facts["schema"] == SEALED_PLAN_FACTS_SCHEMA,
             "invalid_contract", "unsupported sealed plan facts schema")
    binding = journal.recovery_identity()
    engine_run_id = binding.get("run_id") if type(binding) is dict else None
    _require(facts["run_id"] == engine_run_id,
             "identity_mismatch", "F1 run_id differs from the journal recovery identity")
    _require(facts["action_id"] == action_id,
             "identity_mismatch", "F1 action_id differs from the requested action")
    for key in ("dag_id", "plan_id"):
        try:
            identifier(facts[key])
        except (RecoveryContractError, ValueError) as exc:
            raise PlanContractError("invalid_contract", key + " invalid: " + str(exc))
    _validate_manifest_ref(facts["artifact_manifest_ref"])
    _validate_artifact_ref(facts["logical_dag_ref"], "logical_dag")
    _validate_artifact_ref(facts["physical_plan_ref"], "physical_plan")
    _require(type(facts["sealed_at"]) in (int, str),
             "invalid_contract", "sealed_at must be an int or string")


def _v3_read_bounds():
    from .contracts.dataflow import Bounds
    return Bounds(max_nodes=4096, max_edges=8192, max_partitions=4096, max_depth=128,
                    max_control_bytes=MAX_SEALED_PLAN_BYTES, max_row_bytes=1024 * 1024,
                    max_batch_bytes=MAX_SEALED_PLAN_BYTES, max_resident_bytes=MAX_SEALED_PLAN_BYTES,
                    max_spill_bytes=MAX_SEALED_PLAN_BYTES, max_state_bytes=MAX_SEALED_PLAN_BYTES,
                    max_pending_events=4096, max_open_files=256, max_output_rows=1024 * 1024,
                    timeout_ms=60000)


def _v3_read_store(directory, *, store_id):
    """Single adjustable v3 read point (W007/W008).

    Builds the run-rooted artifact store and read bounds used to load a v3 sealed
    dataflow. The root is the writer's .dataflow-store directory and the domain
    comes from its authenticated artifact reference. Reading history creates no
    directories, reservations or journal writes.
    """
    from .dataflow_store import RunArtifactStore
    bounds = _v3_read_bounds()
    store = RunArtifactStore(root=Path(directory).absolute() / ".dataflow-store", store_id=store_id,
                             journal=None, bounds=bounds, readonly=True)
    return store, bounds


def _load_sealed_v3(directory, action_id, marker):
    """v3 branch: read and fully validate the original artifacts via load_dataflow."""
    from .dataflow_seal import load_dataflow
    from .dataflow_history import _ref_from_json
    ref = _ref_from_json(marker["physical_ref"], "physical_ref")
    store, bounds = _v3_read_store(directory, store_id=ref.store_id)
    graph, physical, sealed = load_dataflow(Path(directory), action_id, store=store, bounds=bounds)
    return {"version": "v3", "facts": marker, "graph": graph,
            "physical": physical, "sealed": sealed}


def load_sealed_plan(directory, action_id) -> dict:
    """Read the original F1 sealed plan; readable even without execution bindings.

    An explicit v3 branch delegates to ``load_dataflow`` when the action carries a
    v3 native seal marker; the v1/v2 F1 path below is preserved byte-for-byte (its
    validation and original digests are never recomputed).
    """
    _check_action_id(action_id)
    journal = RunJournal(directory)
    if not journal.path.is_file():
        raise PlanContractError("missing_plan", "run journal is absent")
    v3_marker = journal.source_binding(action_id + "-dataflow-sealed")
    if v3_marker is not None:
        return _load_sealed_v3(directory, action_id, v3_marker)
    facts = journal.source_binding(action_id + _SEALED_PLAN_KEY)
    if facts is None:
        raise PlanContractError("missing_plan", "action has no sealed plan facts")
    _validate_f1(facts, journal, action_id)
    logical_dag = read_plan_artifact(directory, facts["logical_dag_ref"],
                                     max_bytes=MAX_SEALED_PLAN_BYTES)
    physical_plan = read_plan_artifact(directory, facts["physical_plan_ref"],
                                       max_bytes=MAX_SEALED_PLAN_BYTES)
    try:
        validate_logical_dag(logical_dag)
        validate_physical_execution_plan(physical_plan)
    except (ValueError, RecoveryContractError) as exc:
        raise PlanContractError("invalid_contract", str(exc))
    _require(facts["dag_id"] == logical_dag["dag_id"],
             "identity_mismatch", "sealed dag_id differs from F1")
    _require(facts["plan_id"] == physical_plan["plan_id"],
             "identity_mismatch", "sealed plan_id differs from F1")
    _require(logical_dag["action_id"] == action_id,
             "identity_mismatch", "sealed dag action_id differs from F1")
    _require(physical_plan["logical_dag_ref"] == {
        "dag_id": logical_dag["dag_id"], "sha256": identity(logical_dag)},
        "identity_mismatch", "physical logical_dag_ref does not bind the sealed dag")
    _require(physical_plan["artifact_manifest_ref"] == facts["artifact_manifest_ref"],
             "identity_mismatch", "physical manifest reference differs from F1")
    return {"facts": facts, "logical_dag": logical_dag, "physical_plan": physical_plan}


def _load_bindings_v3(directory, action_id, sealed_plan):
    """v3 single layout: the sealed identity bridge bound to the same graph/plan.

    Reads only the v3 bridge carrier; v2 driver placement / worker groups are not
    part of this layout and are rejected rather than misread as worker facts.
    """
    from .dataflow_history import _store_read_json, _ref_json
    graph, physical, sealed = sealed_plan["graph"], sealed_plan["physical"], sealed_plan["sealed"]
    store, _ = _v3_read_store(directory, store_id=sealed.physical_ref.store_id)
    ref = sealed.identity_bridge_ref
    doc = _store_read_json(store, ref, max_bytes=MAX_BINDINGS_BYTES)
    _require(doc.get("action_id") == action_id,
             "identity_mismatch", "v3 bindings action_id differs from the sealed plan")
    _require(doc.get("dag_id") == graph.dag_id,
             "identity_mismatch", "v3 bindings dag_id differs from the sealed graph")
    # The v1 identity bridge maps report occurrences to the logical DAG;
    # physical placement is bound by the enclosing sealed manifest and digest.
    # Unavailable v3 bridge documents additionally carry their physical id.
    if doc.get("schema") != "demiflow.identity_bridge.v1":
        _require(doc.get("plan_id") == physical.plan_id,
                 "identity_mismatch", "v3 bindings plan_id differs from the sealed plan")
    _require("worker_groups" not in doc and "driver" not in doc,
             "unsupported_layout", "v3 bindings must not carry v2 driver/worker placement")
    return {"ref": _ref_json(ref), "document": doc, "version": "v3"}


def load_execution_bindings(directory, action_id, *, sealed_plan) -> dict:
    """Read execution bindings and bind them to the same sealed run/action/dag/plan."""
    _check_action_id(action_id)
    if type(sealed_plan) is dict and sealed_plan.get("version") == "v3":
        return _load_bindings_v3(directory, action_id, sealed_plan)
    _require(type(sealed_plan) is dict and set(sealed_plan) == {"facts", "logical_dag", "physical_plan"},
             "invalid_contract", "sealed_plan must be the {facts, logical_dag, physical_plan} bundle")
    facts = sealed_plan["facts"]
    journal = RunJournal(directory)
    if not journal.path.is_file():
        raise PlanContractError("missing_bindings", "run journal is absent")
    ref = journal.source_binding(action_id + _BINDINGS_KEY)
    if ref is None:
        raise PlanContractError("missing_bindings", "action has no execution bindings")
    _validate_artifact_ref(ref, "execution bindings")
    doc = read_plan_artifact(directory, ref, max_bytes=MAX_BINDINGS_BYTES)
    _require(doc.get("schema") in (EXECUTION_BINDINGS_SCHEMA, EXECUTION_BINDINGS_SCHEMA_V2),
             "invalid_contract", "unsupported execution bindings schema")
    _require(doc.get("run_id") == facts["run_id"],
             "identity_mismatch", "bindings run_id differs from the sealed plan")
    _require(doc.get("action_id") == facts["action_id"] == action_id,
             "identity_mismatch", "bindings action_id differs from the sealed plan")
    _require(doc.get("dag_id") == facts["dag_id"],
             "identity_mismatch", "bindings dag_id differs from the sealed plan")
    _require(doc.get("plan_id") == facts["plan_id"],
             "identity_mismatch", "bindings plan_id differs from the sealed plan")
    _require(doc.get("artifact_manifest_ref") == facts["artifact_manifest_ref"],
             "identity_mismatch", "bindings manifest reference differs from the sealed plan")
    _validate_bindings(doc, sealed_plan["logical_dag"], sealed_plan["physical_plan"],
                       facts["artifact_manifest_ref"])
    return {"ref": ref, "document": doc}


def seal_execution_bindings(run, action, *, logical_dag, physical_plan, profiles,
                            programs, driver_stage_ids, source_bindings,
                            channel_options, checkpoint_rows, supported_slice,
                            executions=None) -> dict:
    """Seal the execution-bindings document; idempotent re-seal must be identical.

    The carrier is content-addressed and unregistered (mirroring F1) so it survives
    run teardown for audit. The reference is bound through the existing immutable
    journal source-binding slot; no new journal table is introduced.
    """
    action_id = action.action_id
    _check_action_id(action_id)
    try:
        validate_logical_dag(logical_dag)
        validate_physical_execution_plan(physical_plan)
    except (ValueError, RecoveryContractError) as exc:
        raise PlanContractError("invalid_contract", str(exc))
    manifest_ref = physical_plan["artifact_manifest_ref"]
    if executions is None:
        doc = {"schema": EXECUTION_BINDINGS_SCHEMA, "run_id": run.ref.run_id,
               "action_id": action_id, "dag_id": logical_dag["dag_id"],
               "plan_id": physical_plan["plan_id"], "artifact_manifest_ref": manifest_ref,
               "supported_slice": supported_slice, "profiles": profiles, "programs": programs,
               "driver_stage_ids": driver_stage_ids, "source_bindings": source_bindings,
               "channel_options": channel_options, "checkpoint_rows": checkpoint_rows}
    else:
        _require(not programs, "invalid_contract",
                 "v2 bindings must pass an empty programs sequence")
        doc = {"schema": EXECUTION_BINDINGS_SCHEMA_V2, "run_id": run.ref.run_id,
               "action_id": action_id, "dag_id": logical_dag["dag_id"],
               "plan_id": physical_plan["plan_id"], "artifact_manifest_ref": manifest_ref,
               "supported_slice": supported_slice, "profiles": profiles, "executions": executions,
               "driver_stage_ids": driver_stage_ids, "source_bindings": source_bindings,
               "channel_options": channel_options, "checkpoint_rows": checkpoint_rows}
    _validate_bindings(doc, logical_dag, physical_plan, manifest_ref)
    try:
        raw = document(doc, MAX_BINDINGS_BYTES)
    except RecoveryContractError as exc:
        raise PlanContractError("artifact_too_large", str(exc))
    digest = hashlib.sha256(raw).hexdigest()
    path = run.directory.joinpath(*_BINDINGS_DIR, digest + ".json")
    ref = {"path": str(path.relative_to(run.directory)), "sha256": digest, "bytes": len(raw)}
    key = action_id + _BINDINGS_KEY
    existing = run.journal.source_binding(key)
    if existing is not None and existing != ref:
        raise PlanContractError("identity_mismatch",
                                "execution bindings already sealed with a different reference")
    if any(item.is_symlink() for item in (path, *path.parents)):
        raise PlanContractError("unsafe_artifact", "execution-bindings path contains a symlink")
    with run.journal.publication_guard(run.fence):
        if not path.exists():
            atomic_bytes(path, raw)
        elif path.read_bytes() != raw:
            raise PlanContractError("corrupt_artifact",
                                    "existing execution-bindings artifact is corrupt")
    run.journal.bind_source(run.fence, key, ref)
    return ref