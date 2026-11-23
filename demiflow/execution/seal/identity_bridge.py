"""Identity bridge: map report observation keys to sealed execution nodes (C).

``build_identity_bridge`` is a pure function that validates resolved bindings
against the sealed logical DAG and produces a fixed-structure document.
``seal_identity_bridge`` persists it through the existing journal side-car slot.
No attempt, no counters, no node state — the bridge describes fixed structure;
observation reads query by selected attempt separately.
"""
from __future__ import annotations

import hashlib

from demiflow.execution.recovery_contract import RecoveryContractError, document, identity
from demiflow.execution.storage.state_artifacts import atomic_bytes

IDENTITY_BRIDGE_SCHEMA = "demiflow.identity_bridge.v1"
_MAX_BRIDGE_BYTES = 1024 * 1024


def build_identity_bridge(logical_dag, *, action_id, report_digest, bindings):
    """Validate resolved bindings and build the bridge document.

    *bindings* is a sequence of resolved items, each::

        {report_node_id, report_occurrence, logical_node_id, occurrence_id, mode}

    Every ``logical_node_id``/``occurrence_id`` must exist in *logical_dag*.
    Each ``(report_node_id, report_occurrence)`` maps to at most one instance.
    ``report_occurrence`` and execution ``occurrence_id`` are two spaces; the
    latter must be an actual instance in the sealed DAG, never recomputed from
    the former.
    """
    if type(report_digest) is not str or not report_digest:
        raise RecoveryContractError("Identity bridge requires a report digest")
    node_by_occ = {n["occurrence_id"]: n for n in logical_dag["nodes"]}
    node_ids = {n["logical_node_id"] for n in logical_dag["nodes"]}
    seen_report_keys = set()
    entries = []
    for item in bindings:
        if type(item) is not dict:
            raise RecoveryContractError("Identity bridge binding must be a mapping")
        required = {"report_node_id", "report_occurrence",
                    "logical_node_id", "occurrence_id", "mode"}
        if set(item) != required:
            raise RecoveryContractError("Identity bridge binding key set is unsupported")
        occ = item["occurrence_id"]
        lid = item["logical_node_id"]
        if occ not in node_by_occ:
            raise RecoveryContractError(
                "Identity bridge occurrence is not in the sealed logical DAG")
        if node_by_occ[occ]["logical_node_id"] != lid:
            raise RecoveryContractError(
                "Identity bridge logical node does not match the occurrence")
        if lid not in node_ids:
            raise RecoveryContractError(
                "Identity bridge logical node is not in the sealed DAG")
        report_key = (item["report_node_id"], item["report_occurrence"])
        if report_key in seen_report_keys:
            raise RecoveryContractError(
                "Identity bridge report observation key maps to multiple instances")
        seen_report_keys.add(report_key)
        if item["mode"] not in ("execute", "reuse"):
            raise RecoveryContractError("Identity bridge mode is unsupported")
        entries.append({
            "report_node_id": item["report_node_id"],
            "report_occurrence": item["report_occurrence"],
            "logical_node_id": lid,
            "occurrence_id": occ,
            "mode": item["mode"],
        })
    bridge = {
        "schema": IDENTITY_BRIDGE_SCHEMA,
        "action_id": action_id,
        "dag_id": logical_dag["dag_id"],
        "report_digest": report_digest,
        "entries": entries,
    }
    document(bridge, _MAX_BRIDGE_BYTES)
    return bridge


def seal_identity_bridge(run, action, bridge):
    """Persist the bridge as a content-addressed artifact and bind it immutably.

    Returns ``{path, sha256, bytes}``.  The journal slot
    ``<action_id>-identity-bridge`` is immutable; an identical re-seal is
    idempotent, a differing one is refused.
    """
    if bridge.get("schema") != IDENTITY_BRIDGE_SCHEMA:
        raise RecoveryContractError("Invalid identity bridge schema")
    if bridge.get("action_id") != action.action_id:
        raise RecoveryContractError("Identity bridge action differs")
    raw = document(bridge, _MAX_BRIDGE_BYTES)
    digest = hashlib.sha256(raw).hexdigest()
    path = run.directory / "scratch" / "identity-bridges" / (digest + ".json")
    # Unregistered, mirroring seal_plan_facts: clean_orphans unlinks registered
    # protected=0 artifacts at run exit, but the workbench audits the bridge post-run.
    with run.journal.publication_guard(run.fence):
        if not path.exists():
            atomic_bytes(path, raw)
        elif path.read_bytes() != raw:
            raise RecoveryContractError("Existing identity bridge artifact is corrupt")
    run.release_temporaries([path.with_name(path.name + ".staging")])
    ref = {"path": str(path.relative_to(run.directory)),
           "sha256": digest, "bytes": len(raw)}
    key = action.action_id + "-identity-bridge"
    previous = run.journal.source_binding(key)
    if previous is not None and previous != ref:
        raise RecoveryContractError(
            "Identity bridge changed since seal; resume refused (fail-closed)")
    run.journal.bind_source(run.fence, key, ref)
    return ref