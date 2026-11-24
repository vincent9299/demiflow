"""Interpret journal-proven historical cuts and their original recovery guarantees."""
from __future__ import annotations

import hashlib
import json
from demiflow.execution.contracts.dataflow import ArtifactRef, CapabilityReport, CutContribution, CutRequest, HistoricalBinding, RecoveryImage, SourceCursor
from demiflow.execution.contracts.dataflow_errors import DataflowError
_err = DataflowError
from demiflow.execution.contracts.recovery_contract import RecoveryContractError
from demiflow.execution.contracts.dataflow_codec import history_carrier_reference as _carrier_ref
from demiflow.execution.storage.reading import read_history_carrier as _read_carrier, read_json_artifact as _store_read_json
from demiflow.execution.seal.plan import SEAL_SCHEMA as DATAFLOW_SEALED_SCHEMA, TRAINING_SEAL_SCHEMA
SEALED_PLAN_FACTS_SCHEMA = "demiflow.sealed_plan_facts.v1"
CHECKPOINT_MANIFEST_SCHEMA = "demiflow.checkpoint_manifest.v1"
_MAX_SEALED_PLAN_BYTES = 4 * 1024 ** 2
_JSON_MEDIA = "application/json"
RUN_STORE_ID = "run"


def latest_checkpoint_image(journal, directory, action_id, *, max_bytes):
    """Translate the latest acked native checkpoint manifest into a RecoveryImage.

    Only the journal-proven committed cut is an authority; files alone never are.
    Returns None when the action has no committed native checkpoint (the caller
    then reports an honest incompatible history rather than fabricating state).
    """
    try:
        commits = journal.execution_commits(action_id)
    except (ValueError, RecoveryContractError):
        return None
    acked = [c for c in commits if c.get("status") == "acked" and type(c.get("prepared")) is dict]
    if not acked:
        return None
    fact = max(acked, key=lambda c: c["sequence"])
    states = fact["prepared"].get("state")
    if type(states) is not dict:
        return None
    for ref in states.values():
        if type(ref) is not dict or not {"path", "sha256", "bytes"} <= set(ref):
            continue
        try:
            manifest = _read_carrier(directory, ref, max_bytes=max_bytes)
        except DataflowError:
            continue
        if manifest.get("schema_version") == CHECKPOINT_MANIFEST_SCHEMA:
            return _translate_manifest(manifest, fact)
    return None

def _translate_manifest(manifest, fact):
    """Pure translation of one native checkpoint manifest into a RecoveryImage.

    Preserves the original checkpoint/commit identity and effect/sink references.
    The single-source scalar progress is carried as ONE source cursor; it is never
    expanded into a guessed multi-source vector. Unknown top-level fields are
    rejected so a future format cannot be silently mis-read.
    """
    allowed = {"schema_version", "checkpoint_id", "run_id", "action_id", "epoch",
               "topology_revision", "route_epoch", "plan_revision", "artifact_manifest_ref",
               "participants", "source_progress", "channel_cuts", "operator_states",
               "inflight", "sink_receipts", "seal"}
    if set(manifest) != allowed:
        raise _err("INCOMPATIBLE_HISTORY", "checkpoint manifest has unknown fields")
    if manifest.get("seal", {}).get("sealed") is not True:
        raise _err("INCOMPATIBLE_HISTORY", "checkpoint manifest is not sealed")
    participants = manifest["participants"]
    sources = participants.get("sources", [])
    progress = manifest["source_progress"]
    if type(sources) is not list or type(progress) is not list:
        raise _err("INCOMPATIBLE_HISTORY", "checkpoint source progress does not match participants")
    progress_by_key = {}
    for item in progress:
        if type(item) is not dict or "source_id" not in item or "partition_id" not in item:
            raise _err("INCOMPATIBLE_HISTORY", "checkpoint source progress entry is unsupported")
        progress_by_key[(item["source_id"], item["partition_id"])] = item
    if len(sources) != len(progress_by_key):
        raise _err("INCOMPATIBLE_HISTORY", "checkpoint source progress does not cover every source")
    cursors = []
    for source in sources:
        item = progress_by_key.get((source["source_id"], source["partition_id"]))
        if item is None:
            raise _err("INCOMPATIBLE_HISTORY", "checkpoint source has no matching progress entry")
        position = item.get("position", {})
        ordinal = position.get("next_ordinal")
        if type(ordinal) is not int or ordinal < 0:
            raise _err("INCOMPATIBLE_HISTORY",
                       "checkpoint source position is not a translatable scalar ordinal")
        split_id = hashlib.sha256(json.dumps(
            ["dataflow-split-v1", source["source_id"], source["partition_id"]],
            sort_keys=True, separators=(",", ":")).encode()).hexdigest()
        cursors.append(SourceCursor(split_id=split_id, offset={"next_ordinal": ordinal},
                                    next_sequence=ordinal, end_of_input=bool(manifest["seal"].get("sealed")),
                                    lease_ref=None, pending_refs=()))
    cut_id = hashlib.sha256(json.dumps(
        ["dataflow-cut-v3", manifest["action_id"], manifest["plan_revision"],
         manifest["route_epoch"], manifest["checkpoint_id"]],
        sort_keys=True, separators=(",", ":")).encode()).hexdigest()
    participant_ids = tuple(sorted(set(
        [s["source_id"] for s in sources]
        + [c["channel_id"] for c in manifest["channel_cuts"]]
        + [o["occurrence_id"] for o in manifest["operator_states"]]
        + [s["sink_id"] for s in manifest["sink_receipts"]])))
    cut = CutRequest(cut_id=cut_id, sequence=int(fact["sequence"]),
                     plan_revision=int(manifest["plan_revision"]),
                     route_epoch=int(manifest["route_epoch"]),
                     participants=participant_ids, final=bool(manifest["seal"].get("sealed")))
    operator_states = {}
    for entry in manifest["operator_states"]:
        ref = entry.get("state_ref")
        if type(ref) is dict and {"path", "sha256", "bytes"} <= set(ref):
            operator_states[entry["occurrence_id"]] = _carrier_ref(ref)
    # Native inflight effect_ref is a journal identifier; only a content-addressed
    # sha256 effect is honestly representable as a v3 ArtifactRef. An identifier-only
    # effect cannot be mapped without guessing, so it is rejected, never fabricated.
    effects = []
    for entry in manifest["inflight"]:
        if entry.get("status") != "known_completed" or "effect_ref" not in entry:
            continue
        effect_ref = entry["effect_ref"]
        if type(effect_ref) is not str or len(effect_ref) != 64 or any(
                ch not in "0123456789abcdef" for ch in effect_ref):
            raise _err("INCOMPATIBLE_HISTORY",
                       "native inflight effect is not a content-addressed artifact")
        effects.append(ArtifactRef(digest=effect_ref, size_bytes=0, store_id=RUN_STORE_ID,
                                   key=effect_ref, media_type=_JSON_MEDIA))
    sinks = {}
    for receipt in manifest["sink_receipts"]:
        commit_id = receipt["commit_id"]
        if type(commit_id) is not str or len(commit_id) != 64 or any(
                ch not in "0123456789abcdef" for ch in commit_id):
            raise _err("INCOMPATIBLE_HISTORY",
                       "native sink receipt commit_id is not a content-addressed identity")
        sinks[receipt["sink_id"]] = ArtifactRef(
            digest=commit_id, size_bytes=0, store_id=RUN_STORE_ID,
            key=receipt["receipt_ref"], media_type=_JSON_MEDIA)
    contribution = CutContribution(
        cut_id=cut_id, participant_id=manifest["action_id"], boot_id=manifest["checkpoint_id"],
        sources=tuple(cursors), operator_states=operator_states, channels={},
        buffers=(), effects=tuple(effects), sinks=sinks, final=cut.final)
    return RecoveryImage(commit_id=fact["commit_id"], cut=cut,
                         contributions=(contribution,), confirmed_sinks=dict(sinks),
                         source_vector=tuple(cursors))

def translate_native_checkpoint(binding: HistoricalBinding, *, store, bounds) -> RecoveryImage:
    """Return the in-memory recovery image for a listed native v1/v2 checkpoint.

    Only the listed native format is translated; the original identity and
    effect/sink references are preserved and history is never overwritten. Unknown
    formats, an absent committed checkpoint, or unsealed/unavailable buffers are
    rejected. The old scalar source position is never guessed into a multi-source
    vector (the translated image keeps exactly the manifest's source cursors).
    """
    if not isinstance(binding, HistoricalBinding):
        raise _err("INVALID_CONTRACT", "translate_native_checkpoint requires a HistoricalBinding")
    if binding.schema_version != SEALED_PLAN_FACTS_SCHEMA:
        raise _err("INCOMPATIBLE_HISTORY",
                   "no listed native checkpoint adapter for schema: " + binding.schema_version)
    if binding.restore_image is None:
        raise _err("INCOMPATIBLE_HISTORY",
                   "action has no journal-proven committed checkpoint; cannot recover from files alone")
    image = binding.restore_image
    if not isinstance(image, RecoveryImage):
        raise _err("INVALID_CONTRACT", "historical restore image is not a RecoveryImage")
    for contribution in image.contributions:
        for ref in contribution.buffers:
            if not store.inspect(ref):
                raise _err("INCOMPATIBLE_HISTORY", "checkpoint buffer is not sealed/durable")
        for ref in contribution.operator_states.values():
            if not store.inspect(ref):
                raise _err("INCOMPATIBLE_HISTORY", "checkpoint operator state is not sealed/durable")
    if len(image.source_vector) != len({c.split_id for c in image.source_vector}):
        raise _err("INVALID_CONTRACT", "recovery source vector has duplicate splits")
    return image

def validate_resume_compatibility(binding: HistoricalBinding, *, current_runtime: str | None = None,
                                  store) -> CapabilityReport:
    """Check original code/source/state/sink/cursor/recovery guarantees item by item.

    Returns a CapabilityReport listing concrete incompatibility reasons. Missing
    original code or an unfinished ``attempt_only`` run never fabricates a new
    graph; the report is unsupported and the original reference is preserved.
    """
    if not isinstance(binding, HistoricalBinding):
        raise _err("INVALID_CONTRACT", "validate_resume_compatibility requires a HistoricalBinding")
    # current_runtime is retained for callers of the old API; provenance is not admission.
    reasons = []
    recovery = "attempt_only"
    domains = []
    profiles = []
    if not binding.compatible:
        reasons.append(binding.reason or "historical binding is marked incompatible")
        return CapabilityReport(supported=False, reasons=tuple(reasons), recovery="attempt_only",
                                required_domains=(), required_profiles=())
    if binding.schema_version not in (SEALED_PLAN_FACTS_SCHEMA, DATAFLOW_SEALED_SCHEMA, TRAINING_SEAL_SCHEMA):
        reasons.append("unsupported historical schema: " + binding.schema_version)
    # Original code must still be available; never substitute current code.
    try:
        code_ok = store.inspect(binding.original_code_pin_ref)
    except DataflowError as exc:
        code_ok = False
        reasons.append("original code pin unreadable: " + exc.code)
    if not code_ok:
        reasons.append("original code pin is missing or cannot be verified as a content-addressed "
                       "program; cannot resume without the executed program")
    # Read the original plan to recover its declared recovery guarantee and domains.
    plan = None
    try:
        plan = _store_read_json(store, binding.original_plan_ref, max_bytes=_MAX_SEALED_PLAN_BYTES)
    except DataflowError as exc:
        reasons.append("original sealed plan unreadable: " + exc.code)
    if plan is not None:
        if binding.schema_version in {DATAFLOW_SEALED_SCHEMA, TRAINING_SEAL_SCHEMA}:
            from demiflow.execution.contracts.dataflow_codec import decode_wire, compute_physical_digest
            try:
                from demiflow.execution.seal.sealed_plans import sealed_read_bounds as _v3_read_bounds
                physical = decode_wire(json.dumps(plan, separators=(",", ":")).encode(),
                                       type_name="PhysicalDataflow", bounds=_v3_read_bounds())
                if physical.plan_id != compute_physical_digest(physical):
                    reasons.append("original sealed plan identity differs from history binding")
                recovery = physical.recovery
                domains = sorted({s.required_domain for s in physical.splits})
                profiles = list(stage.profile for stage in physical.stages)
            except DataflowError as exc:
                reasons.append("original physical plan invalid: " + exc.code)
            try:
                pin = _store_read_json(store, binding.original_code_pin_ref,
                                      max_bytes=_MAX_SEALED_PLAN_BYTES)
            except DataflowError as exc:
                reasons.append("original platform code pin unreadable: " + exc.code)
        else:
            recovery = _v1_recovery(plan)
            domain = plan.get("read_domain")
            if type(domain) is str:
                domains = [domain]
            # F1's definition_ref is a semantic business-code identity paired
            # with an archived source location, not a v3 platform/code pin.
            # Reading that archive (or translating a K6 description) does not
            # prove that this runtime can execute the original native plan.
            # Preserve the original references and require the original engine
            # until a listed runtime adapter supplies that evidence.
            reasons.append("native sealed-plan history has no verified current-runtime adapter; "
                           "resume requires the original engine environment")
        if recovery not in ("checkpoint", "final_commit", "delivery_only", "attempt_only"):
            reasons.append("unknown historical recovery guarantee: " + repr(recovery))
            recovery = "attempt_only"
    if binding.restore_image is None:
        reasons.append("historical binding has no journal-proven recovery image")
    if recovery == "attempt_only":
        reasons.append("attempt_only recovery has no resumable guarantee; unfinished work cannot continue")
    if recovery == "delivery_only":
        reasons.append("delivery_only recovery cannot resume computation; only delivery is guaranteed")
    supported = not reasons
    return CapabilityReport(supported=supported, reasons=tuple(reasons),
                            recovery=recovery if recovery in ("checkpoint", "final_commit", "delivery_only", "attempt_only") else "attempt_only",
                            required_domains=tuple(domains), required_profiles=tuple(profiles))

def _v1_recovery(plan):
    recovery = plan.get("recovery")
    if type(recovery) is not dict:
        return "attempt_only"
    terminals = recovery.get("terminals")
    if type(terminals) is list and terminals:
        return "final_commit"
    if recovery.get("checkpoint_interval_ms"):
        return "checkpoint"
    return "delivery_only"
