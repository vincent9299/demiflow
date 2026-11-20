"""Read-only historical execution binding and plan projection (W001-W004, C12/C13).

History protection is the core deliverable here. The functions read the original
sealed documents the journal points at, validate them by their *original* version
and never recompute a historical identity. A missing or unknown format degrades to
an explicit ``INCOMPATIBLE_HISTORY`` error carrying the original reference; it never
re-projects current code to impersonate a past execution and never creates a
replacement run.

Version dispatch reads the journal directly (``load_execution_version`` does not
call ``load_sealed_plan``/``load_dataflow``) so the v3 loader can consume it without
recursion. ``build_plan_view`` is a pure projection of its in-memory arguments: no
I/O, no recompilation, no observation inference.
"""
from __future__ import annotations

import hashlib
import json
from pathlib import Path

from .contracts.dataflow import (
    ArtifactRef,
    CapabilityReport,
    CutContribution,
    CutRequest,
    DataflowGraph,
    HistoricalBinding,
    NodeObservation,
    PhysicalDataflow,
    RecoveryImage,
    SealedExecution,
    SourceCursor,
)
from .contracts.dataflow_errors import DataflowError
from .recovery_contract import RecoveryContractError, identifier
from .run_journal import RunJournal

# Original sealed-plan facts schema for the native v1/v2 chain/graph generation.
SEALED_PLAN_FACTS_SCHEMA = "demiflow.sealed_plan_facts.v1"
# v3 native seal marker bound by seal_dataflow through the same journal slot family.
from .dataflow_seal import SEAL_SCHEMA as DATAFLOW_SEALED_SCHEMA, _validate_seal_marker
# The only listed native checkpoint format translated into an in-memory image.
CHECKPOINT_MANIFEST_SCHEMA = "demiflow.checkpoint_manifest.v1"
PLAN_VIEW_SCHEMA = "demiflow.plan_view.v3"

_SEALED_PLAN_KEY = "-sealed-plan"
_DATAFLOW_SEALED_KEY = "-dataflow-sealed"
_MAX_SEALED_PLAN_BYTES = 4 * 1024 ** 2
_SEALED_REF_KEYS = ("logical_ref", "physical_ref", "code_pin_ref",
                    "identity_bridge_ref", "constants_ref")
_CODE_PIN_MEDIA = "application/x-demiflow-code-pin"
_JSON_MEDIA = "application/json"
# Run-relative carriers are presented through a run-rooted store view; the store
# domain label is fixed so a historical adapter can resolve them without guessing.
RUN_STORE_ID = "run"


def _err(code, message):
    return DataflowError(code, message)


def _check_action_id(action_id):
    try:
        identifier(action_id)
    except (RecoveryContractError, ValueError) as exc:
        raise _err("INVALID_CONTRACT", "action_id invalid: " + str(exc))


def _check_max_bytes(max_bytes):
    if type(max_bytes) is not int or max_bytes < 0:
        raise _err("INVALID_CONTRACT", "max_bytes must be a nonnegative int")


def _read_carrier(directory, ref, *, max_bytes):
    """Bounded, run-relative, digest-gated read of one v1/v2 carrier."""
    from .sealed_plans import read_plan_artifact
    from .plan_interpreter import PlanContractError
    ceiling = min(max_bytes, _MAX_SEALED_PLAN_BYTES)
    try:
        return read_plan_artifact(directory, ref, max_bytes=ceiling)
    except PlanContractError as exc:
        if exc.reason in ("artifact_too_large",):
            raise _err("BUDGET_EXCEEDED", str(exc.detail))
        if exc.reason in ("unsafe_artifact",):
            raise _err("INVALID_CONTRACT", str(exc.detail))
        raise _err("CORRUPT_ARTIFACT", str(exc.detail))


def _carrier_ref(ref, *, media_type=_JSON_MEDIA):
    """Present a run-relative {path, sha256, bytes} carrier as an ArtifactRef."""
    return ArtifactRef(digest=ref["sha256"], size_bytes=ref["bytes"],
                       store_id=RUN_STORE_ID, key=ref["path"], media_type=media_type)


def _ref_from_json(value, label):
    if type(value) is not dict or set(value) != {"digest", "size_bytes", "store_id", "key", "media_type"}:
        raise _err("INVALID_CONTRACT", label + " is not a closed artifact reference")
    try:
        return ArtifactRef(digest=value["digest"], size_bytes=value["size_bytes"],
                           store_id=value["store_id"], key=value["key"],
                           media_type=value["media_type"])
    except DataflowError:
        raise
    except (ValueError, TypeError) as exc:
        raise _err("INVALID_CONTRACT", label + " invalid: " + str(exc))


def _ref_json(ref):
    return {"digest": ref.digest, "size_bytes": ref.size_bytes, "store_id": ref.store_id,
            "key": ref.key, "media_type": ref.media_type}


def _store_read_json(store, ref, *, max_bytes):
    """Bounded read of one store artifact into a JSON object."""
    chunks = []
    total = 0
    for chunk in store.read(ref, max_bytes=max_bytes):
        total += len(chunk)
        if total > max_bytes:
            raise _err("BUDGET_EXCEEDED", "artifact exceeds the read budget")
        chunks.append(chunk)
    raw = b"".join(chunks)
    if hashlib.sha256(raw).hexdigest() != ref.digest:
        raise _err("CORRUPT_ARTIFACT", "artifact digest differs from its reference")
    try:
        value = json.loads(raw)
    except ValueError:
        raise _err("CORRUPT_ARTIFACT", "artifact is not valid JSON")
    if type(value) is not dict:
        raise _err("INVALID_CONTRACT", "artifact is not a JSON object")
    return value


# --- W001 ------------------------------------------------------------------

def load_execution_version(directory: Path, action_id: str, *, max_bytes: int) -> HistoricalBinding:
    """Read the original sealed facts for one action and bind them by version.

    Reads the journal-pointed documents directly (no ``load_sealed_plan`` /
    ``load_dataflow`` callback). v1/v2 chain facts and the v3 native seal marker
    are validated by their own original rules; a historical identity is never
    recomputed. Unknown/future schemas and absent facts raise
    ``INCOMPATIBLE_HISTORY`` rather than fabricating a current projection.
    """
    _check_action_id(action_id)
    _check_max_bytes(max_bytes)
    directory = Path(directory)
    journal = RunJournal(directory)
    if not journal.path.is_file():
        raise _err("INCOMPATIBLE_HISTORY", "run journal is absent; no sealed execution facts")
    v3 = journal.source_binding(action_id + _DATAFLOW_SEALED_KEY)
    v1 = journal.source_binding(action_id + _SEALED_PLAN_KEY)
    if v3 is not None and v1 is not None:
        raise _err("INVALID_CONTRACT", "action carries both v3 and v1/v2 sealed facts")
    if v3 is not None:
        return _load_v3(journal, directory, action_id, v3)
    if v1 is not None:
        return _load_v1v2(journal, directory, action_id, v1, max_bytes=max_bytes)
    raise _err("INCOMPATIBLE_HISTORY",
               "action has no sealed plan facts (pre-seal run); cannot bind a historical execution")


def _engine_run_id(journal):
    binding = journal.recovery_identity()
    return binding.get("run_id") if type(binding) is dict else None


def _load_v3(journal, directory, action_id, marker):
    _validate_seal_marker(marker)
    schema = marker["schema"]
    if marker["run_id"] != _engine_run_id(journal):
        raise _err("INVALID_CONTRACT", "v3 seal run_id differs from the journal recovery identity")
    if marker["action_id"] != action_id:
        raise _err("INVALID_CONTRACT", "v3 seal action_id differs from the requested action")
    for key in ("dag_id", "plan_id"):
        try:
            identifier(marker[key])
        except (RecoveryContractError, ValueError) as exc:
            raise _err("INVALID_CONTRACT", key + " invalid: " + str(exc))
    refs = {key: _ref_from_json(marker[key], key) for key in _SEALED_REF_KEYS}
    programs = tuple(_ref_from_json(item, "programs[]") for item in _ref_list(marker["programs"]))
    source_refs = tuple(_ref_from_json(item, "source_refs[]") for item in _ref_list(marker["source_refs"]))
    sealed = SealedExecution(logical_ref=refs["logical_ref"], physical_ref=refs["physical_ref"],
                             programs=programs, code_pin_ref=refs["code_pin_ref"],
                             identity_bridge_ref=refs["identity_bridge_ref"],
                             source_refs=source_refs, constants_ref=refs["constants_ref"])
    from .sealed_plans import _v3_read_store
    from .dataflow_seal import load_sealed_dataflow
    store, bounds = _v3_read_store(directory, store_id=sealed.physical_ref.store_id)
    graph, physical, _ = load_sealed_dataflow(sealed, action_id=action_id, store=store, bounds=bounds)
    if "observation_labels" in marker:
        labels = marker["observation_labels"]
        if (type(labels) is not dict
                or not set(labels) <= {node.occurrence_id for node in graph.nodes}
                or any(type(value) is not str or not value for value in labels.values())
                or len(set(labels.values())) != len(labels)):
            raise _err("INVALID_CONTRACT", "v3 observation labels must uniquely name original occurrences")
        from .recovery_contract import document
        try:
            document(labels, bounds.max_control_bytes)
        except RecoveryContractError as exc:
            raise _err("INVALID_CONTRACT", "v3 observation labels exceed their original control budget") from exc
    if (graph.dag_id != marker["dag_id"] or physical.plan_id != marker["plan_id"]
            or graph.dag_id != marker["graph_digest"] or physical.plan_id != marker["physical_digest"]
            or physical.plan_revision != marker["plan_revision"]):
        raise _err("CORRUPT_ARTIFACT", "sealed facts differ from original graph/physical artifacts")
    return HistoricalBinding(schema_version=schema, original_plan_ref=sealed.physical_ref,
                             original_code_pin_ref=sealed.code_pin_ref, compatible=True,
                             reason=None, restore_image=None)


def _ref_list(value):
    if type(value) is not list:
        raise _err("INVALID_CONTRACT", "sealed reference sequence is not a list")
    return value


def _load_v1v2(journal, directory, action_id, facts, *, max_bytes):
    from ..planning.dag import validate_logical_dag, validate_physical_execution_plan
    if type(facts) is not dict:
        raise _err("INCOMPATIBLE_HISTORY", "sealed plan facts are not a JSON object")
    schema = facts.get("schema")
    if schema != SEALED_PLAN_FACTS_SCHEMA:
        raise _err("INCOMPATIBLE_HISTORY",
                   "unsupported sealed plan facts schema: " + repr(schema))
    expected_keys = {"schema", "run_id", "action_id", "dag_id", "plan_id",
                     "artifact_manifest_ref", "logical_dag_ref", "physical_plan_ref", "sealed_at"}
    if set(facts) != expected_keys:
        raise _err("INVALID_CONTRACT", "sealed plan facts key set is unsupported")
    if facts["run_id"] != _engine_run_id(journal):
        raise _err("INVALID_CONTRACT", "F1 run_id differs from the journal recovery identity")
    if facts["action_id"] != action_id:
        raise _err("INVALID_CONTRACT", "F1 action_id differs from the requested action")
    logical = _read_carrier(directory, facts["logical_dag_ref"], max_bytes=max_bytes)
    physical = _read_carrier(directory, facts["physical_plan_ref"], max_bytes=max_bytes)
    try:
        validate_logical_dag(logical)
        validate_physical_execution_plan(physical)
    except (ValueError, RecoveryContractError) as exc:
        raise _err("INVALID_CONTRACT", "sealed plan failed original validation: " + str(exc))
    if logical["dag_id"] != facts["dag_id"] or physical["plan_id"] != facts["plan_id"]:
        raise _err("INVALID_CONTRACT", "sealed dag_id/plan_id differs from the facts")
    if logical["action_id"] != action_id:
        raise _err("INVALID_CONTRACT", "sealed dag action_id differs from the facts")
    code_pin = _read_code_pin(directory, facts, max_bytes=max_bytes)
    restore_image = _latest_checkpoint_image(journal, directory, action_id, max_bytes=max_bytes)
    return HistoricalBinding(schema_version=schema,
                             original_plan_ref=_carrier_ref(facts["physical_plan_ref"]),
                             original_code_pin_ref=code_pin, compatible=True,
                             reason=None, restore_image=restore_image)


def _read_code_pin(directory, facts, *, max_bytes):
    """Extract the original code-pin identity from the sealed K1 manifest.

    The manifest is content-addressed at the run root by its sha256. The code-pin
    digest is the executed business code identity (``definition_ref``), never a
    recomputed hash of current source. A missing code source is reported later by
    ``validate_resume_compatibility``; here a missing manifest is incompatible.
    """
    manifest_ref = facts["artifact_manifest_ref"]
    if type(manifest_ref) is not dict or set(manifest_ref) != {"manifest_id", "sha256"}:
        raise _err("INVALID_CONTRACT", "manifest_ref needs manifest_id/sha256")
    digest = manifest_ref["sha256"]
    try:
        identifier(digest)
    except (RecoveryContractError, ValueError) as exc:
        raise _err("INVALID_CONTRACT", "manifest sha256 invalid: " + str(exc))
    path = directory / (digest + ".json")
    if any(item.is_symlink() for item in (path, *path.parents)):
        raise _err("INVALID_CONTRACT", "manifest path contains a symlink")
    if not path.is_file():
        raise _err("INCOMPATIBLE_HISTORY", "sealed K1 manifest is missing; original code identity unknown")
    ceiling = min(max_bytes, _MAX_SEALED_PLAN_BYTES)
    if path.stat().st_size > ceiling:
        raise _err("BUDGET_EXCEEDED", "sealed manifest exceeds the read budget")
    raw = path.read_bytes()
    if hashlib.sha256(raw).hexdigest() != digest:
        raise _err("CORRUPT_ARTIFACT", "sealed manifest digest differs")
    try:
        manifest = json.loads(raw)
    except ValueError:
        raise _err("CORRUPT_ARTIFACT", "sealed manifest is not valid JSON")
    try:
        entrypoints = manifest["business"]["entrypoints"]
        definition = entrypoints[0]["definition_ref"]
        code_digest = definition["sha256"]
        location = definition["location"]
    except (KeyError, IndexError, TypeError):
        raise _err("INCOMPATIBLE_HISTORY", "sealed manifest has no code-pin definition_ref")
    try:
        identifier(code_digest)
    except (RecoveryContractError, ValueError) as exc:
        raise _err("INVALID_CONTRACT", "code-pin digest invalid: " + str(exc))
    if type(location) is not str or not location:
        raise _err("INVALID_CONTRACT", "code-pin location is not a bounded string")
    size = 0
    code_path = directory / location
    if not any(item.is_symlink() for item in (code_path, *code_path.parents)) and code_path.is_file():
        size = code_path.stat().st_size
    return ArtifactRef(digest=code_digest, size_bytes=size, store_id=RUN_STORE_ID,
                       key=location, media_type=_CODE_PIN_MEDIA)


def _latest_checkpoint_image(journal, directory, action_id, *, max_bytes):
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


# --- W003 ------------------------------------------------------------------

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


# --- W002 ------------------------------------------------------------------

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
    if binding.schema_version not in (SEALED_PLAN_FACTS_SCHEMA, DATAFLOW_SEALED_SCHEMA):
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
        if binding.schema_version == DATAFLOW_SEALED_SCHEMA:
            from .contracts.dataflow_codec import decode_wire, compute_physical_digest
            try:
                from .sealed_plans import _v3_read_bounds
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


# --- W004 ------------------------------------------------------------------

def build_plan_view(graph: DataflowGraph, sealed: SealedExecution, physical: PhysicalDataflow,
                    *, observations: tuple[NodeObservation, ...]) -> dict:
    """Project the real logical/physical plan, resources, exchange reasons and
    recovery limitations into the workbench's sealed-plan view shape.

    Pure: reads only its arguments, performs no I/O and no recompilation. Each
    occurrence is preserved (the same function at two call sites is two nodes, never
    merged). A node without an observation is explicitly unobserved; success is
    never inferred from the run state. Bridge fields are kept for the identity join.
    """
    if not isinstance(graph, DataflowGraph):
        raise _err("INVALID_CONTRACT", "build_plan_view requires a DataflowGraph")
    if not isinstance(sealed, SealedExecution):
        raise _err("INVALID_CONTRACT", "build_plan_view requires a SealedExecution")
    if not isinstance(physical, PhysicalDataflow):
        raise _err("INVALID_CONTRACT", "build_plan_view requires a PhysicalDataflow")
    if type(observations) is not tuple:
        raise _err("INVALID_CONTRACT", "observations must be a tuple of NodeObservation")
    if graph.dag_id != physical.dag_id:
        raise _err("INVALID_CONTRACT", "graph and physical dag_id differ")
    revision = physical.plan_revision
    if type(revision) is not int or not 0 <= revision <= 2**53 - 1:
        raise _err("INVALID_CONTRACT", "plan_revision must be a non-negative safe integer")
    occurrences = {node.occurrence_id for node in graph.nodes}
    for endpoint in (*graph.sources, *graph.terminals):
        if endpoint not in occurrences:
            raise _err("INVALID_CONTRACT", "source/terminal occurrence is outside the node set")
    obs_by_occurrence = {}
    for obs in observations:
        if not isinstance(obs, NodeObservation):
            raise _err("INVALID_CONTRACT", "observations must contain NodeObservation values")
        if obs.plan_id != physical.plan_id:
            raise _err("INVALID_CONTRACT", "observation plan_id differs from the sealed plan")
        obs_by_occurrence[obs.occurrence_id] = obs
    nodes = []
    output_ports = {}
    for edge in graph.edges:
        output_ports.setdefault(edge.producer, [])
        if edge.output_port not in output_ports[edge.producer]:
            output_ports[edge.producer].append(edge.output_port)
    for node in graph.nodes:
        nodes.append({
            "logical_node_id": node.semantic_id,
            "occurrence_id": node.occurrence_id,
            "op_kind": node.kernel.kind,
            "operator_spec_ref": (_ref_json(node.program_ref) if node.program_ref is not None else None),
            "input_ports": [port.name for port in node.kernel.inputs],
            "output_ports": output_ports.get(node.occurrence_id, []),
            "shared_token": node.shared_token,
            "traits": {"state": node.kernel.state, "effect": node.kernel.effect,
                       "resource_role": node.kernel.resource_role, "mergeable": node.kernel.mergeable},
        })
    edges = [{"edge_id": edge.edge_id,
              "from": {"logical_node_id": _semantic(graph, edge.producer),
                       "occurrence_id": edge.producer, "port": edge.output_port},
              "to": {"logical_node_id": _semantic(graph, edge.consumer),
                     "occurrence_id": edge.consumer, "port": edge.input_port}}
             for edge in graph.edges]
    shared = _shared_ancestors(graph)
    dag = {"schema_version": graph.schema, "dag_id": graph.dag_id, "action_id": graph.action_id,
           "nodes": nodes, "edges": edges, "shared_ancestors": shared,
           "sources": list(graph.sources), "terminals": list(graph.terminals)}
    stage_by_occurrence = {}
    for stage in physical.stages:
        for occ in stage.occurrences:
            stage_by_occurrence[occ] = {
                "stage_id": stage.stage_id, "partition_count": len(stage.partitions),
                "profile": _profile_json(stage.profile),
                "replica_min": stage.replica_min, "replica_target": stage.replica_target,
                "replica_max": stage.replica_max, "resident_bytes": stage.resident_bytes,
                "output_bytes": stage.output_bytes, "state_bytes": stage.state_bytes,
            }
    exchanges = [{"channel_id": ex.channel_id, "edge_id": ex.edge_id, "mode": ex.mode,
                  "routing": ex.routing, "keys": list(ex.keys), "reason": ex.reason,
                  "read_domain": ex.read_domain, "route_epoch": ex.route_epoch,
                  "credit_rows": ex.credit_rows, "credit_bytes": ex.credit_bytes,
                  "storage_bytes": ex.storage_bytes,
                  "producer_count": len(ex.producer_partitions),
                  "consumer_count": len(ex.consumer_partitions)}
                 for ex in physical.exchanges]
    physical_view = {
        "plan_id": physical.plan_id, "plan_revision": physical.plan_revision,
        "recovery": physical.recovery, "limitations": list(physical.limitations),
        "stages": stage_by_occurrence, "exchanges": exchanges,
        "policy": {"provider": physical.policy.provider, "durability": physical.policy.durability,
                   "fusion": physical.policy.fusion, "default_partitions": physical.policy.default_partitions,
                   "max_replicas": physical.policy.max_replicas,
                   "required_recovery": physical.policy.required_recovery},
        "residency": {"simultaneous_groups": [list(g) for g in physical.residency.simultaneous_groups],
                      "control_reserved_bytes": physical.residency.control_reserved_bytes,
                      "consumer_reserved_slots": physical.residency.consumer_reserved_slots,
                      "required_spill_bytes": physical.residency.required_spill_bytes},
        "scopes": [{"scope_id": s.scope_id, "generation": s.generation,
                    "membership": list(s.membership), "completion_condition": s.completion_condition}
                   for s in physical.scopes],
    }
    node_observations = []
    for node in graph.nodes:
        obs = obs_by_occurrence.get(node.occurrence_id)
        if obs is None:
            node_observations.append({"occurrence_id": node.occurrence_id, "observed": False,
                                      "availability": "not_observed", "metrics": None})
        else:
            node_observations.append({"occurrence_id": node.occurrence_id, "attempt": obs.attempt,
                                      "plan_id": obs.plan_id, "observed": bool(obs.observed),
                                      "availability": "observed" if obs.observed else "not_observed",
                                      "metrics": dict(obs.metrics)})
    return {"schema": PLAN_VIEW_SCHEMA, "action_id": graph.action_id, "dag_id": graph.dag_id,
            "plan_id": physical.plan_id, "plan_revision": physical.plan_revision,
            "binding_level": "sealed", "status": "sealed", "dag": dag, "physical": physical_view,
            "node_map": {}, "identity_bridge": {"available": False, "reason": "identity_bridge_missing",
                                                "detail": "build_plan_view keeps the bridge ref; joining is done by the caller"},
            "identity_bridge_ref": _ref_json(sealed.identity_bridge_ref),
            "code_pin_ref": _ref_json(sealed.code_pin_ref),
            "node_observations": node_observations}


def _semantic(graph, occurrence_id):
    for node in graph.nodes:
        if node.occurrence_id == occurrence_id:
            return node.semantic_id
    raise _err("INVALID_CONTRACT", "edge references an unknown occurrence")


def _shared_ancestors(graph):
    consumers = {}
    for edge in graph.edges:
        consumers.setdefault(edge.producer, set()).add(edge.consumer)
    shared = []
    seen = set()
    for node in graph.nodes:
        if len(consumers.get(node.occurrence_id, ())) > 1:
            shared.append({"logical_node_id": node.semantic_id, "occurrence_id": node.occurrence_id})
    return shared


def _profile_json(profile):
    to_dict = getattr(profile, "to_dict", None)
    if callable(to_dict):
        return to_dict()
    requests = getattr(profile, "requests", None)
    if isinstance(requests, dict):
        return {"requests": dict(requests)}
    return {"requests": {}}
