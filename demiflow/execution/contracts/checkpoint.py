"""K6 CheckpointManifest and explicit legacy read/reject rules (§11).

Global completion authority stays with the existing RunJournal/
CommitCoordinator (module D); this module only fixes the durable description
and its coverage validation. A single source ordinal never impersonates DAG
coverage: the single-source/single-partition/no-channel case is an explicit
degenerate vector, not a general assumption.
"""

from demiflow.execution.contracts.dataflow import ArtifactRef, CutContribution, CutRequest, SourceCursor
from demiflow.execution.contracts.dataflow_errors import DataflowError
import re

from ..recovery_contract import (
    MAX_STATES, document, identifier, identity, RecoveryContractError,
)
from .errors import ProtocolError

CHECKPOINT_SCHEMA = "demiflow.checkpoint_manifest.v1"
_SCHEMA_PATTERN = re.compile(r"demiflow\.checkpoint_manifest\.v([0-9]+)\Z")
_SHA256 = re.compile(r"[0-9a-f]{64}\Z")

_TOP_KEYS = frozenset({
    "schema_version", "checkpoint_id", "run_id", "action_id", "epoch",
    "topology_revision", "route_epoch", "plan_revision", "artifact_manifest_ref",
    "participants", "source_progress", "channel_cuts", "operator_states",
    "inflight", "sink_receipts", "seal",
})
_MAX_INFLIGHT = 16384


def check_schema_version(schema_version):
    """Unknown majors reject explicitly; no silent migration."""
    match = _SCHEMA_PATTERN.fullmatch(schema_version) if type(schema_version) is str else None
    if match is None or int(match.group(1)) != 1:
        raise ProtocolError("CHECKPOINT_SCHEMA_INCOMPATIBLE",
                            "unsupported checkpoint schema: " + str(schema_version))
    return schema_version


def _digest(value, label):
    if type(value) is not str or _SHA256.fullmatch(value) is None:
        raise ProtocolError("INVALID_SPEC", label + " must be a sha256 hex digest")
    return value


def _source_key(entry):
    return (entry["source_id"], entry["partition_id"])


def validate_participants(value):
    if type(value) is not dict or set(value) != {"sources", "channels", "operators", "sinks"}:
        raise ProtocolError("INVALID_SPEC", "participants key set is unsupported")
    sources = value["sources"]
    if type(sources) is not list or not sources:
        raise ProtocolError("INVALID_SPEC", "participants.sources must be a nonempty list")
    seen = set()
    for entry in sources:
        if type(entry) is not dict or set(entry) != {"source_id", "partition_id"}:
            raise ProtocolError("INVALID_SPEC", "source participant key set is unsupported")
        identifier(entry["source_id"])
        identifier(entry["partition_id"])
        key = _source_key(entry)
        if key in seen:
            raise ProtocolError("INVALID_SPEC", "duplicate source participant")
        seen.add(key)
    channels = value["channels"]
    if type(channels) is not list or any(type(item) is not str for item in channels):
        raise ProtocolError("INVALID_SPEC", "participants.channels must be ids")
    for item in channels:
        identifier(item)
    if len(set(channels)) != len(channels):
        raise ProtocolError("INVALID_SPEC", "duplicate channel participant")
    operators = value["operators"]
    if type(operators) is not list:
        raise ProtocolError("INVALID_SPEC", "participants.operators must be a list")
    for entry in operators:
        if type(entry) is not dict or not {"logical_node_id", "occurrence_id"} <= set(entry) \
                or set(entry) - {"logical_node_id", "occurrence_id", "partition_id"}:
            raise ProtocolError("INVALID_SPEC", "operator participant key set is unsupported")
        identifier(entry["logical_node_id"])
        identifier(entry["occurrence_id"])
        if "partition_id" in entry:
            identifier(entry["partition_id"])
    sinks = value["sinks"]
    if type(sinks) is not list:
        raise ProtocolError("INVALID_SPEC", "participants.sinks must be a list")
    for item in sinks:
        identifier(item)
    if len(set(sinks)) != len(sinks):
        raise ProtocolError("INVALID_SPEC", "duplicate sink participant")
    return value


def validate_checkpoint_manifest(value, *, topology=None):
    """Coverage is checked against the manifest's own participant set and,
    when supplied, the sealed plan topology fixed by topology_revision."""
    if type(value) is not dict or set(value) != _TOP_KEYS:
        raise ProtocolError("INVALID_SPEC", "checkpoint manifest key set is unsupported")
    check_schema_version(value["schema_version"])
    for name in ("checkpoint_id", "run_id", "action_id"):
        identifier(value[name])
    for name in ("epoch", "topology_revision", "route_epoch", "plan_revision"):
        if type(value[name]) is not int or value[name] < 0:
            raise ProtocolError("INVALID_SPEC", name + " must be nonnegative")
    ref = value["artifact_manifest_ref"]
    if type(ref) is not dict or set(ref) != {"manifest_id", "sha256"}:
        raise ProtocolError("INVALID_SPEC", "artifact_manifest_ref needs manifest_id/sha256")
    identifier(ref["manifest_id"])
    _digest(ref["sha256"], "artifact_manifest_ref.sha256")
    participants = validate_participants(value["participants"])
    if topology is not None:
        if type(topology) is not dict or set(topology) != {"sources", "channels"}:
            raise ProtocolError("INVALID_SPEC", "topology needs sources/channels")
        expected_sources = {_source_key(entry) for entry in topology["sources"]}
        actual_sources = {_source_key(entry) for entry in participants["sources"]}
        if expected_sources != actual_sources or set(topology["channels"]) != set(participants["channels"]):
            raise ProtocolError("CHECKPOINT_PARTICIPANT_MISSING",
                                "participant set differs from the sealed topology")
    progress = value["source_progress"]
    if type(progress) is not list:
        raise ProtocolError("INVALID_SPEC", "source_progress must be a list")
    progress_keys = set()
    for entry in progress:
        if type(entry) is not dict or set(entry) != {"source_id", "partition_id", "position"}:
            raise ProtocolError("INVALID_SPEC", "source progress key set is unsupported")
        identifier(entry["source_id"])
        identifier(entry["partition_id"])
        position = entry["position"]
        if type(position) is not dict or len(position) != 1:
            raise ProtocolError("INVALID_SPEC", "position needs exactly next_ordinal or cursor_ref")
        if "next_ordinal" in position:
            if type(position["next_ordinal"]) is not int or not 0 <= position["next_ordinal"] < 2**63:
                raise ProtocolError("INVALID_SPEC", "next_ordinal is invalid")
        elif "cursor_ref" in position:
            identifier(position["cursor_ref"])
        else:
            raise ProtocolError("INVALID_SPEC", "unknown position kind")
        key = _source_key(entry)
        if key in progress_keys:
            raise ProtocolError("INVALID_SPEC", "duplicate source progress entry")
        progress_keys.add(key)
    expected = {_source_key(entry) for entry in participants["sources"]}
    if progress_keys != expected:
        raise ProtocolError("CHECKPOINT_PARTICIPANT_MISSING",
                            "source_progress does not cover every source participant")
    cuts = value["channel_cuts"]
    if type(cuts) is not list:
        raise ProtocolError("INVALID_SPEC", "channel_cuts must be a list")
    cut_channels = set()
    for entry in cuts:
        if type(entry) is not dict or set(entry) != {"channel_id", "producer_cut_sequence", "consumer_offsets"}:
            raise ProtocolError("INVALID_SPEC", "channel cut key set is unsupported")
        identifier(entry["channel_id"])
        if type(entry["producer_cut_sequence"]) is not int or entry["producer_cut_sequence"] < 0:
            raise ProtocolError("INVALID_SPEC", "producer_cut_sequence must be nonnegative")
        offsets = entry["consumer_offsets"]
        if type(offsets) is not dict or any(
                type(reader) is not str or type(offset) is not int or offset < 0
                for reader, offset in offsets.items()):
            raise ProtocolError("INVALID_SPEC", "consumer offsets are invalid")
        if entry["channel_id"] in cut_channels:
            raise ProtocolError("INVALID_SPEC", "duplicate channel cut")
        cut_channels.add(entry["channel_id"])
    if cut_channels != set(participants["channels"]):
        raise ProtocolError("CHECKPOINT_PARTICIPANT_MISSING",
                            "channel_cuts do not cover every channel participant")
    operator_keys = {
        (entry["logical_node_id"], entry["occurrence_id"], entry.get("partition_id"))
        for entry in participants["operators"]
    }
    states = value["operator_states"]
    if type(states) is not list or len(states) > MAX_STATES:
        raise ProtocolError("INVALID_SPEC", "operator_states exceed the state budget")
    for entry in states:
        if type(entry) is not dict or not {"logical_node_id", "occurrence_id", "state_ref"} <= set(entry) \
                or set(entry) - {"logical_node_id", "occurrence_id", "partition_id",
                                 "state_ref", "state_schema_version"}:
            raise ProtocolError("INVALID_SPEC", "operator state key set is unsupported")
        key = (entry["logical_node_id"], entry["occurrence_id"], entry.get("partition_id"))
        if key not in operator_keys:
            raise ProtocolError("CHECKPOINT_PARTICIPANT_MISSING",
                                "operator state has no matching participant")
        state_ref = entry["state_ref"]
        if type(state_ref) is not dict or set(state_ref) != {"state_id", "path", "bytes", "sha256"}:
            raise ProtocolError("INVALID_SPEC", "state_ref key set is unsupported")
        identifier(state_ref["state_id"])
        _digest(state_ref["sha256"], "state_ref.sha256")
        if type(state_ref["bytes"]) is not int or state_ref["bytes"] < 0 or type(state_ref["path"]) is not str:
            raise ProtocolError("INVALID_SPEC", "state_ref bytes/path are invalid")
        if "state_schema_version" in entry and (
                type(entry["state_schema_version"]) is not int or entry["state_schema_version"] < 1):
            raise ProtocolError("INVALID_SPEC", "state_schema_version must be a positive int")
    inflight = value["inflight"]
    if type(inflight) is not list or len(inflight) > _MAX_INFLIGHT:
        raise ProtocolError("INVALID_SPEC", "inflight descriptions exceed budget")
    for entry in inflight:
        if type(entry) is not dict or not {"request_key", "status"} <= set(entry) \
                or set(entry) - {"request_key", "status", "effect_ref"}:
            raise ProtocolError("INVALID_SPEC", "inflight key set is unsupported")
        identifier(entry["request_key"])
        if entry["status"] not in ("known_completed", "unknown"):
            raise ProtocolError("INVALID_SPEC", "inflight status is unsupported")
        if entry["status"] == "known_completed" and "effect_ref" not in entry:
            raise ProtocolError("INVALID_SPEC", "known completion needs an effect reference")
        if "effect_ref" in entry:
            identifier(entry["effect_ref"])
    receipts = value["sink_receipts"]
    if type(receipts) is not list:
        raise ProtocolError("INVALID_SPEC", "sink_receipts must be a list")
    for entry in receipts:
        if type(entry) is not dict or set(entry) != {"sink_id", "commit_id", "receipt_ref"}:
            raise ProtocolError("INVALID_SPEC", "sink receipt key set is unsupported")
        identifier(entry["sink_id"])
        identifier(entry["commit_id"])
        identifier(entry["receipt_ref"])
        if entry["sink_id"] not in participants["sinks"]:
            raise ProtocolError("CHECKPOINT_PARTICIPANT_MISSING",
                                "sink receipt has no matching participant")
    seal = value["seal"]
    if type(seal) is not dict or not {"sealed"} <= set(seal) or set(seal) - {"sealed", "digest"} \
            or type(seal["sealed"]) is not bool:
        raise ProtocolError("INVALID_SPEC", "seal declaration is unsupported")
    if seal["sealed"] and "digest" not in seal:
        raise ProtocolError("INVALID_SPEC", "sealed checkpoint needs a digest")
    if "digest" in seal:
        _digest(seal["digest"], "checkpoint seal digest")
    document(value)
    return value


def checkpoint_id_for(content):
    return identity(content)


def replayable_effects(value):
    """Description-level rule: unknown paid effects are never auto-replayed."""
    validate_checkpoint_manifest(value)
    return [entry["effect_ref"] for entry in value["inflight"]
            if entry["status"] == "known_completed"]


def legacy_checkpoint_fragment(position, *, source_ids, channel_ids=(), partition_count=1):
    """Read a legacy journal single SourcePosition as a degenerate vector.

    Only valid when the sealed plan really is single-source/single-partition
    with no channels; otherwise reject instead of guessing coverage.
    """
    if type(position) is not dict or set(position) != {"source_id", "next_ordinal"}:
        raise ProtocolError("CHECKPOINT_SCHEMA_INCOMPATIBLE",
                            "legacy position is not a SourcePosition")
    try:
        identifier(position["source_id"])
    except RecoveryContractError as error:
        raise ProtocolError("CHECKPOINT_SCHEMA_INCOMPATIBLE", "legacy source_id is invalid") from error
    ordinal = position["next_ordinal"]
    if type(ordinal) is not int or not 0 <= ordinal < 2**63:
        raise ProtocolError("CHECKPOINT_SCHEMA_INCOMPATIBLE", "legacy next_ordinal is invalid")
    if len(source_ids) != 1 or partition_count != 1 or len(channel_ids) != 0 \
            or list(source_ids) != [position["source_id"]]:
        raise ProtocolError("CHECKPOINT_SCHEMA_INCOMPATIBLE",
                            "plan is not a single-source linear chain; single ordinal cannot cover it")
    return {
        "participants": {"sources": [{"source_id": position["source_id"], "partition_id": "p0"}],
                         "channels": []},
        "source_progress": [{"source_id": position["source_id"], "partition_id": "p0",
                             "position": {"next_ordinal": ordinal}}],
        "channel_cuts": [],
    }


def recovery_route(run_facts):
    """Historical runs without protocol artifacts stay on the legacy path.

    The new package never force-restores them; routing is a read-only fact.
    """
    if type(run_facts) is not dict:
        raise ProtocolError("INVALID_SPEC", "run facts must be an object")
    if run_facts.get("artifact_manifest_ref") is None:
        return "legacy"
    return "protocol"


def artifact_json(ref: ArtifactRef) -> dict:
    return {"digest": ref.digest, "size_bytes": ref.size_bytes,
            "store_id": ref.store_id, "key": ref.key, "media_type": ref.media_type}


def artifact_from_json(value) -> ArtifactRef:
    if type(value) is not dict or set(value) != {
            "digest", "size_bytes", "store_id", "key", "media_type"}:
        raise DataflowError("CORRUPT_ARTIFACT", "Invalid artifact reference in recovery image")
    return ArtifactRef(digest=value["digest"], size_bytes=value["size_bytes"],
                       store_id=value["store_id"], key=value["key"],
                       media_type=value["media_type"])


def cursor_json(cursor: SourceCursor) -> dict:
    from demiflow.execution.contracts.dataflow import thaw_record_value
    return {"split_id": cursor.split_id, "offset": thaw_record_value(cursor.offset),
            "next_sequence": cursor.next_sequence, "end_of_input": cursor.end_of_input,
            "lease_ref": artifact_json(cursor.lease_ref) if cursor.lease_ref is not None else None,
            "pending_refs": [artifact_json(r) for r in cursor.pending_refs]}


def cursor_from_json(value) -> SourceCursor:
    if type(value) is not dict or set(value) != {
            "split_id", "offset", "next_sequence", "end_of_input", "lease_ref", "pending_refs"}:
        raise DataflowError("CORRUPT_ARTIFACT", "Invalid source cursor in recovery image")
    return SourceCursor(
        split_id=value["split_id"], offset=value["offset"],
        next_sequence=value["next_sequence"], end_of_input=value["end_of_input"],
        lease_ref=artifact_from_json(value["lease_ref"]) if value["lease_ref"] is not None else None,
        pending_refs=tuple(artifact_from_json(r) for r in value["pending_refs"]))


def cut_json(cut: CutRequest) -> dict:
    return {"cut_id": cut.cut_id, "sequence": cut.sequence,
            "plan_revision": cut.plan_revision, "route_epoch": cut.route_epoch,
            "participants": list(cut.participants), "final": cut.final}


def cut_from_json(value) -> CutRequest:
    if type(value) is not dict or set(value) != {
            "cut_id", "sequence", "plan_revision", "route_epoch", "participants", "final"}:
        raise DataflowError("CORRUPT_ARTIFACT", "Invalid cut request in recovery image")
    return CutRequest(cut_id=value["cut_id"], sequence=value["sequence"],
                      plan_revision=value["plan_revision"], route_epoch=value["route_epoch"],
                      participants=tuple(value["participants"]), final=value["final"])


def contribution_json(contribution: CutContribution) -> dict:
    from demiflow.execution.contracts.dataflow import thaw_record_value
    return {"cut_id": contribution.cut_id, "participant_id": contribution.participant_id,
            "boot_id": contribution.boot_id,
            "sources": [cursor_json(c) for c in contribution.sources],
            "operator_states": {k: artifact_json(v) for k, v in contribution.operator_states.items()},
            "channels": thaw_record_value(contribution.channels),
            "buffers": [artifact_json(r) for r in contribution.buffers],
            "effects": [artifact_json(r) for r in contribution.effects],
            "sinks": {k: artifact_json(v) for k, v in contribution.sinks.items()},
            "final": contribution.final}


def contribution_from_json(value) -> CutContribution:
    if type(value) is not dict or set(value) != {
            "cut_id", "participant_id", "boot_id", "sources", "operator_states",
            "channels", "buffers", "effects", "sinks", "final"}:
        raise DataflowError("CORRUPT_ARTIFACT", "Invalid cut contribution in recovery image")
    return CutContribution(
        cut_id=value["cut_id"], participant_id=value["participant_id"],
        boot_id=value["boot_id"],
        sources=tuple(cursor_from_json(c) for c in value["sources"]),
        operator_states={k: artifact_from_json(v) for k, v in value["operator_states"].items()},
        channels={k: dict(v) for k, v in value["channels"].items()},
        buffers=tuple(artifact_from_json(r) for r in value["buffers"]),
        effects=tuple(artifact_from_json(r) for r in value["effects"]),
        sinks={k: artifact_from_json(v) for k, v in value["sinks"].items()},
        final=value["final"])


def contribution_digest(contribution: CutContribution) -> str:
    return identity(contribution_json(contribution))
