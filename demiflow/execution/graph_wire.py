"""Additive graph-mode wire family (U3/S3, D3/D4 rulings).

Lives in the execution layer (like the U2 CommitCut precedent in chain.py), NOT
in ``execution/contracts/`` which stays byte-frozen. Three schemas:

* ``graph_checkpoint.v1`` — driver -> worker global cut command. The K3
  CheckpointRequest key set is closed (contracts/worker.py), so the ``final``
  flag rides here; the graph worker runtime handles it and converges E1
  ``pending_cut`` internally. K3 itself is untouched (U2 path keeps using it).
* ``cut_contribution.v1`` — worker -> driver per-cut K6 fragment. channel_cuts /
  operator_states / inflight use the frozen K6 entry shapes so the driver can
  assemble a full-coverage CheckpointManifest and commit once.
* ``capacity_request.v1`` — worker -> driver credit/storage escalation.
"""
from .contracts.errors import ProtocolError
from .recovery_contract import MAX_STATES, document, identifier

GRAPH_CHECKPOINT_SCHEMA = "demiflow.graph_checkpoint.v1"
CUT_CONTRIBUTION_SCHEMA = "demiflow.cut_contribution.v1"
CAPACITY_REQUEST_SCHEMA = "demiflow.capacity_request.v1"
_MAX_INFLIGHT = 16384


def _nonnegative_int(value, label):
    if type(value) is not int or value < 0:
        raise ProtocolError("INVALID_SPEC", label + " must be a nonnegative int")


def validate_graph_checkpoint(value):
    if type(value) is not dict or set(value) != {
            "schema_version", "checkpoint_id", "epoch", "cut", "topology_revision",
            "deadline_ms", "final"}:
        raise ProtocolError("INVALID_SPEC", "graph checkpoint key set is unsupported")
    if value["schema_version"] != GRAPH_CHECKPOINT_SCHEMA:
        raise ProtocolError("PROTOCOL_VERSION_UNSUPPORTED", "unsupported graph checkpoint schema")
    identifier(value["checkpoint_id"])
    for name in ("epoch", "topology_revision", "deadline_ms"):
        _nonnegative_int(value[name], name)
    cut = value["cut"]
    if type(cut) is not dict or set(cut) != {"producer_sequence_bound"}:
        raise ProtocolError("INVALID_SPEC", "graph checkpoint cut is unsupported")
    _nonnegative_int(cut["producer_sequence_bound"], "producer_sequence_bound")
    if type(value["final"]) is not bool:
        raise ProtocolError("INVALID_SPEC", "final must be a bool")
    document(value)
    return value


def build_graph_checkpoint(checkpoint_id, *, epoch, producer_sequence_bound,
                           topology_revision=0, deadline_ms=60000, final=False):
    return validate_graph_checkpoint({
        "schema_version": GRAPH_CHECKPOINT_SCHEMA, "checkpoint_id": checkpoint_id,
        "epoch": epoch, "cut": {"producer_sequence_bound": producer_sequence_bound},
        "topology_revision": topology_revision, "deadline_ms": deadline_ms, "final": final})


def _validate_channel_cut(entry):
    if type(entry) is not dict or set(entry) != {
            "channel_id", "producer_cut_sequence", "consumer_offsets"}:
        raise ProtocolError("INVALID_SPEC", "channel cut key set is unsupported")
    identifier(entry["channel_id"])
    _nonnegative_int(entry["producer_cut_sequence"], "producer_cut_sequence")
    offsets = entry["consumer_offsets"]
    if type(offsets) is not dict or any(
            type(reader) is not str or type(offset) is not int or offset < 0
            for reader, offset in offsets.items()):
        raise ProtocolError("INVALID_SPEC", "consumer offsets are invalid")


def _validate_operator_state(entry):
    if type(entry) is not dict or not {"logical_node_id", "occurrence_id", "state_ref"} <= set(entry) \
            or set(entry) - {"logical_node_id", "occurrence_id", "partition_id",
                             "state_ref", "state_schema_version"}:
        raise ProtocolError("INVALID_SPEC", "operator state key set is unsupported")
    identifier(entry["logical_node_id"])
    identifier(entry["occurrence_id"])
    ref = entry["state_ref"]
    if type(ref) is not dict or set(ref) != {"state_id", "path", "bytes", "sha256"}:
        raise ProtocolError("INVALID_SPEC", "state_ref key set is unsupported")
    identifier(ref["state_id"])
    if type(ref["bytes"]) is not int or ref["bytes"] < 0 or type(ref["path"]) is not str:
        raise ProtocolError("INVALID_SPEC", "state_ref bytes/path are invalid")


def _validate_inflight(entry):
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


def _validate_source_progress(entry):
    if type(entry) is not dict or set(entry) != {"source_id", "partition_id", "position"}:
        raise ProtocolError("INVALID_SPEC", "source progress key set is unsupported")
    identifier(entry["source_id"])
    identifier(entry["partition_id"])
    position = entry["position"]
    # Align the frozen K6 position rule (contracts/checkpoint.py): exactly one of
    # next_ordinal (fixed re-readable sources) or cursor_ref (an identifier naming a
    # cursor artifact for incremental Lance/file/queue progress). Additive in the
    # execution layer; the contracts family stays byte-frozen (D2).
    if type(position) is not dict or len(position) != 1:
        raise ProtocolError("INVALID_SPEC", "position needs exactly next_ordinal or cursor_ref")
    if "next_ordinal" in position:
        if type(position["next_ordinal"]) is not int or not 0 <= position["next_ordinal"] < 2**63:
            raise ProtocolError("INVALID_SPEC", "next_ordinal is invalid")
    elif "cursor_ref" in position:
        identifier(position["cursor_ref"])
    else:
        raise ProtocolError("INVALID_SPEC", "unknown position kind")


def validate_cut_contribution(value):
    if type(value) is not dict or set(value) != {
            "schema_version", "checkpoint_id", "allocation_id", "channel_cuts",
            "operator_states", "inflight", "output_descriptor", "source_progress", "final"}:
        raise ProtocolError("INVALID_SPEC", "cut contribution key set is unsupported")
    if value["schema_version"] != CUT_CONTRIBUTION_SCHEMA:
        raise ProtocolError("PROTOCOL_VERSION_UNSUPPORTED", "unsupported cut contribution schema")
    identifier(value["checkpoint_id"])
    identifier(value["allocation_id"])
    if type(value["final"]) is not bool:
        raise ProtocolError("INVALID_SPEC", "final must be a bool")
    progress = value["source_progress"]
    if type(progress) is not list:
        raise ProtocolError("INVALID_SPEC", "source_progress must be a list")
    seen_sources = set()
    for entry in progress:
        _validate_source_progress(entry)
        key = (entry["source_id"], entry["partition_id"])
        if key in seen_sources:
            raise ProtocolError("INVALID_SPEC", "duplicate source progress entry")
        seen_sources.add(key)
    cuts = value["channel_cuts"]
    if type(cuts) is not list:
        raise ProtocolError("INVALID_SPEC", "channel_cuts must be a list")
    seen = set()
    for entry in cuts:
        _validate_channel_cut(entry)
        if entry["channel_id"] in seen:
            raise ProtocolError("INVALID_SPEC", "duplicate channel cut")
        seen.add(entry["channel_id"])
    states = value["operator_states"]
    if type(states) is not list or len(states) > MAX_STATES:
        raise ProtocolError("INVALID_SPEC", "operator_states exceed the state budget")
    for entry in states:
        _validate_operator_state(entry)
    inflight = value["inflight"]
    if type(inflight) is not list or len(inflight) > _MAX_INFLIGHT:
        raise ProtocolError("INVALID_SPEC", "inflight descriptions exceed budget")
    for entry in inflight:
        _validate_inflight(entry)
    descriptor = value["output_descriptor"]
    if descriptor is not None and (type(descriptor) is not dict
                                   or set(descriptor) != {"blocks", "rows"}):
        raise ProtocolError("INVALID_SPEC", "output_descriptor must be a staging descriptor")
    document(value)
    return value


def validate_capacity_request(value):
    if type(value) is not dict or set(value) != {
            "schema_version", "allocation_id", "channel_id", "rows", "bytes", "reason"}:
        raise ProtocolError("INVALID_SPEC", "capacity request key set is unsupported")
    if value["schema_version"] != CAPACITY_REQUEST_SCHEMA:
        raise ProtocolError("PROTOCOL_VERSION_UNSUPPORTED", "unsupported capacity request schema")
    identifier(value["allocation_id"])
    identifier(value["channel_id"])
    _nonnegative_int(value["rows"], "rows")
    _nonnegative_int(value["bytes"], "bytes")
    if type(value["reason"]) is not str:
        raise ProtocolError("INVALID_SPEC", "reason must be a string")
    document(value)
    return value