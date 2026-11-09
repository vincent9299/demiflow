"""K4 DataEnvelope/DataRef and K5 ChannelControl (§9, §10).

Data and control share one channel sequence space. The InputGate is a pure
protocol model for conformance; real channels/transport belong to module C.
Provider SDK locations (ObjectRef/PID/local paths) are transient and never
constitute recoverable identity on their own.
"""
from collections import deque
import re

from ..recovery_contract import document, identifier, identity
from .errors import ProtocolError

DATA_REF_SCHEMA = "demiflow.data_ref.v1"
DATA_ENVELOPE_SCHEMA = "demiflow.data_envelope.v1"
_SHA256 = re.compile(r"[0-9a-f]{64}\Z")

_REF_KINDS = frozenset({"inline", "shared_memory", "file", "object_store", "ray_object"})
_REF_KEYS = frozenset({
    "schema_version", "ref_id", "kind", "read_domain", "location", "bytes",
    "sha256", "durability", "scope_ref",
})
_ENVELOPE_KEYS = frozenset({
    "schema_version", "channel_id", "sequence", "partition_id", "route_epoch",
    "producer", "records", "bounds",
})
_RECORD_KEYS = frozenset({"record_id", "parent_record_ids", "occurrence_id", "size_bytes", "payload"})
_RECORD_OPTIONAL = frozenset({"schema_ref", "checksum"})


def _digest(value, label):
    if type(value) is not str or _SHA256.fullmatch(value) is None:
        raise ProtocolError("INVALID_SPEC", label + " must be a sha256 hex digest")
    return value


def data_ref_id(read_domain, location, sha256):
    return identity([read_domain, location, sha256])


def validate_data_ref(value):
    if type(value) is not dict or set(value) != _REF_KEYS:
        raise ProtocolError("INVALID_SPEC", "data ref key set is unsupported")
    if value["schema_version"] != DATA_REF_SCHEMA:
        raise ProtocolError("PROTOCOL_VERSION_UNSUPPORTED",
                            "unsupported data ref schema: " + str(value["schema_version"]))
    if value["kind"] not in _REF_KINDS:
        raise ProtocolError("INVALID_SPEC", "unknown data ref kind")
    if value["durability"] not in ("ephemeral", "durable"):
        raise ProtocolError("INVALID_SPEC", "durability must be declared")
    if value["kind"] in ("ray_object", "shared_memory") and value["durability"] != "ephemeral":
        raise ProtocolError("INVALID_SPEC",
                            "provider-transient refs cannot claim durability")
    if type(value["read_domain"]) is not str or not value["read_domain"]:
        raise ProtocolError("INVALID_SPEC", "read_domain is required")
    if type(value["location"]) is not str or not value["location"]:
        raise ProtocolError("INVALID_SPEC", "location is required")
    if type(value["bytes"]) is not int or value["bytes"] < 0:
        raise ProtocolError("INVALID_SPEC", "bytes must be nonnegative")
    _digest(value["sha256"], "data ref sha256")
    identifier(value["scope_ref"])
    if value["ref_id"] != data_ref_id(value["read_domain"], value["location"], value["sha256"]):
        raise ProtocolError("ARTIFACT_MISMATCH", "ref_id does not bind domain/location/digest")
    document(value)
    return value


def build_data_ref(*, kind, read_domain, location, payload_bytes, sha256, durability, scope_ref):
    value = {
        "schema_version": DATA_REF_SCHEMA, "kind": kind, "read_domain": read_domain,
        "location": location, "bytes": payload_bytes, "sha256": sha256,
        "durability": durability, "scope_ref": scope_ref,
    }
    value["ref_id"] = data_ref_id(read_domain, location, sha256)
    return validate_data_ref(value)


def check_read_domain(ref, reader_domain):
    """Same-named paths in different read domains are not the same object."""
    validate_data_ref(ref)
    if ref["read_domain"] != reader_domain:
        raise ProtocolError("REF_READ_DOMAIN_MISMATCH",
                            "reader domain %r cannot read ref domain %r" % (reader_domain, ref["read_domain"]))
    return True


def record_identity(source_id, source_ordinal, lineage_path=()):
    """Stable logical record identity; chunking/worker count never enter it."""
    identifier(source_id)
    if type(source_ordinal) is not int or source_ordinal < 0:
        raise ProtocolError("INVALID_SPEC", "source_ordinal must be nonnegative")
    path = list(lineage_path)
    if any(type(index) is not int or index < 0 for index in path):
        raise ProtocolError("INVALID_SPEC", "lineage path indices must be nonnegative ints")
    return identity([source_id, source_ordinal, path])


def expanded_lineage(lineage_path, index):
    """Deterministic expansion index for the k-th child output (0/1/N)."""
    if type(index) is not int or index < 0:
        raise ProtocolError("INVALID_SPEC", "expansion index must be nonnegative")
    return list(lineage_path) + [index]


def validate_data_envelope(value):
    if type(value) is not dict or set(value) != _ENVELOPE_KEYS:
        raise ProtocolError("INVALID_SPEC", "data envelope key set is unsupported")
    if value["schema_version"] != DATA_ENVELOPE_SCHEMA:
        raise ProtocolError("PROTOCOL_VERSION_UNSUPPORTED",
                            "unsupported data envelope schema: " + str(value["schema_version"]))
    identifier(value["channel_id"])
    identifier(value["partition_id"])
    if type(value["sequence"]) is not int or value["sequence"] < 0:
        raise ProtocolError("INVALID_SPEC", "sequence must be nonnegative")
    if type(value["route_epoch"]) is not int or value["route_epoch"] < 0:
        raise ProtocolError("INVALID_SPEC", "route_epoch must be nonnegative")
    producer = value["producer"]
    if type(producer) is not dict or set(producer) != {"logical_node_id", "occurrence_id"}:
        raise ProtocolError("INVALID_SPEC", "producer must carry logical identity only")
    identifier(producer["logical_node_id"])
    identifier(producer["occurrence_id"])
    records = value["records"]
    if type(records) is not list:
        raise ProtocolError("INVALID_SPEC", "records must be a list")
    total_bytes = 0
    for record in records:
        if type(record) is not dict or not _RECORD_KEYS <= set(record) \
                or set(record) - _RECORD_KEYS - _RECORD_OPTIONAL:
            raise ProtocolError("INVALID_SPEC", "record key set is unsupported")
        _digest(record["record_id"], "record_id")
        parents = record["parent_record_ids"]
        if type(parents) is not list:
            raise ProtocolError("INVALID_SPEC", "parent_record_ids must be a list")
        for parent in parents:
            _digest(parent, "parent_record_id")
        identifier(record["occurrence_id"])
        if type(record["size_bytes"]) is not int or record["size_bytes"] < 0:
            raise ProtocolError("INVALID_SPEC", "size_bytes must be nonnegative")
        total_bytes += record["size_bytes"]
        if "checksum" in record:
            _digest(record["checksum"], "record checksum")
        if "schema_ref" in record and type(record["schema_ref"]) is not str:
            raise ProtocolError("INVALID_SPEC", "schema_ref must be a string")
        payload = record["payload"]
        if type(payload) is dict and payload.get("schema_version") == DATA_REF_SCHEMA:
            validate_data_ref(payload)
    bounds = value["bounds"]
    if type(bounds) is not dict or set(bounds) != {"rows", "bytes"}:
        raise ProtocolError("INVALID_SPEC", "bounds key set is unsupported")
    if bounds["rows"] != len(records) or bounds["bytes"] != total_bytes:
        raise ProtocolError("INVALID_SPEC", "bounds must match actual records")
    document(value)
    return value


def check_route_epoch(current, observed):
    if type(observed) is not int or observed < current:
        raise ProtocolError("FENCED", "route_epoch %r is older than %r" % (observed, current))


def validate_credit(value):
    if type(value) is not dict or set(value) != {"channel_id", "reader_id", "credits", "route_epoch", "operation_id"}:
        raise ProtocolError("INVALID_SPEC", "credit key set is unsupported")
    identifier(value["channel_id"])
    identifier(value["reader_id"])
    identifier(value["operation_id"])
    credits = value["credits"]
    if type(credits) is not dict or set(credits) != {"rows", "bytes"} \
            or any(type(credits[name]) is not int or credits[name] < 0 for name in ("rows", "bytes")):
        raise ProtocolError("INVALID_SPEC", "credits need nonnegative rows/bytes")
    if type(value["route_epoch"]) is not int or value["route_epoch"] < 0:
        raise ProtocolError("INVALID_SPEC", "route_epoch must be nonnegative")
    document(value)
    return value


def validate_barrier(value):
    if type(value) is not dict or set(value) != {
            "channel_id", "sequence", "checkpoint_id", "epoch", "cut", "topology_revision"}:
        raise ProtocolError("INVALID_SPEC", "barrier key set is unsupported")
    identifier(value["channel_id"])
    identifier(value["checkpoint_id"])
    for name in ("sequence", "epoch", "topology_revision"):
        if type(value[name]) is not int or value[name] < 0:
            raise ProtocolError("INVALID_SPEC", name + " must be nonnegative")
    cut = value["cut"]
    if type(cut) is not dict or not {"producer_sequence_bound"} <= set(cut) \
            or set(cut) - {"producer_sequence_bound", "source_progress_ref"}:
        raise ProtocolError("INVALID_SPEC", "barrier cut is unsupported")
    if type(cut["producer_sequence_bound"]) is not int or cut["producer_sequence_bound"] < 0:
        raise ProtocolError("INVALID_SPEC", "producer_sequence_bound must be nonnegative")
    if cut["producer_sequence_bound"] > value["sequence"]:
        raise ProtocolError("INVALID_SPEC", "cut cannot exceed the barrier sequence")
    document(value)
    return value


def validate_seal(value):
    if type(value) is not dict or set(value) != {
            "channel_id", "sequence", "seal_scope", "manifest_ref", "status"}:
        raise ProtocolError("INVALID_SPEC", "seal key set is unsupported")
    identifier(value["channel_id"])
    if type(value["sequence"]) is not int or value["sequence"] < 0:
        raise ProtocolError("INVALID_SPEC", "sequence must be nonnegative")
    scope = value["seal_scope"]
    if type(scope) is not dict or set(scope) != {"scope_id", "membership", "generation"}:
        raise ProtocolError("INVALID_SPEC", "seal_scope key set is unsupported")
    identifier(scope["scope_id"])
    if type(scope["membership"]) is not list:
        raise ProtocolError("INVALID_SPEC", "seal membership must be a list")
    for member in scope["membership"]:
        identifier(member)
    if type(scope["generation"]) is not int or scope["generation"] < 0:
        raise ProtocolError("INVALID_SPEC", "generation must be nonnegative")
    ref = value["manifest_ref"]
    if type(ref) is not dict or not {"sha256"} <= set(ref) or set(ref) - {"sha256", "path"}:
        raise ProtocolError("INVALID_SPEC", "seal manifest_ref needs a digest")
    _digest(ref["sha256"], "seal manifest sha256")
    if value["status"] != "success":
        raise ProtocolError("INVALID_SPEC", "only successful completion may seal")
    document(value)
    return value


def seal_allowed(producer_statuses):
    """Failure/cancel/idle are never seal; zero-row success may seal."""
    if type(producer_statuses) not in (list, tuple):
        raise ProtocolError("INVALID_SPEC", "producer statuses must be a sequence")
    if any(status != "success" for status in producer_statuses):
        raise ProtocolError("INVALID_SPEC", "failure/cancel/idle cannot seal a scope")
    return True


def validate_end_of_input(value):
    if type(value) is not dict or set(value) != {"channel_id", "sequence", "producer_partition_id", "final"}:
        raise ProtocolError("INVALID_SPEC", "end-of-input key set is unsupported")
    identifier(value["channel_id"])
    identifier(value["producer_partition_id"])
    if type(value["sequence"]) is not int or value["sequence"] < 0:
        raise ProtocolError("INVALID_SPEC", "sequence must be nonnegative")
    if value["final"] is not True:
        raise ProtocolError("INVALID_SPEC", "end-of-input must be final")
    document(value)
    return value


def validate_channel_cancel(value):
    if type(value) is not dict or not {"operation_id", "reason", "controller_epoch"} <= set(value) \
            or set(value) - {"channel_id", "scope_id", "sequence", "operation_id", "reason", "controller_epoch"}:
        raise ProtocolError("INVALID_SPEC", "cancel key set is unsupported")
    if ("channel_id" in value) == ("scope_id" in value):
        raise ProtocolError("INVALID_SPEC", "cancel targets exactly one channel or scope")
    for name in ("channel_id", "scope_id"):
        if name in value:
            identifier(value[name])
    identifier(value["operation_id"])
    if type(value["reason"]) is not str:
        raise ProtocolError("INVALID_SPEC", "reason must be a string")
    if type(value["controller_epoch"]) is not int or value["controller_epoch"] < 0:
        raise ProtocolError("INVALID_SPEC", "controller_epoch must be nonnegative")
    if "sequence" in value and (type(value["sequence"]) is not int or value["sequence"] < 0):
        raise ProtocolError("INVALID_SPEC", "sequence must be nonnegative")
    document(value)
    return value


class ChannelCursor:
    """Consumer-side total order over data and control in one sequence space."""

    def __init__(self):
        self.last = -1

    def accept(self, sequence):
        if type(sequence) is not int or sequence <= self.last:
            raise ProtocolError("SEQUENCE_ORDER_VIOLATION",
                                "sequence %r does not advance past %r" % (sequence, self.last))
        self.last = sequence
        return sequence


class ChannelSequence:
    """Producer-side issuance; data and control draw from the same space."""

    def __init__(self):
        self._next = 0

    def issue(self):
        sequence = self._next
        self._next += 1
        return sequence


class InputGate:
    """Pure aligned-barrier gate model (§10 invariants, no real channels).

    A barrier only takes effect after every pre-cut datum is consumed; while
    alignment is pending, post-barrier data on arrived channels is held and
    other channels keep progressing. Empty/finished channels stay participants
    until the same cut. Buffers are bounded.
    """

    def __init__(self, channels, *, max_pending=1024):
        if type(channels) not in (list, tuple) or not channels or len(set(channels)) != len(channels):
            raise ProtocolError("INVALID_SPEC", "gate needs a fixed nonempty participant set")
        for channel in channels:
            identifier(channel)
        self.max_pending = max_pending
        self._channels = {
            channel: {"cursor": ChannelCursor(), "pre": deque(), "post": deque(),
                      "barrier": None, "eoi": False}
            for channel in channels
        }

    def _channel(self, channel_id):
        state = self._channels.get(channel_id)
        if state is None:
            raise ProtocolError("INVALID_SPEC", "channel is not a gate participant")
        return state

    def push(self, channel_id, event):
        state = self._channel(channel_id)
        if type(event) is not dict or event.get("kind") not in ("data", "barrier", "eoi"):
            raise ProtocolError("INVALID_SPEC", "gate event kind is unsupported")
        sequence = event.get("sequence")
        state["cursor"].accept(sequence)
        if event["kind"] == "data":
            target = state["post"] if state["barrier"] is not None else state["pre"]
            if len(state["pre"]) + len(state["post"]) >= self.max_pending:
                raise ProtocolError("INVALID_SPEC", "alignment buffer exceeds budget")
            target.append(event)
        elif event["kind"] == "barrier":
            validate_barrier({key: item for key, item in event.items() if key != "kind"})
            if state["barrier"] is not None:
                raise ProtocolError("INVALID_SPEC", "channel already holds a barrier")
            cut = event["cut"]["producer_sequence_bound"]
            if any(item["sequence"] >= cut for item in state["pre"]):
                raise ProtocolError("SEQUENCE_ORDER_VIOLATION",
                                    "post-cut data arrived before its barrier")
            state["barrier"] = event
        else:
            validate_end_of_input({key: item for key, item in event.items() if key != "kind"})
            state["eoi"] = True
        return event

    def consume(self, channel_id):
        """Operator consumes the next pre-barrier datum, if any."""
        state = self._channel(channel_id)
        return state["pre"].popleft() if state["pre"] else None

    def barrier_effect(self, channel_id):
        state = self._channel(channel_id)
        if state["barrier"] is None:
            raise ProtocolError("INVALID_SPEC", "no barrier has arrived")
        if state["pre"]:
            raise ProtocolError("BARRIER_OVERTAKE",
                                "pre-barrier data is not fully consumed")
        return state["barrier"]

    def aligned(self, checkpoint_id):
        return all(
            state["barrier"] is not None and state["barrier"]["checkpoint_id"] == checkpoint_id
            and not state["pre"]
            for state in self._channels.values()
        )

    def release(self, checkpoint_id):
        if not self.aligned(checkpoint_id):
            raise ProtocolError("BARRIER_OVERTAKE",
                                "alignment incomplete; post-barrier data must stay held")
        released = 0
        for state in self._channels.values():
            state["barrier"] = None
            released += len(state["post"])
            state["pre"].extend(state["post"])
            state["post"].clear()
        return released