"""Lossless v3 projections to the frozen K4/K5 exchange contracts."""
from demiflow.execution.contracts.record_codec import PAYLOAD_SCHEMA, RECORD_ENCODING, _DATA_MEDIA_TYPES, _digest, _document, _encode_payload, _fail, _ident, _nonneg_int, _ref_document, _validate_artifact_ref, _validate_record
import base64


from demiflow.execution.contracts.dataflow import CutRequest, ExchangeSpec

from demiflow.execution.contracts.exchange import (
    DATA_ENVELOPE_SCHEMA, build_data_ref, check_read_domain, validate_barrier,
    validate_data_envelope,
)
from demiflow.execution.contracts.recovery_contract import MAX_DOCUMENT_BYTES, RecoveryContractError


_MODES = frozenset({"memory_incremental", "durable_incremental", "finite_sealed"})
_ROUTINGS = frozenset({"forward", "hash", "rebalance", "broadcast", "target"})
_PLAN_KEYS = frozenset({
    "channel_id", "edge_id", "exchange_mode", "routing", "keys",
    "producer_partitions", "consumer_partitions", "route_epoch",
    "credit_budget", "storage_bytes", "read_domain", "scope_id", "reason",
})


# -- X012 ---------------------------------------------------------------------

def to_channel_plan_v1(spec):
    """Lossless projection of a settled v3 ExchangeSpec onto the frozen K4 plan.

    The closed key set carries the full reader set and producer membership;
    nothing is added to the v1 validators and nothing v3 needs is dropped.
    """
    if not isinstance(spec, ExchangeSpec):
        _fail("INVALID_CONTRACT", "spec must be an ExchangeSpec")
    _ident(spec.channel_id, "channel_id")
    _digest(spec.edge_id, "edge_id")
    if spec.mode not in _MODES:
        _fail("INVALID_CONTRACT", "unsupported exchange mode")
    if spec.routing not in _ROUTINGS:
        _fail("INVALID_CONTRACT", "unsupported routing kind")
    for label, partitions in (("producer", spec.producer_partitions),
                              ("consumer", spec.consumer_partitions)):
        if not partitions:
            _fail("INVALID_CONTRACT", "channel has no " + label + " partition")
        if len(set(partitions)) != len(partitions):
            _fail("INVALID_CONTRACT", "duplicate " + label + " partition")
        for partition in partitions:
            _ident(partition, label + " partition")
    if type(spec.keys) is not tuple:
        _fail("INVALID_CONTRACT", "keys must be a tuple")
    for key in spec.keys:
        if type(key) is not str or not key or len(key) > 128:
            _fail("INVALID_CONTRACT", "routing key must be a bounded field name")
    if spec.routing in ("hash", "target") and not spec.keys:
        _fail("INVALID_CONTRACT", spec.routing + " routing needs declared keys")
    _nonneg_int(spec.route_epoch, "route_epoch")
    _nonneg_int(spec.credit_rows, "credit_rows")
    _nonneg_int(spec.credit_bytes, "credit_bytes")
    _nonneg_int(spec.storage_bytes, "storage_bytes")
    if spec.mode == "finite_sealed":
        if spec.storage_bytes <= 0 or not spec.scope_id:
            _fail("INVALID_CONTRACT", "finite_sealed needs storage budget and scope_id")
        _ident(spec.scope_id, "scope_id")
    elif spec.credit_rows <= 0 or spec.credit_bytes <= 0:
        _fail("INVALID_CONTRACT", "incremental channels need positive credit windows")
    elif spec.scope_id is not None:
        _ident(spec.scope_id, "scope_id")
    if type(spec.read_domain) is not str or not spec.read_domain:
        _fail("INVALID_CONTRACT", "read_domain is required")
    if type(spec.reason) is not str:
        _fail("INVALID_CONTRACT", "reason must be a string")
    plan = {
        "channel_id": spec.channel_id,
        "edge_id": spec.edge_id,
        "exchange_mode": spec.mode,
        "routing": spec.routing,
        "keys": list(spec.keys),
        "producer_partitions": list(spec.producer_partitions),
        "consumer_partitions": list(spec.consumer_partitions),
        "route_epoch": spec.route_epoch,
        "credit_budget": {"rows": spec.credit_rows, "bytes": spec.credit_bytes},
        "storage_bytes": spec.storage_bytes,
        "read_domain": spec.read_domain,
        "scope_id": spec.scope_id,
        "reason": spec.reason,
    }
    if set(plan) != _PLAN_KEYS:
        _fail("INVALID_CONTRACT", "channel plan projection key set drifted")
    _document(plan, "channel plan")
    return plan


# -- X013 ---------------------------------------------------------------------

def artifact_to_data_ref(ref, *, reader_domain, owner):
    """Build a validated frozen K4 DataRef for a durable artifact.

    Permanent store objects are object_store/durable; transient Ray locations
    never become DataRefs (they are DataLocation.temporary_locator, owned by
    H014). The ArtifactRef itself is never passed off as a v1 DataRef.
    """
    _validate_artifact_ref(ref, "artifact ref")
    if ref.media_type not in _DATA_MEDIA_TYPES:
        _fail("INVALID_CONTRACT", "artifact media_type is not a data encoding")
    if type(reader_domain) is not str or not reader_domain:
        _fail("INVALID_CONTRACT", "reader_domain is required")
    _ident(owner, "owner")
    value = build_data_ref(kind="object_store", read_domain=reader_domain, location=ref.key,
                           payload_bytes=ref.size_bytes, sha256=ref.digest,
                           durability="durable", scope_ref=owner)
    check_read_domain(value, reader_domain)
    return value


# -- X014 ---------------------------------------------------------------------

def record_to_envelope(record, *, channel_id, route_epoch, sequence):
    """Frame one record as a frozen K4 data envelope with honest encoded size.

    Data and control draw from the same channel sequence space: the caller
    passes the sequence issued by the channel's ChannelSequence. The producer
    slot carries the channel binding (v3 graph identity lives in the sealed
    plan; the frozen envelope signature carries no graph context), and the
    per-record occurrence slot carries the record's stable provenance id.
    """
    _validate_record(record)
    _ident(channel_id, "channel_id")
    _nonneg_int(route_epoch, "route_epoch")
    _nonneg_int(sequence, "sequence")
    if record.inline is not None:
        payload_raw = _encode_payload(record.inline, max_bytes=MAX_DOCUMENT_BYTES)
        if len(payload_raw) != record.encoded_bytes:
            _fail("INVALID_CONTRACT", "record encoded_bytes misreports the inline payload size")
        payload = {"schema_version": PAYLOAD_SCHEMA, "encoding": RECORD_ENCODING,
                   "data": base64.b64encode(payload_raw).decode("ascii"), "ref": None,
                   "encoded_bytes": record.encoded_bytes}
    else:
        payload = {"schema_version": PAYLOAD_SCHEMA, "encoding": "ref", "data": None,
                   "ref": _ref_document(record.payload_ref),
                   "encoded_bytes": record.encoded_bytes}
    parents = record.source_position.get("parent_record_ids", [])
    if type(parents) not in (list, tuple):
        _fail("INVALID_CONTRACT", "parent_record_ids must be a sequence")
    for parent in parents:
        _digest(parent, "parent_record_id")
    envelope = {
        "schema_version": DATA_ENVELOPE_SCHEMA,
        "channel_id": channel_id,
        "sequence": sequence,
        "partition_id": "p0",
        "route_epoch": route_epoch,
        "producer": {"logical_node_id": channel_id, "occurrence_id": channel_id},
        "records": [{
            "record_id": record.record_id,
            "parent_record_ids": list(parents),
            "occurrence_id": record.record_id,
            "size_bytes": record.encoded_bytes,
            "payload": payload,
        }],
        "bounds": {"rows": 1, "bytes": record.encoded_bytes},
    }
    try:
        validate_data_envelope(envelope)
    except RecoveryContractError as exc:
        _fail("INVALID_CONTRACT", "data envelope violates the frozen K4 contract")
    return envelope


# -- X015 ---------------------------------------------------------------------

def cut_to_barrier(request, *, channel_id, sequence):
    """Map a v3 CutRequest onto the frozen barrier format without changing it.

    The producer bound is the barrier's own sequence: every datum issued before
    the barrier is pre-cut, so post-cut data can never overtake it (the frozen
    InputGate enforces this). Empty/EOI inputs align on the same barrier. The
    cut's participants and final flag stay with the driver's cut authority and
    are not stuffed into the channel barrier.
    """
    if not isinstance(request, CutRequest):
        _fail("INVALID_CONTRACT", "request must be a CutRequest")
    _ident(request.cut_id, "cut_id")
    _nonneg_int(request.sequence, "cut sequence")
    _nonneg_int(request.plan_revision, "plan_revision")
    _nonneg_int(request.route_epoch, "route_epoch")
    _ident(channel_id, "channel_id")
    _nonneg_int(sequence, "barrier sequence")
    value = {
        "channel_id": channel_id,
        "sequence": sequence,
        "checkpoint_id": request.cut_id,
        "epoch": request.route_epoch,
        "cut": {"producer_sequence_bound": sequence},
        "topology_revision": request.plan_revision,
    }
    try:
        validate_barrier(value)
    except RecoveryContractError as exc:
        _fail("INVALID_CONTRACT", "barrier violates the frozen K5 contract")
    return value
