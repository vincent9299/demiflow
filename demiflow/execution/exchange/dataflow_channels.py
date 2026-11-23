"""Channel lifecycle, routing, credit, durability and GC for the v3 exchange.

Module C (lane X) implementation of C05 over the frozen K4/K5 contracts. The
three handoff modes share one channel sequence space and one routing ledger;
memory_incremental sends when ready, durable_incremental publishes a verified
segment before consume, finite_sealed additionally requires a successful scope
seal. Every durable write goes through the ArtifactStore protocol (H031/H035);
no persistent state is created outside these functions and no runtime object is
written into a recovery image.
"""
from demiflow.execution.contracts.dataflow import stage_resident_bytes
import base64
import hashlib
import json
import time
from collections.abc import Mapping

from demiflow.execution.contracts.dataflow import (
    ArtifactRef, Bounds, CutRequest, EncodedRecord, ExchangeSpec,
    ProducerCompletion, Record, RecoveryImage, RoutedRecord,
)
from demiflow.execution.contracts.dataflow_errors import DataflowError
from demiflow.execution.recovery_contract import document, identifier, identity
from demiflow.execution.contracts.record_codec import RECORD_ENCODING, SEAL_MEDIA_TYPE, SEGMENT_MEDIA_TYPE, STATE_MEDIA_TYPE, _codec_dumps, _decode_payload, _decode_record_document, _encode_payload, _encode_record_document, _encode_value, _ref_document, _validate_artifact_ref, _validate_record, _validate_tree
from demiflow.execution.exchange.dataflow_bridge import artifact_to_data_ref, record_to_envelope, to_channel_plan_v1
from demiflow.execution.exchange.dataflow_state import ChannelSetState, RoutingCounters

_ROUTINGS = frozenset({"forward", "hash", "rebalance", "broadcast", "target"})
_SEGMENT_SCHEMA = "demiflow.segment.v3"
_SEAL_SCHEMA = "demiflow.seal.v3"
_STATE_SCHEMA = "demiflow.channel_state.v3"


def _fail(code, message):
    raise DataflowError(code, message)


def _ident(value, label):
    try:
        return identifier(value)
    except Exception as exc:
        _fail("INVALID_CONTRACT", label + " is not a valid identifier")


def _nonneg_int(value, label):
    if type(value) is not int or value < 0:
        _fail("INVALID_CONTRACT", label + " must be a nonnegative int")
    return value


def _read_bounded(store, ref, *, max_bytes):
    """Accumulate store chunks under an explicit ceiling before joining."""
    chunks = []
    total = 0
    try:
        for chunk in store.read(ref, max_bytes=max_bytes):
            total += len(chunk)
            if total > max_bytes:
                _fail("BUDGET_EXCEEDED", "artifact exceeds the read budget")
            chunks.append(chunk)
    except FileNotFoundError as exc:
        raise DataflowError("CORRUPT_ARTIFACT", "referenced artifact is missing") from exc
    return b"".join(chunks)


def _put_verified(store, raw, *, media_type):
    """One atomic content-addressed write; the returned ref must match reality."""
    ref = store.put(iter((raw,)), max_bytes=len(raw), media_type=media_type)
    if (not isinstance(ref, ArtifactRef) or ref.size_bytes != len(raw)
            or ref.digest != hashlib.sha256(raw).hexdigest()):
        _fail("CORRUPT_ARTIFACT", "store returned a mismatched artifact ref")
    return ref


def _ref_from_doc(doc):
    if type(doc) is not dict or set(doc) != {"digest", "size_bytes", "store_id", "key", "media_type"}:
        _fail("CORRUPT_ARTIFACT", "saved artifact ref document is malformed")
    ref = ArtifactRef(digest=doc["digest"], size_bytes=doc["size_bytes"],
                      store_id=doc["store_id"], key=doc["key"], media_type=doc["media_type"])
    _validate_artifact_ref(ref, "saved artifact ref")
    return ref


def _require_state(channels):
    if not isinstance(channels, ChannelSetState):
        _fail("INVALID_CONTRACT", "channels must be a ChannelSetState ledger")
    return channels


# -- X001 ---------------------------------------------------------------------

def open_channels(specs, *, services):
    """Assemble the worker-local channel set from settled v3 specs.

    Each spec is validated through the lossless v1 projection (X012), the
    bounded ledger is built once (X016) and a fresh routing ledger bound to the
    common route epoch is attached (X018). Nothing is opened against a stale
    plan: the caller's services carry the fence and the stop check.
    """
    if not isinstance(specs, tuple):
        _fail("INVALID_CONTRACT", "specs must be a tuple of ExchangeSpec")
    services.check_stop()
    for spec in specs:
        to_channel_plan_v1(spec)
    state = ChannelSetState(specs, bounds=services.bounds)
    counters = RoutingCounters(values={}, route_epoch=state.route_epoch)
    state.bind_routing(counters)
    return state


# -- X002 ---------------------------------------------------------------------

def encode_record(record, *, bounds):
    """Encode one record into the versioned typed transfer document.

    The document is a self-contained provenance sidecar plus exactly one
    payload (inline tagged value or durable ref descriptor). Negative zero,
    tuple/list identity, bytes and numeric types survive the roundtrip; code
    objects are rejected. encoded_bytes is measured, never trusted.
    """
    if not isinstance(bounds, Bounds):
        _fail("INVALID_CONTRACT", "bounds must be a Bounds declaration")
    _validate_record(record)
    from demiflow.execution.contracts.record_codec import encode_document
    raw = encode_document(record, bounds)
    return EncodedRecord(record_id=record.record_id, encoding=RECORD_ENCODING, data=raw,
                         ref=None, encoded_bytes=len(raw),
                         decoded_upper_bytes=len(raw), sequence=record.sequence)


# -- X003 ---------------------------------------------------------------------

def resolve_record(encoded, *, store, bounds):
    """Resolve an encoded record back to the original Record, verifying content.

    The declared decode ceiling is checked before any allocation. Inline
    documents must re-encode canonically (tamper detection); ref payloads are
    read bounded from the same store, size- and digest-checked, and decoded
    under the row budget. Provenance (source_position) travels in the sidecar.
    """
    if not isinstance(encoded, EncodedRecord):
        _fail("INVALID_CONTRACT", "encoded must be an EncodedRecord")
    if not isinstance(bounds, Bounds):
        _fail("INVALID_CONTRACT", "bounds must be a Bounds declaration")
    if encoded.encoding != RECORD_ENCODING:
        _fail("INVALID_CONTRACT", "unsupported record encoding")
    from demiflow.execution.contracts.record_codec import document_limit, decode_document
    from demiflow.execution.storage.streams import read_document
    if encoded.decoded_upper_bytes > document_limit(bounds):
        _fail("BUDGET_EXCEEDED", "decoded record exceeds the row byte budget")
    # EncodedRecord owns exactly one *transfer document*: inline bytes or a
    # reference to those bytes. A business payload ref lives inside that
    # document together with provenance; it is not a second EncodedRecord body.
    raw = encoded.data if encoded.data is not None else read_document(encoded.ref, store, bounds)
    if len(raw) != encoded.encoded_bytes:
        _fail("CORRUPT_ARTIFACT", "encoded record size mismatch")
    skeleton = decode_document(raw, record_id=encoded.record_id,
                               sequence=encoded.sequence, bounds=bounds)
    ref = skeleton.payload_ref
    if ref is None:
        return skeleton
    _validate_artifact_ref(ref, "record payload ref")
    if ref.store_id != store.store_id:
        _fail("INVALID_CONTRACT", "record ref belongs to a different store domain")
    from demiflow.execution.contracts.record_codec import is_arrow_record
    from demiflow.execution.storage.arrow_payload import batch_cache
    if is_arrow_record(skeleton):
        batch_cache(store, bounds).locate(skeleton)
        return skeleton
    payload = _read_bounded(store, ref, max_bytes=bounds.max_row_bytes)
    if len(payload) != ref.size_bytes or hashlib.sha256(payload).hexdigest() != ref.digest:
        _fail("CORRUPT_ARTIFACT", "record payload size/digest mismatch")
    _decode_payload(payload, max_bytes=bounds.max_row_bytes)
    return skeleton


# -- X004 ---------------------------------------------------------------------

def route_record(record, *, spec, state):
    """Select reader(s) for one record with a recoverable routing ordinal.

    The ordinal is drawn once per call for every routing kind so a resumed run
    continues the same rebalance/target suffix. Hash keys use the versioned
    stable codec digest (never Python hash); broadcast delivers to every reader
    with a per-(record, edge, reader, ordinal) delivery identity. The record is gated
    through the shared codec (X002 encoding) against the channel byte window
    before routing. K4 control envelopes remain a separate explicit bridge;
    routing never embeds business bytes in a discarded control document.
    """
    ordinal, window = _route_context(record, spec=spec, state=state)
    if record.inline is not None:
        payload_raw = _encode_payload(record.inline, max_bytes=window)
        if len(payload_raw) != record.encoded_bytes:
            _fail("INVALID_CONTRACT", "record encoded_bytes misreports the inline payload size")
    elif record.encoded_bytes > window:
        _fail("BUDGET_EXCEEDED", "record exceeds the channel byte window")
    # Routing validates the actual typed data record above. It must not copy
    # business payload into a legacy control envelope just to discard it: v3
    # sends bounded data bytes and small authenticated control locators. The
    # explicit frozen K4 bridge still validates its own inline/ref callers.
    return _routed_records(record, spec=spec, ordinal=ordinal, window=window)


def _route_record_document(record, *, spec, state, bounds, resident_bytes=None, retained=0):
    """One authenticated payload encoding for routing and worker publication.

    The payload channel window and the full document row bound are distinct.
    Returned bytes belong only to this invocation and can be shared by its
    broadcast readers; there is no payload cache or alternate routing state.
    """
    if not isinstance(bounds, Bounds):
        _fail("INVALID_CONTRACT", "bounds must be a Bounds declaration")
    _, window = _route_context(record, spec=spec, state=state, advance=False, validate=False)
    if record.payload_ref is not None and record.encoded_bytes > window:
        _fail("BUDGET_EXCEEDED", "record exceeds the channel byte window")
    from demiflow.execution.contracts.record_codec import encode_document
    raw = encode_document(record, bounds, payload_limit=window, resident_bytes=resident_bytes, retained=retained)
    # Codec admission can wait for original peer ACKs. A refused/retried
    # workspace must not consume a routing ordinal or change delivery identity.
    ordinal = state.next_ordinal(spec.channel_id)
    return _routed_records(record, spec=spec, ordinal=ordinal, window=window), raw


def _route_context(record, *, spec, state, advance=True, validate=True):
    if validate:
        _validate_record(record)
    elif not isinstance(record, Record):
        _fail("INVALID_CONTRACT", "value must be a Record")
    if not isinstance(spec, ExchangeSpec):
        _fail("INVALID_CONTRACT", "spec must be an ExchangeSpec")
    if not callable(getattr(state, "next_ordinal", None)):
        _fail("INVALID_CONTRACT", "state must be a RoutingState ledger")
    if spec.routing not in _ROUTINGS:
        _fail("INVALID_CONTRACT", "unsupported routing kind")
    consumers = spec.consumer_partitions
    if not consumers:
        _fail("INVALID_CONTRACT", "channel has no reader")
    _ident(spec.channel_id, "channel_id")
    _nonneg_int(spec.route_epoch, "route_epoch")
    ordinal = state.next_ordinal(spec.channel_id) if advance else None
    window = spec.storage_bytes if spec.mode == "finite_sealed" else spec.credit_bytes
    return ordinal, window


def _routed_records(record, *, spec, ordinal, window):
    targets = _route_targets(record, spec=spec, ordinal=ordinal, window=window)
    return tuple(RoutedRecord(reader_id=target,
                              delivery_id=identity(["dataflow-delivery-v3", record.record_id,
                                                    spec.edge_id, target, ordinal]),
                              record=record)
                 for target in targets)


def _route_targets(record, *, spec, ordinal, window):
    """Select original reader IDs after the caller authenticates the record.

    Live routing owns validation, the ordinal and delivery identities above.
    Reading an immutable finite input already verifies the complete canonical
    record, so it can check this same rule without encoding the payload again
    or creating delivery identities that will never be sent.
    """
    consumers = spec.consumer_partitions
    if spec.routing == "forward":
        if len(consumers) != 1:
            _fail("INVALID_CONTRACT", "forward routing needs exactly one reader")
        targets = consumers
    elif spec.routing == "hash":
        if not spec.keys:
            _fail("INVALID_CONTRACT", "hash routing needs declared keys")
        if record.inline is None:
            _fail("INVALID_CONTRACT", "hash routing needs inline key values")
        # A missing declared business column is a KeyError, just as it is in
        # the join/reducer kernel; it is not a malformed channel contract.
        values = [record.inline[key] for key in spec.keys]
        _validate_tree(values, max_bytes=window)
        raw = _codec_dumps([_encode_value(value) for value in values])
        digest = hashlib.sha256(RECORD_ENCODING.encode("utf-8") + b"\x00" + raw).digest()
        targets = (consumers[int.from_bytes(digest[:8], "big") % len(consumers)],)
    elif spec.routing == "rebalance":
        targets = (consumers[ordinal % len(consumers)],)
    elif spec.routing == "target":
        if not spec.keys:
            _fail("INVALID_CONTRACT", "target routing needs a declared key")
        if record.inline is None:
            _fail("INVALID_CONTRACT", "target routing needs an inline target value")
        try:
            value = record.inline[spec.keys[0]]
        except KeyError as exc:
            raise DataflowError("INVALID_CONTRACT", "record is missing the target key") from exc
        if type(value) is not str or value not in consumers:
            _fail("INVALID_CONTRACT", "target value is not a reader of this channel")
        targets = (value,)
    else:
        targets = consumers
    return targets


# -- X005 ---------------------------------------------------------------------

def reserve_credit(channel_id, reader_id, *, rows, size_bytes, operation_id, state):
    """Reserve one connection's credit window atomically (X016 ledger).

    Same operation_id with identical content is idempotent; a conflicting reuse
    is INVALID_CONTRACT. A single record larger than the window can never be
    admitted (BUDGET_EXCEEDED, non-retryable); window exhaustion is the waitable
    CAPACITY_PENDING. Credit is returned only by release after confirmation.
    """
    state = _require_state(state)
    return state.reserve(channel_id, reader_id, rows=rows, size_bytes=size_bytes,
                         operation_id=operation_id)


# -- X006 ---------------------------------------------------------------------

def publish_segment(records, *, spec, store, operation_id):
    """Durable write -> verify -> publish one bounded segment (no tiny files).

    The segment manifest binds channel/route_epoch/producer membership and the
    sequence interval to a content digest; its logical id is the frozen identity
    formula over that binding. Publication is content-addressed: a retry after a
    lost response (same operation_id, same records) yields the same physical
    artifact and never double-charges. Before the atomic put completes nothing
    is visible. Non-checkpoint incremental publication is legal.
    """
    _ident(operation_id, "operation_id")
    if not isinstance(spec, ExchangeSpec):
        _fail("INVALID_CONTRACT", "spec must be an ExchangeSpec")
    if not isinstance(records, tuple) or not records:
        _fail("INVALID_CONTRACT", "records must be a nonempty tuple of EncodedRecord")
    entries = []
    previous = -1
    for encoded in records:
        if not isinstance(encoded, EncodedRecord):
            _fail("INVALID_CONTRACT", "segment records must be EncodedRecord")
        if encoded.encoding != RECORD_ENCODING or type(encoded.data) is not bytes:
            _fail("INVALID_CONTRACT", "segment record encoding is unsupported")
        if len(encoded.data) != encoded.encoded_bytes:
            _fail("CORRUPT_ARTIFACT", "segment record size mismatch")
        if encoded.sequence <= previous:
            _fail("INVALID_CONTRACT", "segment sequences must be strictly increasing")
        previous = encoded.sequence
        entry = {"record_id": encoded.record_id, "sequence": encoded.sequence,
                 "encoded_bytes": encoded.encoded_bytes,
                 "data_b64": base64.b64encode(encoded.data).decode("ascii"),
                 "ref": None, "data_ref": None}
        if encoded.ref is not None:
            _validate_artifact_ref(encoded.ref, "segment record ref")
            entry["ref"] = _ref_document(encoded.ref)
            entry["data_ref"] = artifact_to_data_ref(encoded.ref, reader_domain=spec.read_domain,
                                                     owner=spec.channel_id)
        entries.append(entry)
    content_digest = hashlib.sha256(document(entries)).hexdigest()
    sequence_start = records[0].sequence
    sequence_end = records[-1].sequence
    segment_id = identity(["dataflow-segment-v3", spec.channel_id, spec.route_epoch,
                           list(spec.producer_partitions), sequence_start, sequence_end,
                           content_digest])
    manifest = {"schema": _SEGMENT_SCHEMA, "channel_id": spec.channel_id, "edge_id": spec.edge_id,
                "route_epoch": spec.route_epoch,
                "producer_partitions": list(spec.producer_partitions),
                "sequence_start": sequence_start, "sequence_end": sequence_end,
                "count": len(records), "segment_id": segment_id,
                "content_digest": content_digest, "records": entries}
    raw = document(manifest)
    if spec.mode == "finite_sealed" and len(raw) > spec.storage_bytes:
        _fail("BUDGET_EXCEEDED", "segment exceeds the sealed storage reservation")
    return _put_verified(store, raw, media_type=SEGMENT_MEDIA_TYPE)


# -- X007 ---------------------------------------------------------------------

def seal_scope(scope_id, producers, *, store):
    """Seal a scope only when every member succeeded with a complete manifest.

    Empty success scopes seal (zero-row producers carry last_sequence=-1 and an
    empty manifest); cancelled/failed/unknown members never seal. Each manifest
    must exist in the store. The seal artifact is content-addressed and
    idempotent for the same membership.
    """
    _ident(scope_id, "scope_id")
    if not isinstance(producers, tuple) or not producers:
        _fail("INVALID_CONTRACT", "producers must be a nonempty tuple of ProducerCompletion")
    seen = set()
    generation = producers[0].generation
    members = []
    for completion in producers:
        if not isinstance(completion, ProducerCompletion):
            _fail("INVALID_CONTRACT", "producers must be ProducerCompletion values")
        _ident(completion.partition_id, "producer partition_id")
        if completion.partition_id in seen:
            _fail("INVALID_CONTRACT", "duplicate producer partition in scope")
        seen.add(completion.partition_id)
        if completion.status != "succeeded":
            _fail("INVALID_CONTRACT", "scope member has not succeeded: " + completion.status)
        if completion.generation != generation:
            _fail("INVALID_CONTRACT", "scope members disagree on generation")
        _nonneg_int(generation, "generation")
        if completion.last_sequence < -1:
            _fail("INVALID_CONTRACT", "last_sequence must be >= -1")
        _validate_artifact_ref(completion.manifest_ref, "producer manifest_ref")
        if not store.inspect(completion.manifest_ref):
            _fail("CORRUPT_ARTIFACT", "producer manifest is missing from the store")
        members.append({"partition_id": completion.partition_id, "generation": completion.generation,
                        "status": completion.status, "last_sequence": completion.last_sequence,
                        "manifest_ref": _ref_document(completion.manifest_ref)})
    members.sort(key=lambda member: member["partition_id"])
    seal = {"schema": _SEAL_SCHEMA, "scope_id": scope_id, "generation": generation,
            "membership": [member["partition_id"] for member in members],
            "producers": members, "status": "success"}
    raw = document(seal)
    return _put_verified(store, raw, media_type=SEAL_MEDIA_TYPE)


# -- X008 ---------------------------------------------------------------------

def snapshot_channels(channels, *, cut, store):
    """Seal every channel's cut state into one durable document per channel.

    Buffered records are published as a durable segment (even in memory mode:
    cut-referenced rows must survive recovery) while the live buffer stays
    intact for the running data plane. The state document carries the sequence
    frontier, routing ordinal, reader windows, outstanding leases and lifecycle
    facts as plain Json. A final cut over a fully succeeded finite_sealed scope
    also produces the scope seal. Nothing is released here - reclaim owns that.
    """
    state = _require_state(channels)
    if not isinstance(cut, CutRequest):
        _fail("INVALID_CONTRACT", "cut must be a CutRequest")
    _ident(cut.cut_id, "cut_id")
    if state.closed:
        _fail("FENCED", "channel set is closed")
    refs = {}
    for channel_id in state.channel_ids():
        spec = state.spec(channel_id)
        items = state.buffer_items(channel_id)
        segment_ref = None
        if items:
            segment_ref = publish_segment(tuple(encoded for _, encoded in items), spec=spec,
                                          store=store, operation_id=cut.cut_id)
            state.register_segment(channel_id, segment_ref, seq_end=items[-1][0])
        doc = state.snapshot_document(channel_id)
        doc["buffer_segment"] = _ref_document(segment_ref) if segment_ref is not None else None
        seal_ref = state.seal_ref(channel_id)
        doc["seal_ref"] = _ref_document(seal_ref) if seal_ref is not None else None
        doc["cut_id"] = cut.cut_id
        doc["plan_revision"] = cut.plan_revision
        if cut.final and spec.mode == "finite_sealed":
            completions = state.completions(channel_id)
            if (set(completions) == set(spec.producer_partitions)
                    and all(c.status == "succeeded" for c in completions.values())):
                seal_ref = seal_scope(spec.scope_id,
                                      tuple(completions[p] for p in sorted(completions)),
                                      store=store)
                state.set_seal(channel_id, seal_ref)
                doc["seal_ref"] = _ref_document(seal_ref)
        raw = document(doc)
        refs[channel_id] = _put_verified(store, raw, media_type=STATE_MEDIA_TYPE)
    return refs


# -- X009 ---------------------------------------------------------------------

def restore_channels(specs, refs, *, services):
    """Rebuild the channel set from saved cut documents of the same plan/epoch.

    Only the matching plan revision and route epoch are accepted (FENCED
    otherwise). The sequence frontier, routing ordinals, reader cursors and
    credit windows are restored as recorded - the suffix never routes from zero
    and credit is never re-granted. Buffered records are re-resolved through
    X003 (integrity verified) and re-enqueued, re-charging the resident window.
    """
    if not isinstance(specs, tuple):
        _fail("INVALID_CONTRACT", "specs must be a tuple of ExchangeSpec")
    services.check_stop()
    store = services.store
    bounds = services.bounds
    expected = {spec.channel_id for spec in specs}
    if set(refs) != expected:
        _fail("INVALID_CONTRACT", "saved refs must cover exactly the declared channels")
    docs = {}
    for spec in specs:
        raw = _read_bounded(store, refs[spec.channel_id], max_bytes=bounds.max_control_bytes)
        try:
            doc = json.loads(raw.decode("utf-8"))
        except (UnicodeDecodeError, ValueError) as exc:
            raise DataflowError("CORRUPT_ARTIFACT", "channel state document is not Json") from exc
        if document(doc) != raw:
            _fail("CORRUPT_ARTIFACT", "channel state document is not canonical")
        if doc.get("schema") != _STATE_SCHEMA:
            _fail("CORRUPT_ARTIFACT", "channel state document schema is unsupported")
        if (doc["channel_id"] != spec.channel_id or doc["edge_id"] != spec.edge_id
                or doc["mode"] != spec.mode or doc["routing"] != spec.routing):
            _fail("INVALID_CONTRACT", "saved channel state belongs to a different spec")
        if doc["route_epoch"] != spec.route_epoch:
            _fail("FENCED", "saved channel state belongs to another route_epoch")
        if doc["plan_revision"] != services.assignment.plan_revision:
            _fail("FENCED", "saved channel state belongs to another plan revision")
        docs[spec.channel_id] = doc
    state = ChannelSetState(specs, bounds=bounds)
    counters = RoutingCounters(values={ch: doc["routing_ordinal"] for ch, doc in docs.items()},
                               route_epoch=state.route_epoch)
    state.bind_routing(counters)
    for spec in specs:
        channel_id = spec.channel_id
        doc = docs[channel_id]
        state.restore_channel(channel_id, doc)
        if doc["seal_ref"] is not None:
            state.set_seal(channel_id, _ref_from_doc(doc["seal_ref"]))
        if doc["buffer_segment"] is not None:
            segment_ref = _ref_from_doc(doc["buffer_segment"])
            manifest_raw = _read_bounded(store, segment_ref, max_bytes=bounds.max_spill_bytes)
            manifest = json.loads(manifest_raw.decode("utf-8"))
            if (manifest.get("schema") != _SEGMENT_SCHEMA
                    or manifest["channel_id"] != channel_id
                    or manifest["route_epoch"] != spec.route_epoch):
                _fail("CORRUPT_ARTIFACT", "buffer segment does not belong to this channel")
            if hashlib.sha256(document(manifest["records"])).hexdigest() != manifest["content_digest"]:
                _fail("CORRUPT_ARTIFACT", "buffer segment content digest mismatch")
            for entry in manifest["records"]:
                data = base64.b64decode(entry["data_b64"], validate=True)
                ref = _ref_from_doc(entry["ref"]) if entry["ref"] is not None else None
                encoded = EncodedRecord(record_id=entry["record_id"], encoding=RECORD_ENCODING,
                                        data=data, ref=ref, encoded_bytes=entry["encoded_bytes"],
                                        decoded_upper_bytes=len(data), sequence=entry["sequence"])
                resolve_record(encoded, store=store, bounds=bounds)
                state.enqueue(channel_id, entry["sequence"], encoded)
            state.register_segment(channel_id, segment_ref, seq_end=manifest["sequence_end"])
    return state


# -- X010 ---------------------------------------------------------------------

def reclaim_segments(channels, *, committed, store):
    """Reclaim segments below the safe frontier, retaining every pinned ref.

    The frontier per channel is the minimum of the committed cut sequence and
    every reader's committed cursor offset (next-to-consume). A reader with no
    committed cursor - including a cancel that is not yet confirmed - holds the
    frontier at its last known cursor, so unconfirmed cancels never delete
    early. Prepared/unknown effect and sink refs pin their segments. Reclaim is
    idempotent: dropped entries leave the live index, so repeated crashes never
    accumulate orphans or exhaust the budget.
    """
    state = _require_state(channels)
    if not isinstance(committed, RecoveryImage):
        _fail("INVALID_CONTRACT", "committed must be a RecoveryImage")
    pins = {ref.digest for ref in committed.confirmed_sinks.values()}
    for contribution in committed.contributions:
        pins.update(ref.digest for ref in contribution.buffers)
        pins.update(ref.digest for ref in contribution.effects)
        pins.update(ref.digest for ref in contribution.sinks.values())
    released = []
    for channel_id in state.channel_ids():
        published = state.published_segments(channel_id)
        if not published:
            continue
        spec = state.spec(channel_id)
        committed_cursors = {}
        for contribution in committed.contributions:
            channel_doc = contribution.channels.get(channel_id)
            if channel_doc is None:
                continue
            for reader_id, value in channel_doc.items():
                if not isinstance(value, Mapping) or type(value.get("cursor")) is not int:
                    _fail("INVALID_CONTRACT", "committed channel cursor is malformed")
                committed_cursors[reader_id] = value["cursor"]
        frontier = committed.cut.sequence
        for reader_id in spec.consumer_partitions:
            if reader_id in committed_cursors:
                cursor = committed_cursors[reader_id]
            else:
                cursor = state.reader_cursor(channel_id, reader_id)
            frontier = min(frontier, cursor + 1)
        for digest, entry in published.items():
            if entry["pinned"] or digest in pins or entry["seq_end"] >= frontier:
                continue
            store.release(entry["ref"], owner=channel_id)
            state.drop_segment(channel_id, digest)
            released.append(entry["ref"])
    return tuple(released)


# -- X011 ---------------------------------------------------------------------

def close_channels(channels, *, reason, deadline_ms):
    """Terminate the channel set: fence late data, drain bounded, close carriers.

    Every channel and reader is tombstoned so late sends fail explicitly. The
    drain runs within the deadline; on timeout the remaining buffered data keeps
    its recorded (unknown) ownership instead of being fake-released. Carrier
    shutdown is idempotent and one failure never skips the others (X017).
    """
    state = _require_state(channels)
    if type(reason) is not str:
        _fail("INVALID_CONTRACT", "reason must be a string")
    if type(deadline_ms) is not int or deadline_ms <= 0:
        _fail("INVALID_CONTRACT", "deadline_ms must be a positive int")
    if state.closed:
        return
    deadline = time.monotonic() + deadline_ms / 1000.0
    for channel_id in state.channel_ids():
        state.cancel_channel(channel_id, reason)
    for channel_id in state.channel_ids():
        if time.monotonic() > deadline:
            break
        state.drain_buffer(channel_id)
    state.close()
