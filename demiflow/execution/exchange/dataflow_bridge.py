"""Explicit v3 -> frozen K4/K5 bridges and the versioned record codec (lane X).

Every projection here is lossless and validated against the frozen contracts in
demiflow.execution.contracts.exchange; no field is added to the v1 validators.
The private codec is the single stable typed encoding shared by encode_record,
resolve_record and record_to_envelope: it preserves negative zero, tuple/list
identity, bytes and numeric types, and it is data-only (never code pickle).
"""
import base64
import hashlib
import json
import re
from collections.abc import Mapping

from demiflow.execution.contracts.dataflow import (ArtifactRef, CutRequest, ExchangeSpec, Record,
                                                  _FrozenRowList, thaw_record_value)
from demiflow.execution.contracts.dataflow_errors import DataflowError
from demiflow.execution.contracts.exchange import (
    DATA_ENVELOPE_SCHEMA, build_data_ref, check_read_domain, validate_barrier,
    validate_data_envelope,
)
from demiflow.execution.recovery_contract import (
    MAX_DOCUMENT_BYTES, RecoveryContractError, document, identifier,
)

RECORD_ENCODING = "demiflow.record.v3"
RECORD_MEDIA_TYPE = "application/vnd.demiflow.record.v3"
SEGMENT_MEDIA_TYPE = "application/vnd.demiflow.segment.v3"
SEAL_MEDIA_TYPE = "application/vnd.demiflow.seal.v3"
STATE_MEDIA_TYPE = "application/vnd.demiflow.channel_state.v3"
PAYLOAD_SCHEMA = "demiflow.record_payload.v3"
_CODEC_VERSION = 3
_MAX_DEPTH = 32
_MAX_NODES = 65536
_SHA256 = re.compile(r"[0-9a-f]{64}\Z")
_MODES = frozenset({"memory_incremental", "durable_incremental", "finite_sealed"})
_ROUTINGS = frozenset({"forward", "hash", "rebalance", "broadcast", "target"})
_PLAN_KEYS = frozenset({
    "channel_id", "edge_id", "exchange_mode", "routing", "keys",
    "producer_partitions", "consumer_partitions", "route_epoch",
    "credit_budget", "storage_bytes", "read_domain", "scope_id", "reason",
})
_DATA_MEDIA_TYPES = frozenset({RECORD_MEDIA_TYPE, SEGMENT_MEDIA_TYPE, SEAL_MEDIA_TYPE,
                               STATE_MEDIA_TYPE, "application/octet-stream"})


def _fail(code, message):
    raise DataflowError(code, message)


def _ident(value, label):
    try:
        return identifier(value)
    except RecoveryContractError as exc:
        _fail("INVALID_CONTRACT", label + " is not a valid identifier")


def _digest(value, label):
    if type(value) is not str or _SHA256.fullmatch(value) is None:
        _fail("INVALID_CONTRACT", label + " must be a sha256 hex digest")
    return value


def _nonneg_int(value, label):
    if type(value) is not int or value < 0:
        _fail("INVALID_CONTRACT", label + " must be a nonnegative int")
    return value


def _document(value, label):
    try:
        return document(value)
    except RecoveryContractError as exc:
        _fail("INVALID_CONTRACT", label + " exceeds the Json document budget")


# -- versioned typed record codec (private infrastructure of X002/X003/X014) --

def _validate_tree(value, *, max_bytes):
    """Frontier walk: depth/node/leaf budgets are checked before encoding."""
    pending = [(value, 0)]
    nodes = 0
    while pending:
        item, depth = pending.pop()
        nodes += 1
        if nodes > _MAX_NODES or depth > _MAX_DEPTH:
            _fail("BUDGET_EXCEEDED", "record structure exceeds depth/node budget")
        kind = type(item)
        if isinstance(item, Mapping):
            if len(item) > _MAX_NODES:
                _fail("BUDGET_EXCEEDED", "record mapping exceeds budget")
            for key, child in item.items():
                if type(key) is not str:
                    _fail("INVALID_CONTRACT", "record mapping keys must be strings")
                if len(key.encode("utf-8")) > max_bytes:
                    _fail("BUDGET_EXCEEDED", "record key exceeds byte budget")
                pending.append((child, depth + 1))
        elif kind in (list, tuple, _FrozenRowList):
            if len(item) > _MAX_NODES:
                _fail("BUDGET_EXCEEDED", "record container exceeds budget")
            pending.extend((child, depth + 1) for child in item)
        elif kind is str:
            if len(item.encode("utf-8")) > max_bytes:
                _fail("BUDGET_EXCEEDED", "record string exceeds byte budget")
        elif kind is bytes:
            if len(item) > max_bytes:
                _fail("BUDGET_EXCEEDED", "record bytes exceed byte budget")
        elif kind is int:
            if item.bit_length() > 8 * max_bytes:
                _fail("BUDGET_EXCEEDED", "record integer exceeds byte budget")
        elif kind is float:
            continue
        elif item is None or kind is bool:
            continue
        else:
            from ..contracts.array_values import numpy_value, validate_array
            if numpy_value(item) is not None:
                try:
                    validate_array(item)
                except ValueError as exc:
                    raise DataflowError('INVALID_CONTRACT', str(exc)) from exc
                if item.nbytes > max_bytes:
                    _fail('BUDGET_EXCEEDED', 'NumPy payload exceeds row byte budget')
                continue
            _fail("INVALID_CONTRACT",
                  "record payload carries a non-data object: " + kind.__name__)


def _encode_value(value):
    kind = type(value)
    if value is None:
        return {"t": "null"}
    if kind is bool:
        return {"t": "bool", "v": value}
    if kind is int:
        return {"t": "int", "v": value}
    if kind is float:
        return {"t": "float", "v": value}
    if kind is str:
        return {"t": "str", "v": value}
    if kind is bytes:
        return {"t": "bytes", "v": base64.b64encode(value).decode("ascii")}
    if kind in (list, _FrozenRowList):
        return {"t": "list", "v": [_encode_value(child) for child in value]}
    if kind is tuple:
        return {"t": "tuple", "v": [_encode_value(child) for child in value]}
    if isinstance(value, Mapping):
        return {"t": "map", "v": [[key, _encode_value(value[key])] for key in sorted(value)]}
    from ..contracts.array_values import numpy_value, validate_array
    if numpy_value(value) is not None:
        import numpy as np
        try:
            validate_array(value)
        except ValueError as exc:
            raise DataflowError('INVALID_CONTRACT', str(exc)) from exc
        return {'t': 'numpy_scalar' if isinstance(value, np.generic) else 'numpy_array',
                'dtype': value.dtype.str, 'shape': list(value.shape),
                'v': base64.b64encode(value.tobytes(order='C')).decode('ascii')}
    _fail("INVALID_CONTRACT", "record payload carries a non-data object: " + kind.__name__)


def _decode_value(node):
    if type(node) is not dict or "t" not in node:
        _fail("CORRUPT_ARTIFACT", "record codec node is malformed")
    tag = node["t"]
    if tag == "null":
        return None
    if tag in ("bool", "int", "float", "str"):
        value = node.get("v")
        expected = {"bool": bool, "int": int, "float": float, "str": str}[tag]
        if type(value) is not expected:
            _fail("CORRUPT_ARTIFACT", "record codec leaf type mismatch")
        return value
    if tag == "bytes":
        value = node.get("v")
        if type(value) is not str:
            _fail("CORRUPT_ARTIFACT", "record codec bytes leaf malformed")
        try:
            return base64.b64decode(value, validate=True)
        except ValueError as exc:
            raise DataflowError("CORRUPT_ARTIFACT", "record codec bytes are not base64") from exc
    if tag in ('numpy_scalar', 'numpy_array'):
        import numpy as np
        import math
        try:
            if set(node) != {'t', 'dtype', 'shape', 'v'}:
                raise ValueError('Malformed NumPy node')
            dtype, shape = np.dtype(node['dtype']), node['shape']
            if (dtype.hasobject or dtype.fields is not None or dtype.kind not in 'biufcSUVmM'
                    or dtype.itemsize <= 0 or type(shape) is not list or len(shape) > 32
                    or any(type(n) is not int or n < 0 for n in shape)
                    or tag == 'numpy_scalar' and shape):
                raise ValueError('Unsupported NumPy dtype/shape')
            raw = base64.b64decode(node['v'], validate=True)
            if math.prod(shape) * dtype.itemsize != len(raw):
                raise ValueError('NumPy shape differs from payload bytes')
            array = np.frombuffer(raw, dtype=dtype).reshape(shape)
            return array[()] if tag == 'numpy_scalar' else array
        except (ValueError, TypeError, OverflowError) as exc:
            raise DataflowError('CORRUPT_ARTIFACT', 'Invalid NumPy data payload') from exc
    if tag in ("list", "tuple", "map"):
        value = node.get("v")
        if type(value) is not list:
            _fail("CORRUPT_ARTIFACT", "record codec container malformed")
        if tag == "map":
            result = {}
            for pair in value:
                if type(pair) is not list or len(pair) != 2 or type(pair[0]) is not str:
                    _fail("CORRUPT_ARTIFACT", "record codec map entry malformed")
                if pair[0] in result:
                    _fail("CORRUPT_ARTIFACT", "record codec map has duplicate keys")
                result[pair[0]] = _decode_value(pair[1])
            return result
        children = [_decode_value(child) for child in value]
        return children if tag == "list" else tuple(children)
    _fail("CORRUPT_ARTIFACT", "record codec tag is unsupported")


def _codec_dumps(value):
    return json.dumps(thaw_record_value(value), sort_keys=True, ensure_ascii=False,
                      separators=(",", ":"), allow_nan=True).encode("utf-8")


def _codec_loads(raw):
    try:
        return json.loads(raw.decode("utf-8"))
    except (UnicodeDecodeError, ValueError) as exc:
        raise DataflowError("CORRUPT_ARTIFACT", "record codec bytes are not valid Json") from exc


def _encode_payload(row, *, max_bytes):
    _validate_tree(row, max_bytes=max_bytes)
    raw = _codec_dumps(_encode_value(row))
    if len(raw) > max_bytes:
        _fail("BUDGET_EXCEEDED", "encoded record exceeds the row byte budget")
    return raw


def _decode_payload(raw, *, max_bytes):
    if len(raw) > max_bytes:
        _fail("BUDGET_EXCEEDED", "encoded record exceeds the row byte budget")
    node = _codec_loads(raw)
    value = _decode_value(node)
    if type(value) is not dict:
        _fail("CORRUPT_ARTIFACT", "record payload must decode to a row mapping")
    if _codec_dumps(_encode_value(value)) != raw:
        _fail("CORRUPT_ARTIFACT", "record codec bytes are not canonical")
    return value


def _ref_document(ref):
    return {"digest": ref.digest, "size_bytes": ref.size_bytes, "store_id": ref.store_id,
            "key": ref.key, "media_type": ref.media_type}


def _validate_artifact_ref(ref, label):
    if not isinstance(ref, ArtifactRef):
        _fail("INVALID_CONTRACT", label + " must be an ArtifactRef")
    _digest(ref.digest, label + " digest")
    _nonneg_int(ref.size_bytes, label + " size_bytes")
    _ident(ref.store_id, label + " store_id")
    if type(ref.key) is not str or not ref.key:
        _fail("INVALID_CONTRACT", label + " key must be a nonempty relative store key")
    if (ref.key.startswith("/") or ".." in ref.key.split("/") or "?" in ref.key
            or "://" in ref.key or "@" in ref.key):
        _fail("INVALID_CONTRACT", label + " key is not a clean relative store key")
    if type(ref.media_type) is not str or not ref.media_type:
        _fail("INVALID_CONTRACT", label + " media_type is required")
    return ref


def _validate_record(record):
    if not isinstance(record, Record):
        _fail("INVALID_CONTRACT", "value must be a Record")
    _digest(record.record_id, "record_id")
    _nonneg_int(record.sequence, "record sequence")
    _nonneg_int(record.encoded_bytes, "record encoded_bytes")
    if (record.inline is None) == (record.payload_ref is None):
        _fail("INVALID_CONTRACT", "record must carry exactly one of inline/payload_ref")
    if not isinstance(record.source_position, Mapping):
        _fail("INVALID_CONTRACT", "source_position must be a Json mapping")
    _document(thaw_record_value(record.source_position), "source_position")
    if record.schema_ref is not None:
        from ..record_schema import SCHEMA_MEDIA_TYPE
        _validate_artifact_ref(record.schema_ref, 'schema_ref')
        if record.schema_ref.media_type != SCHEMA_MEDIA_TYPE:
            _fail('INVALID_CONTRACT', 'schema_ref must use the Arrow schema encoding')
    if record.payload_ref is not None:
        _validate_artifact_ref(record.payload_ref, "payload_ref")
        if record.payload_ref.media_type not in _DATA_MEDIA_TYPES:
            _fail("INVALID_CONTRACT", "payload_ref media_type is not a data encoding")
        if record.encoded_bytes != record.payload_ref.size_bytes:
            _fail("INVALID_CONTRACT", "record encoded_bytes misreports the ref payload size")
    return record


def _encode_record_document(record, *, max_bytes):
    """Self-contained transfer document: provenance sidecar + exactly one payload."""
    if record.inline is not None:
        payload_raw = _encode_payload(record.inline, max_bytes=max_bytes)
        if record.encoded_bytes != len(payload_raw):
            _fail("INVALID_CONTRACT", "record encoded_bytes misreports the inline payload size")
        doc = {"v": _CODEC_VERSION, "sp": record.source_position, "eb": record.encoded_bytes,
               "p": _codec_loads(payload_raw), "r": None}
    else:
        doc = {"v": _CODEC_VERSION, "sp": record.source_position, "eb": record.encoded_bytes,
               "p": None, "r": _ref_document(record.payload_ref)}
    if record.schema_ref is not None:
        doc["s"] = _ref_document(record.schema_ref)
    raw = _codec_dumps(doc)
    if len(raw) > max_bytes:
        _fail("BUDGET_EXCEEDED", "encoded record document exceeds the row byte budget")
    return raw


def _decode_record_document(raw, *, record_id, sequence, max_bytes):
    if len(raw) > max_bytes:
        _fail("BUDGET_EXCEEDED", "encoded record document exceeds the row byte budget")
    doc = _codec_loads(raw)
    if type(doc) is not dict or doc.get("v") != _CODEC_VERSION:
        _fail("CORRUPT_ARTIFACT", "record document version is unsupported")
    if set(doc) not in ({"v", "sp", "eb", "p", "r"}, {"v", "sp", "eb", "p", "r", "s"}):
        _fail("CORRUPT_ARTIFACT", "record document key set is unsupported")
    schema_ref = None
    if "s" in doc:
        try:
            schema_ref = ArtifactRef(**doc["s"])
        except (TypeError, ValueError) as exc:
            raise DataflowError("CORRUPT_ARTIFACT", "record schema ref is malformed") from exc
        from ..record_schema import SCHEMA_MEDIA_TYPE
        if schema_ref.media_type != SCHEMA_MEDIA_TYPE:
            _fail("CORRUPT_ARTIFACT", "record schema ref has the wrong encoding")
    if (doc["p"] is None) == (doc["r"] is None):
        _fail("CORRUPT_ARTIFACT", "record document must carry exactly one payload")
    if type(doc["sp"]) is not dict or type(doc["eb"]) is not int or doc["eb"] < 0:
        _fail("CORRUPT_ARTIFACT", "record document provenance is malformed")
    if _codec_dumps(doc) != raw:
        _fail("CORRUPT_ARTIFACT", "record document bytes are not canonical")
    if doc["p"] is not None:
        row = _decode_value(doc["p"])
        if type(row) is not dict:
            _fail("CORRUPT_ARTIFACT", "record document payload must be a row mapping")
        if len(_encode_payload(row, max_bytes=max_bytes)) != doc["eb"]:
            _fail("CORRUPT_ARTIFACT", "record document payload size mismatch")
        return Record(record_id=record_id, source_position=doc["sp"], payload_ref=None,
                      inline=row, encoded_bytes=doc["eb"], sequence=sequence, schema_ref=schema_ref)
    ref_doc = doc["r"]
    if type(ref_doc) is not dict or set(ref_doc) != {"digest", "size_bytes", "store_id", "key", "media_type"}:
        _fail("CORRUPT_ARTIFACT", "record document ref is malformed")
    ref = ArtifactRef(digest=ref_doc["digest"], size_bytes=ref_doc["size_bytes"],
                      store_id=ref_doc["store_id"], key=ref_doc["key"], media_type=ref_doc["media_type"])
    _validate_artifact_ref(ref, "record document ref")
    if ref.media_type not in _DATA_MEDIA_TYPES:
        _fail("INVALID_CONTRACT", "record document ref media_type is not a data encoding")
    if doc["eb"] != ref.size_bytes:
        _fail("CORRUPT_ARTIFACT", "record document ref size mismatch")
    return Record(record_id=record_id, source_position=doc["sp"], payload_ref=ref,
                  inline=None, encoded_bytes=doc["eb"], sequence=sequence, schema_ref=schema_ref)


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
