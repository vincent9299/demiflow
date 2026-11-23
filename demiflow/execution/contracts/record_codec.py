"""Record codec implementation."""
from __future__ import annotations

from dataclasses import asdict

import base64
import hashlib
import json
import math
import sys
import re
from collections.abc import Mapping
from demiflow.execution.contracts.dataflow import ArtifactRef, BlockPartition, Record, _FrozenRowList, _FrozenRowSet, thaw_record_value
from demiflow.execution.contracts.dataflow_errors import DataflowError
from demiflow.execution.recovery_contract import MAX_DOCUMENT_BYTES, RecoveryContractError, document, identifier


ARROW_BATCH_MEDIA = 'application/vnd.demiflow.arrow-batch.v1'

MAPPED_ARROW_MEDIA = 'application/vnd.demiflow.mapped-arrow-batch.v1'

ARROW_MEDIA_TYPES = frozenset({ARROW_BATCH_MEDIA, MAPPED_ARROW_MEDIA})


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

_DATA_MEDIA_TYPES = frozenset({RECORD_MEDIA_TYPE, SEGMENT_MEDIA_TYPE, SEAL_MEDIA_TYPE,
                               STATE_MEDIA_TYPE, "application/octet-stream", *ARROW_MEDIA_TYPES})

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

def _validate_tree(value, *, max_bytes):
    """Frontier walk: depth/node/leaf budgets are checked before encoding."""
    pending = [(value, 0)]
    nodes = 0
    minimum_bytes = 0
    while pending:
        item, depth = pending.pop()
        nodes += 1
        if nodes > _MAX_NODES or depth > _MAX_DEPTH:
            _fail("BUDGET_EXCEEDED", "record structure exceeds depth/node budget")
        kind = type(item)
        minimum_bytes += 1
        if isinstance(item, Mapping):
            if len(item) > _MAX_NODES:
                _fail("BUDGET_EXCEEDED", "record mapping exceeds budget")
            for key, child in item.items():
                if type(key) is not str:
                    _fail("INVALID_CONTRACT", "record mapping keys must be strings")
                minimum_bytes += len(key.encode("utf-8"))
                if len(key.encode("utf-8")) > max_bytes:
                    _fail("BUDGET_EXCEEDED", "record key exceeds byte budget")
                pending.append((child, depth + 1))
        elif kind in (list, tuple, _FrozenRowList, set, frozenset, _FrozenRowSet):
            if len(item) > _MAX_NODES:
                _fail("BUDGET_EXCEEDED", "record container exceeds budget")
            pending.extend((child, depth + 1) for child in item)
        elif kind is str:
            minimum_bytes += len(item.encode("utf-8"))
            if len(item.encode("utf-8")) > max_bytes:
                _fail("BUDGET_EXCEEDED", "record string exceeds byte budget")
        elif kind is bytes:
            minimum_bytes += len(item)
            if len(item) > max_bytes:
                _fail("BUDGET_EXCEEDED", "record bytes exceed byte budget")
        elif kind is int:
            minimum_bytes += item.bit_length() // 8
            if item.bit_length() > 8 * max_bytes:
                _fail("BUDGET_EXCEEDED", "record integer exceeds byte budget")
        elif kind is float:
            pass
        elif item is None or kind is bool:
            pass
        else:
            from demiflow.execution.contracts.scalar_values import scalar_node
            try:
                scalar = scalar_node(item, max_bytes=max_bytes)
            except OverflowError as exc:
                raise DataflowError('BUDGET_EXCEEDED', str(exc)) from exc
            except ValueError as exc:
                raise DataflowError('INVALID_CONTRACT', str(exc)) from exc
            if scalar is not None:
                minimum_bytes += len(_codec_dumps(scalar))
                if minimum_bytes > max_bytes:
                    _fail("BUDGET_EXCEEDED", "record values exceed aggregate byte budget")
                continue
            from demiflow.execution.contracts.array_values import numpy_value, validate_array
            if numpy_value(item) is not None:
                try:
                    validate_array(item)
                except ValueError as exc:
                    raise DataflowError('INVALID_CONTRACT', str(exc)) from exc
                minimum_bytes += int(item.nbytes)
                if minimum_bytes > max_bytes:
                    _fail('BUDGET_EXCEEDED', 'NumPy payload exceeds row byte budget')
                continue
            _fail("INVALID_CONTRACT",
                  "record payload carries a non-data object: " + kind.__name__)
        if minimum_bytes > max_bytes:
            _fail("BUDGET_EXCEEDED", "record values exceed aggregate byte budget")

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
    if kind in (set, frozenset, _FrozenRowSet):
        children = [_encode_value(child) for child in value]
        children.sort(key=_codec_dumps)
        return {'t': 'frozenset' if kind is frozenset else 'set', 'v': children}
    from demiflow.execution.contracts.scalar_values import scalar_node
    scalar = scalar_node(value, max_bytes=sys.maxsize)
    if scalar is not None:
        return scalar
    if isinstance(value, Mapping):
        # The typed map is an ordered entry array. Preserve Python field order
        # so an untyped row can later infer its original Arrow column order.
        # Historical sorted entry arrays still decode/re-encode byte-for-byte;
        # control JSON and semantic identity keep their separate canonical sort.
        return {"t": "map", "v": [[key, _encode_value(child)] for key, child in value.items()]}
    from demiflow.execution.contracts.array_values import numpy_value, validate_array
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

def _decode_value(node, _depth=0, _count=None):
    if _count is None:
        _count = [0]
    _count[0] += 1
    if _depth > _MAX_DEPTH or _count[0] > _MAX_NODES:
        _fail('CORRUPT_ARTIFACT', 'record codec exceeds depth/node budget')
    if type(node) is not dict or "t" not in node:
        _fail("CORRUPT_ARTIFACT", "record codec node is malformed")
    tag = node["t"]
    if type(tag) is not str:
        _fail("CORRUPT_ARTIFACT", "record codec tag must be a string")
    if tag == "null":
        return None
    if tag in ("bool", "int", "float", "str"):
        value = node.get("v")
        expected = {"bool": bool, "int": int, "float": float, "str": str}[tag]
        if type(value) is not expected:
            _fail("CORRUPT_ARTIFACT", "record codec leaf type mismatch")
        return float("nan") if expected is float and math.isnan(value) else value
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
    from demiflow.execution.contracts.scalar_values import SCALAR_TAGS, decode_scalar
    if tag in SCALAR_TAGS:
        try:
            return decode_scalar(node)
        except (ValueError, TypeError, OverflowError) as exc:
            raise DataflowError('CORRUPT_ARTIFACT', 'Invalid scalar data payload') from exc
    if tag in ("list", "tuple", "map", "set", "frozenset"):
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
                result[pair[0]] = _decode_value(pair[1], _depth + 1, _count)
            return result
        children = [_decode_value(child, _depth + 1, _count) for child in value]
        if tag in ('set', 'frozenset'):
            try:
                result = set(children) if tag == 'set' else frozenset(children)
            except TypeError as exc:
                raise DataflowError('CORRUPT_ARTIFACT', 'Unhashable set member') from exc
            if len(result) != len(children):
                _fail('CORRUPT_ARTIFACT', 'Set payload has duplicate members')
            return result
        return children if tag == "list" else tuple(children)
    _fail("CORRUPT_ARTIFACT", "record codec tag is unsupported")

def _codec_json_default(value):
    # Tagged payloads already contain ordinary JSON leaves. Only immutable
    # control mappings need conversion; traversing/copying every scalar again
    # multiplies encoding cost at every transport and spill boundary. JSON's
    # tuple handling preserves the same historical array bytes.
    if isinstance(value, Mapping):
        return dict(value)
    raise TypeError('Object of type ' + type(value).__name__ + ' is not JSON serializable')

_CODEC_ENCODER = json.JSONEncoder(sort_keys=True, ensure_ascii=False,
    separators=(",", ":"), allow_nan=True, default=_codec_json_default)

_RECORD_PREFIXES = {key: ('"' + key + '":').encode('ascii')
                    for key in ('bp', 'eb', 'p', 'r', 's', 'sp', 'v')}

def _codec_dumps(value):
    if value is None:
        return b'null'
    if type(value) is bool:
        return b'true' if value else b'false'
    if type(value) is int:
        return str(value).encode('ascii')
    return _CODEC_ENCODER.encode(value).encode("utf-8")

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
    if value is None:
        _fail("CORRUPT_ARTIFACT", "record payload must decode to a non-null data value")
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
    parents = record.source_position.get('parent_record_ids', ())
    if type(parents) not in (list, tuple):
        _fail('INVALID_CONTRACT', 'parent_record_ids must be a sequence')
    for parent in parents:
        _digest(parent, 'parent_record_id')
    if record.schema_ref is not None:
        from demiflow.execution.record_schema import SCHEMA_MEDIA_TYPE
        _validate_artifact_ref(record.schema_ref, 'schema_ref')
        if record.schema_ref.media_type != SCHEMA_MEDIA_TYPE:
            _fail('INVALID_CONTRACT', 'schema_ref must use the Arrow schema encoding')
    if record.payload_ref is not None:
        _validate_artifact_ref(record.payload_ref, "payload_ref")
        if record.payload_ref.media_type not in _DATA_MEDIA_TYPES:
            _fail("INVALID_CONTRACT", "payload_ref media_type is not a data encoding")
        if record.payload_ref.media_type not in ARROW_MEDIA_TYPES and record.encoded_bytes != record.payload_ref.size_bytes:
            _fail("INVALID_CONTRACT", "record encoded_bytes misreports the ref payload size")
    return record

def _encode_record_document(record, *, max_bytes, payload_max_bytes=None):
    """Self-contained transfer document: provenance sidecar + exactly one payload."""
    if record.inline is not None:
        payload_limit = max_bytes if payload_max_bytes is None else min(max_bytes, payload_max_bytes)
        _validate_tree(record.inline, max_bytes=payload_limit)
        payload = _encode_value(record.inline)
        payload_raw = _codec_dumps(payload)
        if len(payload_raw) > payload_limit:
            _fail("BUDGET_EXCEEDED", "encoded record exceeds the row byte budget")
        if record.encoded_bytes != len(payload_raw):
            _fail("INVALID_CONTRACT", "record encoded_bytes misreports the inline payload size")
        doc = {"v": _CODEC_VERSION, "sp": record.source_position, "eb": record.encoded_bytes,
               "p": payload, "r": None}
    else:
        payload_raw = None
        doc = {"v": _CODEC_VERSION, "sp": record.source_position, "eb": record.encoded_bytes,
               "p": None, "r": _ref_document(record.payload_ref)}
    if record.schema_ref is not None:
        doc["s"] = _ref_document(record.schema_ref)
    if record.block_partition is not None:
        from dataclasses import asdict
        doc["bp"] = asdict(record.block_partition)
    return _record_document_bytes(doc, payload_raw=payload_raw, max_bytes=max_bytes)

def _record_document_bytes(doc, *, payload_raw, max_bytes):
    # Reuse the already sized canonical payload. Serializing the whole document
    # again duplicates the widest field's JSON encoding at every spool/channel.
    # The sorted keys and separators are exactly the original wire format.
    parts, size = [b'{'], 2
    for index, key in enumerate(sorted(doc)):
        value = payload_raw if key == 'p' and payload_raw is not None else _codec_dumps(doc[key])
        prefix = _RECORD_PREFIXES.get(key)
        if prefix is None:
            prefix = _codec_dumps(key) + b':'
        size += len(prefix) + len(value) + bool(index)
        if size > max_bytes:
            _fail('BUDGET_EXCEEDED', 'encoded record document exceeds the row byte budget')
        if index:
            parts.append(b',')
        parts.extend((prefix, value))
    parts.append(b'}')
    return b''.join(parts)

def _same_codec_tree(left, right):
    """Data-only equality used before canonical encoding; NaN stays supported."""
    if type(left) is not type(right):
        return False
    if type(left) is dict:
        return left.keys() == right.keys() and all(_same_codec_tree(left[k], right[k]) for k in left)
    if type(left) is list:
        return len(left) == len(right) and all(_same_codec_tree(a, b) for a, b in zip(left, right))
    if type(left) is float and math.isnan(left):
        return math.isnan(right)
    return left == right

def _decode_record_document(raw, *, record_id, sequence, max_bytes,
                            payload_max_bytes=None, metadata_max_bytes=None):
    if len(raw) > max_bytes:
        _fail("BUDGET_EXCEEDED", "encoded record document exceeds the row byte budget")
    doc = _codec_loads(raw)
    if type(doc) is not dict or doc.get("v") != _CODEC_VERSION:
        _fail("CORRUPT_ARTIFACT", "record document version is unsupported")
    if set(doc) - {"s", "bp"} != {"v", "sp", "eb", "p", "r"}:
        _fail("CORRUPT_ARTIFACT", "record document key set is unsupported")
    block_partition = None
    if "bp" in doc:
        try:
            block_partition = BlockPartition(**doc["bp"])
        except (TypeError, ValueError) as exc:
            raise DataflowError("CORRUPT_ARTIFACT", "record block partition is malformed") from exc
    schema_ref = None
    if "s" in doc:
        try:
            schema_ref = ArtifactRef(**doc["s"])
        except (TypeError, ValueError) as exc:
            raise DataflowError("CORRUPT_ARTIFACT", "record schema ref is malformed") from exc
        from demiflow.execution.record_schema import SCHEMA_MEDIA_TYPE
        if schema_ref.media_type != SCHEMA_MEDIA_TYPE:
            _fail("CORRUPT_ARTIFACT", "record schema ref has the wrong encoding")
    if (doc["p"] is None) == (doc["r"] is None):
        _fail("CORRUPT_ARTIFACT", "record document must carry exactly one payload")
    if type(doc["sp"]) is not dict or type(doc["eb"]) is not int or doc["eb"] < 0:
        _fail("CORRUPT_ARTIFACT", "record document provenance is malformed")
    payload_limit = max_bytes if payload_max_bytes is None else min(max_bytes, payload_max_bytes)
    if payload_max_bytes is not None and doc['eb'] > payload_limit:
        _fail('BUDGET_EXCEEDED', 'encoded payload exceeds max_row_bytes bound')
    if metadata_max_bytes is not None:
        
        _json_shape({**doc, 'p': None}, metadata_max_bytes)
    if doc["p"] is not None:
        row = _decode_value(doc["p"])
        if row is None:
            _fail("CORRUPT_ARTIFACT", "record document payload must be a non-null data value")
        if payload_max_bytes is None:
            payload_raw = _encode_payload(row, max_bytes=payload_limit)
        else:
            # Validate normalization without simultaneously retaining a second
            # complete tagged tree AND both its JSON text/byte copies. Those
            # are separate peaks inside the caller's original codec grant.
            
            _typed_shape(row, payload_limit)
            expected = _encode_value(row)
            if not _same_codec_tree(expected, doc['p']):
                _fail('CORRUPT_ARTIFACT', 'record document bytes are not canonical')
            del expected
            payload_raw = _codec_dumps(doc['p'])
            if len(payload_raw) > payload_limit:
                _fail('BUDGET_EXCEEDED', 'encoded payload exceeds max_row_bytes bound')
        if len(payload_raw) != doc["eb"]:
            _fail("CORRUPT_ARTIFACT", "record document payload size mismatch")
        if _record_document_bytes(doc, payload_raw=payload_raw, max_bytes=max_bytes) != raw:
            _fail("CORRUPT_ARTIFACT", "record document bytes are not canonical")
        return Record(record_id=record_id, source_position=doc["sp"], payload_ref=None,
                      inline=row, encoded_bytes=doc["eb"], sequence=sequence, schema_ref=schema_ref, block_partition=block_partition)
    ref_doc = doc["r"]
    if _record_document_bytes(doc, payload_raw=None, max_bytes=max_bytes) != raw:
        _fail("CORRUPT_ARTIFACT", "record document bytes are not canonical")
    if type(ref_doc) is not dict or set(ref_doc) != {"digest", "size_bytes", "store_id", "key", "media_type"}:
        _fail("CORRUPT_ARTIFACT", "record document ref is malformed")
    ref = ArtifactRef(digest=ref_doc["digest"], size_bytes=ref_doc["size_bytes"],
                      store_id=ref_doc["store_id"], key=ref_doc["key"], media_type=ref_doc["media_type"])
    _validate_artifact_ref(ref, "record document ref")
    if ref.media_type not in _DATA_MEDIA_TYPES:
        _fail("INVALID_CONTRACT", "record document ref media_type is not a data encoding")
    if ref.media_type not in ARROW_MEDIA_TYPES and doc["eb"] != ref.size_bytes:
        _fail("CORRUPT_ARTIFACT", "record document ref size mismatch")
    return Record(record_id=record_id, source_position=doc["sp"], payload_ref=ref,
                  inline=None, encoded_bytes=doc["eb"], sequence=sequence, schema_ref=schema_ref, block_partition=block_partition)


def record_encoded_bytes(raw, *, maximum, frame_maximum=None):
    """Read the canonical control prefix without decoding the business payload.

    Record documents sort keys: optional ``bp`` precedes ``eb``; the potentially
    large typed payload ``p`` follows it. Full decoding still verifies canonical
    bytes, IDs, provenance, schema and the declared payload size after admission.
    """
    if type(raw) is not bytes or len(raw) > (maximum if frame_maximum is None else frame_maximum):
        raise DataflowError('BUDGET_EXCEEDED', 'terminal record frame exceeds its row bound')
    try:
        cursor = len(b'{"eb":')
        if raw.startswith(b'{"bp":'):
            end = raw.find(b'},"eb":', 6, 1024)
            if end < 0:
                raise ValueError('block header exceeds its bounded control prefix')
            BlockPartition(**json.loads(raw[6:end + 1]))
            cursor = end + len(b'},"eb":')
        elif not raw.startswith(b'{"eb":'):
            raise ValueError('record size is not in the canonical header')
        end = raw.find(b',', cursor, cursor + 32)
        digits = raw[cursor:end]
        if end < 0 or not digits or not digits.isdigit():
            raise ValueError('invalid record size')
        size = int(digits)
        if size > maximum:
            raise DataflowError('BUDGET_EXCEEDED', 'terminal payload exceeds its row bound')
        return size
    except (ValueError, TypeError, UnicodeError, IndexError) as exc:
        raise DataflowError('CORRUPT_ARTIFACT', 'invalid terminal record header') from exc


def _frame_budget_error(message):
    raise DataflowError('BUDGET_EXCEEDED', message)

def resident_limit(bounds, resident_bytes=None):
    return min(bounds.max_resident_bytes,
               bounds.max_resident_bytes if resident_bytes is None else resident_bytes)

def document_limit(bounds):
    return min(bounds.max_row_bytes + bounds.max_control_bytes, bounds.max_resident_bytes)

def line_limit(bounds):
    # A valid document contains no literal control characters: only quotes and
    # backslashes grow when it is enclosed in the existing outer JSON string.
    return min(2 * document_limit(bounds) + bounds.max_control_bytes + 32,
               bounds.max_resident_bytes)

_JSON_STRING_ESCAPE = re.compile(r'["\\\x00-\x1f]')

def _string_size(value):
    if value.isascii():
        # Ordinary keys, ids and strings need no escaping. One native scan
        # avoids 34 full scans and both generators without encoding a copy.
        if _JSON_STRING_ESCAPE.search(value) is None:
            return len(value) + 2
        # Keep native count loops for escape-heavy strings: a Python loop over
        # every matching character is slower and needs one match per escape.
        escaped = sum(value.count(char) for char in '"\\\b\f\n\r\t')
        controls = sum(value.count(chr(n)) for n in range(32) if n not in (8, 9, 10, 12, 13))
        return len(value) + 2 + escaped + 5 * controls
    size = 2
    for char in value:
        n = ord(char)
        size += (2 if char in '"\\\b\f\n\r\t' else 6 if n < 32
                 else 1 if n < 128 else 2 if n < 2048 else 3 if n < 65536 else 4)
    return size

def _json_shape(value, maximum, *, depth=0, count=None):
    """Exact UTF-8 JSON length and conservative object charge, without dumps."""
    count = [0] if count is None else count
    count[0] += 1
    if depth > 32 or count[0] > 65536:
        _frame_budget_error('record metadata exceeds depth/node budget')
    kind = type(value)
    if isinstance(value, Mapping):
        size, resident = 2 + max(0, len(value) - 1), 256 + 32 * len(value)
        for key, child in value.items():
            if type(key) is not str:
                raise DataflowError('INVALID_CONTRACT', 'record metadata keys must be strings')
            n, m = _json_shape(child, maximum, depth=depth + 1, count=count)
            size += _string_size(key) + 1 + n
            resident += sys.getsizeof(key) + m
            if size > maximum: _frame_budget_error('record metadata exceeds control byte budget')
    elif kind in (list, tuple):
        size, resident = 2 + max(0, len(value) - 1), 64 + 16 * len(value)
        for child in value:
            n, m = _json_shape(child, maximum, depth=depth + 1, count=count)
            size, resident = size + n, resident + m
            if size > maximum: _frame_budget_error('record metadata exceeds control byte budget')
    elif kind is str:
        if len(value) > maximum: _frame_budget_error('record metadata exceeds control byte budget')
        size, resident = _string_size(value), sys.getsizeof(value)
    elif kind is int:
        if value.bit_length() > maximum * 4: _frame_budget_error('record integer exceeds byte budget')
        size, resident = len(str(value)), sys.getsizeof(value)
    elif kind is float:
        size, resident = len(json.dumps(value, allow_nan=True)), sys.getsizeof(value)
    elif kind is bool:
        size, resident = (4 if value else 5), sys.getsizeof(value)
    elif value is None:
        size, resident = 4, 16
    else:
        raise DataflowError('INVALID_CONTRACT', 'record metadata must be data-only JSON')
    if size > maximum: _frame_budget_error('record metadata exceeds control byte budget')
    return size, resident

def json_encoding_shape(value):
    """Python tree cost, exact compact UTF-8 length and Unicode text width."""
    def width(item):
        if type(item) is str:
            if item.isascii():
                return 1
            return max((1 if ord(c) < 256 else 2 if ord(c) <= 65535 else 4 for c in item), default=1)
        if type(item) is dict:
            return max((max(width(key), width(child)) for key, child in item.items()), default=1)
        if type(item) in (list, tuple):
            return max(map(width, item), default=1)
        return 1
    encoded, resident = _json_shape(value, sys.maxsize)
    return resident, encoded, width(value)

def json_copy_workspace(value):
    """UTF-8 input, decoded Python text and newly reconstructed containers."""
    resident, encoded, width = json_encoding_shape(value)
    return encoded + width * encoded + resident

def _typed_shape(value, maximum, *, depth=0, count=None):
    """Measure the existing tagged format before constructing its JSON tree."""
    from demiflow.execution.contracts.dataflow import _FrozenRowList, _FrozenRowSet
    count = [0] if count is None else count
    count[0] += 1
    if depth > 32 or count[0] > 65536: _frame_budget_error('record structure exceeds depth/node budget')
    kind = type(value)
    native, tree = sys.getsizeof(value), 256
    if value is None:
        size = 12  # {"t":"null"}
    elif kind in (bool, int, float, str):
        tag = {bool:'bool', int:'int', float:'float', str:'str'}[kind]
        n, _ = _json_shape(value, maximum)
        size = 13 + len(tag) + n
    elif kind is bytes:
        n = 4 * ((len(value) + 2) // 3)
        size, tree = 20 + n, tree + n + 64
    elif isinstance(value, Mapping):
        size, tree = 18 + max(0, len(value) - 1), tree + 64 + 16 * len(value)
        for key, child in value.items():
            if type(key) is not str:
                raise DataflowError('INVALID_CONTRACT', 'record mapping keys must be strings')
            n, m, t = _typed_shape(child, maximum, depth=depth + 1, count=count)
            size += 3 + _string_size(key) + n
            native += sys.getsizeof(key) + m
            tree += 80 + t
            if size > maximum: _frame_budget_error('encoded record exceeds the row byte budget')
    elif kind in (list, tuple, _FrozenRowList, set, frozenset, _FrozenRowSet):
        tag = ('tuple' if kind is tuple else 'frozenset' if kind is frozenset
               else 'set' if kind in (set, _FrozenRowSet) else 'list')
        size, tree = 15 + len(tag) + max(0, len(value) - 1), tree + 64 + 16 * len(value)
        for child in value:
            n, m, t = _typed_shape(child, maximum, depth=depth + 1, count=count)
            size, native, tree = size + n, native + m, tree + t
        if kind in (set, frozenset, _FrozenRowSet): tree += size + 64 * len(value)
    else:
        from demiflow.execution.contracts.array_values import numpy_value
        if numpy_value(value) is not None:
            # Original array validation/encoding is still authoritative. Bound
            # the data/base64 copies before constructing their descriptor.
            n = int(value.nbytes)
            if n > maximum: _frame_budget_error('NumPy payload exceeds row byte budget')
            shape = tuple(value.shape)
            import numpy as np
            tag = 'numpy_scalar' if isinstance(value, np.generic) else 'numpy_array'
            node = {'t':tag,'dtype':value.dtype.str,'shape':shape,'v':''}
            size, _ = _json_shape(node, maximum)
            size += 4 * ((n + 2) // 3)
            native += n
            tree += size + n
        else:
            from demiflow.execution.contracts.scalar_values import scalar_node
            if native > maximum: _frame_budget_error('scalar value exceeds row byte budget')
            node = scalar_node(value, max_bytes=maximum)
            if node is None:
                raise DataflowError('INVALID_CONTRACT', 'record payload carries a non-data object')
            size, tree = _json_shape(node, maximum)
    if size > maximum: _frame_budget_error('encoded record exceeds the row byte budget')
    return size, native, tree

def _admit(peak, maximum):
    if peak > maximum:
        raise DataflowError('BUDGET_EXCEEDED',
            f'record frame codec workspace exceeds original resident budget ({peak} > {maximum})',
            context={'codec_workspace_bytes': peak, 'resident_limit_bytes': maximum})

def admit_workspace(size, bounds, *, resident_bytes=None, retained=0):
    """Check a caller-owned reader/metadata peak against the original grant."""
    _admit(size + retained, resident_limit(bounds, resident_bytes))

def metadata_bytes(value):
    """Retained size of platform-owned finite/index metadata, without copying.

    This deliberately does not measure business objects or invoke their hooks.
    References alias immutable strings, so identity de-duplicates those leaves.
    """
    from demiflow.execution.contracts.dataflow import ArtifactRef
    seen = set()
    def visit(item):
        key = id(item)
        if key in seen:
            return 0
        seen.add(key)
        if len(seen) > 262144:
            _frame_budget_error('finite metadata exceeds its bounded node count')
        if type(item) is ArtifactRef:
            return sys.getsizeof(item) + visit(vars(item))
        if isinstance(item, dict):
            size = dict.__sizeof__(item) + 64
            size += sum(visit(k) + visit(v) for k, v in item.items())
            # A validation capability only adds its tuples. Referenced service,
            # assignment and bounds are pre-existing owners, not copied state.
            proof = getattr(item, '_proof', None)
            if proof is not None:
                size += tuple.__sizeof__(proof) + tuple.__sizeof__(proof[-1]) + 64
            return size
        if type(item) in (tuple, list, frozenset):
            return sys.getsizeof(item) + sum(visit(child) for child in item)
        if type(item) in (str, bytes, int, float, bool, type(None)):
            return sys.getsizeof(item)
        raise DataflowError('INVALID_CONTRACT', 'finite metadata must contain only closed platform data')
    return visit(value)

def decode_metadata(raw, bounds, *, resident_bytes=None, retained=0):
    _admit(retained + raw_workspace(raw, typed=False), resident_limit(bounds, resident_bytes))
    return json.loads(raw)

def _record_metadata(record):
    
    metadata = {'v':3, 'sp':record.source_position, 'eb':record.encoded_bytes, 'p':None,
                'r':None if record.payload_ref is None else _ref_document(record.payload_ref)}
    if record.schema_ref is not None: metadata['s'] = _ref_document(record.schema_ref)
    if record.block_partition is not None: metadata['bp'] = asdict(record.block_partition)
    return metadata

def retained_record_bytes(record, bounds):
    native = _typed_shape(record.inline, bounds.max_row_bytes)[1] if record.inline is not None else 0
    return (native + sys.getsizeof(record) + sys.getsizeof(vars(record))
            + _json_shape(record.source_position, bounds.max_control_bytes)[1]
            + metadata_bytes((record.record_id, record.sequence, record.encoded_bytes,
                              record.payload_ref, record.schema_ref)))

def admit_row_copies(value, bounds, *, resident_bytes, retained=0):
    """Check the actual input/frozen copy and the native typed row codec peak."""
    size, native, tree = _typed_shape(value, bounds.max_row_bytes)
    _admit(retained + max(2 * native, native + tree + 2 * size), resident_limit(bounds, resident_bytes))
    return native

def admit_encoded_record(record, raw, bounds, *, resident_bytes=None, retained=0):
    """Re-admit a same-invocation validated document after other owners grew."""
    if type(raw) is not bytes or len(raw) > document_limit(bounds):
        _frame_budget_error('record document exceeds its frame bound')
    native = _typed_shape(record.inline, bounds.max_row_bytes)[1] if record.inline is not None else 0
    _, metadata = _json_shape(_record_metadata(record), bounds.max_control_bytes)
    _admit(retained + native + metadata + len(raw) + 4096, resident_limit(bounds, resident_bytes))

def encode_document(record, bounds, *, resident_bytes=None, payload_limit=None, retained=0):
    
    maximum = bounds.max_row_bytes if payload_limit is None else min(bounds.max_row_bytes, payload_limit)
    if record.inline is not None:
        size, native, tree = _typed_shape(record.inline, maximum)
    else:
        size, native, tree = record.encoded_bytes, 0, 0
    if type(size) is not int or size < 0 or size > maximum:
        _frame_budget_error('encoded record exceeds the row byte budget')
    meta_size, meta_memory = _json_shape(_record_metadata(record), bounds.max_control_bytes)
    doc_size = meta_size + (size - 4 if record.inline is not None else 0)
    if doc_size > document_limit(bounds): _frame_budget_error('record document exceeds its frame bound')
    # Existing row + tagged tree + encoded payload + joined document. Strings
    # in the tagged tree alias the immutable row; native charges them once.
    _admit(retained + native + tree + meta_memory + 2 * doc_size + 512,
           resident_limit(bounds, resident_bytes))
    _validate_record(record)
    return _encode_record_document(record, max_bytes=document_limit(bounds),
                                    payload_max_bytes=maximum)

_JSON_TOKENS = re.compile(rb'["{}\[\],:]')

def raw_workspace(raw, *, typed=True):
    """Bound JSON parse/reconstruction before loading any attacker-sized tree."""
    # Object/list headers dominate tiny numeric containers; strings dominate
    # wide rows. Scan bytes without a text copy and count structural tokens only
    # outside strings. Escapes may shrink, never expand beyond four-byte Unicode.
    objects = arrays = strings = string_bytes = colons = commas = numbers = number_bytes = 0
    decoded_string_bytes = 0
    depth = 0
    # Fixed, bounded lexical context: identify binary leaf v spans without
    # loading their strings or the JSON tree. Last/duplicate fields remain
    # conservatively charged; field order and escaped tag spellings are safe.
    from array import array
    flags = bytearray(128)
    binary_spans = array('Q', [0]) * 128
    extra = 0
    cursor = 0
    while cursor < len(raw):
        token = _JSON_TOKENS.search(raw, cursor)
        if token is None: break
        start = cursor
        while start < token.start() and raw[start] in b' \t\r\n': start += 1
        if start < token.start():
            numbers += 1
            number_bytes += token.start() - start
        byte = raw[token.start()]
        cursor = token.end()
        if byte == 34:
            strings += 1
            start = cursor
            while True:
                end = raw.find(b'"', cursor)
                if end < 0: end = len(raw); break
                slash = end - 1
                while slash >= start and raw[slash] == 92: slash -= 1
                if (end - slash - 1) % 2 == 0: break
                cursor = end + 1
            string_bytes += end - start
            decoded_span = end - start
            escape = raw.find(b'\\', start, end)
            while escape >= 0 and escape + 1 < end:
                unicode_escape = raw[escape + 1] == 117 and escape + 6 <= end
                # ASCII JSON escapes shrink to one character; a Unicode escape
                # needs at most four bytes in a decoded string. Count their
                # surviving representation, not the encoded escape twice.
                decoded_span -= 2 if unicode_escape else 1
                escape = raw.find(b'\\', escape + (6 if unicode_escape else 2), end)
            decoded_string_bytes += decoded_span
            if depth and flags[depth - 1] & 1:
                index = depth - 1
                flag = flags[index]
                token_value = b''
                if flag & (16 | 2) and end - start <= 72:
                    token_value = raw[start:end]
                    if b'\\u' in token_value:
                        try: token_value = json.loads(raw[start - 1:end + 1]).encode('ascii')
                        except (ValueError, UnicodeError): token_value = b''
                if flag & 16:  # Object key, before the colon.
                    flags[index] = (flag & 9) | (2 if token_value == b't' else 4 if token_value == b'v' else 0)
                elif flag & 2 and token_value in (b'bytes', b'numpy_array', b'numpy_scalar'):
                    flags[index] |= 8
                elif flag & 4:
                    binary_spans[index] = max(binary_spans[index], end - start)
            cursor = end + 1
        elif byte in (123, 91):
            objects += byte == 123
            arrays += byte == 91
            if depth >= 128: _frame_budget_error('record frame exceeds codec depth bound')
            flags[depth] = 17 if byte == 123 else 0
            binary_spans[depth] = 0
            depth += 1
        elif byte in (125, 93):
            if depth:
                depth -= 1
                if flags[depth] & 8: extra += binary_spans[depth]
        elif byte == 58: colons += 1
        else:
            commas += 1
            if depth and flags[depth - 1] & 1:
                flags[depth - 1] = (flags[depth - 1] & 9) | 16
    if objects + arrays + strings + colons + commas > 262144:
        _frame_budget_error('record frame structure exceeds codec workspace bound')
    text = len(raw) + 64 if raw.isascii() else 4 * len(raw) + 80
    # Dict/list tables and scalar slots are distinct. Counting every colon
    # and comma as another full object double charges keys already in leaves.
    # 48 bytes per value slot covers a pointer and a small decoded number;
    # long integers are additionally covered by the original raw byte spans.
    containers = objects * 192 + arrays * 64 + (colons + commas) * 16
    containers += numbers * 40 + number_bytes
    leaves = strings * 64 + (decoded_string_bytes if raw.isascii() else 4 * string_bytes)
    # Parsing, row reconstruction/freeze and canonical validation are separate
    # peaks. Include two mutable/frozen container trees and a canonical buffer.
    parsed = len(raw) + text + containers + leaves
    if not typed:
        return max(parsed, 2 * len(raw) + containers + leaves) + 512
    # Binary decoding retains the decoded bytes/ndarray while canonical
    # validation builds a second base64 bytes + text pair. Charge a further
    # full string span for that closed codec family, before JSON allocation.
    return max(parsed, 3 * len(raw) + containers + leaves,
               len(raw) + 2 * containers + leaves) + extra + 2048

def read_chunk_bytes(bounds, resident_bytes=None):
    return max(1, min(4096, bounds.max_row_bytes, resident_limit(bounds, resident_bytes) // 32))

def admit_document(raw, bounds, *, resident_bytes=None, retained=0):
    if type(raw) is not bytes or len(raw) > document_limit(bounds):
        _frame_budget_error('record document exceeds its frame bound')
    workspace = raw_workspace(raw)
    _admit(retained + workspace, resident_limit(bounds, resident_bytes))
    return workspace

def decode_document(raw, *, record_id, sequence, bounds, resident_bytes=None, retained=0,
                    check_workspace=None):
    
    workspace = admit_document(raw, bounds, resident_bytes=resident_bytes, retained=retained)
    if check_workspace is not None:
        # The caller's collection/read-window check and our codec admission
        # share one measurement; both still precede every parse/allocation.
        check_workspace(workspace)
    record = _decode_record_document(raw, record_id=record_id, sequence=sequence,
        max_bytes=document_limit(bounds), payload_max_bytes=bounds.max_row_bytes,
        metadata_max_bytes=bounds.max_control_bytes)
    return _validate_record(record)

class _RecordFrameParts:
    """One invocation's original fragments, optionally written as one small row.

    The default iterator keeps the original 512-byte encoding workspace. Only
    the terminal's current owner can admit the additional single-row peak after
    a block switch has flushed; this carries no reusable allocation or cache.
    """
    __slots__ = ('_record', '_bounds', '_raw', '_suffix', '_size', '_native',
                 '_metadata', '_chunks', '_started')

    def __init__(self, record, bounds, raw, suffix, size, native, metadata):
        self._record, self._bounds = record, bounds
        self._raw, self._suffix, self._size = raw, suffix, size
        self._native, self._metadata = native, metadata
        self._started = False
        self._chunks = self._original_chunks(raw, suffix)

    @staticmethod
    def _original_chunks(raw, suffix):
        yield b'{"document":"'
        for offset in range(0, len(raw), 512):
            yield raw[offset:offset + 512].replace(b'\\', b'\\\\').replace(b'"', b'\\"')
        yield suffix

    def __iter__(self):
        return self

    def __next__(self):
        self._started = True
        if self._chunks is None:
            raise StopIteration
        try:
            return next(self._chunks)
        except StopIteration:
            self.close()
            raise

    def coalesce(self, record, bounds, *, size, max_write_bytes, admit):
        if (self._started or self._chunks is None or self._record is not record
                or self._bounds is not bounds or type(self._raw) is not bytes
                or size != self._size or size > min(16384, max_write_bytes)):
            return None
        # Count original native/provenance objects and document, held suffix,
        # both replace/concatenation temporaries and the final joined row.
        # This conservatively covers the previous fragment too. No large
        # escape/join allocation occurs until the current owner returns True.
        try:
            _, source_metadata = _json_shape(_record_metadata(record), bounds.max_control_bytes)
        except DataflowError as error:
            # Optional sizing cannot add a new refusal to historical callers
            # whose original fragments already passed their existing checks.
            if error.code == 'BUDGET_EXCEEDED':
                return None
            raise
        workspace = (self._native + len(self._raw) + self._metadata + source_metadata
                     + 3 * size + 4096)
        if not admit(workspace, record):
            return None
        escaped = raw = None
        try:
            escaped = self._raw.replace(b'\\', b'\\\\').replace(b'"', b'\\"')
            raw = b'{"document":"' + escaped + self._suffix
            if len(raw) != size:
                raise DataflowError('CORRUPT_ARTIFACT', 'record frame differs from measured size')
            return raw
        finally:
            self.close()
            # An error may preserve this frame through its traceback. Keep
            # no additional escaped/joined copies beyond the admitted call.
            del escaped, raw

    def close(self):
        chunks, self._chunks = self._chunks, None
        if chunks is not None:
            chunks.close()
        self._record = self._raw = self._suffix = None

def frame_parts(record, bounds, *, resident_bytes=None, target=None, raw=None, retained=0, newline=True):
    """Return exact size and a one-use bounded stream of the OLD JSONL bytes."""
    if raw is None: raw = encode_document(record, bounds, resident_bytes=resident_bytes, retained=retained)
    from demiflow.execution.recovery_contract import document
    metadata = {'record_id':record.record_id, 'sequence':record.sequence}
    if target is not None: metadata['target'] = target
    meta_size, meta_memory = _json_shape(metadata, bounds.max_control_bytes)
    escaped = len(raw) + raw.count(b'\\') + raw.count(b'"')
    size = len(b'{"document":"') + escaped + 2 + meta_size - 1 + int(newline)
    if size > line_limit(bounds): _frame_budget_error('record JSONL frame exceeds its frame bound')
    native = _typed_shape(record.inline, bounds.max_row_bytes)[1] if record.inline is not None else 0
    _admit(retained + native + len(raw) + meta_memory + 4096, resident_limit(bounds, resident_bytes))
    suffix = b'",' + document(metadata, bounds.max_control_bytes)[1:] + (b'\n' if newline else b'')
    return size, _RecordFrameParts(record, bounds, raw, suffix, size, native, meta_memory)

def encode_frame(record, bounds, *, resident_bytes=None, target=None, retained=0):
    size, chunks = frame_parts(record, bounds, resident_bytes=resident_bytes, target=target, retained=retained)
    native = _typed_shape(record.inline, bounds.max_row_bytes)[1] if record.inline is not None else 0
    # The suspended generator retains its document while escaped pieces and
    # the joined return value coexist; the previous partition rows stay live.
    _admit(retained + 3 * size + native + 4096,
           resident_limit(bounds, resident_bytes))
    return b''.join(chunks)

def decode_frame(line, bounds, *, resident_bytes=None, retained=0, finite=False, newline=True):
    if (type(line) is not bytes or len(line) > line_limit(bounds)
            or newline and not line.endswith(b'\n')):
        _frame_budget_error('stored record frame exceeds its bound or is truncated')
    fast = _decode_canonical_outer(line, bounds, resident_bytes=resident_bytes, retained=retained, finite=finite)
    if fast is not None:
        return fast
    _admit(retained + raw_workspace(line, typed=False), resident_limit(bounds, resident_bytes))
    try:
        item = json.loads(line)
    except (ValueError, UnicodeError) as exc:
        raise DataflowError('CORRUPT_ARTIFACT', 'invalid record frame JSON') from exc
    expected = {'record_id','sequence','document'} | ({'target'} if finite else set())
    if type(item) is not dict or set(item) != expected or type(item['document']) is not str:
        raise DataflowError('CORRUPT_ARTIFACT', 'invalid record frame fields')
    metadata = {key:value for key,value in item.items() if key != 'document'}
    _json_shape(metadata, bounds.max_control_bytes)
    raw_size = len(item['document']) if item['document'].isascii() else 4 * len(item['document'])
    _admit(retained + len(line) + sys.getsizeof(item['document']) + raw_size + 1024,
           resident_limit(bounds, resident_bytes))
    raw = item.pop('document').encode('utf-8')
    if len(raw) > document_limit(bounds): _frame_budget_error('record document exceeds its frame bound')
    return raw, item

_OUTER_SPECIAL = re.compile(rb'["\\]')

def _decode_native_outer(line, bounds, *, resident_bytes, retained, finite):
    """Use the native string decoder only when its copies fit the same grant.

    The delimiter is a candidate, never proof of a JSON boundary. Loading the
    whole quoted slice must confirm exactly one string; unsupported spellings
    and insufficient conservative room keep the existing precise reader.
    """
    prefix = b'{"document":"'
    if not line.startswith(prefix):
        return None
    end = line.find(b'","record_id":', len(prefix))
    if end < 0:
        return None
    quoted_size = end - len(prefix) + 2  # Includes both JSON quotes.
    tail_size = len(line) - end - 1  # The original suffix plus its opening '{'.
    if tail_size > bounds.max_control_bytes + 1:
        return None
    maximum = resident_limit(bounds, resident_bytes)
    # json.loads(bytes) can retain quoted bytes Q, a 4Q Unicode input and a
    # 4Q decoded Unicode string together. Ten Q plus fixed slack covers that
    # peak, then the smaller string->UTF-8 peak. The original line stays live.
    # Four T only admits constructing the tail, not parsing its object tree.
    base = retained + len(line) + 10 * quoted_size + 4096
    if base + 4 * tail_size > maximum:
        return None
    tail = b'{' + line[end + 2:]
    workspace = raw_workspace(tail, typed=False)
    if base + workspace > maximum:
        return None
    _admit(base + workspace, maximum)
    quoted = line[len(prefix) - 1:end + 1]
    try:
        decoded = json.loads(quoted)
    except (ValueError, UnicodeError):
        return None
    del quoted
    if type(decoded) is not str:
        return None
    try:
        raw = decoded.encode('utf-8')
    except UnicodeError:
        return None
    del decoded
    if len(raw) > document_limit(bounds):
        _frame_budget_error('record document exceeds its frame bound')
    # The admitted tail workspace includes its input, text and object tree;
    # the original line and decoded business document remain resident too.
    try:
        item = json.loads(tail)
    except (ValueError, UnicodeError) as exc:
        raise DataflowError('CORRUPT_ARTIFACT', 'invalid record frame metadata') from exc
    expected = {'record_id', 'sequence'} | ({'target'} if finite else set())
    if type(item) is not dict or set(item) != expected:
        raise DataflowError('CORRUPT_ARTIFACT', 'invalid record frame fields')
    _json_shape(item, bounds.max_control_bytes)
    return raw, item

def _decode_canonical_outer(line, bounds, *, resident_bytes, retained, finite):
    """Unescape the old canonical document string without a full text copy.

    Older noncanonical-but-valid outer JSON retains its original parser path.
    This fast path only accepts the exact writer prefix and its two escapes;
    the inner typed codec still performs all original canonical validation.
    """
    native = _decode_native_outer(line, bounds, resident_bytes=resident_bytes, retained=retained, finite=finite)
    if native is not None:
        return native
    prefix = b'{"document":"'
    if not line.startswith(prefix): return None
    start = cursor = len(prefix)
    decoded_size = 0
    while True:
        match = _OUTER_SPECIAL.search(line, cursor)
        if match is None: return None
        index = match.start()
        decoded_size += index - cursor
        if line[index] == 34:
            end = index
            break
        if index + 1 >= len(line) or line[index + 1] not in (34, 92): return None
        decoded_size += 1
        cursor = index + 2
    if decoded_size > document_limit(bounds): _frame_budget_error('record document exceeds its frame bound')
    if line[end + 1:end + 2] != b',': return None
    tail_size = len(line) - end - 2
    if tail_size + 1 > bounds.max_control_bytes + 1:
        _frame_budget_error('record metadata exceeds control byte budget')
    # A bytearray and its returned bytes coexist; suffix metadata is bounded
    # independently and parsed before exposing any business document.
    _admit(retained + len(line) + 2 * decoded_size + 4 * tail_size + 2048,
           resident_limit(bounds, resident_bytes))
    tail = b'{' + line[end + 2:]
    _admit(retained + len(line) + 2 * decoded_size + raw_workspace(tail, typed=False),
           resident_limit(bounds, resident_bytes))
    try: item = json.loads(tail)
    except (ValueError, UnicodeError) as exc:
        raise DataflowError('CORRUPT_ARTIFACT', 'invalid record frame metadata') from exc
    expected = {'record_id','sequence'} | ({'target'} if finite else set())
    if type(item) is not dict or set(item) != expected:
        raise DataflowError('CORRUPT_ARTIFACT', 'invalid record frame fields')
    _json_shape(item, bounds.max_control_bytes)
    del tail
    output = bytearray(decoded_size)
    cursor, offset = start, 0
    view = memoryview(line)
    destination = memoryview(output)
    while cursor < end:
        match = _OUTER_SPECIAL.search(line, cursor, end)
        index = end if match is None else match.start()
        count = index - cursor
        destination[offset:offset + count] = view[cursor:index]
        offset += count
        if index == end: break
        output[offset] = line[index + 1]
        offset += 1
        cursor = index + 2
    return bytes(output), item
