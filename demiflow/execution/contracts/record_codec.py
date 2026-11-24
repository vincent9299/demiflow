"""Record codec implementation."""
from __future__ import annotations

import hashlib
from typing import Any

import io
from demiflow.data.runtime_contract import check_size

import struct
from dataclasses import dataclass

from dataclasses import asdict

import base64

import json
import math
import sys
import re
from collections.abc import Mapping
from demiflow.execution.contracts.dataflow import ArtifactRef, BlockPartition, Record, _FrozenRowList, _FrozenRowSet, thaw_record_value
from demiflow.execution.contracts.dataflow_errors import DataflowError
from demiflow.execution.contracts.recovery_contract import RecoveryContractError, document, identifier


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
        from demiflow.execution.contracts.record_schema import SCHEMA_MEDIA_TYPE
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
        from demiflow.execution.contracts.record_schema import SCHEMA_MEDIA_TYPE
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
    from demiflow.execution.contracts.recovery_contract import document
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


_MAGIC = b'DFARROW1'

_HEADER = struct.Struct('!I')

_MAX_ROWS = 256

def is_arrow_record(record):
    return record.payload_ref is not None and record.payload_ref.media_type in ARROW_MEDIA_TYPES

def _arrow_error(message):
    raise DataflowError('CORRUPT_ARTIFACT', message)

def _value_upper(array, *, depth=0):
    """Typed-row JSON upper bound from Arrow types/buffers, never Python rows.

    Unknown types and oversized structures use the original exact row codec.
    Scalar conversions below are only list offsets / aggregate validity bits.
    """
    import pyarrow as pa
    import pyarrow.compute as pc
    n, kind = len(array), array.type
    if depth > 24 or n > 65536:
        return None
    if pa.types.is_null(kind):
        return 16 * n
    if pa.types.is_boolean(kind) or pa.types.is_integer(kind):
        return 48 * n
    if pa.types.is_floating(kind):
        if n and pc.any(pc.invert(pc.is_finite(array))).as_py():
            return None  # Preserve the original nonfinite validation.
        return 64 * n
    if pa.types.is_string(kind) or pa.types.is_large_string(kind):
        return 32 * n + 6 * array.nbytes
    if pa.types.is_binary(kind) or pa.types.is_large_binary(kind) or pa.types.is_fixed_size_binary(kind):
        return 32 * n + 4 * ((array.nbytes + 2) // 3)
    if pa.types.is_struct(kind):
        total = 32 * n
        for index, field in enumerate(kind):
            value = _value_upper(array.field(index), depth=depth + 1)
            if value is None:
                return None
            total += n * (len(json.dumps(field.name, ensure_ascii=False).encode()) + 8) + value
        return total
    if pa.types.is_list(kind) or pa.types.is_large_list(kind):
        first = array.offsets[0].as_py()
        last = array.offsets[-1].as_py()
        value = _value_upper(array.values.slice(first, last - first), depth=depth + 1)
        return None if value is None else 32 * n + value
    if pa.types.is_fixed_size_list(kind):
        child = array.values.slice(array.offset * kind.list_size, n * kind.list_size)
        value = _value_upper(child, depth=depth + 1)
        return None if value is None else 32 * n + value
    return None

def row_upper(batch, index):
    total = 32
    for field, column in zip(batch.schema, batch.columns):
        value = _value_upper(column.slice(index, 1))
        if value is None:
            return None
        total += len(json.dumps(field.name, ensure_ascii=False).encode()) + 8 + value
    return total

def _resident_budget(bounds, assignment=None):
    resident = bounds.max_resident_bytes
    if assignment is not None:
        stages = getattr(assignment, 'stages', ())
        for stage in stages:
            resident = min(resident, stage.profile.requests['memory_bytes'])
    return resident

def _window(bounds, assignment=None):
    resident = _resident_budget(bounds, assignment)
    # Source input + IPC encoding + authenticated read + prior yielded batch,
    # and downstream row conversion may coexist. Keep explicit finite headroom.
    return max(0, min(bounds.max_batch_bytes // 8, resident // 32))

@dataclass(frozen=True)
class ArrowPayload:
    ref: object
    encoded_bytes: int

_UTF8_STRING_ESCAPE = re.compile(rb'["\\\x00-\x1f]')

def mapped_row_bytes(batch, index, maximum):
    """Exact tagged-row size for Arrow outputs, without constructing a row or copying its binary values."""
    import pyarrow as pa


    count = 1
    def value_size(value, depth):
        nonlocal count
        count += 1
        if count > _MAX_NODES or depth > _MAX_DEPTH:
            raise DataflowError('BUDGET_EXCEEDED', 'record structure exceeds depth/node budget')
        if not value.is_valid:
            return 12
        dtype = value.type
        if pa.types.is_binary(dtype) or pa.types.is_large_binary(dtype) or pa.types.is_fixed_size_binary(dtype):
            return 20 + 4 * ((value.as_buffer().size + 2) // 3)
        if pa.types.is_string(dtype) or pa.types.is_large_string(dtype):
            # Count JSON escapes in UTF-8; regular expression scanning stays in C
            # for unescaped long strings. json_string_size uses the same escapes.
            buf = memoryview(value.as_buffer()).cast('B')
            size = len(buf) + 2
            for match in _UTF8_STRING_ESCAPE.finditer(buf):
                char = buf[match.start()]
                size += 1 if char in b'\"\\\b\f\n\r\t' else 5
            return 16 + size
        if pa.types.is_struct(dtype):
            return mapping_size(((field.name, value[field.name]) for field in dtype), depth)
        if pa.types.is_list(dtype) or pa.types.is_large_list(dtype) or pa.types.is_fixed_size_list(dtype):
            values = value.values
            return 19 + max(0, len(values) - 1) + sum(value_size(v, depth + 1) for v in values)
        return _typed_shape(value.as_py(), maximum)[0]

    def mapping_size(fields, depth=0):
        size = 18
        for ordinal, (name, value) in enumerate(fields):
            size += int(ordinal > 0) + 3 + _string_size(name) + value_size(value, depth + 1)
        return size

    return mapping_size((field.name, column[index]) for field, column in zip(batch.schema, batch.columns))

def mapped_member(record_id, position):
    from demiflow.execution.contracts.dataflow_codec import _thaw_json as thaw_control_json
    from demiflow.execution.contracts.recovery_contract import identity
    return identity([record_id, thaw_control_json(position)])

def _uncompressed_ipc(buffer, bounds):
    """Inspect bounded IPC messages before Arrow can allocate decoded arrays.

    Our writer emits schema + one uncompressed record batch, no dictionaries.
    The FlatBuffer check reads only Message.header and RecordBatch.compression
    slots; a compressed/custom carrier is not part of this MIME contract.
    """
    import pyarrow as pa
    messages = pa.ipc.MessageReader.open_stream(buffer)
    schema, record = messages.read_next_message(), messages.read_next_message()
    if schema.type != 'schema' or record.type != 'record batch':
        _arrow_error('Arrow carrier requires schema and one record batch')
    try:
        messages.read_next_message()
    except StopIteration:
        pass
    else:
        _arrow_error('Arrow carrier contains extra IPC messages')
    for message in (schema, record):
        if message.metadata.size > min(65536, bounds.max_control_bytes):
            _arrow_error('Arrow IPC metadata exceeds decode bound')
    metadata = memoryview(record.metadata)
    def field(table, index):
        if not 0 <= table <= len(metadata)-4:
            _arrow_error('invalid Arrow IPC table')
        vtable = table - struct.unpack_from('<i', metadata, table)[0]
        if not 0 <= vtable <= len(metadata)-4:
            _arrow_error('invalid Arrow IPC vtable')
        size = struct.unpack_from('<H', metadata, vtable)[0]
        slot = 4 + 2*index
        if slot + 2 > size:
            return None
        offset = struct.unpack_from('<H', metadata, vtable+slot)[0]
        return table+offset if offset else None
    try:
        root = struct.unpack_from('<I', metadata, 0)[0]
        pointer = field(root, 2)  # org.apache.arrow.flatbuf.Message.header
        if pointer is None:
            _arrow_error('Arrow IPC message lacks record header')
        table = pointer + struct.unpack_from('<I', metadata, pointer)[0]
        if field(table, 3) is not None:  # RecordBatch.compression
            _arrow_error('compressed Arrow IPC is not an admitted batch carrier')
    except (struct.error, IndexError) as exc:
        raise DataflowError('CORRUPT_ARTIFACT', 'invalid Arrow IPC record header') from exc


def _error(message):
    return DataflowError('BUDGET_EXCEEDED', 'batch conversion admission: ' + message)

def is_lance_blob_type(dtype):
    """A single known native extension; arbitrary user hooks remain refused."""
    if type(dtype).__module__ != 'lance.blob' or type(dtype).__name__ != 'BlobType':
        return False
    from lance.blob import BlobType
    if type(dtype) is not BlobType:
        return False
    import pyarrow as pa
    fields = tuple(dtype.storage_type)
    expected = (('data', pa.large_binary()), ('uri', pa.string()),
                ('position', pa.uint64()), ('size', pa.uint64()))
    return (len(fields) in (2, 4)
            and all(field.name == name and field.type == kind
                    for field, (name, kind) in zip(fields, expected))
            and all(field.nullable for field in fields[:2]))

def _blob_storage_value(value):
    # Match the native blob constructor without reading a URI or copying bytes.
    if value is None or type(value) is dict:
        return value
    if type(value) is bytes:
        return {'data':value, 'uri':None, 'position':None, 'size':None}
    if type(value) is str:
        return {'data':None, 'uri':value, 'position':None, 'size':None}
    raise TypeError('Lance blob value requires bytes, URI, native storage dict or None')

def _builtin_storage(dtype):
    """Known native storage layouts; never invoke a user extension hook."""
    import pyarrow as pa
    known = tuple(getattr(pa, name) for name in (
        'JsonType', 'UuidType', 'Bool8Type', 'FixedShapeTensorType', 'OpaqueType')
                  if hasattr(pa, name))
    return dtype.storage_type if type(dtype) in known or is_lance_blob_type(dtype) else None

def _binary_size(value):
    if type(value) is bytes or type(value) is str and value.isascii():
        return len(value)
    if type(value) is str:
        # Exact UTF-8 length without allocating a temporary encoded payload.
        return sum(1 if ord(char) < 128 else 2 if ord(char) < 2048 else
                   3 if ord(char) < 65536 else 4 for char in value)
    raise TypeError('Arrow string/binary conversion requires str or bytes')

def writer_metadata_bytes(value):
    return sum(128 + 4 * (len(key) + len(item)) for key, item in (value or {}).items())

def writer_type_bytes(dtype, bounds, *, depth=0, count=None):
    """Schema/IPC descriptors only; array buffers have their own accounting."""
    count = [0] if count is None else count
    count[0] += 1
    if depth > min(16, bounds.max_depth) or count[0] > bounds.max_nodes:
        raise _error('native writer schema exceeds structural budget')
    import pyarrow as pa
    # Includes FlatBuffer field/type, validity/data buffer descriptors, record
    # message framing and alignment. Schema names/metadata are counted below.
    size = 512
    if isinstance(dtype, pa.BaseExtensionType):
        storage = _builtin_storage(dtype)
        if storage is None:
            raise DataflowError('UNSUPPORTED_CAPABILITY', 'unbounded writer extension metadata')
        size += writer_type_bytes(storage, bounds, depth=depth+1, count=count)
    elif pa.types.is_dictionary(dtype):
        size += writer_type_bytes(dtype.index_type, bounds, depth=depth+1, count=count)
        size += writer_type_bytes(dtype.value_type, bounds, depth=depth+1, count=count)
    else:
        for index in range(dtype.num_fields):
            field = dtype.field(index)
            size += 4 * len(field.name) + writer_metadata_bytes(field.metadata)
            size += writer_type_bytes(field.type, bounds, depth=depth+1, count=count)
    return size

@dataclass(frozen=True)
class _Column:
    count: int
    values: object  # Re-iterable factory over existing values; no flattened copy.
    implicit_nulls: int = 0

class _Admission:
    def __init__(self, rows, bounds, reserved_bytes=0, *, resident_workspace=False, writer_copies=0,
                 writer_metadata=False):
        if type(reserved_bytes) is not int or reserved_bytes < 0:
            raise DataflowError('INVALID_CONTRACT', 'conversion reserve must be a nonnegative integer')
        if (type(resident_workspace) is not bool or type(writer_copies) is not int or writer_copies < 0
                or type(writer_metadata) is not bool):
            raise DataflowError('INVALID_CONTRACT', 'invalid conversion workspace contract')
        self.maximum = bounds.max_resident_bytes - reserved_bytes
        if not resident_workspace:
            self.maximum = min(bounds.max_batch_bytes, self.maximum)
        self.writer_copies = writer_copies
        self.writer_metadata = writer_metadata
        self.writer_overhead = io.DEFAULT_BUFFER_SIZE + 2048 if writer_metadata else 0
        self.buffers = self.builder = 0
        self.depth = min(16, bounds.max_depth)
        self.max_types = bounds.max_nodes
        self.types = self.visits = 0
        self.retained = sys.getsizeof(rows)
        for row in rows:
            self.retained += check_size(row, bounds.max_row_bytes)
            self.check(0)
        self.check(0)

    def check(self, extra):
        if self.retained + extra + self.writer_copies * self.buffers + self.writer_overhead > self.maximum:
            raise _error('input and conversion workspace exceed byte budget')

    def visit(self, depth):
        self.visits += 1
        if depth > self.depth or self.visits > max(1, self.maximum // 8):
            raise _error('value traversal exceeds structural budget')

    def buffer(self, size):
        if size < 0 or size > self.maximum:
            raise _error('declared buffer exceeds byte budget')
        if size:
            # Arrow builders grow geometrically, with 64-byte minimum alignment.
            # Reserve the old allocation, its replacement and final compaction
            # per buffer, including fixed-width allocations for null values.
            capacity = 1 << (max(64, size) - 1).bit_length()
            self.buffers += capacity
            self.builder += capacity * 3
        self.check(self.builder)

    def arrow(self, column, dtype, depth=0):
        import pyarrow as pa
        self.types += 1
        if depth > self.depth or self.types > self.max_types:
            raise _error('Arrow type traversal exceeds structural budget')
        self.builder += 256  # bounded builder/type bookkeeping per array
        self.check(self.builder)
        n = column.count
        if pa.types.is_null(dtype):
            return
        if isinstance(dtype, pa.BaseExtensionType):
            storage = _builtin_storage(dtype)
            if storage is not None:
                # Keep known native storage declarations, including empty
                # and null-only typed columns, on the real constructor.
                if is_lance_blob_type(dtype):
                    original = column
                    column = _Column(n, lambda: (_blob_storage_value(value)
                                                  for value in original.values()), column.implicit_nulls)
                    # Native blob_array holds five pointer lists, a null mask,
                    # optional Blob wrappers and per-column constructor lists.
                    self.builder += 512 * n
                    self.check(self.builder)
                self.arrow(column, storage, depth + 1)
                return
            raise DataflowError('UNSUPPORTED_CAPABILITY',
                'batch conversion admission cannot bound Arrow extension type ' + str(dtype))
        self.buffer((n + 7) // 8)
        if pa.types.is_run_end_encoded(dtype):
            # No compression-rate assumption: at most one run per logical row.
            self.arrow(_Column(n, lambda: iter(()), n), dtype.run_end_type, depth + 1)
            self.arrow(column, dtype.value_type, depth + 1)
            return
        if pa.types.is_string_view(dtype) or pa.types.is_binary_view(dtype):
            if self.writer_metadata:
                # A view can own a distinct variadic data buffer per value;
                # IPC includes each descriptor, even for nested view columns.
                self.writer_overhead += 64 * n
                self.check(self.builder)
            self.buffer((max(32, n) if n else 0) * 16)
            # StringHeapBuilder packs non-inline (>12-byte) values into 32KiB
            # blocks, or one larger block for an oversized value. Account for
            # each old block and its compacted replacement before the next one.
            held = allocated = span = used = peak = blocks = 0
            for value in column.values():
                self.visit(depth)
                if value is None:
                    continue
                size = _binary_size(value)
                if size <= 12:
                    continue
                if size > span - used:
                    if allocated and used < span:
                        compact = ((used + 63) // 64) * 64
                        peak = max(peak, held + compact)
                        held += compact - allocated
                    span, used = max(32768, size), 0
                    allocated = ((span + 63) // 64) * 64
                    held += allocated
                    blocks += 1
                    peak = max(peak, held)
                used += size
                self.check(self.builder + peak + blocks * 64)
            if allocated and used < span:
                peak = max(peak, held + ((used + 63) // 64) * 64)
                held += ((used + 63) // 64) * 64 - allocated
            self.buffers += held
            self.builder += peak + blocks * 64
            self.check(self.builder)
            return
        if pa.types.is_dictionary(dtype):
            self.buffer((max(32, n) if n else 0) * dtype.index_type.bit_width // 8)
            self.buffer(n * 64)  # dictionary memo/index; never assume duplicates
            self.arrow(column, dtype.value_type, depth + 1)
            return
        try:
            bits = dtype.bit_width
        except (ValueError, AttributeError):
            bits = None
        if bits is not None:
            # Primitive builders have kMinBuilderCapacity=32. Fixed binary
            # builders reserve the byte width directly (not 32 huge values).
            capacity = n if pa.types.is_fixed_size_binary(dtype) else max(32, n) if n else 0
            self.buffer((capacity * bits + 7) // 8)
            return
        if any(test(dtype) for test in (pa.types.is_string, pa.types.is_large_string,
                                       pa.types.is_binary, pa.types.is_large_binary)):
            width = 8 if pa.types.is_large_string(dtype) or pa.types.is_large_binary(dtype) else 4
            # BinaryBuilder reserves offsets for the actual input length;
            # primitive builders' minimum of 32 values does not apply here.
            self.buffer((n + 1) * width)
            size = capacity = peak = 0
            for value in column.values():
                self.visit(depth)
                if value is None:
                    continue
                size += _binary_size(value)
                if size > capacity:
                    # Pinned Arrow BufferBuilder::GrowByFactor doubles the
                    # previous capacity or uses this larger request, then
                    # aligns to 64 bytes. A large first value is not rounded
                    # up to a power of two. Old/new coexist during realloc.
                    grown = ((max(size, 2 * capacity) + 63) // 64) * 64
                    peak = max(peak, capacity + grown)
                    capacity = grown
                self.check(self.builder + peak)
            compact = ((size + 63) // 64) * 64
            if compact < capacity:
                peak = max(peak, capacity + compact)
            self.buffers += compact
            self.builder += peak
            self.check(self.builder)
            return
        if pa.types.is_struct(dtype):
            for index, field in enumerate(dtype):
                def values(index=index, name=field.name):
                    for value in column.values():
                        if value is None:
                            yield None
                        elif type(value) is dict:
                            yield value.get(name)
                        elif type(value) in (tuple, list):
                            yield value[index] if index < len(value) else None
                        else:
                            raise TypeError('Arrow struct conversion requires dict or tuple')
                self.arrow(_Column(n, values, column.implicit_nulls), field.type, depth + 1)
            return
        if pa.types.is_map(dtype):
            self.buffer((n + 1) * 4)
            def pairs():
                for value in column.values():
                    if value is not None:
                        yield from value.items() if type(value) is dict else value
            count = 0
            for _ in pairs():
                self.visit(depth)
                count += 1
            self.arrow(_Column(count, lambda: (pair[0] for pair in pairs())), dtype.key_type, depth + 1)
            self.arrow(_Column(count, lambda: (pair[1] for pair in pairs())), dtype.item_type, depth + 1)
            return
        if (pa.types.is_list(dtype) or pa.types.is_large_list(dtype) or pa.types.is_fixed_size_list(dtype)
                or pa.types.is_list_view(dtype) or pa.types.is_large_list_view(dtype)):
            fixed = pa.types.is_fixed_size_list(dtype)
            if pa.types.is_list_view(dtype) or pa.types.is_large_list_view(dtype):
                width = 8 if pa.types.is_large_list_view(dtype) else 4
                self.buffer(n * width)  # offsets
                self.buffer(n * width)  # sizes (views have no trailing offset)
            elif not fixed:
                self.buffer((n + 1) * (8 if pa.types.is_large_list(dtype) else 4))
            count = implicit = 0
            if fixed:
                implicit = column.implicit_nulls * dtype.list_size
            for value in column.values():
                self.visit(depth)
                if value is None:
                    if fixed:
                        implicit += dtype.list_size
                elif type(value) in (tuple, list):
                    count += len(value)
                else:
                    raise TypeError('Arrow list conversion requires list or tuple')
            def children():
                for value in column.values():
                    if value is not None:
                        yield from value
            if fixed:
                # Even a malformed short explicit value must not make a large
                # declared child allocation invisible before Arrow rejects it.
                implicit += max(0, n * dtype.list_size - count - implicit)
            self.arrow(_Column(count + implicit, children, implicit), dtype.value_type, depth + 1)
            return
        raise DataflowError('UNSUPPORTED_CAPABILITY',
            'batch conversion admission cannot bound Arrow layout ' + str(dtype))

    def boxed(self, value, depth=0):
        """Conservative Python object output, including Pandas nested objects."""
        self.visit(depth)
        if value is None:
            return 16
        if type(value) in (bool, int, float):
            return max(128, sys.getsizeof(value))
        if type(value) in (str, bytes):
            return sys.getsizeof(value) + 64
        if type(value) in (list, tuple, set, frozenset):
            size = 128 + 8 * len(value)
            for child in value:
                size += self.boxed(child, depth + 1)
                self.check(size)
            return size
        if type(value) is dict:
            size = 256 + 64 * len(value)
            for key, child in value.items():
                size += self.boxed(key, depth + 1) + self.boxed(child, depth + 1)
                self.check(size)
            return size
        # check_size already admitted the closed temporal/Decimal scalar domain.
        return 256 + check_size(value, self.maximum)

    def numpy(self, column):
        import numpy as np
        def value_shape(value, depth=0):
            self.visit(depth)
            if type(value) in (list, tuple):
                shape, dtype = common(iter(value), depth + 1)
                return (len(value), *shape), dtype
            if value is None or type(value) in (dict, set, frozenset):
                return (), np.dtype('object')
            if type(value) is bool:
                return (), np.dtype('bool')
            if type(value) is int:
                kind = 'int64' if -(2**63) <= value < 2**63 else 'uint64' if 0 <= value < 2**64 else 'object'
                return (), np.dtype(kind)
            if type(value) is float:
                return (), np.dtype('float64')
            if type(value) is str:
                return (), np.dtype('U' + str(max(1, len(value))))
            if type(value) is bytes:
                return (), np.dtype('S' + str(max(1, len(value))))
            return (), np.dtype('object')
        def common(values, depth):
            shape = dtype = None
            for value in values:
                child_shape, child_dtype = value_shape(value, depth)
                if shape is not None and child_shape != shape:
                    raise ValueError('setting an array element with a sequence: inhomogeneous shape')
                shape = child_shape
                dtype = child_dtype if dtype is None else np.promote_types(dtype, child_dtype)
            return shape or (), dtype if dtype is not None else np.dtype('float64')
        shape, dtype = common(column.values(), 0)
        count = column.count
        for dimension in shape:
            count *= dimension
        size = count * dtype.itemsize
        # Source column references, final ndarray and a conversion/cast buffer.
        # String promotion is included in dtype.itemsize, not average row width.
        return 256 + 8 * column.count + 2 * size

    def typed_boxed(self, value, dtype, depth=0):
        """Pandas objects after declared Arrow coercions, including nested ones."""
        import pyarrow as pa
        self.visit(depth)
        if value is None:
            return 16
        storage = _builtin_storage(dtype)
        if storage is not None:
            if is_lance_blob_type(dtype):
                value = _blob_storage_value(value)
            return self.typed_boxed(value, storage, depth + 1)
        if pa.types.is_dictionary(dtype) or pa.types.is_run_end_encoded(dtype):
            return self.typed_boxed(value, dtype.value_type, depth + 1)
        if pa.types.is_string(dtype) or pa.types.is_large_string(dtype) or pa.types.is_string_view(dtype):
            return 80 + 4 * len(value) if type(value) is bytes else self.boxed(value, depth)
        if (pa.types.is_binary(dtype) or pa.types.is_large_binary(dtype)
                or pa.types.is_fixed_size_binary(dtype) or pa.types.is_binary_view(dtype)):
            return 48 + 4 * len(value) if type(value) is str else self.boxed(value, depth)
        if pa.types.is_struct(dtype):
            size = 256 + len(dtype) * 64
            for index, field in enumerate(dtype):
                child = value.get(field.name) if type(value) is dict else value[index]
                size += self.boxed(field.name, depth + 1) + self.typed_boxed(child, field.type, depth + 1)
                self.check(size)
            return size
        if (pa.types.is_list(dtype) or pa.types.is_large_list(dtype) or pa.types.is_fixed_size_list(dtype)
                or pa.types.is_list_view(dtype) or pa.types.is_large_list_view(dtype)):
            size = 128 + 8 * len(value)
            for child in value:
                size += self.typed_boxed(child, dtype.value_type, depth + 1)
                self.check(size)
            return size
        if pa.types.is_map(dtype):
            size = 128 + 64 * len(value)
            for key, child in value.items() if type(value) is dict else value:
                size += self.typed_boxed(key, dtype.key_type, depth + 1)
                size += self.typed_boxed(child, dtype.item_type, depth + 1)
                self.check(size)
            return size
        return self.boxed(value, depth)

    def inference_guard(self, values, workspace):
        # Bound inferred type metadata before Arrow inspects it. The set is
        # limited by original max_nodes, not by unbounded business row count.
        seen = set()
        def visit(value, path=(), depth=0):
            self.visit(depth)
            if type(value) is dict:
                for name, child in value.items():
                    key = (*path, name)
                    seen.add(key)
                    if len(seen) > self.max_types:
                        raise _error('inferred Arrow schema exceeds structural budget')
                    self.check(workspace + len(seen) * 256)
                    visit(child, key, depth + 1)
            elif type(value) in (list, tuple):
                for child in value:
                    visit(child, (*path, None), depth + 1)
        for value in values:
            visit(value)

def admit_batch_conversion(rows, batch_format, *, schema, bounds, reserved_bytes=0,
                           resident_workspace=False, writer_copies=0, writer_metadata=False,
                           observe_writer_workspace=None):
    """Return the existing window after proving constructor workspace fits.

    Type inference (only when no schema was declared) creates type metadata from
    an admitted pointer list, never an Arrow/NumPy payload array. Known schemas
    remain unchanged and are passed directly to the actual constructor.
    """
    admission = _Admission(rows, bounds, reserved_bytes,
                           resident_workspace=resident_workspace, writer_copies=writer_copies,
                           writer_metadata=writer_metadata)
    if writer_metadata and schema is not None:
        admission.writer_overhead += writer_metadata_bytes(schema.metadata)
        admission.check(0)
    n = len(rows)
    fmt = 'numpy' if batch_format == 'default' else batch_format
    if fmt == 'list':
        return admission.retained, admission.maximum
    if fmt == 'numpy':
        # NumPy's existing terminal ignores Arrow schema and exposes the actual
        # row columns. A schema must not hide an additional allocating column.
        names = tuple(rows[0]) if rows else ()
    elif fmt == 'pandas' and schema is None:
        names = set()
        for row in rows:
            names.update(row)
            admission.check(len(names) * 256)
        names = tuple(names)
    else:
        names = tuple(schema.names) if schema is not None else tuple(rows[0]) if rows else ()
    if len(names) > bounds.max_nodes:
        raise _error('column count exceeds structural budget')
    if fmt == 'numpy':
        working = 0
        for name in names:
            working += admission.numpy(_Column(n, lambda name=name: (row[name] for row in rows)))
            admission.check(working)
        return admission.retained, admission.maximum
    if fmt not in ('pyarrow', 'pandas'):
        raise DataflowError('INVALID_CONTRACT', 'unsupported batch format')
    if fmt == 'pyarrow' or schema is not None:
        import pyarrow as pa
        for index, name in enumerate(names):
            column = _Column(n, lambda name=name: (row.get(name) for row in rows))
            if schema is not None:
                dtype = schema.field(index).type
            else:
                # This pointer-only inference workspace is gone before builders
                # start. No result array is created merely to estimate its size.
                admission.check(64 + 8 * n)
                values = list(column.values())
                admission.inference_guard(values, 64 + 8 * n)
                dtype = pa.infer_type(values)
                del values
            if writer_metadata:
                admission.writer_overhead += 4*len(name) + writer_type_bytes(dtype,bounds)
                if schema is not None:
                    admission.writer_overhead += writer_metadata_bytes(schema.field(index).metadata)
                admission.check(admission.builder)
            admission.arrow(column, dtype)
    if fmt == 'pandas':
        # Object staging, inferred/native column arrays, block consolidation and
        # index/column metadata remain live during construction. Typed conversion
        # additionally retains every Arrow buffer admitted above.
        working = 512 + len(names) * 256 + n * len(names) * 40
        for row in rows:
            for index, name in enumerate(names):
                value = row.get(name)
                working += (admission.boxed(value) if schema is None else
                            admission.typed_boxed(value, schema.field(index).type))
                admission.check(admission.builder + working)
        admission.check(admission.builder + working)
    else:
        admission.check(admission.builder)
    if observe_writer_workspace is not None:
        observe_writer_workspace(admission.writer_overhead)
    return admission.retained, admission.maximum


def canonical_record_json(value: Any) -> str:
    return json.dumps(
        thaw_record_value(value), ensure_ascii=False, sort_keys=True, separators=(",", ":"), allow_nan=False
    )

def record_identity(parts: Any) -> str:
    return hashlib.sha256(canonical_record_json(parts).encode("utf-8")).hexdigest()


def cast_allocation(table, schema):
    """Upper bound on new payload buffers, while the source remains resident.

    Equal types share their buffers. Reserve data and validity allocations at
    Arrow's 64-byte alignment for conversions, including nested arrays. Unknown
    allocation shapes fail before cast, never after an unbounded allocation.
    This is payload admission, not an RSS bound on Arrow's implementation.
    """
    import pyarrow as pa
    from demiflow.execution.contracts.recovery_contract import RecoveryContractError
    def aligned(size):
        return ((size + 63) // 64) * 64
    def bound(array, target, depth=0):
        if array.type == target:
            return 0
        if depth > 16:
            raise RecoveryContractError('Arrow cast type depth exceeds admission budget')
        n = len(array)
        validity = aligned((n + 7) // 8)
        if pa.types.is_null(target):
            return 0
        try:
            bits = target.bit_width
        except (ValueError, AttributeError):
            bits = None
        if bits is not None:
            dictionary = (bound(array.dictionary, target, depth + 1)
                          if pa.types.is_dictionary(array.type) else 0)
            return validity + aligned((n * bits + 7) // 8) + dictionary
        if pa.types.is_struct(target) and pa.types.is_struct(array.type):
            if array.type.names != target.names:
                raise RecoveryContractError('Arrow cast field mapping has no bounded allocation contract')
            return validity + sum(bound(array.field(i), f.type, depth + 1)
                                  for i, f in enumerate(target))
        if (pa.types.is_list(target) or pa.types.is_large_list(target)) and (
                pa.types.is_list(array.type) or pa.types.is_large_list(array.type)):
            width = 8 if pa.types.is_large_list(target) else 4
            return validity + aligned((n + 1) * width) + bound(array.values, target.value_type, depth + 1)
        binary = (pa.types.is_string, pa.types.is_large_string, pa.types.is_binary, pa.types.is_large_binary)
        if any(check(target) for check in binary):
            width = 8 if pa.types.is_large_string(target) or pa.types.is_large_binary(target) else 4
            if any(check(array.type) for check in binary) or pa.types.is_fixed_size_binary(array.type):
                data = array.get_total_buffer_size()
            elif (pa.types.is_integer(array.type) or pa.types.is_floating(array.type)
                  or pa.types.is_boolean(array.type) or pa.types.is_decimal(array.type)):
                data = n * 128  # bounded textual representation of primitive values
            else:
                raise RecoveryContractError('Arrow cast needs a bounded allocation contract for ' + str(target))
            return validity + aligned((n + 1) * width) + aligned(data)
        raise RecoveryContractError('Arrow cast needs a bounded allocation contract for ' + str(target))
    if table.schema.names != schema.names:
        raise RecoveryContractError('Arrow cast schema fields differ')
    return sum(bound(chunk, field.type) for column, field in zip(table.columns, schema)
               for chunk in column.chunks)


def dumps_row(row: object, *, max_bytes: int) -> bytes:
    return _encode_payload(row, max_bytes=max_bytes)

def loads_row(data: bytes) -> object:
    return _decode_payload(data, max_bytes=max(1, len(data)))

def row_bytes(row: object, *, max_bytes: int) -> int:
    return len(dumps_row(row, max_bytes=max_bytes))

def input_row_bytes(record: Record) -> int:
    if record.inline is not None:
        return record.encoded_bytes
    if record.payload_ref is not None:
        if is_arrow_record(record):
            return record.encoded_bytes
        return record.payload_ref.size_bytes
    raise DataflowError("INVALID_CONTRACT", "record carries neither inline nor payload_ref")

def admit_receive(size, bounds, *, resident_bytes=None, retained=0):
    if type(size) is not int or not 0 <= size <= document_limit(bounds):
        _frame_budget_error('peer document exceeds frame byte bound')
    # Bytearray, returned bytes and one bounded recv fragment. The caller
    # separately retains prior decoded rows / outstanding producer buffers.
    _admit(retained + 2 * size + min(size, peer_chunk_bytes(bounds)) + 256,
           resident_limit(bounds, resident_bytes))

def peer_chunk_bytes(bounds):
    return max(1, min(65536, bounds.max_row_bytes // 4))


def encode_record(record: Record, bounds, *, resident_bytes=None, retained=0) -> bytes:
    # Preserve the original no-newline helper contract and exact JSON bytes.
    return encode_frame(record, bounds, resident_bytes=resident_bytes, retained=retained)[:-1]
