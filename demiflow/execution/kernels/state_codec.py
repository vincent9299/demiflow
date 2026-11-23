"""Data-only snapshots for kernel buffers (no pickle of mappingproxy/actors)."""
import json
from dataclasses import asdict
from collections.abc import Mapping

from ..contracts.dataflow import Record, ArtifactRef, AggregateState
from ..contracts.dataflow_errors import DataflowError
from ..exchange.dataflow_bridge import _encode_value, _decode_value, _encode_record_document, _decode_record_document

RECORD_LIST_PREFIX = b'["demiflow.kernel-state.v3",["list",['
RECORD_LIST_SUFFIX = b']]]'


def record_fragment(record, *, max_bytes):
    """One original list element, reusable for byte admission and publication."""
    raw = json.dumps(['record', record.record_id, record.sequence,
        _encode_record_document(record, max_bytes=max_bytes).decode()],
        separators=(',', ':'), ensure_ascii=False, allow_nan=False).encode()
    if len(raw) + len(RECORD_LIST_PREFIX) + len(RECORD_LIST_SUFFIX) > max_bytes:
        raise DataflowError('BUDGET_EXCEEDED', 'kernel image exceeds state byte budget')
    return raw


def record_list_chunks(fragments):
    yield RECORD_LIST_PREFIX
    for index, raw in enumerate(fragments):
        if index:
            yield b','
        yield raw
    yield RECORD_LIST_SUFFIX


def measure(value, *, max_bytes, workspace_bytes):
    """Preflight the existing kernel encoding before constructing a packed tree.

    Count occurrences, including aliased children which JSON will expand again.
    Return retained bytes and the peak workspace charged to this encoding.
    """
    import sys
    from ..dataflow_record_frames import _admit, _json_shape, _typed_shape, _record_metadata
    wire = len(b'["demiflow.kernel-state.v3",]')
    native = 0
    tree = 0

    def peak():
        # JSON may hold encoder chunks plus a four-byte Unicode result.
        # The UTF-8 encoding and later base64 scalar fit this same window.
        return native + tree + 10 * wire + 1024

    def add(size, held, temporary):
        nonlocal wire, native, tree
        wire += size
        native += held
        tree += temporary
        if wire > max_bytes:
            raise DataflowError('BUDGET_EXCEEDED', 'kernel image exceeds state byte budget')
        _admit(peak(), workspace_bytes)

    def visit(item, depth):
        if depth > 64:
            raise DataflowError('BUDGET_EXCEEDED', 'kernel state exceeds bounded nesting depth')
        if isinstance(item, Mapping):
            count = len(item)
            add(len(b'["dict",[]]') + max(0, count - 1) + 3 * count,
                sys.getsizeof(item), 256 + 112 * count)
            for key, child in item.items():
                visit(key, depth + 1)
                visit(child, depth + 1)
        elif isinstance(item, (list, tuple)):
            count = len(item)
            tag = b'["tuple",[]]' if isinstance(item, tuple) else b'["list",[]]'
            add(len(tag) + max(0, count - 1), sys.getsizeof(item), 256 + 16 * count)
            for child in item:
                visit(child, depth + 1)
        elif isinstance(item, Record):
            # An embedded Record carries an escaped document. Admit the old
            # document encoder before computing its exact JSON string length.
            from ..dataflow_record_frames import _string_size
            n, held, temporary = _typed_shape(item.inline, max_bytes)
            metadata, meta_held = _json_shape(_record_metadata(item), max_bytes)
            _admit(native + tree + 10 * wire + held + temporary
                                  + meta_held + 10 * (metadata + n) + 2048, workspace_bytes)
            raw = _encode_record_document(item, max_bytes=max_bytes).decode()
            add(len(b'["record",,,]') + _string_size(item.record_id)
                + len(str(item.sequence)) + _string_size(raw), held + meta_held,
                len(raw) * 4 + 256)
        elif isinstance(item, ArtifactRef):
            n, held = _json_shape(asdict(item), max_bytes)
            add(len(b'["ref",]') + n, held, held + 256)
        elif isinstance(item, AggregateState):
            add(len(b'["aggregate",]'), 64, 128)
            visit(asdict(item), depth + 1)
        else:
            # These are the original encode_value leaves (including arrays,
            # typed sets, binary and temporal scalars).
            n, held, temporary = _typed_shape(item, min(max_bytes, workspace_bytes))
            add(len(b'["value",]') + n, held, temporary + 96)

    visit(value, 0)
    return native, peak()


def dumps(value, *, max_bytes):
    def pack(v):
        if isinstance(v, Record):
            return ['record', v.record_id, v.sequence,
                    _encode_record_document(v, max_bytes=max_bytes).decode()]
        if isinstance(v, ArtifactRef):
            return ['ref', asdict(v)]
        if isinstance(v, AggregateState):
            return ['aggregate', pack(asdict(v))]
        if isinstance(v, Mapping):
            return ['dict', [[pack(k), pack(x)] for k, x in v.items()]]
        if isinstance(v, (tuple, list)):
            return ['tuple' if isinstance(v, tuple) else 'list', [pack(x) for x in v]]
        return ['value', _encode_value(v)]
    raw = json.dumps(['demiflow.kernel-state.v3', pack(value)], separators=(',', ':'),
                     ensure_ascii=False, allow_nan=False).encode()
    if len(raw) > max_bytes:
        raise DataflowError('BUDGET_EXCEEDED', 'kernel image exceeds state byte budget')
    return raw


def loads(raw, *, max_bytes):
    if len(raw) > max_bytes:
        raise DataflowError('BUDGET_EXCEEDED', 'kernel image exceeds state byte budget')
    def unpack(v):
        kind = v[0]
        if kind == 'record':
            return _decode_record_document(v[3].encode(), record_id=v[1], sequence=v[2], max_bytes=max_bytes)
        if kind == 'ref':
            return ArtifactRef(**v[1])
        if kind == 'aggregate':
            return AggregateState(**unpack(v[1]))
        if kind == 'dict':
            return {unpack(k): unpack(x) for k, x in v[1]}
        if kind in ('tuple', 'list'):
            result = [unpack(x) for x in v[1]]
            return tuple(result) if kind == 'tuple' else result
        if kind == 'value':
            return _decode_value(v[1])
        raise DataflowError('CORRUPT_ARTIFACT', 'unknown kernel state value')
    envelope = json.loads(raw)
    if envelope[0] != 'demiflow.kernel-state.v3':
        raise DataflowError('CORRUPT_ARTIFACT', 'unknown kernel state schema')
    return unpack(envelope[1])
