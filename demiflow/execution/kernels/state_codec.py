"""Data-only snapshots for kernel buffers (no pickle of mappingproxy/actors)."""
import json
from dataclasses import asdict
from collections.abc import Mapping

from ..contracts.dataflow import Record, ArtifactRef, AggregateState
from ..contracts.dataflow_errors import DataflowError
from ..exchange.dataflow_bridge import _encode_value, _decode_value, _encode_record_document, _decode_record_document


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
