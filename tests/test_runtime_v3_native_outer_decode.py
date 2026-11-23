"""Original outer bytes, strict JSON boundaries and admitted native copies."""
import hashlib
import json
import math
from datetime import date
from decimal import Decimal
from pathlib import Path

import pytest

import demiflow
from demiflow.execution.contracts import record_codec as frames
from demiflow.execution.contracts.dataflow import ArtifactRef, Record, thaw_record_value
from demiflow.execution.contracts.dataflow_errors import DataflowError
from demiflow.execution.contracts.record_codec import _encode_payload, _encode_record_document
from demiflow.execution.contracts.record_schema import SCHEMA_MEDIA_TYPE
from demiflow.execution.recovery_contract import document
from test_runtime_v3_c import bounds


@pytest.fixture(autouse=True)
def source_binding():
    root = Path(__file__).resolve().parents[1]
    assert Path(demiflow.__file__).resolve().is_relative_to(root)
    assert Path(frames.__file__).resolve().is_relative_to(root)


def fixture(value, *, finite=False, schema=False):
    row = {'value': value}
    payload = _encode_payload(row, max_bytes=65536)
    ref = ArtifactRef('a' * 64, 123, 'store', 'objects/schema', SCHEMA_MEDIA_TYPE) if schema else None
    record = Record(hashlib.sha256(payload).hexdigest(), {'source': 's', 'offset': 9},
                    None, row, len(payload), 3, schema_ref=ref)
    raw = _encode_record_document(record, max_bytes=65536)
    metadata = {'record_id': record.record_id, 'sequence': record.sequence}
    if finite:
        metadata['target'] = 'p0'
    line = document({'document': raw.decode('utf-8'), **metadata}, 131072) + b'\n'
    return record, raw, metadata, line


@pytest.mark.parametrize('finite', [False, True])
@pytest.mark.parametrize('value', [
    b'\x00\xff' * 4096,
    ('quotes "\\', b'\x00\xff', -0.0, date(2026, 11, 17), Decimal('1.20')),
    '测试🙂' * 300,
    {'nested': ['","record_id":"forged"', '\\"', None, True]},
])
def test_native_decode_preserves_original_typed_document_and_metadata(monkeypatch, finite, value):
    record, raw, metadata, line = fixture(value, finite=finite, schema=True)
    b = bounds(max_row_bytes=32768, max_control_bytes=8192, max_resident_bytes=1024 * 1024)

    class NoPythonScan:
        def search(self, *args):
            pytest.fail('roomy original frame fell back to per-escape Python scanning')
    monkeypatch.setattr(frames, '_OUTER_SPECIAL', NoPythonScan())
    decoded, fields = frames.decode_frame(line, b, finite=finite)
    assert (decoded, fields) == (raw, metadata)
    restored = frames.decode_document(decoded, record_id=fields['record_id'],
                                      sequence=fields['sequence'], bounds=b)
    assert restored.schema_ref == record.schema_ref
    assert restored.source_position == record.source_position
    assert thaw_record_value(restored.inline) == {'value': value}
    if isinstance(value, tuple):
        assert isinstance(restored.inline['value'], tuple)
        assert math.copysign(1, restored.inline['value'][2]) == -1
    assert _encode_record_document(restored, max_bytes=65536) == raw


@pytest.mark.parametrize('resident', [40 * 1024, 80 * 1024])
def test_conservative_native_peak_falls_back_without_rejecting_small_original_grant(resident):
    record, raw, metadata, line = fixture('x' * 7600, finite=True)
    b = bounds(max_row_bytes=8192, max_control_bytes=2048, max_resident_bytes=resident)
    assert frames._decode_native_outer(line, b, resident_bytes=None, retained=0, finite=True) is None
    decoded, fields = frames.decode_frame(line, b, finite=True)
    assert (decoded, fields) == (raw, metadata)
    restored = frames.decode_document(decoded, record_id=record.record_id,
                                      sequence=record.sequence, bounds=b)
    assert thaw_record_value(restored.inline) == {'value': 'x' * 7600}


def test_existing_owners_are_charged_before_native_json_allocation(monkeypatch):
    _, _, _, line = fixture(b'a' * 4096, finite=True)
    b = bounds(max_row_bytes=8192, max_control_bytes=2048, max_resident_bytes=1024 * 1024)
    monkeypatch.setattr(frames.json, 'loads', lambda *args, **kwargs: pytest.fail('parse before admission'))
    assert frames._decode_native_outer(line, b, resident_bytes=None,
        retained=b.max_resident_bytes - len(line), finite=True) is None
    with pytest.raises(DataflowError, match='workspace'):
        frames.decode_frame(line, b, retained=b.max_resident_bytes - len(line), finite=True)


def test_tail_tree_peak_is_admitted_before_json_parsing(monkeypatch):
    # Short, densely nested metadata can exceed its raw text allowance.
    tail = {'record_id': [dict() for _ in range(200)], 'sequence': 0}
    line = b'{"document":"{}",' + json.dumps(tail, separators=(',', ':')).encode()[1:] + b'\n'
    b = bounds(max_row_bytes=8192, max_control_bytes=4096, max_resident_bytes=16000)
    monkeypatch.setattr(frames.json, 'loads', lambda *args, **kwargs: pytest.fail('metadata tree parsed before admission'))
    assert frames._decode_native_outer(line, b, resident_bytes=None, retained=0, finite=False) is None
    with pytest.raises(DataflowError, match='workspace'):
        frames.decode_frame(line, b)


@pytest.mark.parametrize('damage', ['duplicate_document', 'nested_document', 'missing_quote',
                                   'invalid_escape', 'invalid_tail', 'extra_field'])
def test_candidate_delimiter_cannot_accept_invalid_outer_json_or_fields(damage):
    _, _, _, line = fixture('x')
    if damage == 'duplicate_document':
        line = line.replace(b'{"document":', b'{"document":"decoy","document":', 1)
    elif damage == 'nested_document':
        line = b'{"document":{"x":"y"},"record_id":"' + b'a' * 64 + b'","sequence":0}\n'
    elif damage == 'missing_quote':
        line = line.replace(b'","record_id":', b',"record_id":', 1)
    elif damage == 'invalid_escape':
        line = line.replace(b'{"document":"', b'{"document":"\\q', 1)
    elif damage == 'invalid_tail':
        line = line[:-2] + b',}\n'
    else:
        line = line[:-2] + b',"unexpected":1}\n'
    with pytest.raises(DataflowError):
        frames.decode_frame(line, bounds(max_resident_bytes=1024 * 1024))


@pytest.mark.parametrize('spelling', ['escaped_document_key', 'escaped_record_key', 'metadata_order', 'indent'])
def test_other_valid_historical_outer_spellings_keep_original_reader(spelling):
    _, raw, metadata, line = fixture('x', finite=True)
    if spelling == 'escaped_document_key':
        line = line.replace(b'"document":', b'"doc\\u0075ment":', 1)
    elif spelling == 'escaped_record_key':
        line = line.replace(b'"record_id":', b'"record\\u005fid":', 1)
    elif spelling == 'metadata_order':
        line = json.dumps({'document': raw.decode(), 'target': 'p0',
                           'sequence': metadata['sequence'], 'record_id': metadata['record_id']},
                          separators=(',', ':')).encode() + b'\n'
    else:
        line = b' ' + line
    b = bounds(max_resident_bytes=1024 * 1024)
    assert frames._decode_native_outer(line, b, resident_bytes=None, retained=0, finite=True) is None
    assert frames.decode_frame(line, b, finite=True) == (raw, metadata)


def test_inner_canonical_and_provenance_checks_are_still_authoritative():
    record, raw, metadata, _ = fixture(('x', b'a', -0.0), finite=True)
    broken = json.loads(raw)
    broken['sp']['parent_record_ids'] = ['invalid-digest']
    altered = document(broken, 65536)
    line = document({'document': altered.decode(), **metadata}, 131072) + b'\n'
    b = bounds(max_resident_bytes=1024 * 1024)
    decoded, fields = frames.decode_frame(line, b, finite=True)
    with pytest.raises(DataflowError, match='parent_record_id'):
        frames.decode_document(decoded, record_id=record.record_id, sequence=record.sequence, bounds=b)
    broken['sp'].pop('parent_record_ids')
    broken['eb'] += 1
    altered = document(broken, 65536)
    line = document({'document': altered.decode(), **metadata}, 131072) + b'\n'
    decoded, fields = frames.decode_frame(line, b, finite=True)
    with pytest.raises(DataflowError, match='size mismatch'):
        frames.decode_document(decoded, record_id=fields['record_id'], sequence=fields['sequence'], bounds=b)
