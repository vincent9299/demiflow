"""Real closed control codec equivalence and exact pre-encoding limits."""
import json

import pytest

from demiflow.execution.contracts import dataflow_codec as codec
from demiflow.execution.contracts.dataflow_errors import DataflowError
from demiflow.execution.dataflow_hosts import _event_from_dict
from test_runtime_v3_c import make, bounds


@pytest.mark.parametrize('name', sorted(codec._WIRE_TYPES))
def test_every_wire_type_document_and_raw_have_the_original_canonical_bytes(name):
    value = make(name)
    budget = bounds()
    original = codec.encode_wire(value, type_name=name, bounds=budget)
    frame = json.loads(original)
    assert frame == json.loads(original)
    assert codec._measure_document(frame, budget) == len(original)
    for restored in (codec.decode_wire_document(frame, type_name=name, bounds=budget),
                     codec.decode_wire(original, type_name=name, bounds=budget)):
        # The removed post-construction walk is exercised independently on
        # every registered type, including omitted legacy optional fields.
        codec.validate_dataflow_record(restored, type_name=name, bounds=budget)
        assert codec.encode_wire(restored, type_name=name, bounds=budget) == original


@pytest.mark.parametrize('text', ['', 'abc', '中🙂é', '\"\\\b\f\n\r\t',
    ''.join(chr(i) for i in range(32)), '[{\\\"}]', '\u2028\u2029', '\\n\"' * 100])
def test_full_frame_size_counts_utf8_escapes_and_all_envelope_fields(text):
    value = make('WorkerEvent', payload={'text': text, 'numbers': [0, -0.0, 1e30, -9, True, False, None]})
    frame = json.loads(codec.encode_wire(value, type_name='WorkerEvent', bounds=bounds()))
    raw = codec.encode_wire(value, type_name='WorkerEvent', bounds=bounds())
    assert codec._measure_document(frame, bounds()) == len(raw)


def test_exact_escape_bytes_reject_before_any_encoding_or_decoding(monkeypatch):
    value = make('WorkerEvent', payload={'text': '\x01' * 1000})
    frame = json.loads(codec.encode_wire(value, type_name='WorkerEvent', bounds=bounds()))
    raw = codec.encode_wire(value, type_name='WorkerEvent', bounds=bounds())
    narrow = bounds(max_control_bytes=len(raw)-1)
    def forbidden(*args, **kwargs):
        raise AssertionError('document codec must not encode/decode an intermediate raw copy')
    monkeypatch.setattr(codec.json, 'dumps', forbidden)
    monkeypatch.setattr(codec.json, 'loads', forbidden)
    with pytest.raises(DataflowError, match='control byte budget'):
        codec.decode_wire_document(frame, type_name='WorkerEvent', bounds=narrow)
    exact = bounds(max_control_bytes=len(raw))
    restored = _event_from_dict(frame['payload'], bounds=exact)
    assert restored == value and restored.payload['text'] == '\x01' * 1000


@pytest.mark.parametrize('change', [
    lambda f: f.update(extra=0),
    lambda f: f.update(schema='old'),
    lambda f: f.update(schema=True),
    lambda f: f.update(type='Record'),
    lambda f: f['payload'].update(extra=0),
    lambda f: f['payload'].pop('boot_id'),
    lambda f: f['payload'].update(attempt=True),
    lambda f: f['payload'].update(controller_epoch=False),
    lambda f: f['payload'].update(payload_ref=None, payload=None),
    lambda f: f['payload'].update(payload={'x': float('nan')}),
    lambda f: f['payload'].update(payload={'x': float('inf')}),
    lambda f: f['payload'].update(payload={'x': float('-inf')}),
    lambda f: f['payload'].update(payload={1: 'invalid'}),
    lambda f: f['payload'].update(payload={'x': object()}),
    lambda f: f['payload'].update(payload={'x': b'not-json'}),
    lambda f: f['payload'].update(payload={'x': (1, 2)}),
])
def test_document_entry_is_untrusted_closed_and_finite(change):
    frame = json.loads(codec.encode_wire(make('WorkerEvent'), type_name='WorkerEvent', bounds=bounds()))
    change(frame)
    with pytest.raises(DataflowError):
        codec.decode_wire_document(frame, type_name='WorkerEvent', bounds=bounds())


def test_full_frame_container_depth_and_original_payload_node_limit():
    value = make('WorkerEvent', payload={'values': [[]]})
    frame = json.loads(codec.encode_wire(value, type_name='WorkerEvent', bounds=bounds()))
    raw = codec.encode_wire(value, type_name='WorkerEvent', bounds=bounds())
    # Outer frame / event / its payload / list / inner list = depth five.
    for decoder, document in ((codec.decode_wire_document, frame), (codec.decode_wire, raw)):
        with pytest.raises(DataflowError, match='depth budget'):
            decoder(document, type_name='WorkerEvent', bounds=bounds(max_depth=4))
        assert decoder(document, type_name='WorkerEvent', bounds=bounds(max_depth=5)) == value
    frame['payload']['payload'] = {'items': [None] * 65536}
    with pytest.raises(DataflowError, match='node budget'):
        codec.decode_wire_document(frame, type_name='WorkerEvent', bounds=bounds())
    loop = []; loop.append(loop)
    frame['payload']['payload'] = {'items': loop}
    with pytest.raises(DataflowError, match='depth budget'):
        codec.decode_wire_document(frame, type_name='WorkerEvent', bounds=bounds())


def test_raw_path_retains_duplicate_key_and_nonfinite_and_depth_scan():
    original = codec.encode_wire(make('WorkerEvent'), type_name='WorkerEvent', bounds=bounds())
    bad = original.replace(b'"attempt":0', b'"attempt":0,"attempt":0')
    assert bad != original
    with pytest.raises(DataflowError, match='duplicate key'):
        codec.decode_wire(bad, type_name='WorkerEvent', bounds=bounds())
    for value in (b'NaN', b'Infinity', b'-Infinity', b'true'):
        with pytest.raises(DataflowError):
            codec.decode_wire(original.replace(b'"attempt":0', b'"attempt":'+value),
                              type_name='WorkerEvent', bounds=bounds())


def test_exact_large_integer_bytes_and_detached_document():
    event = make('WorkerEvent', payload={'n': 10**1000, 'items': [1, 2]})
    raw = codec.encode_wire(event, type_name='WorkerEvent', bounds=bounds())
    frame = json.loads(codec.encode_wire(event, type_name='WorkerEvent', bounds=bounds()))
    assert codec._measure_document(frame, bounds()) == len(raw)
    with pytest.raises(DataflowError, match='control byte budget'):
        codec.decode_wire_document(frame, type_name='WorkerEvent', bounds=bounds(max_control_bytes=len(raw)-1))
    restored = codec.decode_wire_document(frame, type_name='WorkerEvent', bounds=bounds())
    frame['payload']['payload']['items'].append(3)
    assert tuple(event.payload['items']) == tuple(restored.payload['items']) == (1, 2)


def test_document_path_preserves_original_conservative_payload_admission():
    value = make('WorkerEvent', payload={'items': list(range(100))})
    frame = json.loads(codec.encode_wire(value, type_name='WorkerEvent', bounds=bounds()))
    raw = codec.encode_wire(value, type_name='WorkerEvent', bounds=bounds())
    _, estimate = codec._walk_budget(frame['payload'], bounds().max_depth)
    assert estimate > len(raw)
    narrow = bounds(max_control_bytes=estimate-1)
    for decoder, value in ((codec.decode_wire, raw), (codec.decode_wire_document, frame)):
        with pytest.raises(DataflowError, match='control byte budget'):
            decoder(value, type_name='WorkerEvent', bounds=narrow)
