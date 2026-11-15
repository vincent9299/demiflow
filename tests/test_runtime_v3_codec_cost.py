"""Encoding optimization must preserve historical bytes and all data checks."""
import json
from types import MappingProxyType
import pytest

from demiflow.execution.contracts.dataflow import Record, thaw_record_value
from demiflow.execution.contracts.dataflow_errors import DataflowError
from demiflow.execution.exchange.dataflow_bridge import (
    _codec_dumps, _encode_payload, _encode_value, _encode_record_document,
    _decode_record_document, _decode_payload,
)


def historical(value):
    return json.dumps(thaw_record_value(value), sort_keys=True, ensure_ascii=False,
                      separators=(',', ':'), allow_nan=True).encode('utf-8')


@pytest.mark.parametrize('row', [
    {'negative': [-0.0], 'positive': [0.0], 'tuple': (1, None), 'bytes': b'\0\xff'},
    {'text': '中文é', 'nested': {'b': [1, True, {'key': 'value'}]}, 'set': {1, 3}},
    {'large': 2**100, 'nan': float('nan'), 'infinity': float('inf')},
])
def test_transfer_bytes_equal_original_canonical_codec(row):
    maximum = 16384
    payload = _encode_payload(row, max_bytes=maximum)
    assert payload == historical(_encode_value(row))
    record = Record(record_id='a' * 64, sequence=2,
                    source_position={'offset': 2, 'parents': [1, {'x': [2]}]},
                    inline=row, payload_ref=None, encoded_bytes=len(payload))
    doc = {'v': 'demiflow.record.v3', 'sp': record.source_position, 'eb': record.encoded_bytes,
           'p': json.loads(payload), 'r': None}
    from demiflow.execution.exchange.dataflow_bridge import _CODEC_VERSION
    doc['v'] = _CODEC_VERSION
    encoded = _encode_record_document(record, max_bytes=maximum)
    assert encoded == historical(doc)
    recovered = _decode_record_document(encoded, record_id=record.record_id, sequence=2, max_bytes=maximum)
    assert _encode_payload(recovered.inline, max_bytes=maximum) == payload
    assert _codec_dumps(MappingProxyType({'a': [MappingProxyType({'b': (1, 2)})]})) == b'{"a":[{"b":[1,2]}]}'


def test_canonical_and_budget_rejection_still_run():
    with pytest.raises(DataflowError, match='canonical'):
        _decode_payload(b'{"t": "map", "v": []}', max_bytes=1024)
    with pytest.raises(DataflowError, match='BUDGET_EXCEEDED'):
        _encode_payload({'payload': 'x' * 1024}, max_bytes=64)
    row = {'value': 1}
    encoded = _encode_payload(row, max_bytes=1024)
    record = Record(record_id='a' * 64, sequence=0, source_position={},
                    inline=row, payload_ref=None, encoded_bytes=len(encoded) + 1)
    with pytest.raises(DataflowError, match='misreports'):
        _encode_record_document(record, max_bytes=1024)
