"""Collector decode admission must hold before any Python JSON expansion."""
import json
from types import SimpleNamespace

import pytest

from demiflow.execution.dataflow_gc import _Trace, UnprovenGC


def trace(*, memory=1024 * 1024, depth=64):
    value = object.__new__(_Trace)
    value.bounds = SimpleNamespace(max_resident_bytes=memory, max_depth=depth)
    calls = []
    value.check = lambda: calls.append(True)
    return value, calls


@pytest.mark.parametrize('value', [
    {'digest': 'a' * 64, 'size_bytes': 12, 'key': 'a' * 64},
    {'rows': [None, True, -0.0, {'text': 'escaped \\" : [ { 中文'}]},
    ['state', ['record', 'id', 1, '{"nested":{"a":[1,2]}}']],
])
def test_small_original_carriers_decode_without_changing_values_or_scan_steps(value):
    reader, calls = trace()
    raw = json.dumps(value, ensure_ascii=False).encode()
    assert reader.decode(raw) == value
    assert calls == [True]


def test_punctuation_inside_a_string_uses_original_scan_when_bound_is_conservative():
    reader, calls = trace(memory=1024 * 1024)
    value = {'text': '[{,:' * 2048}
    assert reader.decode(json.dumps(value).encode()) == value
    assert calls == [True]


@pytest.mark.parametrize('raw,memory,depth', [
    (b'[0,0,0,0,0]', 4096, 64),  # raw bytes fit, container expansion does not
    (b'[' * 9 + b'0' + b']' * 9, 1024 * 1024, 8),
    (b'"' + b'a' * 1024 + b'"', 4096, 64),
])
def test_excess_workspace_or_depth_is_rejected_before_json_allocation(monkeypatch, raw, memory, depth):
    reader, _ = trace(memory=memory, depth=depth)
    def forbidden(*args, **kwargs):
        pytest.fail('collector decoded before proving its original memory/depth budget')
    monkeypatch.setattr(json, 'loads', forbidden)
    with pytest.raises(UnprovenGC):
        reader.decode(raw)


def test_large_carrier_retains_original_periodic_scan_steps():
    reader, calls = trace(memory=4 * 1024 * 1024)
    raw = b'"' + b'a' * 65536 + b'"'
    assert reader.decode(raw) == 'a' * 65536
    assert calls == [True, True]


def test_short_carrier_cannot_skip_original_scan_deadline():
    reader, _ = trace()
    def expired():
        raise UnprovenGC('original deadline')
    reader.check = expired
    with pytest.raises(UnprovenGC, match='original deadline'):
        reader.decode(b'{"ref":1}')


@pytest.mark.parametrize('raw', [b'{"ref":}', b'[{]', b'{"a":"unfinished'])
def test_invalid_short_json_remains_invalid(raw):
    reader, _ = trace()
    with pytest.raises(ValueError):
        reader.decode(raw)
