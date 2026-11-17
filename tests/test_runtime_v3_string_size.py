"""Length preflight must retain exact carrier bytes without allocating them."""
import json
import tracemalloc

import pytest

from demiflow.execution import dataflow_record_frames as frames
from demiflow.execution.contracts.dataflow_errors import DataflowError


def test_all_ascii_and_escape_combinations_match_original_json_bytes():
    values = ['', ''.join(map(chr, range(128))),
              'record_id:0191abcf0123456789', '\x7f' * 1024,
              'ordinary' * 1024, '"\\\b\f\n\r\t' * 1024,
              ''.join(map(chr, range(32))) * 128]
    values.extend(chr(n) for n in range(128))
    values.extend('left' + chr(n) + 'middle' + chr(m) + 'right'
                  for n in range(32) for m in (8, 9, 10, 12, 13, 34, 92, 127))
    for value in values:
        encoded = json.dumps(value, ensure_ascii=False, separators=(',', ':')).encode('utf-8')
        assert frames._string_size(value) == len(encoded)
        assert frames._json_shape(value, len(encoded))[0] == len(encoded)
        with pytest.raises(DataflowError, match='control byte budget'):
            frames._json_shape(value, len(encoded) - 1)


@pytest.mark.parametrize('value', [
    '\x7f\x80\u07ff\u0800\ud7ff\ue000\uffff\U00010000\U0010ffff',
    '汉字🙂', 'é\t汉\n🙂"\\\x00\x1f', '\u2028\u2029',
    'x' * 2048 + '🙂' + '\r\t"\\' * 1024,
])
def test_unicode_and_escape_boundaries_keep_exact_utf8_size(value):
    encoded = json.dumps(value, ensure_ascii=False, separators=(',', ':')).encode('utf-8')
    assert frames._string_size(value) == len(encoded)
    assert frames._json_shape(value, len(encoded))[0] == len(encoded)
    with pytest.raises(DataflowError, match='control byte budget'):
        frames._json_shape(value, len(encoded) - 1)


@pytest.mark.parametrize('value', ['x' * (1024 * 1024), '"\\\n\x00' * (256 * 1024)],
                         ids=['plain-ascii', 'escaped-ascii'])
def test_preflight_has_constant_workspace_without_encoding_or_copying(value, monkeypatch):
    expected = len(json.dumps(value, ensure_ascii=False).encode())
    def forbidden(*args, **kwargs):
        raise AssertionError('length preflight must precede JSON allocation')
    with monkeypatch.context() as patch:
        patch.setattr(json, 'dumps', forbidden)
        tracemalloc.start()
        try:
            actual = frames._string_size(value)
            _, peak = tracemalloc.get_traced_memory()
        finally:
            tracemalloc.stop()
    assert actual == expected
    # The input exists before measurement; neither its encoded bytes nor a
    # list of escape matches may be materialized to decide admission.
    assert peak < 32768


def test_preflight_does_not_make_unpaired_surrogates_valid_json_utf8():
    # Retain historical non-ASCII estimator behavior; the unchanged encoder is
    # still responsible for rejecting invalid UTF-8 scalar sequences.
    for value in ('\ud800', '\udfff', 'prefix\ud800suffix'):
        assert frames._string_size(value) == len(value) + 4
        with pytest.raises(UnicodeEncodeError):
            json.dumps(value, ensure_ascii=False).encode('utf-8')
