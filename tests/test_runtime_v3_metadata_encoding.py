"""Metadata acceleration preserves historical bytes and preallocation limits."""
import hashlib
import json
import random

import pytest

from demiflow.execution.contracts.recovery_contract import document, identity, RecoveryContractError


def original_bytes(value):
    encoder = json.JSONEncoder(sort_keys=True, ensure_ascii=False, allow_nan=False,
                               separators=(',', ':'))
    return b''.join(part.encode('utf-8') for part in encoder.iterencode(value))


@pytest.mark.parametrize('value', [None, True, False, -0.0, -1.7976931348623157e308,
    5e-324, -(10**1000), '', '\x01' * 1000, '中🙂é\"\\\n' * 100,
    {'z': (False, None, -0.0), 'a': [{'value': 'hello'}]},
    {'中\"': ''.join(chr(i) for i in range(32))}, list(range(300))])
def test_fast_and_incremental_paths_have_identical_bytes_and_exact_limit(value):
    raw = original_bytes(value)
    assert document(value) == raw
    assert identity(value) == hashlib.sha256(raw).hexdigest()
    assert document(value, len(raw)) == raw
    with pytest.raises(RecoveryContractError, match='byte budget'):
        document(value, len(raw)-1)


def test_deterministic_nested_corpus_preserves_original_identity_bytes():
    rng = random.Random(48291)
    leaves = [None, True, False, -0.0, 0, -77, 1e-200, 1e200,
              '', 'abc', '中🙂', '\x00\"\\\n', 10**300]
    def value(depth):
        choice = rng.randrange(4) if depth else 0
        if choice == 0:
            return rng.choice(leaves)
        items = [value(depth-1) for _ in range(rng.randrange(5))]
        if choice == 1:
            return {str(i) + '中\"': item for i, item in enumerate(items)}
        return tuple(items) if choice == 2 else items
    for _ in range(512):
        item = value(4)
        assert document(item) == original_bytes(item)


@pytest.mark.parametrize('value,maximum', [
    ({'bad': float('nan')}, 1024), ({'bad': float('inf')}, 1024),
    ({1: 'bad'}, 1024), ({'bad': object()}, 1024),
    ([None] * 16385, 1024*1024), ('x' * 1025, 1024),
])
def test_original_structural_and_indivisible_limits_run_before_fast_encoding(monkeypatch, value, maximum):
    def forbidden(*args, **kwargs):
        raise AssertionError('invalid metadata reached the allocating encoder')
    monkeypatch.setattr(json, 'dumps', forbidden)
    with pytest.raises(RecoveryContractError):
        document(value, maximum)


def test_cycles_and_node_limit_are_still_bounded_before_encoding():
    cycle = []; cycle.append(cycle)
    with pytest.raises(RecoveryContractError, match='structural budget'):
        document(cycle)
    with pytest.raises(RecoveryContractError, match='structural budget'):
        document([[None] * 16384 for _ in range(5)])


def test_unpaired_surrogate_keeps_original_encoding_failure():
    for encode in (original_bytes, document):
        with pytest.raises(UnicodeEncodeError):
            encode({'value': '\ud800'})
