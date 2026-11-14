"""Q001 independent unit tests: _canonical_row value fidelity (C06, CODEC/BOUNDS).

Pure function; no run/journal. Proves IEEE-754 byte signatures keep signed-zero
and distinct NaN payloads from collapsing container dedup, bool/int/float stay
distinct, dict insertion order and tuple/list/bytes types survive, and the DAG
walk stays bounded (depth/cycle fallback) without calling opaque user code.
"""
import math
import pickle
import struct

from demiflow.execution.block_sink import _canonical_row, _CANON_MAX_DEPTH


def _bits(value):
    return struct.pack('>d', value)


def test_signed_zero_not_merged_in_dict():
    neg = _canonical_row({"v": -0.0})
    pos = _canonical_row({"v": 0.0})
    assert math.copysign(1.0, neg["v"]) == -1.0
    assert math.copysign(1.0, pos["v"]) == 1.0
    assert pickle.dumps(neg, 4) != pickle.dumps(pos, 4)


def test_signed_zero_not_merged_in_list_and_tuple():
    row = _canonical_row(([0.0], [-0.0]))
    assert row[0] is not row[1]
    assert _bits(row[0][0]) != _bits(row[1][0])
    tup = _canonical_row(((0.0,), (-0.0,)))
    assert _bits(tup[0][0]) != _bits(tup[1][0])


def test_distinct_nan_payloads_preserved_and_round_trip():
    n1 = struct.unpack('>d', bytes.fromhex('7ff8000000000001'))[0]
    n2 = struct.unpack('>d', bytes.fromhex('7ff8000000000002'))[0]
    c1, c2 = _canonical_row([n1]), _canonical_row([n2])
    assert pickle.dumps(c1, 4) != pickle.dumps(c2, 4)
    back = pickle.loads(pickle.dumps(c1, 4))[0]
    assert math.isnan(back) and _bits(back) == bytes.fromhex('7ff8000000000001')


def test_bool_int_float_never_merged():
    assert pickle.dumps(_canonical_row([True, 1, 1.0]), 4) == pickle.dumps([True, 1, 1.0], 4)
    row = _canonical_row(([True], [1], [1.0]))
    assert len({pickle.dumps(r, 4) for r in row}) == 3
    assert type(row[0][0]) is bool and type(row[1][0]) is int and type(row[2][0]) is float


def test_complex_signed_zero_preserved():
    row = _canonical_row([complex(-0.0, 0.0), complex(0.0, -0.0)])
    assert pickle.dumps(row[0], 4) != pickle.dumps(row[1], 4)
    assert math.copysign(1.0, row[0].real) == -1.0
    assert math.copysign(1.0, row[1].imag) == -1.0


def test_dict_insertion_order_preserved():
    value = {"z": 1, "a": 2, "m": 3, "b": 4}
    assert list(_canonical_row(value)) == ["z", "a", "m", "b"]


def test_tuple_list_bytes_types_preserved():
    assert type(_canonical_row((1, 2))) is tuple
    assert type(_canonical_row([1, 2])) is list
    assert _canonical_row(b"\x00\xff\x00") == b"\x00\xff\x00"
    assert type(_canonical_row(b"\x00")) is bytes


def test_identical_value_containers_dedup_encoding_preserving():
    row = _canonical_row(([1, 2], [1, 2]))
    assert row[0] is row[1]  # same encoding -> shared canonical object
    # dedup memoizes the shared object; value semantics round-trip unchanged
    assert pickle.loads(pickle.dumps(row, 4)) == ([1, 2], [1, 2])


def test_input_not_mutated_and_rebuilt():
    value = {"a": [1, 2], "b": (3,)}
    canonical = _canonical_row(value)
    assert value == {"a": [1, 2], "b": (3,)}
    assert canonical == value and canonical is not value


def test_depth_overflow_falls_back_to_original():
    deep = current = []
    for _ in range(_CANON_MAX_DEPTH + 5):
        nxt = []
        current.append(nxt)
        current = nxt
    assert _canonical_row(deep) is deep  # bounded DAG: no recursion blowup


def test_cycle_falls_back_to_original():
    cyclic = []
    cyclic.append(cyclic)
    assert _canonical_row(cyclic) is cyclic


def test_opaque_objects_pass_through_without_user_code():
    class Opaque:
        def __eq__(self, other):
            raise AssertionError("opaque user code must not be called")
        __hash__ = None
    a, b = Opaque(), Opaque()
    row = _canonical_row([a, b])
    assert row[0] is a and row[1] is b
    assert type(row[0]) is Opaque


def test_set_type_preserved_out_of_value_canonical_scope():
    value = {1, 2, 3}
    result = _canonical_row(value)
    assert type(result) is set and result == value