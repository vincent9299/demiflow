"""Public data values survive real workers, materialized replay, and bounded codecs."""
from datetime import date, datetime, time, timedelta, timezone, tzinfo
from decimal import Decimal
import json
import math
import os
from zoneinfo import ZoneInfo

import pytest

from demiflow import data
from demiflow.execution.run_context import RunContext
from demiflow.execution.exchange.dataflow_bridge import _encode_payload, _decode_payload
from demiflow.execution.contracts.dataflow import Record, thaw_record_value
from demiflow.execution.contracts.dataflow_errors import DataflowError


def payload():
    return {'day': date(2026, 11, 15), 'stamp': datetime(2026, 11, 15, 12, 34, 56, 789),
        'utc': datetime(2026, 11, 15, tzinfo=timezone.utc),
        'offset': time(12, 30, 1, 40, tzinfo=timezone(timedelta(hours=5, minutes=30), 'business')),
        'fold': datetime(2026, 11, 1, 1, 30, tzinfo=ZoneInfo('America/New_York'), fold=1),
        'duration': timedelta(days=-3, seconds=2, microseconds=1), 'price': Decimal('-0.0012300'),
        'data': (b'\x00\xff', {'x', 'y'}, frozenset({(1, 'a'), (2, 'b')})),
        'negative': [-0.0], 'positive': [0.0]}


def assert_exact(actual, expected):
    assert actual == expected
    for name, value in expected.items():
        assert type(actual[name]) is type(value)
    assert actual['price'].as_tuple() == expected['price'].as_tuple()
    assert actual['fold'].fold == 1 and actual['fold'].tzinfo.key == 'America/New_York'
    assert actual['offset'].tzname() == 'business'
    assert math.copysign(1, actual['negative'][0]) == -1
    assert math.copysign(1, actual['positive'][0]) == 1
    assert type(actual['data'][1]) is set and type(actual['data'][2]) is frozenset


def test_data_scalar_codec_preserves_type_and_freezes_sets():
    values = payload()
    encoded = _encode_payload(values, max_bytes=8192)
    actual = _decode_payload(encoded, max_bytes=8192)
    assert_exact(actual, values)
    record = Record('a' * 64, {}, None, values, len(encoded), 0)
    values['data'][1].add('later')
    assert 'later' not in record.inline['data'][1]
    business = thaw_record_value(record.inline)
    business['data'][1].add('consumer')
    assert 'consumer' not in record.inline['data'][1]
    assert type(business['data'][1]) is set
    assert _encode_payload(thaw_record_value(record.inline), max_bytes=8192) == encoded


@pytest.mark.parametrize('source', ['items', 'lance'])
def test_public_scalar_materialization_and_committed_replay(tmp_path, source):
    expected = payload()
    if source == 'lance':
        import lance
        import pyarrow as pa
        expected = {k: expected[k] for k in ('day', 'stamp', 'price')}
        schema = pa.schema([('day', pa.date32()), ('stamp', pa.timestamp('us')),
            ('price', pa.decimal128(20, 7))])
        lance.write_dataset(pa.Table.from_pylist([expected], schema=schema), str(tmp_path / 'source.lance'))
    calls = tmp_path / 'calls'
    def observe(row):
        with calls.open('a') as stream:
            stream.write(str(os.getpid()) + '\n')
        return row
    def dataset():
        rows = data.from_items([expected]) if source == 'items' else data.read_lance(str(tmp_path / 'source.lance'), version=1)
        return rows.map(observe)
    class Interrupted(Exception):
        pass
    with pytest.raises(Interrupted):
        with RunContext.start(tmp_path / 'run') as run, data.local_execution():
            reference = run.ref
            materialized = dataset().materialize()
            try:
                first = materialized.take_all()
            finally:
                materialized.release()
            commits = run.journal.execution_commits('action-1')
            assert commits and commits[-1]['status'] == 'acked'
            raise Interrupted()
    with RunContext.resume(reference) as run, data.local_execution():
        materialized = dataset().materialize()
        try:
            restored = materialized.take_all()
        finally:
            materialized.release()
        assert run.attempt == 2
        assert run.journal.execution_commits('action-1') == commits
    assert len(first) == len(restored) == 1
    if source == 'items':
        assert_exact(first[0], expected)
        assert_exact(restored[0], expected)
    else:
        assert first == restored == [expected]
        assert type(restored[0]['day']) is date and type(restored[0]['stamp']) is datetime
        assert restored[0]['price'].as_tuple() == expected['price'].as_tuple()
    assert len(calls.read_text().splitlines()) == 1
    assert int(calls.read_text().strip()) != os.getpid()


@pytest.mark.parametrize('value', [date.min, date.max, time(1, 2, 3, 4, fold=1),
    timedelta.max, timedelta.min, Decimal('-0'), Decimal('1.2300E+100'),
    Decimal('-NaN42'), Decimal('sNaN12'), Decimal('Infinity'), Decimal('-Infinity')])
def test_scalar_boundary_roundtrip(value):
    restored = _decode_payload(_encode_payload({'v': value}, max_bytes=4096), max_bytes=4096)['v']
    assert type(restored) is type(value)
    if type(value) is Decimal:
        assert restored.as_tuple() == value.as_tuple()
    else:
        assert restored == value
        if type(value) is time:
            assert restored.fold == value.fold


def test_new_scalar_domains_are_bounded_and_untrusted_objects_are_refused():
    class BusinessTimezone(tzinfo):
        def utcoffset(self, value):
            raise AssertionError('custom timezone code must never execute')
        def __reduce__(self):
            raise AssertionError('custom timezone must never be pickled')
    class BusinessObject:
        def __reduce__(self):
            raise AssertionError('arbitrary objects must never be copied')
    for value in (BusinessObject(), datetime(2026, 1, 1, tzinfo=BusinessTimezone())):
        with pytest.raises(DataflowError, match='INVALID_CONTRACT'):
            _encode_payload({'v': value}, max_bytes=4096)
        with pytest.raises(DataflowError, match='INVALID_CONTRACT'):
            data.from_items([{'v': value}])
    for value in (Decimal('9' * 1000), {'x' * 70, 'y' * 70}):
        with pytest.raises(DataflowError, match='BUDGET_EXCEEDED'):
            _encode_payload({'v': value}, max_bytes=128)


def test_set_encoding_is_order_independent_and_preserves_nan_members():
    first = {'v': set(['b', (1, 'x'), b'a', frozenset({'q', 'z'})])}
    second = {'v': set(reversed(list(first['v'])))}
    assert _encode_payload(first, max_bytes=4096) == _encode_payload(second, max_bytes=4096)
    nan_set = {float('nan'), float('nan')}
    restored = _decode_payload(_encode_payload({'v': nan_set}, max_bytes=4096), max_bytes=4096)['v']
    assert len(restored) == 2 and all(math.isnan(v) for v in restored)


@pytest.mark.parametrize('node', [
    {'t': 'date', 'v': [2026, 2, 31]}, {'t': 'date', 'v': [True, 1, 1]},
    {'t': 'decimal', 'v': '01.0'}, {'t': 'decimal', 'v': 'not-a-decimal'},
    {'t': 'timedelta', 'v': [0, 86400, 0]}, {'t': 'date', 'v': [2026, 1, 1], 'class': 'arbitrary'},
    {'t': 'set', 'v': [{'t': 'list', 'v': []}]},
    {'t': 'set', 'v': [{'t': 'int', 'v': 1}, {'t': 'bool', 'v': True}]},
    {'t': []},
])
def test_corrupt_data_only_nodes_are_rejected(node):
    from demiflow.execution.exchange.dataflow_bridge import _decode_value
    with pytest.raises(DataflowError, match='CORRUPT_ARTIFACT'):
        _decode_value(node)


def test_named_zone_offset_and_payload_depth_cannot_silently_change():
    from demiflow.execution.exchange.dataflow_bridge import _encode_value, _decode_value
    node = _encode_value(payload()['fold'])
    node['offset_us'] += 3600 * 1000000
    with pytest.raises(DataflowError, match='CORRUPT_ARTIFACT'):
        _decode_value(node)
    for _ in range(40):
        node = {'t': 'tuple', 'v': [node]}
    with pytest.raises(DataflowError, match='depth/node'):
        _decode_value(node)


def test_existing_primitive_payload_and_semantic_identity_bytes_stay_frozen():
    from demiflow.execution.recovery_identity import describe
    from demiflow.execution.recovery_contract import document
    raw = _encode_payload({'legacy': [None, True, 1, -0.0, 'é'], 'tuple': (b'\x00', 2)}, max_bytes=4096)
    assert raw == ('{"t":"map","v":[["legacy",{"t":"list","v":[{"t":"null"},'
        '{"t":"bool","v":true},{"t":"int","v":1},{"t":"float","v":-0.0},'
        '{"t":"str","v":"é"}]}],["tuple",{"t":"tuple","v":[{"t":"bytes",'
        '"v":"AA=="},{"t":"int","v":2}]}]]}').encode()
    expected = '{"frozen":{"collection_type":"frozenset","items":2,"sha256":"4295f72eeb1e3507b8461e240e3b8d18c1e7bd2f1122b11fc9ec40a65894031a"},"raw":{"bytes":1,"bytes_sha256":"6e340b9cffb37a989ca544e6bb780a2c78901d3fb33738768511a30617afa01d"},"set":{"collection_type":"set","items":2,"sha256":"e09614bbb9b54447d2a35b392abfac2983613186fcda65cdff52897548788c59"},"tuple":[1,null]}'
    assert document(describe({'set': {'b', 'a'}, 'frozen': frozenset({1, 2}),
        'tuple': (1, None), 'raw': b'\x00'})) == expected.encode()



def test_thaw_preserves_strict_scalar_identity_subclasses_and_row_markers():
    class BusinessInt(int):
        pass
    class BusinessText(str):
        pass
    values = [None, False, True, 2**100, -0.0, 0.0, float('nan'), float('inf'),
              '\x00中文', b'\x00\xff', BusinessInt(7), BusinessText('x')]
    for value in values:
        assert thaw_record_value(value) is value
    negative, positive = values[4:6]
    record = Record('f' * 64, {}, None,
        {'items': [negative, positive, b'\x00\xff'], 'pair': (1, None), 'set': {2}, 'frozen': frozenset({3})},
        128, 0)
    first = thaw_record_value(record.inline)
    second = thaw_record_value(record.inline)
    assert type(first['items']) is list and type(first['pair']) is tuple
    assert type(first['set']) is set and type(first['frozen']) is frozenset
    assert math.copysign(1, first['items'][0]) == -1
    assert math.copysign(1, first['items'][1]) == 1
    assert first['items'][2] == b'\x00\xff'
    first['items'].append('consumer')
    first['set'].add(4)
    assert len(second['items']) == 3 and second['set'] == {2}
