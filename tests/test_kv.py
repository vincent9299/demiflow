import sqlite3
import pytest
from demiflow import data
from demiflow.kv import SQLiteKV


def test_lookup_and_sink_are_explicit_and_preserve_rows(tmp_path):
    store = SQLiteKV(tmp_path/'index.sqlite', 'documents', missing='empty')
    assert store.get('absent') == {'hit': False, 'value': None}
    assert not tmp_path.joinpath('index.sqlite').exists()
    sink = store.datasink()
    data.from_items([{'key':'u', 'value':{'ref':'object'}}]).write_datasink(sink)
    assert sink.receipt.written_rows == 1
    result = data.from_items([{'key':'u','concept':'a'}, {'key':'missing','concept':'b'}]).lookup(
        store, key='key', output='found').materialize().take(3)
    assert result[0]['found'] == {'hit': True, 'value': {'ref':'object'}}
    assert result[1]['found'] == {'hit': False, 'value': None}
    assert [r['concept'] for r in result] == ['a', 'b']


def test_errors_are_not_misses_and_capacity_failure_rolls_back(tmp_path):
    store = SQLiteKV(tmp_path/'index.sqlite', 'test', max_value_bytes=10000, max_bytes=10000, max_entries=1)
    with pytest.raises(sqlite3.OperationalError):
        store.get('a')
    store.put_many([('a', 1)])
    with pytest.raises(ValueError, match='capacity'):
        store.put_many([('a', 2), ('b', 3)])
    assert store.get('a')['value'] == 1
    assert not store.get('b')['hit']
    reader = SQLiteKV(store.path, 'test', max_value_bytes=1)
    store.put_many([('a', 'too wide')])
    with pytest.raises(ValueError, match='max_value_bytes'):
        reader.get('a')


def test_sink_batch_limit_rejects_wide_values(tmp_path):
    store = SQLiteKV(tmp_path/'index.sqlite', 'test')
    with pytest.raises(ValueError, match='batch_bytes'):
        data.from_items([{'key':'a','value':'x'*1024}]).write_datasink(store.datasink(batch_bytes=32))
    assert not tmp_path.joinpath('index.sqlite').exists()
