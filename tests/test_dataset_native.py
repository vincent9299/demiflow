"""Public Dataset contracts against independent Python answers and real native plans."""
import json
from pathlib import Path

import lance
import pyarrow as pa
import pytest

from demiflow import data
from demiflow.data.api import DataAPI
from demiflow.execution.executors.local import LocalDatasetExecutor
from tests.test_local_relational import _oracle_join, assert_same_rows


def api(tmp_path):
    return DataAPI(LocalDatasetExecutor(resource_root=tmp_path))


def read(api, path, values):
    lance.write_dataset(pa.Table.from_pylist(values), str(path))
    return api.read_lance(str(path), version=1)


def native_reports(dataset):
    return [s for s in dataset.execution_metadata().diagnostics['stages'] if s['name'] == 'datafusion']


def test_engine_session_is_not_a_public_dataset_api():
    assert not hasattr(data, 'datafusion_execution')
    assert not hasattr(data, 'DataFusionOptions')


@pytest.mark.parametrize('how', ['inner', 'left', 'semi', 'anti'])
def test_typed_chain_preserves_duplicates_null_missing_order_and_nested_values(tmp_path, how):
    source = api(tmp_path)
    left = [{'k':'Q2','v':{'flag':None}}, {'k':'Q1','v':None},
            {'k':'Q2','v':{'flag':True}}, {'k':None,'v':{'flag':False}}, {'k':'Q10','v':None}]
    right = [{'k':'Q2','items':[None, {'text':'a'}]}, {'k':'Q2','items':[]}, {'k':None,'items':None}]
    more = [{'k':'Q2','tag':'x'}, {'k':'Q10','tag':'y'}]
    a = read(source, tmp_path/'a', left)
    b = read(source, tmp_path/'b', right)
    c = read(source, tmp_path/'c', more)
    expected = _oracle_join(_oracle_join(left, right, ['k'], ['k'], how), more, ['k'], ['k'], 'left')
    rows = a.join(b, on='k', how=how).join(c, on='k', how='left')
    assert_same_rows(rows.take_all(), expected)
    reports = native_reports(rows)
    assert len(reports) == 1
    assert 'Join' in (Path(reports[0]['query'])/'plan.txt').read_text()
    assert not any(s['name']=='python_boundary' for s in rows.execution_metadata().diagnostics['stages'])


def test_self_join_and_pinned_versions(tmp_path):
    source = api(tmp_path)
    values = [{'k':2,'v':'a'}, {'k':1,'v':'b'}, {'k':2,'v':'c'}]
    a = read(source, tmp_path/'a', values)
    lance.write_dataset(pa.table({'k':[99],'v':['new']}), str(tmp_path/'a'), mode='overwrite')
    result = a.join(a, on='k')
    assert_same_rows(result.take_all(), _oracle_join(values, values, ['k'], ['k'], 'inner'))


def test_compacted_and_deleted_lance_rows_keep_scanner_order(tmp_path):
    path = tmp_path/'fragmented'
    ds = lance.write_dataset(pa.table({'k':[1]*10,'seq':list(range(10))}),str(path))
    ds = lance.write_dataset(pa.table({'k':[1]*10,'seq':list(range(10,20))}),str(path),mode='append')
    ds = lance.write_dataset(pa.table({'k':[1]*100,'seq':list(range(20,120))}),str(path),mode='append')
    ds.optimize.compact_files(target_rows_per_fragment=100)
    ds.delete('seq = 23 OR seq = 5')
    expected = ds.to_table().to_pylist()
    source = api(tmp_path)
    right = read(source, tmp_path/'right', [{'k':1,'tag':'a'}, {'k':1,'tag':'b'}])
    rows = source.read_lance(str(path),version=ds.version).join(right,on='k')
    assert_same_rows(rows.take_all(), _oracle_join(expected,[{'k':1,'tag':'a'},{'k':1,'tag':'b'}],['k'],['k'],'inner'))


def test_unsupported_typed_key_preserves_original_codec_error(tmp_path):
    from datetime import date
    source = api(tmp_path)
    a = read(source, tmp_path/'a', [{'a':date(2026,1,1)}])
    b = read(source, tmp_path/'b', [{'b':date(2026,1,1)}])
    with pytest.raises(TypeError, match='JSON serializable'):
        a.join(b, on='a', right_on='b').take_all()
    with pytest.raises(TypeError, match='JSON serializable'):
        a.reduce_by_key('a', lambda _, row: row).take_all()


def test_thread_callbacks_keep_requested_process_context(tmp_path):
    import os
    path = tmp_path/'source'
    lance.write_dataset(pa.table({'k':[1,2]}), str(path))
    def observe(row):
        return {**row, 'pid':os.getpid()}
    with data.local_execution(workers=2, worker_mode='thread', temp_directory=str(tmp_path)):
        a = data.read_lance(str(path), version=1)
        a.join(a, on='k').map(observe).write_lance(str(tmp_path/'out'),
            mode='create', schema=pa.schema([('k',pa.int64()), ('pid',pa.int64())]))
    assert_same_rows(lance.dataset(str(tmp_path/'out')).to_table().to_pylist(), [
        {'k':1, 'pid':os.getpid()}, {'k':2, 'pid':os.getpid()}])


def test_plain_join_does_not_add_global_order_or_replay_callbacks(tmp_path,monkeypatch):
    import demiflow.execution.dataset_native as native
    monkeypatch.setattr(native,'SORT_STAGE_ROWS',1)
    source=api(tmp_path)
    a=read(source,tmp_path/'a',[{'k':2,'v':'a'},{'k':1,'v':'b'},{'k':2,'v':'c'}])
    b=read(source,tmp_path/'b',[{'k':2,'r':1},{'k':2,'r':2}])
    calls=[]
    def observe(row):
        calls.append((row['k'],row.get('r')))
        return row
    result=a.join(b,on='k',how='left').map(observe)
    expected=[{'k':1,'v':'b'},{'k':2,'v':'a','r':1},{'k':2,'v':'a','r':2},
              {'k':2,'v':'c','r':1},{'k':2,'v':'c','r':2}]
    actual = result.take_all()
    assert_same_rows(actual, expected)
    assert calls==[(r['k'],r.get('r')) for r in actual]
    reports = native_reports(result)
    assert [r['purpose'] for r in reports]==['combined']
    assert all('ORDER BY' not in (Path(r['query'])/'query.sql').read_text() for r in reports)


def test_missing_projection_and_collision_only_on_matched_rows(tmp_path):
    source = api(tmp_path)
    a = read(source, tmp_path/'a', [{'k':1,'v':'a','v_right':'occupied'}])
    b = read(source, tmp_path/'b', [{'k':2,'v':'b'}])
    assert a.join(b,on='k',how='left').take_all()==[{'k':1,'v':'a','v_right':'occupied'}]
    with pytest.raises(KeyError):
        a.join(b.rename_columns({'v':'absent'}),on='k',how='left').select_columns(['absent']).take_all()
    with pytest.raises(ValueError,match='collision'):
        a.join(a.select_columns(['k','v']),on='k').take_all()
    missing=a.join(b.rename_columns({'v':'next_key'}),on='k',how='left')
    c=read(source,tmp_path/'c',[{'next_key':'b'}])
    with pytest.raises(KeyError,match='next_key'):
        missing.join(c,on='next_key').take_all()


def test_large_join_chain_checkpoint_preserves_multiplicity_and_field_presence(tmp_path, monkeypatch):
    import demiflow.execution.dataset_native as native
    monkeypatch.setattr(native, 'SORT_STAGE_ROWS', 1)
    source = api(tmp_path)
    values = [{'k':2, 'v':'a'}, {'k':1, 'v':'b'}, {'k':2, 'v':'c'}, {'k':None, 'v':None}]
    rows = read(source, tmp_path/'base', values)
    expected = values
    for i in range(5):
        right = [{'k':2, f'r{i}':{'flag':None}}, {'k':2, f'r{i}':{'flag':True}}]
        rows = rows.join(read(source, tmp_path/f'right{i}', right), on='k', how='left')
        expected = _oracle_join(expected, right, ['k'], ['k'], 'left')
    calls = []
    def observe(row):
        calls.append(row['v'])
        return row
    rows = rows.map(observe)
    actual = rows.take_all()
    assert_same_rows(actual, expected)
    assert calls == [r['v'] for r in actual]
    assert [r['purpose'] for r in native_reports(rows)] == [
        'relation_checkpoint', 'relation_checkpoint', 'combined']


def test_udf_boundary_runs_once_and_resorts_changed_keys(tmp_path):
    source = api(tmp_path)
    calls=[]
    def change(row):
        calls.append(row['k'])
        return {'k':3-row['k'],'v':row['v']}
    a = read(source,tmp_path/'a',[{'k':1,'v':'a'},{'k':2,'v':'b'}])
    b = read(source,tmp_path/'b',[{'k':1},{'k':2}])
    out = a.join(b,on='k').map(change).reduce_by_key('k',lambda s,r:r)
    assert out.take_all()==[{'k':1,'v':'b'},{'k':2,'v':'a'}]
    assert sorted(calls)==[1,2]
    assert len(native_reports(out))==2


def test_failed_native_query_does_not_rerun_python_or_commit(tmp_path, monkeypatch):
    from demiflow.execution.datafusion import DataFusionSession
    calls=[]
    source=api(tmp_path)
    def records():
        calls.append('scan')
        yield {'k':1}
    def fail(*args,**kwargs):
        raise RuntimeError('native failure')
    monkeypatch.setattr(DataFusionSession,'query',fail)
    target=tmp_path/'target'
    lance.write_dataset(pa.table({'k':[99]}),str(target))
    with pytest.raises(RuntimeError,match='native failure'):
        source.from_iter(records).join(source.from_items([{'k':1}]),on='k').write_lance(
            str(target),mode='overwrite',schema=pa.schema([('k',pa.int64())]))
    assert calls==['scan']
    assert lance.dataset(str(target)).version==1


def test_typed_writer_avoids_row_decoding_and_preserves_nested_nulls(tmp_path, monkeypatch):
    from demiflow.execution.dataset_native import Engine
    source=api(tmp_path)
    a=read(source,tmp_path/'a',[{'k':1,'v':{'flag':True}}, {'k':2,'v':None}])
    b=read(source,tmp_path/'b',[{'k':1,'r':[None,{'n':3}]}])
    def forbidden(*args):
        raise AssertionError('typed writer decoded Python rows')
    monkeypatch.setattr(Engine,'decode',forbidden)
    result=a.join(b,on='k',how='left')
    schema=pa.schema([('k',pa.int64()),('v',pa.struct([('flag',pa.bool_())])),('r',pa.list_(pa.struct([('n',pa.int64())])))])
    result.write_lance(str(tmp_path/'out'),mode='create',schema=schema)
    assert_same_rows(lance.dataset(str(tmp_path/'out')).to_table().to_pylist(), [
        {'k':1,'v':{'flag':True},'r':[None,{'n':3}]},{'k':2,'v':None,'r':None}])


def test_empty_inputs_and_opaque_python_types_keep_original_semantics(tmp_path):
    from datetime import datetime
    source=api(tmp_path)
    assert source.from_items([]).join(source.from_items([]),on='unknown').take_all()==[]
    a=[{'k':v,'payload':(datetime(2026,1,1),b'\x00',{'x'})} for v in [1,1.0,True,None,{'a':2},['nested']]]
    b=[{'k':v,'r':'matched'} for v in [1,True,None,{'a':2},['nested']]]
    assert_same_rows(source.from_items(a).join(source.from_items(b),on='k',how='left').take_all(), _oracle_join(a,b,['k'],['k'],'left'))


def test_partition_callbacks_preserve_task_state_and_failure_before_commit(tmp_path):
    class Number:
        def __init__(self):
            self.i=0
        def __call__(self,row):
            self.i+=1
            return {**row,'within_task':self.i}
    left=tmp_path/'a';right=tmp_path/'b';target=tmp_path/'out'
    lance.write_dataset(pa.table({'k':list(range(7))}),str(left))
    lance.write_dataset(pa.table({'k':list(range(7))}),str(right))
    schema=pa.schema([('k',pa.int64()),('within_task',pa.int64())])
    with data.local_execution(workers=2,worker_mode='process',batch_rows=3) as session:
        rows=data.read_lance(str(left),version=1).join(data.read_lance(str(right),version=1),on='k').map(Number)
        rows.write_lance(str(target),mode='create',schema=schema)
        assert any(s['name']=='python_batch_callbacks' for s in session.stats['stages'])
        assert lance.dataset(str(target)).to_table()['within_task'].to_pylist()==[1,2,3,1,2,3,1]
        expanded = tmp_path/'expanded'
        rows.flat_map(lambda row: [row, row]).write_lance(str(expanded), mode='create', schema=schema)
        assert lance.dataset(str(expanded)).to_table()['within_task'].to_pylist()==[
            1,1,2,2,3,3,1,1,2,2,3,3,1,1]
        state = [0]
        def number_with_closure(row):
            state[0] += 1
            return {**row,'within_task':state[0]}
        for i, callback in enumerate([number_with_closure, Number().__call__]):
            another = tmp_path/f'stateful{i}'
            rows.map(callback).write_lance(str(another),mode='create',schema=schema)
            assert lance.dataset(str(another)).to_table()['within_task'].to_pylist()==[1,2,3,1,2,3,1]
        assert state == [0]
        def fail(row):
            if row['k']==4:
                raise ValueError('callback failed once')
            return row
        with pytest.raises(ValueError,match='callback failed once'):
            rows.map(fail).write_lance(str(target),mode='overwrite',schema=schema)
    assert lance.dataset(str(target)).version==1


def test_large_group_orders_only_keys_and_preserves_sequential_reducer(tmp_path, monkeypatch):
    """80 MiB of opaque input under a 32 MiB engine pool; no wide final sort."""
    from dataclasses import replace
    from demiflow.execution import dataset_native, native_resources
    monkeypatch.setattr(dataset_native, 'SORT_STAGE_ROWS', 1)
    original = native_resources.dataset_session
    def limited(*args, **kwargs):
        session = original(*args, **kwargs)
        session.options = replace(session.options, memory_bytes=32*1024**2,
            max_rss_bytes=2*1024**3, threads=1, partitions=1, timeout_s=120)
        session.resources = tmp_path / 'limited_resources'
        return session
    monkeypatch.setattr(native_resources, 'dataset_session', limited)
    source = api(tmp_path)
    count = 20000
    def records():
        for i in range(count):
            yield {'k': i % 3, 'seq': i, 'payload': bytes([i % 251])*4096}
    def fold(state, row):
        assert row['payload'] == bytes([row['seq'] % 251])*4096
        return {'k': row['k'], 'n': (state or {}).get('n', 0)+1,
            'total': (state or {}).get('total', 0)+row['seq'],
            'first': state['first'] if state else row['seq'], 'last': row['seq']}
    result = source.from_iter(records).reduce_by_key('k', fold)
    assert result.take_all() == [dict(k=k, n=len(range(k,count,3)),
        total=sum(range(k,count,3)), first=k, last=list(range(k,count,3))[-1]) for k in range(3)]
    reports = native_reports(result)
    ordered = [r for r in reports if r['purpose']=='stable_order_and_callbacks']
    assert len(ordered)==1
    directory = Path(ordered[0]['query'])
    request = json.loads((directory/'request.json').read_text())
    worker = json.loads((directory/'worker_result.json').read_text())
    assert request['sql'].startswith('SELECT _rowid AS selected_row_id FROM stage ORDER BY ')
    assert 'payload' not in request['sql']
    assert worker['payload_read']['rows']==count
    assert worker['payload_read']['sort_payload_columns']==0
    assert worker['payload_read']['max_batch_bytes'] <= 8*1024**2
    assert not list(tmp_path.glob('demiflow-dataset-*'))


def test_narrow_group_batches_are_not_shrunk_by_an_unrelated_wide_input(tmp_path, monkeypatch):
    """A wide left sibling must not force thousands of tiny RHS row lookups."""
    from demiflow.execution import dataset_native
    monkeypatch.setattr(dataset_native, 'SORT_STAGE_ROWS', 1)
    source = api(tmp_path)
    wide = [{'k': k, 'payload': bytes([k])*512*1024} for k in range(4)]
    narrow = [{'k': i % 4, 'seq': i} for i in range(4000)]
    def fold(state, row):
        return {'k': row['k'], 'n': (state or {}).get('n', 0)+1,
            'last': row['seq']}
    grouped = source.from_items(narrow).reduce_by_key('k', fold)
    result = source.from_items(wide).join(grouped, on='k')
    assert_same_rows(result.take_all(), [
        {**row, 'n': 1000, 'last': 3996+row['k']} for row in wide])
    reports = native_reports(result)
    ordered = next(r for r in reports if r['purpose']=='stable_order_and_callbacks')
    worker = json.loads((Path(ordered['query'])/'worker_result.json').read_text())
    assert worker['payload_read']['rows'] == 4000
    assert worker['payload_read']['batches'] <= 2
    final = json.loads((Path(reports[-1]['query'])/'request.json').read_text())
    # The final result still carries the wide payload and retains its small bound.
    assert final['options']['batch_rows'] <= 16
    assert not list(tmp_path.glob('demiflow-dataset-*'))
