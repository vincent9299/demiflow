"""Single-machine stage execution: semantics, real worker use, bounded cleanup."""
import os
import time
from pathlib import Path

import pytest

from demiflow import data
from demiflow.data.api import DataAPI
from demiflow.execution.executors.local import LocalDatasetExecutor
from tests.test_local_relational import _oracle_join


def _slow_map(row):
    time.sleep(.003)
    return {'k': row['id'] % 13, 'v': row['id'], 'pid': os.getpid()}


@pytest.mark.parametrize('mode', ['thread', 'process'])
def test_fused_map_shuffle_reduce_uses_workers_and_preserves_fold_order(tmp_path, mode):
    with data.local_execution(workers=3, worker_mode=mode, partitions=7, batch_rows=12,
                              memory_bytes=3*65536, temp_directory=str(tmp_path)) as local:
        result = (data.range(120).map(_slow_map).filter(lambda r: r['v'] % 2 == 0)
                  .reduce_by_key('k', lambda a,r: {'k':r['k'], 'values':(a or {}).get('values',[])+[r['v']],
                                                'pid':os.getpid()}, chunk_bytes=300).take_all())
        assert {r['k']:r['values'] for r in result} == {k:[i for i in range(120) if i%13==k and i%2==0] for k in range(13)}
        workers = local.stats['workers_used']
        assert len(workers) > 1
        if mode == 'process':
            assert len({pid for pid,_ in workers}) > 1 and all(pid != os.getpid() for pid,_ in workers)
        else:
            assert {pid for pid,_ in workers} == {os.getpid()}
        assert local.stats['peak_pending_tasks'] <= 6
        assert local.stats['engine'] == 'datafusion'
        assert any(s['name']=='datafusion' for s in local.stats['stages'])
    assert list(tmp_path.iterdir()) == []


@pytest.mark.parametrize('how', ['inner','left','semi','anti'])
@pytest.mark.parametrize('mode', ['thread','process'])
def test_join_matches_legacy_canonical_semantics(tmp_path, how, mode):
    from datetime import datetime, timezone
    keys=[None,True,False,1,1.0,'1','中"文',['nested'],{'k':2}]
    left=[{'a':keys[i%len(keys)],'b':i%2,'v':i,'native':datetime(2026,1,1,tzinfo=timezone.utc),'bytes':b'\xff'} for i in range(36)]
    right=[{'x':keys[i%len(keys)],'y':i%2,'v':[i]} for i in range(29)]
    with data.local_execution(workers=2, worker_mode=mode, partitions=5, batch_rows=7,
                              memory_bytes=2*65536, temp_directory=str(tmp_path)):
        result=data.from_items(left).join(data.from_items(right),on=['a','b'],right_on=['x','y'],how=how,chunk_bytes=150).take_all()
    assert result == _oracle_join(left,right,['a','b'],['x','y'],how)
    assert list(tmp_path.iterdir()) == []


def test_join_fold_fusion_retains_hidden_key_and_unfuses_after_map():
    def build(api, change=False):
        d=api.from_items([{'k':2,'v':2},{'k':1,'v':1},{'k':2,'v':3}]).join(
            api.from_items([{'k':1,'r':7},{'k':2,'r':9},{'k':2,'r':8}]),on='k')
        if change: d=d.map(lambda r:{**r,'k':3-r['k']})
        return d.reduce_by_key('k',lambda a,r:{'sequence':(a or {}).get('sequence',[])+[(r['v'],r['r'])]})
    for change in [False,True]:
        expected=build(DataAPI(),change).take_all()
        with data.local_execution(workers=2,worker_mode='thread',batch_rows=2,partitions=3) as local:
            assert build(data,change).take_all()==expected
            stages=[s['name'] for s in local.stats['stages']]
            assert stages.count('datafusion') == (2 if change else 1)


def test_group_batches_and_initial_state_are_isolated():
    def group(api):
        return api.range(12).map(lambda r:{'k':r['id']%3,'v':r['id']}).group_batches('k',max_rows=2).take_all()
    expected=group(DataAPI())
    with data.local_execution(workers=2,worker_mode='thread',partitions=5,batch_rows=3):
        assert group(data)==expected
        initial={'values':[]}
        def append(acc,row):
            acc['values'].append(row['id']); return acc
        rows=data.range(3).reduce_by_key('id',append,initial=initial).take_all()
        assert rows == [{'values':[0]},{'values':[1]},{'values':[2]}] and initial=={'values':[]}


def test_narrow_chain_batch_boundaries_and_limit_match_serial():
    def chain(api):
        return (api.range(31).map(lambda r:{'a':r['id']}).flat_map(lambda r:[r,{'a':r['a']+100}])
                .filter(lambda r:r['a']%2==0).map_batches(lambda b:[{'n':len(b),'v':int(b['a'].sum())}],
                    batch_size=5,batch_format='pandas').limit(3).take_all())
    expected=chain(DataAPI())
    with data.local_execution(workers=2,worker_mode='thread',batch_rows=7) as local:
        assert chain(data)==expected
        assert local.stats['peak_pending_tasks']<=4


@pytest.mark.parametrize('mode',['thread','process'])
def test_failure_and_early_consumer_close_cleanup_owned_work(tmp_path,mode):
    calls=[]
    def source():
        for n in range(10000):
            calls.append(n); yield {'id':n}
    with data.local_execution(workers=2,worker_mode=mode,batch_rows=5,temp_directory=str(tmp_path)) as local:
        assert len(data.from_iter(source).map(lambda r:r).take(1))==1
        assert len(calls)<=25
        assert local.stats['status']=='closed_early'
        assert list(tmp_path.iterdir())==[]
        def fail(row):
            if row['id']==9: raise ValueError('worker failure')
            return row
        with pytest.raises(ValueError,match='worker failure'):
            data.range(20).map(fail).reduce_by_key('id',lambda a,r:r).take_all()
        assert local.stats['status']=='failed'
        assert list(tmp_path.iterdir())==[]


def test_lance_fixed_version_narrow_columns_and_normal_writer(tmp_path):
    lance=pytest.importorskip('lance'); import pyarrow as pa
    uri=str(tmp_path/'source.lance');out=str(tmp_path/'output.lance')
    lance.write_dataset(pa.table({'k':[i%3 for i in range(80)],'value':list(range(80)), 'unused':['wide'*100]*80}),uri,
                        max_rows_per_file=11)
    lance.write_dataset(pa.table({'k':[999],'value':[999],'unused':['changed']}),uri,mode='overwrite')
    with data.local_execution(workers=2,worker_mode='process',batch_rows=7,partitions=5) as local:
        ds=data.read_lance(uri,version=1,columns=['k','value'])
        result=ds.reduce_by_key('k',lambda a,r:{'k':r['k'],'sum':(a or {}).get('sum',0)+r['value'],
                                              'values':(a or {}).get('values',[])+[r['value']]})
        result.write_lance(out,mode='overwrite',schema=pa.schema([('k',pa.int64()),('sum',pa.int64()),('values',pa.list_(pa.int64()))]))
        assert local.stats['status']=='complete'
        assert local.stats['engine']=='datafusion'
        assert any(s['name']=='datafusion' for s in local.stats['stages'])
    assert lance.dataset(out).to_table().to_pylist()==[{'k':k,'sum':sum(range(k,80,3)), 'values':list(range(k,80,3))} for k in range(3)]


def test_bloom_filter_is_only_a_prefilter_and_does_not_move_across_udf(tmp_path,monkeypatch):
    from demiflow.execution import local_tasks as kernel
    # Worst-case false positives: exact join must still reject every wrong key.
    monkeypatch.setattr(kernel,'_may_match',lambda value,bitmap:True)
    with data.local_execution(workers=2,worker_mode='thread',batch_rows=3,partitions=5) as local:
        left=data.range(50).map(lambda r:{'k':r['id']+100,'value':r['id']})
        right=data.from_items([{'k':103},{'k':117}])
        assert left.join(right,on='k').take_all()==[{'k':103,'value':3},{'k':117,'value':17}]
        assert local.stats['engine']=='datafusion'
        assert any(s['name']=='datafusion' for s in local.stats['stages'])


def test_context_and_configuration_do_not_silently_change_existing_execution():
    ordinary=data.range(1)
    assert ordinary._executor._local_kernel is None
    with data.local_execution(workers=2,worker_mode='thread'):
        d=data.range(10).map(lambda r:r)
        with pytest.raises(ValueError,match='same local_execution'):
            d.join(ordinary,on='id')
        with pytest.raises(ValueError,match='nested'):
            with data.local_execution(): pass
    with pytest.raises(RuntimeError,match='closed'):
        d.take_all()
    with pytest.raises(ValueError,match='worker_mode'):
        with data.local_execution(worker_mode='magic'): pass
    assert data.range(1)._executor._local_kernel is None


def test_unsupported_child_plan_fails_before_any_callback(tmp_path):
    called=[]
    with data.local_execution(workers=2,worker_mode='thread',temp_directory=str(tmp_path)):
        def source():
            called.append('scan'); yield {'k':1}
        left=data.from_iter(source)
        right=data.from_iter(source).random_shuffle()
        with pytest.raises(NotImplementedError,match='RandomShuffleOp'):
            left.join(right,on='k').take_all()
    assert not called and not list(tmp_path.iterdir())


def test_worker_exception_cannot_publish_partial_lance_output(tmp_path):
    lance=pytest.importorskip('lance'); import pyarrow as pa
    uri=str(tmp_path/'output.lance')
    lance.write_dataset(pa.table({'id':[999]}),uri)
    def fail(row):
        if row['id']==9: raise RuntimeError('do not commit')
        return row
    with data.local_execution(workers=2,worker_mode='process',batch_rows=4):
        with pytest.raises(RuntimeError,match='do not commit'):
            data.range(20).map(fail).write_lance(uri,mode='overwrite',schema=pa.schema([('id',pa.int64())]))
    assert lance.dataset(uri).version==1
    assert lance.dataset(uri).to_table().to_pylist()==[{'id':999}]
