import pytest
from demiflow.data.api import DataAPI


def ds(rows):return DataAPI().from_items(rows)


def test_join_many_to_many_and_nulls():
    left=ds([{'k':1,'v':'a'},{'k':1,'v':'b'},{'k':2,'v':'c'},{'k':None,'v':'null'}])
    right=ds([{'k':1,'v':'x'},{'k':1,'v':'y'},{'k':None,'v':'n'}])
    result=left.join(right,on='k',chunk_bytes=1).take_all()
    assert len(result)==4
    assert {r['v_right'] for r in result}=={'x','y'}
    assert len(left.join(right,on='k',how='left',chunk_bytes=1).take_all())==6
    assert len(left.join(right,on='k',how='semi',chunk_bytes=1).take_all())==2
    assert {r['k'] for r in left.join(right,on='k',how='anti').take_all()}=={2,None}


def test_group_spill_batches_and_composite_keys():
    data=ds([{'a':'x','b':1,'n':i} for i in range(100)])
    batches=data.group_batches(['a','b'],max_rows=7,chunk_bytes=1).take_all()
    assert sum(len(r['items']) for r in batches)==100
    assert max(len(r['items']) for r in batches)==7
    assert sum(r['group_last'] for r in batches)==1
    assert data.reduce_by_key(['a','b'],lambda acc,r:{'a':r['a'],'b':r['b'],'count':(acc or {}).get('count',0)+1},chunk_bytes=1).take_all()[0]['count']==100
    assert ds([]).join(ds([]),on='k').take_all()==[]


def test_checkpoint_cached_map_failure_resume(tmp_path):
    calls=[];failed=[False]
    class Map:
        concurrency=1
        async def __call__(self,row):
            calls.append(row['i'])
            if row['i']==2 and not failed[0]:failed[0]=True;raise RuntimeError('interrupted')
            return {**row,'square':row['i']**2}
    def plan():return ds([{'i':i} for i in range(4)]).map_cached(Map(),cache_dir=tmp_path/'cache',version='v1')
    with pytest.raises(RuntimeError):plan().checkpoint(tmp_path/'out.jsonl',version='v1')
    before=calls.count(0)
    result=plan().checkpoint(tmp_path/'out.jsonl',version='v1')
    assert result.count()==4 and calls.count(0)==before
    n=len(calls)
    assert plan().checkpoint(tmp_path/'out.jsonl',version='v1').count()==4
    assert len(calls)==n
    assert list((tmp_path/'cache').rglob('*.error.json'))
    with pytest.raises(ValueError):plan().checkpoint(tmp_path/'out.jsonl',version='v2')


def test_join_collision_rejected():
    with pytest.raises(ValueError,match='collision'):
        ds([{'k':1,'x':1,'x_right':2}]).join(ds([{'k':1,'x':3}]),on='k').take_all()


def test_checkpoint_sync_and_mixed_prefix(tmp_path):
    class Double:
        async def __call__(self,row):return {**row,'doubled':row['n']*2}
    rows=ds([{'n':1},{'n':2}]).map(lambda r:{'n':r['n']+1}).filter(lambda r:r['n']>2)
    assert rows.checkpoint(tmp_path/'sync.jsonl',version='v1').take_all()==[{'n':3}]
    assert rows.map_cached(Double(),cache_dir=tmp_path/'cache',version='v1').checkpoint(tmp_path/'async.jsonl',version='v1').take_all()==[{'n':3,'doubled':6}]


def test_async_checkpoint_preserves_source_context_across_feed_chunks(tmp_path, monkeypatch):
    import demiflow.execution.stream as stream
    from demiflow.observability import _CURRENT_ACTION
    monkeypatch.setattr(stream, '_FEED_CHUNK', 2)
    class Echo:
        async def __call__(self, row):return row
    before = _CURRENT_ACTION.get()
    result = (ds([{'n':i} for i in range(7)])
              .map_cached(Echo(), cache_dir=tmp_path/'cache', version='v1')
              .checkpoint(tmp_path/'out.jsonl', version='v1').take_all())
    assert sorted(r['n'] for r in result) == list(range(7))
    assert _CURRENT_ACTION.get() is before


def test_consecutive_flat_maps_bind_separate_iterators():
    from demiflow.data.api import DataAPI
    rows = (DataAPI().from_items([{"value": 1}, {"value": 2}])
            .flat_map(lambda r: [{"value": r["value"]}, {"value": r["value"] + 10}])
            .flat_map(lambda r: [{"value": r["value"] * 2}]).take_all())
    assert [r["value"] for r in rows] == [2, 22, 4, 24]


def test_checkpoint_async_in_running_loop_resume_and_version(tmp_path):
    import asyncio
    calls = []
    fail = [True]
    class Actor:
        concurrency = 1
        queue_depth = 1
        async def __call__(self, row):
            calls.append(row['n'])
            await asyncio.sleep(0)
            if row['n'] == 2 and fail[0]:
                fail[0] = False
                raise RuntimeError('failure to preserve')
            return {**row, 'twice': row['n'] * 2}
    def plan():
        return ds([{'n':1}, {'n':2}]).map_cached(Actor(), cache_dir=tmp_path/'cache', version='v1')
    async def notebook():
        with pytest.raises(RuntimeError, match='failure to preserve'):
            await plan().checkpoint_async(tmp_path/'out.jsonl', version='v1')
        assert not (tmp_path/'out.jsonl').exists()
        out = await plan().checkpoint_async(tmp_path/'out.jsonl', version='v1')
        assert out.take_all() == [{'n':1,'twice':2}, {'n':2,'twice':4}]
        assert calls.count(1) == 1
        before = len(calls)
        await plan().checkpoint_async(tmp_path/'out.jsonl', version='v1')
        assert len(calls) == before
        with pytest.raises(ValueError, match='version changed'):
            await plan().checkpoint_async(tmp_path/'out.jsonl', version='v2')
        sync = await ds([{'n':3}]).checkpoint_async(tmp_path/'sync.jsonl', version='v1')
        assert sync.take_all() == [{'n':3}]
    asyncio.run(notebook())
