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


def assert_same_rows(actual, expected):
    """Compare small test multisets, including duplicate rows and nested values."""
    import json
    key = lambda row: json.dumps(row, ensure_ascii=False, sort_keys=True, default=repr)
    assert sorted(actual, key=key) == sorted(expected, key=key)


def _oracle_join(left, right, lk, rk, how):
    """朴素参考算法独立于优化实现，保留旧 canonical 排序和组内稳定次序。"""
    import json
    def key(row, fields):
        return json.dumps([row[f] for f in fields], ensure_ascii=False, sort_keys=True, separators=(',', ':'))
    out = []
    for row in sorted(left, key=lambda r: key(r, lk)):
        matches = [r for r in right if key(row, lk) == key(r, rk)
                   and all(row[f] is not None for f in lk)]
        if how == 'semi':
            if matches: out.append(dict(row))
        elif how == 'anti':
            if not matches: out.append(dict(row))
        elif not matches:
            if how == 'left': out.append(dict(row))
        else:
            for other in matches:
                value = dict(row)
                for k, v in other.items():
                    if k in lk and k in rk and k in value and value[k] == v:
                        continue
                    value[k if k not in value else k + '_right'] = v
                out.append(value)
    return out


@pytest.mark.parametrize('budget', [1, 1000, 32*1024*1024])
@pytest.mark.parametrize('how', ['inner', 'left', 'semi', 'anti'])
def test_relational_hash_spill_match_oracle(budget, how):
    import random
    from datetime import datetime, timezone
    rng = random.Random(917)
    values = [None, True, False, 1, 1.0, '1', '中"文', ['nested'], {'k': 2}]
    left = [{'a': rng.choice(values), 'b': i % 3, 'v': i,
             'native': datetime(2026, 1, 1, tzinfo=timezone.utc), 'bytes': b'\x00\xff'} for i in range(60)]
    right = [{'x': rng.choice(values), 'y': i % 3, 'v': [i]} for i in range(55)]
    actual = ds(left).join(ds(right), on=['a', 'b'], right_on=['x', 'y'], how=how,
                           chunk_bytes=budget).take_all()
    assert_same_rows(actual, _oracle_join(left, right, ['a', 'b'], ['x', 'y'], how))


def test_join_group_order_reuse_and_callback_invalidation(monkeypatch):
    from demiflow.data import local_relational as rel
    calls = []
    original = rel.sorted_rows
    def tracked(*args, **kwargs):
        calls.append(tuple(args[1]))
        return original(*args, **kwargs)
    monkeypatch.setattr(rel, 'sorted_rows', tracked)
    def plan():
        return ds([{'k': 2, 'v': 'a'}, {'k': 1, 'v': 'b'}, {'k': 2, 'v': 'c'}]).join(
            ds([{'k': 1}, {'k': 2}]), on='k')
    def reducer(acc, row):
        return {'k': row['k'], 'vs': (acc or {}).get('vs', '') + row['v']}
    assert plan().reduce_by_key('k', reducer).take_all() == [{'k':1,'vs':'b'}, {'k':2,'vs':'ac'}]
    assert calls == []  # Dataset relations no longer use Python external sorting
    calls.clear()
    batches = plan().group_batches('k', max_rows=1).take_all()
    assert len(batches) == 3 and calls == []
    calls.clear()
    changed = plan().map(lambda row: {**row, 'k': 3-row['k']}).reduce_by_key('k', reducer).take_all()
    assert changed == [{'k':1,'vs':'ac'}, {'k':2,'vs':'b'}]
    assert calls == []


@pytest.mark.parametrize('how', ['inner', 'left', 'semi', 'anti'])
def test_normal_join_groups_do_not_spool(tmp_path, monkeypatch, how):
    from demiflow.data import local_relational as rel
    writes = []
    original = rel._write_records
    def tracked(path, records):
        writes.append(str(path))
        return original(path, records)
    monkeypatch.setattr(rel, '_write_records', tracked)
    left = [{'k': i} for i in range(100)]
    # right table exceeds budget, individual keys fit: use sort/merge without group files
    right = [{'k': i, 'v': 'x'*50} for i in range(100)]
    assert_same_rows(ds(left).join(ds(right), on='k', how=how, chunk_bytes=1000).take_all(), _oracle_join(left, right, ['k'], ['k'], how))
    assert writes == []  # native relations do not spool Python key-group files


def test_hot_key_cartesian_product_and_no_aliasing(tmp_path, monkeypatch):
    from demiflow.data import local_relational as rel
    writes = []
    original = rel._write_records
    def tracked(path, records):
        writes.append(str(path))
        return original(path, records)
    monkeypatch.setattr(rel, '_write_records', tracked)
    left = [{'k': 1, 'l': i} for i in range(3)]
    right = [{'k': 1, 'nested': [i]} for i in range(40)]
    result = ds(left).join(ds(right), on='k', chunk_bytes=500).take_all()
    assert_same_rows(result, _oracle_join(left, right, ['k'], ['k'], 'inner'))
    assert writes == []
    result[0]['nested'].append('mutated')
    assert all('mutated' not in row['nested'] for row in result[1:])


def test_large_stable_sort_multiple_merge_passes(tmp_path):
    from demiflow.data.local_relational import sorted_rows, key_of
    rows = [{'k': i % 7, 'i': i} for i in range(1100)]
    assert [r for _, r in sorted_rows(iter(rows), ['k'], tmp_path, 1)] == sorted(rows, key=lambda r: key_of(r, ['k']))


def test_parallel_sort_matches_serial(tmp_path):
    from demiflow.data.local_relational import sorted_rows
    a = tmp_path/'serial'; a.mkdir()
    b = tmp_path/'parallel'; b.mkdir()
    rows = [{'k': (i*7919)%37, 'i':i, 'payload':'x'*256} for i in range(40000)]
    assert list(sorted_rows(iter(rows), ['k'], a, 8*1024*1024, workers=1)) == list(
        sorted_rows(iter(rows), ['k'], b, 8*1024*1024, workers=2))


def test_sort_and_join_cleanup_on_consumer_error(tmp_path, monkeypatch):
    from demiflow.data import local_relational as rel
    original = rel.tempfile.TemporaryDirectory
    paths = []
    def directory(*args, **kwargs):
        kwargs.setdefault('dir', tmp_path)
        result = original(*args, **kwargs)
        paths.append(result.name)
        return result
    monkeypatch.setattr(rel.tempfile, 'TemporaryDirectory', directory)
    def fail(state, row):
        raise RuntimeError('consumer failed')
    with pytest.raises(RuntimeError, match='consumer failed'):
        ds([{'k':1}]*50).join(ds([{'k':1}]*50), on='k', chunk_bytes=1).reduce_by_key('k', fail).take_all()
    from pathlib import Path
    assert paths and all(not Path(path).exists() for path in paths)


@pytest.mark.parametrize('how,expected_rows', [('inner', 2), ('semi', 2), ('left', 100), ('anti', 100)])
def test_native_join_does_not_call_python_sort(monkeypatch, how, expected_rows):
    from demiflow.data import local_relational as rel
    original = rel.sorted_rows
    scanned = []
    def track(rows, *args, **kwargs):
        def counted():
            for row in rows:
                scanned.append(row['k'])
                yield row
        return original(counted(), *args, **kwargs)
    monkeypatch.setattr(rel, 'sorted_rows', track)
    left = [{'k':i, 'payload':'wide'*100} for i in reversed(range(100))]
    right = [{'k':1}, {'k':4}]
    assert_same_rows(ds(left).join(ds(right), on='k', how=how).take_all(), _oracle_join(left, right, ['k'], ['k'], how))
    assert scanned == []


def test_wide_sort_writes_payload_once_across_merge_passes(tmp_path, monkeypatch):
    from demiflow.data import local_relational as rel
    monkeypatch.setattr(rel, '_MERGE_FAN_IN', 2)
    original = rel._write_records
    encoded_sizes = []
    def track(path, records):
        def checked():
            for key, payload in records:
                encoded_sizes.append(len(payload))
                yield key, payload
        return original(path, checked())
    monkeypatch.setattr(rel, '_write_records', track)
    rows = [{'k': i % 3, 'i': i, 'wide': str(i) * 8192} for i in reversed(range(20))]
    actual = [row for _, row in rel.sorted_rows(iter(rows), ['k'], tmp_path, 512)]
    assert actual == sorted(rows, key=lambda r: rel.key_of(r, ['k']))
    assert len(encoded_sizes) > len(rows)  # Multiple merge passes really occurred.
    assert max(encoded_sizes) == 9  # Only offsets, never wide payload copies.
    assert (tmp_path / 'payloads.pickle').stat().st_size < sum(len(r['wide']) for r in rows) + 4096
