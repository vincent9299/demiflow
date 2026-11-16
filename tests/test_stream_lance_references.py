import lance
import pyarrow as pa
import pytest
from demiflow import data
from _worker_event_fixtures import emit, events


def test_microbatch_references_allow_downstream_before_final_commit(tmp_path):
    schema=pa.schema([('id',pa.string())]);seen=str(tmp_path/'refs.jsonl')
    def verify(row):
        ref=row['source'];table=lance.dataset(**ref)
        assert row['id'] in table.to_table(columns=['id'])['id'].to_pylist()
        assert 'source' not in table.schema.names
        emit(seen,'reference',id=row['id'],version=ref['version']);return row
    stats=(data.from_items([{'id':'a'},{'id':'b'}])
        .save_lance(tmp_path/'first.lance',schema=schema,key='id',stage='first',max_batch=1,output_ref='source')
        .map(verify)
        .save_lance(tmp_path/'second.lance',schema=pa.schema([*schema,('source',pa.struct([('uri',pa.string()),('version',pa.int64())]))]),key='id',stage='second',max_batch=1)
        .run_stream())
    # The first real microbatch creates v1; no empty initialization publication.
    assert [(e['id'],e['version']) for e in events(seen)]==[('a',1),('b',2)]
    assert lance.dataset(str(tmp_path/'first.lance'),version=1).to_table()['id'].to_pylist()==['a']
    assert lance.dataset(str(tmp_path/'first.lance'),version=2).to_table()['id'].to_pylist()==['a','b']
    assert lance.dataset(str(tmp_path/'first.lance')).version==2
    assert lance.dataset(**stats.outputs['second']).count_rows()==2


def test_reference_field_collision_fails_before_row_commit(tmp_path):
    schema=pa.schema([('id',pa.string())]);uri=tmp_path/'out.lance'
    with pytest.raises(ValueError,match='overwrite'):
        (data.from_items([{'id':'a','source':None}])
            .save_lance(uri,schema=schema,key='id',stage='out',output_ref='source').run_stream())
    # Collision is rejected before even an empty table is published.
    assert not uri.exists()
    with pytest.raises(ValueError,match='outside'):
        data.from_items([]).save_lance(uri,schema=schema,key='id',stage='out',output_ref='id')


def test_stream_flat_map_is_lazy_filtered_and_closes_on_failure(tmp_path):
    import asyncio
    observed=str(tmp_path/"flat-map.jsonl")
    consumed = 0
    async def identity(row):return row
    def expand(row):
        try:
            for i in range(100000):
                emit(observed,'advanced',value=i);yield {'i':i}
        finally:emit(observed,'closed')
    async def consume(row):
        nonlocal consumed
        consumed += 1
        emit(observed,'seen',value=row['i'])
        await asyncio.sleep(.01)
        if consumed==3:raise ValueError('stop expansion')
        return row
    with pytest.raises(ValueError,match='stop expansion'):
        (data.from_items([{}]).map_async(identity,concurrency=1,queue_depth=1)
            .flat_map(expand).filter(lambda r:r['i']%2==0)
            .map_async(consume,concurrency=1,queue_depth=1).run_stream())
    actual=events(observed)
    assert [e['value'] for e in actual if e['event']=='seen']==[0,2,4]
    assert 0 < sum(e['event']=='advanced' for e in actual) < 100
    assert sum(e['event']=='closed' for e in actual)==1
