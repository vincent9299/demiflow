import lance
import pyarrow as pa
import pytest
from demiflow import data


def test_microbatch_references_allow_downstream_before_final_commit(tmp_path):
    schema=pa.schema([('id',pa.string())]);seen=[]
    def verify(row):
        ref=row['source'];table=lance.dataset(**ref)
        assert row['id'] in table.to_table(columns=['id'])['id'].to_pylist()
        assert 'source' not in table.schema.names
        seen.append((row['id'],ref['version']));return row
    stats=(data.from_items([{'id':'a'},{'id':'b'}])
        .save_lance(tmp_path/'first.lance',schema=schema,key='id',stage='first',max_batch=1,output_ref='source')
        .map(verify)
        .save_lance(tmp_path/'second.lance',schema=pa.schema([*schema,('source',pa.struct([('uri',pa.string()),('version',pa.int64())]))]),key='id',stage='second',max_batch=1)
        .run_stream())
    assert seen==[('a',2),('b',3)]
    assert lance.dataset(**stats.outputs['second']).count_rows()==2


def test_reference_field_collision_fails_before_row_commit(tmp_path):
    schema=pa.schema([('id',pa.string())]);uri=tmp_path/'out.lance'
    with pytest.raises(ValueError,match='overwrite'):
        (data.from_items([{'id':'a','source':None}])
            .save_lance(uri,schema=schema,key='id',stage='out',output_ref='source').run_stream())
    assert lance.dataset(str(uri)).count_rows()==0
    with pytest.raises(ValueError,match='outside'):
        data.from_items([]).save_lance(uri,schema=schema,key='id',stage='out',output_ref='id')


def test_stream_flat_map_is_lazy_filtered_and_closes_on_failure():
    import asyncio
    advanced=[];closed=[];seen=[]
    async def identity(row):return row
    def expand(row):
        try:
            for i in range(100000):
                advanced.append(i);yield {'i':i}
        finally:closed.append(True)
    async def consume(row):
        seen.append(row['i'])
        await asyncio.sleep(.01)
        if len(seen)==3:raise ValueError('stop expansion')
        return row
    with pytest.raises(ValueError,match='stop expansion'):
        (data.from_items([{}]).map_async(identity,concurrency=1,queue_depth=1)
            .flat_map(expand).filter(lambda r:r['i']%2==0)
            .map_async(consume,concurrency=1,queue_depth=1).run_stream())
    assert seen==[0,2,4] and len(advanced)<100 and closed==[True]
