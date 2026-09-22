import hashlib
import shutil
import lance
import pyarrow as pa
import pytest
from demiflow.lance.consolidate import consolidate_local_tables


def test_single_table_owns_blobs_columns_deletions_after_sources_retire(tmp_path):
    schema=pa.schema([('key',pa.string()),lance.blob_field('data')]);sources=[]
    for n in range(2):
        rows=pa.Table.from_arrays([pa.array([f'{n}-keep',f'{n}-drop']),lance.blob_array([b'pixels'+bytes([n]),b'drop'])],schema=schema)
        ds=lance.write_dataset(rows,str(tmp_path/f'source{n}.lance'))
        ds.add_columns(pa.table({'tags':[['a'],['b']]}));ds.delete(f"key = '{n}-drop'")
        sources.append(ds)
    out=consolidate_local_tables(sources,tmp_path/'all.lance')
    assert out.count_rows()==2
    for n in range(2):shutil.rmtree(tmp_path/f'source{n}.lance')
    reopened=lance.dataset(str(tmp_path/'all.lance'))
    rows=reopened.scanner(columns=['key','tags'],with_row_id=True).to_table().to_pylist()
    assert {r['key'] for r in rows}=={'0-keep','1-keep'}
    for r in rows:
        assert r['tags']==['a']
        assert reopened.take_blobs('data',ids=[r['_rowid']])[0].read()==b'pixels'+bytes([int(r['key'][0])])
    assert len(reopened.get_fragments())==2 and reopened.version==2


def test_reject_mismatched_schemas_without_publishing(tmp_path):
    a=lance.write_dataset(pa.table({'a':[1]}),str(tmp_path/'a'))
    b=lance.write_dataset(pa.table({'b':[1]}),str(tmp_path/'b'))
    with pytest.raises(ValueError,match='schemas'):consolidate_local_tables([a,b],tmp_path/'all')
    assert not (tmp_path/'all').exists()
