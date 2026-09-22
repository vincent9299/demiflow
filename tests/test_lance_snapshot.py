import hashlib
import shutil
import lance
import pyarrow as pa
from demiflow.lance.snapshot import clone_local_snapshot


def test_blob_snapshot_survives_source_retirement_and_preserves_pinned_version(tmp_path):
    schema=pa.schema([('id',pa.string()),lance.blob_field('data')])
    ds=lance.write_dataset(pa.Table.from_arrays([pa.array(['a']),lance.blob_array([b'payload'])],schema=schema),tmp_path/'old.lance')
    lance.write_dataset(pa.Table.from_arrays([pa.array(['b']),lance.blob_array([b'later'])],schema=schema),tmp_path/'old.lance',mode='append')
    clone=clone_local_snapshot(ds,tmp_path/'new.lance')
    clone.add_columns({'extra':'1'})
    assert lance.dataset(tmp_path/'old.lance').schema.names==['id','data']
    shutil.rmtree(tmp_path/'old.lance')
    assert clone.count_rows()==1
    assert clone.take_blobs('data',indices=[0])[0].read()==b'payload'


def test_failed_registered_edit_restores_visible_data(tmp_path):
    import pytest
    from demiflow.lance.transaction import registered_table_edit
    from demiflow.lance.registry import Catalog
    uri='raw/items.lance'
    with registered_table_edit(tmp_path,uri,schema_name='items',schema_version='v1'):
        lance.write_dataset(pa.table({'id':[1]}),str(tmp_path/uri))
    original=Catalog(tmp_path).registered()[-1]
    with pytest.raises(RuntimeError):
        with registered_table_edit(tmp_path,uri,schema_name='items',schema_version='v1') as ds:
            ds.update({'id':'2'})
            raise RuntimeError('failed between edits')
    assert original.open(tmp_path).to_table()['id'].to_pylist()==[1]
    current=max(Catalog(tmp_path).registered(),key=lambda r:r.lance_version)
    assert current.open(tmp_path).to_table()['id'].to_pylist()==[1]
