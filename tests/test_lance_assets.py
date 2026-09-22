"""One-table Blob reads: fixed versions, row IDs, nulls, corruption and cache."""
import hashlib
import shutil
import lance
import pyarrow as pa
import pytest
from demiflow.lance.assets import BlobAssetReader,AssetMissing,AssetCorrupted,AssetError


def write(uri,values,mode='create'):
    schema=pa.schema([('sha256',pa.string()),lance.blob_field('data')])
    table=pa.Table.from_arrays([pa.array([hashlib.sha256(v).hexdigest() for v in values]),lance.blob_array(values)],schema=schema)
    return lance.write_dataset(table,str(uri),mode=mode)


def test_single_table_read_pin_and_deleted_row_ids(tmp_path):
    uri=tmp_path/'assets.lance';ds=write(uri,[b'a',b'b'])
    key=lambda b:hashlib.sha256(b).hexdigest()
    reader=BlobAssetReader(uri)
    assert reader.read(key(b'b'))[0]==b'b'
    ds.delete("sha256 = '"+key(b'a')+"'")
    assert reader.resolve(key(b'a')).status=='missing'
    assert reader.read(key(b'b'))[0]==b'b'
    assert reader.read(key(b'a'),version=1)[0]==b'a'
    pinned=BlobAssetReader(uri,version=1)
    write(uri,[b'c'],mode='append')
    assert pinned.resolve(key(b'c')).status=='missing'
    assert reader.read(key(b'c'))[1]['table']==str(uri)


def test_missing_null_duplicate_corrupt_are_distinct(tmp_path):
    uri=tmp_path/'assets.lance';key=hashlib.sha256(b'a').hexdigest();reader=BlobAssetReader(uri)
    assert reader.resolve(key).status=='missing'
    ds=write(uri,[b'a'])
    schema=ds.schema
    def overwrite(values):
        table=pa.Table.from_arrays([pa.array([key]*len(values)),lance.blob_array(values)],schema=schema)
        lance.write_dataset(table,str(uri),mode='overwrite')
    overwrite([None]);assert reader.resolve(key).status=='missing'
    overwrite([b'wrong']);assert reader.resolve(key).status=='corrupt'
    with pytest.raises(AssetCorrupted):reader.read(key)
    overwrite([b'a',b'a']);assert reader.resolve(key).status=='read_error'
    with pytest.raises(AssetError):reader.read('invalid')


def test_cache_is_rebuilt_and_never_an_authoritative_read_fallback(tmp_path):
    uri=tmp_path/'assets.lance';write(uri,[b'pixels'])
    key=hashlib.sha256(b'pixels').hexdigest();reader=BlobAssetReader(uri,cache_dir=tmp_path/'cache')
    cached=reader.materialize(key,'png');cached.write_bytes(b'bad')
    assert reader.materialize(key,'png').read_bytes()==b'pixels'
    shutil.rmtree(uri)
    with pytest.raises(AssetMissing):reader.read(key)
    with pytest.raises(AssetMissing):reader.materialize(key,'png')


def test_arbitrary_content_columns(tmp_path):
    uri=tmp_path/'audio.lance';key=hashlib.sha256(b'audio').hexdigest()
    table=pa.Table.from_arrays([pa.array([key]),lance.blob_array([b'audio'])],names=['content_hash','payload'])
    lance.write_dataset(table,str(uri))
    assert BlobAssetReader(uri,id_column='content_hash',column='payload').read(key)[0]==b'audio'
