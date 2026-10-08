"""Explicit sink schema survives row callbacks with empty Arrow map values."""
import pyarrow as pa
import pytest
from demiflow import data


@pytest.mark.parametrize('relational', [False, True])
def test_empty_nested_maps_use_declared_schema(tmp_path, relational):
    schema = pa.schema([('id',pa.string()),('sources',pa.list_(pa.struct([
        ('queries',pa.map_(pa.string(),pa.string()))])))])
    rows = data.from_items([{'id':'a','sources':[{'queries':[]}]}])
    if relational:
        rows = rows.join(data.from_items([{'id':'a'}]),on='id').map(lambda row: row)
    path = str(tmp_path/'rows.lance')
    rows.write_lance(path,mode='create',schema=schema)
    assert data.read_lance(path,version=1).take(1) == [{'id':'a','sources':[{'queries':[]}]}]


def test_unknown_fields_are_not_silently_dropped(tmp_path):
    with pytest.raises(ValueError,match='outside'):
        data.from_items([{'id':'a','extra':3}]).write_lance(str(tmp_path/'bad.lance'),
            mode='create',schema=pa.schema([('id',pa.string())]))
