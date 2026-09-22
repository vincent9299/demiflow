"""Registered edits publish one usable version and roll back failed callbacks."""
import lance
import pyarrow as pa
import pytest
from demiflow.lance.transaction import registered_table_edit
from demiflow.lance.registry import Catalog


def test_failed_mutation_restores_registered_rows_and_retains_pinned_version(tmp_path):
    uri='raw/example.lance'
    with registered_table_edit(tmp_path,uri,schema_name='example',schema_version='v1'):
        lance.write_dataset(pa.table({'key':['a'],'value':[1]}),str(tmp_path/uri))
    original=Catalog(tmp_path).latest('raw/example')
    with pytest.raises(RuntimeError,match='fail after commit'):
        with registered_table_edit(tmp_path,uri,schema_name='example',schema_version='v1') as ds:
            ds.update({'value':'2'})
            raise RuntimeError('fail after commit')
    restored=Catalog(tmp_path).latest('raw/example')
    assert restored.lance_version>original.lance_version
    assert restored.open(tmp_path).to_table().equals(original.open(tmp_path).to_table())
    assert lance.dataset(str(tmp_path/uri)).version==restored.lance_version


def test_unregistered_head_and_failed_creation_are_not_adopted(tmp_path):
    uri='raw/example.lance'
    with pytest.raises(RuntimeError):
        with registered_table_edit(tmp_path,uri,schema_name='example',schema_version='v1'):
            lance.write_dataset(pa.table({'key':['a']}),str(tmp_path/uri))
            raise RuntimeError('fail')
    assert not (tmp_path/uri).exists() and not Catalog(tmp_path).registered()
    lance.write_dataset(pa.table({'key':['unowned']}),str(tmp_path/uri))
    with pytest.raises(ValueError,match='Unregistered'):
        with registered_table_edit(tmp_path,uri,schema_name='example',schema_version='v1'):pass
    assert lance.dataset(str(tmp_path/uri)).to_table()['key'].to_pylist()==['unowned']
