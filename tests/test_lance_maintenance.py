import lance
import pyarrow as pa
import pytest
from demiflow.lance.maintenance import retire_tables
from demiflow.lance.records import LanceRecordStore
from demiflow.lance.refs import DatasetRef
from demiflow.lance.registry import Catalog, ReleaseRegistry
from demiflow.lance.storage import schema_hash


def registered(root):
    uri = 'runs/sample/output.lance'
    ds = lance.write_dataset(pa.table({'value': [1]}), str(root/uri))
    ref = DatasetRef('sample', uri, ds.version, 'sample', 'v1', schema_hash(ds.schema), 1)
    Catalog(root).register(ref)
    return ref


def test_surviving_release_blocks_delete(tmp_path):
    ref = registered(tmp_path)
    ReleaseRegistry(tmp_path).register('live', release_kind='sample', table_refs=[ref])
    with pytest.raises(ValueError, match='Surviving release'):
        retire_tables(tmp_path, table_uris=[ref.relative_uri], operation_id='blocked', reason='test')
    assert ref.open(tmp_path).count_rows() == 1
    assert Catalog(tmp_path).registered() == [ref]


def test_retirement_is_audited_and_repeatable(tmp_path):
    ref = registered(tmp_path)
    ReleaseRegistry(tmp_path).register('old', release_kind='sample', table_refs=[ref])
    kw = dict(table_uris=[ref.relative_uri], release_ids=['old'], operation_id='cleanup', reason='test')
    assert retire_tables(tmp_path, **kw) == {'tables': 1, 'releases': 1}
    assert not (tmp_path/ref.relative_uri).exists()
    assert Catalog(tmp_path).registered() == []
    assert ReleaseRegistry(tmp_path).get('old') is None
    record = LanceRecordStore(tmp_path, 'runs/maintenance/cleanup/records.lance').get('plan')
    assert record['catalog_rows'][0]['relative_uri'] == ref.relative_uri
    assert retire_tables(tmp_path, **kw) == {'tables': 1, 'releases': 1}


def test_reference_in_validation_is_protected(tmp_path):
    old = registered(tmp_path)
    ReleaseRegistry(tmp_path).register('live', release_kind='sample', table_refs=[old],
                                      validation={'provenance': old.to_dict()})
    with pytest.raises(ValueError, match='Surviving release'):
        retire_tables(tmp_path, table_uris=[old.relative_uri], operation_id='blocked', reason='test')


@pytest.mark.parametrize('uri', ['../bad.lance', '/tmp/bad.lance', 'registry/datasets.lance'])
def test_retirement_rejects_unsafe_paths(tmp_path, uri):
    with pytest.raises(ValueError, match='Unsafe'):
        retire_tables(tmp_path, table_uris=[uri], operation_id='bad', reason='test')


def test_replacement_preserves_pinned_version_and_rejects_stale_head(tmp_path):
    from demiflow.lance.registry import replace_registered_table
    from demiflow.errors import LanceWriteConflict
    ref = registered(tmp_path)
    new_ref = replace_registered_table(tmp_path, ref, pa.table({'value': [2, 3]}))
    assert ref.open(tmp_path).to_table()['value'].to_pylist() == [1]
    assert new_ref.open(tmp_path).to_table()['value'].to_pylist() == [2, 3]
    with pytest.raises(LanceWriteConflict):
        replace_registered_table(tmp_path, ref, pa.table({'value': [99]}))
    empty = replace_registered_table(tmp_path, new_ref, pa.table({'value': pa.array([], type=pa.int64())}))
    assert empty.open(tmp_path).count_rows() == 0
