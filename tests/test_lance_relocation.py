"""Physical relocation preserves old versions, record refs, blobs and new writes."""
import json
from pathlib import Path

import lance
import pyarrow as pa
import pytest

from demiflow.lance.blobs import LanceBlobStore
from demiflow.lance.records import LanceRecordStore
from demiflow.lance.refs import DatasetRef
from demiflow.lance.registry import Catalog
from demiflow.lance.storage import normalize_lance_uri, resolve_local_uri, schema_hash
from demiflow.lance.transaction import registered_table_edit


def relocate(root, pairs):
    for old, new in pairs.items():
        target = root / new
        target.parent.mkdir(parents=True, exist_ok=True)
        (root / old).rename(target)
    manifest = root / '_demiflow/lance_locations.json'
    manifest.parent.mkdir(exist_ok=True)
    manifest.write_text(json.dumps({'version': 1, 'tables': pairs}))


def test_frozen_versions_survive_and_new_writer_uses_same_table(tmp_path):
    old, new = 'old/items.lance', 'pipeline/datasets/items.lance'
    first = lance.write_dataset(pa.table({'id': [1]}), tmp_path / old)
    ref = DatasetRef('stable_id', old, 1, 'items', 'v1', schema_hash(first.schema), 1)
    Catalog(tmp_path).register(ref)
    relocate(tmp_path, {old: new})
    assert ref.to_dict()['relative_uri'] == old
    assert ref.open(tmp_path).to_table()['id'].to_pylist() == [1]
    assert normalize_lance_uri((tmp_path / old).as_uri()) == str(tmp_path / new)
    with registered_table_edit(tmp_path, new, schema_name='items', schema_version='v1') as ds:
        assert ds.version == 1
        lance.write_dataset(pa.table({'id': [2]}), str(tmp_path / new), mode='append')
    assert ref.open(tmp_path).count_rows() == 1
    assert lance.dataset(tmp_path / new).count_rows() == 2
    assert not (tmp_path / old).exists()


def test_record_and_blob_references_survive(tmp_path):
    records = LanceRecordStore(tmp_path, 'old/records.lance')
    pinned = records.put('case', {'state': 'before'})
    records.put('case', {'state': 'after'}, immutable=False)
    blob = LanceBlobStore(tmp_path, 'old/blobs.lance').put(b'original pixels')
    relocate(tmp_path, {'old/records.lance': 'pipeline/datasets/records.lance',
                        'old/blobs.lance': 'pipeline/datasets/blobs.lance'})
    assert pinned.read(tmp_path) == {'state': 'before'}
    assert LanceRecordStore(tmp_path, 'old/records.lance').get('case') == {'state': 'after'}
    assert blob.read(tmp_path) == b'original pixels'


def test_missing_target_does_not_fall_back_or_escape(tmp_path):
    manifest = tmp_path / '_demiflow/lance_locations.json'
    manifest.parent.mkdir()
    manifest.write_text(json.dumps({'version': 1, 'tables': {'old.lance': '../escape.lance'}}))
    with pytest.raises(ValueError, match='Invalid relocated'):
        resolve_local_uri(tmp_path / 'old.lance')
    assert resolve_local_uri(tmp_path / 'unmapped.lance') == tmp_path / 'unmapped.lance'
