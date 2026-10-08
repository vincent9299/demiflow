"""Real CPU index builds, fixed snapshots and failures before publication."""
from pathlib import Path

import lance
import numpy as np
import pyarrow as pa
import pytest

from demiflow import data
from demiflow.errors import LanceWriteConflict
from demiflow.execution.datafusion import DataFusionQueryError, DataFusionSession
from demiflow.lance import vector_index_config


@pytest.fixture
def table(tmp_path):
    vectors = np.random.default_rng(7).normal(size=(64, 16)).astype('float32')
    vectors /= np.linalg.norm(vectors, axis=1, keepdims=True)
    values = pa.table({'id': list(range(64)),
        'embedding': pa.FixedSizeListArray.from_arrays(vectors.ravel(), 16)})
    uri = str(tmp_path / 'vectors.lance')
    lance.write_dataset(values, uri)
    return uri, values


@pytest.fixture
def options():
    return dict(num_partitions=2, m=8, ef_construction=32, threads=2,
        memory_bytes=64 * 2**20, max_rss_bytes=768 * 2**20,
        max_scratch_bytes=64 * 2**20, max_index_bytes=64 * 2**20,
        cgroup_headroom_bytes=128 * 2**20, timeout_s=60, admission_timeout_s=30)


def test_build_reuse_append_and_changed_graph_parameters(table, options):
    uri, values = table
    first = data.ensure_lance_vector_index(uri, expected_version=1, options=options)
    assert first['status'] == 'ready' and first['version'] == 2
    assert first['coverage']['num_indexed_rows'] == 64 and first['coverage']['num_unindexed_rows'] == 0
    assert lance.dataset(uri, version=1).list_indices() == []
    second = data.ensure_lance_vector_index(uri, expected_version=2, options=options)
    assert second['reused'] and second['version'] == 2
    assert second['index_uuids'] == first['index_uuids']
    appended = lance.write_dataset(values, uri, mode='append')
    third = data.ensure_lance_vector_index(uri, expected_version=appended.version, options=options)
    assert third['action'] == 'rebuild' and third['coverage']['num_indexed_rows'] == 128
    assert third['coverage']['num_unindexed_rows'] == 0
    changed = data.ensure_lance_vector_index(uri, expected_version=third['version'], options={**options, 'm': 12})
    assert changed['action'] == 'rebuild' and changed['index_uuids'] != third['index_uuids']
    assert len(lance.dataset(uri).list_indices()) == 1
    assert len(lance.dataset(uri, version=first['version']).list_indices()) == 1


@pytest.mark.parametrize('when', ['after_prepare', 'inside_commit'])
def test_version_conflict_never_publishes_staged_index(table, options, monkeypatch, when):
    uri, values = table
    if when == 'after_prepare':
        original = DataFusionSession._prepare_lance_index
        def race(self, request):
            result = original(self, request)
            lance.write_dataset(values.slice(0, 1), uri, mode='append')
            return result
        monkeypatch.setattr(DataFusionSession, '_prepare_lance_index', race)
    else:
        original = lance.LanceDataset.commit
        def race(*args, **kwargs):
            lance.write_dataset(values.slice(0, 1), uri, mode='append')
            return original(*args, **kwargs)
        monkeypatch.setattr(lance.LanceDataset, 'commit', race)
    with pytest.raises(LanceWriteConflict):
        data.ensure_lance_vector_index(uri, expected_version=1, options=options)
    ds = lance.dataset(uri)
    assert ds.version == 2 and ds.count_rows() == 65 and not ds.list_indices()
    assert not list((Path(uri) / '_indices').glob('*/index.idx'))


@pytest.mark.parametrize('guard', ['rss', 'timeout', 'disk'])
def test_resource_failure_does_not_commit(table, options, guard):
    uri, _ = table
    if guard == 'rss':
        options.update(memory_bytes=512 * 1024, max_rss_bytes=1024 * 1024)
    elif guard == 'timeout':
        options['timeout_s'] = .01
    else:
        options['max_index_bytes'] = 1
    with pytest.raises((DataFusionQueryError, ValueError)):
        data.ensure_lance_vector_index(uri, expected_version=1, options=options)
    ds = lance.dataset(uri)
    assert ds.version == 1 and not ds.list_indices()
    assert not list((Path(uri) / '_indices').glob('*/index.idx'))


def test_post_commit_replacement_keeps_published_snapshot(table, options, monkeypatch):
    uri, values = table
    original = DataFusionSession._prepare_lance_index
    published = {}

    def replace_after_commit(self, request):
        result = original(self, request)
        if request.get('inspect_only'):
            published.update(version=request['version'], uuid=request['uuid'])
            # Overwrite drops the index from head, but the committed older
            # snapshot is still a valid reader-owned reference.
            lance.write_dataset(values, uri, mode='overwrite')
        return result

    monkeypatch.setattr(DataFusionSession, '_prepare_lance_index', replace_after_commit)
    with pytest.raises(LanceWriteConflict):
        data.ensure_lance_vector_index(uri, expected_version=1, options=options)
    assert not lance.dataset(uri).list_indices()
    assert (Path(uri) / '_indices' / published['uuid'] / 'index.idx').is_file()
    old = lance.dataset(uri, version=published['version'])
    result = old.to_table(columns=['id'], nearest={'column': 'embedding',
        'q': values['embedding'][0].as_py(), 'k': 1, 'metric': 'cosine'})
    assert result['id'][0].as_py() == 0


def test_empty_invalid_vectors_and_explicit_version(table, options):
    uri, values = table
    empty = lance.write_dataset(values.slice(0, 0), uri, mode='overwrite')
    result = data.ensure_lance_vector_index(uri, expected_version=empty.version, options=options)
    assert result['status'] == 'empty' and result['version'] == empty.version
    assert result['coverage']['num_indexed_rows'] == 0
    bad = values.set_column(1, 'embedding', pa.FixedSizeListArray.from_arrays(np.zeros(64 * 16, dtype='float32'), 16))
    ds = lance.write_dataset(bad, uri, mode='overwrite')
    with pytest.raises(ValueError, match='nonzero'):
        data.ensure_lance_vector_index(uri, expected_version=ds.version, options=options)
    with pytest.raises(LanceWriteConflict):
        data.ensure_lance_vector_index(uri, expected_version=1, options=options)
    assert lance.dataset(uri).version == ds.version


def test_configuration_and_training_admission(table, options):
    uri, _ = table
    for change in ({'m': 0}, {'ef_construction': 1}, {'timeout_s': float('nan')},
                   {'metric': 'l2'}, {'name': '../bad'}, {'threads': True}, {'extra': 1}):
        with pytest.raises(ValueError):
            vector_index_config({**options, **change})
    with pytest.raises(ValueError, match='partitions exceeds'):
        data.ensure_lance_vector_index(uri, expected_version=1, options={**options, 'num_partitions': 65})
    assert lance.dataset(uri).version == 1
