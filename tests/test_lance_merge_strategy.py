"""Automatic physical selection with real Lance data, unchanged Dataset API."""
import lance
import pyarrow as pa
import pytest

from demiflow import data
from demiflow.errors import InvalidLanceRequest
from demiflow.lance.model import LanceWriteSpec
from demiflow.lance.mutate import _merge_write_mode


def _files(ds):
    return {f.path for fragment in ds.get_fragments() for f in fragment.metadata.files}


@pytest.mark.parametrize('scope,policy,expected', [
    ('dense', 'error', 'rewrite_columns'),
    ('sparse', 'error', 'rewrite_columns'),
    ('limited', 'error', 'rewrite_columns'),
    ('filtered', 'error', 'auto'),
    ('dense', 'ignore', 'auto'),
    ('dense', 'insert', 'auto'),
    ('full_rows', 'error', 'auto'),
])
def test_dataset_merge_strategy_and_values(tmp_path, monkeypatch, scope, policy, expected):
    target = str(tmp_path / 'target.lance')
    source = str(tmp_path / 'patch.lance')
    original = pa.table({'id': range(10), 'value': ['old'] * 10,
                         'wide': [[{'label': 'keep', 'n': i}] for i in range(10)]})
    old = lance.write_dataset(original, target, max_rows_per_file=5, max_rows_per_group=5)
    changes = pa.table({'id': range(6), 'value': ['new'] * 6})
    if scope == 'sparse':
        changes = changes.slice(0, 4)
    if scope == 'full_rows':
        changes = changes.append_column('wide', original['wide'].slice(0, 6))
    source_ds = lance.write_dataset(changes, source)
    modes = []
    import demiflow.lance.mutate as mutation
    import demiflow.execution.lance_merge as managed
    original_selection = mutation._merge_write_mode

    def capture(*args):
        mode = original_selection(*args)
        modes.append(mode)
        return mode

    monkeypatch.setattr(mutation, '_merge_write_mode', capture)
    monkeypatch.setattr(managed, '_merge_write_mode', capture)
    options = {'limit': 1} if scope == 'limited' else {'filter': 'id < 2'} if scope == 'filtered' else {}
    receipt = data.read_lance(source, version=source_ds.version, **options).write_lance(
        target, mode='merge', on='id', update_columns=changes.schema.names[1:],
        when_not_matched=policy, expected_version=old.version, return_receipt=True)
    assert modes and all(mode == expected for mode in modes)
    changed_n = 1 if scope == 'limited' else 2 if scope == 'filtered' else 4 if scope == 'sparse' else 6
    current = lance.dataset(target)
    expected_table = original.set_column(1, 'value', pa.array(['new'] * changed_n + ['old'] * (10 - changed_n)))
    assert current.to_table().sort_by('id').equals(expected_table)
    assert old.to_table().equals(original)
    assert receipt.merge_stats == {'updated_rows': changed_n, 'inserted_rows': 0, 'ignored_rows': 0}
    if expected == 'rewrite_columns':
        assert _files(old) <= _files(current)  # untouched wide payload files are reused
        assert [f.fragment_id for f in old.get_fragments()] == [f.fragment_id for f in current.get_fragments()]


@pytest.mark.parametrize('bad_ids', [[0, 0, 2], [0, 1, 99], [0, 1, None]])
def test_sparse_column_merge_invalid_patch_is_atomic(tmp_path, bad_ids):
    target, source = str(tmp_path / 'target.lance'), str(tmp_path / 'patch.lance')
    old = lance.write_dataset(pa.table({'id': range(10), 'value': ['old'] * 10, 'keep': range(10)}), target)
    lance.write_dataset(pa.table({'id': bad_ids, 'value': ['new'] * 3}), source)
    with pytest.raises(InvalidLanceRequest):
        data.read_lance(source, version=1).write_lance(target, mode='merge', on='id', update_columns=['value'])
    assert lance.dataset(target).version == old.version
    assert lance.dataset(target).to_table().equals(old.to_table())


def test_blob_metadata_cannot_be_hidden_by_patch_schema(tmp_path):
    spec = LanceWriteSpec(str(tmp_path / 'unused.lance'), mode='merge', on=('id',), update_columns=('value',))
    target = pa.schema([('id', pa.int64()),
                        pa.field('value', pa.large_binary(), metadata={b'lance-encoding:blob': b'true'}),
                        ('keep', pa.string())])
    patch = pa.schema([('id', pa.int64()), ('value', pa.large_binary())])
    assert _merge_write_mode(spec, target, patch, 9, 10) == 'auto'


def test_composite_keys_nested_values_and_nulls(tmp_path):
    target, source = str(tmp_path / 'target.lance'), str(tmp_path / 'patch.lance')
    original = pa.table({'id': [1, 1, 2], 'lang': ['en', 'zh', 'en'],
                         'value': [[{'v': 'a'}], [{'v': 'b'}], [{'v': 'c'}]], 'keep': [1, 2, 3]})
    old = lance.write_dataset(original, target)
    patch = original.select(['id', 'lang', 'value']).slice(0, 2).set_column(
        2, 'value', pa.array([None, [{'v': 'updated'}]], type=original.schema.field('value').type))
    lance.write_dataset(patch, source)
    data.read_lance(source, version=1).write_lance(target, mode='merge', on=['id', 'lang'], update_columns=['value'])
    actual = lance.dataset(target).to_table().sort_by([('id', 'ascending'), ('lang', 'ascending')])
    assert actual['value'].to_pylist() == [None, [{'v': 'updated'}], [{'v': 'c'}]]
    assert actual['keep'].equals(original['keep'])
    assert old.to_table().equals(original)


def test_isolated_prepare_commit_race_is_rejected(tmp_path, monkeypatch):
    from demiflow.execution.datafusion import DataFusionSession
    from demiflow.errors import LanceWriteConflict
    target, source = str(tmp_path / 'target.lance'), str(tmp_path / 'patch.lance')
    old = lance.write_dataset(pa.table({'id': [1, 2], 'value': ['old', 'old'], 'keep': [7, 8]}), target)
    lance.write_dataset(pa.table({'id': [1, 2], 'value': ['new', 'new']}), source)
    prepare = DataFusionSession._prepare_lance_merge

    def race(self, *args):
        prepared = prepare(self, *args)
        lance.write_dataset(pa.table({'id': [3], 'value': ['competitor'], 'keep': [9]}), target, mode='append')
        return prepared

    monkeypatch.setattr(DataFusionSession, '_prepare_lance_merge', race)
    with pytest.raises(LanceWriteConflict):
        data.read_lance(source, version=1).write_lance(target, mode='merge', on='id', update_columns=['value'])
    result = lance.dataset(target)
    assert result.version == old.version + 1
    assert result.to_table().sort_by('id')['value'].to_pylist() == ['old', 'old', 'competitor']


def test_isolated_prepare_guard_cannot_commit(tmp_path, monkeypatch):
    import demiflow.execution.lance_merge as managed
    from demiflow.execution.datafusion import DataFusionOptions, DataFusionSession, DataFusionQueryError
    target, source = str(tmp_path / 'target.lance'), str(tmp_path / 'patch.lance')
    old = lance.write_dataset(pa.table({'id': [1, 2], 'value': ['old', 'old'], 'keep': [7, 8]}), target)
    lance.write_dataset(pa.table({'id': [1, 2], 'value': ['new', 'new']}), source)

    def tiny_session(executor, directory, **kwargs):
        return DataFusionSession(resource_directory=tmp_path / 'slots', temp_directory=directory,
            options=DataFusionOptions(memory_bytes=512 * 1024, max_rss_bytes=1024 * 1024, threads=1,
                                      timeout_s=15, admission_timeout_s=15))

    monkeypatch.setattr(managed, 'dataset_session', tiny_session)
    with pytest.raises(DataFusionQueryError) as exc:
        data.read_lance(source, version=1).write_lance(target, mode='merge', on='id', update_columns=['value'])
    assert exc.value.report['guard'] == 'rss'
    assert lance.dataset(target).version == old.version
    assert lance.dataset(target).to_table().equals(old.to_table())


@pytest.mark.parametrize('duplicate', [False, True])
def test_key_spill_full_merge_and_late_duplicate(tmp_path, duplicate):
    # Exceed the real 100k in-memory prefix. A duplicate at the very end must
    # still find a key first seen before the disk transition and block commit.
    target, source = str(tmp_path / 'target.lance'), str(tmp_path / 'patch.lance')
    n = 100_010
    old = lance.write_dataset(pa.table({'id': range(n), 'value': ['old'] * n, 'keep': range(n)}), target)
    ids = [*range(n), 0] if duplicate else range(n)
    lance.write_dataset(pa.table({'id': ids, 'value': ['new'] * len(ids)}), source)
    patch = data.read_lance(source, version=1, batch_size=8192)
    if duplicate:
        with pytest.raises(InvalidLanceRequest, match='unique'):
            patch.write_lance(target, mode='merge', on='id', update_columns=['value'])
        assert lance.dataset(target).version == old.version
    else:
        receipt = patch.write_lance(target, mode='merge', on='id', update_columns=['value'], return_receipt=True)
        assert receipt.merge_stats['updated_rows'] == n
        current = lance.dataset(target).to_table().sort_by('id')
        assert current['value'].to_pylist() == ['new'] * n
        assert current['keep'].to_pylist() == list(range(n))


def test_disk_keys_preserve_composite_binary_identity_and_cleanup(tmp_path, monkeypatch):
    import demiflow.lance.mutate as mutation
    monkeypatch.setattr(mutation, '_KEY_MEMORY_ROWS', 1)
    keys = mutation._UniqueMergeKeys()
    try:
        keys.check(pa.table({'a': [b'a', b'ab'], 'b': [b'bc', b'c']}), ['a', 'b'])
        directory = keys.directory.name
        with pytest.raises(InvalidLanceRequest, match='unique'):
            keys.check(pa.table({'a': [b'a'], 'b': [b'bc']}), ['a', 'b'])
    finally:
        keys.close()
    from pathlib import Path
    assert not Path(directory).exists()


def test_large_reservation_excludes_other_native_jobs(tmp_path, monkeypatch):
    import demiflow.execution.datafusion as native
    monkeypatch.setattr(native, '_cgroup_memory', lambda: None)
    monkeypatch.setattr(native, '_cpu_capacity', lambda: 4)
    options = native.DataFusionOptions(memory_bytes=16 * 1024**2, max_rss_bytes=64 * 1024**2,
                                      threads=1, admission_timeout_s=0.1)
    with native.DataFusionSession(resource_directory=tmp_path, options=options, max_concurrent=2) as session:
        with session._admit(slots_required=2) as (_, locks):
            assert len(locks) == 2
            with pytest.raises(TimeoutError, match='admission'):
                with session._admit():
                    pytest.fail('A second job entered the reserved capacity')
        with session._admit():
            pass  # Both locks are released when the larger job leaves.
