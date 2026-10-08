"""Real temporary Lance tables: column ownership, schema evolution and commit races."""
import lance
import pyarrow as pa
import pytest

from demiflow import data
from demiflow.errors import InvalidLanceRequest, LanceWriteConflict, LanceWriteError
from demiflow.lance.model import LanceWriteReceipt


@pytest.fixture
def target(tmp_path):
    uri = str(tmp_path / 'target.lance')
    data.from_items([{'id': 1, 'value': 'old', 'other': 'keep'},
                     {'id': 2, 'value': 'second', 'other': 'untouched'}]).write_lance(uri)
    return uri


def patch(uri, rows, **kwargs):
    return data.from_items(rows).write_lance(uri, mode='merge', on='id',
        update_columns=['value'], return_receipt=True, **kwargs)


def test_partial_columns_and_old_versions(target):
    old = lance.dataset(target)
    receipt = patch(target, [{'id': 1, 'value': 'new'}], expected_version=old.version)
    assert receipt.committed_version == old.version + 1
    assert LanceWriteReceipt.from_dict(receipt.to_dict()) == receipt
    assert lance.dataset(target).to_table().sort_by('id').to_pylist() == [
        {'id': 1, 'value': 'new', 'other': 'keep'}, {'id': 2, 'value': 'second', 'other': 'untouched'}]
    assert old.to_table()['value'].to_pylist() == ['old', 'second']


@pytest.mark.parametrize('policy', ['error', 'ignore', 'insert'])
def test_unmatched_policy_and_atomicity(target, policy):
    old = lance.dataset(target).version
    rows = [{'id': 1, 'value': 'new'}, {'id': 3, 'value': 'added'}]
    if policy == 'error':
        with pytest.raises(InvalidLanceRequest, match='absent'):
            patch(target, rows)
        assert lance.dataset(target).version == old
        return
    receipt = patch(target, rows, when_not_matched=policy)
    assert receipt.merge_stats == {'updated_rows': 1, 'inserted_rows': int(policy == 'insert'),
                                   'ignored_rows': int(policy == 'ignore')}
    assert LanceWriteReceipt.from_dict(receipt.to_dict()) == receipt
    result = lance.dataset(target).to_table().sort_by('id').to_pylist()
    assert result[0]['other'] == 'keep'
    if policy == 'insert':
        assert result[-1] == {'id': 3, 'value': 'added', 'other': None}


@pytest.mark.parametrize('rows', [[{'id': 1, 'value': 'a'}, {'id': 1, 'value': 'b'}],
                                 [{'id': None, 'value': 'a'}]])
def test_invalid_keys_never_commit(target, rows):
    old = lance.dataset(target).version
    with pytest.raises(InvalidLanceRequest):
        patch(target, rows, schema=pa.schema([('id', pa.int64()), ('value', pa.string())]))
    assert lance.dataset(target).version == old


def test_explicit_null_and_empty_patch(target):
    receipt = patch(target, [{'id': 1, 'value': None}],
                    schema=pa.schema([('id', pa.int64()), ('value', pa.string())]))
    assert lance.dataset(target).to_table(filter='id = 1').to_pylist()[0] == {
        'id': 1, 'value': None, 'other': 'keep'}
    empty = patch(target, [])
    assert empty.committed_version == receipt.committed_version
    assert empty.written_rows == 0


def test_extra_columns_and_undeclared_schema_change_rejected(target):
    with pytest.raises(InvalidLanceRequest, match='update_columns'):
        patch(target, [{'id': 1, 'value': 'a', 'other': 'bad'}])
    with pytest.raises(InvalidLanceRequest, match='existing target'):
        data.from_items([{'id': 1, 'new': 'x'}]).write_lance(target, mode='merge', on='id')


def test_duplicate_target_keys_are_rejected(target):
    data.from_items([{'id': 1, 'value': 'duplicate', 'other': 'x'}]).write_lance(target)
    with pytest.raises(InvalidLanceRequest, match='not unique'):
        patch(target, [{'id': 1, 'value': 'x'}])


def test_merge_race_after_native_staging_is_not_rebased(target, monkeypatch):
    old = lance.dataset(target).version
    builder = type(lance.dataset(target).merge_insert('id'))
    execute = builder.execute_uncommitted
    def race(self, *args, **kwargs):
        result = execute(self, *args, **kwargs)
        data.from_items([{'id': 3, 'value': 'competitor', 'other': 'safe'}]).write_lance(target)
        return result
    monkeypatch.setattr(builder, 'execute_uncommitted', race)
    with pytest.raises(LanceWriteConflict):
        patch(target, [{'id': 1, 'value': 'should not commit'}], expected_version=old)
    assert lance.dataset(target).version == old + 1
    assert lance.dataset(target).to_table(filter='id = 1')['value'][0].as_py() == 'old'


def test_commit_response_loss_is_indeterminate(target, monkeypatch):
    commit = lance.LanceDataset.commit
    def lost(*args, **kwargs):
        commit(*args, **kwargs)
        raise OSError('lost commit response')
    monkeypatch.setattr(lance.LanceDataset, 'commit', lost)
    with pytest.raises(LanceWriteError) as error:
        patch(target, [{'id': 1, 'value': 'committed'}])
    assert error.value.receipt.status == 'indeterminate'
    assert lance.dataset(target).to_table(filter='id = 1')['value'][0].as_py() == 'committed'


def test_add_nullable_columns_then_fill_subset(target):
    old = lance.dataset(target)
    receipt = data.add_lance_columns(target, pa.schema([('label', pa.string())]), expected_version=old.version)
    assert receipt.committed_version == old.version + 1
    assert lance.dataset(target).to_table()['label'].to_pylist() == [None, None]
    data.from_items([{'id': 2, 'label': 'done'}]).write_lance(target, mode='merge', on='id',
        update_columns=['label'], expected_version=receipt.committed_version)
    assert old.schema.names == ['id', 'value', 'other']
    assert lance.dataset(target).to_table().sort_by('id')['label'].to_pylist() == [None, 'done']
    with pytest.raises(InvalidLanceRequest, match='existing'):
        data.add_lance_columns(target, pa.schema([('label', pa.string())]))


def test_schema_commit_race_preserves_competing_write(target, monkeypatch):
    old = lance.dataset(target).version
    add = lance.LanceDataset.add_columns
    def race(self, columns, *args, **kwargs):
        data.from_items([{'id': 3, 'value': 'competitor', 'other': 'safe'}]).write_lance(target)
        return add(self, columns, *args, **kwargs)
    monkeypatch.setattr(lance.LanceDataset, 'add_columns', race)
    with pytest.raises(LanceWriteConflict):
        data.add_lance_columns(target, pa.schema([('label', pa.string())]), expected_version=old)
    assert lance.dataset(target).schema.names == ['id', 'value', 'other']
    assert lance.dataset(target).version == old + 1


def test_nested_null_leaves_cast_without_losing_other_columns(tmp_path):
    uri = str(tmp_path / 'nested.lance')
    item = pa.struct([('label', pa.large_string()), ('missing', pa.string()),
                      ('support', pa.struct([('text', pa.large_string())]))])
    schema = pa.schema([('id', pa.int32()), ('items', pa.list_(item)), ('other', pa.string())])
    data.from_items([{'id': 1, 'items': [], 'other': 'keep'}]).write_lance(uri, schema=schema)
    rows = [{'id': 1, 'items': [{'label': 'a', 'missing': None, 'support': None},
                               {'label': 'b', 'missing': None, 'support': None}]}]
    data.from_items(rows).write_lance(uri, mode='merge', on='id', update_columns=['items'])
    assert lance.dataset(uri).to_table().to_pylist() == [{**rows[0], 'other': 'keep'}]
    old = lance.dataset(uri).version
    with pytest.raises(pa.ArrowInvalid):
        data.from_items([{'id': 1.5, 'items': []}]).write_lance(uri, mode='merge', on='id')
    assert lance.dataset(uri).version == old


def test_later_batch_extra_columns_are_not_silently_dropped(target):
    from demiflow.lance.model import LanceWriteSpec
    from demiflow.lance.write import write_lance
    old = lance.dataset(target).version
    with pytest.raises(InvalidLanceRequest, match='exactly'):
        write_lance(LanceWriteSpec(target, mode='merge', on='id', update_columns=['value']), [
            pa.table({'id': [1], 'value': ['ok']}),
            pa.table({'id': [2], 'value': ['no'], 'other': ['must reject']})])
    assert lance.dataset(target).version == old


def test_explicit_empty_update_allowlist_rejects_values(target):
    before = lance.dataset(target).version
    with pytest.raises(InvalidLanceRequest, match='update_columns'):
        data.from_items([{'id': 1, 'value': 'must not overwrite'}]).write_lance(
            target, mode='merge', on='id', update_columns=[])
    assert lance.dataset(target).version == before
