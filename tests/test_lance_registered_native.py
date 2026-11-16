"""Registry writes use the native action's original receipt and recovery only."""
import json
import shutil

import lance
import pyarrow as pa
import pytest

from demiflow.execution import RunContext
from demiflow.execution.run_context import current_run
from demiflow.execution.run_journal import RunJournal
from demiflow.lance.control import control_directory
from demiflow.lance.registry import Catalog, write_registered_table

SCHEMA = pa.schema([('id', pa.int64())])


def original_rows():
    yield {'id': 1}
    yield {'id': 2}


def refusing_rows():
    raise AssertionError('a confirmed registration must not execute the new factory')
    yield


def registered(root, factory=original_rows, **kwargs):
    return write_registered_table(root, 'rows.lance', schema=SCHEMA,
        schema_name='ids', schema_version='1', rows_factory=factory,
        fingerprint='original-input-v1', **kwargs)


def original_journal(root):
    control = control_directory(str(root / 'rows.lance'))
    pointer = json.loads((control / 'registered-native.json').read_text())
    assert set(pointer) == {'schema', 'run_id', 'directory', 'relative', 'action_id'}
    path = control / pointer['directory'] if pointer['relative'] else pointer['directory']
    return pointer, RunJournal(path)


@pytest.mark.parametrize('root,relative', [
    ('s3://bucket', 'rows.lance'), ('gs://bucket', 'rows.lance'),
    ('file://foreign/tmp/data', 'rows.lance'),
    (None, 's3://bucket/rows.lance'),
])
def test_remote_registration_paths_reject_before_source_or_local_files(tmp_path, monkeypatch, root, relative):
    from demiflow.errors import InvalidLanceRequest
    monkeypatch.chdir(tmp_path)
    with pytest.raises(InvalidLanceRequest):
        write_registered_table(root or tmp_path/'local', relative,
            schema=SCHEMA, schema_name='ids', schema_version='1',
            rows_factory=refusing_rows, fingerprint='remote-path', register=False)
    assert list(tmp_path.iterdir()) == []


def test_registered_file_uri_resolves_to_the_same_local_root(tmp_path):
    root = tmp_path/'local'
    ref, rows, replayed = registered(root.as_uri(), register=False)
    assert rows == 2 and not replayed
    assert ref.open(root).to_table().to_pylist() == [{'id': 1}, {'id': 2}]


def test_catalog_failure_projects_original_ack_and_survives_move(tmp_path, monkeypatch):
    root = tmp_path / 'old'; root.mkdir()
    original = Catalog.register
    def fail(*args, **kwargs):
        raise RuntimeError('catalog unavailable after native ACK')
    monkeypatch.setattr(Catalog, 'register', fail)
    with pytest.raises(RuntimeError, match='catalog unavailable after native ACK'):
        registered(root)
    pointer, journal = original_journal(root)
    commits = journal.execution_commits(pointer['action_id'])
    assert len(commits) == 1 and commits[0]['status'] == 'acked'
    assert journal.source_binding(pointer['action_id'] + '-registered-write')['fingerprint'] == 'original-input-v1'
    assert pointer['relative'] and pointer['directory'] == 'registered-run'
    assert lance.dataset(str(root / 'rows.lance')).version == 1
    monkeypatch.setattr(Catalog, 'register', original)
    moved = tmp_path / 'moved'; shutil.move(str(root), str(moved))
    ref, rows, replayed = registered(moved, refusing_rows)
    assert rows == 2 and replayed and ref.lance_version == 1
    assert ref.relative_uri == 'rows.lance'
    assert original_journal(moved)[1].execution_commits(pointer['action_id']) == commits
    assert Catalog(moved).latest(ref.dataset_id) == ref
    assert lance.dataset(ref.resolve(moved), version=ref.lance_version).to_table().to_pylist() == [{'id': 1}, {'id': 2}]


def test_active_run_is_reused_and_remains_open(tmp_path):
    root = tmp_path / 'datasets'; root.mkdir()
    with RunContext.start(tmp_path / 'owning-run') as run:
        ref, rows, replayed = registered(root)
        pointer, journal = original_journal(root)
        assert not pointer['relative'] and pointer['directory'] == str(run.directory)
        assert pointer['run_id'] == run.ref.run_id
        assert current_run() is run and run.active
        assert rows == 2 and not replayed
        assert journal.execution_action(pointer['action_id'])['declaration']['kind'] == 'write_table'
        assert journal.latest_execution_commit(pointer['action_id'])['status'] == 'acked'
        # A read of already confirmed facts does not stage this different code.
        assert registered(root, refusing_rows) == (ref, rows, True)
        assert current_run() is run
    assert current_run() is None


@pytest.mark.parametrize('changed', [False, True])
def test_unfinished_registration_uses_original_recovery_and_code_pin(tmp_path, monkeypatch, changed):
    import demiflow.execution.commit as protocol
    from demiflow.execution.recovery_contract import RecoveryContractError
    from demiflow.execution.contracts.dataflow_errors import DataflowError
    def stop(name, commit_id):
        if name == 'after_prepared':
            raise RuntimeError('prepared boundary')
    monkeypatch.setattr(protocol, 'fault_point', stop)
    with pytest.raises(RuntimeError, match='prepared boundary'):
        registered(tmp_path)
    pointer, journal = original_journal(tmp_path)
    first = journal.execution_commits(pointer['action_id'])
    assert first[-1]['status'] == 'prepared' and not first[-1]['receipts']
    assert not (tmp_path / 'rows.lance').exists()
    monkeypatch.setattr(protocol, 'fault_point', lambda *args: None)
    if changed:
        with pytest.raises((RecoveryContractError, DataflowError), match='changed|differs|identity|code|declaration'):
            registered(tmp_path, refusing_rows)
        assert journal.execution_commits(pointer['action_id']) == first
        assert not (tmp_path / 'rows.lance').exists()
    else:
        ref, rows, replayed = registered(tmp_path)
        assert rows == 2 and not replayed and ref.lance_version == 1
        assert len(journal.execution_commits(pointer['action_id'])) == 1
        assert journal.latest_execution_commit(pointer['action_id'])['status'] == 'acked'
        assert lance.dataset(str(tmp_path / 'rows.lance')).to_table().to_pylist() == [{'id': 1}, {'id': 2}]


def test_same_schema_count_version_foreign_table_cannot_replace_original_publication(tmp_path):
    from demiflow.execution.recovery_contract import RecoveryContractError
    ref, rows, replayed = registered(tmp_path)
    pointer, journal = original_journal(tmp_path)
    before = journal.execution_commits(pointer['action_id'])
    original = tmp_path / 'original.lance'
    (tmp_path / 'rows.lance').rename(original)
    lance.write_dataset(pa.table({'id': [101, 102]}, schema=SCHEMA), str(tmp_path / 'rows.lance'))
    foreign = lance.dataset(str(tmp_path / 'rows.lance'))
    assert foreign.version == ref.lance_version and foreign.count_rows() == rows
    with pytest.raises(RecoveryContractError, match='publication identity differs'):
        registered(tmp_path, refusing_rows)
    assert foreign.to_table().to_pylist() == [{'id': 101}, {'id': 102}]
    assert journal.execution_commits(pointer['action_id']) == before
    # Restore the original native version at another physical location. Its
    # transaction marker, not the filesystem inode or absolute path, is proof.
    (tmp_path / 'rows.lance').rename(tmp_path / 'foreign.lance')
    original.rename(tmp_path / 'rows.lance')
    assert registered(tmp_path, refusing_rows) == (ref, rows, True)
