import json
from pathlib import Path

import pytest

from demiflow.execution.storage.artifacts import resolve_local_artifact
from demiflow.operator_llm.sqlite_offline import externalize, restore


def test_object_read_survives_optional_manifest_disappearing_after_stat(tmp_path, monkeypatch):
    from demiflow.objects import LocalObjectStore
    ref = LocalObjectStore(tmp_path / 'objects').put(b'body')
    manifest = tmp_path / '_demiflow/artifact_locations.json'
    manifest.parent.mkdir()
    manifest.write_text(json.dumps({'version': 1, 'files': {}}))
    original = Path.open
    attempts = []
    def opening(path, *args, **kwargs):
        # The manifest disappears after its stat, just before it is opened.
        if path == manifest:
            attempts.append(path)
            path.unlink()
        return original(path, *args, **kwargs)
    monkeypatch.setattr(Path, 'open', opening)
    assert ref.read(max_bytes=16) == b'body'
    assert attempts == [manifest]


def test_location_mapping_precedes_existing_path_and_only_unreadable_manifests_count_as_absent(tmp_path, monkeypatch):
    old, moved = tmp_path / 'old', tmp_path / 'new'
    old.write_bytes(b'old')
    moved.write_bytes(b'new')
    manifest = tmp_path / '_demiflow/artifact_locations.json'
    manifest.parent.mkdir()
    manifest.write_text(json.dumps({'version': 1, 'files': {'old': 'new'}}))
    assert resolve_local_artifact(old) == moved
    # A malformed manifest is an error, not an absent mapping.
    broken = tmp_path / 'broken'
    (broken / '_demiflow').mkdir(parents=True)
    (broken / '_demiflow/artifact_locations.json').write_text(json.dumps({'version': 2, 'files': {}}))
    with pytest.raises(ValueError, match='Invalid artifact location manifest'):
        resolve_local_artifact(broken / 'file')
    # An unreadable ancestor manifest counts as absent: a sandbox (Landlock) may allow
    # stat but deny open outside its read scope (7c4b06dd).
    original = Path.stat
    def stat(path, *args, **kwargs):
        if path == manifest:
            raise PermissionError('denied')
        return original(path, *args, **kwargs)
    monkeypatch.setattr(Path, 'stat', stat)
    assert resolve_local_artifact(old) == old.resolve()


def test_saved_offline_input_restores_after_exact_relocation(tmp_path):
    value = 'data:image/png;base64,aW1hZ2U='
    saved = externalize(value, tmp_path / 'old.inputs')
    old = saved['_demiflow_offline_input']['path']
    target = tmp_path / 'new.inputs'
    (tmp_path / 'old.inputs').rename(target)
    manifest = tmp_path / '_demiflow/artifact_locations.json'
    manifest.parent.mkdir()
    from pathlib import Path
    manifest.write_text(json.dumps({'version': 1, 'files': {
        str(Path(old).relative_to(tmp_path)): str((target / Path(old).name).relative_to(tmp_path))}}))
    assert restore(saved) == value
    assert saved['_demiflow_offline_input']['path'] == old
    (target / Path(old).name).write_bytes(b'changed')
    with pytest.raises(ValueError, match='digest mismatch'):
        restore(saved)


def test_artifact_mapping_rejects_escape_and_does_not_guess(tmp_path):
    manifest = tmp_path / '_demiflow/artifact_locations.json'
    manifest.parent.mkdir()
    manifest.write_text(json.dumps({'version': 1, 'files': {'old.sqlite': '../outside.sqlite'}}))
    with pytest.raises(ValueError, match='Invalid relocated'):
        resolve_local_artifact(tmp_path / 'old.sqlite')
    assert resolve_local_artifact(tmp_path / 'unmapped.sqlite') == tmp_path / 'unmapped.sqlite'


def test_moved_offline_journal_reuses_original_request_bytes(tmp_path, monkeypatch):
    from demiflow.operator_llm.sqlite_offline import materialize
    from demiflow.operator_llm.sqlite_journal import SQLitePromptJournal
    record = {'request_sha256': 'fixed', 'image': 'data:image/png;base64,aW1hZ2U='}
    monkeypatch.setattr('demiflow.operator_llm.sqlite_offline.request_record', lambda _: dict(record))
    old = tmp_path / 'old/calls.sqlite'
    _, ref = materialize(None, {'path': str(old)})
    journal = SQLitePromptJournal(old)
    before = journal._connect().execute('SELECT request_json FROM calls').fetchone()[0]
    journal.close()
    (tmp_path / 'old').rename(tmp_path / 'new')
    manifest = tmp_path / '_demiflow/artifact_locations.json'
    manifest.parent.mkdir()
    files = {'old/calls.sqlite': 'new/calls.sqlite'}
    for file in (tmp_path / 'new/calls.inputs').iterdir():
        files['old/calls.inputs/' + file.name] = 'new/calls.inputs/' + file.name
    manifest.write_text(json.dumps({'version': 1, 'files': files}))
    current, new_ref = materialize(None, {'path': str(old)})
    assert ref.read() == current == record == new_ref.read()
    journal = SQLitePromptJournal(old)
    assert journal._connect().execute('SELECT request_json FROM calls').fetchall() == [(before,)]
    journal.close()
