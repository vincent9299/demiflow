"""Host decisions over native read facts: absence never releases ownership."""
from contextlib import closing
import hashlib
import json
import sqlite3

import pytest

from demiflow.forge.schema import ForgeError
from demiflow.forge.runs import domain, requests, manager

from test_forge_history_admission import run_fixture, private_domain  # noqa: F401


def test_owner_slot_needs_terminal_attempt_and_unchanged_real_path(tmp_path, monkeypatch):
    target, journal = run_fixture(tmp_path)
    owner = dict(root=str(tmp_path), run_id=target.name, attempt=1)
    state = dict(status='succeeded', execution_stopped=True, cleanup='complete', attempt=1)
    with closing(sqlite3.connect(target / 'run.sqlite')) as db:
        db.execute('UPDATE attempts SET document=? WHERE attempt=1', (json.dumps(state),))
        db.commit()
    monkeypatch.setattr(manager, 'execution_stopped', lambda target, state: True)
    before = hashlib.sha256((target / 'run.sqlite').read_bytes()).digest()
    assert domain._owner_finished(owner)
    assert hashlib.sha256((target / 'run.sqlite').read_bytes()).digest() == before
    moved = target.with_name('original-journal')
    target.rename(moved)
    target.symlink_to(moved, target_is_directory=True)
    assert domain._owner_finished(owner) is False
    target.unlink()
    assert domain._owner_finished(owner) is False


def test_incomplete_preparation_reads_without_initializing_or_erasing_evidence(tmp_path):
    target = tmp_path / 'run'; target.mkdir()
    path = target / 'run.sqlite'; path.touch()
    assert requests._incomplete_seal(target, 'request', 'run', tmp_path)
    assert path.read_bytes() == b''
    with closing(sqlite3.connect(path)) as db:
        db.execute('CREATE TABLE execution_state(document TEXT)')
        db.execute('INSERT INTO execution_state VALUES (?)', (json.dumps(dict(status=None, revision=0, attempt=1)),))
        db.execute('CREATE TABLE manifest(document TEXT)')
        db.execute('INSERT INTO manifest VALUES (?)', (json.dumps(dict(prepare_request_id='request', run_id='run', root=str(tmp_path))),))
        db.commit()
    before = path.read_bytes()
    assert requests._incomplete_seal(target, 'request', 'run', tmp_path)
    assert path.read_bytes() == before
    with pytest.raises(ForgeError, match='ownership differs'):
        requests._incomplete_seal(target, 'foreign', 'run', tmp_path)
    with closing(sqlite3.connect(path)) as db:
        db.execute('CREATE TABLE action_events(document TEXT)')
        db.execute("INSERT INTO action_events VALUES ('{}')")
        db.commit()
    before = path.read_bytes()
    with pytest.raises(ForgeError, match='execution facts'):
        requests._incomplete_seal(target, 'request', 'run', tmp_path)
    assert path.read_bytes() == before


@pytest.mark.parametrize('table', ['execution_state', 'manifest'])
def test_a_preparation_whose_journal_holds_a_non_text_document_stays_pending(tmp_path, table):
    """The gate before a run directory is deleted: a document the engine could not have written is not proof.

    Everything else about this journal says "prepared, never started, owned by this request". Only the storage
    class of one document is wrong. The answer must be a refusal, which leaves the request pending, never True.
    """
    target = tmp_path / 'run'; target.mkdir()
    path = target / 'run.sqlite'
    documents = {'execution_state': dict(status=None, revision=0, attempt=1),
                 'manifest': dict(prepare_request_id='request', run_id='run', root=str(tmp_path))}
    with closing(sqlite3.connect(path)) as db:
        for name, value in documents.items():
            text = json.dumps(value)
            db.execute(f'CREATE TABLE {name}(document TEXT)')
            db.execute(f'INSERT INTO {name} VALUES (?)', (sqlite3.Binary(text.encode()) if name == table else text,))
        db.commit()
    before = path.read_bytes()
    with pytest.raises(ForgeError):
        requests._incomplete_seal(target, 'request', 'run', tmp_path)
    assert path.read_bytes() == before and target.exists()
