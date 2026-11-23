"""Object recovery observes bounded native namespaces without writer authority."""
import hashlib
import json
import os
import sqlite3

import pytest

from demiflow.execution.journal import run_reader
from demiflow.execution.publication_facts import object_namespace
from demiflow.execution.recovery_contract import RecoveryContractError


def fixture(tmp_path, count=1):
    target = tmp_path / 'run'
    target.mkdir()
    owner = 'a' * 32
    records = [object_namespace(owner, str(tmp_path / ('objects-' + str(i)))) for i in range(count)]
    with sqlite3.connect(target / 'run.sqlite') as db:
        db.execute('CREATE TABLE runtime_identity(document TEXT)')
        db.execute('INSERT INTO runtime_identity VALUES (?)', (json.dumps({'run_id': owner}),))
        db.execute('CREATE TABLE runtime_object_roots(root_id TEXT,logical_root TEXT,physical_root TEXT,run_id TEXT)')
        db.executemany('INSERT INTO runtime_object_roots VALUES (?,?,?,?)',
                       [tuple(r[k] for k in ('root_id','logical_root','physical_root','run_id')) for r in records])
    return target, records


def digests(target):
    return {p.name: hashlib.sha256(p.read_bytes()).hexdigest() for p in target.iterdir() if p.is_file()}


def test_bounded_registry_read_does_not_write_or_grant_authority(tmp_path):
    target, records = fixture(tmp_path, 64)
    before = digests(target)
    fds = len(os.listdir('/proc/self/fd'))
    reader = run_reader._ReadonlyJournal(target)
    assert reader.object_roots() == sorted(records, key=lambda r: r['root_id'])
    assert not hasattr(reader, 'register_object_root') and not hasattr(reader, 'execution_guard')
    with reader._connection() as db:
        with pytest.raises(sqlite3.OperationalError):
            db.execute('DELETE FROM runtime_object_roots')
    assert digests(target) == before and len(os.listdir('/proc/self/fd')) <= fds


@pytest.mark.parametrize('mutation,match', [
    ("UPDATE runtime_object_roots SET logical_root='" + 'x'*4097 + "'", 'byte budget'),
    ("UPDATE runtime_object_roots SET logical_root=x'00'", 'byte budget'),
    ("UPDATE runtime_object_roots SET run_id='" + 'b'*32 + "'", 'namespace differs'),
    ("UPDATE runtime_identity SET document='{}'", 'invalid native run identity'),
    ("DELETE FROM runtime_identity", 'namespace differs'),
])
def test_invalid_registry_cannot_become_an_empty_success(tmp_path, mutation, match):
    target, _ = fixture(tmp_path)
    with sqlite3.connect(target / 'run.sqlite') as db:
        db.execute(mutation)
    before = digests(target)
    with pytest.raises(RecoveryContractError, match=match):
        run_reader._ReadonlyJournal(target).object_roots()
    assert digests(target) == before


def test_root_count_ceiling_and_absent_legacy_table(tmp_path):
    target, _ = fixture(tmp_path, 65)
    with pytest.raises(RecoveryContractError, match='count exceeds'):
        run_reader._ReadonlyJournal(target).object_roots()
    with sqlite3.connect(target / 'run.sqlite') as db:
        db.execute('DROP TABLE runtime_object_roots')
    before = digests(target)
    assert run_reader._ReadonlyJournal(target).object_roots() == []
    assert digests(target) == before


def test_owner_and_roots_share_one_snapshot(tmp_path, monkeypatch):
    target, records = fixture(tmp_path)
    writer = sqlite3.connect(target / 'run.sqlite')
    writer.execute('PRAGMA journal_mode=WAL')
    original = run_reader._read_object_roots
    changed = []
    def concurrent(db):
        result = original(db)
        if not changed:
            changed.append(True)
            writer.execute("UPDATE runtime_identity SET document=?", (json.dumps({'run_id': 'b'*32}),))
            writer.commit()
        return result
    monkeypatch.setattr(run_reader, '_read_object_roots', concurrent)
    try:
        assert run_reader._ReadonlyJournal(target).object_roots() == records
        with pytest.raises(RecoveryContractError, match='namespace differs'):
            run_reader._ReadonlyJournal(target).object_roots()
    finally:
        writer.close()


def test_deadline_symlink_and_replacement_keep_read_boundary(tmp_path, monkeypatch):
    target, _ = fixture(tmp_path)
    before = digests(target)
    with pytest.raises(run_reader.RunReadError, match='deadline'):
        run_reader._ReadonlyJournal(target, deadline=0).object_roots()
    link = tmp_path / 'linked'
    link.symlink_to(target, target_is_directory=True)
    with pytest.raises(run_reader.RunReadError, match='Symlink'):
        run_reader._ReadonlyJournal(link).object_roots()
    original = run_reader._read_object_roots
    def replace(db):
        result = original(db)
        (target / 'run.sqlite').rename(target / 'saved.sqlite')
        (target / 'run.sqlite').write_bytes((target / 'saved.sqlite').read_bytes())
        return result
    monkeypatch.setattr(run_reader, '_read_object_roots', replace)
    fds = len(os.listdir('/proc/self/fd'))
    with pytest.raises(run_reader.RunReadError, match='identity changed'):
        run_reader._ReadonlyJournal(target).object_roots()
    assert len(os.listdir('/proc/self/fd')) <= fds
    assert hashlib.sha256((target / 'saved.sqlite').read_bytes()).hexdigest() == before['run.sqlite']
