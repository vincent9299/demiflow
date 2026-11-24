"""``execution.run_reader`` read policy: bounded, read-only, no schema creation."""
import hashlib
import json
import os
from pathlib import Path
import shutil
import sqlite3
import subprocess
import sys

import pytest

from demiflow.execution.journal import run_reader
from demiflow.execution.journal.run_reader import RunReadError, open_journal, read_run
from test_forge_history_admission import private_domain, run_fixture  # noqa: F401


def _digests(directory):
    return {path.name: hashlib.sha256(path.read_bytes()).hexdigest()
            for path in sorted(Path(directory).iterdir()) if path.is_file()}


def _row(target, sql, *parameters):
    db = sqlite3.connect((target / 'run.sqlite').resolve().as_uri() + '?mode=ro', uri=True)
    try:
        return db.execute(sql, parameters).fetchone()
    finally:
        db.close()


@pytest.mark.parametrize('extra', [0, 1])
def test_training_provenance_combined_byte_limit(tmp_path, extra):
    from demiflow.execution.contracts.recovery_contract import MAX_DOCUMENT_BYTES, document

    table = dict(attempt=1, action_id='scores', occurrence='predict', provenance_id='owner',
                 instance='model', manifest_digest='manifest')
    provenance = {'uri': '图'}
    expected = dict(table=table, provenance=provenance)
    provenance['uri'] += 'x' * (MAX_DOCUMENT_BYTES - len(document(expected)) + extra)
    owner = dict(instance='model', manifest='manifest', provenance=provenance)
    with sqlite3.connect(tmp_path / 'run.sqlite') as db:
        db.execute('CREATE TABLE runtime_sources (action_id TEXT PRIMARY KEY, document TEXT)')
        db.executemany('INSERT INTO runtime_sources VALUES (?, ?)', [
            (run_reader.fit_table_prefix(1, 'scores') + 'predict', document(table).decode()),
            ('owner', document(owner).decode()),
        ])
    with read_run(tmp_path) as reader:
        if extra:
            with pytest.raises(RunReadError) as refused:
                reader.training_provenance(1, 'scores', 'predict')
            assert refused.value.code == 'resource_limit'
        else:
            assert reader.training_provenance(1, 'scores', 'predict') == expected


def test_every_read_leaves_the_journal_byte_identical(tmp_path):
    target, _ = run_fixture(tmp_path)
    before = _digests(target)
    with read_run(target) as reader:
        state, current = reader.state()
        reader.tables(), reader.has_table('attempts'), reader.has_rows('action_events')
        reader.manifest_identity(), reader.state_projection(), reader.attempt_state(current)
        reader.document('execution_state', 65536), reader.runtime_summary()
        if reader.has_table('node_observations') and reader.observations_supported():
            reader.observation_revision(current), reader.observation_scope(current)
            list(reader.observation_rows(current, limit=8)), reader.observation_summaries(current)
        reader.action_sequence(current), reader.output_facts(current, 'action-1')
        if reader.control_sequence(current) is not None:
            list(reader.control_events(current))
        with pytest.raises(sqlite3.OperationalError):
            reader.db.execute('CREATE TABLE forged(value)')
        with pytest.raises(sqlite3.OperationalError):
            reader.db.execute("UPDATE execution_state SET document='{}'")
    run_reader.storage_snapshot(target)
    assert _digests(target) == before


def test_state_readers_return_the_recorded_documents(tmp_path):
    target, _ = run_fixture(tmp_path)
    recorded = json.loads(_row(target, 'SELECT document FROM execution_state')[0])
    with read_run(target) as reader:
        assert reader.state() == (recorded, recorded['attempt'])
        assert reader.state(recorded['attempt']) == (recorded, recorded['attempt'])
        assert reader.document('execution_state', 65536) == recorded
        selected, current = reader.state_projection()
        assert selected == current and all(selected[key] == recorded.get(key) for key in selected)
        row = _row(target, 'SELECT document FROM attempts WHERE attempt=?', recorded['attempt'])
        assert reader.attempt_state(recorded['attempt']) == (json.loads(row[0]) if row else None)
        assert reader.attempt_state(31) is None
        with pytest.raises(RunReadError) as unknown:
            reader.state(31)
        assert unknown.value.code == 'not_found'
        with pytest.raises(RunReadError) as unknown:
            reader.state_projection(31)
        assert unknown.value.code == 'not_found'
        for invalid in (0, 33, '1', True):
            with pytest.raises(RunReadError) as refused:
                reader.state(invalid)
            assert refused.value.code == 'invalid_request'
        with pytest.raises(RunReadError) as oversized:
            reader.document('execution_state', 4)
        assert oversized.value.code == 'resource_limit'
        for table in ('runtime_commits', 'sqlite_master', 'execution_state; DROP TABLE attempts'):
            with pytest.raises(ValueError):
                reader.document(table, 16)
            with pytest.raises(ValueError):
                reader.has_rows(table)


def test_open_refuses_symlinks_missing_oversized_and_irregular_journals(tmp_path):
    target, _ = run_fixture(tmp_path)
    open_journal(target).close()
    link = tmp_path / 'linked-run'
    link.symlink_to(target, target_is_directory=True)
    with pytest.raises(RunReadError, match='Symlink'):
        open_journal(link)
    with pytest.raises(RunReadError, match='unavailable'):
        open_journal(tmp_path / 'absent')
    copy = tmp_path / 'copy'
    shutil.copytree(target, copy)
    (copy / 'run.sqlite').unlink()
    (copy / 'run.sqlite').symlink_to(target / 'run.sqlite')
    with pytest.raises(RunReadError, match='Symlink'):
        open_journal(copy)
    (copy / 'run.sqlite').unlink()
    (copy / 'run.sqlite').mkdir()
    with pytest.raises(RunReadError, match='unavailable'):
        open_journal(copy)
    large = tmp_path / 'large'
    shutil.copytree(target, large)
    os.truncate(large / 'run.sqlite', run_reader.JOURNAL_BYTES + 1)
    with pytest.raises(RunReadError, match='exceeds budget'):
        open_journal(large)
    assert (large / 'run.sqlite').stat().st_size == run_reader.JOURNAL_BYTES + 1


def test_storage_snapshot_refuses_a_journal_replaced_during_open(tmp_path, monkeypatch):
    from demiflow.execution.contracts.recovery_contract import RecoveryContractError
    target, _ = run_fixture(tmp_path)
    original, changed = sqlite3.connect, []

    def connect(*args, **kwargs):
        if not changed:
            changed.append(True)
            (target / 'run.sqlite').rename(target / 'saved.sqlite')
            shutil.copyfile(target / 'saved.sqlite', target / 'run.sqlite')
        return original(*args, **kwargs)
    monkeypatch.setattr(sqlite3, 'connect', connect)
    descriptors = len(os.listdir('/proc/self/fd'))
    with pytest.raises(RecoveryContractError, match='identity changed'):
        run_reader.storage_snapshot(target)
    assert changed and len(os.listdir('/proc/self/fd')) == descriptors


def test_storage_snapshot_rejects_an_oversized_journal_before_any_sql(tmp_path, monkeypatch):
    from demiflow.execution.contracts.recovery_contract import RecoveryContractError
    target, _ = run_fixture(tmp_path)
    os.truncate(target / 'run.sqlite', run_reader.JOURNAL_BYTES + 1)
    monkeypatch.setattr(sqlite3, 'connect', lambda *args, **kwargs: pytest.fail('SQLite was reached'))
    with pytest.raises(RecoveryContractError, match='type/size exceeds budget'):
        run_reader.storage_snapshot(target)
    with pytest.raises(RunReadError, match='exceeds budget'):
        open_journal(target)


def test_reading_never_initializes_an_empty_journal(tmp_path):
    (tmp_path / 'run.sqlite').write_bytes(b'')
    with read_run(tmp_path, transaction=False) as reader:
        assert reader.tables() == set()
        assert reader.runtime_summary() is None and reader.control_sequence(1) is None
        assert reader.document('manifest', 16) is None and not reader.has_rows('stages')
    assert (tmp_path / 'run.sqlite').read_bytes() == b''
    assert sorted(path.name for path in tmp_path.iterdir() if path.name.startswith('run.sqlite')) == ['run.sqlite']


def test_a_deadline_interrupts_instead_of_waiting(tmp_path):
    target, _ = run_fixture(tmp_path)
    with pytest.raises(sqlite3.OperationalError, match='interrupted'):
        with read_run(target, deadline=0.0) as reader:
            reader.db.execute(
                'WITH RECURSIVE n(i) AS (SELECT 1 UNION ALL SELECT i+1 FROM n WHERE i<10000000) '
                'SELECT count(*) FROM n').fetchone()


@pytest.mark.parametrize('module', ['journal.run_reader', 'journal.sealed_plan_facts'])
def test_reader_modules_do_not_load_hosts(module):
    root = Path(__file__).resolve().parents[1]
    result = subprocess.run([sys.executable, '-I', '-B', '-c',
        'import sys,importlib;sys.path.insert(0,sys.argv[1]);importlib.import_module("demiflow.execution."+sys.argv[2]);'
        'assert not any(n.startswith(("demiflow.workbench","demiflow.forge")) for n in sys.modules)',
        str(root), module], capture_output=True, text=True, timeout=60)
    assert result.returncode == 0, result.stderr


def test_read_run_binds_identity_before_open_returns(tmp_path, monkeypatch):
    target, _ = run_fixture(tmp_path)
    original = run_reader.open_journal
    def replaced(*args, **kwargs):
        db = original(*args, **kwargs)
        (target / 'run.sqlite').rename(target / 'original.sqlite')
        shutil.copyfile(target / 'original.sqlite', target / 'run.sqlite')
        return db
    monkeypatch.setattr(run_reader, 'open_journal', replaced)
    descriptors = len(os.listdir('/proc/self/fd'))
    with pytest.raises(RunReadError, match='identity changed'):
        with read_run(target):
            pytest.fail('A replaced pathname reached the reader body')
    assert len(os.listdir('/proc/self/fd')) <= descriptors


def _minimal_journal(directory, *, binary=False, state=None, manifest=None, summary=None):
    """A journal with only the control tables under test. ``binary`` stores the documents as BLOB."""
    directory.mkdir(parents=True, exist_ok=True)
    wrap = (lambda text: sqlite3.Binary(text.encode())) if binary else (lambda text: text)
    with sqlite3.connect(directory / 'run.sqlite') as db:
        for table, value in (('execution_state', state), ('manifest', manifest), ('runtime_summary', summary)):
            if value is not None:
                db.execute(f'CREATE TABLE {table}(document TEXT)')
                db.execute(f'INSERT INTO {table} VALUES (?)', (wrap(json.dumps(value, ensure_ascii=False)),))
        db.execute('CREATE TABLE attempts(attempt INTEGER, document TEXT)')
        if state is not None:
            db.execute('INSERT INTO attempts VALUES (?,?)', (7, wrap(json.dumps({'attempt': 7, 'status': 'failed'}))))
    return directory


def test_a_control_document_must_be_text_and_fit_its_byte_budget(tmp_path):
    """The budget is counted in bytes inside SQLite, and a document of any other storage class is refused.

    The engine only ever writes TEXT. A BLOB-typed document is evidence that something else wrote the
    journal, and a host that decides whether a run directory may be deleted must not accept it.
    """
    state, manifest, summary = {'status': None, 'revision': 0, 'attempt': 1}, {'run_id': 'run'}, {'workers': 1}
    with read_run(_minimal_journal(tmp_path / 'text', state=state, manifest=manifest, summary=summary)) as reader:
        assert reader.document('execution_state', 65536) == state and reader.document('manifest', 1024) == manifest
        assert reader.state() == (state, 1) and reader.runtime_summary() == summary
        assert reader.state(7) == ({'attempt': 7, 'status': 'failed'}, 1)
    with read_run(_minimal_journal(tmp_path / 'blob', binary=True, state=state, manifest=manifest, summary=summary)) as reader:
        for read in (lambda: reader.document('execution_state', 65536), lambda: reader.document('manifest', 1024),
                     reader.state, lambda: reader.state(7), reader.runtime_summary):
            with pytest.raises(RunReadError):
                read()
    # 40 characters, 120 bytes: inside a 100-character budget, outside a 100-byte one.
    wide = {'k': '中' * 38}
    assert len(json.dumps(wide, ensure_ascii=False)) < 100 < len(json.dumps(wide, ensure_ascii=False).encode())
    with read_run(_minimal_journal(tmp_path / 'wide', manifest=wide)) as reader:
        with pytest.raises(RunReadError):
            reader.document('manifest', 100)
        assert reader.document('manifest', 200) == wide
