import fcntl
from pathlib import Path
import pytest
from demiflow.lance.control import control_directory, table_lock_path
from demiflow.lance.control_migration import migrate_local_controls


def test_control_relocation_preserves_receipt_and_refuses_active_writer(tmp_path):
    table = tmp_path / 'rows.lance'
    table.mkdir()
    old = Path(str(table) + '.demiflow-checkpoint.json')
    old.write_text('{"version": 1}')
    lock_path = Path(str(table) + '.demiflow-checkpoint.lock')
    with lock_path.open('a') as lock:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        with pytest.raises(BlockingIOError):
            migrate_local_controls(tmp_path)
        assert old.exists()
    assert len(migrate_local_controls(tmp_path)) == 2
    assert (control_directory(table) / 'checkpoint.json').read_text() == '{"version": 1}'
    assert table_lock_path(table).exists()
    assert not old.exists() and not lock_path.exists()
    assert migrate_local_controls(tmp_path) == []


def test_conflicting_receipt_and_orphans_fail(tmp_path):
    table = tmp_path / 'rows.lance'
    table.mkdir()
    old = Path(str(table) + '.demiflow-checkpoint.json')
    old.write_text('old')
    table_lock_path(table)
    (control_directory(table) / 'checkpoint.json').write_text('new')
    with pytest.raises(ValueError, match='Conflicting'):
        migrate_local_controls(tmp_path)
    assert old.read_text() == 'old'
    old.unlink()
    (tmp_path / 'gone.lance.demiflow-checkpoint.json').write_text('{}')
    with pytest.raises(ValueError, match='without a live table'):
        migrate_local_controls(tmp_path)
