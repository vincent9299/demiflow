"""SQLite allocation admission uses locked page geometry and the original run."""
from pathlib import Path
import sqlite3

import pytest

from demiflow import data
from demiflow import _kv_sink
from demiflow.execution.run_context import RunContext, current_run
from demiflow.kv import SQLiteKV


@pytest.mark.parametrize('phase', ['prepare', 'commit'])
@pytest.mark.parametrize('grow', [False, True])
def test_locked_page_geometry_and_concurrent_growth_are_reserved(tmp_path, monkeypatch, phase, grow):
    store = SQLiteKV(tmp_path/'index.sqlite', 'test')
    store.put_many([('history', 'keep')])
    with sqlite3.connect(store.path) as db:
        db.execute('PRAGMA page_size=65536')
        db.execute('VACUUM')
        assert db.execute('PRAGMA page_size').fetchone()[0] == 65536
    connection = _kv_sink._connection
    writers = []
    audits = []

    class AuditedConnection:
        def __init__(self, db):
            self.db = db
        def __enter__(self):
            self.db.__enter__()
            return self
        def __exit__(self, *args):
            return self.db.__exit__(*args)
        def close(self):
            self.db.close()
        def execute(self, sql, *args):
            if sql.startswith(('CREATE TABLE', 'INSERT INTO')):
                assert self.db.in_transaction
                page_size = self.db.execute('PRAGMA page_size').fetchone()[0]
                page_count = self.db.execute('PRAGMA page_count').fetchone()[0]
                # Check the reservation before any SQLite mutation. The old
                # fixed 32 KiB/row allowance and lock-free size snapshot fail.
                lower_bound = 2 * max(_kv_sink._target_bytes(Path(store.path)), page_size * page_count) + 64 * page_size
                with current_run().journal._connection() as run_db:
                    reserved = run_db.execute("SELECT bytes FROM runtime_artifacts WHERE path LIKE 'scratch/kv-%'").fetchall()
                assert len(reserved) == 1 and reserved[0][0] >= lower_bound
                audits.append((page_size, page_count, reserved[0][0]))
            return self.db.execute(sql, *args)

    def raced(store_arg, run, *, readonly=False):
        if readonly:
            return connection(store_arg, run, readonly=True)
        writers.append(True)
        chosen = len(writers) == (1 if phase == 'prepare' else 2)
        if chosen and grow:
            # An independent real SQLite transaction grows the target after
            # _target_bytes was observed and before this driver takes its lock.
            store.put_many([('external-'+str(i), 'x'*16384) for i in range(128)])
        db = connection(store_arg, run)
        return AuditedConnection(db) if chosen else db

    monkeypatch.setattr(_kv_sink, '_connection', raced)
    with RunContext.start(tmp_path/'run'), data.local_execution(workers=1, worker_mode='thread'):
        data.from_items([{'key':'new','value':1}]).write_datasink(store.datasink())
    assert audits and all(item[0] == 65536 for item in audits)
    assert store.get('history')['value'] == 'keep' and store.get('new')['value'] == 1
    if grow:
        assert store.get('external-127')['value'] == 'x'*16384
