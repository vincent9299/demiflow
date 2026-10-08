"""Run lifecycle journal: manifest, committed stages and explicit empty reset.

SQLite stores control-plane records; DatasetRef points at business data in Lance.
All mutations share the run's nonblocking writer lock. No arbitrary key/value API.
"""
from pathlib import Path
import json
import sqlite3
import time
from contextlib import contextmanager

from .artifacts import run_lock, encoded


class RunJournal:
    def __init__(self, directory):
        self.directory = Path(directory)
        self.path = self.directory / 'run.sqlite'

    def _connect(self):
        self.directory.mkdir(parents=True, exist_ok=True)
        db = sqlite3.connect(self.path, timeout=30)
        db.execute('PRAGMA synchronous=FULL')
        db.executescript('''
          CREATE TABLE IF NOT EXISTS manifest (singleton INTEGER PRIMARY KEY CHECK(singleton=1), document TEXT NOT NULL);
          CREATE TABLE IF NOT EXISTS stages (name TEXT PRIMARY KEY, fingerprint TEXT NOT NULL, dataset_ref TEXT NOT NULL);
          CREATE TABLE IF NOT EXISTS completion (singleton INTEGER PRIMARY KEY CHECK(singleton=1), document TEXT NOT NULL);
          CREATE TABLE IF NOT EXISTS activities (kind TEXT NOT NULL, identity TEXT NOT NULL, PRIMARY KEY(kind, identity));
          CREATE TABLE IF NOT EXISTS resets (id INTEGER PRIMARY KEY, actor TEXT NOT NULL, reason TEXT NOT NULL, time REAL NOT NULL, old_manifest TEXT NOT NULL);
        ''')
        return db

    @contextmanager
    def _transaction(self):
        db = self._connect()
        try:
            with db:
                yield db
        finally:
            db.close()

    def initialize(self, manifest):
        value = encoded(manifest).decode()
        with run_lock(self.directory), self._transaction() as db:
            db.execute('BEGIN IMMEDIATE')
            old = db.execute('SELECT document FROM manifest').fetchone()
            if old and old[0] != value:
                raise ValueError('Immutable manifest differs; use a new run or reset an empty run')
            db.execute('INSERT OR IGNORE INTO manifest VALUES (1, ?)', (value,))

    def stage(self, name, fingerprint, dataset_ref):
        value = encoded(dataset_ref).decode()
        with run_lock(self.directory), self._transaction() as db:
            db.execute('BEGIN IMMEDIATE')
            old = db.execute('SELECT fingerprint, dataset_ref FROM stages WHERE name=?', (name,)).fetchone()
            if old and old != (fingerprint, value):
                raise ValueError('Committed stage differs: ' + name)
            db.execute('INSERT OR IGNORE INTO stages VALUES (?, ?, ?)', (name, fingerprint, value))

    def activity(self, kind, identity):
        """Mark a nonempty run before a model call or external publication."""
        with run_lock(self.directory), self._transaction() as db:
            db.execute('INSERT OR IGNORE INTO activities VALUES (?, ?)', (kind, identity))

    def finish(self, state):
        with run_lock(self.directory), self._transaction() as db:
            db.execute('INSERT OR REPLACE INTO completion VALUES (1, ?)', (encoded(state).decode(),))

    def reset_empty(self, *, actor, reason):
        """Remove only a manifest with zero stages, activity, completion or files.

        Unknown files are evidence of possible work and block reset. Audit history
        survives; callers initialize the replacement manifest explicitly afterward.
        """
        if not actor.strip() or not reason.strip():
            raise ValueError('actor and reason are required')
        with run_lock(self.directory), self._transaction() as db:
            db.execute('BEGIN IMMEDIATE')
            for table in ('stages', 'activities', 'completion'):
                if db.execute(f'SELECT 1 FROM {table} LIMIT 1').fetchone():
                    raise ValueError('Run is not empty: ' + table)
            extras = [p for p in self.directory.rglob('*') if p.is_file()
                      and p.name not in {'.lock', 'run.sqlite', 'run.sqlite-journal'}]
            if extras:
                raise ValueError('Run has unaccounted artifacts: ' + str(extras[0]))
            old = db.execute('SELECT document FROM manifest').fetchone()
            if old is None:
                raise ValueError('Run has no manifest')
            cursor = db.execute('INSERT INTO resets(actor, reason, time, old_manifest) VALUES (?, ?, ?, ?)',
                                (actor, reason, time.time(), old[0]))
            db.execute('DELETE FROM manifest')
            receipt = {'reset_id': cursor.lastrowid, 'actor': actor, 'reason': reason}
        return receipt

    def inspect(self):
        db = self._connect()
        try:
            manifest = db.execute('SELECT document FROM manifest').fetchone()
            completion = db.execute('SELECT document FROM completion').fetchone()
            return {'manifest': json.loads(manifest[0]) if manifest else None,
                    'stages': {name: {'fingerprint': fingerprint, 'dataset_ref': json.loads(ref)}
                               for name, fingerprint, ref in db.execute('SELECT * FROM stages')},
                    'completion': json.loads(completion[0]) if completion else None,
                    'resets': [dict(zip(('id', 'actor', 'reason', 'time', 'old_manifest'), row))
                               for row in db.execute('SELECT * FROM resets')]}
        finally:
            db.close()
