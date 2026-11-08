"""Transactional SQLite/PostgreSQL storage for the native task protocol.

Publication counters are ordinary locked rows, updated with the records they
publish. A rolled-back or stalled publisher cannot leave a visible prefix hole.
"""
from contextlib import contextmanager
import json
import math
from pathlib import Path
import sqlite3
import time
import threading
import uuid

from .queue_contract import queue_config
from .recovery_contract import RecoveryContractError, document


SCHEMA = (
    'CREATE TABLE IF NOT EXISTS queue_identity (id INTEGER PRIMARY KEY, queue_uuid TEXT NOT NULL, declaration TEXT NOT NULL)',
    '''CREATE TABLE IF NOT EXISTS queue_pools (
        pool TEXT PRIMARY KEY, generation TEXT NOT NULL UNIQUE, producer_id TEXT NOT NULL,
        source TEXT NOT NULL, expected_tasks BIGINT, epoch BIGINT NOT NULL, token TEXT NOT NULL,
        producer_contract TEXT, sealed INTEGER NOT NULL DEFAULT 0, eos INTEGER NOT NULL DEFAULT 0,
        source_position BIGINT NOT NULL DEFAULT 0, output_offset BIGINT NOT NULL DEFAULT 0,
        source_cursor TEXT, tasks BIGINT NOT NULL DEFAULT 0,
        digest TEXT NOT NULL, consumer_contract TEXT, deadline DOUBLE PRECISION NOT NULL)''',
    'CREATE TABLE IF NOT EXISTS queue_counts (generation TEXT NOT NULL,status TEXT NOT NULL,amount BIGINT NOT NULL,PRIMARY KEY(generation,status))',
    'CREATE TABLE IF NOT EXISTS queue_domain_counts (generation TEXT NOT NULL,domain_id TEXT NOT NULL,amount BIGINT NOT NULL,PRIMARY KEY(generation,domain_id))',
    '''CREATE TABLE IF NOT EXISTS queue_publications (
        generation TEXT NOT NULL, kind TEXT NOT NULL, ordinal BIGINT NOT NULL,
        PRIMARY KEY(generation,kind))''',
    '''CREATE TABLE IF NOT EXISTS queue_tasks (
        generation TEXT NOT NULL, task_id TEXT NOT NULL, fingerprint TEXT NOT NULL,
        payload TEXT NOT NULL, occurrence TEXT NOT NULL, ordinal BIGINT NOT NULL, producer_commit TEXT NOT NULL,
        status TEXT NOT NULL, epoch BIGINT NOT NULL DEFAULT 0, token TEXT, lease_until DOUBLE PRECISION,
        worker_id TEXT, dispatch_domain TEXT, artifact_domain TEXT,
        acquisition_scope TEXT, final_scope TEXT,
        PRIMARY KEY(generation,task_id), UNIQUE(generation,ordinal), UNIQUE(generation,occurrence))''',
    'CREATE INDEX IF NOT EXISTS queue_claimable ON queue_tasks(generation,status,lease_until,ordinal)',
    'CREATE INDEX IF NOT EXISTS queue_worker_inflight ON queue_tasks(worker_id,status)',
    '''CREATE TABLE IF NOT EXISTS queue_workers (
        worker_id TEXT PRIMARY KEY, generation TEXT NOT NULL, domain TEXT NOT NULL,
        consumer_contract TEXT NOT NULL, limits TEXT NOT NULL, started DOUBLE PRECISION NOT NULL, claimed BIGINT NOT NULL DEFAULT 0, settled BIGINT NOT NULL DEFAULT 0, exhausted BIGINT NOT NULL DEFAULT 0)''',
    '''CREATE TABLE IF NOT EXISTS queue_results (
        generation TEXT NOT NULL, task_id TEXT NOT NULL, commit_id TEXT NOT NULL UNIQUE,
        row_document TEXT NOT NULL, PRIMARY KEY(generation,task_id))''',
    '''CREATE TABLE IF NOT EXISTS queue_completions (
        generation TEXT NOT NULL, task_id TEXT NOT NULL, ordinal BIGINT NOT NULL,
        proof TEXT NOT NULL, PRIMARY KEY(generation,task_id), UNIQUE(generation,ordinal))''',
    '''CREATE TABLE IF NOT EXISTS queue_observations (
        generation TEXT NOT NULL, task_id TEXT NOT NULL, ordinal BIGINT NOT NULL,
        proof TEXT NOT NULL, PRIMARY KEY(generation,task_id), UNIQUE(generation,ordinal))''',
    '''CREATE TABLE IF NOT EXISTS runtime_commit_scopes (
        scope_id TEXT PRIMARY KEY, parent_action_id TEXT NOT NULL, declaration TEXT NOT NULL)''',
    '''CREATE TABLE IF NOT EXISTS runtime_commits (
        commit_id TEXT PRIMARY KEY, action_id TEXT NOT NULL, sequence BIGINT NOT NULL,
        status TEXT NOT NULL, intent TEXT NOT NULL, prepared TEXT, UNIQUE(action_id,sequence))''',
    'CREATE TABLE IF NOT EXISTS runtime_receipts (commit_id TEXT NOT NULL, sink_id TEXT NOT NULL, document TEXT NOT NULL, PRIMARY KEY(commit_id,sink_id))',
    'CREATE TABLE IF NOT EXISTS runtime_budget_guard (id INTEGER PRIMARY KEY)',
    'CREATE TABLE IF NOT EXISTS runtime_budgets (name TEXT PRIMARY KEY, consumed BIGINT NOT NULL, ceiling BIGINT NOT NULL)',
    'CREATE TABLE IF NOT EXISTS runtime_reservations (name TEXT NOT NULL, reservation_id TEXT NOT NULL, amount BIGINT NOT NULL, PRIMARY KEY(name,reservation_id))',
)


class SQL:
    def __init__(self, connection, backend):
        self.connection, self.backend = connection, backend

    def execute(self, statement, params=()):
        if self.backend == 'postgresql':
            statement = statement.replace('?', '%s')
        return self.connection.execute(statement, params)

    def lock(self, statement, *, skip=False):
        return statement + (' FOR UPDATE SKIP LOCKED' if skip else ' FOR UPDATE') if self.backend == 'postgresql' else statement

    def now(self):
        if self.backend == 'postgresql':
            return self.execute('SELECT EXTRACT(EPOCH FROM clock_timestamp())').fetchone()[0]
        return time.time()


class QueueDatabase:
    def __init__(self, config):
        self.config = queue_config(config)
        self.queue_uuid = None
        self._connections=threading.BoundedSemaphore(2)

    @contextmanager
    def connection(self, *, write=False, initialize=False):
        if not self._connections.acquire(timeout=self.config.limits.db_timeout_s):
            raise RecoveryContractError('Native queue database connection budget exhausted')
        try:
            with self._connection(write=write,initialize=initialize) as db:yield db
        finally:self._connections.release()

    @contextmanager
    def _connection(self, *, write=False, initialize=False):
        c = self.config
        if c.backend == 'sqlite':
            from .local_storage import require_local_storage
            path = Path(c.path)
            if initialize:
                from .raw_objects import _directory
                import os
                os.close(_directory(path.parent,create=True))
                require_local_storage(path.parent)
            if any(p.is_symlink() for p in (path,*path.parents)):
                raise RecoveryContractError('Queue database path contains symlink')
            db = sqlite3.connect(path if initialize else path.as_uri()+('?mode=rw' if write else '?mode=ro'),uri=not initialize,
                                 timeout=c.limits.db_timeout_s)
            try:
                db.execute('PRAGMA synchronous=FULL')
                db.execute('PRAGMA cache_size=-4096')
                if initialize:
                    db.execute('PRAGMA journal_mode=WAL')
                db.execute('PRAGMA wal_autocheckpoint=256')
                page_size = db.execute('PRAGMA page_size').fetchone()[0]
                if db.execute('PRAGMA page_count').fetchone()[0]*page_size>c.limits.max_control_bytes:
                    raise RecoveryContractError('Queue control store byte budget exceeded')
                if write or initialize:
                    db.execute('PRAGMA max_page_count='+str(c.limits.max_control_bytes//page_size))
                else:db.execute('PRAGMA query_only=ON')
                db.execute('BEGIN IMMEDIATE' if write else 'BEGIN')
                yield SQL(db,'sqlite')
                db.commit()
            except BaseException:
                db.rollback()
                raise
            finally:
                db.close()
        else:
            try:
                import psycopg
            except ImportError:
                raise ImportError('PostgreSQL task channels require demiflow[queue-postgresql]') from None
            # The DSN is resolved in memory and is never included in exceptions.
            try:
                db = psycopg.connect(c.dsn.resolve(),connect_timeout=math.ceil(c.limits.db_timeout_s))
            except psycopg.Error:
                raise RecoveryContractError('PostgreSQL queue connection failed') from None
            try:
                with db.transaction():
                    if not write:db.execute('SET TRANSACTION READ ONLY')
                    db.execute("SELECT set_config('statement_timeout',%s,true)",(str(int(c.limits.db_timeout_s*1000)),))
                    db.execute("SELECT set_config('lock_timeout',%s,true)",(str(int(c.limits.db_timeout_s*1000)),))
                    db.execute("SELECT set_config('idle_in_transaction_session_timeout',%s,true)",(str(int(c.limits.db_timeout_s*1000)),))
                    if initialize:
                        db.execute('CREATE SCHEMA IF NOT EXISTS '+c.namespace)
                    db.execute('SET LOCAL search_path TO '+c.namespace)
                    yield SQL(db,'postgresql')
            finally:
                db.close()

    def initialize(self):
        declaration = document(self.config.declaration(),65536).decode()
        if self.config.backend=='sqlite':
            path=Path(self.config.path)
            if any(p.is_symlink() for p in (path,*path.parents)):
                raise RecoveryContractError('Queue database path contains symlink')
            if path.exists():
                # Reject v1/unrelated archives before journal-mode, page-limit
                # or schema changes. A failed migration must preserve its bytes.
                with sqlite3.connect(path.as_uri()+'?mode=ro',uri=True) as existing:
                    names=existing.execute("SELECT name FROM sqlite_master WHERE type='table' LIMIT 129").fetchall()
                    if len(names)>128:raise RecoveryContractError('Existing queue schema exceeds native table budget')
                    tables={r[0] for r in names}
                    if tables and 'queue_identity' not in tables:
                        raise RecoveryContractError('Existing v1 or unrelated queue store cannot be migrated in place')
                    if 'queue_identity' in tables:
                        old=existing.execute('SELECT declaration FROM queue_identity WHERE id=1').fetchone()
                        if old is None or old[0]!=declaration:
                            raise RecoveryContractError('Queue identity or resource declaration changed')
        with self.connection(write=True,initialize=True) as db:
            if self.config.backend == 'sqlite':
                tables={r[0] for r in db.execute("SELECT name FROM sqlite_master WHERE type='table'")}
                if tables and 'queue_identity' not in tables:
                    raise RecoveryContractError('Existing v1 or unrelated queue store cannot be migrated in place')
            for statement in SCHEMA:
                db.execute(statement)
            from .raw_budget import schema
            schema(db)
            db.execute('INSERT INTO runtime_budget_guard VALUES (1) ON CONFLICT(id) DO NOTHING')
            db.execute('INSERT INTO queue_identity VALUES (1,?,?) ON CONFLICT(id) DO NOTHING',
                       (uuid.uuid4().hex,declaration))
            row = db.execute('SELECT queue_uuid,declaration FROM queue_identity WHERE id=1').fetchone()
            if row[1] != declaration:
                raise RecoveryContractError('Queue identity or resource declaration changed')
            self.queue_uuid = row[0]
        return self.queue_uuid

    def attach(self):
        with self.connection() as db:
            row=db.execute('SELECT queue_uuid,declaration FROM queue_identity WHERE id=1').fetchone()
        if row is None or json.loads(row[1]) != self.config.declaration():
            raise RecoveryContractError('Queue identity or declaration differs')
        self.queue_uuid=row[0]
        return self.queue_uuid

    @staticmethod
    def lock_budgets(db):
        db.execute(db.lock('SELECT id FROM runtime_budget_guard WHERE id=1')).fetchone()

    @staticmethod
    def publication(db, generation, kind):
        # This row lock survives until the transaction publishes its records.
        row=db.execute(db.lock('SELECT ordinal FROM queue_publications WHERE generation=? AND kind=?'),
                       (generation,kind)).fetchone()
        if row is None:
            raise RecoveryContractError('Unknown queue publication stream')
        value=row[0]+1
        db.execute('UPDATE queue_publications SET ordinal=? WHERE generation=? AND kind=?',(value,generation,kind))
        return value
