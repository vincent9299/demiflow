"""SQLite destination receipts for the original native commit protocol.

The run owns staging and scratch reservations. SQLite owns only its destination
identity and atomic publication receipt, committed in the same transaction as
the KV changes. No execution progress, recovery policy or ACK is stored here.
"""
from contextlib import closing, contextmanager
from dataclasses import asdict
import hashlib
import json
import os
from pathlib import Path
import sqlite3
import stat
import struct
import time
import uuid

from .execution.recovery_contract import RecoveryContractError, UnresolvedCommit, identifier, identity

_MAGIC = b'DFKV\x00\x01\x00\x00'
_HEADER = struct.Struct('!II')
_MAX_RECEIPTS = 65536


def _sync(path):
    fd = os.open(path, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


def _info(path):
    if any(p.is_symlink() for p in (path, *path.parents)):
        raise UnresolvedCommit('KV destination or staging contains a symlink')
    try:
        value = path.stat()
    except FileNotFoundError:
        return None
    if not stat.S_ISREG(value.st_mode):
        raise UnresolvedCommit('KV destination or staging is not a regular file')
    return dict(device=value.st_dev, inode=value.st_ino, bytes=value.st_size)


def _run(context):
    from .execution.run_context import RunContext, current_run
    run = current_run()
    if (not isinstance(run, RunContext) or not run.active
            or run.ref.run_id != context.run_id or run.attempt != context.attempt
            or run.fence != context.fence):
        raise RecoveryContractError('KV sink requires the active driver commit context')
    for value in (context.run_id, context.action_id, context.commit_id):
        identifier(value)
    run.check_stop()
    return run


@contextmanager
def _scratch(run, context, size):
    # The native SQLite rollback/growth extent is reserved in the *same* run
    # artifact accounting as all other temporaries, before SQLite can write.
    # This is an admission reservation, not a published artifact or result.
    path = run.directory / 'scratch' / ('kv-' + context.commit_id + '.reservation')
    run.allocate_artifact(path, size=size, atomic=False)
    try:
        # Recheck against locked SQLite geometry without introducing another
        # allocator or raising the original run's total admission ceiling.
        yield lambda required: run.reserve_artifact_bytes(path, max(size, required))
    finally:
        run.release_temporaries([path])


def _connection(store, run, *, readonly=False):
    path = Path(store.path)
    timeout = max(0, min(store.timeout_s, run.deadline - time.time(),
                        run.limits['timeout_s'] - (time.monotonic() - run.started)))
    db = sqlite3.connect(path.as_uri() + ('?mode=ro' if readonly else '?mode=rw'),
                         uri=True, timeout=timeout)
    # Connection-local page cache; this is not a claim to control SQLite RSS.
    cache_kib = max(1, min(1024, run.limits['memory_bytes'] // (8 * 1024)))
    db.execute('PRAGMA cache_size=-' + str(cache_kib))
    db.execute('PRAGMA temp_store=FILE')
    if not readonly:
        db.execute('PRAGMA synchronous=FULL')
    db.set_progress_handler(lambda: int(time.time() > run.deadline or
        time.monotonic() - run.started > run.limits['timeout_s']), 10000)
    return db


def _target_bytes(path):
    # Include existing WAL/journal extents before opening a connection which
    # could recover a previous SQLite transaction.
    return sum((_info(Path(str(path) + suffix)) or {}).get('bytes', 0)
               for suffix in ('', '-wal', '-journal'))


def _locked_extent(db, path, *, rows=0, payload_bytes=0):
    if not db.in_transaction:
        raise RecoveryContractError('SQLite scratch sizing requires the original write transaction')
    page_size = db.execute('PRAGMA page_size').fetchone()[0]
    page_count = db.execute('PRAGMA page_count').fetchone()[0]
    if page_size not in (512, 1024, 2048, 4096, 8192, 16384, 32768, 65536) or page_count < 0:
        raise RecoveryContractError('SQLite page geometry is invalid')
    # BEGIN IMMEDIATE prevents another writer extending this snapshot between
    # sizing and our writes. Account for logical WAL pages as well as extents;
    # the structure allowance is pages, never an assumed 4096-byte page size.
    existing = max(_target_bytes(path), page_size * page_count)
    return 2 * existing + 4 * payload_bytes + (rows + 8) * 8 * page_size


class DurableKVSink:
    def recovery_identity(self):
        return dict(kind='sqlite_kv', store=asdict(self.store), key=self.key,
                    value=self.value, batch_rows=self.batch_rows, batch_bytes=self.batch_bytes)

    def _path(self, run, context):
        return run.directory / 'materializations' / context.action_id / ('kv-' + context.commit_id + '.bin')

    def _target(self, run, context):
        path = Path(self.store.path)
        extent = 2 * _target_bytes(path) + 128 * 1024
        with _scratch(run, context, extent) as reserve:
            # Race-safe creation only; a losing creator must not update the
            # winner's kv_usage before taking and sizing its own write lock.
            if not path.exists():
                self.store._create_database()
            with closing(_connection(self.store, run)) as db, db:
                db.execute('BEGIN IMMEDIATE')
                reserve(_locked_extent(db, path))
                db.execute('CREATE TABLE IF NOT EXISTS kv_store_identity '
                           '(singleton INTEGER PRIMARY KEY CHECK(singleton=1), database_id TEXT NOT NULL)')
                db.execute('INSERT OR IGNORE INTO kv_store_identity VALUES (1,?)', (uuid.uuid4().hex,))
                db.execute('CREATE TABLE IF NOT EXISTS kv_sink_commits '
                           '(namespace TEXT NOT NULL,run_id TEXT NOT NULL,action_id TEXT NOT NULL,'
                           'commit_id TEXT NOT NULL,prepared_digest TEXT NOT NULL,receipt TEXT NOT NULL,'
                           'PRIMARY KEY(namespace,run_id,action_id,commit_id))')
                database_id = db.execute('SELECT database_id FROM kv_store_identity WHERE singleton=1').fetchone()[0]
            _sync(path.parent)
        identifier(database_id)
        physical = _info(path)
        return dict(database_id=database_id, device=physical['device'], inode=physical['inode'])

    def prepare(self, blocks, context):
        from .kv import encode
        run = _run(context)
        path = self._path(run, context)
        path.parent.mkdir(parents=True, exist_ok=True)
        run.allocate_artifact(path, commit_id=context.commit_id, atomic=False)
        digest = hashlib.sha256()
        size = rows = batches = batch_size = batch_count = 0
        fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_TRUNC | os.O_NOFOLLOW, 0o600)
        try:
            with os.fdopen(fd, 'wb') as stream:
                def write(*chunks):
                    nonlocal size
                    extent = size + sum(map(len, chunks))
                    if extent > run.limits['max_output_bytes']:
                        raise RecoveryContractError('KV staging exceeds output byte budget')
                    run.reserve_artifact_bytes(path, extent)
                    for raw in chunks:
                        stream.write(raw)
                        digest.update(raw)
                    size = extent
                write(_MAGIC)
                for block in blocks:
                    for index in range(block.num_rows):
                        run.check_stop()
                        # Decode one row, never the complete incoming Arrow block.
                        row = block.slice(index, 1).to_pylist()[0]
                        key = self.store.key(row[self.key]).encode()
                        value = encode(row[self.value], self.store.max_value_bytes)
                        if len(value) > self.batch_bytes:
                            raise ValueError('KV sink single value exceeds batch_bytes')
                        if rows >= run.limits['max_output_rows']:
                            raise RecoveryContractError('KV staging exceeds output row budget')
                        if batch_count and (batch_count >= self.batch_rows or batch_size + len(value) > self.batch_bytes):
                            batches += 1
                            batch_count = batch_size = 0
                        write(_HEADER.pack(len(key), len(value)), key, value)
                        rows += 1
                        batch_count += 1
                        batch_size += len(value)
                batches += bool(batch_count)
                stream.flush()
                os.fsync(stream.fileno())
            _sync(path.parent)
            # All input/byte validation precedes any target DB creation.
            target = self._target(run, context)
            return dict(schema='demiflow.kv.prepared.v1', run_id=context.run_id,
                        action_id=context.action_id, commit_id=context.commit_id,
                        identity=self.recovery_identity(), target=target,
                        path=run.journal.artifact_name(path), bytes=size, sha256=digest.hexdigest(),
                        rows=rows, batches=batches)
        except BaseException:
            path.unlink(missing_ok=True)
            raise

    def _validate(self, prepared, context, run):
        if (prepared.get('schema') != 'demiflow.kv.prepared.v1'
                or prepared.get('identity') != self.recovery_identity()
                or any(prepared.get(name) != getattr(context, name) for name in ('run_id', 'action_id', 'commit_id'))
                or prepared.get('path') != run.journal.artifact_name(self._path(run, context))):
            raise RecoveryContractError('KV preparation identity differs')
        for name in ('rows', 'bytes', 'batches'):
            if type(prepared.get(name)) is not int or prepared[name] < 0:
                raise RecoveryContractError('KV preparation counts differ')
        if prepared['rows'] > run.limits['max_output_rows'] or prepared['bytes'] > run.limits['max_output_bytes']:
            raise RecoveryContractError('KV preparation exceeds current admission')
        identifier(prepared['sha256'])
        return identity(prepared)

    def _open_verified(self, prepared, run, *, readonly):
        path = Path(self.store.path)
        physical = _info(path)
        expected = prepared['target']
        if physical is None or any(physical[key] != expected[key] for key in ('device', 'inode')):
            raise UnresolvedCommit('KV destination is missing or was replaced')
        try:
            db = _connection(self.store, run, readonly=readonly)
            database_id = db.execute('SELECT database_id FROM kv_store_identity WHERE singleton=1').fetchone()
            if database_id != (expected['database_id'],):
                raise UnresolvedCommit('KV destination identity differs')
            return db
        except BaseException:
            if 'db' in locals():
                db.close()
            raise

    def _receipt(self, prepared, context, digest):
        return dict(commit_id=context.commit_id, sink_id='sqlite-kv', status='committed',
                    data=dict(target=self.store.path, namespace=self.store.namespace,
                              database_id=prepared['target']['database_id'], rows=prepared['rows'],
                              batches=prepared['batches'], prepared_digest=digest, atomicity='commit'))

    def _inspect_db(self, db, prepared, context, digest):
        row = db.execute('SELECT prepared_digest,receipt FROM kv_sink_commits '
                         'WHERE namespace=? AND run_id=? AND action_id=? AND commit_id=?',
                         (self.store.namespace, context.run_id, context.action_id, context.commit_id)).fetchone()
        if row is None:
            return None
        expected = self._receipt(prepared, context, digest)
        if row[0] != digest or json.loads(row[1]) != expected:
            raise UnresolvedCommit('KV commit receipt differs from its preparation')
        return expected

    def inspect(self, prepared, context):
        run = _run(context)
        digest = self._validate(prepared, context, run)
        try:
            with closing(self._open_verified(prepared, run, readonly=True)) as db:
                db.execute('BEGIN')
                return self._inspect_db(db, prepared, context, digest)
        except (OSError, sqlite3.Error, ValueError) as exc:
            raise UnresolvedCommit('KV publication cannot be inspected conclusively') from exc

    def _rows(self, prepared, context, run):
        path = self._path(run, context)
        physical = _info(path)
        if physical is None or physical['bytes'] != prepared['bytes']:
            raise RecoveryContractError('KV staging size differs')
        digest = hashlib.sha256()
        count = 0
        fd = os.open(path, os.O_RDONLY | os.O_NOFOLLOW)
        with os.fdopen(fd, 'rb') as stream:
            def read(size):
                value = stream.read(size)
                if len(value) != size:
                    raise RecoveryContractError('KV staging is truncated')
                digest.update(value)
                return value
            if read(len(_MAGIC)) != _MAGIC:
                raise RecoveryContractError('KV staging format differs')
            while stream.tell() < prepared['bytes']:
                run.check_stop()
                key_size, value_size = _HEADER.unpack(read(_HEADER.size))
                if not 0 < key_size <= 65536 or not 0 <= value_size <= min(self.store.max_value_bytes, self.batch_bytes):
                    raise RecoveryContractError('KV staging row exceeds original bounds')
                key, value = read(key_size).decode(), read(value_size)
                self.store.key(key)
                count += 1
                if count > prepared['rows']:
                    raise RecoveryContractError('KV staging contains extra rows')
                yield key, value
            if count != prepared['rows'] or digest.hexdigest() != prepared['sha256']:
                raise RecoveryContractError('KV staging checksum or count differs')

    def commit(self, prepared, context):
        run = _run(context)
        digest = self._validate(prepared, context, run)
        path = Path(self.store.path)
        # Conservative rollback/growth admission includes existing database and
        # journal extents, new payload, key/index pages and fixed receipt pages.
        extent = 2 * _target_bytes(path) + 4 * prepared['bytes'] + (prepared['rows'] + 8) * 32768
        with _scratch(run, context, extent) as reserve, closing(self._open_verified(prepared, run, readonly=False)) as db, db:
            db.execute('BEGIN IMMEDIATE')
            receipt = self._inspect_db(db, prepared, context, digest)
            if receipt is not None:
                return receipt
            reserve(_locked_extent(db, path, rows=prepared['rows'], payload_bytes=prepared['bytes']))
            if db.execute('SELECT COUNT(*) FROM kv_sink_commits WHERE namespace=?', (self.store.namespace,)).fetchone()[0] >= _MAX_RECEIPTS:
                raise ValueError('KV sink receipt capacity exceeded')
            has_origins = db.execute("SELECT 1 FROM sqlite_master WHERE name='kv_origins'").fetchone() is not None
            count, size = db.execute('SELECT entries,bytes FROM kv_usage WHERE namespace=?',
                                     (self.store.namespace,)).fetchone() or (0, 0)
            for key, value in self._rows(prepared, context, run):
                old = db.execute('SELECT length(value) FROM kv WHERE namespace=? AND key=?',
                                 (self.store.namespace, key)).fetchone()
                count += old is None
                old_origin = (db.execute('SELECT length(CAST(origin AS BLOB)) FROM kv_origins WHERE namespace=? AND key=?',
                                         (self.store.namespace, key)).fetchone() if has_origins else None)
                size += len(value) - (old[0] if old else 0) - (old_origin[0] if old_origin else 0)
                if count > self.store.max_entries or size > self.store.max_bytes:
                    raise ValueError('KV capacity exceeded')
                db.execute('INSERT INTO kv VALUES (?,?,?) ON CONFLICT(namespace,key) DO UPDATE SET value=excluded.value',
                           (self.store.namespace, key, value))
                if has_origins:
                    db.execute('DELETE FROM kv_origins WHERE namespace=? AND key=?', (self.store.namespace, key))
            db.execute('INSERT INTO kv_usage VALUES (?,?,?) ON CONFLICT(namespace) DO UPDATE SET entries=excluded.entries,bytes=excluded.bytes',
                       (self.store.namespace, count, size))
            receipt = self._receipt(prepared, context, digest)
            db.execute('INSERT INTO kv_sink_commits VALUES (?,?,?,?,?,?)',
                       (self.store.namespace, context.run_id, context.action_id, context.commit_id,
                        digest, json.dumps(receipt, sort_keys=True, separators=(',', ':'))))
            run.check_stop()
            physical = _info(path)
            if physical is None or any(physical[key] != prepared['target'][key] for key in ('device', 'inode')):
                raise UnresolvedCommit('KV destination changed during publication')
        _sync(path.parent)
        return receipt

    def abort(self, prepared, context):
        run = _run(context)
        self._validate(prepared, context, run)
        if self.inspect(prepared, context) is None:
            path = self._path(run, context)
            path.unlink(missing_ok=True)
            _sync(path.parent)
