"""One controller-owned SQLite writer with an OS-enforced WAL file limit.

This module must run in the dedicated outer controller, never the Web process.
``FileSizeGuard.install`` changes that process's file-size rlimit. All normal
SQLite writes go to WAL under its smaller limit. Only a serialized checkpoint,
startup or backup may temporarily lift the soft limit to the main DB limit.
No SQL transaction may run during that lift. This is a per-file bound; the
deployment must additionally reserve DB + WAL + old/new backup + metadata.
"""
from __future__ import annotations

from contextlib import contextmanager
import fcntl
import hashlib
import os
from pathlib import Path
import resource
import signal
import sqlite3
import stat
import threading

from demiflow.forge.io import no_symlinks
from demiflow.forge.schema import ForgeError
from demiflow.workbench.analysis.contracts import reject

MIB = 1024 * 1024
SCHEMA_VERSION = 4

_SCHEMA_V2 = """
CREATE TABLE analysis_schema (revision_sha256 TEXT NOT NULL);
CREATE TABLE source_snapshots (
  project TEXT NOT NULL, digest TEXT NOT NULL, body BLOB NOT NULL,
  PRIMARY KEY (project,digest));
CREATE TABLE source_locators (
  project TEXT NOT NULL, digest TEXT NOT NULL, body BLOB NOT NULL,
  PRIMARY KEY(project,digest));
CREATE TABLE documents (
  project TEXT NOT NULL, kind TEXT NOT NULL, id TEXT NOT NULL,
  revision INTEGER NOT NULL, PRIMARY KEY (project,kind,id));
CREATE TABLE document_revisions (
  project TEXT NOT NULL, kind TEXT NOT NULL, id TEXT NOT NULL,
  revision INTEGER NOT NULL, digest TEXT NOT NULL, body BLOB NOT NULL,
  principal TEXT NOT NULL, time TEXT NOT NULL,
  PRIMARY KEY (project,kind,id,revision));
CREATE TABLE question_definitions (
  project TEXT NOT NULL, digest TEXT NOT NULL, body BLOB NOT NULL,
  PRIMARY KEY (project,digest));
CREATE TABLE annotation_events (
  sequence INTEGER PRIMARY KEY AUTOINCREMENT,
  project TEXT NOT NULL, source TEXT NOT NULL, case_key TEXT NOT NULL,
  variant TEXT NOT NULL, field TEXT NOT NULL, principal TEXT NOT NULL,
  definition TEXT NOT NULL, revision INTEGER NOT NULL,
  state TEXT NOT NULL, value BLOB NOT NULL, origin BLOB NOT NULL,
  time TEXT NOT NULL,
  UNIQUE(project,source,case_key,variant,field,principal,definition,revision));
CREATE INDEX annotation_target ON annotation_events
 (project,source,case_key,variant,field,principal,definition,sequence);
CREATE TABLE annotation_heads (
  project TEXT NOT NULL, source TEXT NOT NULL, case_key TEXT NOT NULL,
  variant TEXT NOT NULL, field TEXT NOT NULL, principal TEXT NOT NULL,
  definition TEXT NOT NULL, revision INTEGER NOT NULL, sequence INTEGER NOT NULL,
  PRIMARY KEY(project,source,case_key,variant,field,principal,definition));
CREATE TABLE operation_receipts (
  project TEXT NOT NULL, principal TEXT NOT NULL, request_id TEXT NOT NULL,
  operation TEXT NOT NULL, payload_digest TEXT NOT NULL, result BLOB NOT NULL,
  time TEXT NOT NULL, PRIMARY KEY(project,principal,request_id));
CREATE TABLE analysis_objects (
  project TEXT NOT NULL, id TEXT NOT NULL UNIQUE, publish_id TEXT NOT NULL UNIQUE,
  generation INTEGER NOT NULL, execution_id TEXT NOT NULL, state TEXT NOT NULL, seed BLOB NOT NULL,
  held_bytes INTEGER NOT NULL, held_entries INTEGER NOT NULL,
  manifest_digest TEXT, manifest BLOB, prepared BLOB, reason TEXT,
  PRIMARY KEY(project,id));
CREATE TABLE analysis_sessions (
  project TEXT NOT NULL, id TEXT NOT NULL UNIQUE, principal TEXT NOT NULL,
  notebook_id TEXT NOT NULL, notebook_revision INTEGER NOT NULL,
  generation TEXT NOT NULL, state TEXT NOT NULL, job_id TEXT,
  sources BLOB NOT NULL, web_parent BLOB NOT NULL,
  created TEXT NOT NULL, updated TEXT NOT NULL, reason TEXT,
  PRIMARY KEY(project,id));
CREATE TABLE analysis_session_inputs (
  project TEXT NOT NULL, session_id TEXT NOT NULL, generation TEXT NOT NULL,
  alias TEXT NOT NULL, source_snapshot TEXT NOT NULL, handle BLOB NOT NULL,
  PRIMARY KEY(session_id,generation,alias));
CREATE TABLE analysis_executions (
  sequence INTEGER PRIMARY KEY AUTOINCREMENT,
  project TEXT NOT NULL, id TEXT NOT NULL UNIQUE,
  session_id TEXT NOT NULL, generation TEXT NOT NULL,
  cell_id TEXT NOT NULL, notebook_revision INTEGER NOT NULL,
  source_sha256 TEXT NOT NULL, language TEXT NOT NULL, result_name TEXT,
  state TEXT NOT NULL, output_id TEXT, status TEXT, reason TEXT,
  created TEXT NOT NULL, updated TEXT NOT NULL,
  UNIQUE(project,id));
CREATE INDEX analysis_execution_queue ON analysis_executions(session_id,generation,state,sequence);
CREATE TABLE analysis_operations (
  project TEXT NOT NULL, id TEXT NOT NULL UNIQUE,
  session_id TEXT NOT NULL, generation TEXT NOT NULL, execution_id TEXT NOT NULL,
  client_request TEXT NOT NULL, operation TEXT NOT NULL, payload BLOB NOT NULL,
  state TEXT NOT NULL, reservation BLOB, input_id TEXT, job_id TEXT,
  result BLOB, transfer_metadata BLOB, cleanup_proven INTEGER NOT NULL DEFAULT 0,
  held_input_bytes INTEGER NOT NULL DEFAULT 0,
  created TEXT NOT NULL, updated TEXT NOT NULL, reason TEXT,
  PRIMARY KEY(project,id), UNIQUE(session_id,generation,client_request));
CREATE TABLE analysis_execution_dependencies (
  project TEXT NOT NULL, execution_id TEXT NOT NULL, kind TEXT NOT NULL,
  reference_key TEXT NOT NULL, body BLOB NOT NULL,
  PRIMARY KEY(project,execution_id,kind,reference_key));
CREATE TABLE analysis_output_presentations (
  project TEXT NOT NULL, origin_id TEXT NOT NULL, execution_id TEXT NOT NULL,
  output_id TEXT NOT NULL, output_binding TEXT NOT NULL, output_index INTEGER NOT NULL,
  descriptor_digest TEXT NOT NULL, source_snapshot TEXT NOT NULL,
  descriptor BLOB NOT NULL, binding BLOB NOT NULL,
  PRIMARY KEY(project,origin_id),
  UNIQUE(project,output_id,output_binding,output_index,descriptor_digest));
CREATE INDEX analysis_output_presentation_source ON analysis_output_presentations(project,source_snapshot);
PRAGMA user_version=2;
"""
_V3_ADDITIONS = """
CREATE TABLE annotation_media_basis (
  project TEXT NOT NULL, event_sequence INTEGER NOT NULL, basis BLOB NOT NULL,
  PRIMARY KEY(project,event_sequence));
CREATE TABLE analysis_media_captures (
  project TEXT NOT NULL, principal TEXT NOT NULL, request_id TEXT NOT NULL,
  capture_id TEXT NOT NULL UNIQUE, request_digest TEXT NOT NULL,
  source_snapshot TEXT NOT NULL, case_key TEXT NOT NULL, field TEXT NOT NULL,
  resource_binding TEXT NOT NULL, object_id TEXT NOT NULL, state TEXT NOT NULL,
  body BLOB NOT NULL, export_id TEXT NOT NULL UNIQUE, export_receipt BLOB,
  result BLOB, held_input_bytes INTEGER NOT NULL DEFAULT 0,
  cleanup_proven INTEGER NOT NULL DEFAULT 0, created TEXT NOT NULL,
  updated TEXT NOT NULL, reason TEXT,
  PRIMARY KEY(project,principal,request_id));
CREATE INDEX analysis_media_capture_lookup ON analysis_media_captures(
  project,source_snapshot,case_key,field,resource_binding,state,updated);
"""
_SCHEMA_V3 = _SCHEMA_V2.replace("PRAGMA user_version=2;",_V3_ADDITIONS+"PRAGMA user_version=3;")
_V4_ADDITIONS = """
CREATE TABLE analysis_generation_environments (
  project TEXT NOT NULL, session_id TEXT NOT NULL, generation TEXT NOT NULL,
  digest TEXT NOT NULL, body BLOB NOT NULL, created TEXT NOT NULL,
  PRIMARY KEY(project,session_id,generation));
CREATE TABLE analysis_cleanup_receipts (
  project TEXT NOT NULL, owner_kind TEXT NOT NULL, owner_id TEXT NOT NULL,
  generation TEXT NOT NULL, digest TEXT NOT NULL, body BLOB NOT NULL,
  updated TEXT NOT NULL, backend_mode TEXT NOT NULL,
  PRIMARY KEY(project,owner_kind,owner_id,generation));
"""
_SCHEMA = _SCHEMA_V3.replace("PRAGMA user_version=3;",_V4_ADDITIONS+"PRAGMA user_version=4;")
_SCHEMA_SHA256 = hashlib.sha256(("documents-v1/case-v1/object-payload-v1/dependencies-v1/presentation-v1/media-capture-v1/annotation-media-basis-v1/generation-environment-v1/scoped-cleanup-v1\n" + _SCHEMA).encode("utf-8")).hexdigest()



class FileSizeGuard:
    """Explicit bootstrap for a dedicated controller; no implicit installation."""

    def __init__(self, *, database_bytes=128*MIB, wal_bytes=32*MIB, preserve_hard_limit=False):
        if (type(database_bytes) is not int or type(wal_bytes) is not int
                or not 64*1024 <= wal_bytes < database_bytes <= 128*MIB
                or database_bytes % 4096 or wal_bytes % 4096):
            reject("Invalid controller DB/WAL file limits")
        if type(preserve_hard_limit) is not bool:
            reject("File-size inheritance policy must be an explicit boolean")
        self.database_bytes = database_bytes
        self.wal_bytes = wal_bytes
        self.preserve_hard_limit = preserve_hard_limit
        self.inherited_limits = None
        self._hard_limit = None
        self._normal_soft = None
        self._checkpoint_soft = None
        self._object_soft = None
        self.pid = None
        self._elevated = False
        self._database = None

    def install(self):
        if self.pid is not None or threading.active_count() != 1:
            reject("File-size guard requires a fresh, single-thread controller")
        soft, hard = resource.getrlimit(resource.RLIMIT_FSIZE)
        if not self.preserve_hard_limit and hard != resource.RLIM_INFINITY and hard < self.database_bytes:
            reject("Controller file-size hard limit is too small")
        self.inherited_limits = (soft, hard)
        self._hard_limit = hard if self.preserve_hard_limit else self.database_bytes
        def limit(value):
            # A trusted controller does not raise either of the account's
            # inherited limits. Its children receive inherited_limits through
            # their server-owned launch context, never the DB's soft ceiling.
            if self.preserve_hard_limit:
                for ceiling in (soft, hard):
                    if ceiling != resource.RLIM_INFINITY:
                        value = min(value, ceiling)
            return value
        self._normal_soft = limit(self.wal_bytes)
        self._checkpoint_soft = limit(self.database_bytes)
        self._object_soft = limit(min(64*MIB, self.database_bytes))
        signal.signal(signal.SIGXFSZ, signal.SIG_IGN)
        resource.setrlimit(resource.RLIMIT_CORE,(0,0))
        resource.setrlimit(resource.RLIMIT_FSIZE, (self._normal_soft, self._hard_limit))
        self.pid = os.getpid()
        return self

    def check(self):
        if (self.pid != os.getpid() or self._elevated
                or resource.getrlimit(resource.RLIMIT_FSIZE) != (self._normal_soft, self._hard_limit)):
            reject("Dedicated controller file-size guard is not active")

    @contextmanager
    def checkpoint(self):
        self.check()
        self._elevated = True
        resource.setrlimit(resource.RLIMIT_FSIZE, (self._checkpoint_soft, self._hard_limit))
        try:
            yield
        finally:
            resource.setrlimit(resource.RLIMIT_FSIZE, (self._normal_soft, self._hard_limit))
            self._elevated = False

    @contextmanager
    def object_write(self):
        """One admitted opaque chunk write; SQL is forbidden in this scope.

        The same single-thread controller receives bytes without Arrow/ZMQ
        decoding. Its admitted object/input file may exceed the32 MiB WAL cap.
        Raise only the soft limit for this write, then restore it before any
        event-loop callback or DB access. ``Database.locked`` checks this guard.
        """
        self.check()
        if threading.active_count() != 1:
            reject("Opaque object IO requires the sole controller thread")
        database = self._database
        if database is None or database._mutex.locked() or database.connection.in_transaction:
            reject("Opaque object IO cannot hold a DB lock or unfinished transaction")
        self._elevated = True
        resource.setrlimit(resource.RLIMIT_FSIZE, (self._object_soft, self._hard_limit))
        try:
            yield
        finally:
            resource.setrlimit(resource.RLIMIT_FSIZE, (self._normal_soft, self._hard_limit))
            self._elevated = False


def private_directory(path):
    path = no_symlinks(Path(path).absolute())
    path.mkdir(mode=0o700, parents=True, exist_ok=True)
    st = path.stat()
    if st.st_uid != os.geteuid() or stat.S_IMODE(st.st_mode) != 0o700:
        reject("Analysis state directory must be owned by its controller and mode 0700")
    return path


class Database:
    def __init__(self, directory, *, guard):
        guard.check()
        if guard._database is not None:
            reject("File-size guard already owns an open controller database")
        self.guard = guard
        self.directory = private_directory(directory)
        self.path = no_symlinks(self.directory / "records.sqlite")
        self._mutex = threading.Lock()
        self._closed = False
        lock_path = no_symlinks(self.directory / "writer.lock")
        self._lock_fd = os.open(lock_path, os.O_CREAT | os.O_RDWR | os.O_NOFOLLOW, 0o600)
        self.connection = None
        try:
            fcntl.flock(self._lock_fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
            allowed = {"records.sqlite", "records.sqlite-wal", "records.sqlite-shm", "writer.lock", "backup.sqlite", "backup.new.sqlite"}
            for index, entry in enumerate(self.directory.iterdir()):
                if index >= len(allowed) or entry.name not in allowed:
                    reject("Unexpected file in private analysis database directory")
                no_symlinks(entry)
                info = entry.stat()
                if not stat.S_ISREG(info.st_mode) or info.st_uid != os.geteuid() or stat.S_IMODE(info.st_mode) != 0o600:
                    reject("Analysis DB file ownership/mode is invalid")
                if entry.name.startswith("backup") and info.st_size > guard.database_bytes:
                    reject("Analysis backup exceeds its file reservation", code="resource_limit")
            for name in ("records.sqlite", "records.sqlite-wal", "records.sqlite-shm"):
                path = no_symlinks(self.directory / name)
                if path.exists():
                    info = path.stat()
                    if not stat.S_ISREG(info.st_mode) or info.st_uid != os.geteuid() or stat.S_IMODE(info.st_mode) != 0o600:
                        reject("Analysis DB file ownership/mode is invalid")
            if self.path.exists() and self.path.stat().st_size > guard.database_bytes:
                reject("Analysis database exceeds the configured limit", code="resource_limit")
            wal_path = self.directory / "records.sqlite-wal"
            if wal_path.exists() and wal_path.stat().st_size > guard.wal_bytes:
                reject("Analysis WAL exceeds the configured limit", code="resource_limit")
            if self.path.exists():
                # A read-only connection cannot migrate/checkpoint an unknown
                # schema. Do this before any header-changing pragma or RW open.
                readonly = sqlite3.connect(self.path.as_uri() + "?mode=ro", uri=True, timeout=0.1)
                try:
                    version = readonly.execute("PRAGMA user_version").fetchone()[0]
                    if version not in {0,1,2,3,SCHEMA_VERSION}:
                        reject("Unknown analysis database version; no writes or migration attempted")
                    if version == 0 and readonly.execute("SELECT count(*) FROM sqlite_master").fetchone()[0]:
                        reject("Unversioned existing analysis database is not adopted")
                    if version:
                        from demiflow.workbench.analysis.schema_migrations import validate
                        validate(readonly,version,core_v2=_SCHEMA_V2,core_v3=_SCHEMA_V3,current=_SCHEMA,current_digest=_SCHEMA_SHA256)
                finally:
                    readonly.close()
            # Initialization and WAL recovery may checkpoint. Hold the sole
            # writer lock and admit no requests until the smaller limit returns.
            with guard.checkpoint():
                fd = os.open(self.path, os.O_CREAT | os.O_RDWR | os.O_NOFOLLOW, 0o600)
                os.close(fd)
                self.connection = sqlite3.connect(self.path, timeout=0.1, isolation_level=None, check_same_thread=False)
                self.connection.row_factory = sqlite3.Row
                self.connection.execute("PRAGMA busy_timeout=100")
                self.connection.execute("PRAGMA page_size=4096")
                page_size = self.connection.execute("PRAGMA page_size").fetchone()[0]
                if page_size != 4096:
                    reject("Unsupported analysis database page size")
                actual_max = self.connection.execute(f"PRAGMA max_page_count={guard.database_bytes // page_size}").fetchone()[0]
                if actual_max > guard.database_bytes // page_size:
                    reject("Analysis database pages exceed configured limit", code="resource_limit")
                self.connection.execute("PRAGMA cache_size=-2048")
                self.connection.execute("PRAGMA temp_store=MEMORY")
                self.connection.execute("PRAGMA journal_mode=WAL")
                self.connection.execute("PRAGMA synchronous=FULL")
                self.connection.execute("PRAGMA wal_autocheckpoint=0")
                version = self.connection.execute("PRAGMA user_version").fetchone()[0]
                if version == 0:
                    if self.connection.execute("SELECT count(*) FROM sqlite_master").fetchone()[0]:
                        reject("Unversioned existing analysis database is not adopted")
                    self.connection.executescript("BEGIN IMMEDIATE;" + _SCHEMA)
                    self.connection.execute("INSERT INTO analysis_schema VALUES (?)", (_SCHEMA_SHA256,))
                    self.connection.execute("COMMIT")
                elif version in {1,2,3}:
                    from demiflow.workbench.analysis.schema_migrations import migrate
                    migrate(self.connection,self.directory,version,core_v2=_SCHEMA_V2,core_v3=_SCHEMA_V3,
                        additions=_V3_ADDITIONS,generation_additions=_V4_ADDITIONS,
                        current=_SCHEMA,current_digest=_SCHEMA_SHA256,maximum_bytes=guard.database_bytes)
                elif version != SCHEMA_VERSION:
                    reject("Unknown analysis database version; no writes or migration attempted")
                self._checkpoint()
            for path in self.directory.iterdir():
                if path.name in {"records.sqlite", "records.sqlite-wal", "records.sqlite-shm"}:
                    os.chmod(path, 0o600)
            guard._database = self
        except BaseException:
            # Unknown-version close can checkpoint existing WAL. Restore the
            # larger file limit only while no transaction/requests can run.
            if self.connection is not None:
                with guard.checkpoint():
                    self.connection.close()
            os.close(self._lock_fd)
            raise

    def _checkpoint(self):
        if self.connection.in_transaction:
            reject("Cannot checkpoint an active analysis transaction")
        busy, _, _ = self.connection.execute("PRAGMA wal_checkpoint(TRUNCATE)").fetchone()
        if busy:
            reject("Analysis checkpoint is busy", code="busy")

    def _headroom(self, payload_bytes):
        # Conservative reservation includes old/new pages, indexes and receipt.
        # RLIMIT_FSIZE is the final per-file guard, including SQLite overhead.
        reserve = payload_bytes * 4 + 32*4096
        if reserve > self.guard.wal_bytes:
            reject("Analysis transaction exceeds WAL reservation", code="resource_limit")
        wal = self.directory / "records.sqlite-wal"
        size = wal.stat().st_size if wal.exists() else 0
        if size + reserve > self.guard.wal_bytes:
            with self.guard.checkpoint():
                self._checkpoint()

    @contextmanager
    def locked(self):
        self.guard.check()
        if self._closed:
            reject("Analysis database is closed")
        if not self._mutex.acquire(blocking=False):
            reject("Analysis controller is busy", code="busy")
        try:
            yield self.connection
        finally:
            self._mutex.release()

    @contextmanager
    def transaction(self, *, payload_bytes):
        with self.locked() as db:
            try:
                self._headroom(payload_bytes)
                db.execute("BEGIN IMMEDIATE")
                yield db
                db.execute("COMMIT")
            except BaseException as exc:
                if db.in_transaction:
                    db.execute("ROLLBACK")
                if isinstance(exc, sqlite3.Error):
                    code = getattr(exc, "sqlite_errorcode", 0) & 255
                    error = "resource_limit" if code == sqlite3.SQLITE_FULL else "busy" if code in {sqlite3.SQLITE_BUSY, sqlite3.SQLITE_LOCKED} else "operation_failed"
                    raise ForgeError("Analysis storage transaction failed; query the original receipt before retry", code=error) from exc
                raise

    def backup(self):
        """Explicit bounded offline backup, not a schema migration."""
        with self.locked() as db:
            target = no_symlinks(self.directory / "backup.sqlite")
            temporary = no_symlinks(self.directory / "backup.new.sqlite")
            if temporary.exists():
                reject("Unresolved analysis backup staging exists", code="conflict")
            with self.guard.checkpoint():
                self._checkpoint()
                fd = os.open(temporary, os.O_CREAT | os.O_EXCL | os.O_RDWR | os.O_NOFOLLOW, 0o600)
                os.close(fd)
                destination = sqlite3.connect(temporary, timeout=0.1)
                try:
                    destination.execute("PRAGMA cache_size=-2048")
                    db.backup(destination, pages=64, sleep=0)
                    if destination.execute("PRAGMA quick_check").fetchone()[0] != "ok":
                        reject("Analysis backup verification failed")
                    # A standalone backup is one file, not a second live WAL
                    # database with unreserved sidecars when opened later.
                    if destination.execute("PRAGMA journal_mode=DELETE").fetchone()[0] != "delete":
                        reject("Analysis backup could not become a standalone file")
                finally:
                    destination.close()
                fd = os.open(temporary, os.O_RDONLY | os.O_NOFOLLOW)
                try:
                    os.fsync(fd)
                finally:
                    os.close(fd)
                os.replace(temporary, target)
                fd = os.open(self.directory, os.O_RDONLY | os.O_DIRECTORY)
                try:
                    os.fsync(fd)
                finally:
                    os.close(fd)
            return {"path": str(target), "bytes": target.stat().st_size, "schema_version": SCHEMA_VERSION}

    def close(self):
        if self._closed:
            return
        with self.locked():
            with self.guard.checkpoint():
                self._checkpoint()
                self.connection.close()
            self._closed = True
            os.close(self._lock_fd)
            self.guard._database = None

