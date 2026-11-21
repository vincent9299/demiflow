"""One project-local transaction authority for fit ownership and model retention."""
from __future__ import annotations

from contextlib import closing, contextmanager
import json
import socket
from pathlib import Path
import sqlite3

from ..contracts.training import FitBinding, FitRequest
from ..recovery_contract import MAX_DOCUMENT_BYTES, RecoveryContractError, document, identity


class FitConflict(RecoveryContractError):
    pass


class TrainingIndex:
    def __init__(self, binding: FitBinding, *, readonly=False):
        self.readonly = readonly
        self.binding = binding
        directory = Path(binding.project_dir)
        if (not directory.is_absolute() or not directory.is_dir()
                or directory.resolve() != directory or (directory / 'fit.sqlite').is_symlink()):
            raise ValueError("A training project requires an existing absolute control directory")
        self.path = directory / 'fit.sqlite'
        if readonly:
            if not self.path.is_file():
                raise FitConflict("No project training index exists")
            return
        with closing(self._open()) as db:
            db.executescript("""
                CREATE TABLE IF NOT EXISTS fit_settings (name TEXT PRIMARY KEY, value TEXT NOT NULL);
                CREATE TABLE IF NOT EXISTS fit_instances (
                    instance TEXT PRIMARY KEY, project TEXT NOT NULL,
                    fit_key TEXT NOT NULL, run_id TEXT NOT NULL,
                    run_dir TEXT NOT NULL, action TEXT NOT NULL, occurrence TEXT NOT NULL,
                    attempt INTEGER NOT NULL, epoch INTEGER NOT NULL, provider_scope TEXT NOT NULL,
                    launch INTEGER NOT NULL, request_key TEXT NOT NULL, state TEXT NOT NULL,
                    snapshot TEXT, job TEXT, manifest TEXT, manifest_digest TEXT, commit_id TEXT, reason TEXT, request TEXT NOT NULL
                );
                CREATE UNIQUE INDEX IF NOT EXISTS fit_manifest_digest ON fit_instances(project, manifest_digest);
                CREATE TABLE IF NOT EXISTS fit_reservations (
                    request_key TEXT PRIMARY KEY, project TEXT NOT NULL, currency TEXT NOT NULL,
                    reserved INTEGER NOT NULL, billed INTEGER
                );
                CREATE TABLE IF NOT EXISTS fit_objects (
                    store TEXT NOT NULL, digest TEXT NOT NULL, bytes INTEGER NOT NULL, ready INTEGER NOT NULL,
                    PRIMARY KEY (store,digest)
                );
                CREATE INDEX IF NOT EXISTS fit_pending_objects ON fit_objects(store,ready,digest);
                CREATE TABLE IF NOT EXISTS fit_snapshots (
                    instance TEXT NOT NULL, step INTEGER NOT NULL, snapshot TEXT NOT NULL,
                    PRIMARY KEY (instance,step)
                );
                CREATE TABLE IF NOT EXISTS fit_holds (
                    owner TEXT NOT NULL, instance TEXT NOT NULL, kind TEXT NOT NULL,
                    target TEXT, PRIMARY KEY (owner, instance)
                );
            """)

        # A local controller owns this SQLite admission domain. Do not infer
        # cross-host fencing from locks on a network-mounted project directory.
        machine_id = Path('/etc/machine-id')
        control_host = machine_id.read_text().strip() if machine_id.is_file() else socket.gethostname()
        with self._write() as db:
            for name, setting in dict(control_host=control_host, model_store=binding.model_store_uri,
                                      model_bytes=binding.max_model_bytes, files=binding.max_files,
                                      instances=binding.max_instances, references=binding.max_references).items():
                self._bind_setting(db, name, setting)

    @staticmethod
    def _bind_setting(db, name, value):
        raw = document(value).decode()
        db.execute('INSERT OR IGNORE INTO fit_settings VALUES (?,?)', (name, raw))
        old = db.execute('SELECT value FROM fit_settings WHERE name=?', (name,)).fetchone()[0]
        if old != raw:
            raise FitConflict("Training deployment disagrees with the project setting " + name)

    def _open(self):
        location = self.path.as_uri() + '?mode=ro' if self.readonly else self.path
        db = sqlite3.connect(location, timeout=1.0, isolation_level=None, uri=self.readonly)
        db.row_factory = sqlite3.Row
        return db

    @contextmanager
    def _write(self):
        with closing(self._open()) as db, db:
            db.execute('BEGIN IMMEDIATE')
            yield db

    @staticmethod
    def _row(db, instance):
        return db.execute('SELECT * FROM fit_instances WHERE instance=?', (instance,)).fetchone()

    def read(self, instance):
        with closing(self._open()) as db:
            row = self._row(db, instance)
            return dict(row) if row else None

    @staticmethod
    def request_key(instance, launch, scope, run_id):
        return identity(['training-submission', run_id, instance, launch, scope])

    def _recorded_key(self, row, launch):
        make_key = self.request_key
        # Preserve the exact original request namespace for in-flight v1 rows.
        if row['request_key'] == identity([row['instance'], row['launch'], row['provider_scope']]):
            return identity([row['instance'], launch, row['provider_scope']])
        if row['request_key'] != make_key(row['instance'], row['launch'], row['provider_scope'], row['run_id']):
            raise FitConflict("Training launch key differs from the recorded producer")
        return make_key(row['instance'], launch, row['provider_scope'], row['run_id'])

    def request_keys(self, row):
        if not 0 <= row['launch'] <= self.binding.max_resumes:
            raise FitConflict("Training launch history exceeds its declared restart limit")
        return tuple(self._recorded_key(row, i) for i in range(row['launch']+1))

    def claim_or_get(self, request: FitRequest, *, run_id, run_dir, action, occurrence, attempt):
        """Only the owner of the original run may take over its instance."""
        instance = request.instance_id
        scope = self.binding.provider_scope
        with self._write() as db:
            row = self._row(db, instance)
            if row is None:
                if db.execute('SELECT count(*) FROM fit_instances').fetchone()[0] >= self.binding.max_instances:
                    raise FitConflict("Project training instance quota exceeded")
                request_key = self.request_key(instance, 0, scope, run_id)
                db.execute('''INSERT INTO fit_instances
                    (instance,project,fit_key,run_id,run_dir,action,occurrence,attempt,epoch,
                     provider_scope,launch,request_key,state,request) VALUES (?,?,?,?,?,?,?,?,1,?,0,?,'claimed',?)''',
                           (instance, request.project_id, request.fit_key, run_id, run_dir, action,
                            occurrence, attempt, scope, request_key, document(request.to_dict()).decode()))
            elif (row['run_id'], row['action'], row['occurrence']) == (run_id, action, occurrence):
                if row['run_dir'] != run_dir or row['provider_scope'] != scope or attempt < row['attempt']:
                    raise FitConflict("Producer authority or provider scope differs")
                if attempt > row['attempt']:
                    db.execute('UPDATE fit_instances SET epoch=epoch+1, attempt=? WHERE instance=?',
                               (attempt, instance))
            return dict(self._row(db, instance))

    def launch_is_active(self, instance, request_key, *, run_id, action, occurrence, attempt):
        """Caller holds the original run's fit_guard through the admission or activation."""
        row = self.read(instance)
        if (row is None or (row['request_key'],row['run_id'],row['action'],row['occurrence'],row['attempt'],
                row['provider_scope']) != (request_key,run_id,action,occurrence,attempt,self.binding.provider_scope)):
            raise FitConflict("Original training launch identity or ownership differs")
        return row['state'] == 'claimed'

    def find_manifest(self, project, digest):
        with closing(self._open()) as db:
            row = db.execute("SELECT * FROM fit_instances WHERE project=? AND manifest_digest=? AND state='published'",
                             (project, digest)).fetchone()
            return dict(row) if row else None

    def _owner(self, db, instance, epoch, key):
        row = self._row(db, instance)
        if not row or row['epoch'] != epoch or row['request_key'] != key:
            raise FitConflict("Training owner or launch was superseded")
        return row

    def bind_job(self, instance, epoch, key, job):
        raw = document(job.to_dict()).decode()
        with self._write() as db:
            row = self._owner(db, instance, epoch, key)
            if row['job'] not in (None, raw) or row['provider_scope'] != job.scope:
                raise FitConflict("Returned job differs from its recorded submission")
            db.execute('UPDATE fit_instances SET job=? WHERE instance=?', (raw, instance))

    def reserve_object(self, store, digest, size):
        """Reserve immutable content once. False means project quota is full."""
        with self._write() as db:
            old = db.execute('SELECT bytes FROM fit_objects WHERE store=? AND digest=?',
                             (store, digest)).fetchone()
            if old:
                if old[0] != size:
                    raise FitConflict("Model object length changed")
                return True
            total, files = db.execute('SELECT COALESCE(SUM(bytes),0),COUNT(*) FROM fit_objects').fetchone()
            if total + size > self.binding.max_model_bytes or files >= self.binding.max_files:
                return False
            db.execute('INSERT INTO fit_objects VALUES (?,?,?,0)', (store, digest, size))
            return True

    def pending_objects(self, store, after):
        with closing(self._open()) as db:
            rows = db.execute('SELECT digest FROM fit_objects WHERE store=? AND ready=0 AND digest>? '
                              'ORDER BY digest LIMIT 64', (store,after)).fetchall()
            return [row[0] for row in rows]

    def complete_object(self, store, digest):
        with self._write() as db:
            db.execute('UPDATE fit_objects SET ready=1 WHERE store=? AND digest=?', (store,digest))

    def release_unwritten_object(self, store, digest):
        """Caller holds the digest partition lock and proved the object absent."""
        with self._write() as db:
            db.execute('DELETE FROM fit_objects WHERE store=? AND digest=? AND ready=0', (store,digest))

    def save_steps(self, instance, epoch, key, snapshots):
        from ..contracts.training import TrainingSnapshotRef
        with self._write() as db:
            self._owner(db, instance, epoch, key)
            for snapshot in snapshots:
                raw = document(snapshot.to_dict()).decode()
                old = db.execute('SELECT snapshot FROM fit_snapshots WHERE instance=? AND step=?',
                                 (instance, snapshot.step)).fetchone()
                if old and old[0] != raw:
                    raise FitConflict("A saved optimizer step changed")
                db.execute('INSERT OR IGNORE INTO fit_snapshots VALUES (?,?,?)',
                           (instance, snapshot.step, raw))
            total, count = db.execute("""SELECT COALESCE(SUM(length(CAST(snapshot AS BLOB))),0),COUNT(*)
                FROM fit_snapshots WHERE instance=?""", (instance,)).fetchone()
            if total > MAX_DOCUMENT_BYTES or count > 16384:
                raise FitConflict("Saved step manifest exceeds budget")
            rows = db.execute('SELECT snapshot FROM fit_snapshots WHERE instance=? ORDER BY step LIMIT 16385',
                              (instance,)).fetchall()
            if len(rows) > 16384:
                raise FitConflict("Saved step count exceeds budget")
            return tuple(TrainingSnapshotRef.from_dict(json.loads(r[0])) for r in rows)

    def reserve_cost(self, instance, epoch, key):
        if not self.binding.max_charge:
            return
        binding = self.binding
        with self._write() as db:
            row = self._owner(db, instance, epoch, key)
            self._bind_setting(db, 'charge_limit/'+binding.currency, binding.project_charge_limit)
            entry = db.execute('SELECT reserved,currency FROM fit_reservations WHERE request_key=?',
                               (key,)).fetchone()
            if entry:
                if (entry['reserved'], entry['currency']) != (binding.max_charge, binding.currency):
                    raise FitConflict("Provider fee reservation differs")
                return
            charged = db.execute('''SELECT COALESCE(SUM(COALESCE(billed,reserved)),0)
                                     FROM fit_reservations WHERE project=? AND currency=?''',
                                 (row['project'], binding.currency)).fetchone()[0]
            if charged + binding.max_charge > binding.project_charge_limit:
                raise FitConflict("Shared training charge budget exceeded")
            db.execute('INSERT INTO fit_reservations VALUES (?,?,?,?,NULL)',
                       (key,row['project'],binding.currency,binding.max_charge))

    def release_unsent_cost(self, instance, epoch, key):
        with self._write() as db:
            self._owner(db, instance, epoch, key)
            db.execute('UPDATE fit_reservations SET billed=0 WHERE request_key=? AND billed IS NULL', (key,))

    def settle_cost(self, instance, epoch, key, billed):
        with self._write() as db:
            self._owner(db, instance, epoch, key)
            old = db.execute('SELECT reserved,billed FROM fit_reservations WHERE request_key=?',
                             (key,)).fetchone()
            if (old is None or type(billed) is not int or not 0 <= billed <= old['reserved'] or
                    old['billed'] not in (None,billed)):
                raise FitConflict("Invalid or inconsistent final provider charge")
            db.execute('UPDATE fit_reservations SET billed=? WHERE request_key=?', (billed,key))

    def relaunch(self, instance, epoch, key, snapshot):
        with self._write() as db:
            row = self._owner(db, instance, epoch, key)
            if row['state'] != 'claimed' or row['launch'] >= self.binding.max_resumes:
                raise FitConflict("Training cannot restart within its recovery budget")
            launch = row['launch'] + 1
            new_key = self._recorded_key(row, launch)
            db.execute('UPDATE fit_instances SET launch=?,request_key=?,job=NULL,snapshot=? WHERE instance=?',
                       (launch,new_key,document(snapshot.to_dict()).decode(),instance))

    def cancel(self, instance, epoch, key):
        with self._write() as db:
            row = self._owner(db, instance, epoch, key)
            if row['state'] == 'claimed':
                db.execute("UPDATE fit_instances SET state='cancelling' WHERE instance=?", (instance,))
                return 'cancelling'
            return row['state']

    def terminate(self, instance, epoch, key, reason):
        with self._write() as db:
            row = self._owner(db, instance, epoch, key)
            if row['state'] not in ('claimed','cancelling','cancelled','failed'):
                raise FitConflict("A published model cannot be failed or cancelled")
            state = 'cancelled' if row['state'] in ('cancelling','cancelled') else 'failed'
            db.execute('UPDATE fit_instances SET state=?,reason=? WHERE instance=?',
                       (state, str(reason)[:4096], instance))

    def publish(self, instance, epoch, key, commit_id, manifest_ref):
        raw = document(manifest_ref.to_dict()).decode()
        with self._write() as db:
            row = self._owner(db, instance, epoch, key)
            if row['state'] == 'published' and (row['manifest'],row['commit_id']) == (raw,commit_id):
                return
            if row['state'] != 'claimed':
                raise FitConflict("Cannot publish a cancelled, failed or conflicting training result")
            db.execute("UPDATE fit_instances SET state='published',manifest=?,manifest_digest=?,commit_id=? WHERE instance=?",
                       (raw,manifest_ref.sha256,commit_id,instance))

    def hold(self, owner, instance, *, kind, target=None):
        """Pin an immutable model before a consuming table can become visible."""
        if kind not in ('pending_table','table','active','parent'):
            raise ValueError("Unknown training reference kind")
        locator = document(target).decode() if target is not None else None
        with self._write() as db:
            row = self._row(db,instance)
            if not row or row['state'] != 'published':
                raise FitConflict("A hold requires a published model")
            existing = db.execute('SELECT kind,target FROM fit_holds WHERE owner=? AND instance=?',
                                  (owner,instance)).fetchone()
            if existing is None:
                if db.execute('SELECT count(*) FROM fit_holds').fetchone()[0] >= self.binding.max_references:
                    raise FitConflict('Training reference quota exceeded')
                db.execute('INSERT INTO fit_holds VALUES (?,?,?,?)',(owner,instance,kind,locator))
            elif (existing['kind'] == 'pending_table' and kind == 'table' and locator
                    and json.loads(existing['target']) == dict(provenance=target['provenance'], receipt=None)):
                db.execute('UPDATE fit_holds SET kind=?,target=? WHERE owner=? AND instance=?',
                           (kind,locator,owner,instance))
            elif (existing['kind'] == 'table' and kind == 'pending_table' and locator
                    and json.loads(existing['target'])['provenance'] == target['provenance']):
                return  # Final ownership survives replay of the original preparation.
            elif tuple(existing) != (kind,locator):
                raise FitConflict("Published model reference was changed")

    def release_active(self, owner, instance):
        with self._write() as db:
            db.execute("DELETE FROM fit_holds WHERE owner=? AND instance=? AND kind='active'", (owner,instance))
