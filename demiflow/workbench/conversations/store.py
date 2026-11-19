"""Transactional intents, bounded event pages, leases and single-use responses.

DELETE journal mode avoids a growing WAL. max_page_count bounds the database;
rollback journal capacity is separately reserved by the deployment partition.
"""
from __future__ import annotations

from contextlib import contextmanager
from dataclasses import asdict
import json
import os
from pathlib import Path
import secrets
import sqlite3
import time

from demiflow.workbench.conversations.common import digest
from demiflow.workbench.conversations.common import identifier
from demiflow.workbench.conversations.common import integer
from demiflow.workbench.conversations.common import reject
from demiflow.workbench.conversations.common import text
from demiflow.workbench.conversations.common import wire
from demiflow.forge.schema import ForgeError

TERMINAL = {'completed', 'failed', 'cancelled', 'interrupted'}


class StorageUnavailable(ForgeError):
    """Control persistence failed, distinct from request/identity rejection."""


class Store:
    def __init__(self, root, limits, *, initialize=False):
        self.root, self.limits = Path(root), limits
        self.path = self.root / 'chat.sqlite3'
        if initialize:
            self.root.mkdir(mode=0o700, parents=True, exist_ok=True)
            if self.root.is_symlink() or self.root.stat().st_mode & 0o077:
                reject('Development state must be private (0700)')
            with self.connect() as db:
                db.executescript('''
                CREATE TABLE IF NOT EXISTS sessions(
                  id TEXT PRIMARY KEY, workspace TEXT NOT NULL, root TEXT NOT NULL,
                  provider TEXT NOT NULL, request_key TEXT NOT NULL, policy_hash TEXT NOT NULL,
                  title TEXT NOT NULL DEFAULT '', created REAL NOT NULL, revision INTEGER NOT NULL DEFAULT 0,
                  sequence INTEGER NOT NULL DEFAULT 0, bytes INTEGER NOT NULL DEFAULT 0,
                  provider_session TEXT, UNIQUE(workspace, request_key));
                CREATE TABLE IF NOT EXISTS turns(
                  id TEXT PRIMARY KEY, session TEXT NOT NULL, submission TEXT NOT NULL, body_hash TEXT NOT NULL,
                  text TEXT NOT NULL, state TEXT NOT NULL, dispatch TEXT NOT NULL, cleanup TEXT NOT NULL,
                  epoch TEXT NOT NULL, created REAL NOT NULL, stop_at REAL,
                  bytes INTEGER NOT NULL DEFAULT 0, events INTEGER NOT NULL DEFAULT 0,
                  reserve INTEGER NOT NULL, provider_turn TEXT, process TEXT, context TEXT,
                  delivery TEXT NOT NULL DEFAULT 'prepared', baseline TEXT, result TEXT,
                  UNIQUE(session, submission));
                CREATE TABLE IF NOT EXISTS events(
                  session TEXT NOT NULL, sequence INTEGER NOT NULL, turn TEXT NOT NULL,
                  size INTEGER NOT NULL, body TEXT NOT NULL, PRIMARY KEY(session, sequence));
                CREATE TABLE IF NOT EXISTS pending(
                  id TEXT PRIMARY KEY, turn TEXT NOT NULL, epoch TEXT NOT NULL, digest TEXT NOT NULL,
                  request TEXT NOT NULL, expires REAL NOT NULL, answer TEXT, status TEXT NOT NULL);
                CREATE TABLE IF NOT EXISTS project_conversations(workspace TEXT PRIMARY KEY, session TEXT NOT NULL UNIQUE);
                CREATE TABLE IF NOT EXISTS project_conversation_members(
                  workspace TEXT NOT NULL, session TEXT NOT NULL UNIQUE,
                  PRIMARY KEY(workspace,session));
                CREATE INDEX IF NOT EXISTS events_turn ON events(turn,sequence);
                CREATE INDEX IF NOT EXISTS turns_session ON turns(session, created);
                CREATE INDEX IF NOT EXISTS pending_turn ON pending(turn);
                ''')
                if 'metadata_bytes' not in {r[1] for r in db.execute('PRAGMA table_info(sessions)')}:
                    # Preserve pre-release histories; account for their existing
                    # payloads once instead of rescanning wide text on admission.
                    db.execute('BEGIN IMMEDIATE')
                    db.execute('ALTER TABLE sessions ADD COLUMN metadata_bytes INTEGER NOT NULL DEFAULT 0')
                    db.execute('''UPDATE sessions SET metadata_bytes =
                      coalesce((SELECT sum(length(CAST(text AS BLOB)) +
                        coalesce(length(CAST(context AS BLOB)),0) + coalesce(length(CAST(baseline AS BLOB)),0) +
                        coalesce(length(CAST(process AS BLOB)),0) + coalesce(length(CAST(result AS BLOB)),0))
                        FROM turns WHERE session=sessions.id),0) +
                      coalesce((SELECT sum(length(CAST(p.request AS BLOB)) + coalesce(length(CAST(p.answer AS BLOB)),0))
                        FROM pending p JOIN turns t ON p.turn=t.id WHERE t.session=sessions.id),0)''')
                    db.commit()
                if 'launch_phase' not in {r[1] for r in db.execute('PRAGMA table_info(turns)')}:
                    # Legacy missing process metadata is not proof of no spawn.
                    db.execute("ALTER TABLE turns ADD COLUMN launch_phase TEXT NOT NULL DEFAULT 'unknown'")
                if 'operation' not in {r[1] for r in db.execute('PRAGMA table_info(turns)')}:
                    db.execute("ALTER TABLE turns ADD COLUMN operation TEXT NOT NULL DEFAULT 'message'")
                columns = {r[1] for r in db.execute('PRAGMA table_info(turns)')}
                for name, definition in {'development': 'TEXT', 'development_control': 'TEXT', 'gate_process': 'TEXT',
                        'gate_phase': "TEXT NOT NULL DEFAULT 'none'",
                        'gate_cleanup': "TEXT NOT NULL DEFAULT 'complete'"}.items():
                    if name not in columns:
                        db.execute(f'ALTER TABLE turns ADD COLUMN {name} {definition}')
            os.chmod(self.path, 0o600)
            reserve = self.root / 'control.reserve'
            if not reserve.exists() and not (self.root / 'storage-pressure').exists():
                fd = os.open(reserve, os.O_RDWR | os.O_CREAT | os.O_EXCL, 0o600)
                try: os.posix_fallocate(fd, 0, self.limits.control_reserve)
                finally: os.close(fd)

    @contextmanager
    def connect(self, *, write=False):
        db = None
        try:
            db = sqlite3.connect(self.path, timeout=2, isolation_level=None)
            db.row_factory = sqlite3.Row
            db.execute('PRAGMA journal_mode=DELETE')
            db.execute('PRAGMA synchronous=FULL')
            db.execute('PRAGMA cache_size=-1024')
            db.execute('PRAGMA temp_store=FILE')
            pages = max(32, self.limits.database_bytes // 4096)
            if db.execute('PRAGMA page_size').fetchone()[0] != 4096:
                reject('Development database page size is not supported', 'resource_limit')
            if db.execute(f'PRAGMA max_page_count={pages}').fetchone()[0] > pages:
                reject('Existing development database exceeds configured budget', 'resource_limit')
            if write:
                db.execute('BEGIN IMMEDIATE')
            yield db
            if write:
                db.commit()
        except sqlite3.DatabaseError as exc:
            if db is not None and db.in_transaction:
                try: db.rollback()
                except sqlite3.DatabaseError: pass
            if getattr(exc, 'sqlite_errorcode', None) == sqlite3.SQLITE_FULL:
                try: (self.root / 'control.reserve').unlink()
                except FileNotFoundError: pass
                try: (self.root / 'storage-pressure').touch(mode=0o600, exist_ok=True)
                except OSError: pass
            raise StorageUnavailable('Development database unavailable: ' + str(exc)[:120], code='busy') from exc
        finally:
            if db is not None: db.close()

    def _session(self, db, workspace, sid):
        row = db.execute('SELECT * FROM sessions WHERE id=? AND workspace=?', (identifier(sid), workspace)).fetchone()
        if row is None:
            reject('Development session not found', 'not_found')
        return dict(row)

    def create(self, workspace, identity, provider, request_id, policy_hash):
        identifier(request_id)
        with self.connect(write=True) as db:
            old = db.execute('SELECT * FROM sessions WHERE workspace=? AND request_key=?', (workspace, request_id)).fetchone()
            if old:
                if old['provider'] != provider or old['policy_hash'] != policy_hash:
                    reject('Session idempotency key has different content', 'conflict')
                return dict(old)
            if db.execute('SELECT count(*) FROM sessions').fetchone()[0] >= self.limits.sessions:
                reject('Session count budget exhausted', 'resource_limit')
            sid = 'd' + secrets.token_hex(16)
            db.execute('INSERT INTO sessions(id,workspace,root,provider,request_key,policy_hash,created) VALUES(?,?,?,?,?,?,?)',
                       (sid, workspace, wire(identity).decode(), provider, request_id, policy_hash, time.time()))
            return self._session(db, workspace, sid)

    def _primary(self, db, workspace, identity, policy_hash):
        pinned = db.execute('SELECT session FROM project_conversations WHERE workspace=?', (workspace,)).fetchone()
        if pinned:
            return self._session(db, workspace, pinned['session'])
        # Old records remain unchanged. Prefer the newest usable conversation
        # with messages over an unused empty session; old grants never upgrade.
        row = db.execute("""SELECT * FROM sessions WHERE workspace=? AND root=? AND policy_hash=?
            AND NOT EXISTS(SELECT 1 FROM project_conversation_members m WHERE m.workspace=sessions.workspace AND m.session=sessions.id)
            ORDER BY (revision>0) DESC,coalesce((SELECT max(created) FROM turns WHERE session=sessions.id),created) DESC,id LIMIT 1""",
            (workspace, wire(identity).decode(), policy_hash)).fetchone()
        return dict(row) if row else None

    def primary(self, workspace, identity, policy_hash):
        with self.connect() as db:
            return self._primary(db, workspace, identity, policy_hash)

    def ensure_project(self, workspace, identity, provider, request_id, policy_hash):
        identifier(request_id)
        with self.connect(write=True) as db:
            current = self._primary(db, workspace, identity, policy_hash)
            if current:
                if (current['provider'] != provider or current['policy_hash'] != policy_hash
                        or json.loads(current['root']) != identity):
                    reject('Project conversation provider, policy or root changed', 'conflict')
                sid = current['id']
            else:
                if db.execute('SELECT count(*) FROM sessions').fetchone()[0] >= self.limits.sessions:
                    reject('Session count budget exhausted', 'resource_limit')
                sid = 'd' + secrets.token_hex(16)
                db.execute('INSERT INTO sessions(id,workspace,root,provider,request_key,policy_hash,created) VALUES(?,?,?,?,?,?,?)',
                           (sid,workspace,wire(identity).decode(),provider,'project-'+secrets.token_hex(16),policy_hash,time.time()))
            db.execute('INSERT OR IGNORE INTO project_conversations(workspace,session) VALUES(?,?)',(workspace,sid))
            return self._session(db,workspace,sid)

    def create_conversation(self, workspace, identity, provider, request_id, policy_hash):
        """Explicit writable membership is created with the session, never inferred from history."""
        identifier(request_id)
        with self.connect(write=True) as db:
            old = db.execute('SELECT * FROM sessions WHERE workspace=? AND request_key=?',
                             (workspace, request_id)).fetchone()
            if old:
                member = db.execute('SELECT 1 FROM project_conversation_members WHERE workspace=? AND session=?',
                                    (workspace, old['id'])).fetchone()
                if (not member or old['provider'] != provider or old['policy_hash'] != policy_hash
                        or json.loads(old['root']) != identity):
                    reject('Session idempotency key has different content or membership', 'conflict')
                return dict(old)
            if db.execute('SELECT count(*) FROM sessions').fetchone()[0] >= self.limits.sessions:
                reject('Session count budget exhausted', 'resource_limit')
            # Pin the old migration choice before adding an empty conversation.
            # Otherwise creating a member could silently replace an unpinned main.
            primary = self._primary(db, workspace, identity, policy_hash)
            sid = 'd' + secrets.token_hex(16)
            db.execute('INSERT INTO sessions(id,workspace,root,provider,request_key,policy_hash,created) VALUES(?,?,?,?,?,?,?)',
                       (sid, workspace, wire(identity).decode(), provider, request_id, policy_hash, time.time()))
            db.execute('INSERT INTO project_conversation_members(workspace,session) VALUES(?,?)', (workspace, sid))
            db.execute('INSERT OR IGNORE INTO project_conversations(workspace,session) VALUES(?,?)',
                       (workspace, primary['id'] if primary else sid))
            return self._session(db, workspace, sid)

    def writable(self, workspace, sid, identity, policy_hash, providers, *, pin=False):
        with self.connect(write=pin) as db:
            session = self._session(db, workspace, sid)
            primary = self._primary(db, workspace, identity, policy_hash)
            current = (session['policy_hash'] == policy_hash and json.loads(session['root']) == identity
                       and session['provider'] in providers)
            member = db.execute('SELECT 1 FROM project_conversation_members WHERE workspace=? AND session=?',
                                (workspace, sid)).fetchone()
            allowed = bool(current and (member or primary and primary['id'] == sid))
            if allowed and pin and primary:
                db.execute('INSERT OR IGNORE INTO project_conversations(workspace,session) VALUES(?,?)',
                           (workspace, primary['id']))
            return allowed

    def project_turns(self, workspace, offset=0, limit=20, kind="all", session_id=None):
        integer(offset,0,self.limits.sessions*self.limits.turns);integer(limit,1,20)
        if kind not in {'all','timeline'}:reject('Unknown history filter','invalid_request')
        predicate = "AND (t.operation='compact' OR json_extract(t.result,'$.changed_files')>0 OR EXISTS(SELECT 1 FROM events e WHERE e.turn=t.id AND json_extract(e.body,'$.type')='check.reference'))" if kind=='timeline' else '' 
        with self.connect() as db:
            args = [workspace]
            if session_id is not None:
                self._session(db, workspace, session_id)
                predicate += ' AND t.session=?'
                args.append(session_id)
            rows=db.execute(f"""SELECT t.id,t.session,t.created,t.state,t.cleanup,t.operation,
                substr(t.text,1,80) AS title,t.result,
                (SELECT min(sequence) FROM events WHERE turn=t.id) AS sequence,
                EXISTS(SELECT 1 FROM events WHERE turn=t.id AND json_extract(body,'$.type')='check.reference') AS checks
                FROM turns t JOIN sessions s ON s.id=t.session WHERE s.workspace=? {predicate}
                ORDER BY t.created DESC,t.id LIMIT ? OFFSET ?""",(*args,limit+1,offset)).fetchall()
            result=[]
            for row in rows[:limit]:
                value=dict(row);facts=json.loads(value.pop('result') or '{}')
                value.update(changed_files=facts.get('changed_files',0),revision=facts.get('revision'),compaction=facts.get('compaction'))
                result.append(value)
            return dict(turns=result,next_offset=offset+limit if len(rows)>limit else None)

    def list(self, workspace, offset=0, limit=20, *, identity=None, policy_hash=None, providers=()):
        integer(offset, 0, self.limits.sessions); integer(limit, 1, 20)
        with self.connect() as db:
            primary = self._primary(db, workspace, identity, policy_hash) if identity is not None else None
            rows = db.execute('''SELECT s.id,s.title,s.provider,s.created,s.revision,s.root,s.policy_hash,
                EXISTS(SELECT 1 FROM project_conversation_members m WHERE m.workspace=s.workspace AND m.session=s.id) AS member,
                t.id AS turn_id,t.state,t.cleanup,json_extract(t.development,'$.busy') AS development_busy
                FROM sessions s LEFT JOIN turns t ON t.id=(
                    SELECT id FROM turns WHERE session=s.id ORDER BY created DESC,id DESC LIMIT 1)
                WHERE s.workspace=? ORDER BY s.created DESC,s.id LIMIT ? OFFSET ?''',
                (workspace, limit + 1, offset)).fetchall()
            sessions = []
            for row in rows[:limit]:
                value = {k: row[k] for k in ('id', 'title', 'provider', 'created', 'revision')}
                value['writable'] = bool(identity is not None and json.loads(row['root']) == identity
                    and row['policy_hash'] == policy_hash and row['provider'] in providers
                    and (row['member'] or primary and primary['id'] == row['id']))
                value['turn'] = dict(id=row['turn_id'], state=row['state'], cleanup=row['cleanup'],
                    development=dict(busy=bool(row['development_busy']))) if row['turn_id'] else None
                sessions.append(value)
            busy, active_session_id = False, None
            if identity is not None:
                root = Path(identity['path'])
                for writer in db.execute('''SELECT s.id,s.workspace,s.root FROM turns t JOIN sessions s ON s.id=t.session
                        WHERE t.cleanup!='complete' OR t.gate_cleanup!='complete' OR t.gate_phase='repair_pending' '''):
                    other = Path(json.loads(writer['root'])['path'])
                    if root == other or root in other.parents or other in root.parents:
                        busy = True
                        if writer['workspace'] == workspace:
                            active_session_id = writer['id']
            return dict(sessions=sessions, next_offset=offset + limit if len(rows) > limit else None,
                        writer_busy=busy, active_session_id=active_session_id)

    def show(self, workspace, sid):
        with self.connect() as db:
            row = self._session(db, workspace, sid)
            turn = db.execute('SELECT * FROM turns WHERE session=? ORDER BY created DESC LIMIT 1', (sid,)).fetchone()
            row['turn'] = self.public_turn(dict(turn)) if turn else None
            if turn:
                row['pending'] = [dict(r) for r in db.execute('SELECT id,digest,request,expires,status FROM pending WHERE turn=? AND status IN (\'waiting\',\'responding\') LIMIT 4', (turn['id'],))]
                for pending in row['pending']:
                    pending['request'] = json.loads(pending['request'])
            archived = db.execute("""SELECT e.sequence FROM events e JOIN turns t ON t.id=e.turn
                WHERE e.session=? AND t.operation='compact' AND t.state='completed' AND t.cleanup='complete'
                  AND json_extract(e.body,'$.type')='compaction.completed'
                  AND json_extract(e.body,'$.data.status')='compacted'
                ORDER BY e.sequence DESC LIMIT 1""",(sid,)).fetchone()
            row['archive_sequence'] = archived['sequence'] if archived else 0
            row.pop('provider_session', None)
            return row

    @staticmethod
    def public_turn(turn):
        value = {k: v for k, v in turn.items() if k not in {'text', 'process', 'baseline', 'context', 'gate_process', 'gate_phase', 'development_control'}}
        value['development'] = json.loads(value['development']) if value.get('development') else None
        if value['development'] is not None and turn.get('development_control'):
            control = json.loads(turn['development_control'])
            value['development'].update(repair_round=control['repair_round'], review_attempts=control['review_attempts'],
                deadline=control['deadline'], remaining_repairs=3-control['repair_round'], segments=control['segments'])
        return value

    def session(self, sid):
        with self.connect() as db:
            row = db.execute('SELECT * FROM sessions WHERE id=?', (identifier(sid),)).fetchone()
            if row is None: reject('Unknown session', 'not_found')
            return dict(row)

    def turn(self, tid):
        with self.connect() as db:
            row = db.execute('SELECT * FROM turns WHERE id=?', (identifier(tid),)).fetchone()
            if row is None: reject('Unknown turn', 'not_found')
            return dict(row)

    def has_dispatched(self, sid):
        with self.connect() as db:
            return db.execute("SELECT 1 FROM turns WHERE session=? AND dispatch!='not_sent' LIMIT 1", (sid,)).fetchone() is not None

    def submission(self, sid, key):
        with self.connect() as db:
            row = db.execute('SELECT * FROM turns WHERE session=? AND submission=?', (sid, identifier(key))).fetchone()
            return dict(row) if row else None

    def submit(self, workspace, sid, submission_id, expected_revision, message, identity, policy_hash, operation="message", development=None):
        text(message); identifier(submission_id); integer(expected_revision, 0, 2**31)
        if operation not in {'message','compact'}:reject('Unknown conversation operation')
        hashed = digest({'text': message, **({'operation':operation} if operation!='message' else {})})
        with self.connect(write=True) as db:
            session = self._session(db, workspace, sid)
            old = db.execute('SELECT * FROM turns WHERE session=? AND submission=?', (sid, submission_id)).fetchone()
            if old:
                if old['body_hash'] != hashed: reject('Submission key has different text', 'conflict')
                return dict(old), False
            if session['revision'] != expected_revision or session['policy_hash'] != policy_hash or json.loads(session['root']) != identity:
                reject('Session policy, root or revision changed', 'conflict')
            active = db.execute("SELECT t.*,s.root FROM turns t JOIN sessions s ON s.id=t.session WHERE t.cleanup != 'complete' OR t.gate_cleanup != 'complete' OR t.gate_phase='repair_pending'").fetchall()
            if len(active) >= self.limits.active: reject('Development capacity reserved', 'busy')
            root = Path(identity['path'])
            for turn in active:
                other = Path(json.loads(turn['root'])['path'])
                if turn['session'] == sid or root == other or root in other.parents or other in root.parents:
                    reject('This physical project already has an active or unresolved writer', 'conflict')
            if db.execute('SELECT count(*) FROM turns WHERE session=?', (sid,)).fetchone()[0] >= self.limits.turns:
                reject('Turn count budget exhausted', 'resource_limit')
            reserve = (self.limits.output_bytes + self.limits.context_total * 4 +
                       32 * (32768 + 16384) + 32768 + 2 * 16384 + 32 * 16384)
            if development is not None: reserve += 96 * 16384
            for where, args, maximum in [('id=?', (sid,), self.limits.session_bytes), ('workspace=?', (workspace,), self.limits.project_bytes), ('1=1', (), self.limits.global_bytes)]:
                used = db.execute('SELECT coalesce(sum(bytes+metadata_bytes),0) FROM sessions WHERE ' + where, args).fetchone()[0]
                held = db.execute("SELECT coalesce(sum(t.reserve),0) FROM turns t WHERE (cleanup != 'complete' OR gate_cleanup != 'complete' OR gate_phase='repair_pending') AND session IN (SELECT id FROM sessions WHERE " + where + ')', args).fetchone()[0]
                if used + held + reserve > maximum: reject('Development history byte reservation exhausted', 'resource_limit')
            tid, epoch = 't' + secrets.token_hex(16), secrets.token_hex(16)
            db.execute('INSERT INTO turns(id,session,submission,body_hash,text,state,dispatch,cleanup,epoch,created,reserve,launch_phase) VALUES(?,?,?,?,?,?,?,?,?,?,?,?)',
                       (tid, sid, submission_id, hashed, message, 'preparing', 'not_sent', 'pending', epoch, time.time(), reserve, 'admitted'))
            db.execute('UPDATE sessions SET revision=revision+1,title=CASE WHEN title=\'\' THEN ? ELSE title END WHERE id=?', (message[:80], sid))
            db.execute('UPDATE sessions SET metadata_bytes=metadata_bytes+? WHERE id=?', (len(message.encode()), sid))
            db.execute('UPDATE turns SET operation=? WHERE id=?',(operation,tid))
            if development is not None:
                created = db.execute('SELECT created FROM turns WHERE id=?', (tid,)).fetchone()[0]
                control = dict(repair_round=0, review_attempts=0, deadline=created+self.limits.turn_seconds,
                    failure_signature=None, failure_repeats=0, feedback=None, segments=[])
                self._update(db, tid, {'development': wire(development, 16384).decode(),
                                     'development_control': wire(control, 32768).decode()})
            self._event(db,sid,tid,epoch,'message.user' if operation=='message' else 'compaction.requested',{'text':message})
            return dict(db.execute('SELECT * FROM turns WHERE id=?', (tid,)).fetchone()), True

    def _event(self, db, sid, tid, epoch, kind, data, *, control=False):
        turn = db.execute('SELECT bytes,events FROM turns WHERE id=? AND epoch=?', (tid, epoch)).fetchone()
        if turn is None: reject('Worker epoch is stale', 'conflict')
        seq = db.execute('SELECT sequence FROM sessions WHERE id=?', (sid,)).fetchone()[0] + 1
        value = dict(session_id=sid, turn_id=tid, sequence=seq, provider_epoch=epoch, type=kind, observed_at=time.time(), data=data)
        raw = wire(value, self.limits.event_bytes)
        if not control and (turn['bytes'] + len(raw) > self.limits.output_bytes or turn['events'] >= self.limits.events_per_turn):
            reject('Turn event budget exceeded', 'resource_limit')
        # Four provider segments and their gate states share this one bounded turn.
        maximum_control = 32
        if control and 'development_control' in {r[1] for r in db.execute('PRAGMA table_info(turns)')}:
            if db.execute('SELECT development_control FROM turns WHERE id=?', (tid,)).fetchone()[0]: maximum_control = 128
        if control and (len(raw) > 16384 or turn['events'] >= self.limits.events_per_turn + maximum_control):
            reject('Control event budget exceeded', 'resource_limit')
        db.execute('INSERT INTO events VALUES(?,?,?,?,?)', (sid, seq, tid, len(raw), raw.decode()))
        db.execute('UPDATE sessions SET sequence=?,bytes=bytes+? WHERE id=?', (seq, len(raw), sid))
        db.execute('UPDATE turns SET bytes=bytes+?,events=events+1 WHERE id=?', (len(raw), tid))
        return value

    def event(self, tid, kind, data, *, control=False):
        with self.connect(write=True) as db:
            t = db.execute('SELECT session,epoch FROM turns WHERE id=?', (tid,)).fetchone()
            return self._event(db, t['session'], tid, t['epoch'], kind, data, control=control)

    def update(self, tid, **changes):
        with self.connect(write=True) as db:
            self._update(db, tid, changes)

    def _update(self, db, tid, changes):
        allowed = {'state', 'dispatch', 'cleanup', 'provider_turn', 'process', 'context', 'delivery', 'baseline', 'result', 'launch_phase',
                   'development', 'development_control', 'gate_phase', 'gate_cleanup', 'gate_process'}
        if changes.keys() - allowed: raise ValueError('Unknown turn fields')
        old = dict(db.execute('SELECT * FROM turns WHERE id=?', (tid,)).fetchone())
        delta = 0
        for key in ('context', 'baseline', 'result', 'process', 'development', 'gate_process', 'development_control'):
            if key in changes:
                maximum = self.limits.context_total * 2 if key in {'context', 'baseline'} else 32768 if key == 'development_control' else 16384
                size = len(changes[key].encode())
                if size > maximum:
                    reject('Persistent turn metadata exceeds byte budget', 'resource_limit')
                delta += size - len((old[key] or '').encode())
        # A concurrent cancellation cannot be overwritten by running/awaiting.
        if old['stop_at'] and changes.get('state') not in TERMINAL:
            changes['state'] = 'stopping'
        db.execute('UPDATE turns SET ' + ','.join(k + '=?' for k in changes) + ' WHERE id=?', (*changes.values(), tid))
        if delta:
            db.execute('UPDATE sessions SET metadata_bytes=metadata_bytes+? WHERE id=?', (delta, old['session']))
        if 'state' in changes or 'delivery' in changes:
            self._event(db, old['session'], tid, old['epoch'], 'turn.state', {k: v for k, v in changes.items() if k not in {'context', 'baseline', 'process', 'development', 'development_control', 'gate_process', 'gate_phase'}}, control=True)
        if 'development' in changes:
            self._event(db, old['session'], tid, old['epoch'], 'development.state', json.loads(changes['development']), control=True)
        if changes.get('cleanup') == 'complete':
            db.execute("UPDATE pending SET status='expired' WHERE turn=? AND status IN ('waiting','responding')", (tid,))

    def claim_worker(self, tid, epoch, unit_identity, supervisor):
        """A worker must bind the inherited fence before any tool can execute."""
        with self.connect(write=True) as db:
            turn = dict(db.execute('SELECT * FROM turns WHERE id=?', (tid,)).fetchone())
            ownership = json.loads(turn['process'] or '{}')
            if (turn['epoch'] != epoch or turn['launch_phase'] != 'spawning' or turn['cleanup'] == 'complete'
                    or ownership.get('nonce') != epoch or ownership.get('unit_identity') != unit_identity):
                reject('Worker launch identity is stale', 'conflict')
            ownership['supervisor'] = supervisor
            self._update(db, tid, dict(process=wire(ownership).decode(), launch_phase='claimed', state='starting'))

    def finish_provider(self, tid, *, state, cleanup, result):
        """The provider drain receipt and managed gate outbox commit together."""
        with self.connect(write=True) as db:
            turn = dict(db.execute('SELECT * FROM turns WHERE id=?', (tid,)).fetchone())
            changes = dict(state=state, cleanup=cleanup, result=wire(result).decode())
            if turn.get('development'):
                development = json.loads(turn['development'])
                eligible = (cleanup == 'complete' and state == 'completed' and not turn['stop_at']
                            and result.get('provider_completed') and turn['delivery'] == turn['dispatch'] == 'acknowledged')
                uncertain = cleanup != 'complete' or turn['delivery'] == 'unknown' or turn['dispatch'] == 'unknown'
                status = ('queued' if eligible else 'cancelled' if turn['stop_at'] or state == 'cancelled'
                          else 'unknown' if uncertain else 'failed')
                changes.update(development=wire(dict(development, state=status, busy=eligible or cleanup != 'complete',
                    reason=None if eligible else result.get('reason') or 'Provider did not produce a drained completed candidate')).decode(),
                    gate_phase='queued' if eligible else 'none', gate_cleanup='pending' if eligible else 'complete')
                if turn.get('development_control'):
                    control = json.loads(turn['development_control'])
                    receipt = dict(round=control['repair_round'], epoch=turn['epoch'], provider_turn=turn['provider_turn'],
                        state=state, dispatch=turn['dispatch'], cleanup=cleanup, context_digest=(json.loads(turn['context'] or '{}')).get('digest'),
                        reason=result.get('reason'), execution_assurance=result.get('execution_assurance', 'gate_only'),
                        changed_files=result.get('changed_files', 0), source_revision=result.get('revision'))
                    if not any(item['epoch'] == turn['epoch'] for item in control['segments']):
                        control['segments'].append(receipt)
                    changes['development_control'] = wire(control, 32768).decode()
            self._update(db, tid, changes)

    def claim_gate(self, tid, epoch, unit_identity, supervisor):
        with self.connect(write=True) as db:
            turn = dict(db.execute('SELECT * FROM turns WHERE id=?', (tid,)).fetchone())
            ownership = json.loads(turn['gate_process'] or '{}')
            if (turn['epoch'] != epoch or turn['gate_phase'] != 'spawning' or turn['gate_cleanup'] == 'complete'
                    or turn['cleanup'] != 'complete' or ownership.get('nonce') != epoch
                    or ownership.get('unit_identity') != unit_identity):
                reject('Development gate launch identity is stale', 'conflict')
            ownership['supervisor'] = supervisor
            self._update(db, tid, dict(gate_process=wire(ownership).decode(), gate_phase='claimed'))

    def gate_state(self, tid, development, *, phase=None, cleanup=None):
        with self.connect(write=True) as db:
            turn = dict(db.execute('SELECT * FROM turns WHERE id=?', (tid,)).fetchone())
            if turn['stop_at'] and cleanup is None:
                reject('Development gate was cancelled', 'conflict')
            if cleanup is None:
                development = dict(development, busy=True)
            changes = {'development': wire(development, 16384).decode()}
            if phase is not None: changes['gate_phase'] = phase
            if cleanup is not None: changes['gate_cleanup'] = cleanup
            self._update(db, tid, changes)

    def gate_pending(self):
        with self.connect() as db:
            return [dict(row) for row in db.execute("SELECT * FROM turns WHERE gate_cleanup != 'complete' OR gate_phase='repair_pending' ORDER BY created LIMIT ?", (self.limits.active + 1,))]

    def accept_drained_gate(self, tid, accept, *, drained):
        """Caller proved gate-tree drain; serialize acceptance against cancellation.

        The domain receipt and SQLite cannot commit atomically. Consumers must
        require this committed turn receipt as well as the domain fingerprint.
        A crash between those writes therefore remains closed, never replayed.
        """
        if drained is not True:
            reject('Acceptance requires confirmed owned-process cleanup', 'conflict')
        with self.connect(write=True) as db:
            turn = dict(db.execute('SELECT * FROM turns WHERE id=?', (tid,)).fetchone())
            if (turn['gate_phase'] != 'ready' or turn['cleanup'] != 'complete'
                    or turn['dispatch'] != 'acknowledged' or turn['delivery'] != 'acknowledged'):
                reject('Acceptance requires a completed provider and ready gate', 'conflict')
            value = json.loads(turn['development'])
            deadline = json.loads(turn.get('development_control') or '{}').get('deadline', turn['created']+self.limits.turn_seconds)
            if turn['stop_at']:
                value = dict(value, state='cancelled', busy=False, reason='stop_requested')
            elif time.time() >= deadline:
                value = dict(value, state='unknown', busy=False, reason='Development request deadline exhausted before acceptance')
            else:
                try:
                    value = accept(value)
                except Exception as exc:
                    value = dict(value, state='unknown', busy=False, reason=(type(exc).__name__+': '+str(exc))[:2048])
            self._update(db, tid, dict(development=wire(value, 16384).decode(), gate_phase='finished', gate_cleanup='complete'))
            return value

    def claim_review(self, tid, fingerprint):
        """Persist the possibly paid request boundary before contacting a reviewer."""
        with self.connect(write=True) as db:
            turn = dict(db.execute('SELECT * FROM turns WHERE id=?', (tid,)).fetchone())
            control = json.loads(turn['development_control'])
            if turn['stop_at'] or control['deadline'] <= time.time() or control['review_attempts'] >= 2:
                return False
            control['review_attempts'] += 1
            control['review_fingerprint'] = fingerprint
            self._update(db, tid, dict(development_control=wire(control, 32768).decode(), gate_phase='reviewing'))
            return True

    def schedule_repair(self, tid, development, feedback):
        """Only a determined failed gate can produce a bounded continuation."""
        with self.connect(write=True) as db:
            turn = dict(db.execute('SELECT * FROM turns WHERE id=?', (tid,)).fetchone())
            control = json.loads(turn['development_control'])
            feedback = json.loads(wire(feedback, 12000))
            # Log invocation paths and excerpt formatting are observations, not
            # implementation progress. Same source and failure reasons stop twice.
            reasons = [{key: issue.get(key) for key in ('rule_id', 'path', 'reason')}
                       for issue in feedback]
            signature = digest(dict(fingerprint=development.get('source_fingerprint'), feedback=reasons))
            repeats = control['failure_repeats'] + 1 if control['failure_signature'] == signature else 1
            control.update(failure_signature=signature, failure_repeats=repeats, feedback=feedback)
            review_exhausted = development.get('gates', {}).get('review') == 'failed' and control['review_attempts'] >= 2
            allowed = not turn['stop_at'] and control['deadline'] > time.time() and control['repair_round'] < 3 and repeats < 2 and not review_exhausted
            reason = ('Repeated identical failure without source changes' if repeats >= 2 else
                      'Independent review budget exhausted' if review_exhausted else
                      'Automatic repair budget exhausted' if control['repair_round'] >= 3 else
                      'Development request deadline exhausted' if control['deadline'] <= time.time() else
                      'stop_requested' if turn['stop_at'] else 'Controller is preparing the next bounded repair')
            value = dict(development, state='repair_pending' if allowed else 'cancelled' if turn['stop_at'] else 'failed',
                         busy=True, reason=reason)
            self._update(db, tid, dict(development=wire(value, 16384).decode(), development_control=wire(control, 32768).decode(),
                gate_phase='repair_pending' if allowed else 'finished'))
            return allowed

    def begin_repair(self, tid):
        """Move one fully drained gate to one provider segment without a new user turn."""
        with self.connect(write=True) as db:
            turn = dict(db.execute('SELECT * FROM turns WHERE id=?', (tid,)).fetchone())
            control = json.loads(turn['development_control'])
            if turn['gate_phase'] != 'repair_pending' or turn['gate_cleanup'] != 'complete' or turn['cleanup'] != 'complete':
                reject('Repair requires confirmed provider and gate cleanup', 'conflict')
            if turn['stop_at'] or control['deadline'] <= time.time() or control['repair_round'] >= 3:
                value = dict(json.loads(turn['development']), state='cancelled' if turn['stop_at'] else 'failed', busy=False,
                    reason='stop_requested' if turn['stop_at'] else 'Automatic repair budget exhausted')
                self._update(db, tid, dict(development=wire(value).decode(), gate_phase='finished'))
                return None
            control['repair_round'] += 1
            epoch = secrets.token_hex(16)
            db.execute('UPDATE turns SET epoch=? WHERE id=?', (epoch, tid))
            value = dict(json.loads(turn['development']), state='repairing', busy=True, reason=None)
            self._update(db, tid, dict(development=wire(value).decode(), development_control=wire(control, 32768).decode(),
                state='preparing', dispatch='not_sent', cleanup='pending', launch_phase='admitted',
                provider_turn='', process='', context='', baseline='', result='', delivery='prepared',
                gate_phase='none', gate_cleanup='complete', gate_process=''))
            self._event(db, turn['session'], tid, epoch, 'development.repair.started',
                        dict(round=control['repair_round'], feedback=control['feedback']), control=True)
            return dict(db.execute('SELECT * FROM turns WHERE id=?', (tid,)).fetchone())

    def interrupt_unresolved(self, tid):
        with self.connect(write=True) as db:
            turn = dict(db.execute('SELECT * FROM turns WHERE id=?', (tid,)).fetchone())
            # A surviving worker may finish while startup inventories its old
            # turns. Never overwrite its committed drain/acknowledgement proof.
            if turn['cleanup'] != 'complete':
                self._update(db, tid, dict(state='interrupted', cleanup='unknown',
                    dispatch='unknown' if turn['dispatch'] == 'sent' else turn['dispatch']))

    def finish_unstarted(self, saved):
        """Caller holds service lock and, for spawning, proved the unit unlocked."""
        with self.connect(write=True) as db:
            current = dict(db.execute('SELECT * FROM turns WHERE id=?', (saved['id'],)).fetchone())
            if current['cleanup'] == 'complete': return True
            if (saved['launch_phase'] not in {'admitted', 'spawning'} or
                    any(current[k] != saved[k] for k in ('epoch', 'launch_phase', 'process'))):
                return False
            changes = dict(state='cancelled' if current['stop_at'] else 'failed', cleanup='complete',
                result=wire(dict(reason='worker_not_started', cleanup='complete', proof='pre_spawn' if
                    saved['launch_phase'] == 'admitted' else 'unclaimed_unit_stopped')).decode())
            if current['development']:
                changes.update(development=wire(dict(json.loads(current['development']),
                    state='cancelled' if current['stop_at'] else 'failed', busy=False, reason='worker_not_started')).decode(),
                    gate_phase='none', gate_cleanup='complete')
            self._update(db, saved['id'], changes)
            return True

    def bind_provider(self, sid, provider_session):
        text(provider_session, 256)
        with self.connect(write=True) as db:
            db.execute('UPDATE sessions SET provider_session=? WHERE id=?', (provider_session, sid))

    def events(self, workspace, sid, after=0, limit=50):
        integer(after, 0, 2**53); integer(limit, 1, 50)
        with self.connect() as db:
            session = self._session(db, workspace, sid)
            if after > session['sequence']: reject('Event cursor exceeds saved history', 'conflict')
            rows, used = [], 0
            for row in db.execute('SELECT body,size FROM events WHERE session=? AND sequence>? ORDER BY sequence LIMIT ?', (sid, after, limit)):
                if used + row['size'] > 256 * 1024: break
                rows.append(json.loads(row['body'])); used += row['size']
            return dict(events=rows, next_sequence=rows[-1]['sequence'] if rows else after, last_sequence=session['sequence'])

    def cancel(self, workspace, sid, tid):
        with self.connect(write=True) as db:
            self._session(db, workspace, sid)
            row = db.execute('SELECT * FROM turns WHERE id=? AND session=?', (identifier(tid), sid)).fetchone()
            if row is None: reject('Turn does not belong to session', 'not_found')
            if (row['cleanup'] != 'complete' or row['gate_cleanup'] != 'complete' or row['gate_phase'] == 'repair_pending') and row['stop_at'] is None:
                db.execute('UPDATE turns SET stop_at=? WHERE id=?', (time.time(), tid))
                changes = {'state': 'stopping'} if row['cleanup'] != 'complete' else {}
                if row['development']:
                    development = json.loads(row['development'])
                    changes['development'] = wire(dict(development, state='stopping', busy=True, reason='stop_requested')).decode()
                if changes: self._update(db, tid, changes)
            return self.public_turn(dict(db.execute('SELECT * FROM turns WHERE id=?', (tid,)).fetchone()))

    def request(self, tid, request):
        raw = wire(request, 32768).decode(); hashed = digest(request)
        with self.connect(write=True) as db:
            turn = dict(db.execute('SELECT * FROM turns WHERE id=?', (tid,)).fetchone())
            if turn['stop_at'] or turn['cleanup'] == 'complete': reject('Turn no longer accepts input', 'conflict')
            if db.execute("SELECT count(*) FROM pending WHERE turn=? AND status IN ('waiting','responding')", (tid,)).fetchone()[0] >= 4:
                reject('Too many pending requests', 'resource_limit')
            for previous in db.execute('SELECT request FROM pending WHERE turn=? LIMIT 32', (tid,)):
                if json.loads(previous['request']).get('rpc_id') == request.get('rpc_id'):
                    reject('Provider reused an input request identity', 'conflict')
            if db.execute('SELECT count(*) FROM pending WHERE turn=?', (tid,)).fetchone()[0] >= 32:
                reject('Turn request count exceeded budget', 'resource_limit')
            key = 'q' + secrets.token_hex(16)
            expires = min(time.time() + self.limits.input_seconds, turn['created'] + self.limits.turn_seconds)
            db.execute('INSERT INTO pending VALUES(?,?,?,?,?,?,?,?)', (key, tid, turn['epoch'], hashed, raw, expires, None, 'waiting'))
            db.execute('UPDATE sessions SET metadata_bytes=metadata_bytes+? WHERE id=?', (len(raw.encode()), turn['session']))
            self._event(db, turn['session'], tid, turn['epoch'], 'input.requested', dict(id=key, digest=hashed, request=request, expires=expires))
            return key

    def dispatch_provider_reply(self, tid, epoch, send):
        """Serialize a bounded transport send against stop and epoch changes."""
        with self.connect(write=True) as db:
            turn = dict(db.execute('SELECT * FROM turns WHERE id=?', (tid,)).fetchone())
            deadline = json.loads(turn.get('development_control') or '{}').get('deadline', turn['created']+self.limits.turn_seconds)
            if turn['epoch'] != epoch or turn['stop_at'] or turn['cleanup'] == 'complete' or time.time() >= deadline:
                return False
            send()
            return True

    def respond(self, workspace, sid, tid, pending_id, expected_digest, answer):
        raw = wire(answer, 16384).decode()
        with self.connect(write=True) as db:
            self._session(db, workspace, sid)
            turn = db.execute('SELECT * FROM turns WHERE id=? AND session=?', (tid, sid)).fetchone()
            row = db.execute('SELECT * FROM pending WHERE id=? AND turn=?', (identifier(pending_id), tid)).fetchone()
            if turn is None or row is None or row['digest'] != expected_digest or row['epoch'] != turn['epoch']:
                reject('Input identity mismatch', 'conflict')
            if row['answer'] is not None:
                if row['answer'] != raw: reject('Input already has a different answer', 'conflict')
                return dict(id=pending_id, status=row['status'])
            if row['expires'] < time.time() or turn['stop_at'] or turn['cleanup'] == 'complete' or row['status'] != 'waiting':
                reject('Input request expired', 'conflict')
            db.execute("UPDATE pending SET answer=?,status='responding' WHERE id=?", (raw, pending_id))
            db.execute('UPDATE sessions SET metadata_bytes=metadata_bytes+? WHERE id=?', (len(raw.encode()), sid))
            return dict(id=pending_id, status='responding')

    def pending(self, tid):
        with self.connect() as db:
            return [dict(row) for row in db.execute("SELECT * FROM pending WHERE turn=? AND status IN ('waiting','responding') LIMIT 4", (tid,))]

    def resolved(self, key):
        with self.connect(write=True) as db:
            row = db.execute('SELECT * FROM pending WHERE id=?', (key,)).fetchone()
            turn = db.execute('SELECT * FROM turns WHERE id=?', (row['turn'],)).fetchone()
            db.execute("UPDATE pending SET status='delivered' WHERE id=?", (key,))
            self._event(db, turn['session'], turn['id'], turn['epoch'], 'input.resolved', dict(id=key, status='delivered'), control=True)

    def unresolved(self):
        with self.connect() as db:
            return [dict(row) for row in db.execute("SELECT * FROM turns WHERE cleanup != 'complete' LIMIT ?", (self.limits.active + 1,))]
