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


# ':' was never valid in a client request key. This reserves a namespace for
# new card sessions, including empty ones. Pure cell histories from older
# releases can be recognized without modifying any saved session or event.
_SCOPED_SESSIONS = """(SELECT s.*, CASE WHEN s.request_key GLOB 'notebook:*'
    OR (EXISTS (SELECT 1 FROM turns WHERE session=s.id)
        AND NOT EXISTS (SELECT 1 FROM turns WHERE session=s.id
            AND operation NOT IN ('notebook_cell','notebook_cell_cli')))
    THEN 'notebook' ELSE 'development' END AS session_kind FROM sessions s)"""


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
                fresh = db.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='turns'").fetchone() is None
                if not fresh and db.execute('PRAGMA user_version').fetchone()[0] != 1:
                    reject('Development state requires a fresh directory for this schema', 'conflict')
                if fresh:
                    db.executescript('''
                BEGIN IMMEDIATE;
                CREATE TABLE IF NOT EXISTS sessions(
                  id TEXT PRIMARY KEY, workspace TEXT NOT NULL, root TEXT NOT NULL,
                  provider TEXT NOT NULL, request_key TEXT NOT NULL, policy_hash TEXT NOT NULL,
                  title TEXT NOT NULL DEFAULT '', created REAL NOT NULL, revision INTEGER NOT NULL DEFAULT 0,
                  sequence INTEGER NOT NULL DEFAULT 0, bytes INTEGER NOT NULL DEFAULT 0,
                  provider_session TEXT, metadata_bytes INTEGER NOT NULL DEFAULT 0, UNIQUE(workspace, request_key));
                CREATE TABLE IF NOT EXISTS turns(
                  id TEXT PRIMARY KEY, session TEXT NOT NULL, submission TEXT NOT NULL, body_hash TEXT NOT NULL,
                  text TEXT NOT NULL, state TEXT NOT NULL, dispatch TEXT NOT NULL, cleanup TEXT NOT NULL,
                  epoch TEXT NOT NULL, created REAL NOT NULL, stop_at REAL,
                  bytes INTEGER NOT NULL DEFAULT 0, events INTEGER NOT NULL DEFAULT 0,
                  reserve INTEGER NOT NULL, provider_turn TEXT, process TEXT, context TEXT,
                  delivery TEXT NOT NULL DEFAULT 'prepared', baseline TEXT, result TEXT,
                  launch_phase TEXT NOT NULL, operation TEXT NOT NULL,
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
                PRAGMA user_version=1;
                COMMIT;
                ''')
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
        row = db.execute(f'SELECT * FROM {_SCOPED_SESSIONS} WHERE id=? AND workspace=?', (identifier(sid), workspace)).fetchone()
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
            current = self._session(db, workspace, pinned['session'])
            if current['session_kind'] == 'development':
                return current
        # Old records remain unchanged. Prefer the newest usable conversation
        # with messages over an unused empty session; old grants never upgrade.
        row = db.execute(f"""SELECT * FROM {_SCOPED_SESSIONS} sessions WHERE workspace=? AND root=? AND policy_hash=? AND session_kind='development'
            AND NOT EXISTS(SELECT 1 FROM project_conversation_members m WHERE m.workspace=sessions.workspace AND m.session=sessions.id)
            ORDER BY (revision>0) DESC,coalesce((SELECT max(created) FROM turns WHERE session=sessions.id),created) DESC,id LIMIT 1""",
            (workspace, wire(identity).decode(), policy_hash)).fetchone()
        return dict(row) if row else None

    def primary(self, workspace, identity, policy_hash):
        with self.connect() as db:
            return self._primary(db, workspace, identity, policy_hash)

    def _pin_development(self, db, workspace, sid):
        previous = db.execute('SELECT session FROM project_conversations WHERE workspace=?', (workspace,)).fetchone()
        if previous:
            if previous['session'] == sid:
                return
            # An older card could have occupied the default slot. Preserve
            # that explicit grant when the first development write replaces it.
            if self._session(db, workspace, previous['session'])['session_kind'] != 'notebook':
                return
            db.execute('INSERT OR IGNORE INTO project_conversation_members(workspace,session) VALUES(?,?)',
                       (workspace, previous['session']))
        db.execute("""INSERT INTO project_conversations(workspace,session) VALUES(?,?)
            ON CONFLICT(workspace) DO UPDATE SET session=excluded.session""", (workspace, sid))

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
            self._pin_development(db, workspace, sid)
            return self._session(db,workspace,sid)

    def create_conversation(self, workspace, identity, provider, request_id, policy_hash, *, session_kind="development"):
        """Explicit writable membership is created with the session, never inferred from history."""
        identifier(request_id)
        card_key = 'notebook:' + request_id
        with self.connect(write=True) as db:
            old = db.execute(f'SELECT * FROM {_SCOPED_SESSIONS} WHERE workspace=? AND request_key IN (?,?)',
                             (workspace, request_id, card_key)).fetchone()
            if old:
                member = db.execute('SELECT 1 FROM project_conversation_members WHERE workspace=? AND session=?',
                                    (workspace, old['id'])).fetchone()
                if (not member or old['session_kind'] != session_kind or old['provider'] != provider or old['policy_hash'] != policy_hash
                        or json.loads(old['root']) != identity):
                    reject('Session idempotency key has different content or membership', 'conflict')
                return dict(old)
            if db.execute('SELECT count(*) FROM sessions').fetchone()[0] >= self.limits.sessions:
                reject('Session count budget exhausted', 'resource_limit')
            # Pin the old migration choice before adding an empty conversation.
            # Otherwise creating a member could silently replace an unpinned main.
            primary = self._primary(db, workspace, identity, policy_hash) if session_kind == 'development' else None
            key = card_key if session_kind == 'notebook' else request_id
            sid = 'd' + secrets.token_hex(16)
            db.execute('INSERT INTO sessions(id,workspace,root,provider,request_key,policy_hash,created) VALUES(?,?,?,?,?,?,?)',
                       (sid, workspace, wire(identity).decode(), provider, key, policy_hash, time.time()))
            db.execute('INSERT INTO project_conversation_members(workspace,session) VALUES(?,?)', (workspace, sid))
            if session_kind == 'development':
                self._pin_development(db, workspace, primary['id'] if primary else sid)
            return self._session(db, workspace, sid)

    def writable(self, workspace, sid, identity, policy_hash, providers, *, pin=False):
        with self.connect(write=pin) as db:
            session = self._session(db, workspace, sid)
            primary = self._primary(db, workspace, identity, policy_hash)
            current = (session['policy_hash'] == policy_hash and json.loads(session['root']) == identity
                       and session['provider'] in providers)
            member = db.execute("""SELECT 1 FROM project_conversation_members WHERE workspace=? AND session=?
                UNION ALL SELECT 1 FROM project_conversations WHERE workspace=? AND session=?""",
                (workspace, sid, workspace, sid)).fetchone()
            allowed = bool(current and (member or primary and primary['id'] == sid))
            if allowed and pin and primary:
                self._pin_development(db, workspace, primary['id'])
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
                FROM turns t JOIN {_SCOPED_SESSIONS} s ON s.id=t.session WHERE s.workspace=? AND s.session_kind='development' {predicate}
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
            rows = db.execute(f'''SELECT s.id,s.title,s.provider,s.created,s.revision,s.root,s.policy_hash,
                EXISTS(SELECT 1 FROM project_conversation_members m WHERE m.workspace=s.workspace AND m.session=s.id) AS member,
                t.id AS turn_id,t.state,t.cleanup
                FROM {_SCOPED_SESSIONS} s LEFT JOIN turns t ON t.id=(
                    SELECT id FROM turns WHERE session=s.id ORDER BY created DESC,id DESC LIMIT 1)
                WHERE s.workspace=? AND s.session_kind='development' ORDER BY s.created DESC,s.id LIMIT ? OFFSET ?''',
                (workspace, limit + 1, offset)).fetchall()
            sessions = []
            for row in rows[:limit]:
                value = {k: row[k] for k in ('id', 'title', 'provider', 'created', 'revision')}
                value['writable'] = bool(identity is not None and json.loads(row['root']) == identity
                    and row['policy_hash'] == policy_hash and row['provider'] in providers
                    and (row['member'] or primary and primary['id'] == row['id']))
                value['turn'] = dict(id=row['turn_id'], state=row['state'], cleanup=row['cleanup']) if row['turn_id'] else None
                sessions.append(value)
            busy, active_session_id = False, None
            if identity is not None:
                root = Path(identity['path'])
                for writer in db.execute(f'''SELECT s.id,s.workspace,s.root,s.session_kind FROM turns t JOIN {_SCOPED_SESSIONS} s ON s.id=t.session
                        WHERE t.cleanup!='complete' '''):
                    other = Path(json.loads(writer['root'])['path'])
                    if root == other or root in other.parents or other in root.parents:
                        busy = True
                        if writer['workspace'] == workspace and writer['session_kind'] == 'development':
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
        value = {k: v for k, v in turn.items() if k not in {'text', 'process', 'baseline', 'context'}}
        return value

    def session(self, sid):
        with self.connect() as db:
            row = db.execute(f'SELECT * FROM {_SCOPED_SESSIONS} WHERE id=?', (identifier(sid),)).fetchone()
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

    def submit(self, workspace, sid, submission_id, expected_revision, message, identity, policy_hash, operation="message"):
        text(message); identifier(submission_id); integer(expected_revision, 0, 2**31)
        if operation not in {'message','compact','notebook_cell','notebook_cell_cli'}:reject('Unknown conversation operation')
        hashed = digest({'text': message, **({'operation':operation} if operation!='message' else {})})
        with self.connect(write=True) as db:
            session = self._session(db, workspace, sid)
            old = db.execute('SELECT * FROM turns WHERE session=? AND submission=?', (sid, submission_id)).fetchone()
            if old:
                if old['body_hash'] != hashed: reject('Submission key has different text', 'conflict')
                return dict(old), False
            if session['revision'] != expected_revision or session['policy_hash'] != policy_hash or json.loads(session['root']) != identity:
                reject('Session policy, root or revision changed', 'conflict')
            cell_turn = operation in ('notebook_cell', 'notebook_cell_cli')
            if cell_turn and session['session_kind'] == 'development' and session['revision']:
                reject('请从目标卡片开启独立会话。', 'conflict')
            if not cell_turn and session['session_kind'] == 'notebook':
                reject('此会话属于 Notebook 卡片。', 'conflict')
            active = db.execute("SELECT t.*,s.root FROM turns t JOIN sessions s ON s.id=t.session WHERE t.cleanup != 'complete'").fetchall()
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
            for where, args, maximum in [('id=?', (sid,), self.limits.session_bytes), ('workspace=?', (workspace,), self.limits.project_bytes), ('1=1', (), self.limits.global_bytes)]:
                used = db.execute('SELECT coalesce(sum(bytes+metadata_bytes),0) FROM sessions WHERE ' + where, args).fetchone()[0]
                held = db.execute("SELECT coalesce(sum(t.reserve),0) FROM turns t WHERE cleanup != 'complete' AND session IN (SELECT id FROM sessions WHERE " + where + ')', args).fetchone()[0]
                if used + held + reserve > maximum: reject('Development history byte reservation exhausted', 'resource_limit')
            tid, epoch = 't' + secrets.token_hex(16), secrets.token_hex(16)
            db.execute('INSERT INTO turns(id,session,submission,body_hash,text,state,dispatch,cleanup,epoch,created,reserve,launch_phase,operation) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)',
                       (tid, sid, submission_id, hashed, message, 'preparing', 'not_sent', 'pending', epoch, time.time(), reserve, 'admitted', operation))
            db.execute('UPDATE sessions SET revision=revision+1,title=CASE WHEN title=\'\' THEN ? ELSE title END WHERE id=?', (message[:80], sid))
            db.execute('UPDATE sessions SET metadata_bytes=metadata_bytes+? WHERE id=?', (len(message.encode()), sid))
            self._event(db,sid,tid,epoch,'compaction.requested' if operation=='compact' else 'message.user',{'text':message})
            return dict(db.execute('SELECT * FROM turns WHERE id=?', (tid,)).fetchone()), True

    def _event(self, db, sid, tid, epoch, kind, data, *, control=False):
        turn = db.execute('SELECT bytes,events FROM turns WHERE id=? AND epoch=?', (tid, epoch)).fetchone()
        if turn is None: reject('Worker epoch is stale', 'conflict')
        seq = db.execute('SELECT sequence FROM sessions WHERE id=?', (sid,)).fetchone()[0] + 1
        value = dict(session_id=sid, turn_id=tid, sequence=seq, provider_epoch=epoch, type=kind, observed_at=time.time(), data=data)
        raw = wire(value, self.limits.event_bytes)
        if not control and (turn['bytes'] + len(raw) > self.limits.output_bytes or turn['events'] >= self.limits.events_per_turn):
            reject('Turn event budget exceeded', 'resource_limit')
        if control and (len(raw) > 16384 or turn['events'] >= self.limits.events_per_turn + 32):
            reject('Control event budget exceeded', 'resource_limit')
        db.execute('INSERT INTO events VALUES(?,?,?,?,?)', (sid, seq, tid, len(raw), raw.decode()))
        db.execute('UPDATE sessions SET sequence=?,bytes=bytes+? WHERE id=?', (seq, len(raw), sid))
        db.execute('UPDATE turns SET bytes=bytes+?,events=events+1 WHERE id=?', (len(raw), tid))
        return value

    def event(self, tid, kind, data, *, control=False):
        with self.connect(write=True) as db:
            t = db.execute('SELECT session,epoch FROM turns WHERE id=?', (tid,)).fetchone()
            return self._event(db, t['session'], tid, t['epoch'], kind, data, control=control)

    def events_batch(self, tid, items):
        """Persist a bounded batch of events for one turn in a single transaction."""
        if not items:
            return []
        with self.connect(write=True) as db:
            t = db.execute('SELECT session,epoch FROM turns WHERE id=?', (tid,)).fetchone()
            if t is None:
                reject('Unknown turn', 'not_found')
            sid, epoch = t['session'], t['epoch']
            return [self._event(db, sid, tid, epoch, kind, data, control=control) for kind, data, control in items]

    def update(self, tid, **changes):
        with self.connect(write=True) as db:
            self._update(db, tid, changes)

    def _update(self, db, tid, changes):
        allowed = {'state', 'dispatch', 'cleanup', 'provider_turn', 'process', 'context', 'delivery', 'baseline', 'result', 'launch_phase'}
        if changes.keys() - allowed: raise ValueError('Unknown turn fields')
        old = dict(db.execute('SELECT * FROM turns WHERE id=?', (tid,)).fetchone())
        delta = 0
        for key in ('context', 'baseline', 'result', 'process'):
            if key in changes:
                maximum = self.limits.context_total * 2 if key in {'context', 'baseline'} else 16384
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
            self._event(db, old['session'], tid, old['epoch'], 'turn.state', {k: v for k, v in changes.items() if k not in {'context', 'baseline', 'process'}}, control=True)
        if changes.get('cleanup') == 'complete':
            db.execute("UPDATE pending SET status='expired' WHERE turn=? AND status IN ('waiting','claimed','responding')", (tid,))

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
        """Commit the drain receipt and any source checks in the same state transition."""
        with self.connect(write=True) as db:
            turn = dict(db.execute('SELECT * FROM turns WHERE id=?', (tid,)).fetchone())
            if turn['stop_at'] and 'pipeline_checks' in result:
                result = {key: value for key, value in result.items() if key != 'pipeline_checks'}
                state = 'cancelled'
            changes = dict(state=state, cleanup=cleanup, result=wire(result).decode())
            self._update(db, tid, changes)

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

    def replay(self, workspace, sid, *, limit=200, max_turns=12):
        """Return a bounded, display-compacted tail of recent turns without altering history."""
        integer(limit, 1, 200); integer(max_turns, 1, 32)
        with self.connect() as db:
            session = self._session(db, workspace, sid)
            last_seq = session['sequence']
            archived = db.execute("""SELECT e.sequence FROM events e JOIN turns t ON t.id=e.turn
                WHERE e.session=? AND t.operation='compact' AND t.state='completed' AND t.cleanup='complete'
                  AND json_extract(e.body,'$.type')='compaction.completed'
                  AND json_extract(e.body,'$.data.status')='compacted'
                ORDER BY e.sequence DESC LIMIT 1""", (sid,)).fetchone()
            archive_seq = archived['sequence'] if archived else 0
            turns = db.execute('SELECT id,cleanup FROM turns WHERE session=? ORDER BY created DESC,id DESC LIMIT ?',
                               (sid, max_turns + 1)).fetchall()
            older_turns = len(turns) > max_turns
            selected = turns[:max_turns]
            if not selected or last_seq == 0:
                return dict(events=[], first_sequence=last_seq, next_sequence=last_seq,
                            last_sequence=last_seq, has_earlier=bool(archive_seq > 0))
            active_turns = {row['id'] for row in selected if row['cleanup'] != 'complete'}
            oldest_seq_row = db.execute('SELECT min(sequence) FROM events WHERE turn=?', (selected[-1]['id'],)).fetchone()
            turn_start = oldest_seq_row[0] if oldest_seq_row and oldest_seq_row[0] is not None else 1
            start_after = max(archive_seq - 1 if archive_seq > 0 else 0, turn_start - 1, max(0, last_seq - 4096))
            truncated_scan = start_after > 0
            raw_rows = [(row['sequence'], row['size'], json.loads(row['body']))
                        for row in db.execute('SELECT sequence,size,body FROM events WHERE session=? AND sequence>? ORDER BY sequence LIMIT 4096',
                                              (sid, start_after))]
        # Compact display-redundant events in completed turns only; keep active turns verbatim.
        # 1) Identify message spans terminated by message.completed without intervening tool events.
        completed_msg_final = {}
        span_id = {}
        tool_first = {}
        tool_last = {}
        tool_has_delta = set()
        last_turn_state = {}
        span_counter = 0
        for seq, _, ev in raw_rows:
            tid = ev.get('turn_id')
            if tid in active_turns:
                continue
            kind = ev.get('type')
            data = ev.get('data') or {}
            epoch = ev.get('provider_epoch') or ''
            if kind in ('message.delta', 'message.completed'):
                mkey = (tid, epoch, data.get('channel') or 'answer', data.get('item_id'))
                sid_span = span_id.setdefault(mkey, span_counter)
                if kind == 'message.completed':
                    completed_msg_final[(mkey, sid_span)] = (seq, bool(data.get('paged')))
            else:
                if kind.startswith('item.') or kind == 'output.delta':
                    span_counter += 1
                    span_id.clear()
                if kind == 'item.updated':
                    tkey = (tid, epoch, data.get('id'))
                    tool_first.setdefault(tkey, seq)
                    tool_last[tkey] = seq
                elif kind == 'output.delta':
                    tool_has_delta.add((tid, epoch, data.get('item_id')))
                elif kind == 'turn.state':
                    last_turn_state[tid] = seq

        compacted = []
        span_id.clear()
        span_counter = 0
        for seq, size, ev in raw_rows:
            tid = ev.get('turn_id')
            if tid in active_turns:
                compacted.append((seq, size, ev))
                continue
            kind = ev.get('type')
            data = ev.get('data') or {}
            epoch = ev.get('provider_epoch') or ''
            if kind in ('usage.observed', 'context.bound', 'provider.bound'):
                continue
            if kind in ('message.delta', 'message.completed'):
                mkey = (tid, epoch, data.get('channel') or 'answer', data.get('item_id'))
                sid_span = span_id.setdefault(mkey, span_counter)
                final_info = completed_msg_final.get((mkey, sid_span))
                if final_info is not None:
                    final_seq, is_paged = final_info
                    if kind == 'message.delta':
                        continue
                    if seq != final_seq and not is_paged:
                        continue
                compacted.append((seq, size, ev))
                continue
            if kind.startswith('item.') or kind == 'output.delta':
                span_counter += 1
                span_id.clear()
            if kind == 'item.updated':
                tkey = (tid, epoch, data.get('id'))
                if tkey in tool_has_delta:
                    if seq != tool_first[tkey] and seq != tool_last[tkey]:
                        continue
                elif seq != tool_last[tkey]:
                    continue
            elif kind == 'turn.state' and seq != last_turn_state.get(tid):
                continue
            compacted.append((seq, size, ev))

        # Fit within limit and 256 KiB. If over budget, first drop non-latest completed
        # turns' process/tool rows while preserving each turn's user prompt & final answer.
        newest_tid = selected[0]['id']
        budget_bytes = 256 * 1024
        if len(compacted) > limit or sum(s for _, s, _ in compacted) > budget_bytes:
            final_answer_seq = {}
            for seq, _, ev in compacted:
                tid = ev.get('turn_id')
                data = ev.get('data') or {}
                if ev.get('type') in ('message.completed', 'message.delta') and data.get('channel') not in ('process', 'summary'):
                    final_answer_seq[tid] = seq
            pruned = []
            for entry in compacted:
                seq, _, ev = entry
                tid = ev.get('turn_id')
                kind = ev.get('type')
                data = ev.get('data') or {}
                if tid != newest_tid and tid not in active_turns:
                    if kind in ('item.updated', 'item.started', 'output.delta'):
                        continue
                    if kind in ('message.delta', 'message.completed') and (
                            data.get('channel') in ('process', 'summary') or seq != final_answer_seq.get(tid)):
                        continue
                pruned.append(entry)
            compacted = pruned

        kept, used, omitted = [], 0, False
        for seq, size, ev in reversed(compacted):
            if len(kept) >= limit or used + size > budget_bytes:
                omitted = True
                break
            kept.append(ev); used += size
        kept.reverse()
        first_seq = kept[0]['sequence'] if kept else last_seq
        has_earlier = bool(older_turns or truncated_scan or omitted or archive_seq > 0)
        return dict(events=kept, first_sequence=first_seq, next_sequence=last_seq,
                    last_sequence=last_seq, has_earlier=has_earlier)

    def cancel(self, workspace, sid, tid):
        with self.connect(write=True) as db:
            self._session(db, workspace, sid)
            row = db.execute('SELECT * FROM turns WHERE id=? AND session=?', (identifier(tid), sid)).fetchone()
            if row is None: reject('Turn does not belong to session', 'not_found')
            if row['cleanup'] != 'complete' and row['stop_at'] is None:
                db.execute('UPDATE turns SET stop_at=? WHERE id=?', (time.time(), tid))
                self._update(db, tid, dict(state='stopping'))
            return self.public_turn(dict(db.execute('SELECT * FROM turns WHERE id=?', (tid,)).fetchone()))

    def request(self, tid, request):
        raw = wire(request, 32768).decode(); hashed = digest(request)
        with self.connect(write=True) as db:
            turn = dict(db.execute('SELECT * FROM turns WHERE id=?', (tid,)).fetchone())
            if turn['stop_at'] or turn['cleanup'] == 'complete': reject('Turn no longer accepts input', 'conflict')
            if db.execute("SELECT count(*) FROM pending WHERE turn=? AND status IN ('waiting','claimed','responding')", (tid,)).fetchone()[0] >= 4:
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
            deadline = turn['created'] + self.limits.turn_seconds
            if turn['epoch'] != epoch or turn['stop_at'] or turn['cleanup'] == 'complete' or time.time() >= deadline:
                return False
            send()
            return True

    def claim_cell_command(self, workspace, sid, tid, pending_id, expected_digest):
        """Only the first live notebook subscriber may apply a command."""
        with self.connect(write=True) as db:
            self._session(db, workspace, sid)
            row = db.execute("""SELECT p.*,t.stop_at,t.cleanup,t.operation FROM pending p
                JOIN turns t ON t.id=p.turn AND t.epoch=p.epoch
                WHERE p.id=? AND p.turn=? AND t.session=?""", (identifier(pending_id), tid, sid)).fetchone()
            if (not row or row['digest'] != expected_digest or row['operation'] != 'notebook_cell_cli'
                    or json.loads(row['request']).get('method') != 'notebook/command'):
                reject('Notebook command identity mismatch', 'conflict')
            if row['stop_at'] or row['cleanup'] != 'pending' or row['expires'] <= time.time():
                reject('Notebook command expired', 'conflict')
            updated = db.execute("UPDATE pending SET status='claimed' WHERE id=? AND status='waiting'", (pending_id,))
            return {'claimed': bool(updated.rowcount)}

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
            if row['expires'] < time.time() or turn['stop_at'] or turn['cleanup'] == 'complete' or row['status'] not in {'waiting','claimed'}:
                reject('Input request expired', 'conflict')
            cell = json.loads(row['request']).get('method') == 'notebook/command'
            if cell and row['status'] != 'claimed':
                reject('Notebook command has not been claimed', 'conflict')
            status = 'delivered' if cell else 'responding'
            db.execute("UPDATE pending SET answer=?,status=? WHERE id=?", (raw, status, pending_id))
            db.execute('UPDATE sessions SET metadata_bytes=metadata_bytes+? WHERE id=?', (len(raw.encode()), sid))
            return dict(id=pending_id, status=status)

    def pending(self, tid):
        with self.connect() as db:
            return [dict(row) for row in db.execute("SELECT * FROM pending WHERE turn=? AND status IN ('waiting','claimed','responding') LIMIT 4", (tid,))]

    def resolved(self, key):
        with self.connect(write=True) as db:
            row = db.execute('SELECT * FROM pending WHERE id=?', (key,)).fetchone()
            turn = db.execute('SELECT * FROM turns WHERE id=?', (row['turn'],)).fetchone()
            db.execute("UPDATE pending SET status='delivered' WHERE id=?", (key,))
            self._event(db, turn['session'], turn['id'], turn['epoch'], 'input.resolved', dict(id=key, status='delivered'), control=True)

    def unresolved(self):
        with self.connect() as db:
            return [dict(row) for row in db.execute("SELECT * FROM turns WHERE cleanup != 'complete' LIMIT ?", (self.limits.active + 1,))]
