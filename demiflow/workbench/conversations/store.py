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
from demiflow.workbench.conversations.policy import turn_deadline
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
                db.execute("""CREATE TABLE IF NOT EXISTS followups(
                    session TEXT NOT NULL, submission TEXT NOT NULL, origin_turn TEXT NOT NULL,
                    text TEXT NOT NULL, error TEXT NOT NULL DEFAULT '', PRIMARY KEY(session,submission))""")
                db.execute("""CREATE TABLE IF NOT EXISTS steers(
                    turn TEXT NOT NULL, submission TEXT NOT NULL, sequence INTEGER NOT NULL,
                    status TEXT NOT NULL, PRIMARY KEY(turn,submission))""")
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

    def ensure_project(self, workspace, identity, provider, request_id, policy_hash, *, session_id=None):
        identifier(request_id)
        if session_id is not None:
            identifier(session_id)
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
                if session_id is not None and not db.execute('SELECT 1 FROM sessions WHERE id=?', (session_id,)).fetchone():
                    sid = session_id
                else:
                    sid = 'd' + secrets.token_hex(16)
                db.execute('INSERT INTO sessions(id,workspace,root,provider,request_key,policy_hash,created) VALUES(?,?,?,?,?,?,?)',
                           (sid,workspace,wire(identity).decode(),provider,'project-'+secrets.token_hex(16),policy_hash,time.time()))
            self._pin_development(db, workspace, sid)
            return self._session(db,workspace,sid)

    def create_conversation(self, workspace, identity, provider, request_id, policy_hash, *, session_kind="development", session_id=None):
        """Explicit writable membership is created with the session, never inferred from history."""
        identifier(request_id)
        if session_id is not None:
            identifier(session_id)
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
            if session_id is not None and not db.execute('SELECT 1 FROM sessions WHERE id=?', (session_id,)).fetchone():
                sid = session_id
            else:
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
            allowed = bool(json.loads(session['root']) == identity and bool(providers))
            if allowed and pin and session['session_kind'] == 'development':
                db.execute('INSERT OR IGNORE INTO project_conversation_members(workspace,session) VALUES(?,?)', (workspace, sid))
                primary = self._primary(db, workspace, identity, policy_hash)
                if primary:
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
                value['writable'] = bool(identity is not None and json.loads(row['root']) == identity and bool(providers))
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
            row['queued_messages'] = [dict(item) for item in db.execute(
                'SELECT submission,text,error FROM followups WHERE session=? ORDER BY rowid', (sid,))]
            row.pop('provider_session', None)
            return row

    @staticmethod
    def public_turn(turn):
        value = {k: v for k, v in turn.items() if k not in {'text', 'process', 'baseline', 'context','has_steer'}}
        return value

    def session(self, sid):
        with self.connect() as db:
            row = db.execute(f'SELECT * FROM {_SCOPED_SESSIONS} WHERE id=?', (identifier(sid),)).fetchone()
            if row is None: reject('Unknown session', 'not_found')
            return dict(row)

    def turn(self, tid):
        with self.connect() as db:
            row = db.execute("""SELECT t.*, EXISTS(SELECT 1 FROM steers WHERE turn=t.id AND status='pending')
                AS has_steer FROM turns t WHERE t.id=?""", (identifier(tid),)).fetchone()
            if row is None: reject('Unknown turn', 'not_found')
            return dict(row)

    def has_dispatched(self, sid):
        with self.connect() as db:
            return db.execute("SELECT 1 FROM turns WHERE session=? AND dispatch!='not_sent' LIMIT 1", (sid,)).fetchone() is not None

    def review_inputs(self, sid, *, include_paths):
        """Only actual user turns and observed changes from this development session."""
        with self.connect() as db:
            boundary = db.execute("""SELECT coalesce(max(t.created),0) FROM events e JOIN turns t ON t.id=e.turn
                    WHERE e.session=? AND t.state='completed' AND t.cleanup='complete'
                      AND json_extract(e.body,'$.type')='review.completed'
                      AND json_extract(e.body,'$.data.status')='passed'
                      AND json_extract(e.body,'$.data.scope.mode')='observed_changes'""", (sid,)).fetchone()[0]
            goals, size = [], 0
            for message, reason in db.execute("""SELECT text,json_extract(result,'$.reason') FROM turns
                    WHERE session=? AND created>? AND operation='message' ORDER BY created DESC,id DESC""",
                    (sid, boundary)):
                if include_paths and (reason or '').startswith('Change observation incomplete:'):
                    reject('一次源码观察没有完整记录，请指定 Flow 入口评审当前实现。')
                size += len(message.encode()) + (2 if goals else 0)
                if size > 8192:
                    if include_paths or not goals:
                        reject('交付目标超出预算，请简述当前目标并指定 Flow 入口。', 'resource_limit')
                    break
                goals.append(message)
            goal = '\n\n'.join(reversed(goals))
            if not include_paths:
                return goal, {}
            changes = {}
            current_turn = None
            for turn, path, before, after in db.execute("""SELECT t.id, json_extract(file.value,'$.path'),
                    json_extract(file.value,'$.before.sha256'), json_extract(file.value,'$.after.sha256')
                    FROM events e JOIN turns t ON t.id=e.turn,
                    json_each(e.body,'$.data.files') file
                    WHERE e.session=? AND t.created>? AND t.operation='message'
                      AND json_extract(e.body,'$.type')='change.observed' ORDER BY e.sequence""", (sid,boundary)):
                if path in changes:
                    if changes[path]['after_sha256'] != before:
                        reject('两轮之间的源码记录不连续，请指定 Flow 入口评审当前实现。')
                    changes[path]['after_sha256'] = after
                    continue
                if len(changes) >= 48:
                    reject('会话改动超出评审范围，请按 Flow 入口拆分。', 'resource_limit')
                original = None
                if before is not None:
                    if current_turn != turn:
                        record = db.execute('SELECT baseline FROM turns WHERE id=?', (turn,)).fetchone()
                        baseline = json.loads(record[0]) if record and record[0] else {}
                        current_turn = turn
                    fact = baseline.get(path) or {}
                    if fact.get('sha256') != before or 'preview' not in fact:
                        reject('缺少改动前的完整源码，请指定 Flow 入口评审当前实现。')
                    original = fact['preview']
                changes[path] = dict(before=original, before_sha256=before, after_sha256=after)
        sources = {path: dict(before=change['before'], after_sha256=change['after_sha256'])
                   for path, change in changes.items() if change['before_sha256'] != change['after_sha256']}
        return goal, sources

    def submission(self, sid, key):
        with self.connect() as db:
            row = db.execute('SELECT * FROM turns WHERE session=? AND submission=?', (sid, identifier(key))).fetchone()
            return dict(row) if row else None

    def steer(self, workspace, sid, tid, submission, message, *, from_queue=False):
        if type(from_queue) is not bool:
            reject('Invalid steering source','invalid_request')
        identifier(submission)
        text(message)
        with self.connect(write=True) as db:
            session = self._session(db, workspace, sid)
            turn = db.execute('SELECT * FROM turns WHERE session=? AND id=?', (sid,identifier(tid))).fetchone()
            if not turn or session['session_kind'] != 'development' or turn['operation'] != 'message':
                reject('此轮不属于当前开发会话。', 'conflict')
            old = db.execute("""SELECT e.body FROM steers s JOIN events e ON e.turn=s.turn AND e.sequence=s.sequence
                                WHERE s.turn=? AND s.submission=?""",(tid,submission)).fetchone()
            if old:
                if json.loads(old[0])['data']['text'] != message:
                    reject('Submission key has different text','conflict')
                return
            if from_queue:
                removed = db.execute('DELETE FROM followups WHERE session=? AND submission=? AND text=?',
                                     (sid,submission,message)).rowcount
                if not removed:
                    reject('这条消息已离开队列，请核对会话。','conflict')
            if turn['stop_at'] or turn['cleanup']=='complete' or turn['state'] not in (
                    'preparing','starting','running','waiting_approval','waiting_input'):
                reject('这一轮已经结束，请用 Enter 继续会话。', 'conflict')
            if db.execute('SELECT count(*) FROM steers WHERE turn=?',(tid,)).fetchone()[0]>=8:
                reject('本轮插话已达八条，可用 Enter 排队。', 'resource_limit')
            event = self._event(db,sid,tid,turn['epoch'],'message.steer',{'item_id':'steer:'+submission,'text':message})
            db.execute('INSERT INTO steers VALUES(?,?,?,?)',(tid,submission,event['sequence'],'pending'))

    def claim_steer(self, tid, epoch):
        with self.connect(write=True) as db:
            row = db.execute("""SELECT s.submission,e.body FROM steers s JOIN turns t ON t.id=s.turn
                JOIN events e ON e.turn=s.turn AND e.sequence=s.sequence
                WHERE t.id=? AND t.epoch=? AND t.stop_at IS NULL AND t.state='running'
                AND t.cleanup!='complete' AND s.status='pending' ORDER BY s.sequence LIMIT 1""", (tid,epoch)).fetchone()
            if row:
                db.execute("UPDATE steers SET status='sending' WHERE turn=? AND submission=?", (tid,row[0]))
                return row[0], json.loads(row[1])['data']['text']

    def finish_steer(self, tid, submission, status):
        with self.connect(write=True) as db:
            turn = db.execute('SELECT session,epoch FROM turns WHERE id=?',(tid,)).fetchone()
            if db.execute("UPDATE steers SET status=? WHERE turn=? AND submission=? AND status='sending'",
                          (status,tid,submission)).rowcount:
                self._event(db,turn['session'],tid,turn['epoch'],'message.steer_status',
                            {'item_id':'steer:'+submission,'status':status},control=True)

    def enqueue(self, workspace, sid, turn_id, submission, message):
        identifier(submission)
        identifier(turn_id)
        text(message)
        with self.connect(write=True) as db:
            session = self._session(db, workspace, sid)
            if session['session_kind'] != 'development':
                reject('此会话属于 Notebook 卡片。', 'conflict')
            sent = db.execute('SELECT text FROM turns WHERE session=? AND submission=?', (sid, submission)).fetchone()
            old = db.execute('SELECT origin_turn,text FROM followups WHERE session=? AND submission=?',
                             (sid, submission)).fetchone()
            steered = db.execute('''SELECT e.body FROM steers s JOIN events e ON e.turn=s.turn AND e.sequence=s.sequence
                WHERE e.session=? AND s.submission=? LIMIT 1''',(sid,submission)).fetchone()
            if steered:
                if json.loads(steered[0])['data']['text'] != message:
                    reject('Submission key has different text', 'conflict')
                return
            if old or sent:
                if (old and old['origin_turn'] != turn_id) or (old or sent)['text'] != message:
                    reject('Submission key has different content', 'conflict')
                return
            latest = db.execute('SELECT id,operation,state FROM turns WHERE session=? ORDER BY created DESC LIMIT 1',
                                (sid,)).fetchone()
            if not latest or latest['id'] != turn_id or latest['operation'] != 'message':
                reject('会话已进入下一轮，请核对后重新发送。', 'conflict')
            if latest['state'] in ('stopping','cancelled','failed','interrupted'):
                reject('上一轮已停止，请核对后重新发送。', 'conflict')
            if db.execute('SELECT count(*) FROM followups WHERE session=?', (sid,)).fetchone()[0] >= 8:
                reject('此会话已有八条待发送消息。', 'resource_limit')
            # Only active conversations normally queue. Bound retained messages
            # too, including paused queues from earlier grants or restarts.
            if db.execute('SELECT count(*) FROM followups').fetchone()[0] >= 128:
                reject('待发送消息已满，请稍后重试。', 'resource_limit')
            db.execute('INSERT INTO followups(session,submission,origin_turn,text) VALUES(?,?,?,?)',
                       (sid, submission, turn_id, message))

    def hold_queue(self, sid, reason):
        with self.connect(write=True) as db:
            db.execute('UPDATE followups SET error=? WHERE session=?', (reason[:220], sid))

    def change_queue(self, workspace, sid, submission, action):
        identifier(submission)
        with self.connect(write=True) as db:
            self._session(db, workspace, sid)
            if action == 'remove':
                db.execute('DELETE FROM followups WHERE session=? AND submission=?', (sid, submission))
            elif action == 'resume':
                head = db.execute('SELECT submission FROM followups WHERE session=? ORDER BY rowid LIMIT 1',
                                  (sid,)).fetchone()
                if not head or head[0] != submission:
                    reject('排队内容已变化，请刷新。', 'conflict')
                db.execute("UPDATE followups SET error='' WHERE session=?", (sid,))
            else:
                reject('Invalid queue action', 'invalid_request')

    def submit(self, workspace, sid, submission_id, expected_revision, message, identity, policy_hash, operation="message", provider=None):
        text(message); identifier(submission_id); integer(expected_revision, 0, 2**31)
        if operation not in {'message','compact','notebook_cell','notebook_cell_cli'}:reject('Unknown conversation operation')
        hashed = digest({'text': message, **({'operation':operation} if operation!='message' else {})})
        with self.connect(write=True) as db:
            session = self._session(db, workspace, sid)
            old = db.execute('SELECT * FROM turns WHERE session=? AND submission=?', (sid, submission_id)).fetchone()
            if old:
                if old['body_hash'] != hashed: reject('Submission key has different text', 'conflict')
                return dict(old), False
            if session['revision'] != expected_revision or json.loads(session['root']) != identity:
                reject('Session root or revision changed', 'conflict')
            if provider is not None and session['provider'] != provider:
                db.execute('UPDATE sessions SET policy_hash=?,provider=?,provider_session=? WHERE id=?',
                           (policy_hash, provider, '', sid))
            elif session['policy_hash'] != policy_hash:
                db.execute('UPDATE sessions SET policy_hash=? WHERE id=?', (policy_hash, sid))
            if session['session_kind'] == 'development':
                db.execute('INSERT OR IGNORE INTO project_conversation_members(workspace,session) VALUES(?,?)', (workspace, sid))
            cell_turn = operation in ('notebook_cell', 'notebook_cell_cli')
            if cell_turn and session['session_kind'] == 'development' and session['revision']:
                reject('请从目标卡片开启独立会话。', 'conflict')
            if not cell_turn and session['session_kind'] == 'notebook':
                reject('此会话属于 Notebook 卡片。', 'conflict')
            queued = db.execute('SELECT submission,text,error FROM followups WHERE session=? ORDER BY rowid LIMIT 1',
                                (sid,)).fetchone()
            if queued and (queued['submission'] != submission_id or queued['text'] != message
                           or queued['error'] or operation != 'message'):
                reject('请先发送或移除当前会话的排队消息。', 'conflict')
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
            db.execute('DELETE FROM followups WHERE session=? AND submission=?', (sid, submission_id))
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
        if changes.get('cleanup') in ('complete','unknown'):
            for row in db.execute("SELECT submission,status FROM steers WHERE turn=? AND status IN ('pending','sending')",(tid,)).fetchall():
                status = 'missed' if row['status']=='pending' else 'unknown'
                db.execute('UPDATE steers SET status=? WHERE turn=? AND submission=?',(status,tid,row['submission']))
                self._event(db,old['session'],tid,old['epoch'],'message.steer_status',
                            {'item_id':'steer:'+row['submission'],'status':status},control=True)
        if changes.get('state') in ('failed','interrupted','cancelled','stopping'):
            db.execute("UPDATE followups SET error='上一轮未完成，排队已暂停。' WHERE session=?", (old['session'],))
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

    def finish_provider(self, tid, *, state, cleanup, result, review=None):
        """Commit the drain receipt and any source checks in the same state transition."""
        with self.connect(write=True) as db:
            turn = dict(db.execute('SELECT * FROM turns WHERE id=?', (tid,)).fetchone())
            if turn['stop_at'] and ('pipeline_checks' in result or review is not None):
                result = {key: value for key, value in result.items() if key != 'pipeline_checks'}
                state = 'cancelled'
                if review is not None:
                    result.update(pipeline_review='incomplete', review_reason='评审已停止。')
            elif review is not None:
                from .pipeline_review import report_events
                # Close the review and turn in one transaction. A response observed
                # after cancellation cannot become a completed delivery review.
                db.execute('SAVEPOINT review_report')
                try:
                    for kind, data in report_events(review):
                        self._event(db, turn['session'], tid, turn['epoch'], kind, data)
                    result['pipeline_review'] = review['status']
                except ValueError:
                    db.execute('ROLLBACK TO review_report')
                    result.update(pipeline_review='incomplete', review_reason='评审报告超出会话记录的限制。')
                finally:
                    db.execute('RELEASE review_report')
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

    def replay(self, workspace, sid, *, limit=200, max_turns=12, before_sequence=None):
        """Return a bounded, display-compacted slice of turns without altering history."""
        integer(limit, 1, 200); integer(max_turns, 1, 32)
        if before_sequence is not None:
            integer(before_sequence, 0, 2**62)
        with self.connect() as db:
            session = self._session(db, workspace, sid)
            last_seq = session['sequence']
            upper_bound = min(before_sequence, last_seq + 1) if before_sequence is not None else last_seq + 1
            if before_sequence is not None:
                archive_seq = 0
                turns = db.execute(
                    """SELECT t.id, t.cleanup FROM turns t
                       WHERE t.session=?
                         AND EXISTS (SELECT 1 FROM events e WHERE e.turn=t.id AND e.sequence<?)
                       ORDER BY t.created DESC, t.id DESC LIMIT ?""",
                    (sid, upper_bound, max_turns + 1),
                ).fetchall()
            else:
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
            if not selected or last_seq == 0 or upper_bound <= 1:
                return dict(events=[], first_sequence=max(0, upper_bound - 1), next_sequence=last_seq,
                            last_sequence=last_seq, has_earlier=bool(archive_seq > 0))
            active_turns = {row['id'] for row in selected if row['cleanup'] != 'complete'}
            oldest_seq_row = db.execute('SELECT min(sequence) FROM events WHERE turn=?', (selected[-1]['id'],)).fetchone()
            turn_start = oldest_seq_row[0] if oldest_seq_row and oldest_seq_row[0] is not None else 1
            window_end = upper_bound - 1
            archive_floor = archive_seq - 1 if archive_seq > 0 else 0
            scan_floor = max(archive_floor, turn_start - 1)
            start_after = max(scan_floor, max(0, window_end - 4096))
            truncated_scan = False
            raw_rows = [(row['sequence'], row['size'], json.loads(row['body']))
                        for row in db.execute(
                            'SELECT sequence,size,body FROM events WHERE session=? AND sequence>? AND sequence<? ORDER BY sequence LIMIT 4096',
                            (sid, start_after, upper_bound),
                        )]
            if start_after > scan_floor:
                backfilled = []
                for row in selected:
                    tid = row['id']
                    t_min_row = db.execute('SELECT min(sequence) FROM events WHERE turn=?', (tid,)).fetchone()
                    t_min = t_min_row[0] if t_min_row else None
                    if t_min is None or t_min > start_after:
                        continue
                    if row['cleanup'] != 'complete':
                        truncated_scan = True
                        continue
                    # Backfill display-essential events for completed turns predating the
                    # 4096-event tail scan so long sessions never drop earlier turns.
                    for erow in db.execute(
                        """SELECT sequence, size, body FROM events
                           WHERE turn=? AND sequence>? AND sequence<=? AND sequence<?
                             AND (json_extract(body,'$.type') IN (
                                   'message.user','turn.state','change.observed','review.completed',
                                   'protected.violation','compaction.completed','history.gap','check.reference')
                                  OR (json_extract(body,'$.type')='message.completed'
                                      AND COALESCE(json_extract(body,'$.data.channel'),'') NOT IN ('process','summary')))
                           ORDER BY sequence""",
                        (tid, archive_floor, start_after, upper_bound),
                    ):
                        backfilled.append((erow['sequence'], erow['size'], json.loads(erow['body'])))
                if backfilled:
                    by_seq = {seq: (seq, size, ev) for seq, size, ev in backfilled}
                    for seq, size, ev in raw_rows:
                        by_seq[seq] = (seq, size, ev)
                    raw_rows = [by_seq[seq] for seq in sorted(by_seq)]
        # Compact display-redundant events in completed turns, and drop non-display noise
        # plus already-closed message.delta chunks in active turns while keeping in-flight
        # streaming deltas and tool rows verbatim.
        # 1) Identify message spans terminated by message.completed without intervening tool events.
        completed_msg_final = {}
        span_id = {}
        tool_first = {}
        tool_last = {}
        tool_has_delta = set()
        last_turn_state = {}
        change_last = {}
        change_files = {}
        span_counter = 0
        for seq, _, ev in raw_rows:
            tid = ev.get('turn_id')
            kind = ev.get('type')
            data = ev.get('data') or {}
            epoch = ev.get('provider_epoch') or ''
            if kind in ('message.delta', 'message.completed'):
                mkey = (tid, epoch, data.get('channel') or 'answer', data.get('item_id'))
                sid_span = span_id.setdefault(mkey, span_counter)
                if kind == 'message.completed':
                    completed_msg_final[(mkey, sid_span)] = (seq, bool(data.get('paged')))
                continue
            if kind.startswith('item.') or kind == 'output.delta':
                span_counter += 1
                span_id.clear()
            if tid in active_turns:
                continue
            if kind == 'item.updated':
                tkey = (tid, epoch, data.get('id'))
                tool_first.setdefault(tkey, seq)
                tool_last[tkey] = seq
            elif kind == 'output.delta':
                tool_has_delta.add((tid, epoch, data.get('item_id')))
            elif kind == 'turn.state':
                last_turn_state[tid] = seq
            elif kind == 'change.observed':
                change_last[tid] = seq
                bucket = change_files.setdefault(tid, {})
                for f in data.get('files') or ():
                    if isinstance(f, dict) and isinstance(f.get('path'), str):
                        bucket[f['path']] = f

        compacted = []
        span_id.clear()
        span_counter = 0
        for seq, size, ev in raw_rows:
            tid = ev.get('turn_id')
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
                    if kind == 'message.delta' and (tid not in active_turns or seq < final_seq):
                        continue
                    if tid not in active_turns and seq != final_seq and not is_paged:
                        continue
                compacted.append((seq, size, ev))
                continue
            if kind.startswith('item.') or kind == 'output.delta':
                span_counter += 1
                span_id.clear()
            if tid in active_turns:
                compacted.append((seq, size, ev))
                continue
            if kind == 'item.updated':
                tkey = (tid, epoch, data.get('id'))
                if tkey in tool_has_delta:
                    if seq != tool_first[tkey] and seq != tool_last[tkey]:
                        continue
                elif seq != tool_last[tkey]:
                    continue
            elif kind == 'turn.state' and seq != last_turn_state.get(tid):
                continue
            elif kind == 'change.observed':
                if seq != change_last.get(tid):
                    continue
                merged_files = list((change_files.get(tid) or {}).values())
                if len(merged_files) > 1:
                    compact_files = []
                    strip_diffs = len(merged_files) > 16
                    for f in merged_files[:64]:
                        if strip_diffs and 'diff' in f:
                            cf = {k: v for k, v in f.items() if k != 'diff'}
                            cf.setdefault('diff_unavailable', 'compacted_in_replay')
                            compact_files.append(cf)
                        else:
                            compact_files.append(f)
                    ev = dict(ev, data=dict(data, files=compact_files))
                    size = len(json.dumps(ev, ensure_ascii=False).encode('utf-8'))
            compacted.append((seq, size, ev))

        # Fit within limit and 256 KiB. If over budget, first drop non-latest completed
        # turns' process/tool rows while preserving each turn's user prompt & final answer;
        # if still over budget, also prune the newest completed turn's process/tool rows
        # so its user prompt and final answer are never crowded out.
        newest_tid = selected[0]['id']
        budget_bytes = 256 * 1024
        if len(compacted) > limit or sum(s for _, s, _ in compacted) > budget_bytes:
            final_answer_seq = {}
            for seq, _, ev in compacted:
                tid = ev.get('turn_id')
                data = ev.get('data') or {}
                if ev.get('type') in ('message.completed', 'message.delta') and data.get('channel') not in ('process', 'summary'):
                    final_answer_seq[tid] = seq

            def _prune_turns(entries, include_newest):
                out = []
                for entry in entries:
                    seq, _, ev = entry
                    tid = ev.get('turn_id')
                    kind = ev.get('type')
                    data = ev.get('data') or {}
                    if tid not in active_turns and (include_newest or tid != newest_tid):
                        if kind in ('item.updated', 'item.started', 'output.delta'):
                            continue
                        if kind in ('message.delta', 'message.completed') and (
                                data.get('channel') in ('process', 'summary') or seq != final_answer_seq.get(tid)):
                            continue
                    out.append(entry)
                return out

            compacted = _prune_turns(compacted, include_newest=False)
            if len(compacted) > limit or sum(s for _, s, _ in compacted) > budget_bytes:
                compacted = _prune_turns(compacted, include_newest=True)

        kept, used, omitted = [], 0, False
        for seq, size, ev in reversed(compacted):
            if len(kept) >= limit or used + size > budget_bytes:
                omitted = True
                break
            kept.append(ev); used += size
        kept.reverse()
        first_seq = kept[0]['sequence'] if kept else last_seq
        # Always preserve prior completed turns' compact summaries on initial/reconnect replay
        # (so switching tabs during or after a long turn never hides earlier turns) and preserve
        # the user prompt bubble (`message.user`) for every turn shown in the replay window
        # (both initial tail and upward pagination pages), while keeping `first_seq` anchored to
        # the contiguous tail so upward pagination never skips intermediate events.
        if kept:
            kept_seqs = {ev['sequence'] for ev in kept}
            anchored = []
            if before_sequence is None and omitted:
                prior_budget = budget_bytes + 128 * 1024
                prior_by_turn = {}
                for seq, size, ev in compacted:
                    tid = ev.get('turn_id')
                    if not tid or tid == newest_tid or tid in active_turns or seq in kept_seqs:
                        continue
                    prior_by_turn.setdefault(tid, []).append((seq, size, ev))
                prior_order = [row['id'] for row in selected if row['id'] in prior_by_turn]
                prior_limit = max_turns * 8
                prior_count = 0
                for tid in prior_order:
                    turn_items = prior_by_turn[tid]
                    if prior_count + len(turn_items) > prior_limit:
                        break
                    turn_bytes = sum(size for _, size, _ in turn_items)
                    if used + turn_bytes > prior_budget:
                        minimal = [item for item in turn_items if item[2].get('type') in ('message.user', 'turn.state')]
                        min_bytes = sum(size for _, size, _ in minimal)
                        if minimal and used + min_bytes <= prior_budget:
                            for seq, size, ev in minimal:
                                anchored.append(ev)
                                kept_seqs.add(seq)
                                used += size
                                prior_count += 1
                        continue
                    for seq, size, ev in turn_items:
                        anchored.append(ev)
                        kept_seqs.add(seq)
                        used += size
                        prior_count += 1
            seen_user_turns = {ev.get('turn_id') for ev in kept if ev.get('type') == 'message.user'}
            seen_user_turns.update(ev.get('turn_id') for ev in anchored if ev.get('type') == 'message.user')
            missing_tids = []
            for ev in kept:
                tid = ev.get('turn_id')
                if tid and tid not in seen_user_turns and tid not in missing_tids:
                    missing_tids.append(tid)
            if missing_tids:
                with self.connect() as db:
                    for tid in missing_tids[:max_turns]:
                        row = db.execute(
                            """SELECT sequence, size, body FROM events
                               WHERE session=? AND turn=? AND json_extract(body,'$.type')='message.user'
                               ORDER BY sequence LIMIT 1""",
                            (sid, tid),
                        ).fetchone()
                        if row is not None and row['sequence'] not in kept_seqs and used + row['size'] <= budget_bytes + 196608:
                            anchored.append(json.loads(row['body']))
                            kept_seqs.add(row['sequence'])
                            used += row['size']
            if anchored:
                kept = sorted(anchored + kept, key=lambda item: item['sequence'])
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
            expires = min(time.time() + self.limits.input_seconds, turn_deadline(turn, self.limits))
            db.execute('INSERT INTO pending VALUES(?,?,?,?,?,?,?,?)', (key, tid, turn['epoch'], hashed, raw, expires, None, 'waiting'))
            db.execute('UPDATE sessions SET metadata_bytes=metadata_bytes+? WHERE id=?', (len(raw.encode()), turn['session']))
            self._event(db, turn['session'], tid, turn['epoch'], 'input.requested', dict(id=key, digest=hashed, request=request, expires=expires))
            return key

    def dispatch_provider_reply(self, tid, epoch, send):
        """Serialize a bounded transport send against stop and epoch changes."""
        with self.connect(write=True) as db:
            turn = dict(db.execute('SELECT * FROM turns WHERE id=?', (tid,)).fetchone())
            deadline = turn_deadline(turn, self.limits)
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
