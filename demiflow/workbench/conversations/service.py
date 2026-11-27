"""Project binding, bounded admission, persistent intent and owned workers."""
from __future__ import annotations

import array
import fcntl
import json
import os
from pathlib import Path
import secrets
import select
import shutil
import socket
import subprocess
import sys
import tempfile
import threading
import time

from demiflow.workbench.conversations.common import private_directory_bytes
from demiflow.workbench.conversations.common import digest
from demiflow.workbench.conversations.common import identifier
from demiflow.forge.process import process_identity
from demiflow.workbench.conversations.common import reject
from demiflow.workbench.conversations.common import root_identity
from demiflow.workbench.conversations.common import wire
from demiflow.workbench.conversations.policy import Deployment
from demiflow.workbench.conversations.policy import POLICY_BYTES
from demiflow.workbench.conversations.policy import proxy_environment
from demiflow.workbench.conversations.store import Store
from demiflow.workbench.conversations.store import StorageUnavailable
from demiflow.workbench.conversations.provider import adapter_for
from demiflow.resources.process_observation import unit_stopped
from demiflow.forge.schema import ForgeError


class _WarmWorker:
    def __init__(self, session_id, policy_hash, provider, root_path, process, sock, *, workspace=None, standby=False, reserved=False, operation='message'):
        self.session_id = session_id
        self.workspace = workspace
        self.policy_hash = policy_hash
        self.provider = provider
        self.root_path = root_path
        self.process = process
        self.sock = sock
        self.idle = False
        self.standby = standby
        self.reserved = reserved
        self.operation = operation
        self.dead = False
        self.busy_tid = None
        self.sandbox_deadline = 0.0
        self.accepted_tid = None
        self.accepted_event = threading.Event()


class Service:
    remote = False

    def __init__(self, config_path, *, input_denied_paths=()):
        # Host authority is not a user-maintained input allowlist. Pass these
        # paths only to the trusted worker, never to the provider environment.
        self.settings = None
        self.workflow_registry = None
        self.deployment = Deployment.load(config_path)
        self.input_denied_paths = tuple(dict.fromkeys(str(Path(p).absolute()) for p in (
            config_path, *input_denied_paths,
            *(cfg['native_credentials'] for cfg in self.deployment.providers.values() if cfg.get('native_credentials')),
        )))
        self.root = self.deployment.state
        self.root.mkdir(mode=0o700, parents=True, exist_ok=True)
        if self.root.is_symlink() or self.root.stat().st_mode & 0o077:
            reject('Development state must be a private physical directory')
        self.owner = os.open(self.root / 'service.lock', os.O_CREAT | os.O_RDWR | os.O_NOFOLLOW, 0o600)
        try: fcntl.flock(self.owner, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            os.close(self.owner); reject('Development state already belongs to a service', 'busy')
        try:
            self._initialize_state()
        except BaseException:
            os.close(self.owner)
            raise

    def _initialize_state(self):
        if self.root.stat().st_dev != Path(self.deployment.value['host_budget']['device_path']).stat().st_dev:
            reject('Development state is outside the declared disk device partition')
        self.store = Store(self.root, self.deployment.limits, initialize=True)
        self.lock, self.workers, self.closed = threading.RLock(), {}, False
        self.wakeup = threading.Condition()
        self.worker_targets = {}
        self._worker_socks = {}
        self.warm_workers = {}
        self._policy_meta_cache = {}
        self.queue_readers = set()
        self.queue_shutdown = threading.Event()
        with self.store.connect(write=True) as db:
            db.execute("UPDATE followups SET error='服务已重启，排队已暂停。' WHERE error=''")
        self.coding_catalog = None
        self.policy_hash, self.config_path = self.seal_policy(self.deployment)
        with self.store.connect() as db:
            completed = db.execute("SELECT id FROM turns WHERE cleanup='complete' AND json_extract(process,'$.forge_writer') IS NOT NULL")
            for row in completed:
                self.store.retire_writer(row[0])
        for t in self.store.unresolved():
            self.store.interrupt_unresolved(t['id'])

    def seal_policy(self, deployment):
        """Persist the exact bounded non-secret policy before creating a session."""
        from demiflow.forge.io import fsync_directory
        value = json.loads(wire(deployment.value))
        for p in value['projects'].values(): p.pop('identity', None)
        revision = digest(value); path = self.root / (revision + '.json')
        raw = wire(value, POLICY_BYTES)
        if not path.exists():
            total = count = policies = 0
            with os.scandir(self.root) as entries:
                for entry in entries:
                    count += 1
                    if count > self.deployment.limits.sessions + 128: reject('Development policy inventory exceeds budget', 'resource_limit')
                    if entry.name.endswith('.json') and entry.name != 'global-development-settings.json':
                        policies += 1
                        info=entry.stat(follow_symlinks=False)
                        total += max(info.st_size,info.st_blocks*512)
            if policies >= 32 or total + len(raw) > 2 * 1024**2: reject('Development policy history is full; explicit maintenance required', 'resource_limit')
            fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600)
            with os.fdopen(fd, 'wb') as stream:
                stream.write(raw); stream.flush(); os.fsync(stream.fileno())
            fsync_directory(self.root)
        return revision, path

    def binding(self, workspace, root, *, seal=False):
        if self.settings is not None:
            return self.settings.binding(workspace, root, seal=seal)
        return self.deployment, self.policy_hash, self.config_path

    def current_hash(self, workspace, root):
        return self.binding(workspace, root)[1]

    def session_policy(self, session):
        from .common import read_file, strict_json
        value=strict_json(read_file(self.root/(session['policy_hash']+'.json'),POLICY_BYTES))
        if digest(value)!=session['policy_hash']: reject('Conversation policy snapshot changed','conflict')
        return Deployment(value)

    def project(self, workspace, root):
        return self.binding(workspace, root)[0].project(workspace, root)

    def _feedback_scope(self, workspace, root):
        from demiflow.forge.sandbox import development_read_scope
        deployment, _, config_path = self.binding(workspace, root)
        project = deployment.project(workspace, root)
        denied = deployment.private_paths(config_path, *self.input_denied_paths)
        allowed = development_read_scope(root, read_roots=project.get('read_roots'), denied_paths=denied)
        return allowed, tuple(denied)

    def capabilities(self, workspace, root):
        deployment, _, _ = self.binding(workspace, root)
        caps=deployment.capabilities(workspace, root)
        return dict(caps, message_queue=True, message_steering=True, service_configured=True, project_authorized=caps['enabled'])

    def create(self, workspace, root, request_id, provider_id):
        self.project(workspace, root)
        identifier(provider_id)
        deployment, revision, _ = self.binding(workspace, root, seal=True)
        if provider_id not in deployment.providers: reject('Provider is not configured')
        return self.store.create(workspace, root_identity(root), provider_id, request_id, revision)

    def conversation(self, workspace, root):
        self.project(workspace,root)
        session=self.store.primary(workspace,root_identity(root),self.current_hash(workspace,root))
        return dict(session=self.show(workspace,session['id']) if session else None)

    def create_project(self, workspace, root, request_id, provider_id, new_conversation=False, session_kind="development"):
        if type(new_conversation) is not bool:
            reject('new_conversation must be a boolean', 'invalid_request')
        if session_kind not in ('development','notebook') or (session_kind=='notebook' and not new_conversation):
            reject('Invalid project conversation kind','invalid_request')
        self.project(workspace,root);identifier(provider_id)
        deployment, revision, _ = self.binding(workspace, root, seal=True)
        if provider_id not in deployment.providers:reject('Provider is not configured')
        identity = root_identity(root)
        root_path = identity['path']
        reserved_warm = None
        warm_map = getattr(self, 'warm_workers', None)
        if warm_map:
            expected_op = 'message' if session_kind == 'development' else (
                'notebook_cell_cli' if self.capabilities(workspace, root).get('notebook_cli') else 'notebook_cell')
            for warm in list(warm_map.values()):
                if (warm.reserved and not warm.dead and warm.process.poll() is None
                        and warm.operation == expected_op
                        and warm.workspace == workspace and warm.root_path == root_path
                        and warm.provider == provider_id and warm.policy_hash == revision):
                    reserved_warm = warm
                    break
        reserved_sid = reserved_warm.session_id if reserved_warm is not None else None
        args = (workspace, identity, provider_id, request_id, revision)
        if new_conversation:
            session = self.store.create_conversation(*args, session_kind=session_kind, session_id=reserved_sid)
        else:
            session = self.store.ensure_project(*args, session_id=reserved_sid)
        if reserved_warm is not None and session['id'] == reserved_sid:
            reserved_warm.reserved = False
        return dict(session, writable=True)

    def _ensure_coding_catalog(self):
        if self.coding_catalog is None:
            from demiflow.forge.api_reference import REFERENCE_BYTES, build_reference
            raw = wire(build_reference(), REFERENCE_BYTES)
            catalog = tempfile.TemporaryFile(dir=self.root)
            try:
                catalog.write(raw)
                catalog.flush()
            except BaseException:
                catalog.close()
                raise
            self.coding_catalog = catalog
        return self.coding_catalog.fileno()

    def _worker_env(self, policy):
        env = dict(PATH='/usr/bin:/bin', LANG='C.UTF-8', PYTHONDONTWRITEBYTECODE='1')
        # The trusted worker resolves the same OS/account boundaries as the host.
        # Provider launchers build their own environment and private HOME.
        env['HOME'] = str(Path.home())
        for key in ('CODEX_HOME', 'DSH_HOME'):
            value = os.environ.get(key)
            if value and Path(value).is_absolute():
                env[key] = value

        if self.workflow_registry is not None:
            env['DEMIFLOW_WORKFLOW_REGISTRY'] = wire(self.workflow_registry, 8192).decode()
        env['DEMIFLOW_INPUT_DENIED_PATHS'] = wire(list(self.input_denied_paths), 65536).decode()
        if self.deployment.value.get('runtime_domain'):
            env['DEMIFLOW_FORGE_DOMAIN'] = self.deployment.value['runtime_domain']
        for config in policy.providers.values():
            if config.get('credential_env') and config['credential_env'] in os.environ:
                env[config['credential_env']] = os.environ[config['credential_env']]
            env.update(proxy_environment(config))
        return env

    def prewarm(self, workspace, root, *, session_id=None, provider_id=None, session_kind='development'):
        """Best-effort standby pre-warming so Turn 1 skips cold CLI startup."""
        if session_kind not in ('development', 'notebook'):
            reject('Invalid project conversation kind', 'invalid_request')
        warm_map = getattr(self, 'warm_workers', None)
        if warm_map is None:
            return dict(status='unsupported')
        project = self.project(workspace, root)
        deployment, revision, _ = self.binding(workspace, root, seal=True)
        caps = self.capabilities(workspace, root)
        available_providers = [p['id'] for p in caps['providers'] if p['available']]
        if not caps['start'] or not available_providers:
            return dict(status='unavailable')
        if session_kind == 'notebook':
            if not caps.get('notebook_cells'):
                return dict(status='unavailable')
            operation = 'notebook_cell_cli' if caps.get('notebook_cli') else 'notebook_cell'
        else:
            operation = 'message'
        identity = root_identity(root)
        root_path = identity['path']
        reserved = session_id is None
        target_sid = None
        if not reserved:
            identifier(session_id)
            session = self.store.show(workspace, session_id)
            if (session.get('session_kind') != session_kind
                    or not self.store.writable(workspace, session_id, identity, revision, deployment.providers)):
                return dict(status='skipped')
            target_provider = session['provider'] if session['provider'] in available_providers else available_providers[0]
            target_sid = session_id
        else:
            if provider_id is not None:
                identifier(provider_id)
            target_provider = provider_id if provider_id in available_providers else available_providers[0]
        with self.lock:
            if self.closed:
                return dict(status='stopping')
            self._reap()
            target_path = Path(root_path)
            for tid, (_, _, _, active_root_json) in self.worker_targets.items():
                if tid in self.workers:
                    active_root = Path(json.loads(active_root_json)['path'])
                    if target_path == active_root or target_path in active_root.parents or active_root in target_path.parents:
                        return dict(status='busy')
            warm_map = getattr(self, 'warm_workers', None)
            if warm_map is None:
                return dict(status='unsupported')
            def _valid_deadline(w):
                return (self.deployment.mode != 'restricted' or w.sandbox_deadline == 0.0
                        or w.sandbox_deadline - time.time() > self.deployment.limits.turn_seconds + 60)
            if not reserved:
                existing = warm_map.get(target_sid)
                if (existing is not None and not existing.dead and existing.process.poll() is None
                        and existing.operation == operation
                        and existing.policy_hash == revision and existing.provider == target_provider
                        and existing.root_path == root_path and _valid_deadline(existing)):
                    return dict(status='ready' if existing.idle else 'warming', session_id=target_sid)
            else:
                for existing in warm_map.values():
                    if (existing.reserved and not existing.dead and existing.process.poll() is None
                            and existing.operation == operation
                            and existing.workspace == workspace and existing.policy_hash == revision
                            and existing.provider == target_provider and existing.root_path == root_path
                            and _valid_deadline(existing)):
                        return dict(status='ready' if existing.idle else 'warming', standby=True)
            try:
                self.deployment.validate_runtime(admit=True)
                if (self.root / 'storage-pressure').exists():
                    return dict(status='skipped')
                fs = os.statvfs(self.root)
                if fs.f_bavail * fs.f_frsize < self.deployment.limits.provider_bytes + self.deployment.limits.output_bytes:
                    return dict(status='skipped')
                homes = private_directory_bytes(self.root, entries=self.deployment.limits.sessions + 64,
                                             codex_executable=deployment.providers.get('codex', {}).get('executable'))
                used = sum(homes.values())
                allowance = self.deployment.limits.provider_bytes
                reserved_bytes = sum(max(0, allowance - homes.get(t['session'], 0)) for t in self.store.unresolved())
                if used + reserved_bytes + max(0, allowance - homes.get(target_sid or '', 0)) > self.deployment.limits.private_total_bytes:
                    return dict(status='skipped')
            except Exception:
                return dict(status='skipped')
            # Keep at most one warm/standby worker in memory at a time.
            to_retire = []
            for sid, old in list(warm_map.items()):
                if old.busy_tid is None:
                    warm_map.pop(sid, None)
                    to_retire.append(old)
            for old in to_retire:
                same_root = Path(old.root_path) == target_path
                self._retire_warm(old, wait_s=0.5 if same_root else 0.0)
            coding_fd = self._ensure_coding_catalog() if (operation == 'message' and project['kind'] == 'pipeline') else None
            if reserved:
                target_sid = 'd' + secrets.token_hex(16)
            parent_sock = worker_sock = None
            try:
                parent_sock, worker_sock = socket.socketpair(socket.AF_UNIX, socket.SOCK_STREAM)
                worker = Path(__file__).with_name('worker.py')
                env = self._worker_env(deployment)
                meta = json.dumps(dict(workspace=workspace, root_path=root_path, session_id=target_sid,
                                       provider=target_provider, policy_hash=revision, operation=operation), separators=(',', ':'))
                command = [sys.executable, '-I', '-B', str(worker), '--standby',
                           str(self.root / (revision + '.json')), meta, str(worker_sock.fileno())]
                inherited = (worker_sock.fileno(),)
                if coding_fd is not None:
                    command.append(str(coding_fd))
                    inherited += (coding_fd,)
                process = subprocess.Popen(command, cwd=self.root, env=env,
                    stdin=subprocess.DEVNULL, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
                    pass_fds=inherited, start_new_session=True)
                warm = _WarmWorker(target_sid, revision, target_provider, root_path, process, parent_sock,
                                   workspace=workspace, standby=True, reserved=reserved, operation=operation)
                warm_map[target_sid] = warm
                parent_sock = None
                threading.Thread(target=self._watch_worker, args=(warm,), name='demiflow-dev-watch', daemon=True).start()
            finally:
                if worker_sock is not None:
                    worker_sock.close()
                if parent_sock is not None:
                    self._close_life(parent_sock)
            return dict(status='warming', standby=True, session_id=None if reserved else target_sid)

    def sessions(self, workspace, root, offset=0, limit=20):
        self.project(workspace, root)
        return self.store.list(workspace, offset, limit, identity=root_identity(root),
                               policy_hash=self.current_hash(workspace,root), providers=self.binding(workspace,root)[0].providers)

    def start_project(self, workspace, root, session_id, submission_id, expected_revision, text, operation='message', auto_review=False, rework_depth=0, purpose='message', forge_options=None):
        self._require_writable(workspace,root,session_id)
        cell_capability = {'notebook_cell':'notebook_cells', 'notebook_cell_cli':'notebook_cli'}.get(operation)
        if cell_capability and not self.capabilities(workspace,root).get(cell_capability):
            reject('当前开发服务未提供只读的 Notebook 单元格生成。')
        return self.start(workspace,root,session_id,submission_id,expected_revision,text,operation=operation,auto_review=auto_review,rework_depth=rework_depth,purpose=purpose,forge_options=forge_options)

    def steer(self, workspace, root, session_id, turn_id, submission_id, text, from_queue=False):
        with self.lock:
            self._require_writable(workspace, root, session_id)
            if self.closed or not self.capabilities(workspace,root)['start']:
                reject('当前开发服务无法发送消息。','busy')
            self.store.steer(workspace, session_id, turn_id, submission_id, text, from_queue=from_queue)
            return {'accepted': True}

    def enqueue(self, workspace, root, session_id, turn_id, submission_id, text, auto_review=False, purpose='message'):
        with self.lock:
            self._require_writable(workspace, root, session_id)
            if self.closed or not self.capabilities(workspace, root)['start']:
                reject('当前开发服务无法发送消息。', 'busy')
            self.store.enqueue(workspace, session_id, turn_id, submission_id, text,
                               forge_options=dict(auto_review=bool(auto_review), purpose=purpose, origin='user'))
            self._watch_queue(workspace, root, session_id)
            return {'accepted': True}

    def change_queue(self, workspace, root, session_id, submission_id, action):
        with self.lock:
            self._require_writable(workspace, root, session_id)
            self.store.change_queue(workspace, session_id, submission_id, action)
            if action == 'resume':
                self._watch_queue(workspace, root, session_id)
            return {'accepted': True}

    def _watch_queue(self, workspace, root, sid):
        if self.closed or sid in self.queue_readers:
            return
        self.queue_readers.add(sid)
        def drain():
            while not self.queue_shutdown.is_set():
                with self.lock:
                    try:
                        if self.closed:
                            break
                        self._reap()
                        session = self.store.show(workspace, sid)
                        queue = session['queued_messages']
                        if not queue or queue[0]['error']:
                            self.queue_readers.discard(sid)
                            return
                        if session['turn']['cleanup'] == 'complete':
                            head = queue[0]
                            options = json.loads(head.get('forge_options') or '{}')
                            if options.get('legacy'):
                                self.store.hold_queue(sid, '历史自动反馈缺少候选绑定，已保留；请重新检查后决定是否发送。')
                                self.queue_readers.discard(sid)
                                return
                            if options.get('origin') == 'forge':
                                from demiflow.forge.development import feedback_current
                                saved_session = self.store.session(sid)
                                fb_allowed, fb_denied = self._feedback_scope(workspace, root)
                                if not feedback_current(root, options, dict(provider=saved_session['provider'],
                                                                          session=saved_session['provider_session']),
                                                        allowed_roots=fb_allowed, denied_paths=fb_denied):
                                    self.store.discard_feedback(workspace, sid, head['submission'], 'stale')
                                    continue
                            self.start_project(workspace, root, sid, head['submission'], session['revision'], head['text'],
                                               forge_options=options, auto_review=bool(options.get('auto_review')),
                                               rework_depth=int(options.get('round', 0)))
                    except (OSError, ValueError) as exc:
                        self.queue_readers.discard(sid)
                        self.store.hold_queue(sid, str(exc))
                        return
                self.queue_shutdown.wait(.35)
            with self.lock:
                self.queue_readers.discard(sid)
        threading.Thread(target=drain, daemon=True, name='development-followups').start()

    def _require_writable(self, workspace, root, session_id):
        self.project(workspace,root)
        if not self.store.writable(workspace, session_id, root_identity(root), self.current_hash(workspace,root),
                                   self.binding(workspace,root)[0].providers, pin=True):
            reject('This is a read-only earlier conversation or its grant changed', 'conflict')

    def compact(self, workspace, root, session_id, submission_id, expected_revision):
        self._require_writable(workspace,root,session_id)
        session=self.store.session(session_id)
        if not session['provider_session'] or not self.store.has_dispatched(session_id):
            reject('No provider conversation is available to compact', 'conflict')
        return self.start(workspace,root,session_id,submission_id,expected_revision,'压缩上下文',operation='compact')

    def _maybe_drain_queue(self, workspace, root_path, sid):
        if self.closed or not workspace or not root_path or not sid:
            return
        try:
            session = self.store.show(workspace, sid)
            queue = session.get('queued_messages') or []
            if queue and not queue[0].get('error'):
                self._watch_queue(workspace, root_path, sid)
        except (OSError, ValueError):
            pass

    def start(self, workspace, root, session_id, submission_id, expected_revision, text, *, operation="message", auto_review=False, rework_depth=0, purpose="message", forge_options=None):
        self.project(workspace, root)
        deployment, current_hash, _ = self.binding(workspace, root, seal=True)
        caps = self.capabilities(workspace, root)
        session = self.store.show(workspace, session_id)
        available_providers = [p['id'] for p in caps['providers'] if p['available']]
        if not caps['start'] or not available_providers:
            reject('Development provider is unavailable in this deployment')
        options = (forge_options if forge_options is not None else
                   dict(origin='user', purpose=purpose, auto_review=bool(auto_review), round=int(rework_depth)))
        if options.get('origin') == 'forge':
            from demiflow.forge.development import feedback_current
            fb_allowed, fb_denied = self._feedback_scope(workspace, root)
            if (session['provider'] not in available_providers or options.get('policy_hash') != current_hash
                    or not feedback_current(root, options, dict(provider=session['provider'],
                        session=self.store.session(session_id)['provider_session']),
                        allowed_roots=fb_allowed, denied_paths=fb_denied)):
                reject('原作者配置或候选已变化，自动修正已停止，请重新检查。', 'conflict')
        target_provider = session['provider'] if session['provider'] in available_providers else available_providers[0]
        with self.lock:
            if self.closed: reject('Development service is stopping', 'busy')
            self._reap()
            self.deployment.validate_runtime(admit=True)
            if (self.root / 'storage-pressure').exists():
                reject('Development storage reserve was consumed; explicit maintenance is required', 'busy')
            fs = os.statvfs(self.root)
            if fs.f_bavail * fs.f_frsize < self.deployment.limits.provider_bytes + self.deployment.limits.output_bytes:
                reject('Insufficient free space for the development turn reservation', 'resource_limit')
            # Retained provider homes count after completion too. A new active
            # turn reserves a full private allowance before it can spawn.
            existing = self.store.submission(session_id, submission_id)
            if existing is not None:
                if existing['body_hash'] != digest({'text':text,**({'operation':operation} if operation!='message' else {})}): reject('Submission key has different text', 'conflict')
                if json.loads(existing['forge_options']) != options:
                    reject('Submission key has different engineering options', 'conflict')
                return self.store.public_turn(existing)
            homes = private_directory_bytes(self.root, entries=self.deployment.limits.sessions + 64,
                                             codex_executable=deployment.providers.get('codex', {}).get('executable'))
            used = sum(homes.values())
            allowance = self.deployment.limits.provider_bytes
            reserved = sum(max(0, allowance - homes.get(t['session'], 0)) for t in self.store.unresolved())
            if used + reserved + max(0, allowance - homes.get(session_id, 0)) > self.deployment.limits.private_total_bytes:
                reject('Retained provider homes exhaust private disk reservation', 'resource_limit')
            value, fresh = self.store.submit(workspace, session_id, submission_id, expected_revision, text, root_identity(root), current_hash,
                                             operation=operation, provider=target_provider, forge_options=options)
            if not fresh: return self.store.public_turn(value)
            session = self.store.show(workspace, session_id)
            return self._launch_provider(value, session)

    def notify(self):
        wakeup = getattr(self, 'wakeup', None)
        if wakeup is not None:
            with wakeup:
                wakeup.notify_all()

    def wait_update(self, timeout):
        if timeout <= 0:
            return
        wakeup = getattr(self, 'wakeup', None)
        if wakeup is not None:
            with wakeup:
                wakeup.wait(timeout)
        else:
            time.sleep(timeout)

    def _unit_path(self, turn):
        return self.root / turn['session'] / (turn['id'] + '.unit')

    @staticmethod
    def _close_life(life):
        if life is None or life == -1:
            return
        if isinstance(life, socket.socket):
            try: life.close()
            except OSError: pass
        elif isinstance(life, int) and life >= 0:
            try: os.close(life)
            except OSError: pass

    def _close_turn_life(self, tid, life):
        sock_map = getattr(self, '_worker_socks', None)
        sock = sock_map.pop(tid, None) if sock_map is not None else None
        if sock is not None:
            try: sock.close()
            except OSError: pass
            return
        self._close_life(life)

    def _cleanup_reserved(self, warm):
        if warm is None or not warm.reserved or warm.process.poll() is None:
            return
        try:
            with self.store.connect() as db:
                exists = db.execute('SELECT 1 FROM sessions WHERE id=?', (warm.session_id,)).fetchone()
            if not exists:
                shutil.rmtree(self.root / warm.session_id, ignore_errors=True)
            warm.reserved = False
        except Exception:
            pass

    def _retire_warm(self, warm, *, wait_s=3.0):
        if warm is None:
            return
        warm.idle = False
        warm.standby = False
        warm.dead = True
        sock = warm.sock
        warm.sock = None
        if sock is not None:
            try: sock.sendall(b'{"cmd":"shutdown"}\n')
            except OSError: pass
            try: sock.close()
            except OSError: pass
        if wait_s > 0 and warm.process.poll() is None:
            try: warm.process.wait(timeout=wait_s)
            except subprocess.TimeoutExpired:
                try: warm.process.terminate()
                except OSError: pass
        self._cleanup_reserved(warm)

    def _retire_conflicting_warm(self, session_id, root_path):
        warm_map = getattr(self, 'warm_workers', None)
        if not warm_map:
            return
        target = Path(root_path)
        to_retire = []
        for sid, warm in list(warm_map.items()):
            if warm.dead or warm.process.poll() is not None:
                warm_map.pop(sid, None)
                continue
            other = Path(warm.root_path)
            if sid == session_id or target == other or target in other.parents or other in target.parents:
                warm_map.pop(sid, None)
                to_retire.append(warm)
        for warm in to_retire:
            self._retire_warm(warm, wait_s=2.0)

    def _watch_worker(self, warm):
        buf = bytearray()
        try:
            while True:
                sock = warm.sock
                if sock is None or sock.fileno() < 0:
                    break
                try:
                    ready, _, _ = select.select([sock], [], [], 0.2)
                except (OSError, ValueError):
                    break
                if not ready:
                    if warm.process.poll() is not None:
                        break
                    continue
                try:
                    chunk = sock.recv(4096)
                except OSError:
                    break
                if not chunk:
                    break
                buf.extend(chunk)
                while b'\n' in buf:
                    line, rest = bytes(buf).split(b'\n', 1)
                    buf = bytearray(rest)
                    if not line.strip():
                        continue
                    try:
                        msg = json.loads(line)
                    except ValueError:
                        continue
                    event = msg.get('event')
                    if event == 'notify':
                        self.notify()
                    elif event == 'accepted':
                        warm.accepted_tid = msg.get('tid')
                        warm.accepted_event.set()
                    elif event == 'idle':
                        finished_tid = msg.get('tid')
                        dl = float(msg.get('sandbox_deadline') or 0.0)
                        with self.lock:
                            warm.idle = True
                            warm.busy_tid = None
                            warm.sandbox_deadline = dl
                            if finished_tid:
                                self.workers.pop(finished_tid, None)
                                self._worker_socks.pop(finished_tid, None)
                                self.worker_targets.pop(finished_tid, None)
                            if self.closed:
                                self.warm_workers.pop(warm.session_id, None)
                                self._retire_warm(warm, wait_s=0.0)
                            else:
                                self._maybe_drain_queue(warm.workspace, warm.root_path, warm.session_id)
                        self.notify()
        finally:
            warm.dead = True
            warm.idle = False
            warm.standby = False
            warm.accepted_event.set()
            with self.lock:
                if getattr(self, 'warm_workers', None) and self.warm_workers.get(warm.session_id) is warm:
                    self.warm_workers.pop(warm.session_id, None)
                sock = warm.sock
                warm.sock = None
                if sock is not None:
                    try: sock.close()
                    except OSError: pass
                self._cleanup_reserved(warm)
                self._maybe_drain_queue(warm.workspace, warm.root_path, warm.session_id)
            self.notify()

    def _launch_provider(self, value, session):
        workspace, session_id = session['workspace'], session['id']
        policy = self.session_policy(session)
        tid = value['id']
        lease = self._unit_path(value)
        fd = worker_sock = parent_sock = None
        try:
            try:
                coding_fd = None
                root_path = json.loads(session['root'])['path']
                project = policy.project(workspace, root_path)
                if value.get('operation') == 'message' and project['kind'] == 'pipeline':
                    coding_fd = self._ensure_coding_catalog()
                # Admission durably says no spawn has been attempted. All
                # allocation failures before spawning are safely recoverable.
                lease.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
                fd = os.open(lease, os.O_RDWR | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600)
                fcntl.flock(fd, fcntl.LOCK_SH)
                info = os.fstat(fd)
                os.fsync(fd)
                from demiflow.forge.io import fsync_directory
                fsync_directory(lease.parent)
                fsync_directory(self.root)
                ownership = dict(unit_path=str(lease), unit_identity=dict(device=info.st_dev, inode=info.st_ino), nonce=value['epoch'])
                if value.get('operation') == 'message':
                    from demiflow.forge.development_state import claim_writer, writer_key
                    ownership['forge_writer'] = writer_key(str(self.root) + ':' + tid)
                self.store.update(tid, process=wire(ownership).decode())
                if ownership.get('forge_writer'):
                    try:
                        claim_writer(root_path, str(self.root) + ':' + tid, lease, ownership['unit_identity'])
                    except ForgeError as exc:
                        if exc.code != 'busy':
                            raise
                        # A terminal is visible just before the worker retires
                        # its claim, or retirement may have failed after commit.
                        self.store.retire_completed_writers(root_path)
                        claim_writer(root_path, str(self.root) + ':' + tid, lease, ownership['unit_identity'])

                # Try reusing a matching idle or standby warm worker for message / notebook turns
                # (strictly matching operation so readonly notebook workers and developer workers never mix).
                turn_op = value.get('operation', 'message')
                warmable_op = turn_op in {'message', 'notebook_cell', 'notebook_cell_cli'}
                warm_map = getattr(self, 'warm_workers', None)
                warm = warm_map.get(session_id) if (warm_map is not None and warmable_op) else None
                if (warm is not None and (warm.idle or warm.standby) and warm.busy_tid is None
                        and not warm.dead and warm.process.poll() is None
                        and warm.sock is not None
                        and warm.operation == turn_op
                        and warm.policy_hash == session['policy_hash']
                        and warm.provider == session['provider']
                        and warm.root_path == root_path
                        and (self.deployment.mode != 'restricted'
                             or warm.sandbox_deadline == 0.0
                             or warm.sandbox_deadline - time.time() > self.deployment.limits.turn_seconds + 60)):
                    if ownership.get('forge_writer'):
                        from demiflow.forge.development_state import start_writer
                        start_writer(ownership['forge_writer'])
                    self.store.update(tid, launch_phase='spawning')
                    self.worker_targets[tid] = (workspace, session_id, value['epoch'], session['root'])
                    warm.idle = False
                    warm.standby = False
                    warm.reserved = False
                    warm.busy_tid = tid
                    warm.accepted_tid = None
                    warm.accepted_event.clear()
                    handoff_ok = False
                    try:
                        payload = (json.dumps({'cmd': 'turn', 'tid': tid}, separators=(',', ':')) + '\n').encode()
                        warm.sock.sendmsg([payload], [(socket.SOL_SOCKET, socket.SCM_RIGHTS, array.array('i', [fd]))])
                        if warm.accepted_event.wait(2.0) and warm.accepted_tid == tid:
                            self.workers[tid] = (warm.process, warm.sock.fileno())
                            self._worker_socks[tid] = warm.sock
                            handoff_ok = True
                    except OSError:
                        handoff_ok = False
                    if handoff_ok:
                        return self.store.public_turn(self.store.turn(tid))
                    warm_map.pop(session_id, None)
                    self._retire_warm(warm, wait_s=1.0)

                self._retire_conflicting_warm(session_id, root_path)
                parent_sock, worker_sock = socket.socketpair(socket.AF_UNIX, socket.SOCK_STREAM)
                worker = Path(__file__).with_name('worker.py')
                env = self._worker_env(policy)
                # Must commit uncertainty before Popen: absence of a returned
                # Popen object does not prove absence of a real child.
                if ownership.get('forge_writer'):
                    from demiflow.forge.development_state import start_writer
                    start_writer(ownership['forge_writer'])
                self.store.update(tid, launch_phase='spawning')
                self.worker_targets[tid] = (workspace, session_id, value['epoch'], session['root'])
                command = [sys.executable, '-I', '-B', str(worker), str(self.root / (session['policy_hash'] + '.json')), tid, str(fd), str(worker_sock.fileno())]
                inherited = (fd, worker_sock.fileno())
                if coding_fd is not None:
                    command.append(str(coding_fd))
                    inherited += (coding_fd,)
                process = subprocess.Popen(command,
                    cwd=self.root, env=env, stdin=subprocess.DEVNULL, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
                    pass_fds=inherited, start_new_session=True)
                warm = _WarmWorker(session_id, session['policy_hash'], session['provider'], root_path, process, parent_sock,
                                   workspace=workspace, operation=turn_op)
                warm.busy_tid = tid
                if warm_map is not None and warmable_op:
                    warm_map[session_id] = warm
                self.workers[tid] = (process, parent_sock.fileno())
                self._worker_socks[tid] = parent_sock
                parent_sock = None
                threading.Thread(target=self._watch_worker, args=(warm,), name='demiflow-dev-watch', daemon=True).start()
            finally:
                try:
                    if worker_sock is not None: worker_sock.close()
                finally:
                    if fd is not None: os.close(fd)
        except BaseException:
            if parent_sock is not None:
                self._close_life(parent_sock)
            if tid in self.workers:
                proc, life = self.workers[tid]
                self._close_turn_life(tid, life)
                self.workers[tid] = (proc, -1)
            if tid not in self.workers: self.worker_targets.pop(tid, None)
            # A lost constructor return closes the parent pipe, so a real
            # child drains itself. Until then the inherited unit stays held.
            try: self._finish_unstarted(self.store.turn(tid))
            except Exception: pass  # Durable phase permits later recovery.
            raise
        return self.store.public_turn(self.store.turn(tid))

    def _finish_unstarted(self, turn):
        if turn['cleanup'] == 'complete':
            self.store.retire_writer(turn['id'])
            return True
        if turn['launch_phase'] == 'admitted':
            done = self.store.finish_unstarted(turn)
            if done: self.store.retire_writer(turn['id'])
            return done
        if turn['launch_phase'] == 'spawning':
            ownership = json.loads(turn['process'] or '{}')
            lease = self._unit_path(turn)
            if (ownership.get('nonce') != turn['epoch'] or ownership.get('unit_path') != str(lease)
                    or not ownership.get('unit_identity') or ownership.get('supervisor')):
                return False
            try: stopped = unit_stopped(lease, ownership['unit_identity'])
            except OSError: return False
            # Workers claim before running any tool. An unclaimed, unlocked
            # matching unit proves no worker remains able to enter that phase.
            if stopped:
                done = self.store.finish_unstarted(turn)
                if done: self.store.retire_writer(turn['id'])
                return done
        # Once claimed, require the worker's descendant-drain receipt. An absent
        # PID or unlocked fence alone cannot prove arbitrary tools were drained.
        return False

    def _reap(self):
        for tid, (process, life) in list(self.workers.items()):
            if process.poll() is not None:
                self._close_turn_life(tid, life)
                del self.workers[tid]
                target = self.worker_targets.pop(tid, None)
                if self.store.turn(tid)['cleanup'] != 'complete':
                    if not self._finish_unstarted(self.store.turn(tid)):
                        self.store.update(tid, state='interrupted', cleanup='unknown')
                self.store.retire_writer(tid)
                if target and not self.closed:
                    try:
                        root_path = json.loads(target[3])['path']
                        self._maybe_drain_queue(target[0], root_path, target[1])
                    except (ValueError, KeyError, IndexError):
                        pass
        warm_map = getattr(self, 'warm_workers', None)
        if warm_map:
            for sid, warm in list(warm_map.items()):
                if warm.dead or warm.process.poll() is not None:
                    warm_map.pop(sid, None)
                    self._close_life(warm.sock)
                    warm.sock = None
                    self._cleanup_reserved(warm)

    def show(self, workspace, sid):
        with self.lock:
            self._reap()
            value = self.store.show(workspace, sid)
        value['writable'] = False
        session_identity = json.loads(self.store.session(sid)['root'])
        try: project = self.project(workspace, session_identity['path'])
        except (ValueError, OSError): project = None
        if project:
            try:
                self.project(workspace, project['root'])
                value['writable'] = self.store.writable(workspace, sid, root_identity(project['root']),
                    self.current_hash(workspace,project['root']), self.binding(workspace,project['root'])[0].providers)
            except (ValueError, OSError):
                pass  # Old history stays readable after its root or grant changed.
        session = self.store.session(sid)
        if session:
            # Public policy metadata belongs to this session's immutable grant,
            # including busy turns and history read after a policy replacement.
            try:
                phash = session['policy_hash']
                cache = getattr(self, '_policy_meta_cache', None)
                providers = cache.get(phash) if cache is not None else None
                if providers is None:
                    from .common import read_file, strict_json
                    policy = strict_json(read_file(self.root / (phash + '.json'), POLICY_BYTES))
                    if digest(policy) == phash:
                        providers = policy['providers']
                        if cache is not None:
                            if len(cache) >= 64:
                                cache.pop(next(iter(cache)))
                            cache[phash] = providers
                if providers is not None:
                    provider = providers.get(session['provider'], {})
                    value.update(model=provider.get('model'), effort=provider.get('effort'))
            except Exception:
                pass  # Historical events remain readable if the grant is missing.
        return value

    def claim_cell_command(self, workspace, root, session_id, turn_id, pending_request_id, expected_digest):
        self.project(workspace, root)
        return self.store.claim_cell_command(workspace, session_id, turn_id, pending_request_id, expected_digest)

    def input(self, workspace, root, session_id, turn_id, pending_request_id, expected_digest, answer):
        self.project(workspace, root)
        self.store.show(workspace, session_id)
        pending = next((p for p in self.store.pending(turn_id) if p['id'] == pending_request_id), None)
        if pending:
            request = json.loads(pending['request'])
            if request.get('method') == 'notebook/command':
                if type(answer) is not dict or set(answer) not in ({'value'}, {'error'}):
                    reject('Invalid notebook command response', 'invalid_request')
            else:
                adapter_for(self.store.session(session_id)['provider']).answer(request, answer)
        return self.store.respond(workspace, session_id, turn_id, pending_request_id, expected_digest, answer)

    def cancel(self, workspace, root, session_id, turn_id, request_id):
        self.project(workspace, root); identifier(request_id)
        identifier(session_id); identifier(turn_id)
        with self.lock:
            target = self.worker_targets.get(turn_id)
            if target and (target[:2] != (workspace, session_id) or json.loads(target[3]) != root_identity(root)):
                reject('Turn does not belong to session/project', 'not_found')
            try:
                return self.store.cancel(workspace, session_id, turn_id)
            except StorageUnavailable:
                # Only a confirmed immutable owned target and a persistence
                # failure may use the independent stop channel. Rejections do not.
                owned = self.workers.get(turn_id)
                if target and owned and owned[1] not in (None, -1):
                    self._close_turn_life(turn_id, owned[1])
                    self.workers[turn_id] = (owned[0], -1)
                raise

    def recover(self, workspace, root, session_id, expected_revision):
        self.project(workspace, root)
        with self.lock:
            self._reap()
            value = self.store.show(workspace, session_id)
            if value['revision'] != expected_revision: reject('Session revision changed', 'conflict')
            turn = value['turn']
            if turn and turn['cleanup'] != 'complete':
                saved = self.store.turn(turn['id']); process = json.loads(saved['process'] or '{}')
                if self._finish_unstarted(saved):
                    value = self.store.show(workspace, session_id)
                    value['recovery'] = dict(status='clean', can_continue=True)
                    return value
                supervisor = process.get('supervisor')
                value['recovery'] = dict(status='live' if supervisor and process_identity(supervisor['pid']) == supervisor else 'ownership_unknown',
                    can_continue=False, reason='No new prompt will be sent; missing descendant cleanup proof retains the project lease')
            else: value['recovery'] = dict(status='clean', can_continue=True)
            return value

    def close(self):
        failure = None
        warm_to_wait = []
        with self.lock:
            if self.closed:
                return
            self.closed = True
            self.queue_shutdown.set()
            for tid, (_, life) in list(self.workers.items()):
                try:
                    t = self.store.turn(tid); s = self.store.session(t['session'])
                    self.store.cancel(s['workspace'], s['id'], tid)
                except Exception:
                    # The parent pipe is an independent stop path when control
                    # persistence is unavailable. Do not abandon live workers.
                    self._close_turn_life(tid, life)
                    self.workers[tid] = (self.workers[tid][0], -1)
            warm_map = getattr(self, 'warm_workers', None)
            if warm_map:
                for sid, warm in list(warm_map.items()):
                    warm_map.pop(sid, None)
                    warm_to_wait.append(warm)
                    if warm.idle or warm.standby:
                        self._retire_warm(warm, wait_s=0.0)
        deadline = time.monotonic() + 12
        while time.monotonic() < deadline:
            with self.lock:
                try:
                    self._reap()
                except Exception as exc:
                    failure = exc
                all_warm_done = all(w.process.poll() is not None for w in warm_to_wait)
                if not self.workers and all_warm_done: break
            time.sleep(0.05)
        for tid, (_, life) in list(self.workers.items()):
            self._close_turn_life(tid, life)
        for warm in warm_to_wait:
            self._retire_warm(warm, wait_s=0.5)
        self.workers.clear()
        self._worker_socks.clear()
        self.worker_targets.clear()
        with self.lock:
            if self.coding_catalog is not None:
                self.coding_catalog.close()
                self.coding_catalog = None
        os.close(self.owner)
        if failure is not None:
            reject('Development shutdown could not read final cleanup state', 'busy')


def connect(config_path, *, input_denied_paths=()):
    """Choose a closed backend without constructing a second local owner."""
    from demiflow.workbench.conversations.common import read_file
    from demiflow.workbench.conversations.common import strict_json
    from demiflow.workbench.conversations.existing_http import ExistingHTTP
    from demiflow.workbench.conversations.existing_http import FORMAT
    value = strict_json(read_file(config_path, POLICY_BYTES))
    if type(value) is not dict:
        reject('Invalid development deployment fields')
    return ExistingHTTP(config_path) if value.get('format') == FORMAT else Service(
        config_path, input_denied_paths=input_denied_paths)
