"""Project binding, bounded admission, persistent intent and owned workers."""
from __future__ import annotations

import fcntl
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import threading
import time

from demiflow.workbench.conversations.common import private_directory_bytes
from demiflow.workbench.conversations.common import digest
from demiflow.workbench.conversations.common import identifier
from demiflow.workbench.process import process_identity
from demiflow.workbench.conversations.common import reject
from demiflow.workbench.conversations.common import root_identity
from demiflow.workbench.conversations.common import wire
from demiflow.workbench.conversations.policy import Deployment
from demiflow.workbench.conversations.policy import proxy_environment
from demiflow.workbench.conversations.store import Store
from demiflow.workbench.conversations.store import StorageUnavailable
from demiflow.workbench.conversations.provider import adapter_for
from demiflow.resources.process_observation import unit_stopped


class Service:
    remote = False

    def __init__(self, config_path, *, input_denied_paths=()):
        # Host authority is not a user-maintained input allowlist. Pass these
        # paths only to the trusted worker, never to the provider environment.
        self.input_denied_paths = tuple(dict.fromkeys(str(Path(p).absolute())
            for p in (config_path, *input_denied_paths)))
        self.settings = None
        self.deployment = Deployment.load(config_path)
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
        self.coding_catalog = None
        self.policy_hash, self.config_path = self.seal_policy(self.deployment)
        for t in self.store.unresolved():
            self.store.interrupt_unresolved(t['id'])

    def seal_policy(self, deployment):
        """Persist the exact bounded non-secret policy before creating a session."""
        from demiflow.forge.io import fsync_directory
        value = json.loads(wire(deployment.value))
        for p in value['projects'].values(): p.pop('identity', None)
        revision = digest(value); path = self.root / (revision + '.json')
        raw = wire(value, 65536)
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
        value=strict_json(read_file(self.root/(session['policy_hash']+'.json'),65536))
        if digest(value)!=session['policy_hash']: reject('Conversation policy snapshot changed','conflict')
        return Deployment(value)

    def project(self, workspace, root):
        return self.binding(workspace, root)[0].project(workspace, root)

    def capabilities(self, workspace, root):
        deployment, _, _ = self.binding(workspace, root)
        caps=deployment.capabilities(workspace, root)
        return dict(caps, service_configured=True, project_authorized=caps['enabled'])

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
        args = (workspace, root_identity(root), provider_id, request_id, revision)
        if new_conversation:
            session = self.store.create_conversation(*args, session_kind=session_kind)
        else:
            session = self.store.ensure_project(*args)
        return dict(session, writable=True)

    def sessions(self, workspace, root, offset=0, limit=20):
        self.project(workspace, root)
        return self.store.list(workspace, offset, limit, identity=root_identity(root),
                               policy_hash=self.current_hash(workspace,root), providers=self.binding(workspace,root)[0].providers)

    def start_project(self, workspace, root, session_id, submission_id, expected_revision, text, operation='message'):
        self._require_writable(workspace,root,session_id)
        cell_capability = {'notebook_cell':'notebook_cells', 'notebook_cell_cli':'notebook_cli'}.get(operation)
        if cell_capability and not self.capabilities(workspace,root).get(cell_capability):
            reject('当前开发服务未提供只读的 Notebook 单元格生成。')
        return self.start(workspace,root,session_id,submission_id,expected_revision,text,operation=operation)

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

    def start(self, workspace, root, session_id, submission_id, expected_revision, text, *, operation="message"):
        self.project(workspace, root)
        caps = self.capabilities(workspace, root)
        session = self.store.show(workspace, session_id)
        if not caps['start'] or not any(p['id'] == session['provider'] and p['available'] for p in caps['providers']):
            reject('Development provider is unavailable in this deployment')
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
                return self.store.public_turn(existing)
            homes = private_directory_bytes(self.root, entries=self.deployment.limits.sessions + 64)
            used = sum(homes.values())
            allowance = self.deployment.limits.provider_bytes
            reserved = sum(max(0, allowance - homes.get(t['session'], 0)) for t in self.store.unresolved())
            if used + reserved + max(0, allowance - homes.get(session_id, 0)) > self.deployment.limits.private_total_bytes:
                reject('Retained provider homes exhaust private disk reservation', 'resource_limit')
            value, fresh = self.store.submit(workspace, session_id, submission_id, expected_revision, text, root_identity(root), self.current_hash(workspace,root),
                                             operation=operation)
            if not fresh: return self.store.public_turn(value)
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

    def _launch_provider(self, value, session):
        workspace, session_id = session['workspace'], session['id']
        policy = self.session_policy(session)
        tid = value['id']
        lease = self._unit_path(value)
        fd = read_fd = write_fd = None
        try:
            try:
                coding_fd = None
                project = policy.project(workspace, json.loads(session['root'])['path'])
                if value.get('operation') == 'message' and project['kind'] == 'pipeline':
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
                    coding_fd = self.coding_catalog.fileno()
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
                self.store.update(tid, process=wire(ownership).decode())
                read_fd, write_fd = os.pipe()
                worker = Path(__file__).with_name('worker.py')
                env = dict(PATH='/usr/bin:/bin', LANG='C.UTF-8', PYTHONDONTWRITEBYTECODE='1')
                env['DEMIFLOW_INPUT_DENIED_PATHS'] = wire(list(self.input_denied_paths), 65536).decode()
                if self.deployment.value.get('runtime_domain'):
                    env['DEMIFLOW_FORGE_DOMAIN'] = self.deployment.value['runtime_domain']
                for config in policy.providers.values():
                    if config.get('credential_env') and config['credential_env'] in os.environ:
                        env[config['credential_env']] = os.environ[config['credential_env']]
                    env.update(proxy_environment(config))
                # Must commit uncertainty before Popen: absence of a returned
                # Popen object does not prove absence of a real child.
                self.store.update(tid, launch_phase='spawning')
                self.worker_targets[tid] = (workspace, session_id, value['epoch'], session['root'])
                command = [sys.executable, '-I', '-B', str(worker), str(self.root / (session['policy_hash'] + '.json')), tid, str(fd), str(read_fd)]
                inherited = (fd, read_fd)
                if coding_fd is not None:
                    command.append(str(coding_fd))
                    inherited += (coding_fd,)
                process = subprocess.Popen(command,
                    cwd=self.root, env=env, stdin=subprocess.DEVNULL, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
                    pass_fds=inherited, start_new_session=True)
                self.workers[tid] = (process, write_fd)
            finally:
                try:
                    if read_fd is not None: os.close(read_fd)
                finally:
                    if fd is not None: os.close(fd)
        except BaseException:
            if write_fd is not None:
                os.close(write_fd)
                if tid in self.workers:
                    self.workers[tid] = (self.workers[tid][0], -1)
            if tid not in self.workers: self.worker_targets.pop(tid, None)
            # A lost constructor return closes the parent pipe, so a real
            # child drains itself. Until then the inherited unit stays held.
            try: self._finish_unstarted(self.store.turn(tid))
            except Exception: pass  # Durable phase permits later recovery.
            raise
        return self.store.public_turn(self.store.turn(tid))

    def _finish_unstarted(self, turn):
        if turn['cleanup'] == 'complete': return True
        if turn['launch_phase'] == 'admitted':
            return self.store.finish_unstarted(turn)
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
            if stopped: return self.store.finish_unstarted(turn)
        # Once claimed, require the worker's descendant-drain receipt. An absent
        # PID or unlocked fence alone cannot prove arbitrary tools were drained.
        return False

    def _reap(self):
        for tid, (process, life) in list(self.workers.items()):
            if process.poll() is not None:
                if life >= 0: os.close(life)
                del self.workers[tid]
                self.worker_targets.pop(tid, None)
                if self.store.turn(tid)['cleanup'] != 'complete':
                    if not self._finish_unstarted(self.store.turn(tid)):
                        self.store.update(tid, state='interrupted', cleanup='unknown')

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
                from .common import read_file, strict_json
                policy = strict_json(read_file(self.root / (session['policy_hash'] + '.json'), 65536))
                if digest(policy) == session['policy_hash']:
                    provider=policy['providers'].get(session['provider'],{})
                    value.update(model=provider.get('model'),effort=provider.get('effort'))
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
                if target and owned and owned[1] >= 0:
                    os.close(owned[1])
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
        with self.lock:
            if self.closed:
                return
            self.closed = True
            for tid, (_, life) in self.workers.items():
                try:
                    t = self.store.turn(tid); s = self.store.session(t['session'])
                    self.store.cancel(s['workspace'], s['id'], tid)
                except Exception:
                    # The parent pipe is an independent stop path when control
                    # persistence is unavailable. Do not abandon live workers.
                    if life >= 0:
                        os.close(life)
                        self.workers[tid] = (self.workers[tid][0], -1)
        deadline = time.monotonic() + 12
        while time.monotonic() < deadline:
            with self.lock:
                try:
                    self._reap()
                except Exception as exc:
                    failure = exc
                if not self.workers: break
            time.sleep(0.05)
        for _, life in self.workers.values():
            if life >= 0: os.close(life)
        self.workers.clear()
        self.worker_targets.clear()
        if self.coding_catalog is not None:
            self.coding_catalog.close()
        os.close(self.owner)
        if failure is not None:
            reject('Development shutdown could not read final cleanup state', 'busy')


def connect(config_path, *, input_denied_paths=()):
    """Choose a closed backend without constructing a second local owner."""
    from demiflow.workbench.conversations.common import read_file
    from demiflow.workbench.conversations.common import strict_json
    from demiflow.workbench.conversations.existing_http import ExistingHTTP
    from demiflow.workbench.conversations.existing_http import FORMAT
    value = strict_json(read_file(config_path, 65536))
    if type(value) is not dict:
        reject('Invalid development deployment fields')
    return ExistingHTTP(config_path) if value.get('format') == FORMAT else Service(
        config_path, input_denied_paths=input_denied_paths)
