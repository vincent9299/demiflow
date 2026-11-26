"""One dedicated Linux subreaper per active/warm session worker, never a Dataset executor."""
from __future__ import annotations

import array
import ctypes
import fcntl
import json
import os
from pathlib import Path
import select
import signal
import socket
import stat
import subprocess
import sys
import tempfile
import time

# Executed by absolute path with -I: no imports from the business cwd.
if __package__ in {None, ''}:
    sys.path.insert(0, str(Path(__file__).resolve().parents[3]))

from demiflow.resources.process_observation import descendants
from demiflow.resources.process_observation import drain_owned
from demiflow.resources.process_observation import require_pidfds
from demiflow.resources.process_observation import signal_owned
from demiflow.workbench.conversations.common import digest
from demiflow.workbench.conversations.common import directory_bytes
from demiflow.workbench.process import process_identity
from demiflow.workbench.conversations.common import read_file
from demiflow.workbench.conversations.common import reject
from demiflow.workbench.conversations.common import snapshot
from demiflow.workbench.conversations.common import wire
from demiflow.forge.schema import ForgeError
from demiflow.workbench.conversations.policy import Deployment, turn_deadline
from demiflow.workbench.conversations.store import Store
from demiflow.workbench.conversations.transport import Transport
from demiflow.workbench.conversations.provider import adapter_for
from demiflow.workbench.conversations.context import assemble
from demiflow.workbench.conversations.context import authoritative_source_paths
from demiflow.workbench.conversations.context import BridgeServer, INPUT_TOOL
from demiflow.forge.development import assess_turn, review_requested as needs_review, candidate_identity
from demiflow.forge.project_guard import capture_protected_files, enforce_protected_files
from demiflow.forge.source_snapshot import capture
from demiflow.forge.source_snapshot import differences
from demiflow.workbench.conversations.execution import launch_provider, runtime_paths

WARM_IDLE_SECONDS = 600
WARM_SANDBOX_SECONDS = 2400


def effective_project_python(project):
    """Return only explicitly configured per-project business Python; never inherit workbench env."""
    configured = project.get('python')
    return configured.strip() if isinstance(configured, str) and configured.strip() else None


_ROOT_OBSERVE_SKIP = {'workspace.json', 'AGENTS.md', 'PIPELINE_SPEC.md', 'notebooks', 'data_tools'}


def _root_entry_names(project_root) -> set[str]:
    """Visible top-level project entries; used to observe newly created root files/directories."""
    from demiflow.forge.source_snapshot import IGNORED_DIRS
    names = set()
    try:
        with os.scandir(project_root) as entries:
            for count, entry in enumerate(entries):
                if count >= 4096:
                    break
                if (entry.name.startswith('.') or entry.name in IGNORED_DIRS or entry.name in _ROOT_OBSERVE_SKIP
                        or entry.is_symlink()):
                    continue
                names.add(entry.name)
    except OSError:
        pass
    return names


def _declared_readonly_locations(root: Path, denied: list[Path], private_home: Path) -> list[Path]:
    """Return physical read-only local data roots explicitly declared in workspace.json.

    Never grants write access and skips any path overlapping denied/system scopes.
    """
    ws_file = root / 'workspace.json'
    if not ws_file.is_file():
        return []
    try:
        doc = json.loads(ws_file.read_text(encoding='utf-8'))
    except (OSError, ValueError):
        return []
    locations = doc.get('locations') if isinstance(doc, dict) else None
    if not isinstance(locations, dict):
        return []
    root_phys = root.resolve(strict=False)
    forbidden = [Path(p).resolve(strict=False) for p in (*denied, private_home, Path('/dev'), Path('/proc'), Path('/sys'), Path('/run'))]
    extra: list[Path] = []
    for loc in locations.values():
        if not isinstance(loc, dict):
            continue
        if loc.get('kind') not in (None, 'local'):
            continue
        is_readonly = (loc.get('writable') is False) or (loc.get('read_only') is True and loc.get('writable') is not True)
        if not is_readonly:
            continue
        raw_root = loc.get('root')
        if not isinstance(raw_root, str) or not raw_root:
            continue
        candidate = Path(raw_root).resolve(strict=False)
        if not candidate.is_dir():
            continue
        if candidate == root_phys or candidate.is_relative_to(root_phys):
            continue
        if any(candidate == bad or candidate.is_relative_to(bad) or bad.is_relative_to(candidate) for bad in forbidden):
            continue
        extra.append(candidate)
    return extra


def sandbox_spec(deployment, project, session, turn, manifest, config, private_home, *, seconds=None,
                 extra_read_paths=(), extra_runtime_paths=()):
    """Assemble only controller-declared scope; no provider-supplied path grants."""
    from demiflow.workbench.conversations.sandbox import SandboxSpec
    root = Path(project['root'])
    readonly = turn.get('operation') in ('compact', 'notebook_cell', 'notebook_cell_cli')
    notebook_op = turn.get('operation') in ('notebook_cell', 'notebook_cell_cli')
    sources = [] if notebook_op else [Path(source['canonical_path']) for source in manifest.get('sources', [])
               if source.get('origin') == 'authoritative_file']
    read = [] if notebook_op else [root, *[path for path in sources if path.is_file()], *(Path(p) for p in extra_read_paths)]
    openssl = Path('/etc/ssl/openssl.cnf')
    if openssl.is_file(): read.append(openssl)
    direct_network = bool(
        deployment.mode == 'restricted'
        and (getattr(deployment, 'direct_model', False) or (not readonly and bool(project.get('dev_network', False))))
    )
    active_cred = None
    if direct_network:
        for net_path in ('/etc/resolv.conf', '/etc/hosts', '/etc/nsswitch.conf', '/etc/gai.conf',
                         '/etc/ssl', '/etc/ca-certificates', '/usr/share/ca-certificates', '/etc/ld.so.cache'):
            candidate = Path(net_path)
            if candidate.exists():
                read.append(candidate)
        if getattr(deployment, 'direct_model', False) and config.get('native_credentials'):
            cred_path = Path(config['native_credentials']).resolve()
            if cred_path.is_file():
                active_cred = cred_path
                read.append(cred_path)
    from demiflow.workbench.python_sources import project_dependencies
    dependencies = None if notebook_op else project_dependencies(root, project=project)
    project_root_write = (
        not readonly
        and dependencies is None
        and project.get('kind') == 'pipeline'
        and project.get('phase') == 'implementation'
    )
    writes = (
        [] if readonly else
        [root] if project_root_write else
        [root / name for name in project['writes'] if Path(name).parts[:1] != ('data_tools',) and (root / name).exists()]
    )
    protected = [] if notebook_op else [root / name for name in ('.git', '.forge', '.venv', '.env', '.env.local', 'AGENTS.md', 'workspace.json', 'PIPELINE_SPEC.md', 'data_tools')]
    if not notebook_op and root.is_dir():
        try:
            with os.scandir(root) as root_entries:
                for entry in root_entries:
                    if entry.name.startswith('.env.') and entry.name not in {'.env.example', '.env.sample'}:
                        protected.append(root / entry.name)
        except OSError:
            pass
    if dependencies is not None:
        python = dependencies['environment']['interpreter']
        package_files = [root / item['path'] for item in dependencies['packages'].values()]
        read.extend(package_files)
        protected.extend((root / 'pyproject.toml', root / 'packages', Path(python['prefix']), *package_files))
    protected.extend(path for path in sources if path.name in {'AGENTS.md', 'PIPELINE_SPEC.md'})
    if not notebook_op:
        protected.extend(root / name for name in project.get('protected', []))
    if active_cred is not None:
        protected.append(active_cred)
    denied = [deployment.state / 'chat.sqlite3',
              deployment.state / 'service.lock', deployment.state / 'control.reserve']
    if deployment.value.get('runtime_domain'): denied.append(Path(deployment.value['runtime_domain']))
    with os.scandir(deployment.state) as entries:
        for index, entry in enumerate(entries):
            if index > deployment.limits.sessions + 64: reject('Sandbox control inventory exceeds budget', 'resource_limit')
            if entry.name == session['id']:
                with os.scandir(entry.path) as owned:
                    for count, item in enumerate(owned):
                        if count > deployment.limits.turns * 8 + 32: reject('Sandbox session inventory exceeds budget', 'resource_limit')
                        if Path(item.path) != private_home: denied.append(Path(item.path))
            elif entry.is_dir(follow_symlinks=False) or entry.name.endswith('.json') or entry.name.startswith('ai-key-'):
                denied.append(Path(entry.path))
    for provider in deployment.providers.values():
        if provider.get('native_credentials'):
            cred = Path(provider['native_credentials'])
            if active_cred is None or cred.resolve() != active_cred:
                denied.append(cred)
    if not notebook_op:
        read.extend(_declared_readonly_locations(root, denied, Path(private_home)))
    unique = lambda paths: tuple(sorted({str(path.absolute()) for path in paths}))
    if seconds is not None:
        duration = seconds
    elif deployment.limits.turn_seconds == 0 and not readonly:
        duration = 0
    else:
        duration = max(.1, turn['created'] + (deployment.limits.turn_seconds or 3600) - time.time())
    spec_seconds = 0 if (not readonly and duration == 0) else min(86400, max(.1, duration))
    rt = tuple(sorted({*runtime_paths(config, project_python=None if notebook_op else effective_project_python(project)),
                       *(str(Path(p).absolute()) for p in extra_runtime_paths)}))
    return SandboxSpec(read_paths=unique(read), write_paths=unique(writes),
        runtime_paths=rt,
        protected_paths=unique(protected), denied_paths=unique(denied), private_home=str(private_home),
        role='readonly' if readonly else 'developer',
        seconds=spec_seconds, rss_bytes=deployment.limits.rss_bytes,
        processes=deployment.limits.processes, file_bytes=min(64*1024**2, deployment.limits.provider_bytes),
        private_bytes=min(deployment.limits.provider_bytes, deployment.limits.private_total_bytes),
        direct_network=direct_network,
        read_policy='scoped' if readonly or dependencies is not None else 'inherit_os',
        write_policy='project_root' if project_root_write else 'scoped')


def automatic_approval(project, request, *, assurance):
    """Reuse an existing grant; business input and permission expansion still wait."""
    if not request.get('allow_once'):
        return False
    if project['kind'] != 'pipeline' or assurance != 'enforced':
        return False
    method = request['method']
    if method.endswith('requestUserInput') or method == 'item/permissions/requestApproval': return False
    if method == 'item/fileChange/requestApproval':
        # The adapter has already matched every effective changed path to writes.
        return True
    if method in {'item/commandExecution/requestApproval', 'session/request_permission'}:
        return True
    return False


def _teardown_provider(process, transport, provider, model_broker, control_bridge, shims=None):
    """Cooperatively close and drain owned provider/broker/bridge resources."""
    reason = None
    if control_bridge is not None:
        try: control_bridge.close()
        except BaseException: pass
    drained = False
    try:
        if process is not None:
            if provider is not None and getattr(provider, 'acp', False) and transport is not None and not transport.closed:
                try:
                    close_key = provider.close_request(); close_deadline = time.monotonic() + 2
                    while time.monotonic() < close_deadline and close_key not in transport.responses and not transport.closed:
                        transport.poll(.05)
                    if close_key in transport.responses: transport.take(close_key)
                except Exception:
                    # A failed cooperative close must never skip owned-tree
                    # termination or release its reservation prematurely.
                    pass
            signal_owned(os.getpid(), signal.SIGTERM)
            drained = drain_owned(process, timeout_s=5)
        else:
            # Public CLI subprocesses may have failed before provider spawn.
            signal_owned(os.getpid(), signal.SIGKILL)
            drained = not descendants(os.getpid())
        if transport is not None: transport.close()
    except BaseException as exc:
        reason = 'Cleanup failed: ' + type(exc).__name__
    if model_broker is not None:
        try: model_broker.close()
        except BaseException as exc:
            reason = reason or ('Model broker cleanup failed: ' + type(exc).__name__)
            drained = False
    if shims is not None:
        try: shims.close()
        except BaseException: pass
    return drained, reason


def _sandbox_compatible(standby_spec, actual_spec):
    if standby_spec is None or actual_spec is None:
        return standby_spec is actual_spec
    return (
        standby_spec.read_paths == actual_spec.read_paths
        and standby_spec.write_paths == actual_spec.write_paths
        and standby_spec.runtime_paths == actual_spec.runtime_paths
        and standby_spec.protected_paths == actual_spec.protected_paths
        and standby_spec.private_home == actual_spec.private_home
        and standby_spec.role == actual_spec.role
        and standby_spec.direct_network == actual_spec.direct_network
        and standby_spec.read_policy == actual_spec.read_policy
        and standby_spec.write_policy == actual_spec.write_policy
    )


def _poll_parent_cmd(parent_sock, buf, fds, *, timeout=0.0):
    """Non-blocking or bounded read of one parent command (+ optional SCM_RIGHTS fd)."""
    def _pop_cmd():
        while b'\n' in buf:
            line, rest = bytes(buf).split(b'\n', 1)
            buf[:] = rest
            if not line.strip():
                continue
            msg = json.loads(line)
            if msg.get('cmd') == 'turn' and isinstance(msg.get('tid'), str) and fds:
                next_lease = fds.pop(0)
                return ('turn', msg['tid'], next_lease)
            if msg.get('cmd') == 'shutdown':
                return ('shutdown',)
        return None

    cmd = _pop_cmd()
    if cmd is not None:
        return cmd
    if parent_sock is None:
        return ('closed',)
    ready, _, _ = select.select([parent_sock], [], [], timeout)
    if not ready:
        return None
    try:
        data, ancdata, _, _ = parent_sock.recvmsg(4096, socket.CMSG_SPACE(array.array('i').itemsize * 4))
    except BlockingIOError:
        return None
    except OSError:
        return ('closed',)
    for cmsg_level, cmsg_type, cmsg_data in ancdata:
        if cmsg_level == socket.SOL_SOCKET and cmsg_type == socket.SCM_RIGHTS:
            received = array.array('i')
            received.frombytes(cmsg_data[:len(cmsg_data) - (len(cmsg_data) % received.itemsize)])
            fds.extend(received)
    if not data:
        return ('closed',)
    buf.extend(data)
    if len(buf) > 65536:
        return ('closed',)
    return _pop_cmd()


def _wait_next_turn(parent_sock, *, process, transport, stopping_signal, idle_deadline, sandbox_deadline, min_remaining, buf=None, fds=None):
    """Park a warm worker until the next turn arrives or idle/shutdown triggers."""
    buf = buf if buf is not None else bytearray()
    fds = fds if fds is not None else []
    while True:
        if stopping_signal[0]:
            return None
        if process is None or process.poll() is not None:
            return None
        if transport is None or transport.closed:
            return None
        now = time.time()
        if now >= idle_deadline or (sandbox_deadline > 0 and sandbox_deadline - now <= min_remaining):
            return None
        cmd = _poll_parent_cmd(parent_sock, buf, fds, timeout=0.1)
        if cmd is None:
            continue
        if cmd[0] == 'turn':
            return cmd[1], cmd[2]
        return None


def run(config_path, tid, lease_fd, parent_fd, coding_fd=None, *, standby=None):
    deployment = Deployment.load(config_path)
    store = Store(deployment.state, deployment.limits)
    limits = deployment.limits
    denied_extra = json.loads(os.environ.pop('DEMIFLOW_INPUT_DENIED_PATHS', '[]'))
    parent_is_sock = stat.S_ISSOCK(os.fstat(parent_fd).st_mode)
    parent_sock = socket.socket(fileno=parent_fd) if parent_is_sock else None
    if parent_sock is not None:
        parent_sock.setblocking(False)
    else:
        os.set_blocking(parent_fd, False)

    stopping_signal = [False]
    for sig in (signal.SIGTERM, signal.SIGINT):
        signal.signal(sig, lambda *_: stopping_signal.__setitem__(0, True))
    if ctypes.CDLL(None, use_errno=True).prctl(36, 1, 0, 0, 0):
        reject('Cannot adopt owned provider descendants')
    require_pidfds()

    def send_parent(event, *, reliable=False, **fields):
        if parent_sock is None:
            return False
        raw = (json.dumps({'event': event, **fields}, separators=(',', ':')) + '\n').encode()
        if not reliable:
            try:
                parent_sock.sendall(raw)
                return True
            except OSError:
                return False
        parent_sock.setblocking(True)
        try:
            parent_sock.settimeout(2.0)
            parent_sock.sendall(raw)
            return True
        except OSError:
            return False
        finally:
            parent_sock.setblocking(False)

    def parent_closed():
        if parent_sock is not None:
            try:
                peek = parent_sock.recv(1, socket.MSG_PEEK | socket.MSG_DONTWAIT)
                return peek == b''
            except BlockingIOError:
                return False
            except OSError:
                return True
        try:
            return os.read(parent_fd, 1) == b''
        except BlockingIOError:
            return False

    process = transport = provider = model_broker = control_bridge = coding = shims = None
    effective_bound = None
    standby_spec = None
    confined = None
    sandbox_deadline = 0.0
    warm_ready = False
    warm_operation = None
    provider_opened = False
    already_claimed = False
    acp = False
    parent_buf = bytearray()
    parent_fds = []

    try:
        if standby is not None:
            standby_info = json.loads(standby)
            st_operation = standby_info.get('operation', 'message')
            st_cell_cli = st_operation == 'notebook_cell_cli'
            st_notebook = st_cell_cli or st_operation == 'notebook_cell'
            if st_notebook and deployment.mode != 'restricted':
                return
            st_project = deployment.project(standby_info['workspace'], standby_info['root_path'])
            if st_notebook:
                st_project = dict(st_project, writes=[])
            st_guided = (not st_notebook) and st_project['kind'] == 'pipeline'
            if st_guided and coding_fd is not None:
                from demiflow.forge.api_reference import REFERENCE_BYTES
                try:
                    raw_coding = os.pread(coding_fd, REFERENCE_BYTES + 1, 0)
                finally:
                    os.close(coding_fd)
                    coding_fd = None
                if len(raw_coding) <= REFERENCE_BYTES:
                    coding = json.loads(raw_coding)
            st_home = deployment.state / standby_info['session_id']
            st_home.mkdir(mode=0o700, exist_ok=True)
            st_provider_home = st_home / 'provider' if deployment.mode == 'restricted' else st_home
            st_provider_home.mkdir(mode=0o700, exist_ok=True)
            st_broker_home = (st_home / 'control' / 'standby' / 'model'
                              if deployment.mode == 'restricted' else None)
            if st_broker_home is not None:
                st_broker_home.mkdir(mode=0o700, parents=True, exist_ok=True)
            st_config = deployment.providers[standby_info['provider']]
            st_adapter = adapter_for(standby_info['provider'], st_operation)
            acp = getattr(st_adapter, 'acp', False)
            if st_cell_cli:
                from demiflow.workbench.notebooks.cell_commands import CellCommands
                control_bridge = CellCommands(store, None, 0.0)
            elif acp and st_guided:
                control_bridge = BridgeServer(None)
            st_manifest = dict(sources=[] if st_notebook else [
                dict(origin='authoritative_file', canonical_path=str(p))
                for p in authoritative_source_paths(st_project)])
            st_session = dict(id=standby_info['session_id'], workspace=standby_info['workspace'],
                              provider=standby_info['provider'], policy_hash=standby_info['policy_hash'])
            st_py = effective_project_python(st_project)
            if st_guided:
                from demiflow.workbench.conversations.sandbox_shims import SandboxShims
                shims = SandboxShims(st_project, execution_mode=deployment.mode,
                                     project_python=st_py, input_access=True)
            warm_sandbox_secs = (0 if limits.turn_seconds == 0
                                 else min(86400, max(limits.turn_seconds, WARM_SANDBOX_SECONDS)))
            standby_spec = (sandbox_spec(deployment, st_project, st_session,
                                         {'operation': st_operation, 'created': time.time()},
                                         st_manifest, st_config, st_provider_home, seconds=warm_sandbox_secs,
                                         extra_read_paths=shims.read_paths if shims is not None else (),
                                         extra_runtime_paths=shims.runtime_paths if shims is not None else ())
                            if deployment.mode == 'restricted' else None)
            warm_operation = st_operation
            standby_lease = None
            try:
                st_cwd = str(st_provider_home) if st_notebook else st_project['root']
                if standby_spec is not None:
                    sandbox_deadline = (time.time() + standby_spec.seconds) if standby_spec.seconds > 0 else 0.0
                    if st_cell_cli:
                        from dataclasses import replace
                        standby_spec = replace(standby_spec, runtime_paths=(*standby_spec.runtime_paths, str(control_bridge.bin)))
                    from demiflow.workbench.conversations.sandbox import specification
                    specification(standby_spec, [sys.executable], {}, st_cwd)
                    if not getattr(deployment, 'direct_model', False):
                        from demiflow.workbench.conversations.model_broker import start_model_broker
                        model_broker = start_model_broker(deployment, None, st_session, st_broker_home, defer_handshake=True)
                        st_config = model_broker.child_config
                    if st_cell_cli:
                        st_config = {**st_config, '_cell_bin': str(control_bridge.bin)}
                else:
                    standby_lease = tempfile.TemporaryFile(dir=st_home)
                if st_notebook:
                    st_py = None
                if st_py:
                    st_config = {**st_config, '_project_python': st_py}
                if shims is not None:
                    st_config = {**st_config, '_shim_bin': str(shims.bin)}
                process = launch_provider(
                    mode=deployment.mode, adapter=st_adapter, config=st_config,
                    home=st_provider_home, root=st_cwd,
                    lease_fd=standby_lease.fileno() if standby_lease is not None else None,
                    sandbox_spec=standby_spec,
                    control_socket=control_bridge.path if control_bridge is not None else None,
                    model_socket=model_broker.socket_path if model_broker is not None else None,
                )
            finally:
                if standby_lease is not None:
                    standby_lease.close()
            transport = Transport(process, limits, acp=acp)
            provider = st_adapter(transport, st_config, dict(st_project, root=st_cwd) if st_notebook else st_project, None)

            def _check_standby_parent():
                nonlocal tid, lease_fd, already_claimed
                if stopping_signal[0]:
                    return 'stop'
                cmd = _poll_parent_cmd(parent_sock, parent_buf, parent_fds, timeout=0.0)
                if cmd is None:
                    return None
                if cmd[0] == 'turn' and tid is None:
                    next_tid, next_lease = cmd[1], cmd[2]
                    if not send_parent('accepted', reliable=True, tid=next_tid):
                        try: os.close(next_lease)
                        except OSError: pass
                        return 'stop'
                    tid, lease_fd = next_tid, next_lease
                    try:
                        turn_row = store.turn(tid)
                        unit = os.fstat(lease_fd)
                        store.claim_worker(tid, turn_row['epoch'], dict(device=unit.st_dev, inode=unit.st_ino), process_identity(os.getpid()))
                        already_claimed = True
                        send_parent('notify')
                    except BaseException:
                        pass
                    return None
                return 'stop'

            def _wait_standby(key, timeout=40):
                until = time.monotonic() + timeout
                while True:
                    status = _check_standby_parent()
                    if status == 'stop':
                        return None
                    if tid is not None:
                        try:
                            if store.turn(tid)['stop_at']:
                                raise InterruptedError('stop_requested')
                        except BaseException:
                            raise
                    if time.monotonic() > until:
                        raise TimeoutError('Standby provider handshake timeout')
                    transport.poll(0.05)
                    response = transport.take(key)
                    if response is not None:
                        return response
                    if transport.closed:
                        raise ConnectionError('Standby provider stdout closed before acknowledgement')

            standby_aborted = False
            try:
                handshake = _wait_standby(provider.initialize(), timeout=40)
                if handshake is None:
                    standby_aborted = True
                else:
                    configuration_key = provider.initialized(handshake)
                    cfg_resp = _wait_standby(configuration_key, timeout=30) if configuration_key else {}
                    if cfg_resp is None:
                        standby_aborted = True
                    else:
                        effective = provider.verify_config(cfg_resp)
                        effective['execution_assurance'] = getattr(process, 'execution_assurance', 'host_permissions')
                        effective_bound = dict(effective)
                        if model_broker is not None:
                            broker_until = time.monotonic() + 35
                            while not model_broker._ready.wait(0.05):
                                if _check_standby_parent() == 'stop':
                                    standby_aborted = True
                                    break
                                if tid is not None and store.turn(tid)['stop_at']:
                                    raise InterruptedError('stop_requested')
                                if time.monotonic() > broker_until:
                                    raise TimeoutError('Standby model broker handshake timed out')
                            if not standby_aborted:
                                model_broker.wait_ready(timeout=1)
                        if not standby_aborted:
                            warm_ready = True
                            provider_opened = False
            except BaseException:
                _teardown_provider(process, transport, provider, model_broker, control_bridge, shims)
                process = transport = provider = model_broker = control_bridge = shims = None
                effective_bound = None
                standby_spec = None
                warm_ready = False
                provider_opened = False
                if tid is None:
                    return
                # A real turn was already accepted mid-boot; fall through into the
                # main turn loop so it either cancels cleanly or falls back to cold spawn.

            if standby_aborted:
                if tid is None:
                    return
                _teardown_provider(process, transport, provider, model_broker, control_bridge, shims)
                process = transport = provider = model_broker = control_bridge = shims = None
                warm_ready = False
                provider_opened = False

            if tid is None:
                if not send_parent('idle', reliable=True, tid=None, standby=True, sandbox_deadline=sandbox_deadline):
                    return
                next_work = _wait_next_turn(
                    parent_sock,
                    process=process,
                    transport=transport,
                    stopping_signal=stopping_signal,
                    idle_deadline=time.time() + WARM_IDLE_SECONDS,
                    sandbox_deadline=sandbox_deadline if deployment.mode == 'restricted' else 0.0,
                    min_remaining=limits.turn_seconds + 60,
                    buf=parent_buf,
                    fds=parent_fds,
                )
                if next_work is None:
                    return
                next_tid, next_lease = next_work
                if not send_parent('accepted', reliable=True, tid=next_tid):
                    try: os.close(next_lease)
                    except OSError: pass
                    return
                tid, lease_fd = next_tid, next_lease

        while True:
            turn = store.turn(tid); session = store.session(turn['session'])
            compact = turn.get('operation') == 'compact'
            cell_cli = turn.get('operation') == 'notebook_cell_cli'
            notebook = cell_cli or turn.get('operation') == 'notebook_cell'
            with store.connect() as db:
                history_boundary = db.execute('SELECT min(sequence)-1 FROM events WHERE turn=?', (tid,)).fetchone()[0] or 0
            project = deployment.project(session['workspace'], json.loads(session['root'])['path'])
            guided = not (compact or notebook) and project['kind'] == 'pipeline'
            delivery_review = None
            if guided and project['phase'] == 'implementation':
                from demiflow.workbench.conversations.pipeline_review import DeliveryReview
                delivery_review = DeliveryReview(store, turn, project)
            watched = [] if notebook else project['watch']
            excluded = project.get('protected', ()) if guided else ()
            home = deployment.state / session['id']; scratch = home / turn['id']; scratch.mkdir(mode=0o700, exist_ok=True)
            forge_opts = json.loads(turn.get('forge_options') or '{}')
            auto_review = bool(forge_opts.get('auto_review'))
            rework_depth = int(forge_opts.get('round', forge_opts.get('rework_depth')) or 0)
            broker_home = (home / 'control' / (tid + '-' + turn['epoch']) / 'model'
                           if deployment.mode == 'restricted' else None)
            final, reason, completed = 'failed', None, False
            requested = False
            deadline = turn_deadline(turn, limits)
            stop_started = None
            last_sample, peaks = 0, {'rss_bytes': 0, 'private_bytes': 0, 'processes': 0}
            last_turn_check, cached_turn = 0.0, turn
            provider_turn_bound = bool(turn.get('provider_turn'))
            pending_events, pending_bytes, last_flush = [], 0, time.monotonic()
            outstanding = {}
            steering = steer_rpc = None
            input_access = None

            if already_claimed:
                already_claimed = False
            else:
                unit = os.fstat(lease_fd)
                store.claim_worker(tid, turn['epoch'], dict(device=unit.st_dev, inode=unit.st_ino), process_identity(os.getpid()))
                send_parent('notify')

            def flush_events():
                nonlocal pending_bytes, last_flush
                if not pending_events:
                    return
                batch = list(pending_events)
                pending_events.clear()
                pending_bytes = 0
                last_flush = time.monotonic()
                store.events_batch(tid, batch)
                send_parent('notify')

            def tick(*, force=False):
                nonlocal last_sample, last_turn_check, cached_turn, requested, stop_started, reason
                now = time.monotonic()
                if force or now - last_turn_check >= 0.05:
                    cached_turn = store.turn(tid)
                    last_turn_check = now
                current = cached_turn
                parent_gone = parent_closed()
                stop = bool(current['stop_at'] or parent_gone or stopping_signal[0] or time.time() > deadline)
                if stop and not requested:
                    requested = True; stop_started = now
                    reason = 'parent_closed' if parent_gone else 'deadline' if time.time() > deadline else 'stop_requested'
                    flush_events()
                    store.update(tid, state='stopping')
                    send_parent('notify')
                    if provider is not None: provider.interrupt()
                if requested and now - stop_started >= 5:
                    signal_owned(os.getpid(), signal.SIGTERM)
                    if now - stop_started >= 7:
                        signal_owned(os.getpid(), signal.SIGKILL)
                        raise InterruptedError(reason)
                if now - last_sample >= 0.2:
                    last_sample = now
                    rows = descendants(os.getpid(), include_owner=True)
                    rss = sum(r['rss_bytes'] for r in rows.values())
                    peaks['rss_bytes'] = max(peaks['rss_bytes'], rss); peaks['processes'] = max(peaks['processes'], len(rows))
                    if rss > limits.rss_bytes or len(rows) > limits.processes:
                        reject('Development process tree exceeded sampled budget', 'resource_limit')
                    used = directory_bytes(home)
                    peaks['private_bytes'] = max(peaks['private_bytes'], used)
                    if used > limits.provider_bytes: reject('Provider private directory exceeded sampled budget', 'resource_limit')
                return current

            def wait(key, timeout=30):
                until = time.monotonic() + timeout
                while True:
                    tick()
                    if requested: raise InterruptedError(reason)
                    if time.monotonic() > until: raise TimeoutError('Provider RPC acknowledgement timeout')
                    transport.poll()
                    response = transport.take(key)
                    if response is not None: return response
                    if transport.closed: raise ConnectionError('Provider stdout closed before acknowledgement')

            def emit(kind, data):
                nonlocal pending_bytes
                if kind in {'message.delta', 'message.completed', 'output.delta'}:
                    content = data.get('text', '')
                    encoded_len = len(content.encode())
                    if encoded_len > 32768:
                        if kind == 'message.completed':
                            pending_events.append(('message.completed', dict(data, text='', paged=True), False))
                            kind = 'output.delta'
                        for offset in range(0, len(content), 8000):
                            part = content[offset:offset + 8000]
                            pending_events.append((kind, dict(data, text=part), False))
                            pending_bytes += len(part.encode())
                        flush_events()
                        return
                    if kind in {'message.delta', 'output.delta'} and pending_events:
                        prev_kind, prev_data, _ = pending_events[-1]
                        if prev_kind == kind and {k: v for k, v in prev_data.items() if k != 'text'} == {k: v for k, v in data.items() if k != 'text'}:
                            prev_text = prev_data.get('text', '')
                            if len(prev_text.encode()) + encoded_len <= 16384:
                                pending_events[-1] = (prev_kind, dict(prev_data, text=prev_text + content), False)
                                pending_bytes += encoded_len
                                if pending_bytes >= 4096 or time.monotonic() - last_flush >= 0.016:
                                    flush_events()
                                return
                    pending_events.append((kind, data, False))
                    pending_bytes += encoded_len
                    if kind == 'message.completed' or pending_bytes >= 4096 or time.monotonic() - last_flush >= 0.016:
                        flush_events()
                    return
                pending_events.append((kind, data, False))
                pending_bytes += 256
                if kind != 'usage.observed' or len(pending_events) >= 16 or time.monotonic() - last_flush >= 0.016:
                    flush_events()

            protected_snapshot = {}
            root_names_before = None
            try:
                tick()
                if requested: raise InterruptedError(reason)
                if guided:
                    from demiflow.forge.api_reference import REFERENCE_BYTES, sdk_identity
                    if coding is None:
                        if coding_fd is None:
                            reject('Current SDK reference was not prepared')
                        try:
                            raw = os.pread(coding_fd, REFERENCE_BYTES + 1, 0)
                        finally:
                            os.close(coding_fd)
                            coding_fd = None
                        if len(raw) > REFERENCE_BYTES:
                            reject('SDK reference exceeds byte budget', 'resource_limit')
                        coding = json.loads(raw)
                    if sdk_identity(coding['catalog']) != coding['sdk_revision']:
                        reject('Native SDK changed before context delivery', 'conflict')
                baseline = capture(project['root'], watched, exclude=excluded, source_dir=scratch / 'sources')
                root_names_before = _root_entry_names(project['root']) if guided else None
                protected_snapshot = capture_protected_files(project['root']) if (not compact and not notebook) else {}
                store.update(tid, baseline=wire(baseline).decode())
                if compact:
                    prompt = ''
                    manifest = dict(operation='compact', provider=session['provider'], delivery='not_applicable')
                    store.update(tid, context=wire(manifest).decode(), delivery='not_applicable')
                elif notebook:
                    if deployment.mode != 'restricted':
                        reject('Notebook generation requires a read-only project scope')
                    project = dict(project, writes=[])
                    if cell_cli:
                        if not (warm_ready and warm_operation == turn.get('operation', 'message') and control_bridge is not None):
                            from demiflow.workbench.notebooks.cell_commands import CellCommands
                            control_bridge = CellCommands(store, turn, deadline)
                        skill = Path(__file__).parents[1] / 'notebooks/skills/dataskill/SKILL.md'
                        prompt = skill.read_text(encoding='utf-8')
                    else:
                        prompt = ('你是当前 Notebook 的数据助手。根据用户意图和前文生成一格可以直接在当前 Python 内核运行的代码。'
                                  '查阅项目 data_tools 中已声明的函数、类和组件，按真实签名组合。需要交互时使用 ipywidgets，'
                                  '将筛选、联动和人工确认保存在控件内。数据和单元格内容是参考资料。不要编辑文件或运行用户的任务。'
                                  '对于写入解释保存目标，把写入放在显式确认回调中。信息不足时直接提问。'
                                  '简短回答，把完整单元格放在一个围栏标为 python 的代码块里。')
                    manifest = dict(operation=turn['operation'], sources=[], bytes=len(prompt.encode()))
                    store.update(tid, context=wire(manifest).decode(), delivery='prepared')
                else:
                    if guided:
                        from demiflow.workbench.conversations.inputs import InputAccess
                        denied_inputs = [deployment.state, Path(config_path).resolve()]
                        denied_inputs.extend(Path(p) for p in denied_extra)
                        if deployment.value.get('runtime_domain'):
                            denied_inputs.append(Path(deployment.value['runtime_domain']))
                        denied_inputs.extend(Path(value['native_credentials']) for value in deployment.providers.values()
                                             if value.get('native_credentials'))
                        input_access = InputAccess(store, tid, project, scratch, denied_paths=denied_inputs,
                            runtime_paths=runtime_paths(deployment.providers[session['provider']]), deadline=deadline)
                    from demiflow.workbench.conversations.pipeline_review import recent_review
                    manifest, prompt = assemble(project, turn['text'], limits,
                        execution_mode=deployment.mode, input_access=input_access is not None, coding=coding,
                        delivery_review=delivery_review is not None,
                        previous_review=recent_review(store, session['id']) if guided else None)
                    store.update(tid, context=wire(manifest).decode(), delivery='prepared')
                    store.event(tid, 'context.bound', {'digest': manifest['digest'], 'kind': manifest['kind'],
                        'sources': len(manifest['sources']), 'bytes': manifest['bytes'], 'delivery': 'prepared'})
                    send_parent('notify')
                tick()
                if requested: raise InterruptedError(reason)

                if forge_opts.get('origin') == 'forge':
                    from demiflow.forge.development import feedback_current
                    original = store.session(session['id'])
                    if not feedback_current(project['root'], forge_opts,
                            dict(provider=original['provider'], session=original['provider_session'])):
                        reject('自动修正候选或原作者已变化，输入未发送。', 'inputs_changed')

                turn_op = turn.get('operation', 'message')
                if warm_ready and warm_operation != turn_op:
                    _teardown_provider(process, transport, provider, model_broker, control_bridge, shims)
                    process = transport = provider = model_broker = control_bridge = shims = None
                    effective_bound = None
                    standby_spec = None
                    warm_ready = False
                    provider_opened = False

                if warm_ready and not provider_opened:
                    proj_py = effective_project_python(project)
                    shims_ok = ((shims is None and not guided) or (shims is not None and shims.project_python == proj_py
                                  and shims.dependency_lock == project.get('dependency_lock')))
                    compatible = (not compact) and (warm_operation == turn_op) and shims_ok
                    if compatible and deployment.mode == 'restricted':
                        provider_home = home / 'provider'
                        provider_home.mkdir(mode=0o700, exist_ok=True)
                        actual_spec = sandbox_spec(deployment, project, session, turn, manifest,
                            deployment.providers[session['provider']], provider_home,
                            seconds=(None if sandbox_deadline == 0.0 else max(.1, sandbox_deadline - time.time())),
                            extra_read_paths=shims.read_paths if shims is not None else (),
                            extra_runtime_paths=shims.runtime_paths if shims is not None else ())
                        if cell_cli and control_bridge is not None:
                            from dataclasses import replace
                            actual_spec = replace(actual_spec, runtime_paths=(*actual_spec.runtime_paths, str(control_bridge.bin)))
                        from demiflow.workbench.conversations.sandbox import specification
                        specification(actual_spec, [sys.executable], {}, project['root'])
                        compatible = _sandbox_compatible(standby_spec, actual_spec)
                        confined = standby_spec
                    if not compatible:
                        _teardown_provider(process, transport, provider, model_broker, control_bridge, shims)
                        process = transport = provider = model_broker = control_bridge = shims = None
                        effective_bound = None
                        standby_spec = None
                        warm_ready = False

                if warm_ready:
                    if model_broker is not None:
                        model_broker.begin_turn(turn, limits)
                    if cell_cli and control_bridge is not None:
                        control_bridge.set_turn(turn, deadline)
                    elif control_bridge is not None and input_access is not None:
                        layout_root = Path(project['root']) if (
                            not compact and not notebook
                            and (project.get('writes') or (project.get('kind') == 'pipeline' and project.get('phase') == 'implementation'))
                        ) else None
                        control_bridge.set_turn(input_access, delivery_review=delivery_review, project_root=layout_root)
                    transport.frame_count = 0
                    transport.output_bytes = 0
                    provider.items.clear()
                    provider.turn = None
                    if not provider_opened:
                        saved_session = session['provider_session'] if acp or store.has_dispatched(session['id']) else None
                        provider.thread = saved_session
                        if acp and guided:
                            provider.mcp_servers = [dict(name='development-protocol', command=sys.executable,
                                args=['-I', '-B', str(Path(__file__).with_name('mcp_bridge.py')), '--connect', str(control_bridge.path),
                                      control_bridge.token, tid, turn['epoch']], env=[])]
                        store.event(tid, 'provider.bound', dict(effective_bound or {}, warm_reused=True, standby_prewarmed=True))
                        store.update(tid, delivery='sent')
                        send_parent('notify')
                        open_key = provider.open(prompt) if acp else provider.open(prompt, tools=(INPUT_TOOL,) if guided else ())
                        thread = provider.opened(wait(open_key))
                        store.bind_provider(session['id'], thread)
                        store.update(tid, delivery='prepared' if acp else 'acknowledged')
                        if acp and session['provider_session']:
                            store.event(tid, 'history.gap', dict(reason='ACP resume does not replay provider transcript; saved application events remain authoritative'))
                        provider_opened = True
                    else:
                        if acp:
                            provider.acknowledged = False
                        store.event(tid, 'provider.bound', dict(effective_bound or {}, warm_reused=True))
                        store.update(tid, delivery='prepared' if acp else 'acknowledged')
                        send_parent('notify')
                else:
                    config = deployment.providers[session['provider']]
                    adapter = adapter_for(session['provider'], turn_op)
                    acp = getattr(adapter, 'acp', False)
                    if cell_cli and control_bridge is None:
                        from demiflow.workbench.notebooks.cell_commands import CellCommands
                        control_bridge = CellCommands(store, turn, deadline)
                    elif acp and guided:
                        layout_root = Path(project['root']) if (
                            not compact and not notebook
                            and (project.get('writes') or (project.get('kind') == 'pipeline' and project.get('phase') == 'implementation'))
                        ) else None
                        control_bridge = BridgeServer(input_access, delivery_review=delivery_review, project_root=layout_root)
                    provider_home = home / 'provider' if deployment.mode == 'restricted' else home
                    provider_home.mkdir(mode=0o700, exist_ok=True)
                    if broker_home is not None:
                        broker_home.mkdir(mode=0o700, parents=True, exist_ok=True)
                    proj_py = effective_project_python(project)
                    if guided:
                        from demiflow.workbench.conversations.sandbox_shims import SandboxShims
                        shims = SandboxShims(project, execution_mode=deployment.mode,
                                             project_python=proj_py, input_access=input_access is not None)
                    warm_sandbox_secs = ((0 if limits.turn_seconds == 0
                                          else min(86400, max(limits.turn_seconds, WARM_SANDBOX_SECONDS)))
                                         if (parent_sock is not None and not compact) else None)
                    confined = (sandbox_spec(deployment, project, session, turn, manifest, config, provider_home,
                                             seconds=warm_sandbox_secs,
                                             extra_read_paths=shims.read_paths if shims is not None else (),
                                             extra_runtime_paths=shims.runtime_paths if shims is not None else ())
                                if deployment.mode == 'restricted' else None)
                    warm_operation = turn_op
                    provider_cwd = str(provider_home) if notebook else project['root']
                    if confined is not None:
                        sandbox_deadline = (time.time() + confined.seconds) if confined.seconds > 0 else 0.0
                        if cell_cli:
                            from dataclasses import replace
                            confined = replace(confined, runtime_paths=(*confined.runtime_paths, str(control_bridge.bin)))
                        from demiflow.workbench.conversations.sandbox import specification
                        specification(confined, [sys.executable], {}, provider_cwd)
                        if not getattr(deployment, 'direct_model', False):
                            from demiflow.workbench.conversations.model_broker import start_model_broker
                            model_broker = start_model_broker(deployment, turn, session, broker_home, defer_handshake=True)
                            config = model_broker.child_config
                        if cell_cli:
                            config = {**config, '_cell_bin': str(control_bridge.bin)}
                    if notebook:
                        proj_py = None
                    if proj_py:
                        config = {**config, '_project_python': proj_py}
                    if shims is not None:
                        config = {**config, '_shim_bin': str(shims.bin)}
                    process = launch_provider(mode=deployment.mode, adapter=adapter, config=config,
                        home=provider_home, root=provider_cwd, lease_fd=lease_fd, sandbox_spec=confined,
                        control_socket=control_bridge.path if control_bridge is not None else None,
                        model_socket=model_broker.socket_path if model_broker is not None else None)
                    transport = Transport(process, limits, acp=acp)
                    saved_session = session['provider_session'] if acp or store.has_dispatched(session['id']) else None
                    provider = adapter(transport, config, dict(project, root=provider_cwd) if notebook else project, saved_session)
                    if acp and guided:
                        provider.mcp_servers = [dict(name='development-protocol', command=sys.executable,
                            args=['-I', '-B', str(Path(__file__).with_name('mcp_bridge.py')), '--connect', str(control_bridge.path),
                                  control_bridge.token, tid, turn['epoch']], env=[])]
                    handshake = wait(provider.initialize(), timeout=40)
                    configuration_key = provider.initialized(handshake)
                    effective = provider.verify_config(wait(configuration_key) if configuration_key else {})
                    effective['execution_assurance'] = getattr(process, 'execution_assurance', 'host_permissions')
                    effective_bound = dict(effective)
                    store.event(tid, 'provider.bound', effective)
                    store.update(tid, delivery='not_applicable' if compact else 'sent')
                    send_parent('notify')
                    open_key = provider.open(prompt) if acp else provider.open(prompt, tools=(INPUT_TOOL,) if guided else ())
                    thread = provider.opened(wait(open_key))
                    store.bind_provider(session['id'], thread)
                    store.update(tid, delivery='not_applicable' if compact else 'prepared' if acp else 'acknowledged')
                    if not compact and acp and session['provider_session']:
                        store.event(tid, 'history.gap', dict(reason='ACP resume does not replay provider transcript; saved application events remain authoritative'))
                    if model_broker is not None:
                        model_broker.wait_ready()
                    provider_opened = True

                # Compare again after context preparation and handshake; no baseline reset.
                if watched and capture(project['root'], watched, exclude=excluded, source_dir=scratch / 'sources') != baseline:
                    reject('Watched sources changed during preparation; no model turn sent', 'conflict')
                tick(force=True)
                if requested: raise InterruptedError(reason)
                # Persist unknown BEFORE writing to the provider pipe: a crash cannot
                # turn an uncertain paid request into a supposedly safe automatic retry.
                store.update(tid, dispatch='unknown')
                start_key = provider.start(turn['text'], turn['submission'])
                if acp:
                    native_turn = provider.turn
                    store.update(tid, dispatch='sent', delivery='not_applicable' if compact else 'sent', provider_turn=native_turn, state='running')
                    provider_turn_bound = bool(native_turn)
                else:
                    started = wait(start_key)
                    native_turn = provider.started(started)
                    store.update(tid, dispatch='acknowledged', provider_turn=native_turn, state='running')
                    provider_turn_bound = bool(native_turn)
                send_parent('notify')
                while not completed:
                    current = tick()
                    transport.poll(0.01 if pending_events else 0.05)
                    while transport.events:
                        event = transport.pop()
                        notifications = provider.event(event)
                        if provider.turn and not provider_turn_bound:
                            flush_events()
                            store.update(tid, provider_turn=provider.turn)
                            provider_turn_bound = True
                        for kind, data in notifications:
                            if kind == 'activity':
                                if not provider.acknowledged:
                                    provider.acknowledged = True
                                    flush_events()
                                    store.update(tid, delivery='not_applicable' if compact else 'acknowledged', dispatch='acknowledged')
                                    send_parent('notify')
                            elif kind == 'steer.result':
                                if steering and data['submission'] == steering:
                                    flush_events()
                                    store.finish_steer(tid, steering, data['status'])
                                    send_parent('notify')
                                    steering = None
                            elif kind == 'done':
                                flush_events()
                                final = data['status']; completed = True
                                if data.get('error'): reason = str(data['error'])[:2048]
                            elif kind == 'request':
                                flush_events()
                                tick(force=True)
                                if requested: continue
                                if automatic_approval(project, data, assurance=getattr(process, 'execution_assurance', 'host_permissions')):
                                    reply = dict(id=data['rpc_id'], result=provider.answer(data, 'allow_once'))
                                    if store.dispatch_provider_reply(tid, turn['epoch'], lambda: transport.send(reply)):
                                        store.event(tid, 'approval.automatic', dict(method=data['method'], authorization='existing_deployment_scope'))
                                        send_parent('notify')
                                    continue
                                key = store.request(tid, data); outstanding[key] = data
                                store.update(tid, state='waiting_input' if data['method'].endswith('requestUserInput') else 'waiting_approval')
                                send_parent('notify')
                            elif kind == 'bridge':
                                flush_events()
                                tick(force=True)
                                if requested: continue
                                try:
                                    if input_access is None or data['tool'] != INPUT_TOOL['name']:
                                        reject('Input tool is unavailable for this turn')
                                    answer = input_access.invoke(data['arguments'])
                                    result = dict(success=True, contentItems=[dict(type='inputText', text=wire(answer, 65536).decode())])
                                except Exception as exc:
                                    result = dict(success=False, contentItems=[dict(type='inputText', text=str(exc)[:2048])])
                                transport.send(dict(id=data['rpc_id'], result=result))
                            else: emit(kind, data)
                    if pending_events and time.monotonic() - last_flush >= 0.016:
                        flush_events()
                    if outstanding:
                        for pending in store.pending(tid):
                            if cell_cli and json.loads(pending['request']).get('method') == 'notebook/command':
                                continue
                            tick(force=True)
                            if requested: break
                            request = outstanding.get(pending['id'])
                            if request is None: reject('Pending request belongs to a lost provider connection')
                            if pending['expires'] < time.time(): raise TimeoutError('Input request expired without approval')
                            if pending['answer'] is not None:
                                answer = provider.answer(request, json.loads(pending['answer']))
                                if not store.dispatch_provider_reply(tid, turn['epoch'], lambda: transport.send(dict(id=request['rpc_id'], result=answer))):
                                    break
                                store.resolved(pending['id']); del outstanding[pending['id']]
                                store.update(tid, state='running' if not outstanding else 'waiting_input')
                                send_parent('notify')
                    if acp and start_key in transport.responses:
                        flush_events()
                        response = transport.take(start_key)
                        outcome = provider.completion(response)
                        final, completed = outcome['status'], True
                        store.update(tid, delivery='not_applicable' if compact else 'acknowledged', dispatch='acknowledged')
                    if steer_rpc and steer_rpc in transport.responses:
                        try:
                            reply = transport.take(steer_rpc)
                            accepted = reply.get('turnId') == provider.turn
                        except ValueError:
                            accepted = False
                        flush_events()
                        store.finish_steer(tid, steering, 'accepted' if accepted else 'missed')
                        send_parent('notify')
                        steering = steer_rpc = None
                    if (not steering and not completed and not requested and current.get('has_steer')
                            and (not acp or provider.acknowledged or provider.metrics_sequence >= 0)):
                        pending_steer = store.claim_steer(tid, turn['epoch'])
                        if pending_steer:
                            steering, message = pending_steer
                            steer_rpc = provider.steer(message, steering)
                    if transport.closed and not completed:
                        rc = process.poll() if process is not None else None
                        if rc is None and process is not None:
                            try:
                                rc = process.wait(timeout=0.5)
                            except Exception:
                                rc = process.poll()
                        stderr_tail = bytes(getattr(transport, 'stderr', b'')).decode('utf-8', 'replace').strip()[-400:]
                        detail = 'Provider connection lost'
                        if rc is not None:
                            detail += f' (exit={rc})'
                        if stderr_tail:
                            detail += ': ' + ' '.join(stderr_tail.splitlines()[-3:])[:320]
                        raise ConnectionError(detail)
                    # Interrupt has its own RPC result; it is not proof of child cleanup.
                    for key in list(transport.responses):
                        transport.take(key)
                flush_events()
                if requested: final = 'cancelled'
            except BaseException as exc:
                try: flush_events()
                except Exception: pass
                reason = reason or (type(exc).__name__ + ': ' + str(exc))[:2048]
                final = 'cancelled' if requested else 'interrupted' if isinstance(exc, (ConnectionError, TimeoutError)) else 'failed'
                current = store.turn(tid)
                if current['delivery'] == 'sent': store.update(tid, delivery='unknown')
                if current['dispatch'] == 'sent': store.update(tid, dispatch='unknown')

            compaction_metadata = getattr(provider, 'compaction_result', None)
            assurance = (getattr(process, 'execution_assurance', 'host_permissions')
                         if process is not None else (effective_bound or {}).get('execution_assurance', 'not_started'))
            host_layout_paths = []
            if cell_cli and control_bridge is not None:
                control_bridge.park_idle()
            elif guided and control_bridge is not None:
                host_layout_paths = control_bridge.take_layout_paths()
                control_bridge.set_turn(None)
            review_requested = delivery_review is not None and delivery_review.selection is not None
            keep_warm = (
                parent_sock is not None
                and not compact and not review_requested
                and final == 'completed' and completed and not requested
                and process is not None and process.poll() is None
                and transport is not None and not transport.closed
                and (deployment.mode != 'restricted' or sandbox_deadline == 0.0 or sandbox_deadline - time.time() > limits.turn_seconds + 60)
            )
            drained = True
            author_process = process
            if not keep_warm:
                drained, cleanup_err = _teardown_provider(process, transport, provider, model_broker, control_bridge, shims)
                if cleanup_err and not reason:
                    reason = cleanup_err
                process = transport = provider = model_broker = control_bridge = shims = None
            protected_violations = []
            try:
                protected_violations = enforce_protected_files(project['root'], protected_snapshot,
                                                                authorized=host_layout_paths)
                if protected_violations:
                    store.event(tid, 'protected.violation', dict(paths=protected_violations, restored=True))
            except BaseException:
                pass
            try:
                changes = []; after = {}; revision = None; watched_after = list(watched); new_root_entries = []
                saved = store.turn(tid)
                if saved['baseline'] and watched:
                    # New top-level entries created this turn (e.g. tables/, scripts/, debug.py) are
                    # outside the static watch list; observe them so the Forge hook still runs.
                    if root_names_before is not None:
                        new_root_entries = sorted(_root_entry_names(project['root']) - root_names_before
                                                  - set(watched) - set(excluded))
                    before = json.loads(saved['baseline'])
                    try:
                        after = capture(project['root'], [*watched, *new_root_entries], exclude=excluded, source_dir=scratch / 'sources')
                        watched_after = [*watched, *new_root_entries]
                    except ForgeError:
                        # An oversized new tree is still reported to DF-02 below without hashing it.
                        after = capture(project['root'], watched, exclude=excluded, source_dir=scratch / 'sources')
                    if project.get('kind') == 'pipeline' and project.get('phase') == 'implementation' and not compact and not notebook:
                        authorized_writes = list(dict.fromkeys([*project['writes'], *watched_after, *host_layout_paths]))
                    else:
                        authorized_writes = list(dict.fromkeys([*project['writes'], *host_layout_paths]))
                    changes = list(differences(before, after, authorized_writes, root=project['root']))
                    if changes:
                        source_hash = lambda values: digest({name: (fact or {}).get('sha256') for name, fact in values.items()})
                        revision = dict(before=source_hash(before), after=source_hash(after))
                    for change in changes:
                        store.event(tid, 'change.observed', dict(files=[change], authorship='unknown_shared_workspace', evidence='stale'))
                    if any(not change['within_scope'] for change in changes):
                        final, reason = 'failed', 'Watched changes outside authorized scope; all contents preserved'
                        if keep_warm:
                            keep_warm = False
                            drained, _ = _teardown_provider(process, transport, provider, model_broker, control_bridge, shims)
                            process = transport = provider = model_broker = control_bridge = shims = None
            except BaseException as exc:
                final, reason = 'failed', ('Change observation incomplete: ' + type(exc).__name__ + ': ' + str(exc))[:2048]
                if keep_warm:
                    keep_warm = False
                    drained, _ = _teardown_provider(process, transport, provider, model_broker, control_bridge, shims)
                    process = transport = provider = model_broker = control_bridge = shims = None

            try:
                result = dict(reason=reason, peaks=peaks, changed_files=len(changes), evidence='stale' if changes else 'missing',
                              cleanup='complete' if drained else 'unknown', provider_completed=completed)
                if protected_violations:
                    result['protected_violations'] = protected_violations
                result['execution_assurance'] = assurance
                if revision: result['revision'] = revision
                if compact and completed and final == 'completed':
                    metadata = compaction_metadata
                    if metadata is None:
                        final, result['reason'] = 'failed', 'Provider did not confirm a context compaction item'
                    else:
                        result['compaction'] = dict(metadata, history_through_sequence=history_boundary)
                        store.event(tid, 'compaction.completed', result['compaction'])
                report = receipt = None
                selection = delivery_review.selection if delivery_review is not None else None
                if selection is None and forge_opts.get('selection') is not None:
                    selection = forge_opts['selection']
                if selection is None and forge_opts.get('purpose') == 'review':
                    selection = {}
                want_review = guided and needs_review(changes=changes, auto_review=auto_review, selection=selection)
                if want_review and keep_warm:
                    keep_warm = False
                    drained, cleanup_err = _teardown_provider(process, transport, provider, model_broker, control_bridge, shims)
                    if cleanup_err and not reason:
                        reason = cleanup_err
                    process = transport = provider = model_broker = control_bridge = shims = None

                def check_forge_cancel():
                    tick(force=True)
                    if requested or store.turn(tid)['stop_at']:
                        raise InterruptedError(reason or '工程检查已取消。')

                def source_current():
                    return (not coding or sdk_identity(coding['catalog']) == coding['sdk_revision']) and (
                        not watched or capture(project['root'], watched_after, exclude=excluded,
                            source_dir=scratch / 'sources') == after)

                if guided and not compact and not notebook:
                    from dataclasses import replace
                    from functools import partial
                    from demiflow.workbench.conversations.pipeline_review import run_deployment_reviewer
                    review_author_spec = (replace(confined, runtime_paths=tuple(p for p in confined.runtime_paths
                                                if Path(p).exists())) if confined is not None else None)
                    reviewer = partial(run_deployment_reviewer, deployment=deployment, turn=turn,
                        session=session, scratch=scratch, broker_home=broker_home, author_spec=review_author_spec,
                        lease_fd=lease_fd, deadline=min(deadline, time.time() + float(
                            forge_opts.get('budget', {}).get('seconds', limits.review_seconds))), check_cancel=check_forge_cancel)
                    def started_review():
                        if delivery_review is not None and delivery_review.selection is None:
                            delivery_review.request({})
                        store.event(tid, 'review.started', {'request_id': tid})
                        send_parent('notify')
                    try:
                        current_session = store.session(session['id'])
                        receipt = assess_turn(project['root'], submission=tid,
                            author=dict(provider=session['provider'], session=current_session['provider_session'],
                                completed=final == 'completed' and completed, cleanup='complete' if drained else 'unknown',
                                acknowledged=saved['dispatch'] == saved['delivery'] == 'acknowledged'),
                            goal=lambda: store.review_inputs(session['id'], include_paths=not selection or auto_review),
                            changes=changes, selection=selection, auto_review=auto_review, round=rework_depth,
                            budget=forge_opts.get('budget') or dict(calls=34, seconds=float(limits.review_seconds)), reference=coding, exclude=excluded, reviewer=reviewer,
                            check_cancel=check_forge_cancel, source_current=source_current,
                            protected_violations=protected_violations, project_python=effective_project_python(project),
                            on_review=started_review, inspect=coding is not None)
                        if receipt['checks'] is not None:
                            result['pipeline_checks'] = receipt['checks']
                        report = receipt['review']
                        # The host's candidate fingerprint is rechecked again before transport.
                        if receipt.get('feedback'):
                            receipt['feedback']['candidate'] = dict(watch=watched_after, exclude=list(excluded),
                                digest=candidate_identity(after))
                    except InterruptedError:
                        final = 'cancelled'
                        report = dict(status='incomplete', diagnostics=[dict(detail='评审已停止。')])
                    except (OSError, ValueError, TimeoutError) as exc:
                        report = dict(status='incomplete', diagnostics=[dict(detail=str(exc)[:2048])])
                    finally:
                        if want_review and descendants(os.getpid()):
                            signal_owned(os.getpid(), signal.SIGKILL)
                            drained = drain_owned(author_process, timeout_s=5)
                            if not drained:
                                report = dict(status='incomplete', diagnostics=[dict(detail='评审进程尚未清理。')])
                                if receipt:
                                    receipt['feedback'] = None
                                    receipt['status'] = 'incomplete'
                result['cleanup'] = 'complete' if drained else 'unknown'
                store.finish_provider(tid, state=final if drained else 'stopping', cleanup=result['cleanup'],
                                      result=result, review=report, forge=receipt)
                send_parent('notify')
            except BaseException:
                try: store.update(tid, state='stopping', cleanup='unknown')
                except BaseException: pass
                if keep_warm:
                    keep_warm = False
                    _teardown_provider(process, transport, provider, model_broker, control_bridge, shims)
                    process = transport = provider = model_broker = control_bridge = shims = None

            if lease_fd is not None:
                os.close(lease_fd)
                lease_fd = None

            if not keep_warm:
                break

            warm_ready = True
            if model_broker is not None:
                model_broker.park_idle()
            if not send_parent('idle', reliable=True, tid=tid, sandbox_deadline=sandbox_deadline):
                break
            next_work = _wait_next_turn(
                parent_sock,
                process=process,
                transport=transport,
                stopping_signal=stopping_signal,
                idle_deadline=time.time() + WARM_IDLE_SECONDS,
                sandbox_deadline=sandbox_deadline if deployment.mode == 'restricted' else 0.0,
                min_remaining=limits.turn_seconds + 60,
                buf=parent_buf,
                fds=parent_fds,
            )
            if next_work is None:
                break
            next_tid, next_lease = next_work
            if not send_parent('accepted', reliable=True, tid=next_tid):
                try: os.close(next_lease)
                except OSError: pass
                break
            tid, lease_fd = next_tid, next_lease
    finally:
        for leftover in parent_fds:
            try: os.close(leftover)
            except OSError: pass
        if process is not None or transport is not None or model_broker is not None or control_bridge is not None or shims is not None:
            _teardown_provider(process, transport, provider, model_broker, control_bridge, shims)
        if coding_fd is not None:
            try: os.close(coding_fd)
            except OSError: pass
        if lease_fd is not None:
            try: os.close(lease_fd)
            except OSError: pass
        if parent_sock is not None:
            try: parent_sock.close()
            except OSError: pass
        else:
            try: os.close(parent_fd)
            except OSError: pass


if __name__ == '__main__':
    if len(sys.argv) >= 5 and sys.argv[1] == '--standby':
        run(sys.argv[2], None, None, int(sys.argv[4]), int(sys.argv[5]) if len(sys.argv) > 5 else None, standby=sys.argv[3])
    else:
        run(sys.argv[1], sys.argv[2], *(int(fd) for fd in sys.argv[3:]))
