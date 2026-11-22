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
from demiflow.workbench.conversations.policy import Deployment
from demiflow.workbench.conversations.store import Store
from demiflow.workbench.conversations.transport import Transport
from demiflow.workbench.conversations.provider import adapter_for
from demiflow.workbench.conversations.context import assemble
from demiflow.workbench.conversations.context import BridgeServer, INPUT_TOOL
from demiflow.workbench.conversations.changes import capture
from demiflow.workbench.conversations.changes import differences
from demiflow.workbench.conversations.execution import launch_provider, runtime_paths

WARM_IDLE_SECONDS = 600
WARM_SANDBOX_SECONDS = 2400


def sandbox_spec(deployment, project, session, turn, manifest, config, private_home, *, seconds=None):
    """Assemble only controller-declared scope; no provider-supplied path grants."""
    from demiflow.workbench.conversations.sandbox import SandboxSpec
    root = Path(project['root'])
    sources = [Path(source['canonical_path']) for source in manifest.get('sources', [])
               if source.get('origin') == 'authoritative_file']
    read = [root, *[path for path in sources if path.is_file()]]
    openssl = Path('/etc/ssl/openssl.cnf')
    if openssl.is_file(): read.append(openssl)
    readonly = turn.get('operation') in ('compact', 'notebook_cell', 'notebook_cell_cli')
    writes = [] if readonly else [root / name for name in project['writes']]
    protected = [root / name for name in ('.git', '.forge', 'AGENTS.md', 'workspace.json', 'PIPELINE_SPEC.md')]
    protected.extend(path for path in sources if path.name in {'AGENTS.md', 'PIPELINE_SPEC.md'})
    protected.extend(root / name for name in project.get('protected', []))
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
        if provider.get('native_credentials'): denied.append(Path(provider['native_credentials']))
    unique = lambda paths: tuple(sorted({str(path.absolute()) for path in paths}))
    duration = seconds if seconds is not None else max(.1, turn['created'] + deployment.limits.turn_seconds - time.time())
    return SandboxSpec(read_paths=unique(read), write_paths=unique(writes), runtime_paths=runtime_paths(config),
        protected_paths=unique(protected), denied_paths=unique(denied), private_home=str(private_home),
        role='readonly' if readonly else 'developer',
        seconds=min(3600, max(.1, duration)), rss_bytes=deployment.limits.rss_bytes,
        processes=deployment.limits.processes, file_bytes=min(64*1024**2, deployment.limits.provider_bytes),
        private_bytes=min(deployment.limits.provider_bytes, deployment.limits.private_total_bytes))


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


def _teardown_provider(process, transport, provider, model_broker, control_bridge):
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
    return drained, reason


def _wait_next_turn(parent_sock, *, process, transport, stopping_signal, idle_deadline, sandbox_deadline, min_remaining):
    """Park a warm worker until the next turn arrives or idle/shutdown triggers."""
    buf = bytearray()
    fds = []
    try:
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
            ready, _, _ = select.select([parent_sock], [], [], 0.1)
            if not ready:
                continue
            try:
                data, ancdata, _, _ = parent_sock.recvmsg(4096, socket.CMSG_SPACE(array.array('i').itemsize * 4))
            except BlockingIOError:
                continue
            except OSError:
                return None
            for cmsg_level, cmsg_type, cmsg_data in ancdata:
                if cmsg_level == socket.SOL_SOCKET and cmsg_type == socket.SCM_RIGHTS:
                    received = array.array('i')
                    received.frombytes(cmsg_data[:len(cmsg_data) - (len(cmsg_data) % received.itemsize)])
                    fds.extend(received)
            if not data:
                return None
            buf.extend(data)
            if len(buf) > 65536:
                return None
            while b'\n' in buf:
                line, rest = bytes(buf).split(b'\n', 1)
                buf = bytearray(rest)
                if not line.strip():
                    continue
                msg = json.loads(line)
                if msg.get('cmd') == 'turn' and isinstance(msg.get('tid'), str) and fds:
                    next_lease = fds.pop(0)
                    return msg['tid'], next_lease
                if msg.get('cmd') == 'shutdown':
                    return None
    finally:
        for leftover in fds:
            try: os.close(leftover)
            except OSError: pass


def run(config_path, tid, lease_fd, parent_fd, coding_fd=None):
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

    process = transport = provider = model_broker = control_bridge = coding = None
    effective_bound = None
    sandbox_deadline = 0.0
    warm_ready = False
    acp = False

    try:
        while True:
            turn = store.turn(tid); session = store.session(turn['session'])
            compact = turn.get('operation') == 'compact'
            cell_cli = turn.get('operation') == 'notebook_cell_cli'
            notebook = cell_cli or turn.get('operation') == 'notebook_cell'
            with store.connect() as db:
                history_boundary = db.execute('SELECT min(sequence)-1 FROM events WHERE turn=?', (tid,)).fetchone()[0] or 0
            project = deployment.project(session['workspace'], json.loads(session['root'])['path'])
            guided = not (compact or notebook) and project['kind'] == 'pipeline'
            watched = [] if notebook else project['watch']
            excluded = project.get('protected', ()) if guided else ()
            home = deployment.state / session['id']; scratch = home / turn['id']; scratch.mkdir(mode=0o700, exist_ok=True)
            final, reason, completed = 'failed', None, False
            requested = False
            deadline = turn['created'] + limits.turn_seconds
            stop_started = None
            last_sample, peaks = 0, {'rss_bytes': 0, 'private_bytes': 0, 'processes': 0}
            last_turn_check, cached_turn = 0.0, turn
            provider_turn_bound = bool(turn.get('provider_turn'))
            pending_events, pending_bytes, last_flush = [], 0, time.monotonic()
            outstanding = {}
            input_access = None

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
                baseline = capture(project['root'], watched, exclude=excluded)
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
                    manifest, prompt = assemble(project, turn['text'], limits,
                        execution_mode=deployment.mode, input_access=input_access is not None, coding=coding)
                    store.update(tid, context=wire(manifest).decode(), delivery='prepared')
                    store.event(tid, 'context.bound', {'digest': manifest['digest'], 'kind': manifest['kind'],
                        'sources': len(manifest['sources']), 'bytes': manifest['bytes'], 'delivery': 'prepared'})
                    send_parent('notify')
                tick()
                if requested: raise InterruptedError(reason)

                if warm_ready:
                    if model_broker is not None:
                        model_broker.begin_turn(turn, limits)
                    if control_bridge is not None and input_access is not None:
                        control_bridge.set_turn(input_access)
                    transport.frame_count = 0
                    transport.output_bytes = 0
                    provider.items.clear()
                    provider.turn = None
                    if acp:
                        provider.acknowledged = False
                        provider.prompt = prompt
                    store.event(tid, 'provider.bound', dict(effective_bound or {}, warm_reused=True))
                    store.update(tid, delivery='prepared' if acp else 'acknowledged')
                    send_parent('notify')
                else:
                    config = deployment.providers[session['provider']]
                    adapter = adapter_for(session['provider'], turn.get('operation', 'message'))
                    acp = getattr(adapter, 'acp', False)
                    if acp and guided:
                        control_bridge = BridgeServer(input_access)
                    provider_home = home / 'provider' if deployment.mode == 'restricted' else home
                    provider_home.mkdir(mode=0o700, exist_ok=True)
                    if deployment.mode == 'restricted':
                        broker_home = home / 'control' / (tid + '-' + turn['epoch']) / 'model'
                        broker_home.mkdir(mode=0o700, exist_ok=True)
                    warm_sandbox_secs = (min(3600, max(limits.turn_seconds, WARM_SANDBOX_SECONDS))
                                         if (parent_sock is not None and not compact and not notebook) else None)
                    confined = (sandbox_spec(deployment, project, session, turn, manifest, config, provider_home, seconds=warm_sandbox_secs)
                                if deployment.mode == 'restricted' else None)
                    if confined is not None:
                        sandbox_deadline = time.time() + confined.seconds
                        if cell_cli:
                            from dataclasses import replace
                            confined = replace(confined, runtime_paths=(*confined.runtime_paths, str(control_bridge.bin)))
                        from demiflow.workbench.conversations.sandbox import specification
                        specification(confined, [sys.executable], {}, project['root'])
                        from demiflow.workbench.conversations.model_broker import start_model_broker
                        model_broker = start_model_broker(deployment, turn, session, broker_home, defer_handshake=True)
                        config = model_broker.child_config
                        if cell_cli:
                            config = {**config, '_cell_bin': str(control_bridge.bin)}
                    process = launch_provider(mode=deployment.mode, adapter=adapter, config=config,
                        home=provider_home, root=project['root'], lease_fd=lease_fd, sandbox_spec=confined,
                        control_socket=control_bridge.path if control_bridge is not None else None,
                        model_socket=model_broker.socket_path if model_broker is not None else None)
                    transport = Transport(process, limits, acp=acp)
                    saved_session = session['provider_session'] if acp or store.has_dispatched(session['id']) else None
                    provider = adapter(transport, config, project, saved_session)
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

                # Compare again after context preparation and handshake; no baseline reset.
                if watched and capture(project['root'], watched, exclude=excluded) != baseline:
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
                    tick()
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
                    if transport.closed and not completed: raise ConnectionError('Provider connection lost')
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
            keep_warm = (
                parent_sock is not None
                and not compact and not notebook
                and final == 'completed' and completed and not requested
                and process is not None and process.poll() is None
                and transport is not None and not transport.closed
                and (deployment.mode != 'restricted' or sandbox_deadline - time.time() > limits.turn_seconds + 60)
            )
            drained = True
            if not keep_warm:
                drained, cleanup_err = _teardown_provider(process, transport, provider, model_broker, control_bridge)
                if cleanup_err and not reason:
                    reason = cleanup_err
                process = transport = provider = model_broker = control_bridge = None
            try:
                changes = []; revision = None
                saved = store.turn(tid)
                if saved['baseline'] and watched:
                    before = json.loads(saved['baseline']); after = capture(project['root'], watched, exclude=excluded)
                    changes = list(differences(before, after, project['writes']))
                    if changes:
                        source_hash = lambda values: digest({name: (fact or {}).get('sha256') for name, fact in values.items()})
                        revision = dict(before=source_hash(before), after=source_hash(after))
                    for change in changes:
                        store.event(tid, 'change.observed', dict(files=[change], authorship='unknown_shared_workspace', evidence='stale'))
                    if any(not change['within_scope'] for change in changes):
                        final, reason = 'failed', 'Watched changes outside authorized scope; all contents preserved'
                        if keep_warm:
                            keep_warm = False
                            drained, _ = _teardown_provider(process, transport, provider, model_broker, control_bridge)
                            process = transport = provider = model_broker = control_bridge = None
            except BaseException as exc:
                final, reason = 'failed', ('Change observation incomplete: ' + type(exc).__name__ + ': ' + str(exc))[:2048]
                if keep_warm:
                    keep_warm = False
                    drained, _ = _teardown_provider(process, transport, provider, model_broker, control_bridge)
                    process = transport = provider = model_broker = control_bridge = None

            try:
                result = dict(reason=reason, peaks=peaks, changed_files=len(changes), evidence='stale' if changes else 'missing',
                              cleanup='complete' if drained else 'unknown', provider_completed=completed)
                result['execution_assurance'] = assurance
                if (coding is not None and changes and drained and completed and final == 'completed'
                        and not saved['stop_at'] and saved['dispatch'] == saved['delivery'] == 'acknowledged'):
                    from demiflow.forge.pipeline_check import check_project, result as check_result
                    from demiflow.forge.project_rules import diagnostic
                    try:
                        report = check_project(project['root'], paths=[change['path'] for change in changes],
                                               reference=coding, exclude=excluded)
                        if (sdk_identity(coding['catalog']) != coding['sdk_revision']
                                or capture(project['root'], watched, exclude=excluded) != after):
                            reject('SDK or project source changed before checks could be reported', 'conflict')
                        result['pipeline_checks'] = {key: report[key] for key in (
                            'status', 'native_flow', 'project_layout', 'catalog_id', 'rules_id', 'source_digest',
                            'source_count', 'check_digest')}
                    except (ValueError, OSError, KeyError, RecursionError) as exc:
                        incomplete = check_result([diagnostic('DF-01', '', str(exc), status='incomplete')], ())
                        result['pipeline_checks'] = dict(native_flow=incomplete, project_layout=incomplete)
                if revision: result['revision'] = revision
                if compact and completed and final == 'completed':
                    metadata = compaction_metadata
                    if metadata is None:
                        final, result['reason'] = 'failed', 'Provider did not confirm a context compaction item'
                    else:
                        result['compaction'] = dict(metadata, history_through_sequence=history_boundary)
                        store.event(tid, 'compaction.completed', result['compaction'])
                store.finish_provider(tid, state=final if drained else 'stopping', cleanup='complete' if drained else 'unknown', result=result)
                send_parent('notify')
            except BaseException:
                try: store.update(tid, state='stopping', cleanup='unknown')
                except BaseException: pass
                if keep_warm:
                    keep_warm = False
                    _teardown_provider(process, transport, provider, model_broker, control_bridge)
                    process = transport = provider = model_broker = control_bridge = None

            if lease_fd is not None:
                os.close(lease_fd)
                lease_fd = None

            if not keep_warm:
                break

            warm_ready = True
            if model_broker is not None:
                model_broker.park_idle()
            if control_bridge is not None:
                control_bridge.set_turn(None)
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
        if process is not None or transport is not None or model_broker is not None or control_bridge is not None:
            _teardown_provider(process, transport, provider, model_broker, control_bridge)
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
    run(sys.argv[1], sys.argv[2], *(int(fd) for fd in sys.argv[3:]))
