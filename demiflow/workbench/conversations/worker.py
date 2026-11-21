"""One dedicated Linux subreaper per active turn, never a Dataset executor."""
from __future__ import annotations

import ctypes
import fcntl
import json
import os
from pathlib import Path
import signal
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


def sandbox_spec(deployment, project, session, turn, manifest, config, private_home):
    """Assemble only controller-declared scope; no provider-supplied path grants."""
    from demiflow.workbench.conversations.sandbox import SandboxSpec
    root = Path(project['root'])
    sources = [Path(source['canonical_path']) for source in manifest.get('sources', [])
               if source.get('origin') == 'authoritative_file']
    read = [root, *[path for path in sources if path.is_file()]]
    openssl = Path('/etc/ssl/openssl.cnf')
    if openssl.is_file(): read.append(openssl)
    readonly = turn.get('operation') in ('compact','notebook_cell','notebook_cell_cli')
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
    return SandboxSpec(read_paths=unique(read), write_paths=unique(writes), runtime_paths=runtime_paths(config),
        protected_paths=unique(protected), denied_paths=unique(denied), private_home=str(private_home),
        role='readonly' if readonly else 'developer',
        seconds=max(.1, turn['created'] + deployment.limits.turn_seconds-time.time()), rss_bytes=deployment.limits.rss_bytes,
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


def run(config_path, tid, lease_fd, parent_fd, coding_fd=None):
    deployment = Deployment.load(config_path)
    store = Store(deployment.state, deployment.limits)
    turn = store.turn(tid); session = store.session(turn['session'])
    compact = turn.get('operation') == 'compact'
    cell_cli = turn.get('operation') == 'notebook_cell_cli'
    notebook = cell_cli or turn.get('operation') == 'notebook_cell'
    with store.connect() as db:
        history_boundary = db.execute('SELECT min(sequence)-1 FROM events WHERE turn=?',(tid,)).fetchone()[0] or 0
    project = deployment.project(session['workspace'], json.loads(session['root'])['path'])
    guided = not (compact or notebook) and project['kind'] == 'pipeline'
    watched = [] if notebook else project['watch']
    excluded = project.get('protected', ()) if guided else ()
    coding = None
    home = deployment.state / session['id']; scratch = home / turn['id']; scratch.mkdir(mode=0o700, exist_ok=True)
    limits = deployment.limits
    process = transport = provider = None
    final, reason, completed, draining = 'failed', None, False, False
    requested = False
    deadline = turn['created'] + limits.turn_seconds
    stop_started = None
    last_sample, peaks = 0, {'rss_bytes': 0, 'private_bytes': 0, 'processes': 0}
    outstanding = {}
    input_access = None
    control_bridge = None
    model_broker = None
    os.set_blocking(parent_fd, False)
    stopping_signal = [False]
    for sig in (signal.SIGTERM, signal.SIGINT):
        signal.signal(sig, lambda *_: stopping_signal.__setitem__(0, True))
    if ctypes.CDLL(None, use_errno=True).prctl(36, 1, 0, 0, 0):
        reject('Cannot adopt owned provider descendants')
    require_pidfds()
    unit = os.fstat(lease_fd)
    store.claim_worker(tid, turn['epoch'], dict(device=unit.st_dev, inode=unit.st_ino), process_identity(os.getpid()))

    def tick():
        nonlocal last_sample, requested, stop_started, reason
        current = store.turn(tid)
        try: parent_gone = os.read(parent_fd, 1) == b''
        except BlockingIOError: parent_gone = False
        stop = bool(current['stop_at'] or parent_gone or stopping_signal[0] or time.time() > deadline)
        if stop and not requested:
            requested = True; stop_started = time.monotonic()
            reason = 'parent_closed' if parent_gone else 'deadline' if time.time() > deadline else 'stop_requested'
            store.update(tid, state='stopping')
            if provider is not None: provider.interrupt()
        if requested and time.monotonic() - stop_started >= 5:
            signal_owned(os.getpid(), signal.SIGTERM)
            if time.monotonic() - stop_started >= 7:
                signal_owned(os.getpid(), signal.SIGKILL)
                raise InterruptedError(reason)
        if time.monotonic() - last_sample >= 0.2:
            last_sample = time.monotonic()
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
            # Retain bounded notifications that arrive before an RPC response.
            # They are processed in order after the turn identity is acknowledged.

    def emit(kind, data):
        # Text is chunked before database/HTTP copies. Other indivisible records
        # are rejected by the store rather than truncated into a false result.
        if kind in {'message.delta', 'message.completed', 'output.delta'}:
            content = data.get('text', '')
            if len(content.encode()) > 32768:
                if kind == 'message.completed':
                    # Completion text can duplicate deltas; mark a paged body.
                    store.event(tid, 'message.completed', dict(data, text='', paged=True))
                    kind = 'output.delta'
                for offset in range(0, len(content), 8000):
                    store.event(tid, kind, dict(data, text=content[offset:offset + 8000]))
                return
        store.event(tid, kind, data)

    try:
        tick()
        if requested: raise InterruptedError(reason)
        if guided:
            from demiflow.forge.api_reference import REFERENCE_BYTES, sdk_identity
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
                prompt = ('根据要求、数据字典与已声明工具生成当前 cell 的完整源码。数据与源码仅供参考。'
                          '只返回说明和一个 Python 代码块，不编辑项目或执行任务。写入操作由用户显式确认。')
            manifest = dict(operation=turn['operation'], sources=[], bytes=len(prompt.encode()))
            store.update(tid, context=wire(manifest).decode(), delivery='prepared')
        else:
            if guided:
                from demiflow.workbench.conversations.inputs import InputAccess
                denied_inputs = [deployment.state, Path(config_path).resolve()]
                denied_inputs.extend(Path(p) for p in json.loads(os.environ.pop('DEMIFLOW_INPUT_DENIED_PATHS', '[]')))
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
        tick()
        if requested: raise InterruptedError(reason)
        config = deployment.providers[session['provider']]
        adapter = adapter_for(session['provider'],turn.get('operation','message'))
        acp = getattr(adapter, 'acp', False)
        if acp and guided:
            control_bridge = BridgeServer(input_access)
        provider_home = home / 'provider' if deployment.mode == 'restricted' else home
        provider_home.mkdir(mode=0o700, exist_ok=True)
        if deployment.mode == 'restricted':
            broker_home = home / 'control' / (tid + '-' + turn['epoch']) / 'model'
            broker_home.mkdir(mode=0o700, parents=True, exist_ok=True)
        confined = sandbox_spec(deployment, project, session, turn, manifest, config, provider_home) if deployment.mode == 'restricted' else None
        if confined is not None:
            if cell_cli:
                from dataclasses import replace
                confined = replace(confined, runtime_paths=(*confined.runtime_paths, str(control_bridge.bin)))
            from demiflow.workbench.conversations.sandbox import specification
            specification(confined, [sys.executable], {}, project['root'])
            from demiflow.workbench.conversations.model_broker import start_model_broker
            model_broker = start_model_broker(deployment, turn, session, broker_home)
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
        store.event(tid, 'provider.bound', effective)
        store.update(tid, delivery='not_applicable' if compact else 'sent')
        open_key = provider.open(prompt) if acp else provider.open(prompt, tools=(INPUT_TOOL,) if guided else ())
        thread = provider.opened(wait(open_key))
        store.bind_provider(session['id'], thread)
        store.update(tid, delivery='not_applicable' if compact else 'prepared' if acp else 'acknowledged')
        if not compact and acp and session['provider_session']:
            store.event(tid, 'history.gap', dict(reason='ACP resume does not replay provider transcript; saved application events remain authoritative'))
        # Compare again after context preparation and handshake; no baseline reset.
        if watched and capture(project['root'], watched, exclude=excluded) != baseline:
            reject('Watched sources changed during preparation; no model turn sent', 'conflict')
        tick()
        if requested: raise InterruptedError(reason)
        # Persist unknown BEFORE writing to the provider pipe: a crash cannot
        # turn an uncertain paid request into a supposedly safe automatic retry.
        store.update(tid, dispatch='unknown')
        start_key = provider.start(turn['text'], turn['submission'])
        if acp:
            native_turn = provider.turn
            store.update(tid, dispatch='sent', delivery='not_applicable' if compact else 'sent', provider_turn=native_turn, state='running')
        else:
            started = wait(start_key)
            native_turn = provider.started(started)
            store.update(tid, dispatch='acknowledged', provider_turn=native_turn, state='running')
        while not completed:
            current = tick()
            transport.poll()
            while transport.events:
                event = transport.pop()
                notifications = provider.event(event)
                if provider.turn and not store.turn(tid)['provider_turn']:
                    store.update(tid,provider_turn=provider.turn)
                for kind, data in notifications:
                    if kind == 'activity':
                        if not provider.acknowledged:
                            provider.acknowledged = True
                            store.update(tid, delivery='not_applicable' if compact else 'acknowledged', dispatch='acknowledged')
                    elif kind == 'done':
                        final = data['status']; completed = True
                        if data.get('error'): reason = str(data['error'])[:2048]
                    elif kind == 'request':
                        tick()
                        if requested: continue
                        if automatic_approval(project, data, assurance=getattr(process, 'execution_assurance', 'host_permissions')):
                            reply = dict(id=data['rpc_id'], result=provider.answer(data, 'allow_once'))
                            if store.dispatch_provider_reply(tid, turn['epoch'], lambda: transport.send(reply)):
                                store.event(tid, 'approval.automatic', dict(method=data['method'], authorization='existing_deployment_scope'))
                            continue
                        key = store.request(tid, data); outstanding[key] = data
                        store.update(tid, state='waiting_input' if data['method'].endswith('requestUserInput') else 'waiting_approval')
                    elif kind == 'bridge':
                        tick()
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
            for pending in store.pending(tid):
                if cell_cli and json.loads(pending['request']).get('method') == 'notebook/command':
                    continue
                tick()
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
            if acp and start_key in transport.responses:
                response = transport.take(start_key)
                outcome = provider.completion(response)
                final, completed = outcome['status'], True
                store.update(tid, delivery='not_applicable' if compact else 'acknowledged', dispatch='acknowledged')
            if transport.closed and not completed: raise ConnectionError('Provider connection lost')
            # Interrupt has its own RPC result; it is not proof of child cleanup.
            for key in list(transport.responses):
                transport.take(key)
        if requested: final = 'cancelled'
    except BaseException as exc:
        reason = reason or (type(exc).__name__ + ': ' + str(exc))[:2048]
        final = 'cancelled' if requested else 'interrupted' if isinstance(exc, (ConnectionError, TimeoutError)) else 'failed'
        current = store.turn(tid)
        if current['delivery'] == 'sent': store.update(tid, delivery='unknown')
        if current['dispatch'] == 'sent': store.update(tid, dispatch='unknown')
    finally:
        if control_bridge is not None: control_bridge.close()
        drained = False
        try:
            if process is not None:
                if provider is not None and getattr(provider, 'acp', False) and not transport.closed:
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
            reason = reason or ('Cleanup failed: ' + type(exc).__name__)
        if model_broker is not None:
            try: model_broker.close()
            except BaseException as exc:
                reason = reason or ('Model broker cleanup failed: ' + type(exc).__name__)
                drained = False
        try:
            changes = []; revision = None
            saved = store.turn(tid)
            if saved['baseline'] and watched:
                before = json.loads(saved['baseline']); after = capture(project['root'], watched, exclude=excluded)
                changes = list(differences(before, after, project['writes']))
                if changes:
                    source_hash = lambda values: digest({name:(fact or {}).get('sha256') for name,fact in values.items()})
                    revision = dict(before=source_hash(before),after=source_hash(after))
                for change in changes:
                    store.event(tid, 'change.observed', dict(files=[change], authorship='unknown_shared_workspace', evidence='stale'))
                if any(not change['within_scope'] for change in changes):
                    final, reason = 'failed', 'Watched changes outside authorized scope; all contents preserved'
        except BaseException as exc:
            # Source observation and event quotas can fail after a proven drain.
            # Preserve that cleanup fact, but never claim a complete diff.
            final, reason = 'failed', ('Change observation incomplete: ' + type(exc).__name__ + ': ' + str(exc))[:2048]
        try:
            result = dict(reason=reason, peaks=peaks, changed_files=len(changes), evidence='stale' if changes else 'missing',
                          cleanup='complete' if drained else 'unknown', provider_completed=completed)
            result['execution_assurance'] = (getattr(process, 'execution_assurance', 'host_permissions')
                                             if process is not None else 'not_started')
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
                metadata = getattr(provider,'compaction_result',None)
                if metadata is None:
                    final, result['reason'] = 'failed', 'Provider did not confirm a context compaction item'
                else:
                    result['compaction'] = dict(metadata,history_through_sequence=history_boundary)
                    store.event(tid,'compaction.completed',result['compaction'])
            store.finish_provider(tid, state=final if drained else 'stopping', cleanup='complete' if drained else 'unknown', result=result)
        except BaseException:
            # No false cleanup receipt on disk-full, observation or descendant
            # ownership failure. The persistent reservation remains held.
            try: store.update(tid, state='stopping', cleanup='unknown')
            except BaseException: pass
        if coding_fd is not None:
            os.close(coding_fd)
        os.close(lease_fd); os.close(parent_fd)


if __name__ == '__main__':
    run(sys.argv[1], sys.argv[2], *(int(fd) for fd in sys.argv[3:]))
