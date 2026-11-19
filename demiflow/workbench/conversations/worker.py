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
from demiflow.workbench.conversations.context import ProtocolBridge
from demiflow.workbench.conversations.context import BridgeServer
from demiflow.workbench.conversations.changes import capture
from demiflow.workbench.conversations.changes import differences
from demiflow.workbench.conversations.execution import launch_provider
from demiflow.workbench.conversations.development import managed, prepare, provider_message, request_deadline, runtime_paths, project_tasks, current_request


def sandbox_spec(deployment, project, session, turn, manifest, config, private_home):
    """Assemble only controller-declared scope; no provider-supplied path grants."""
    from demiflow.workbench.conversations.sandbox import SandboxSpec
    root = Path(project['root'])
    sources = [Path(source['canonical_path']) for source in manifest.get('sources', [])
               if source.get('origin') in {'authoritative_file', 'managed_development_api'}]
    read = [root, *[path for path in sources if path.is_file()]]
    openssl = Path('/etc/ssl/openssl.cnf')
    if openssl.is_file(): read.append(openssl)
    writes = [] if turn.get('operation') == 'compact' else [root / name for name in project['writes']]
    protected = [root / '.git', root / '.forge', root / '.demiforge', root / 'AGENTS.md']
    protected.extend(path for path in sources if path.name in {'AGENTS.md', 'PIPELINE_SPEC.md'})
    # The immutable registered declaration owns verifier argv and readonly roots.
    if managed(project):
        from demiflow.forge.development.project import Project
        declared = Project(root)
        protected.extend((root / path).resolve() for path in declared.config['readonly'])
        read.extend(path for path in protected if path.exists())
    denied = [deployment.state / 'chat.sqlite3', deployment.state / 'forge-development',
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
            elif entry.is_dir(follow_symlinks=False) or entry.name.endswith('.json'):
                denied.append(Path(entry.path))
    for provider in deployment.providers.values():
        if provider.get('native_credentials'): denied.append(Path(provider['native_credentials']))
    unique = lambda paths: tuple(sorted({str(path.absolute()) for path in paths}))
    return SandboxSpec(read_paths=unique(read), write_paths=unique(writes), runtime_paths=runtime_paths(config),
        protected_paths=unique(protected), denied_paths=unique(denied), private_home=str(private_home),
        role='readonly' if turn.get('operation') == 'compact' else 'developer',
        seconds=max(.1, request_deadline(turn, deployment.limits)-time.time()), rss_bytes=deployment.limits.rss_bytes,
        processes=deployment.limits.processes, file_bytes=min(64*1024**2, deployment.limits.provider_bytes),
        private_bytes=min(deployment.limits.provider_bytes, deployment.limits.private_total_bytes))


def automatic_approval(project, request, *, assurance):
    """Reuse an existing grant; business input and permission expansion still wait."""
    if not managed(project) or not request.get('allow_once'): return False
    method = request['method']
    if method.endswith('requestUserInput') or method == 'item/permissions/requestApproval': return False
    if method == 'item/fileChange/requestApproval':
        # The adapter has already matched every effective changed path to writes.
        return True
    if assurance == 'enforced' and method in {'item/commandExecution/requestApproval', 'session/request_permission'}:
        return True
    if method == 'session/request_permission':
        item = request.get('params', {}).get('known_tool') or {}
        raw = item.get('rawInput') or {}
        path = raw.get('path') if type(raw) is dict else None
        if item.get('kind') == 'edit' and isinstance(path, str):
            from demiflow.workbench.conversations.common import safe_path
            try:
                relative = str(Path(path).relative_to(project['root'])) if Path(path).is_absolute() else path
                safe_path(project['root'], relative, missing=True)
                return any(relative == allowed or relative.startswith(allowed + '/') for allowed in project['writes'])
            except (ValueError, OSError): pass
    return False


def run(config_path, tid, lease_fd, parent_fd):
    deployment = Deployment.load(config_path)
    store = Store(deployment.state, deployment.limits)
    turn = store.turn(tid); session = store.session(turn['session'])
    compact = turn.get('operation') == 'compact'
    with store.connect() as db:
        history_boundary = db.execute('SELECT min(sequence)-1 FROM events WHERE turn=?',(tid,)).fetchone()[0] or 0
    project = deployment.project(session['workspace'], json.loads(session['root'])['path'])
    home = deployment.state / session['id']; scratch = home / turn['id']; scratch.mkdir(mode=0o700, exist_ok=True)
    limits = deployment.limits
    process = transport = provider = None
    final, reason, completed, draining = 'failed', None, False, False
    requested = False
    deadline = request_deadline(turn, limits)
    stop_started = None
    last_sample, peaks = 0, {'rss_bytes': 0, 'private_bytes': 0, 'processes': 0}
    outstanding = {}
    protocol_bridge = None
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
                    store.event(tid, 'message.completed', dict(item_id=data['item_id'], text='', paged=True))
                    kind = 'output.delta'
                for offset in range(0, len(content), 8000):
                    store.event(tid, kind, dict(data, text=content[offset:offset + 8000]))
                return
        store.event(tid, kind, data)

    try:
        tick()
        if requested: raise InterruptedError(reason)
        if deployment.mode == 'restricted' and project['kind'] == 'demiforge_registered' and not managed(project):
            reject('Restricted registered development requires controller-managed tasks')
        baseline = capture(project['root'], project['watch'])
        store.update(tid, baseline=wire(baseline).decode())
        if compact:
            prompt = ''
            manifest = dict(operation='compact', provider=session['provider'], delivery='not_applicable')
            store.update(tid, context=wire(manifest).decode(), delivery='not_applicable')
        else:
            managed_tasks = prepare(deployment, store, turn, session, project) if managed(project) else None
            manifest, prompt = assemble(project, turn['text'], scratch, limits,
                managed_tasks=managed_tasks, task_id=session['id'] if managed_tasks is not None else None,
                execution_mode=deployment.mode, current_request=current_request(turn) if managed_tasks is not None else None)
            store.update(tid, context=wire(manifest).decode(), delivery='prepared')
            protocol_bridge = ProtocolBridge(store, tid, project, scratch)
            store.event(tid, 'context.bound', {'digest': manifest['digest'], 'kind': manifest['kind'],
                'task_id': manifest['task_id'], 'sources': len(manifest['sources']), 'bytes': manifest['bytes'], 'delivery': 'prepared'})
        tick()
        if requested: raise InterruptedError(reason)
        config = deployment.providers[session['provider']]
        adapter = adapter_for(session['provider'],turn.get('operation','message'))
        acp = getattr(adapter, 'acp', False)
        if not compact and acp and project['kind'] == 'demiforge_registered':
            control_bridge = BridgeServer(protocol_bridge, epoch=turn['epoch'], deadline=deadline)
        provider_home = home / 'provider' if deployment.mode == 'restricted' else home
        provider_home.mkdir(mode=0o700, exist_ok=True)
        if deployment.mode == 'restricted':
            broker_home = home / 'control' / (tid + '-' + turn['epoch']) / 'model'
            broker_home.mkdir(mode=0o700, parents=True, exist_ok=True)
        confined = sandbox_spec(deployment, project, session, turn, manifest, config, provider_home) if deployment.mode == 'restricted' else None
        if confined is not None:
            from demiflow.workbench.conversations.sandbox import specification
            specification(confined, [sys.executable], {}, project['root'])
            from demiflow.workbench.conversations.model_broker import start_model_broker
            model_broker = start_model_broker(deployment, turn, session, broker_home)
            config = model_broker.child_config
        process = launch_provider(mode=deployment.mode, adapter=adapter, config=config,
            home=provider_home, root=project['root'], lease_fd=lease_fd, sandbox_spec=confined,
            control_socket=control_bridge.path if control_bridge is not None else None,
            model_socket=model_broker.socket_path if model_broker is not None else None)
        transport = Transport(process, limits, acp=acp)
        continuing_repair = bool(json.loads(turn.get('development_control') or '{}').get('repair_round'))
        saved_session = session['provider_session'] if acp or continuing_repair or store.has_dispatched(session['id']) else None
        provider = adapter(transport, config, project, saved_session)
        if not compact and acp and project['kind'] == 'demiforge_registered':
            provider.mcp_servers = [dict(name='development-protocol', command=sys.executable,
                args=['-I', '-B', str(Path(__file__).with_name('mcp_bridge.py')), '--connect', str(control_bridge.path),
                      control_bridge.token, tid, turn['epoch']], env=[])]
        handshake = wait(provider.initialize(), timeout=40)
        configuration_key = provider.initialized(handshake)
        effective = provider.verify_config(wait(configuration_key) if configuration_key else {})
        effective['execution_assurance'] = getattr(process, 'execution_assurance', 'gate_only')
        store.event(tid, 'provider.bound', effective)
        store.update(tid, delivery='not_applicable' if compact else 'sent')
        thread = provider.opened(wait(provider.open(prompt)))
        store.bind_provider(session['id'], thread)
        store.update(tid, delivery='not_applicable' if compact else 'prepared' if acp else 'acknowledged')
        if not compact and acp and session['provider_session']:
            store.event(tid, 'history.gap', dict(reason='ACP resume does not replay provider transcript; saved application events remain authoritative'))
        # Compare again after context preparation and handshake; no baseline reset.
        if capture(project['root'], project['watch']) != baseline:
            reject('Watched sources changed during preparation; no model turn sent', 'conflict')
        tick()
        if requested: raise InterruptedError(reason)
        # Persist unknown BEFORE writing to the provider pipe: a crash cannot
        # turn an uncertain paid request into a supposedly safe automatic retry.
        store.update(tid, dispatch='unknown')
        message, submission = provider_message(turn)
        start_key = provider.start(message, submission)
        if acp:
            native_turn = provider.turn
            store.update(tid, dispatch='sent', delivery='not_applicable' if compact else 'sent', provider_turn=native_turn, state='running')
        else:
            started = wait(start_key)
            native_turn = provider.started(started)
            store.update(tid, dispatch='acknowledged', provider_turn=native_turn, state='running')
        if turn.get('development'):
            development = json.loads(store.turn(tid)['development'])
            repair = json.loads(turn.get('development_control') or '{}').get('repair_round', 0)
            store.update(tid, development=wire(dict(development, state='repairing' if repair else 'developing')).decode())
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
                        if automatic_approval(project, data, assurance=getattr(process, 'execution_assurance', 'gate_only')):
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
                            answer = protocol_bridge.invoke(data['arguments'])
                            result = dict(success=True, contentItems=[dict(type='inputText', text=wire(answer, 65536).decode())])
                        except Exception as exc:
                            result = dict(success=False, contentItems=[dict(type='inputText', text=str(exc)[:2048])])
                        transport.send(dict(id=data['rpc_id'], result=result))
                        if result['success']: protocol_bridge.sent()
                    else: emit(kind, data)
            for pending in store.pending(tid):
                tick()
                if requested: break
                request = outstanding.get(pending['id'])
                if request is None: reject('Pending request belongs to a lost provider connection')
                if pending['expires'] < time.time(): raise TimeoutError('Input request expired without approval')
                if pending['answer'] is not None:
                    answer = provider.answer(request, json.loads(pending['answer']))
                    if not store.dispatch_provider_reply(tid, turn['epoch'], lambda: transport.send(dict(id=request['rpc_id'], result=answer))):
                        break
                    if not compact and managed_tasks is not None and request['method'].endswith('requestUserInput'):
                        managed_tasks.note(session['id'], 'decision', pending['answer'], event_id=pending['id'])
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
            if saved['baseline']:
                before = json.loads(saved['baseline']); after = capture(project['root'], project['watch'])
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
            result['execution_assurance'] = getattr(process, 'execution_assurance', 'gate_only')
            if (not compact and managed(project) and drained and completed and final == 'completed'
                    and result['execution_assurance'] == 'enforced'):
                evidence = getattr(process, 'execution_evidence', None)
                if evidence is None: reject('Enforced provider omitted its actual boundary evidence')
                tasks = project_tasks(deployment, session, project)
                candidate = tasks.status(session['id'])
                if candidate['source_fingerprint'] is not None:
                    receipt = tasks.review_authority.issue_execution(session['id'], candidate['source_fingerprint'],
                        phase='developer', backend=evidence['backend'], implementation=evidence['implementation'], execution_id=evidence['execution_id'])
                    tasks.record_execution(session['id'], candidate['source_fingerprint'], receipt=receipt)
                result['execution'] = evidence
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
        os.close(lease_fd); os.close(parent_fd)


if __name__ == '__main__':
    run(sys.argv[1], sys.argv[2], int(sys.argv[3]), int(sys.argv[4]))
