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

from demiflow.execution.process_observation import descendants
from demiflow.execution.process_observation import drain_owned
from demiflow.execution.process_observation import require_pidfds
from demiflow.execution.process_observation import signal_owned
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
from demiflow.workbench.conversations.changes import capture
from demiflow.workbench.conversations.changes import differences


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
    deadline = turn['created'] + limits.turn_seconds
    stop_started = None
    last_sample, peaks = 0, {'rss_bytes': 0, 'private_bytes': 0, 'processes': 0}
    outstanding = {}
    protocol_bridge = None
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
        baseline = capture(project['root'], project['watch'])
        store.update(tid, baseline=wire(baseline).decode())
        if compact:
            prompt = ''
            manifest = dict(operation='compact', provider=session['provider'], delivery='not_applicable')
            store.update(tid, context=wire(manifest).decode(), delivery='not_applicable')
        else:
            manifest, prompt = assemble(project, turn['text'], scratch, limits)
            store.update(tid, context=wire(manifest).decode(), delivery='prepared')
            protocol_bridge = ProtocolBridge(store, tid, project, scratch)
            store.event(tid, 'context.bound', {'digest': manifest['digest'], 'kind': manifest['kind'],
                'task_id': manifest['task_id'], 'sources': len(manifest['sources']), 'bytes': manifest['bytes'], 'delivery': 'prepared'})
        tick()
        if requested: raise InterruptedError(reason)
        config = deployment.providers[session['provider']]
        adapter = adapter_for(session['provider'],turn.get('operation','message'))
        acp = getattr(adapter, 'acp', False)
        command, env = adapter.launch(config, home, project['root'])
        env['DEMIFLOW_EXECUTION_UNIT_FD'] = str(lease_fd)
        launcher = Path(__file__).with_name('provider_exec.py')
        process = subprocess.Popen([sys.executable, '-I', '-B', str(launcher), str(os.getpid()), *command],
            cwd=home, env=env, stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
            start_new_session=True, pass_fds=(lease_fd,))
        transport = Transport(process, limits, acp=acp)
        saved_session = session['provider_session'] if acp or store.has_dispatched(session['id']) else None
        provider = adapter(transport, config, project, saved_session)
        if not compact and acp and project['kind'] == 'demiforge_registered':
            provider.mcp_servers = [dict(name='development-protocol', command=sys.executable,
                args=['-I', '-B', str(Path(__file__).with_name('mcp_bridge.py')), str(config_path), tid], env=[])]
        handshake = wait(provider.initialize(), timeout=40)
        configuration_key = provider.initialized(handshake)
        effective = provider.verify_config(wait(configuration_key) if configuration_key else {})
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
                        key = store.request(tid, data); outstanding[key] = data
                        store.update(tid, state='waiting_input' if data['method'].endswith('requestUserInput') else 'waiting_approval')
                    elif kind == 'bridge':
                        try:
                            answer = protocol_bridge.invoke(data['arguments'])
                            result = dict(success=True, contentItems=[dict(type='inputText', text=wire(answer, 65536).decode())])
                        except Exception as exc:
                            result = dict(success=False, contentItems=[dict(type='inputText', text=str(exc)[:2048])])
                        transport.send(dict(id=data['rpc_id'], result=result))
                        if result['success']: protocol_bridge.sent()
                    else: emit(kind, data)
            for pending in store.pending(tid):
                request = outstanding.get(pending['id'])
                if request is None: reject('Pending request belongs to a lost provider connection')
                if pending['expires'] < time.time(): raise TimeoutError('Input request expired without approval')
                if pending['answer'] is not None:
                    answer = provider.answer(request, json.loads(pending['answer']))
                    transport.send(dict(id=request['rpc_id'], result=answer))
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
            if revision: result['revision'] = revision
            if compact and completed and final == 'completed':
                metadata = getattr(provider,'compaction_result',None)
                if metadata is None:
                    final, result['reason'] = 'failed', 'Provider did not confirm a context compaction item'
                else:
                    result['compaction'] = dict(metadata,history_through_sequence=history_boundary)
                    store.event(tid,'compaction.completed',result['compaction'])
            store.update(tid, state=final if drained else 'stopping', cleanup='complete' if drained else 'unknown', result=wire(result).decode())
        except BaseException:
            # No false cleanup receipt on disk-full, observation or descendant
            # ownership failure. The persistent reservation remains held.
            try: store.update(tid, state='stopping', cleanup='unknown')
            except BaseException: pass
        os.close(lease_fd); os.close(parent_fd)


if __name__ == '__main__':
    run(sys.argv[1], sys.argv[2], int(sys.argv[3]), int(sys.argv[4]))
