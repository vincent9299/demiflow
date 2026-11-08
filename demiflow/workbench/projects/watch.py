"""One bounded foreground observer; independent of HTTP and execution."""

from collections import deque
from contextlib import ExitStack
import json
import signal
import threading
import time
import uuid

from demiflow.workbench.projects import registry as console
from demiflow.workbench.projects import workspace as ws
from demiflow.forge.platform import content_digest
from demiflow.forge.schema import ForgeError


def _step(root, state, *, session, interval, full_interval):
    """At most one content check per project turn; all targets share a cursor."""
    _, value, _ = ws.load_workspace(root)
    if value['format'] != ws.FORMAT_V3:
        return False
    policy = content_digest(value)
    if state.get('policy') != policy:
        state.clear()
        state.update(policy=policy, next_observe=0, cursor=0, hint=None)
    now = time.monotonic()
    if now >= state['next_observe']:
        state['next_observe'] = now + interval
        try:
            observed = ws.audit_observe(root, expected_policy=policy, session=session, previous_hint=state['hint'])
            state['hint'] = observed['hint']
        except ForgeError:
            # A failed hint must not starve full verification of other targets.
            # observe has already marked prior observations pending when possible.
            state['hint'] = None
    current = ws.audit_status(root)
    candidates = [row for row in current['observations'] if row['freshness'] != 'unconfigured']
    for offset in range(len(candidates)):
        index = (state['cursor'] + offset) % len(candidates)
        row = candidates[index]
        if row['freshness'] in {'unknown', 'pending', 'overdue'}:
            state['cursor'] = (index + 1) % len(candidates)
            ws.audit_check(root, kind=row['kind'], report_digest=row['identity'] if row['kind'] == 'report' else None,
                           expected_policy=policy, full_interval=full_interval)
            return True
    return False


def watch(registry, *, interval=30, full_interval=300, stop=None):
    """Run until stopped. No listener, report creation, or Flow execution."""
    if type(interval) is not int or not 1 <= interval <= 300 or type(full_interval) is not int or not interval <= full_interval <= 3600:
        raise ForgeError('Invalid watcher intervals', code='invalid_request')
    stop = stop if stop is not None else threading.Event()
    session = uuid.uuid4().hex
    states, leases, ring = {}, {}, deque()
    next_registry = 0
    old_signals = {}
    if threading.current_thread() is threading.main_thread():
        for signum in (signal.SIGINT, signal.SIGTERM):
            old_signals[signum] = signal.signal(signum, lambda *_: stop.set())
    try:
        while not stop.is_set():
            now = time.monotonic()
            if now >= next_registry:
                inventory = console.load_registry(registry)
                wanted = {key: item['root'] for key, item in inventory['projects'].items()}
                for key in list(states):
                    if wanted.get(key) != states[key]['root']:
                        leases.pop(key).close()
                        del states[key]
                for key, root in wanted.items():
                    if key in states:
                        continue
                    stack = ExitStack()
                    try:
                        _, value, _ = ws.load_workspace(root)
                        if ws.workspace_id(root, value) != key:
                            raise ForgeError('Registered workspace identity changed')
                        if value['format'] != ws.FORMAT_V3:
                            continue
                        stack.enter_context(ws.audit_lease(root, 'watch'))
                    except (OSError, ValueError) as exc:
                        stack.close()
                        print(json.dumps(dict(workspace_id=key, status='unavailable', error=str(exc)[:512])), flush=True)
                        continue
                    leases[key], states[key] = stack, dict(root=root, schedule={}, retry_at=0)
                ring = deque(key for key in ring if key in states)
                ring.extend(key for key in states if key not in ring)
                next_registry = now + interval
            if not ring:
                stop.wait(min(1, interval))
                continue
            key = ring.popleft()
            ring.append(key)
            item = states[key]
            if now < item['retry_at']:
                stop.wait(.1)
                continue
            try:
                # Identity is checked each turn, not merely at registration.
                _, control, _ = ws.load_workspace(item['root'])
                if ws.workspace_id(item['root'], control) != key:
                    raise ForgeError('Registered workspace identity changed')
                worked = _step(item['root'], item['schedule'], session=session, interval=interval, full_interval=full_interval)
                if not worked:
                    item['retry_at'] = time.monotonic() + interval
                    stop.wait(.1)
            except (OSError, ValueError) as exc:
                item['retry_at'] = time.monotonic() + interval
                print(json.dumps(dict(workspace_id=key, status='unavailable', error=str(exc)[:512])), flush=True)
    finally:
        for lease in leases.values():
            lease.close()
        for signum, previous in old_signals.items():
            signal.signal(signum, previous)
