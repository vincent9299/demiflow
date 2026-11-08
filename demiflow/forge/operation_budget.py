"""One ephemeral deadline for a public control call and its nested workers.

No run/capacity facts live here. A one-shot worker may inherit a validated
parent bound; a resident reader or native business worker must start without
these transport fields. The shared server never modifies its environment.
"""
from contextlib import contextmanager
from contextvars import ContextVar
from functools import wraps
import math
import os
import time

from .schema import ForgeError

OPERATION_SECONDS = 60
SCOPE_ENV = 'DEMIFLOW_CONTROL_SCOPE'
DEADLINE_ENV = 'DEMIFLOW_CONTROL_DEADLINE_NS'
_SCOPE = 'oneshot-v1'
_CURRENT = ContextVar('demiflow_control_deadline', default=None)
_worker_bound = None


def current_deadline():
    current = _CURRENT.get()
    if _worker_bound is None:
        return current
    return _worker_bound if current is None else min(current, _worker_bound)


def check(deadline=None):
    current = current_deadline()
    if deadline is not None:
        current = deadline if current is None else min(deadline, current)
    if current is not None and time.monotonic() >= current:
        raise ForgeError('Control operation deadline exceeded', code='resource_limit')


@contextmanager
def scope(*, deadline=None):
    limit = time.monotonic() + OPERATION_SECONDS
    if deadline is not None:
        if type(deadline) not in (int, float) or not math.isfinite(deadline):
            raise ForgeError('Invalid control deadline', code='resource_limit')
        limit = min(limit, deadline)
    inherited = current_deadline()
    if inherited is not None:
        limit = min(limit, inherited)
    token = _CURRENT.set(limit)
    try:
        check()
        yield limit
        # A committed ready/launch fact is not undone by a late response.
    finally:
        _CURRENT.reset(token)


def controlled(function):
    @wraps(function)
    def call(*args, **kwargs):
        with scope():
            return function(*args, **kwargs)
    return call


def child_deadline(seconds):
    if type(seconds) not in (int, float) or not math.isfinite(seconds) or seconds <= 0:
        raise ForgeError('Invalid child operation limit', code='resource_limit')
    deadline = time.monotonic() + min(seconds, OPERATION_SECONDS)
    inherited = current_deadline()
    if inherited is not None:
        deadline = min(deadline, inherited)
    check(deadline)
    return deadline


def worker_environment(deadline):
    return {SCOPE_ENV: _SCOPE, DEADLINE_ENV: str(int(deadline * 10**9))}


def clear_worker_environment(environment):
    environment.pop(SCOPE_ENV, None)
    environment.pop(DEADLINE_ENV, None)
    return environment


def bind_worker():
    """Called only after _bind_parent has verified the launching parent.

    Absence is a no-op: do not turn a native worker or resident reader into a
    timed control worker. This installs no per-message reader protocol.
    """
    global _worker_bound
    marker = os.environ.get(SCOPE_ENV)
    raw = os.environ.get(DEADLINE_ENV)
    if marker is None and raw is None:
        return
    if (marker != _SCOPE or type(raw) is not str or not raw.isascii() or
            not raw.isdecimal() or not 1 <= len(raw) <= 20):
        raise ForgeError('Invalid control worker deadline transport', code='resource_limit')
    deadline = int(raw) / 10**9
    if deadline <= 0 or deadline > time.monotonic() + OPERATION_SECONDS:
        raise ForgeError('Expanded control worker deadline', code='resource_limit')
    if _worker_bound is not None and deadline != _worker_bound:
        raise ForgeError('A one-shot worker cannot rebind its operation', code='resource_limit')
    _worker_bound = deadline
    # Generic/native subprocesses must not inherit stale transport by env copy.
    # Nested _capture explicitly exports its remaining validated bound again.
    clear_worker_environment(os.environ)
