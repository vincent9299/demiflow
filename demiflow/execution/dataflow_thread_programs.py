"""Bounded attempt-local programs, never recoverable pickle substitutes."""
import secrets
import threading

from .contracts.dataflow_errors import DataflowError

_VALUES = {}
_LOCK = threading.Lock()
_MAX_VALUES = 4096


def owner(action):
    run = action.context
    return {'run_id': run.ref.run_id, 'action_id': action.action_id,
            'attempt': run.attempt, 'fence': run.fence}


def _key(binding, occurrence_id, program_key):
    if (type(binding) is not dict or set(binding) != {'run_id', 'action_id', 'attempt', 'fence'}
            or type(binding['attempt']) is not int or binding['attempt'] < 0
            or any(type(binding[name]) is not str or not binding[name] for name in ('run_id', 'action_id', 'fence'))):
        raise DataflowError('INCOMPATIBLE_HISTORY', 'live program requires an authenticated attempt binding')
    return (binding['run_id'], binding['action_id'], binding['attempt'], binding['fence'], occurrence_id, program_key)


_KEYS = {}


def marker(occurrence_id, code_identity):
    from .recovery_contract import identity
    return {'schema': 'demiflow.thread_attempt.v1', 'capability': 'attempt_only',
            'program_key': identity(['thread-program-v3', occurrence_id, code_identity])}


def register(value, *, binding, occurrence_id, max_entries, program_marker=None):
    program_marker = marker(occurrence_id, {}) if program_marker is None else program_marker
    key = _key(binding, occurrence_id, program_marker['program_key'])
    with _LOCK:
        if key in _KEYS:
            old = _VALUES[_KEYS[key]]
            if old[2] is not value:
                raise DataflowError('FENCED', 'live declaration changed within one authenticated attempt')
            return program_marker
        if len(_VALUES) >= min(_MAX_VALUES, max_entries):
            raise DataflowError('BUDGET_EXCEEDED', 'live thread program registry is full')
        token = secrets.token_hex(32)
        _VALUES[token] = (dict(binding), occurrence_id, value, key)
        _KEYS[key] = token
        return program_marker


def lookup(program_marker, *, binding, occurrence_id):
    if (not isinstance(program_marker, dict)
            or set(program_marker) != {'schema', 'capability', 'program_key'}
            or program_marker['schema'] != 'demiflow.thread_attempt.v1'
            or program_marker['capability'] != 'attempt_only'):
        raise DataflowError('INCOMPATIBLE_HISTORY', 'invalid live program capability')
    key = _key(binding, occurrence_id, program_marker['program_key'])
    with _LOCK:
        item = _VALUES.get(_KEYS.get(key))
        if item is None or item[:2] != (dict(binding), occurrence_id):
            raise DataflowError('INCOMPATIBLE_HISTORY', 'live thread program registry is absent or fenced')
        return item[2]


def release(binding):
    """Called only after all original allocations have confirmed physical exit."""
    with _LOCK:
        for token in [key for key, item in _VALUES.items() if item[0] == binding]:
            item = _VALUES.pop(token)
            _KEYS.pop(item[3], None)


def select_worker_mode(request, local):
    """Explicit streaming thread scopes select a local host unless overridden."""
    if request.provider != 'local':
        return None
    if local is not None and getattr(local, 'worker_mode_explicit', True):
        return local.worker_mode
    from ..planning.dataflow_compile import expand_relational_inputs, expand_operation_inputs
    from ..data.plan import AsyncMapOp, MapOp, BoundMapOp
    from ..planning.dataflow_policy import resolve_runtime_policy
    _, bounds = resolve_runtime_policy({}, request.action.context.limits, provider='local')
    pending = [(request.source, request.plan, ())]
    count = 0
    while pending:
        source, plan, path = pending.pop()
        count += 1 + len(plan.operations)
        if count > bounds.max_nodes:
            raise DataflowError('BUDGET_EXCEEDED', 'thread mode selection exceeds graph budget')
        for operation in plan.operations:
            stream = operation.stream_options if isinstance(operation, (MapOp, BoundMapOp)) else None
            if (stream is not None and stream.execution == 'thread'
                    or isinstance(operation, AsyncMapOp) and operation.execution == 'thread'):
                return 'thread'
            pending.extend((b.source, b.plan, b.path) for b in
                           expand_operation_inputs(operation, path=path, bounds=bounds))
        pending.extend((b.source, b.plan, b.path) for b in expand_relational_inputs(source, path=path, bounds=bounds))
    return getattr(local, 'worker_mode', 'process')
