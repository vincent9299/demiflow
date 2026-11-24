"""Bounded attempt-local programs, never recoverable pickle substitutes."""
import secrets
import threading

from demiflow.execution.contracts.dataflow_errors import DataflowError

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
    from demiflow.execution.contracts.recovery_contract import identity
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
