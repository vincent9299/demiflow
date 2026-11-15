"""Data-only worker failures; a fixed exception vocabulary, never remote imports."""
import builtins
import traceback

from .contracts.dataflow_errors import DataflowError, DataflowRecoveryError, DATAFLOW_ERROR_CODES
from .recovery_contract import UnresolvedCommit
from .recovery_contract import RecoveryContractError

_BUILTINS = {name: getattr(builtins, name) for name in (
    'ValueError', 'TypeError', 'KeyError', 'IndexError', 'LookupError', 'TimeoutError',
    'RuntimeError', 'AssertionError', 'OSError', 'FileNotFoundError', 'PermissionError',
    'NotImplementedError', 'ZeroDivisionError')}
_KNOWN = {cls.__module__ + '.' + cls.__qualname__: cls for cls in _BUILTINS.values()}
from .queue_contract import QueueIncomplete, QueueIdleTimeout, StaleQueueOwner, ArtifactDomainUnavailable, HistoryReadLimit
_KNOWN.update({cls.__module__ + '.' + cls.__qualname__: cls for cls in (
    QueueIncomplete, QueueIdleTimeout, StaleQueueOwner, ArtifactDomainUnavailable)})
_RECOVERY_TYPE = DataflowRecoveryError.__module__ + '.' + DataflowRecoveryError.__qualname__
_KNOWN[RecoveryContractError.__module__ + '.' + RecoveryContractError.__qualname__] = RecoveryContractError
_KNOWN[UnresolvedCommit.__module__ + '.' + UnresolvedCommit.__qualname__] = UnresolvedCommit


def failure_payload(exc):
    name = type(exc).__module__ + '.' + type(exc).__qualname__
    message = str(exc) or type(exc).__name__
    payload = {'code': getattr(exc, 'code', 'INTERNAL_PROTOCOL'),
            'error_type': name if name in _KNOWN or name == _RECOVERY_TYPE else None,
            'message': message[:2048], 'traceback': traceback.format_exc()[-8000:]}
    if type(exc) is HistoryReadLimit:
        from .recovery_contract import document
        import json
        payload['error_type'] = 'demiflow.execution.queue_contract.HistoryReadLimit'
        payload['next_cursor'] = json.loads(document(exc.next_cursor, 4096))
    return payload


def restore_failure(payload):
    code = payload.get('code', 'INTERNAL_PROTOCOL')
    message = str(payload.get('message') or 'worker failed')[:2048]
    cls = _KNOWN.get(payload.get('error_type'))
    if payload.get('error_type') == 'demiflow.execution.queue_contract.HistoryReadLimit':
        from .recovery_contract import document
        import json
        error = HistoryReadLimit(message, json.loads(document(payload['next_cursor'], 4096)))
    elif payload.get('error_type') == _RECOVERY_TYPE:
        error = DataflowRecoveryError(code if code in DATAFLOW_ERROR_CODES else 'UNKNOWN_EFFECT', message)
    elif cls is not None:
        error = cls(message)
    else:
        error = DataflowError(code if code in DATAFLOW_ERROR_CODES else 'INTERNAL_PROTOCOL', message)
    if payload.get('traceback'):
        error.add_note(str(payload['traceback'])[-8000:])
    return error
