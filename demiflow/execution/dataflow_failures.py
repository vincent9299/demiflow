"""Data-only worker failures; a fixed exception vocabulary, never remote imports."""
import asyncio
import builtins
import traceback
from collections.abc import Mapping

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
_KNOWN[asyncio.CancelledError.__module__ + '.' + asyncio.CancelledError.__qualname__] = asyncio.CancelledError
_RECOVERY_TYPE = DataflowRecoveryError.__module__ + '.' + DataflowRecoveryError.__qualname__
_KNOWN[RecoveryContractError.__module__ + '.' + RecoveryContractError.__qualname__] = RecoveryContractError
_KNOWN[UnresolvedCommit.__module__ + '.' + UnresolvedCommit.__qualname__] = UnresolvedCommit
from ..operator_llm.errors import PromptBudgetExceededError
_KNOWN[PromptBudgetExceededError.__module__ + '.' + PromptBudgetExceededError.__qualname__] = PromptBudgetExceededError


def failure_payload(exc, *, _depth=0, _budget=None):
    _budget = [32] if _budget is None else _budget
    _budget[0] -= 1
    name = type(exc).__module__ + '.' + type(exc).__qualname__
    message = str(exc) or type(exc).__name__
    payload = {'code': getattr(exc, 'code', 'INTERNAL_PROTOCOL'),
            'error_type': name if name in _KNOWN or name == _RECOVERY_TYPE else None,
            'message': message[:2048], 'traceback': traceback.format_exc()[-8000:]}
    if _depth:
        payload['traceback'] = payload['traceback'][-2048:]
    if isinstance(exc, BaseExceptionGroup) and _depth < 4 and _budget[0] > 0:
        payload['error_type'] = 'builtins.BaseExceptionGroup'
        children = []
        for child in exc.exceptions[:16]:
            if not _budget[0]:
                break
            children.append(failure_payload(child, _depth=_depth + 1, _budget=_budget))
        payload['children'] = children
        if len(children) != len(exc.exceptions):
            payload['message'] += ' [additional cleanup errors truncated by control budget]'
    if type(exc) is HistoryReadLimit:
        from .recovery_contract import document
        import json
        payload['error_type'] = 'demiflow.execution.queue_contract.HistoryReadLimit'
        payload['next_cursor'] = json.loads(document(exc.next_cursor, 4096))
    return payload


def restore_failure(payload, *, _depth=0, _budget=None):
    _budget = [32] if _budget is None else _budget
    _budget[0] -= 1
    if _budget[0] < 0 or not isinstance(payload, Mapping):
        raise DataflowError('INVALID_CONTRACT', 'failure payload exceeds node budget')
    code = payload.get('code', 'INTERNAL_PROTOCOL')
    message = str(payload.get('message') or 'worker failed')[:2048]
    cls = _KNOWN.get(payload.get('error_type'))
    if payload.get('error_type') == 'builtins.BaseExceptionGroup':
        children = payload.get('children')
        if _depth >= 4 or type(children) not in (list, tuple) or not 1 <= len(children) <= 16:
            raise DataflowError('INVALID_CONTRACT', 'invalid failure group depth/membership')
        error = BaseExceptionGroup(message, [restore_failure(child, _depth=_depth + 1, _budget=_budget) for child in children])
    elif payload.get('error_type') == 'demiflow.execution.queue_contract.HistoryReadLimit':
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
