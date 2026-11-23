"""Data-only worker failures; a fixed exception vocabulary, never remote imports."""
import asyncio
import builtins
import traceback
import re
import sqlite3
from collections.abc import Mapping

from demiflow.execution.contracts.dataflow_errors import DataflowError, DataflowRecoveryError, DataflowUnresolvedCommit, DATAFLOW_ERROR_CODES
from demiflow.execution.recovery_contract import UnresolvedCommit
from demiflow.execution.recovery_contract import RecoveryContractError
from demiflow.execution.run_journal import ExecutionBudgetExceeded

_BUILTINS = {name: getattr(builtins, name) for name in (
    'ValueError', 'TypeError', 'KeyError', 'IndexError', 'LookupError', 'TimeoutError',
    'RuntimeError', 'AssertionError', 'OSError', 'FileNotFoundError', 'PermissionError',
    'NotImplementedError', 'ZeroDivisionError')}
_KNOWN = {cls.__module__ + '.' + cls.__qualname__: cls for cls in _BUILTINS.values()}
_OS_ERRORS = frozenset(cls for cls in _BUILTINS.values() if issubclass(cls, OSError))
_KNOWN['sqlite3.OperationalError'] = sqlite3.OperationalError
# Located JSON readers historically expose these parse errors to their callers.
# Admit only the two fixed library classes, never import a remote type name.
try:
    from ijson.common import JSONError, IncompleteJSONError
except ImportError:
    pass
else:
    _KNOWN.update({cls.__module__ + '.' + cls.__qualname__: cls
                   for cls in (JSONError, IncompleteJSONError)})
from demiflow.execution.queue_contract import QueueIncomplete, QueueIdleTimeout, StaleQueueOwner, ArtifactDomainUnavailable, HistoryReadLimit
_KNOWN.update({cls.__module__ + '.' + cls.__qualname__: cls for cls in (
    QueueIncomplete, QueueIdleTimeout, StaleQueueOwner, ArtifactDomainUnavailable)})
_KNOWN[asyncio.CancelledError.__module__ + '.' + asyncio.CancelledError.__qualname__] = asyncio.CancelledError
_RECOVERY_TYPE = DataflowRecoveryError.__module__ + '.' + DataflowRecoveryError.__qualname__
_UNRESOLVED_TYPE = DataflowUnresolvedCommit.__module__ + '.' + DataflowUnresolvedCommit.__qualname__
_BUDGET_TYPE = ExecutionBudgetExceeded.__module__ + '.' + ExecutionBudgetExceeded.__qualname__
_KNOWN[RecoveryContractError.__module__ + '.' + RecoveryContractError.__qualname__] = RecoveryContractError
_KNOWN[UnresolvedCommit.__module__ + '.' + UnresolvedCommit.__qualname__] = UnresolvedCommit
from demiflow.data.runtime_contract import RuntimeContractError
_KNOWN[RuntimeContractError.__module__ + "." + RuntimeContractError.__qualname__] = RuntimeContractError
from demiflow.operator_llm.errors import PromptBudgetExceededError, PromptReplayMissError
_KNOWN[PromptBudgetExceededError.__module__ + '.' + PromptBudgetExceededError.__qualname__] = PromptBudgetExceededError
_KNOWN[PromptReplayMissError.__module__ + '.' + PromptReplayMissError.__qualname__] = PromptReplayMissError
from demiflow.operator_llm.stream_recovery import _CapacityPending
# Preserve the original non-admission signal so the existing async client
# releases its journal slot and retries; never block the driver response lane.
_KNOWN[_CapacityPending.__module__ + '.' + _CapacityPending.__qualname__] = _CapacityPending
from demiflow.operator_llm.journal import UncertainPromptCall
from demiflow.embeddings.payload import EmbeddingProtocolError
_KNOWN.update({cls.__module__ + '.' + cls.__qualname__: cls
               for cls in (UncertainPromptCall, EmbeddingProtocolError)})
from demiflow.services.request_limits import ServiceStopped
_KNOWN[ServiceStopped.__module__ + '.' + ServiceStopped.__qualname__] = ServiceStopped
_HTTP_TYPE = 'httpx.HTTPStatusError'


def _os_error_fields(value, *, decode=False):
    """Closed, bounded OS facts; never deserialize arbitrary exception args."""
    if (not isinstance(value, Mapping) or not {'errno', 'strerror'} <= set(value)
            or set(value) - {'errno', 'strerror', 'filename', 'filename2'}
            or type(value['errno']) is not int or not -(1 << 31) <= value['errno'] < (1 << 31)
            or value['strerror'] is not None and (
                type(value['strerror']) is not str or len(value['strerror']) > 2048)):
        raise DataflowError('INVALID_CONTRACT', 'Invalid bounded OS failure fields')
    fields = dict(value)
    for key in ('filename', 'filename2'):
        if key not in fields:
            continue
        path = fields[key]
        if path is None or type(path) is str and len(path) <= 4096:
            continue
        if (isinstance(path, Mapping) and set(path) == {'bytes_hex'}
                and type(path['bytes_hex']) is str and len(path['bytes_hex']) <= 8192
                and len(path['bytes_hex']) % 2 == 0
                and re.fullmatch(r'[0-9a-f]*', path['bytes_hex'])):
            if decode:
                fields[key] = bytes.fromhex(path['bytes_hex'])
            continue
        raise DataflowError('INVALID_CONTRACT', 'Invalid bounded OS failure filename')
    return fields


def _os_error_payload(exc):
    number = exc.errno
    if type(number) is not int or not -(1 << 31) <= number < (1 << 31):
        return None  # Preserve the legacy message-only OSError surface.
    text = exc.strerror
    fields = {'errno': number, 'strerror': text[:2048] if type(text) is str else None}
    for key in ('filename', 'filename2'):
        path = getattr(exc, key, None)
        if type(path) is str and len(path) <= 4096:
            fields[key] = path
        elif type(path) is bytes and len(path) <= 4096:
            fields[key] = {'bytes_hex': path.hex()}
        # Oversized/non-data optional fields stay only in bounded diagnostics;
        # they cannot turn exception transport into arbitrary object loading.
    return fields


def _http_status_fields(value, *, redact=False):
    from urllib.parse import urlsplit, urlunsplit
    if (not isinstance(value, Mapping) or set(value) != {'method', 'url', 'status'}
            or type(value['method']) is not str or not re.fullmatch(r'[A-Z]{1,16}', value['method'])
            or type(value['status']) is not int or not 100 <= value['status'] <= 599
            or type(value['url']) is not str or len(value['url']) > 4096):
        raise DataflowError('INVALID_CONTRACT', 'Invalid bounded HTTP failure fields')
    try:
        parts = urlsplit(value['url'])
        if parts.scheme not in {'http', 'https'} or not parts.hostname:
            raise ValueError('HTTP URL required')
        host = '[' + parts.hostname + ']' if ':' in parts.hostname else parts.hostname
        if parts.port is not None:
            host += ':' + str(parts.port)
        safe = urlunsplit((parts.scheme, host, parts.path, '', ''))
        if not redact and safe != value['url']:
            raise ValueError('HTTP failure URL contains nonpublic components')
    except (ValueError, UnicodeError) as error:
        raise DataflowError('INVALID_CONTRACT', 'Invalid public HTTP failure URL') from error
    return {'method': value['method'], 'url': safe, 'status': value['status']}


def failure_payload(exc, *, _depth=0, _budget=None):
    _budget = [32] if _budget is None else _budget
    _budget[0] -= 1
    name = type(exc).__module__ + '.' + type(exc).__qualname__
    message = (exc._message if isinstance(exc, DataflowError) else str(exc)) or type(exc).__name__
    payload = {'code': getattr(exc, 'code', 'INTERNAL_PROTOCOL'),
            'error_type': name if name in _KNOWN or name in {_RECOVERY_TYPE, _UNRESOLVED_TYPE, _BUDGET_TYPE} else None,
            'message': message[:2048], 'traceback': traceback.format_exc()[-8000:]}
    if type(exc) in _OS_ERRORS:
        fields = _os_error_payload(exc)
        if fields is not None:
            payload['os_error'] = fields
    if name == _HTTP_TYPE:
        fields = _http_status_fields({'method': exc.request.method, 'url': str(exc.request.url),
                                     'status': exc.response.status_code}, redact=True)
        # Exception text/traceback can contain headers, credentials or provider
        # body excerpts. HTTP diagnostics carry only the whitelisted public fact.
        payload.update(error_type=_HTTP_TYPE, http=fields, traceback='',
                       message=f"HTTP {fields['status']} for {fields['method']} {fields['url']}")
    if isinstance(exc, UncertainPromptCall):
        payload['code'] = 'UNKNOWN_EFFECT'
    if isinstance(exc, UnresolvedCommit):
        payload.update(code='UNKNOWN_EFFECT', error_type=_UNRESOLVED_TYPE)
    if type(exc) is ExecutionBudgetExceeded:
        try:
            budget = _budget_name(exc.budget)
        except DataflowError:
            # Error reporting must still return a control reply. Invalid local
            # exception fields do not become a definite budget rejection.
            payload.update(code='INVALID_CONTRACT', error_type=None,
                           message='Invalid execution budget failure', traceback='')
        else:
            payload.update(code='BUDGET_EXCEEDED', budget=budget)
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
        def contains_http(value):
            return value.get('error_type') == _HTTP_TYPE or any(
                contains_http(child) for child in value.get('children', ()))
        if any(contains_http(child) for child in children):
            payload.update(message='Worker exception group with HTTP status failure', traceback='')
        if len(children) != len(exc.exceptions):
            payload['message'] += ' [additional cleanup errors truncated by control budget]'
    if type(exc) is HistoryReadLimit:
        from demiflow.execution.recovery_contract import document
        import json
        payload['error_type'] = 'demiflow.execution.queue_contract.HistoryReadLimit'
        payload['next_cursor'] = json.loads(document(exc.next_cursor, 4096))
    return payload


def _budget_name(value):
    from demiflow.execution.recovery_contract import identifier
    try:
        identifier(value)  # The original journal admits per-node names with '-'.
    except RecoveryContractError as exc:
        raise DataflowError('INVALID_CONTRACT', 'execution budget name must be a bounded identifier') from exc
    return value


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
        from demiflow.execution.recovery_contract import document
        import json
        error = HistoryReadLimit(message, json.loads(document(payload['next_cursor'], 4096)))
    elif payload.get('error_type') == _RECOVERY_TYPE:
        error = DataflowRecoveryError(code if code in DATAFLOW_ERROR_CODES else 'UNKNOWN_EFFECT', message)
    elif payload.get('error_type') == _UNRESOLVED_TYPE:
        if code != 'UNKNOWN_EFFECT':
            raise DataflowError('INVALID_CONTRACT', 'unresolved publication must retain its unknown-effect classification')
        error = DataflowUnresolvedCommit(message)
    elif payload.get('error_type') == _BUDGET_TYPE:
        if code != 'BUDGET_EXCEEDED':
            raise DataflowError('INVALID_CONTRACT', 'unknown failure cannot be promoted to a definite budget rejection')
        error = ExecutionBudgetExceeded(_budget_name(payload.get('budget')), message=message)
    elif payload.get('error_type') == _HTTP_TYPE:
        import httpx
        fields = _http_status_fields(payload.get('http'))
        request = httpx.Request(fields['method'], fields['url'])
        # Ignore remote text/traceback for this type even if a sender forges it.
        message = f"HTTP {fields['status']} for {fields['method']} {fields['url']}"
        error = httpx.HTTPStatusError(message, request=request,
                                    response=httpx.Response(fields['status'], request=request))
    elif cls is UncertainPromptCall:
        if code != 'UNKNOWN_EFFECT':
            raise DataflowError('INVALID_CONTRACT', 'Uncertain request must retain UNKNOWN_EFFECT')
        error = cls(message)
    elif cls is UnresolvedCommit:
        # Compatibility with an older host's fixed legacy exception tag.
        error = DataflowUnresolvedCommit(message)
    elif cls in _OS_ERRORS and 'os_error' in payload:
        fields = _os_error_fields(payload['os_error'], decode=True)
        error = cls(fields['errno'], fields['strerror'])
        for key in ('filename', 'filename2'):
            if key in fields:
                setattr(error, key, fields[key])
    elif cls is not None:
        error = cls(message)
    else:
        error = DataflowError(code if code in DATAFLOW_ERROR_CODES else 'INTERNAL_PROTOCOL', message)
    if payload.get('traceback') and payload.get('error_type') != _HTTP_TYPE:
        error.add_note(str(payload['traceback'])[-8000:])
    return error
