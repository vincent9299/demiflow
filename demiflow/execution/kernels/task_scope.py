"""Logical callback-task state; no live callable is stored in a checkpoint."""
from __future__ import annotations

import dis
import inspect
import json
from pathlib import PurePath, PurePosixPath, PureWindowsPath, PosixPath, WindowsPath
from types import ModuleType

from ..contracts.dataflow_errors import DataflowError
from ..operator_policy import _json_fields

TASK_KINDS = frozenset({'MapOp', 'BoundMapOp', 'FlatMapOp', 'FilterOp', 'AddColumnOp'})


def function_fields(function):
    """Describe only direct function-owned data, without invoking user code.

    Mutable module globals and nested callable state do not belong to this task.
    They require an explicit state owner; claiming they restore would be false.
    """
    if not inspect.isfunction(function):
        return {}, None
    result = {}
    for name, cell in zip(function.__code__.co_freevars, function.__closure__ or ()):
        try:
            value = cell.cell_contents
        except ValueError:
            continue
        result['cell:' + name] = value
    result['defaults'] = function.__defaults__
    result['kwdefaults'] = function.__kwdefaults__
    result['attributes'] = function.__dict__
    # A default tuple is a declaration wrapper, not a JSON business value.
    result['defaults'] = list(result['defaults']) if result['defaults'] is not None else None
    immutable = (str, bytes, int, float, bool, type(None), PurePath, PurePosixPath, PureWindowsPath, PosixPath, WindowsPath)
    instructions = tuple(dis.get_instructions(function))
    written_cells = {'cell:' + item.argval for item in instructions
                     if item.opname in {'STORE_DEREF', 'DELETE_DEREF'}}
    fields = {}
    for name, value in result.items():
        valid, _ = _json_fields(value, 1 << 30)
        if valid:
            fields[name] = value
        elif name in written_cells:
            return {}, 'task function rebinds a non-JSON closure cell: ' + name
        elif type(value) in immutable or inspect.isclass(value) or type(value) is ModuleType:
            continue  # Sealed immutable dependency, never overwritten by restore.
        elif inspect.isfunction(value) and not value.__closure__ and not value.__defaults__ and not value.__dict__:
            continue
        else:
            return {}, 'task function captures state without a data-only adapter: ' + name
    names = {instruction.argval for instruction in instructions
             if instruction.opname in {'LOAD_GLOBAL', 'STORE_GLOBAL', 'DELETE_GLOBAL'}}
    for name in names:
        value = function.__globals__.get(name)
        if type(value) in (dict, list, set) or any(instruction.opname in {'STORE_GLOBAL', 'DELETE_GLOBAL'}
                and instruction.argval == name for instruction in instructions):
            return {}, 'task function uses mutable module-global state: ' + name
    return fields, None


def function_has_state(function):
    if not inspect.isfunction(function):
        return False
    fields, reason = function_fields(function)
    return bool(reason or function.__closure__ or function.__defaults__ or function.__kwdefaults__ or function.__dict__)


def snapshot_function(function, *, max_bytes):
    fields, reason = function_fields(function)
    if reason:
        return {'mode': 'nonrecoverable', 'reason': reason}
    # JSON cannot preserve aliases/cycles. Reject them explicitly, never silently
    # change an identity-sensitive closure while restoring its numerical values.
    seen = set()
    pending = list(fields.values())
    while pending:
        value = pending.pop()
        if type(value) in (dict, list):
            if id(value) in seen:
                return {'mode': 'nonrecoverable', 'reason': 'task closure has aliased mutable state; declare a state adapter'}
            seen.add(id(value))
            pending.extend(value.values() if type(value) is dict else value)
    valid, reason = _json_fields(fields, max_bytes)
    if not valid:
        return {'mode': 'nonrecoverable', 'reason': reason}
    raw = json.dumps(fields, separators=(',', ':'), ensure_ascii=False, allow_nan=False).encode()
    if len(raw) > max_bytes:
        raise DataflowError('BUDGET_EXCEEDED', 'task closure state exceeds max_state_bytes')
    return {'mode': 'task_function', 'value': json.loads(raw)}


def restore_function(function, state, *, max_bytes):
    expected = snapshot_function(function, max_bytes=max_bytes)
    fields = state.get('value')
    if expected['mode'] != 'task_function' or type(fields) is not dict or set(fields) != set(expected['value']):
        raise DataflowError('INCOMPATIBLE_HISTORY', 'task closure state shape changed')
    valid, reason = _json_fields(fields, max_bytes)
    if not valid:
        raise DataflowError('CORRUPT_ARTIFACT', 'invalid task closure state: ' + str(reason))
    raw = json.dumps(fields, separators=(',', ':'), ensure_ascii=False, allow_nan=False).encode()
    if len(raw) > max_bytes:
        raise DataflowError('BUDGET_EXCEEDED', 'task closure state exceeds max_state_bytes')
    fields = json.loads(raw)
    for name, cell in zip(function.__code__.co_freevars, function.__closure__ or ()):
        if 'cell:' + name in fields:
            cell.cell_contents = fields['cell:' + name]
    function.__defaults__ = tuple(fields['defaults']) if fields.get('defaults') is not None else None
    function.__kwdefaults__ = fields.get('kwdefaults')
    function.__dict__.clear()
    function.__dict__.update(fields.get('attributes', {}))
