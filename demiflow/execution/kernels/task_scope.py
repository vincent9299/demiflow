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


def _resident_slots(target, *, call_args=(), call_kwargs=None):
    """Owned function data, with declaration dependencies left in their slots.

    This is deliberately narrower than serializing a callable: no module state,
    arbitrary object, shared mutable alias, or user hook enters a checkpoint.
    """
    import functools
    import types
    slots, bindings, seen = {}, {}, set()
    call_kwargs = {} if call_kwargs is None else call_kwargs
    partials = []
    function = target
    if (type(function) is types.MethodWrapperType and function.__name__ == '__call__'
            and type(function.__self__) is functools.partial):
        function = function.__self__
    while type(function) is functools.partial:
        partials.append(function)
        function = function.func
        if len(partials) > 32:
            return {}, {}, 'function partial nesting exceeds structural budget'
    if inspect.isbuiltin(function) and not partials and not call_args and not call_kwargs:
        owner = function.__self__
        return ({}, {}, None) if owner is None or type(owner) is types.ModuleType else (
            {}, {}, 'bound builtin owns unadapted external state')
    if not inspect.isfunction(function):
        return {}, {}, 'function state requires a plain function declaration'
    if (function.__defaults__ is not None and type(function.__defaults__) is not tuple
            or function.__kwdefaults__ is not None and type(function.__kwdefaults__) is not dict
            or type(call_args) is not tuple or type(call_kwargs) is not dict):
        return {}, {}, 'function binding containers must be plain tuple/dict'

    def dependency(value, depth=0):
        if depth > 16:
            return False
        if type(value) in (str, bytes, int, float, bool, type(None), PurePath,
                           PurePosixPath, PureWindowsPath, PosixPath, WindowsPath, types.ModuleType):
            return True
        if inspect.isclass(value):
            return True
        if inspect.isbuiltin(value):
            return value.__self__ is None or type(value.__self__) is types.ModuleType
        if inspect.isfunction(value):
            return not value.__closure__ and not value.__defaults__ and not value.__kwdefaults__ and not value.__dict__
        if type(value) is tuple:
            return all(dependency(item, depth + 1) for item in value)
        return False

    def add(name, value, binding, *, written=False, attribute=False):
        if len(slots) >= 65536:
            return 'function state slots exceed structural budget'
        if value is function:
            return None  # The cloned function self-reference is rebound, not serialized.
        mutable = type(value) in (list, dict)
        if not (mutable or written or attribute):
            if dependency(value):
                return None
        valid, reason = _json_fields(value, 1 << 30)
        if not valid:
            if not written and dependency(value):
                return None
            return 'function slot has no data-only adapter: ' + name + ': ' + str(reason)
        if not (mutable or written or attribute):
            return None
        pending = [value]
        while pending:
            item = pending.pop()
            if type(item) in (list, dict):
                if id(item) in seen:
                    return 'function state has aliased mutable data; declare a state adapter'
                seen.add(id(item))
                pending.extend(item.values() if type(item) is dict else item)
        slots[name], bindings[name] = value, binding
        return None

    codes, instructions, seen_codes = [function.__code__], [], set()
    while codes:
        code = codes.pop()
        if id(code) in seen_codes:
            continue
        seen_codes.add(id(code))
        if len(seen_codes) > 1024:
            return {}, {}, 'function code exceeds structural budget'
        instructions.extend(dis.get_instructions(code))
        if len(instructions) > 65536:
            return {}, {}, 'function code exceeds structural budget'
        codes.extend(value for value in code.co_consts if type(value) is types.CodeType)
    written = {i.argval for i in instructions if i.opname in {'STORE_DEREF', 'DELETE_DEREF'}}
    for name, cell in zip(function.__code__.co_freevars, function.__closure__ or ()):
        try:
            value = cell.cell_contents
        except ValueError:
            if name in written:
                return {}, {}, 'function initializes an unbound state cell; declare a state adapter'
            continue
        reason = add('cell:' + name, value, ('cell', cell, None), written=name in written)
        if reason:
            return {}, {}, reason
    for index, value in enumerate(function.__defaults__ or ()):
        reason = add('default:' + str(index), value, ('default', function, index))
        if reason:
            return {}, {}, reason
    for name, value in (function.__kwdefaults__ or {}).items():
        reason = add('kwdefault:' + name, value, ('kwdefault', function, name))
        if reason:
            return {}, {}, reason
    for name, value in function.__dict__.items():
        reason = add('attribute:' + name, value, ('attribute', function, name), attribute=True)
        if reason:
            return {}, {}, reason
    for index, partial in enumerate(partials):
        for number, value in enumerate(partial.args):
            reason = add(f'partial:{index}:arg:{number}', value, ('partial_arg', partial, number))
            if reason:
                return {}, {}, reason
        for name, value in (partial.keywords or {}).items():
            reason = add(f'partial:{index}:keyword:{name}', value, ('partial_keyword', partial, name))
            if reason:
                return {}, {}, reason
        for name, value in partial.__dict__.items():
            reason = add(f'partial:{index}:attribute:{name}', value, ('attribute', partial, name), attribute=True)
            if reason:
                return {}, {}, reason
    for index, value in enumerate(call_args):
        reason = add('call:arg:'+str(index), value, ('call_arg', call_args, index))
        if reason:
            return {}, {}, reason
    for name, value in call_kwargs.items():
        reason = add('call:keyword:'+name, value, ('call_keyword', call_kwargs, name))
        if reason:
            return {}, {}, reason
    self_reference = any(function.__globals__.get(i.argval) is function
                         for i in instructions if i.opname == 'LOAD_GLOBAL')
    for cell in function.__closure__ or ():
        try:
            self_reference |= cell.cell_contents is function
        except ValueError:
            pass
    if self_reference and (any(i.opname in {'STORE_ATTR', 'DELETE_ATTR'} and i.argval not in function.__dict__
                               for i in instructions)
            or any(i.opname == 'LOAD_GLOBAL' and i.argval in {'setattr','delattr','vars'} for i in instructions)
            or any(i.opname == 'LOAD_ATTR' and i.argval in {'__globals__','__dict__','__defaults__','__kwdefaults__','__closure__'}
                   for i in instructions)):
        return {}, {}, 'function dynamically changes undeclared attribute slots; declare a state adapter'
    for instruction in instructions:
        if instruction.opname not in {'LOAD_GLOBAL', 'STORE_GLOBAL', 'DELETE_GLOBAL'}:
            continue
        value = function.__globals__.get(instruction.argval)
        if instruction.argval in {'globals', 'eval', 'exec'}:
            return {}, {}, 'dynamic global state needs an explicit adapter'
        if instruction.opname != 'LOAD_GLOBAL' or (value is not function and not dependency(value)):
            return {}, {}, 'function uses external mutable or unadapted global: ' + instruction.argval
    return slots, bindings, None


def function_residency_kind(function, *, call_args=(), call_kwargs=None):
    fields, _, reason = _resident_slots(function, call_args=call_args, call_kwargs=call_kwargs)
    return 'nonrecoverable' if reason else 'adapter' if fields else 'none'


def snapshot_resident_function(function, *, max_bytes, call_args=(), call_kwargs=None):
    fields, _, reason = _resident_slots(function, call_args=call_args, call_kwargs=call_kwargs)
    if reason:
        return {'mode': 'nonrecoverable', 'reason': reason}
    from ...data.runtime_contract import check_size, RuntimeContractError
    try:
        check_size(fields, max_bytes)
    except RuntimeContractError as exc:
        raise DataflowError('BUDGET_EXCEEDED', 'function state exceeds max_state_bytes') from exc
    raw = json.dumps(fields, separators=(',', ':'), ensure_ascii=False, allow_nan=False).encode()
    if len(raw) > max_bytes:
        raise DataflowError('BUDGET_EXCEEDED', 'function state exceeds max_state_bytes')
    return {'mode': 'function_slots_v1', 'value': json.loads(raw)}


def restore_resident_function(function, state, *, max_bytes, call_args=(), call_kwargs=None):
    expected, bindings, reason = _resident_slots(function, call_args=call_args, call_kwargs=call_kwargs)
    fields = state.get('value')
    if (reason or state.get('mode') != 'function_slots_v1' or type(fields) is not dict
            or fields.keys() != expected.keys()):
        raise DataflowError('INCOMPATIBLE_HISTORY', 'function state slots differ from the sealed declaration')
    valid, reason = _json_fields(fields, max_bytes)
    if not valid:
        raise DataflowError('CORRUPT_ARTIFACT', 'invalid function state: ' + str(reason))
    from ...data.runtime_contract import check_size, RuntimeContractError
    try:
        check_size(fields, max_bytes)
    except RuntimeContractError as exc:
        raise DataflowError('BUDGET_EXCEEDED', 'function state exceeds max_state_bytes') from exc
    raw = json.dumps(fields, separators=(',', ':'), ensure_ascii=False, allow_nan=False).encode()
    if len(raw) > max_bytes:
        raise DataflowError('BUDGET_EXCEEDED', 'function state exceeds max_state_bytes')
    detached = json.loads(raw)
    # Read-only partial.args cannot be replaced: only its owned mutable value
    # may change. Validate these slots before changing ANY callable state.
    for name, (kind, owner, key) in bindings.items():
        current = owner.args[key] if kind == 'partial_arg' else owner[key] if kind == 'call_arg' else None
        if kind in ('partial_arg','call_arg') and type(detached[name]) is not type(current):
            raise DataflowError('INCOMPATIBLE_HISTORY', 'partial argument state kind changed')
    for name, value in detached.items():
        kind, owner, key = bindings[name]
        if kind == 'cell':
            owner.cell_contents = value
        elif kind == 'default':
            defaults = list(owner.__defaults__)
            defaults[key] = value
            owner.__defaults__ = tuple(defaults)
        elif kind == 'kwdefault':
            owner.__kwdefaults__[key] = value
        elif kind == 'attribute':
            owner.__dict__[key] = value
        elif kind == 'partial_keyword':
            owner.keywords[key] = value
        elif kind == 'call_keyword':
            owner[key] = value
        elif kind in ('partial_arg','call_arg'):
            current = owner.args[key] if kind == 'partial_arg' else owner[key]
            if type(current) is list:
                current[:] = value
            else:
                current.clear()
                current.update(value)


def clone_resident_callable(target, *, max_bytes, call_args=(), call_kwargs=None):
    """Create one state owner per kernel/slot without changing a sealed program.

    Only admitted data slots are copied. Code and readonly declared dependencies
    are reused, and no arbitrary object's deepcopy or pickle hook is executed.
    """
    import functools
    import types
    wrapper = (type(target) is types.MethodWrapperType and target.__name__ == '__call__'
               and type(target.__self__) is functools.partial)
    function = target.__self__ if wrapper else target
    partials = []
    while type(function) is functools.partial:
        partials.append(function)
        function = function.func
    call_kwargs = {} if call_kwargs is None else call_kwargs
    state = snapshot_resident_function(target, max_bytes=max_bytes, call_args=call_args, call_kwargs=call_kwargs)
    if state['mode'] != 'function_slots_v1':
        raise DataflowError('INCOMPATIBLE_HISTORY', 'function has no data-only residency adapter')
    values = state['value']
    def selected(key, original):
        return values[key] if key in values else original
    def clone_cell(name, cell):
        try:
            value = cell.cell_contents
        except ValueError:
            return types.CellType()  # Preserve an unused unbound lexical cell.
        return types.CellType(selected('cell:'+name, value))
    cells = tuple(clone_cell(name, cell)
                  for name, cell in zip(function.__code__.co_freevars, function.__closure__ or ()))
    defaults = (tuple(selected('default:'+str(i), value) for i, value in enumerate(function.__defaults__))
                if function.__defaults__ is not None else None)
    # Use the same by-value global-name closure as sealing. Self-recursion must
    # resolve to this owner; copying an entire module namespace is unbounded.
    from ..chain import _code_global_names
    names = _code_global_names(function.__code__)
    namespace = {name:function.__globals__[name] for name in names if name in function.__globals__}
    for name in ('__builtins__', '__name__', '__package__'):
        if name in function.__globals__:
            namespace[name] = function.__globals__[name]
    cloned = types.FunctionType(function.__code__, namespace, function.__name__, defaults, cells or None)
    cloned.__kwdefaults__ = (None if function.__kwdefaults__ is None else {
        name:selected('kwdefault:'+name, value) for name,value in function.__kwdefaults__.items()})
    cloned.__dict__.update({name:selected('attribute:'+name,value) for name,value in function.__dict__.items()})
    cloned.__annotations__ = dict(function.__annotations__)
    cloned.__module__, cloned.__qualname__, cloned.__doc__ = function.__module__, function.__qualname__, function.__doc__
    for name, value in tuple(namespace.items()):
        if value is function:
            namespace[name] = cloned
    for cell in cells:
        try:
            value = cell.cell_contents
        except ValueError:
            continue
        if value is function:
            cell.cell_contents = cloned
    result = cloned
    for index in reversed(range(len(partials))):
        original = partials[index]
        args = tuple(selected(f'partial:{index}:arg:{i}',value) for i,value in enumerate(original.args))
        kwargs = {name:selected(f'partial:{index}:keyword:{name}',value)
                  for name,value in original.keywords.items()}
        result = functools.partial(result,*args,**kwargs)
        result.__dict__.update({name:selected(f'partial:{index}:attribute:{name}',value)
                              for name,value in original.__dict__.items()})
    args = tuple(selected('call:arg:'+str(i), value) for i,value in enumerate(call_args))
    kwargs = {name:selected('call:keyword:'+name,value) for name,value in call_kwargs.items()}
    return (result.__call__ if wrapper else result), args, kwargs


def clone_resident_function(target, *, max_bytes):
    return clone_resident_callable(target, max_bytes=max_bytes)[0]
