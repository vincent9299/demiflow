"""Logical callback-task state; no live callable is stored in a checkpoint."""
from __future__ import annotations
from demiflow.execution.compiler_support.operator_policy import _resident_slots, function_fields


import inspect
import json


from demiflow.execution.contracts.dataflow_errors import DataflowError

from demiflow.execution.compiler_support.operator_policy import _json_fields


def snapshot_function(function, *, max_bytes, occurrence=None):
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
        from demiflow.execution.contracts.state_codec import size_error
        raise size_error('task closure state exceeds max_state_bytes', max_bytes, actual=len(raw), occurrence=occurrence)
    return {'mode': 'task_function', 'value': json.loads(raw)}


def restore_function(function, state, *, max_bytes, occurrence=None):
    expected = snapshot_function(function, max_bytes=max_bytes, occurrence=occurrence)
    fields = state.get('value')
    if expected['mode'] != 'task_function' or type(fields) is not dict or set(fields) != set(expected['value']):
        raise DataflowError('INCOMPATIBLE_HISTORY', 'task closure state shape changed')
    valid, reason = _json_fields(fields, max_bytes)
    if not valid:
        raise DataflowError('CORRUPT_ARTIFACT', 'invalid task closure state: ' + str(reason))
    raw = json.dumps(fields, separators=(',', ':'), ensure_ascii=False, allow_nan=False).encode()
    if len(raw) > max_bytes:
        from demiflow.execution.contracts.state_codec import size_error
        raise size_error('task closure state exceeds max_state_bytes', max_bytes, actual=len(raw), occurrence=occurrence)
    fields = json.loads(raw)
    for name, cell in zip(function.__code__.co_freevars, function.__closure__ or ()):
        if 'cell:' + name in fields:
            cell.cell_contents = fields['cell:' + name]
    function.__defaults__ = tuple(fields['defaults']) if fields.get('defaults') is not None else None
    function.__kwdefaults__ = fields.get('kwdefaults')
    function.__dict__.clear()
    function.__dict__.update(fields.get('attributes', {}))


def measurable_task_fields(target, args, kwargs, occurrence):
    """Borrow the current task-owned plain data, never reconstruct an instance."""
    if inspect.isfunction(target):
        fields, _, reason = _resident_slots(target, call_args=args, call_kwargs=kwargs)
    else:
        fields = vars(target)
        from demiflow.execution.contracts.recovery_contract import MAX_DOCUMENT_BYTES
        _, reason = _json_fields(fields, MAX_DOCUMENT_BYTES)
    if reason is not None:
        raise DataflowError('RESOURCE_INFEASIBLE',
            f'task state cannot be measured inside the stage grant: {reason} (occurrence_id={occurrence})',
            context={'occurrence_id': occurrence})
    return fields


def _copy_resident_fields(fields, max_bytes, occurrence):
    from demiflow.data.runtime_contract import check_size, RuntimeContractError
    from demiflow.execution.contracts.state_codec import size_error
    try:
        check_size(fields, max_bytes)
    except RuntimeContractError as exc:
        raise size_error(f'function state rejected before encoding: {exc}', max_bytes, occurrence=occurrence) from exc
    raw = json.dumps(fields, separators=(',', ':'), ensure_ascii=False, allow_nan=False).encode()
    if len(raw) > max_bytes:
        raise size_error('function state exceeds max_state_bytes', max_bytes, actual=len(raw), occurrence=occurrence)
    return json.loads(raw)


def snapshot_resident_function(function, *, max_bytes, call_args=(), call_kwargs=None, occurrence=None):
    fields, _, reason = _resident_slots(function, call_args=call_args, call_kwargs=call_kwargs)
    if reason:
        return {'mode': 'nonrecoverable', 'reason': reason}
    if not fields:
        return {'mode': 'function_slots_v1', 'value': {}}
    return {'mode': 'function_slots_v1', 'value': _copy_resident_fields(fields, max_bytes, occurrence)}


def restore_resident_function(function, state, *, max_bytes, call_args=(), call_kwargs=None, occurrence=None):
    expected, bindings, reason = _resident_slots(function, call_args=call_args, call_kwargs=call_kwargs)
    fields = state.get('value')
    if (reason or state.get('mode') != 'function_slots_v1' or type(fields) is not dict
            or fields.keys() != expected.keys()):
        raise DataflowError('INCOMPATIBLE_HISTORY', 'function state slots differ from the sealed declaration')
    valid, reason = _json_fields(fields, max_bytes)
    if not valid:
        raise DataflowError('CORRUPT_ARTIFACT', 'invalid function state: ' + str(reason))
    detached = _copy_resident_fields(fields, max_bytes, occurrence)
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


def clone_resident_callable(target, *, max_bytes, call_args=(), call_kwargs=None, occurrence=None):
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
    state = snapshot_resident_function(target, max_bytes=max_bytes, call_args=call_args, call_kwargs=call_kwargs,
                                       occurrence=occurrence)
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
    from demiflow.execution.seal.business_sources import code_global_names
    names = code_global_names(function.__code__)
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
