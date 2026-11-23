"""C04 unified operator state policy: classification and bounded auto_json.

One policy function shared by caller precheck, runtime registration, semantic
identity, OperatorSpec compilation and trait derivation, so no consumer guesses
state ownership independently. ``auto_json`` is the platform's adaptation for
plain ``__dict__`` data instances, not a second journal: the data still flows
through OperatorStates -> StateArtifacts -> the same commit. A hookless instance
is never treated as stateless; a class is a factory blueprint and is never
constructed here.
"""
from __future__ import annotations

import dis
from pathlib import PurePath, PurePosixPath, PureWindowsPath, PosixPath, WindowsPath
from types import FunctionType, ModuleType

import inspect

import math
import types
from dataclasses import dataclass
from typing import Literal

from demiflow.execution.recovery_contract import RecoveryContractError, MAX_DOCUMENT_BYTES


AUTO_STATE_SCHEMA = "demiflow.auto_state.v1"
_OWNERSHIP = ("none", "partitionable", "shared_colocated")
_PHASES = ("declaration", "instance")
# Built-in object-protocol descriptors are not user state; only genuine custom
# data descriptors, properties and slots force a native adapter.
_PROTOCOL_DESCRIPTORS = (
    types.WrapperDescriptorType, types.MethodWrapperType, types.MethodDescriptorType,
    types.BuiltinFunctionType, types.BuiltinMethodType, types.GetSetDescriptorType,
)


@dataclass(frozen=True)
class StatePolicy:
    mode: Literal["function", "factory", "hooks", "auto_json", "unsupported"]
    ownership: Literal["none", "partitionable", "shared_colocated"]
    adapter_present: bool
    reason: str | None


def _resolved_ownership(mode, ownership):
    # A function carries no cross-row business state. Anything else whose caller
    # has not declared ownership defaults to shared_colocated: unknown is never
    # silently downgraded to stateless.
    if mode == "function":
        return "none"
    return "shared_colocated" if ownership == "none" else ownership


def _json_fields(value, maximum):
    """Bounded, type-only strict-JSON admission. Executes no user code.

    Tuples/sets/bytes/custom objects are rejected: auto_json must round-trip
    losslessly through JSON, which cannot preserve those types.
    """
    pending = [(value, 0)]
    nodes = 0
    size = 0
    while pending:
        item, depth = pending.pop()
        nodes += 1
        if nodes > 65536 or depth > 32:
            return False, "auto_json state exceeds structural budget"
        kind = type(item)
        if kind is dict:
            if len(item) > 16384 or any(type(k) is not str for k in item):
                return False, "auto_json state keys must be bounded strings"
            pending.extend((v, depth + 1) for v in item.values())
        elif kind is list:
            if len(item) > 16384:
                return False, "auto_json state collection exceeds budget"
            pending.extend((v, depth + 1) for v in item)
        elif kind is float:
            if not math.isfinite(item):
                return False, "auto_json state must be finite"
        elif kind is str:
            size += len(item)
            if size > maximum:
                return False, "auto_json state exceeds byte budget"
        elif kind not in (int, bool, type(None)):
            return False, "auto_json state must be strict JSON, not " + kind.__name__
    return True, None


def _auto_json_eligible(owner):
    """Decide plain-data adaptability from type structure, without probing code."""
    cls = type(owner)
    if cls.__getattribute__ is not object.__getattribute__:
        return False, "custom __getattribute__ requires a native state adapter"
    if not any(isinstance(vars(k).get("__dict__"), types.GetSetDescriptorType)
               for k in cls.__mro__):
        return False, "auto_json requires a plain __dict__ instance; slots are unsupported"
    for klass in cls.__mro__:
        for name, attr in vars(klass).items():
            if name in ("__dict__", "__weakref__"):
                continue
            if isinstance(attr, property):
                return False, "property descriptors require a native state adapter"
            if isinstance(attr, types.MemberDescriptorType):
                return False, "slots require a native state adapter"
            if isinstance(attr, _PROTOCOL_DESCRIPTORS) or inspect.isfunction(attr) \
                    or isinstance(attr, (classmethod, staticmethod)):
                continue
            attr_type = type(attr)
            if hasattr(attr_type, "__set__") or hasattr(attr_type, "__delete__"):
                return False, "custom data descriptors require a native state adapter"
    return _json_fields(vars(owner), MAX_DOCUMENT_BYTES)


def classify_operator_state(target, *, phase, ownership="none"):
    """Return the single C04 StatePolicy for a function, factory or instance."""
    if phase not in _PHASES:
        raise RecoveryContractError("unknown state classification phase: " + str(phase))
    if ownership not in _OWNERSHIP:
        raise RecoveryContractError("unknown state ownership: " + str(ownership))
    if inspect.isfunction(target) or inspect.isbuiltin(target):
        return StatePolicy("function", "none", False, None)
    if inspect.isclass(target):
        # Factory blueprint: the platform's deferred adaptation is available, but
        # the real instance is still verified after construction. Never built here.
        return StatePolicy("factory", _resolved_ownership("factory", ownership), True, None)
    owner = target.__self__ if inspect.ismethod(target) else target
    snapshot = callable(getattr(owner, "snapshot_state", None))
    restore = callable(getattr(owner, "restore_state", None))
    if snapshot and restore:
        return StatePolicy("hooks", _resolved_ownership("hooks", ownership), True, None)
    if snapshot != restore:
        return StatePolicy("unsupported", ownership, False,
                           "half-declared state adapter: exactly one of snapshot_state/restore_state")
    eligible, reason = _auto_json_eligible(owner)
    if not eligible:
        return StatePolicy("unsupported", ownership, False, reason)
    if ownership == "partitionable":
        return StatePolicy("unsupported", ownership, False,
                           "auto_json state is shared_colocated; partitionable needs a native adapter")
    return StatePolicy("auto_json", _resolved_ownership("auto_json", ownership), True, None)


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

def _resident_slots(target, *, call_args=(), call_kwargs=None, task_private=False):
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
                           PurePosixPath, PureWindowsPath, PosixPath, WindowsPath):
            return True
        if type(value) is types.ModuleType or inspect.isclass(value):
            return not task_private
        if inspect.isbuiltin(value):
            return value.__self__ is None or type(value.__self__) is types.ModuleType
        if inspect.isfunction(value):
            # Immutable default values are part of a sealed declaration, e.g.
            # task_message(..., pool='default'). They do not make its caller
            # own mutable helper state. Mutable defaults/closures/attributes
            # still need an explicit owner and cannot silently be restored.
            # A helper can own further globals. Rehydrate its sealed graph for
            # a private task, including any aliases inside those dependencies.
            return (not task_private and not value.__closure__ and not value.__dict__
                    and all(dependency(item, depth + 1) for item in value.__defaults__ or ())
                    and all(dependency(item, depth + 1) for item in (value.__kwdefaults__ or {}).values()))
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

def task_uses_function_slots(declaration):
    """Can this task share its program and isolate state through declared slots?

    A helper or class can itself own mutable globals or defaults. Reconstruct
    opaque dependency graphs from the sealed payload to preserve their aliases.
    """
    from demiflow.data.plan import CallableSpec
    if type(declaration) is not CallableSpec or type(declaration.target) is not FunctionType:
        return False
    _, _, reason = _resident_slots(declaration.target, call_args=declaration.call_args,
                                  call_kwargs=declaration.call_kwargs, task_private=True)
    return reason is None

def owned_factory_declaration(spec):
    """A class blueprint is durable only when it borrows no mutable state.

    Reuse the existing passive function-state policy for methods and constructor
    arguments. A serializable external list is still shared caller state; pickle
    success alone cannot justify replacing it with a disconnected copy.
    """
    import types
    if spec is None or not spec.is_class:
        return False
    immutable = (str, bytes, int, float, bool, type(None))
    classes = type.__getattribute__(spec.target, '__mro__')
    if len(classes) > 32:
        return False
    visited = 0
    for cls in classes:
        if cls is object:
            continue
        fields = type.__getattribute__(cls, '__dict__')
        visited += len(fields)
        if visited > 1024:
            return False
        for name, value in fields.items():
            if name in {'__dict__', '__weakref__', '__module__', '__doc__', '__qualname__'}:
                continue
            if type(value) in (classmethod, staticmethod):
                value = value.__func__
            if type(value) is types.FunctionType:
                args, kwargs = (spec.constructor_args, spec.constructor_kwargs) if name in {'__new__', '__init__'} else (
                    (spec.call_args, spec.call_kwargs) if name == '__call__' else ((), {}))
                if function_residency_kind(value, call_args=args, call_kwargs=kwargs) != 'none':
                    return False
            elif type(value) not in immutable:
                return False
    return True

def task_has_measurable_state(declaration):
    """Whether a private task can account its own plain data without callbacks."""
    from demiflow.data.plan import CallableSpec
    if type(declaration) is not CallableSpec:
        return False
    target = declaration.target
    if type(target) is FunctionType:
        return task_uses_function_slots(declaration)
    if (type(target) is not type
            or inspect.getattr_static(target, '__getattribute__') is not object.__getattribute__
            or inspect.getattr_static(target, '__new__') is not object.__new__
            or any(inspect.getattr_static(target, name, None) is not None for name in
                   ('snapshot_state', 'restore_state', '__demiflow_snapshot__', '__demiflow_restore__',
                    '__slots__', '__getattr__'))):
        return False
    return owned_factory_declaration(declaration)
