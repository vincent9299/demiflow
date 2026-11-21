"""The v3 wire representation of SDK symbols affected by the D1/D2 import move.

A v3 function pin contains its *whole file* digest, even when its own code did
not change. For these audited source revisions only, encode the historical
file identity after verifying both the actual module binding and the live code
against the measured source. Business source hashes and program artifact
digests are verified independently. An unrecognised revision uses its own pin.
"""
import builtins
from dis import get_instructions
from functools import lru_cache
import sys
from types import CodeType, FunctionType

from .artifacts import code_record
from .recovery_contract import identity


# Current source SHA-256, v3 pre-move source SHA-256.
_REVISIONS = {
    'demiflow.data.dataset': (
        'dc1d00c71a1bef0c1b2db7d069298c3d2839c7094f69c7c1508a1dee5d6428f6',
        '1ba0e80f1ed0853948a97792dc65125fdaec17504bc3396b5f53ffb593da7e88'),
    'demiflow.data.sources': (
        '4c349450034860cbaaa81d7b88e39b5caeea6c9ea56849e031804007cd226e79',
        '3033f1f14141c9759d29679ee5275492bfbc83bcd41b51c7bf9962afd115387f'),
    'demiflow.execution.local_kernel': (
        '474ffe83ef57fcf0dd18d84dc2e03bf4a52d382d83090ae54a85e8543d17de7b',
        'e67e3703f03ea14f8bcefb803bafc3251072e78bc17a5c1f2df60fad2ca2f83f'),
    'demiflow.data.result_cache': (
        '2ac2ab41bb6a1b4eda601b8c06f78e7c0bc74caf638d9c54afe6360190a4912b',
        'dc6858f90a4ea48fb5c950027e405084d9f820ba26b3159592b38a9f42d46788'),
}
_RELATIONAL_METHODS = ('join', 'reduce_by_key', 'group_batches', 'exclude_keys', 'union')


@lru_cache(maxsize=len(_REVISIONS))
def _declared_codes(source):
    # Compile only measured, allowlisted platform source. No module is executed.
    module = compile(source, '<sdk-v3>', 'exec', dont_inherit=True)
    functions = {}
    for item in module.co_consts:
        if isinstance(item, CodeType):
            functions[item.co_qualname] = item
            for member in item.co_consts:
                if isinstance(member, CodeType):
                    functions[member.co_qualname] = member
    return functions


def _owners(module, qualname):
    names = qualname.split('.')
    owner = vars(module).get(names[0])
    if len(names) == 2 and isinstance(owner, type):
        owner = vars(owner).get(names[1])
    elif len(names) != 1:
        return ()
    if isinstance(owner, (staticmethod, classmethod)):
        owner = owner.__func__
    if isinstance(owner, property):
        return owner.fget, owner.fset, owner.fdel
    if type(owner) is FunctionType:
        return owner, vars(owner).get('__wrapped__')
    return ()


def _children(code):
    for item in code.co_consts:
        if isinstance(item, CodeType):
            yield item
            yield from _children(item)


def _matches_definition(fn, module, compiled, root_name, code):
    live_globals = fn.__globals__ is vars(module)
    # Cloudpickle restores SDK-created closures by value. Only a closed body
    # with no global names can use this route. Its original closure/defaults
    # remain independently bound by describe; no supplied global is trusted.
    restored = (not live_globals and not fn.__code__.co_names and fn.__code__.co_qualname == fn.__qualname__
                and fn.__builtins__ is vars(builtins)
                and set(fn.__globals__) == {'__name__', '__package__', '__file__', '__builtins__'}
                and fn.__globals__['__name__'] == module.__name__
                and fn.__globals__['__package__'] == module.__package__
                and fn.__globals__['__file__'] == module.__file__)
    for root in _owners(module, root_name):
        if type(root) is not FunctionType or root.__globals__ is not vars(module):
            continue
        if fn is root:
            return code == identity(code_record(compiled))
        if root_name == fn.__qualname__ or not (live_globals or restored):
            continue
        if identity(code_record(root.__code__)) != identity(code_record(compiled)):
            continue
        for child in _children(root.__code__):
            if child.co_qualname != fn.__qualname__:
                continue
            if live_globals and fn.__code__ is child:
                return True
            if restored and code == identity(code_record(child)):
                # Inspect the full code tree before accepting a restored scope.
                return not any(descendant.co_names for descendant in _children(child))
    return False


def _method_pin(fn, code):
    # v3 expresses this one declaration import by its original package. Every
    # other instruction, constant (including the relative level) and operand
    # still enters code_record. This works with the caller's Python optimizer.
    if (fn.__module__ != 'demiflow.data.dataset' or fn.__name__ not in _RELATIONAL_METHODS
            or fn.__qualname__ != 'Dataset.' + fn.__name__):
        return code
    instructions = tuple(get_instructions(fn))
    uses = [index for index, op in enumerate(instructions) if op.argval == 'data.sources']
    if len(uses) != 1 or fn.__code__.co_names.count('data.sources') != 1:
        return code
    index = uses[0]
    if (instructions[index].opname != 'IMPORT_NAME' or index + 1 == len(instructions)
            or instructions[index + 1].opname != 'IMPORT_FROM'
            or instructions[index + 1].argval != 'PlanInput'):
        return code
    names = tuple('execution.local_kernel' if name == 'data.sources' else name
                  for name in fn.__code__.co_names)
    return identity(code_record(fn.__code__.replace(co_names=names)))


def function_pin(fn, source, source_hash, code):
    revision = _REVISIONS.get(fn.__module__) if type(fn.__module__) is str else None
    if revision is None or source_hash != revision[0]:
        return source_hash, code
    module = sys.modules.get(fn.__module__)
    if module is None:
        return source_hash, code
    root_name = fn.__qualname__.partition('.<locals>.')[0]
    if fn.__module__ == 'demiflow.data.result_cache' and root_name == '_state_policy':
        # Its runtime type test changed; the function itself keeps its new pin.
        return source_hash, code
    compiled = _declared_codes(source).get(root_name)
    if compiled is None or not _matches_definition(fn, module, compiled, root_name, code):
        return source_hash, code
    return revision[1], _method_pin(fn, code)


def class_method_pins(cls, methods):
    # Class v3 identities contain method hashes but no file hash. Only the
    # actual SDK Dataset has changed method instructions in this migration.
    module = sys.modules.get('demiflow.data.dataset')
    if module is None or cls is not vars(module).get('Dataset'):
        return methods
    for name in _RELATIONAL_METHODS:
        fn = vars(cls).get(name)
        if type(fn) is FunctionType and fn.__globals__ is vars(module):
            methods[name] = _method_pin(fn, methods[name])
    return methods
