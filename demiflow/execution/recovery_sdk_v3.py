"""The v3 wire representation of SDK symbols affected by the D1/D2 import move.

A v3 function pin contains its *whole file* digest, even when its own code did
not change. For these audited source revisions only, encode the historical
file identity after verifying both the actual module binding and the live code
against the measured source. Business source hashes and program artifact
digests are verified independently. An unrecognised revision uses its own pin.
"""
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
# A v3 class pins only its methods, not the source file. An unrelated
# module edit must not invalidate unchanged Dataset method pins.
_RELATIONAL_METHODS = {
    'join': 'e112dd6eccbd697cfad76e42b10fb5a3e51338e59c0b73c601c4fd3500cdd242',
    'reduce_by_key': 'a3942ee0527daa30ed54278c27754438de944e655fa47385050e55360b770424',
    'group_batches': 'fdc6608779fd319db1d03ebe966a80dfb454f24b347578fcb7e446f107e9c723',
    'exclude_keys': '8d6b971dc304a9e0f1d94e8de6429c7c808b7835d5e514d5870a3b74211c0770',
    'union': '0f9fc1b1618f644ceb473ad918614a238d177763755afb6bf9b57a61d91dd5b5',
}


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


def _canonical_function(fn, module):
    if fn.__globals__ is not vars(module):
        return False
    names = fn.__qualname__.split('.')
    owner = vars(module).get(names[0])
    if len(names) == 2 and isinstance(owner, type):
        owner = vars(owner).get(names[1])
    elif len(names) != 1:
        return False
    if isinstance(owner, (staticmethod, classmethod)):
        owner = owner.__func__
    if isinstance(owner, property):
        return fn is owner.fget or fn is owner.fset or fn is owner.fdel
    return fn is owner or type(owner) is FunctionType and fn is vars(owner).get('__wrapped__')


def _method_pin(fn, code):
    # The D1 edit kept the IMPORT_NAME instruction and its relative import
    # level. Its one changed co_names entry is the declaration's package.
    if (fn.__module__ == 'demiflow.data.dataset' and fn.__name__ in _RELATIONAL_METHODS
            and fn.__qualname__ == 'Dataset.' + fn.__name__):
        names = tuple('execution.local_kernel' if name == 'data.sources' else name
                      for name in fn.__code__.co_names)
        return identity(code_record(fn.__code__.replace(co_names=names)))
    return code


def function_pin(fn, source, source_hash, code):
    revision = _REVISIONS.get(fn.__module__) if type(fn.__module__) is str else None
    if revision is None or source_hash != revision[0]:
        return source_hash, code
    module = sys.modules.get(fn.__module__)
    if module is None or not _canonical_function(fn, module):
        return source_hash, code
    if fn.__module__ == 'demiflow.data.result_cache' and fn.__qualname__ == '_state_policy':
        # Its runtime type test changed; the function itself keeps its new pin.
        return source_hash, code
    compiled = _declared_codes(source).get(fn.__qualname__)
    if compiled is None or identity(code_record(compiled)) != code:
        return source_hash, code
    return revision[1], _method_pin(fn, code)


def class_method_pins(cls, methods):
    # Class v3 identities contain method hashes but no file hash. Only the
    # actual SDK Dataset has changed method instructions in this migration.
    module = sys.modules.get('demiflow.data.dataset')
    if module is None or cls is not vars(module).get('Dataset'):
        return methods
    for name, approved_code in _RELATIONAL_METHODS.items():
        fn = vars(cls).get(name)
        if (methods.get(name) == approved_code and type(fn) is FunctionType
                and _canonical_function(fn, module)):
            methods[name] = _method_pin(fn, approved_code)
    return methods
