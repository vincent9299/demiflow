"""The v3 wire representation of SDK symbols affected by the D1/D2 import move.

A v3 function pin contains its *whole file* digest, even when its own code did
not change. For these audited source revisions only, encode the historical
file identity after verifying both the actual module binding and the live code
against the measured source. Business source hashes and program artifact
digests are verified independently. An unrecognised revision uses its own pin.
"""
from dis import get_instructions
from functools import lru_cache
from os.path import samefile
from pathlib import Path
import sys
from types import CodeType, FunctionType

from .artifacts import code_record
from .recovery_contract import identity


# Current source SHA-256, v3 pre-move source SHA-256.
_REVISIONS = {
    'demiflow.collect.net': (
        '486592d6e616d61145b22a9f4169812ed5f3d4a53e1b133e079fac7f4abdc6ef',
        '6434ec51ebce0c0d575b1522273341a7e97da1cfe07b1af71876d8bf5b3a29e7'),
    'demiflow.collect.llm': (
        '9fc966441838dc954d7e60ff5618c0d1dd0356f597e94495641e14d842ef1d42',
        '0acad54f342ecf3269418d44d0f9e978ebcfa92bcdbe4b47e92d112c87b5ccd0'),
    'demiflow.execution.request_limits': (
        '431b27e1065e3fdc02cfd708d60c20b924a1489c78ff2bd6afd36546cf56e134',
        '54bc1fa489826c74c7d67432fdeaba9fb9e54131d681936b5c2ea18180c117e9'),
    'demiflow.data.dataset': (
        '50f2757ff445ecbeb379a88d29e55386dcf241643c694bb95ee0623c0ca17b50',
        '1ba0e80f1ed0853948a97792dc65125fdaec17504bc3396b5f53ffb593da7e88'),
    'demiflow.data.sources': (
        '4c349450034860cbaaa81d7b88e39b5caeea6c9ea56849e031804007cd226e79',
        '3033f1f14141c9759d29679ee5275492bfbc83bcd41b51c7bf9962afd115387f'),
    'demiflow.execution.local_kernel': (
        '5fac23cf24cf8e2b27e79b36f9faf28ab2a0104517a13d1c02a249a94a74cb24',
        'e67e3703f03ea14f8bcefb803bafc3251072e78bc17a5c1f2df60fad2ca2f83f'),
    'demiflow.planning.dataflow_graph_adapter': (
        '219b7b61382f2c0a5a71a160a762047b088f03107a0bd1f1096bf3b48e11c5d5',
        '0ab9e3db6f4dffe74c2f614ac887ce0a69749910b083db63eaa95c819652cbcf'),
    'demiflow.data.result_cache': (
        '2ac2ab41bb6a1b4eda601b8c06f78e7c0bc74caf638d9c54afe6360190a4912b',
        'dc6858f90a4ea48fb5c950027e405084d9f820ba26b3159592b38a9f42d46788'),
}
_ROUTING_MODULE = 'demiflow.planning.dataflow_graph_adapter'
_ROUTING_FUNCTIONS = frozenset(('_identity', '_tag', '_unwrap'))
_RELATIONAL_METHODS = ('join', 'reduce_by_key', 'group_batches', 'exclude_keys', 'union')


# The explicit checkpoint operation and arguments already rejected before
# these removed branches. Pin only measured still-present declarations.
_DATASET_CHECKPOINT_METHODS = {
    'Dataset.checkpoint': (
        '7a3505e339739860403b1cc5435db189f55a605cf2820da4389f061ea575d637',
        'ff6767926e0022b28ebecc8e4084ce4bea5a5d66fbcefef109079cabb727d247'),
    'Dataset.map_async': (
        'e129a81cd80980ab713dbdc3ae94d6b949ab368c6f33f43c2c92c4207f0b51bc',
        '0578db7e142a4a404ee0c3bb43342b3d9216e5498163dbc757aaf101f7dff1b2'),
    'Dataset.batch_map': (
        '2e280d2d116e06a79530cd7b8647ad4ddbbfc13823decccf7035a00165c25328',
        '6457fd535618671ed1c8a5ecc0c0384c2cf11af1616b1091a4161d99fc66711c'),
}


# Published captures contain public client getters and the bounded row queue's
# actual methods. Match only these audited live definitions. The queue retains
# its row budgets, ordering and timing; private stream control tokens retired.
_RETAINED_METHODS = {
    'demiflow.collect.net': {
        'get_client': (
            '670a5171eb55ced3daffec42853d277e2dea0577372a984533058c76579d1649',
            'a6858a8d7f523b63cc9c72dbcc890865b8805badbb9d6eea0cf4925cb64e5519'),
        'get_download_client': (
            '2ca3b3718e68c2e00491ab61f32b64f91aff4e29835b00f402a386fdcdcd915f',
            'cbd2816aa8bb6d501b4f36e52fc671fd79b2d267729d41581f8438af2d602042'),
    },
    'demiflow.collect.llm': {
        'get_llm_client': (
            'ad20c08d982f2302fd8cb4a80b347adfebe2b1d622588aec30cf986bb8aa00e7',
            '49a4ecc8e57ed2b2f87e62f747c88798108d21f8c162775e002058da9a7ec8cc'),
    },
    'demiflow.execution.request_limits': {
        'ObservedQueue.put_nowait': (
            '2388fd31560b996b14c63dea8ee8b1dac4332e076896940d27766322e8deb04f',
            '6dc19610b1f055108b41dce283f6fe31f77a1b76cdb577635a06549224c802bb'),
        'ObservedQueue.get_nowait': (
            'a34929c61ee5e66df278413202ab479035d962d86a2edcbae66e127c430cc438',
            '77a333d49524d51dbca48bd3cabe16888a3f14302718cc5ff73ff0a5d6fa4495'),
        'ObservedQueue.full': (
            '0ac55e8e05d6c22d5d7ea8695916bfb5f01fcfd97c6c4375a6e4d8f9a7e37b81',
            '0ac55e8e05d6c22d5d7ea8695916bfb5f01fcfd97c6c4375a6e4d8f9a7e37b81'),
        'ObservedQueue.__init__': (
            'e57607d3509439597143d7ee612488134c85410e28ccbf043567b8c32b9a48c7',
            'e57607d3509439597143d7ee612488134c85410e28ccbf043567b8c32b9a48c7'),
    },
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


def _same_module_file(recorded, loaded):
    if type(recorded) is not str or type(loaded) is not str:
        return False
    if recorded == loaded:
        return True
    try:
        return samefile(recorded, loaded)
    except (OSError, ValueError):
        return False


def _restored_source(fn, module, source):
    # cloudpickle preserves the defining installation's own file coordinates;
    # v3 binds its bytes, not the path or inode of the reader's installation.
    recorded = fn.__globals__.get('__file__')
    filename = fn.__code__.co_filename
    if not _same_module_file(recorded, filename):
        return False
    if module is None:
        # A cold worker can unpickle a closed SDK lambda without importing
        # its parent module. Resolve only the already allowlisted SDK source.
        installed = Path(__file__).parents[1].joinpath(*fn.__module__.split('.')[1:]).with_suffix('.py')
    else:
        installed = module.__file__
    if _same_module_file(filename, str(installed)):
        return True
    try:
        with Path(installed).open('rb') as stream:
            return stream.read(len(source) + 1) == source
    except (OSError, ValueError, TypeError):
        return False


def _matches_definition(fn, module, compiled, root_name, code, source):
    roots = _owners(module, root_name) if module is not None else ()
    if module is not None and fn.__globals__ is vars(module):
        for root in roots:
            if type(root) is not FunctionType or root.__globals__ is not vars(module):
                continue
            if fn is root:
                return code == identity(code_record(compiled))
            if root_name != fn.__qualname__ and any(child is fn.__code__ for child in _children(root.__code__)):
                return identity(code_record(root.__code__)) == identity(code_record(compiled))
        return False

    # A by-value closure has its own globals. Only a body with no global
    # names can be recognized from the audited parent in the installed source;
    # unrelated globals cannot affect it. Closure/defaults are pinned normally.
    if (root_name == fn.__qualname__ or fn.__code__.co_qualname != fn.__qualname__
            or fn.__code__.co_names or fn.__globals__.get('__name__') != fn.__module__
            or fn.__globals__.get('__package__') != fn.__module__.rpartition('.')[0]
            or not _restored_source(fn, module, source)):
        return False
    if module is not None and not any(
            type(root) is FunctionType and root.__globals__ is vars(module)
            and identity(code_record(root.__code__)) == identity(code_record(compiled)) for root in roots):
        return False
    return any(child.co_qualname == fn.__qualname__ and identity(code_record(child)) == code
               and not any(nested.co_names for nested in _children(child)) for child in _children(compiled))


def _method_pin(fn, code):
    # v3 expresses this one declaration import by its original package. Every
    # other instruction, constant (including the relative level) and operand
    # still enters code_record. This works with the caller's Python optimizer.
    if fn.__module__ == 'demiflow.data.dataset' and fn.__qualname__ in _DATASET_CHECKPOINT_METHODS:
        current, published = _DATASET_CHECKPOINT_METHODS[fn.__qualname__]
        return published if current == code else code
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
    root_name = fn.__qualname__.partition('.<locals>.')[0]
    if fn.__module__ == _ROUTING_MODULE and root_name not in _ROUTING_FUNCTIONS:
        # Only the three unchanged row-routing functions were sealed as programs.
        return source_hash, code
    if fn.__module__ == 'demiflow.data.dataset' and root_name == 'Dataset.fit':
        # The additive v4 method never existed in the audited v3 file.
        return source_hash, code
    if fn.__module__ == 'demiflow.data.result_cache' and root_name == '_state_policy':
        # Its runtime type test changed; the function itself keeps its new pin.
        return source_hash, code
    retained = _RETAINED_METHODS.get(fn.__module__)
    if retained is not None:
        pins = retained.get(fn.__qualname__)
        if pins is None or pins[0] != code:
            return source_hash, code
        if fn.__closure__ and not _queue_method(fn, module):
            return source_hash, code
    compiled = _declared_codes(source).get(root_name)
    if compiled is None or not _matches_definition(fn, module, compiled, root_name, code, source):
        return source_hash, code
    return revision[1], pins[1] if retained is not None else _method_pin(fn, code)



def routing_source_revisions(fn):
    """The same three routed row functions had a distinct file digest in f12f2e40.

    New plans use the published b2613ba8 pin. An original older program may
    use the earlier hash only for its canonical engine function and only after
    the current file and live instructions authenticated in ``function_pin``.
    """
    if (type(fn) is not FunctionType or fn.__module__ != _ROUTING_MODULE or fn.__qualname__ not in _ROUTING_FUNCTIONS):
        return None
    module = sys.modules.get(_ROUTING_MODULE)
    if module is None or fn is not vars(module).get(fn.__qualname__):
        return None
    return _REVISIONS[_ROUTING_MODULE][1], 'cee316a5adf1fb7542a57efd9471ae4a1c836ae6d107a570ac5fc81b179fce16'


def _queue_method(fn, module):
    queue = vars(module).get('ObservedQueue') if module is not None else None
    if (type(fn) is not FunctionType or not isinstance(queue, type)
            or vars(queue).get(fn.__name__) is not fn or fn.__globals__ is not vars(module)
            or fn.__code__.co_freevars != ('__class__',) or not fn.__closure__):
        return False
    try:
        return fn.__closure__[0].cell_contents is queue
    except ValueError:
        return False


def class_method_pins(cls, methods):
    # Class v3 identities contain method hashes but no file hash.
    queue_module = sys.modules.get('demiflow.execution.request_limits')
    if queue_module is not None and cls is vars(queue_module).get('ObservedQueue'):
        for name in ('put_nowait', 'get_nowait'):
            fn = vars(cls).get(name)
            pins = _RETAINED_METHODS[queue_module.__name__]['ObservedQueue.' + name]
            if _queue_method(fn, queue_module) and methods.get(name) == pins[0]:
                methods[name] = pins[1]
        return methods
    module = sys.modules.get('demiflow.data.dataset')
    if module is None or cls is not vars(module).get('Dataset'):
        return methods
    for name in (*_RELATIONAL_METHODS, 'checkpoint', 'map_async', 'batch_map'):
        fn = vars(cls).get(name)
        if type(fn) is FunctionType and fn.__globals__ is vars(module):
            methods[name] = _method_pin(fn, methods[name])
    return methods
