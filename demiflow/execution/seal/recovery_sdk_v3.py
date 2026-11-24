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

from demiflow.execution.contracts.recovery_sdk_v3 import code_record, LAYOUT_RELOCATIONS
from demiflow.execution.contracts.recovery_contract import identity


# Current source SHA-256, v3 pre-move source SHA-256.
_REVISIONS = {'demiflow.collect.net': ('486592d6e616d61145b22a9f4169812ed5f3d4a53e1b133e079fac7f4abdc6ef',
                          '6434ec51ebce0c0d575b1522273341a7e97da1cfe07b1af71876d8bf5b3a29e7'),
 'demiflow.collect.llm': ('9fc966441838dc954d7e60ff5618c0d1dd0356f597e94495641e14d842ef1d42',
                          '0acad54f342ecf3269418d44d0f9e978ebcfa92bcdbe4b47e92d112c87b5ccd0'),
 'demiflow.execution.request_limits': ('431b27e1065e3fdc02cfd708d60c20b924a1489c78ff2bd6afd36546cf56e134',
                                       '54bc1fa489826c74c7d67432fdeaba9fb9e54131d681936b5c2ea18180c117e9'),
 'demiflow.data.dataset': ('3a5eef124cf8eb85e280f24be742f9ccf239920646c80f58aa48537a2ecaec35',
                           '1ba0e80f1ed0853948a97792dc65125fdaec17504bc3396b5f53ffb593da7e88'),
 'demiflow.data.sources': ('4c349450034860cbaaa81d7b88e39b5caeea6c9ea56849e031804007cd226e79',
                           '3033f1f14141c9759d29679ee5275492bfbc83bcd41b51c7bf9962afd115387f'),
 'demiflow.execution.local_kernel': ('5fac23cf24cf8e2b27e79b36f9faf28ab2a0104517a13d1c02a249a94a74cb24',
                                     'e67e3703f03ea14f8bcefb803bafc3251072e78bc17a5c1f2df60fad2ca2f83f'),
 'demiflow.planning.dataflow_graph_adapter': ('fe4b4be2f8a9dc80a2d0ad28a9a21475641eea6141f9e7f56c1dfc12ee5cf006',
                                              '0ab9e3db6f4dffe74c2f614ac887ce0a69749910b083db63eaa95c819652cbcf'),
 'demiflow.data.result_cache': ('28962de9fd0c6a5296ce522761c06cbe4ae2c25c476ae6523f5f26f95322521d',
                                'dc6858f90a4ea48fb5c950027e405084d9f820ba26b3159592b38a9f42d46788')}
_REVISIONS.update({'demiflow.data.read_api': ('f3237c8a7de9cf5b7c1d920e7cabd19d4445b05c1e6230c92d1f1f2bfb78914e',
                            '1ff5e8431b684f758d2bc2bbe4818783c58ef4fb604e1ad564c4bc9c61f650f1'),
 'demiflow.data.guards': ('77fca99debe0b691ef1a64a75de9fae0f8bb1f2921c5ecd20b0b3e6cfed77c7c',
                          'ce6022c75921d310d962578306c85ed71bd4fa76e88164efcd8834c848c17cec'),
 'demiflow.lance.registered_write': ('0476b16950d5c448a263cecc39f7fcb4e6646b11e364e8d489034d716b731fb1',
                                     '6f04e5f92a72de79c08a8a9d73eaff71f984bb69cc59faf17852e799889456ec')})

# Measured import operands only; executable bodies are authenticated below.
_LAYOUT_IMPORT_PINS = {'demiflow.data.read_api': {'read_legacy_queue_records': ('eeed1979f39e3f4b225212ed11d0eec142f3528bd19c7b148848374dd2595f67',
                                                          '86f249dd104fe43c1e03c01efd923339eea70b5a00f95927efd2663206ac112f'),
                            'from_items': ('0d0424c310038a576634bd9b7ca1b52e7ee933d4df7c153036193243e374b4ff',
                                           '2fce1f015e244e3441c331d34e576efef3c41bd6745c8fc46d77726e4db6c0b8')},
 'demiflow.data.guards': {'require': ('486b78c7736f700d825d53f1144ab5756a4d28af0a9073535b0a34e20aebe653',
                                      '93eb9c55b49dd757e0e980c77bf39b222a52510268188bf7b732e3571aa49448')},
 'demiflow.lance.registered_write': {'_confirmed': ('1f4d3424e9b6b7c2876e54d41cdd960da002fd82f833bb77bf5080bea635ab7e',
                                                    '51a2e7f519b42c32b11981f13f1cad301f0f8b5b0dcab05bd9c08b6d95900178'),
                                     'write_registered_native': ('75ab080304ec4a8ab64b155c3437f1ecd256ca988430b85fe2a32678d6121b8e',
                                                                 '72bbc5624481ec04208e4d6000bffe1a386bc172d5c0aa1001674952f5ddac96')}}

_ROUTING_MODULE = 'demiflow.planning.dataflow_graph_adapter'
_ROUTING_FUNCTIONS = frozenset(('_identity', '_tag', '_unwrap'))
_RELATIONAL_METHODS = ('join', 'reduce_by_key', 'group_batches', 'exclude_keys', 'union')


# The explicit checkpoint operation and arguments already rejected before
# these removed branches. Pin only measured still-present declarations.
_DATASET_METHODS = {'Dataset.map_embeddings': ('4319758ab733dfdb7dc059abfc23753249f07af1259b00ef3fdcb9c669d257d8',
                            '7b2c26f325b5a33519ba5b213c2e35f5b39a8f1224dc43e3c7b19be4f357a33f'),
 'Dataset.map_prompt_async': ('ec04f2b257e2405b0d82150ef74b07924e8c4e1e7f25120c6588510b31a4653f',
                              '995d11e4cb7a97cba1a936b67faeec7656cba225f274387927fd5846e3c40f9d'),
 'Dataset.enqueue': ('71419a5bc22bb91799fe27a6592471441e2cc53bde90e7850499fceddfafc35b',
                     'b532dc94028784498f89ad6308e264ea8c44cde65d321463aeb3131433a9702b'),
 'Dataset.save_lance': ('72fd4e75d41ea5438756108a26700d25e4ffb5588f52e398d9da3e47e205a853',
                        '70380580e66d2c6569ee32c248ef79e4f13abfcf292d210e5e44ae678d8db62a'),
 'Dataset.checkpoint': ('2e47242208e4a3ecd474fa1f19f3213bf14d22bc72ab71b8a3b06a6a6f100914',
                        'ff6767926e0022b28ebecc8e4084ce4bea5a5d66fbcefef109079cabb727d247'),
 'Dataset.map_async': ('249d51ebc65a942fdee637b3cb31448199819d25666853c003e39a3c01d8e6e6',
                       '0578db7e142a4a404ee0c3bb43342b3d9216e5498163dbc757aaf101f7dff1b2'),
 'Dataset.batch_map': ('407a3649a77f329e6eee7d2fb42e3951ae52b1fc130ef9d9a4f4105f364ed73e',
                       '6457fd535618671ed1c8a5ecc0c0384c2cf11af1616b1091a4161d99fc66711c'),
 'Dataset.checkpoint_async': ('6c6b6992592e4f072ecb92f8aa4d979424f2f887d98457aa1322af0c4009533c',
                              '1781511d6fe514790264ad5b5157b0fed353eba65502cfbe79ab688f0f22b0d9'),
 'Dataset.checkpoint_lance': ('1355f621e34a31685e50f4d48a36d317183376da2a179310b61b88ef7859b93b',
                              '578db7fdd0c271d21c4721151756b3852d4dd62dd19786b64070da4c53a7967c'),
 'Dataset.checkpoint_lance_async': ('20e3b3b1f431809fb13139714d8b757757e46dc5fe449a7ec1e1b9f45f82d0a2',
                                    'a9bd4397a371cad04a16dd30e1ee65efe0f40490344506b4981d1cfc7389deaf'),
 'Dataset.run_stream': ('655d02cdc0def6e81a841d1d56a305d7bd86e578acb75a520af8d2be9196540d',
                        '21fc84b10feaeaeb469142f7273a161e1edd8e31e87d047575008676a8ed90fc'),
 'Dataset.group_batches': ('187d93ef1dbba44417fcbc2f5b9efc373a7a3a642dd2fa38ea788209d981a984',
                           'f7718d0b3fd97703efd2b984cea446579798f3dee36d637022745c91e9805125'),
 'Dataset.admit_rows': ('a68edf8d3cb18ef57f273e4165d1757427dc5722b8f3f9dd49bccbe3e8b2cca6',
                        '8e513b315f586801b301bf457f5d6d7bddb04571dcbaef25441578eaeb3eaf4b'),
 'Dataset.lookup_lance': ('8423dcd182b1c0af44540a50665065874e089a44b802406f22c201a014f63452',
                          'a19a4c9ce708e85aa09f1060b82a5ead02693c1339c3cdea55d556bdb14e7790'),
 'Dataset.deduplicate': ('de420504149d1d6e56e8d651d431015492aa2f254adb2a98b3a0cc477859e324',
                         'dd486724d1ccd3946d68b998f82d819a2c1eac105662adbe9b46b67fd21db58e'),
 'Dataset.write_datasink': ('68a90808d036f8ab4e6db9925ba22c1ac9b2530d7195781dbfde17ffe32c766e',
                            'fde84c1d9bd72baea6cd92bdc3efb61b8865b53be5532861eefaef2106479009')}


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
def _declared_codes(source, optimize=-1):
    # Compile only measured, allowlisted platform source. No module is executed.
    module = compile(source, '<sdk-v3>', 'exec', dont_inherit=True, optimize=optimize)
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
        installed = Path(__file__).parents[2].joinpath(*fn.__module__.split('.')[1:]).with_suffix('.py')
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
    pins = _LAYOUT_IMPORT_PINS.get(fn.__module__, {}).get(fn.__qualname__)
    if pins is not None and pins[0] == code:
        return pins[1]
    # v3 expresses this one declaration import by its original package. Every
    # other instruction, constant (including the relative level) and operand
    # still enters code_record. This works with the caller's Python optimizer.
    if fn.__module__ == 'demiflow.data.dataset' and fn.__qualname__ in _DATASET_METHODS:
        # A v3 class records each method body, not surrounding file contents.
        # Its caller has already checked the class's actual method globals.
        # Preserve that representation in each measured interpreter mode.
        entry = LAYOUT_RELOCATIONS.get(fn.__module__ + '.' + fn.__qualname__)
        if entry is not None:
            variants = entry.get('optimization_variants', {}).get(str(sys.flags.optimize), {})
            if code in variants:
                return variants[code]
        current, published = _DATASET_METHODS[fn.__qualname__]
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
    relocated = _layout_source_type(fn)
    if relocated is not None and relocated.get('preserve_identity', True) and source_hash == relocated['source']:
        pins = (relocated['code'] if relocated.get('kind') == 'function'
                else relocated['methods'].get(fn.__qualname__.rsplit('.', 1)[-1]))
        if pins is not None and code == pins[0]:
            return relocated['old_source'], pins[1]
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
    if fn.__module__ == 'demiflow.data.result_cache' and root_name in {
            '_state_policy', 'CacheCall.bind_checkpoint_cache', 'CacheCall.snapshot_state', 'CacheCall.restore_state'}:
        # Runtime state binding changed; these definitions retain their actual pins.
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
    relocated = _layout_source_type(cls)
    if relocated is not None and relocated.get('preserve_identity', True):
        return {name: relocated['methods'][name][1] for name in methods}
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
    for name in (*_RELATIONAL_METHODS, *(key.removeprefix('Dataset.') for key in _DATASET_METHODS)):
        fn = vars(cls).get(name)
        if type(fn) is FunctionType and fn.__globals__ is vars(module):
            methods[name] = _method_pin(fn, methods[name])
    return methods


# Original v3 source-carrier names are persistent data, independent of file
# placement. These are the five actual types found in published program
# artifacts. Both installed source and every method are measured; a changed
# definition uses its real identity and is never certified as the old one.
_LAYOUT_SOURCE_TYPES = {'demiflow.execution.pipeline.package_loader.pipeline_bundle_digest': {'kind': 'function',
                                                                       'old_module': 'demiflow.execution.contracts.pipeline',
                                                                       'old_source': '98b4717cfa7c3989411c3021a9254e69756b405ecb75ee134220aa0667d27f9a',
                                                                       'source': '3958b7f612a3ee4e994cd599d6072967e9311696ec15d68bb42908795f924212',
                                                                       'code': ['0a882b37d1543344a1ffe8736a25e9b1aee2bba4de166349940d38fb95231878',
                                                                                '0a882b37d1543344a1ffe8736a25e9b1aee2bba4de166349940d38fb95231878']},
 'demiflow.execution.sources.legacy_queue.LegacyQueueHistoryDatasource': {'old_module': 'demiflow.execution.legacy_queue',
                                                                          'old_source': '5fe509ab9503850c6979648145056e3399a2d0ee823754e1d627b013edf52070',
                                                                          'source': '727a2fafc81433e11bf32f7d1731a94c3699c435f227f0ff871bbc89d96b131c',
                                                                          'methods': {'__init__': ['4ae277bea5c1e24191add012bffe7ffe2d2e05960f9dfcc0905021ab8870208c',
                                                                                                   '4ae277bea5c1e24191add012bffe7ffe2d2e05960f9dfcc0905021ab8870208c'],
                                                                                      'snapshot_identity': ['c04d235814cd2f9ad1678b90055882cb4dbf86ba637e9eb4945502b63d6be577',
                                                                                                            'c04d235814cd2f9ad1678b90055882cb4dbf86ba637e9eb4945502b63d6be577'],
                                                                                      'recovery_identity': ['bdacb53a986cbf300b60e08a755087655ede304db5c394a79b6d582b5cde6365',
                                                                                                            'bdacb53a986cbf300b60e08a755087655ede304db5c394a79b6d582b5cde6365'],
                                                                                      'get_read_tasks': ['d587bf7375e6a9606e0e8b6dc7f6d3c01a757361e3e8a8332f30d5dfca00bc28',
                                                                                                         'd587bf7375e6a9606e0e8b6dc7f6d3c01a757361e3e8a8332f30d5dfca00bc28'],
                                                                                      'rows': ['4489eb4c7d97716f791c63c5e28be5f9d73449814728d92835283599924fddb3',
                                                                                               'f8ed4c30d25799a24ec9322fe62ac5a724e88d886ba8198fc4b58906c8de2a3c']},
                                                                          'optimization_variants': {'0': {'__init__': {'4ae277bea5c1e24191add012bffe7ffe2d2e05960f9dfcc0905021ab8870208c': '4ae277bea5c1e24191add012bffe7ffe2d2e05960f9dfcc0905021ab8870208c'},
                                                                                                          'snapshot_identity': {'c04d235814cd2f9ad1678b90055882cb4dbf86ba637e9eb4945502b63d6be577': 'c04d235814cd2f9ad1678b90055882cb4dbf86ba637e9eb4945502b63d6be577'},
                                                                                                          'recovery_identity': {'bdacb53a986cbf300b60e08a755087655ede304db5c394a79b6d582b5cde6365': 'bdacb53a986cbf300b60e08a755087655ede304db5c394a79b6d582b5cde6365'},
                                                                                                          'get_read_tasks': {'d587bf7375e6a9606e0e8b6dc7f6d3c01a757361e3e8a8332f30d5dfca00bc28': 'd587bf7375e6a9606e0e8b6dc7f6d3c01a757361e3e8a8332f30d5dfca00bc28'},
                                                                                                          'rows': {'4489eb4c7d97716f791c63c5e28be5f9d73449814728d92835283599924fddb3': 'f8ed4c30d25799a24ec9322fe62ac5a724e88d886ba8198fc4b58906c8de2a3c'}},
                                                                                                    '1': {'__init__': {'4ae277bea5c1e24191add012bffe7ffe2d2e05960f9dfcc0905021ab8870208c': '4ae277bea5c1e24191add012bffe7ffe2d2e05960f9dfcc0905021ab8870208c'},
                                                                                                          'snapshot_identity': {'c04d235814cd2f9ad1678b90055882cb4dbf86ba637e9eb4945502b63d6be577': 'c04d235814cd2f9ad1678b90055882cb4dbf86ba637e9eb4945502b63d6be577'},
                                                                                                          'recovery_identity': {'bdacb53a986cbf300b60e08a755087655ede304db5c394a79b6d582b5cde6365': 'bdacb53a986cbf300b60e08a755087655ede304db5c394a79b6d582b5cde6365'},
                                                                                                          'get_read_tasks': {'d587bf7375e6a9606e0e8b6dc7f6d3c01a757361e3e8a8332f30d5dfca00bc28': 'd587bf7375e6a9606e0e8b6dc7f6d3c01a757361e3e8a8332f30d5dfca00bc28'},
                                                                                                          'rows': {'4489eb4c7d97716f791c63c5e28be5f9d73449814728d92835283599924fddb3': 'f8ed4c30d25799a24ec9322fe62ac5a724e88d886ba8198fc4b58906c8de2a3c'}},
                                                                                                    '2': {'__init__': {'4ae277bea5c1e24191add012bffe7ffe2d2e05960f9dfcc0905021ab8870208c': '4ae277bea5c1e24191add012bffe7ffe2d2e05960f9dfcc0905021ab8870208c'},
                                                                                                          'snapshot_identity': {'c04d235814cd2f9ad1678b90055882cb4dbf86ba637e9eb4945502b63d6be577': 'c04d235814cd2f9ad1678b90055882cb4dbf86ba637e9eb4945502b63d6be577'},
                                                                                                          'recovery_identity': {'bdacb53a986cbf300b60e08a755087655ede304db5c394a79b6d582b5cde6365': 'bdacb53a986cbf300b60e08a755087655ede304db5c394a79b6d582b5cde6365'},
                                                                                                          'get_read_tasks': {'d587bf7375e6a9606e0e8b6dc7f6d3c01a757361e3e8a8332f30d5dfca00bc28': 'd587bf7375e6a9606e0e8b6dc7f6d3c01a757361e3e8a8332f30d5dfca00bc28'},
                                                                                                          'rows': {'4489eb4c7d97716f791c63c5e28be5f9d73449814728d92835283599924fddb3': 'f8ed4c30d25799a24ec9322fe62ac5a724e88d886ba8198fc4b58906c8de2a3c'}}},
                                                                          'variants': {'__init__': {'4ae277bea5c1e24191add012bffe7ffe2d2e05960f9dfcc0905021ab8870208c': '4ae277bea5c1e24191add012bffe7ffe2d2e05960f9dfcc0905021ab8870208c'},
                                                                                       'snapshot_identity': {'c04d235814cd2f9ad1678b90055882cb4dbf86ba637e9eb4945502b63d6be577': 'c04d235814cd2f9ad1678b90055882cb4dbf86ba637e9eb4945502b63d6be577'},
                                                                                       'recovery_identity': {'bdacb53a986cbf300b60e08a755087655ede304db5c394a79b6d582b5cde6365': 'bdacb53a986cbf300b60e08a755087655ede304db5c394a79b6d582b5cde6365'},
                                                                                       'get_read_tasks': {'d587bf7375e6a9606e0e8b6dc7f6d3c01a757361e3e8a8332f30d5dfca00bc28': 'd587bf7375e6a9606e0e8b6dc7f6d3c01a757361e3e8a8332f30d5dfca00bc28'},
                                                                                       'rows': {'4489eb4c7d97716f791c63c5e28be5f9d73449814728d92835283599924fddb3': 'f8ed4c30d25799a24ec9322fe62ac5a724e88d886ba8198fc4b58906c8de2a3c'}}},
 'demiflow.execution.sources.materialized_input.InputMaterializedHandle': {'old_module': 'demiflow.execution.dataflow_inputs',
                                                                           'old_source': '23505a1d134a3312a8568aa497ab3d5f8375eb0bfd1bfd0b59942c17383e8456',
                                                                           'source': 'bbb8f81ac7a05886eef0c01996487a704e63d726884c11e005663c2e12e26536',
                                                                           'methods': {'__init__': ['b85b85852c1db8125162fcdf6c26a65c5a20c587813dd325935af2f134d63109',
                                                                                                    'b85b85852c1db8125162fcdf6c26a65c5a20c587813dd325935af2f134d63109'],
                                                                                       'check_open': ['158b5ef55654f20db50327de2fdbad18c1de3730db97d379e004483892a9b59b',
                                                                                                      '158b5ef55654f20db50327de2fdbad18c1de3730db97d379e004483892a9b59b'],
                                                                                       'rows': ['38c8e778ac98101357234fb093f164b2f1b37f824a64f630aaae7827f6627f25',
                                                                                                '38c8e778ac98101357234fb093f164b2f1b37f824a64f630aaae7827f6627f25'],
                                                                                       'typed_rows': ['7097d7209054104273ca70f9209fb23e5b665aa2ed8563927be16eb2f684fe70',
                                                                                                      '7097d7209054104273ca70f9209fb23e5b665aa2ed8563927be16eb2f684fe70'],
                                                                                       'num_blocks': ['6687d269076d2184a8bfa1bbd2ec411ed18ceb60b2f11e0b4526f9e07c982c2c',
                                                                                                      '6687d269076d2184a8bfa1bbd2ec411ed18ceb60b2f11e0b4526f9e07c982c2c'],
                                                                                       'release': ['fc9b6aacc5bcba65b477ee5fd678ecb81dde9ba7c7cbd632c10fbdc8f6ae974d',
                                                                                                   'fc9b6aacc5bcba65b477ee5fd678ecb81dde9ba7c7cbd632c10fbdc8f6ae974d'],
                                                                                       '_check_open': ['0f9d15bb10ce332a23bcfa5b2b72319bd8c70789d41e54e721060aa7d1fad0d5',
                                                                                                       '0f9d15bb10ce332a23bcfa5b2b72319bd8c70789d41e54e721060aa7d1fad0d5'],
                                                                                       '_iter_rows': ['5b3db5429597954b884f7638f667564e387bc7144d2c04812dd03cf4c2ed14fb',
                                                                                                      '5b3db5429597954b884f7638f667564e387bc7144d2c04812dd03cf4c2ed14fb']}},
 'demiflow.execution.native_queue.dataflow_acquisition_call.QueueSourceDeclaration': {'old_module': 'demiflow.execution.dataflow_acquisition_call',
                                                                                      'old_source': '597a8f30caa94c77fcb6b25b68e6793d45d2f3608ffd5b8e7a8ec4dd39bda40a',
                                                                                      'source': '363465db148c43004e97af506c76bcdb1068292803748a255bd15cc6ebced63a',
                                                                                      'methods': {'snapshot_identity': ['69bfa6567d9c3e07eb4254ae209103c4bb2e78dc98982b4ccfb16f12b70fe4f7',
                                                                                                                        '69bfa6567d9c3e07eb4254ae209103c4bb2e78dc98982b4ccfb16f12b70fe4f7'],
                                                                                                  'recovery_identity': ['69bfa6567d9c3e07eb4254ae209103c4bb2e78dc98982b4ccfb16f12b70fe4f7',
                                                                                                                        '69bfa6567d9c3e07eb4254ae209103c4bb2e78dc98982b4ccfb16f12b70fe4f7'],
                                                                                                  '__init__': ['7c8b6b3fa0d38825de9d0588467274613a48ed729c22c56d86a458eb88db10a1',
                                                                                                               '7c8b6b3fa0d38825de9d0588467274613a48ed729c22c56d86a458eb88db10a1'],
                                                                                                  '__repr__': ['67deaf69a2f6a014e3d72d57980f0891fb4525004ae6aee457d30e6039a467fc',
                                                                                                               '67deaf69a2f6a014e3d72d57980f0891fb4525004ae6aee457d30e6039a467fc'],
                                                                                                  '__eq__': ['391fbc67de36ab6f3e2f328343a5ed49dd951968f1ad1d94992edf49cb2b6062',
                                                                                                             '391fbc67de36ab6f3e2f328343a5ed49dd951968f1ad1d94992edf49cb2b6062'],
                                                                                                  '__setattr__': ['0137c190ba161472435ed96a6e37d567f8dc97f9bbfca214206d83e2a4932184',
                                                                                                                  '0137c190ba161472435ed96a6e37d567f8dc97f9bbfca214206d83e2a4932184'],
                                                                                                  '__delattr__': ['f7ccb7b231364c4d509be3a1172e696edbc0401ce3c665aa2df006834b1d0e05',
                                                                                                                  'f7ccb7b231364c4d509be3a1172e696edbc0401ce3c665aa2df006834b1d0e05'],
                                                                                                  '__hash__': ['8a3d4fecaa32782bdeb9d03af0ff10207692c481e2973776c104406fbbb2a00e',
                                                                                                               '8a3d4fecaa32782bdeb9d03af0ff10207692c481e2973776c104406fbbb2a00e']},
                                                                                      'optimization_variants': {'0': {'snapshot_identity': {'69bfa6567d9c3e07eb4254ae209103c4bb2e78dc98982b4ccfb16f12b70fe4f7': '69bfa6567d9c3e07eb4254ae209103c4bb2e78dc98982b4ccfb16f12b70fe4f7'},
                                                                                                                      'recovery_identity': {'69bfa6567d9c3e07eb4254ae209103c4bb2e78dc98982b4ccfb16f12b70fe4f7': '69bfa6567d9c3e07eb4254ae209103c4bb2e78dc98982b4ccfb16f12b70fe4f7'},
                                                                                                                      '__init__': {'7c8b6b3fa0d38825de9d0588467274613a48ed729c22c56d86a458eb88db10a1': '7c8b6b3fa0d38825de9d0588467274613a48ed729c22c56d86a458eb88db10a1'},
                                                                                                                      '__repr__': {'67deaf69a2f6a014e3d72d57980f0891fb4525004ae6aee457d30e6039a467fc': '67deaf69a2f6a014e3d72d57980f0891fb4525004ae6aee457d30e6039a467fc'},
                                                                                                                      '__eq__': {'391fbc67de36ab6f3e2f328343a5ed49dd951968f1ad1d94992edf49cb2b6062': '391fbc67de36ab6f3e2f328343a5ed49dd951968f1ad1d94992edf49cb2b6062'},
                                                                                                                      '__setattr__': {'0137c190ba161472435ed96a6e37d567f8dc97f9bbfca214206d83e2a4932184': '0137c190ba161472435ed96a6e37d567f8dc97f9bbfca214206d83e2a4932184'},
                                                                                                                      '__delattr__': {'f7ccb7b231364c4d509be3a1172e696edbc0401ce3c665aa2df006834b1d0e05': 'f7ccb7b231364c4d509be3a1172e696edbc0401ce3c665aa2df006834b1d0e05'},
                                                                                                                      '__hash__': {'8a3d4fecaa32782bdeb9d03af0ff10207692c481e2973776c104406fbbb2a00e': '8a3d4fecaa32782bdeb9d03af0ff10207692c481e2973776c104406fbbb2a00e'}},
                                                                                                                '1': {'snapshot_identity': {'69bfa6567d9c3e07eb4254ae209103c4bb2e78dc98982b4ccfb16f12b70fe4f7': '69bfa6567d9c3e07eb4254ae209103c4bb2e78dc98982b4ccfb16f12b70fe4f7'},
                                                                                                                      'recovery_identity': {'69bfa6567d9c3e07eb4254ae209103c4bb2e78dc98982b4ccfb16f12b70fe4f7': '69bfa6567d9c3e07eb4254ae209103c4bb2e78dc98982b4ccfb16f12b70fe4f7'},
                                                                                                                      '__init__': {'7c8b6b3fa0d38825de9d0588467274613a48ed729c22c56d86a458eb88db10a1': '7c8b6b3fa0d38825de9d0588467274613a48ed729c22c56d86a458eb88db10a1'},
                                                                                                                      '__repr__': {'67deaf69a2f6a014e3d72d57980f0891fb4525004ae6aee457d30e6039a467fc': '67deaf69a2f6a014e3d72d57980f0891fb4525004ae6aee457d30e6039a467fc'},
                                                                                                                      '__eq__': {'391fbc67de36ab6f3e2f328343a5ed49dd951968f1ad1d94992edf49cb2b6062': '391fbc67de36ab6f3e2f328343a5ed49dd951968f1ad1d94992edf49cb2b6062'},
                                                                                                                      '__setattr__': {'0137c190ba161472435ed96a6e37d567f8dc97f9bbfca214206d83e2a4932184': '0137c190ba161472435ed96a6e37d567f8dc97f9bbfca214206d83e2a4932184'},
                                                                                                                      '__delattr__': {'f7ccb7b231364c4d509be3a1172e696edbc0401ce3c665aa2df006834b1d0e05': 'f7ccb7b231364c4d509be3a1172e696edbc0401ce3c665aa2df006834b1d0e05'},
                                                                                                                      '__hash__': {'8a3d4fecaa32782bdeb9d03af0ff10207692c481e2973776c104406fbbb2a00e': '8a3d4fecaa32782bdeb9d03af0ff10207692c481e2973776c104406fbbb2a00e'}},
                                                                                                                '2': {'snapshot_identity': {'69bfa6567d9c3e07eb4254ae209103c4bb2e78dc98982b4ccfb16f12b70fe4f7': '69bfa6567d9c3e07eb4254ae209103c4bb2e78dc98982b4ccfb16f12b70fe4f7'},
                                                                                                                      'recovery_identity': {'69bfa6567d9c3e07eb4254ae209103c4bb2e78dc98982b4ccfb16f12b70fe4f7': '69bfa6567d9c3e07eb4254ae209103c4bb2e78dc98982b4ccfb16f12b70fe4f7'},
                                                                                                                      '__init__': {'7c8b6b3fa0d38825de9d0588467274613a48ed729c22c56d86a458eb88db10a1': '7c8b6b3fa0d38825de9d0588467274613a48ed729c22c56d86a458eb88db10a1'},
                                                                                                                      '__repr__': {'67deaf69a2f6a014e3d72d57980f0891fb4525004ae6aee457d30e6039a467fc': '67deaf69a2f6a014e3d72d57980f0891fb4525004ae6aee457d30e6039a467fc'},
                                                                                                                      '__eq__': {'391fbc67de36ab6f3e2f328343a5ed49dd951968f1ad1d94992edf49cb2b6062': '391fbc67de36ab6f3e2f328343a5ed49dd951968f1ad1d94992edf49cb2b6062'},
                                                                                                                      '__setattr__': {'0137c190ba161472435ed96a6e37d567f8dc97f9bbfca214206d83e2a4932184': '0137c190ba161472435ed96a6e37d567f8dc97f9bbfca214206d83e2a4932184'},
                                                                                                                      '__delattr__': {'f7ccb7b231364c4d509be3a1172e696edbc0401ce3c665aa2df006834b1d0e05': 'f7ccb7b231364c4d509be3a1172e696edbc0401ce3c665aa2df006834b1d0e05'},
                                                                                                                      '__hash__': {'8a3d4fecaa32782bdeb9d03af0ff10207692c481e2973776c104406fbbb2a00e': '8a3d4fecaa32782bdeb9d03af0ff10207692c481e2973776c104406fbbb2a00e'}}},
                                                                                      'variants': {'snapshot_identity': {'69bfa6567d9c3e07eb4254ae209103c4bb2e78dc98982b4ccfb16f12b70fe4f7': '69bfa6567d9c3e07eb4254ae209103c4bb2e78dc98982b4ccfb16f12b70fe4f7'},
                                                                                                   'recovery_identity': {'69bfa6567d9c3e07eb4254ae209103c4bb2e78dc98982b4ccfb16f12b70fe4f7': '69bfa6567d9c3e07eb4254ae209103c4bb2e78dc98982b4ccfb16f12b70fe4f7'},
                                                                                                   '__init__': {'7c8b6b3fa0d38825de9d0588467274613a48ed729c22c56d86a458eb88db10a1': '7c8b6b3fa0d38825de9d0588467274613a48ed729c22c56d86a458eb88db10a1'},
                                                                                                   '__repr__': {'67deaf69a2f6a014e3d72d57980f0891fb4525004ae6aee457d30e6039a467fc': '67deaf69a2f6a014e3d72d57980f0891fb4525004ae6aee457d30e6039a467fc'},
                                                                                                   '__eq__': {'391fbc67de36ab6f3e2f328343a5ed49dd951968f1ad1d94992edf49cb2b6062': '391fbc67de36ab6f3e2f328343a5ed49dd951968f1ad1d94992edf49cb2b6062'},
                                                                                                   '__setattr__': {'0137c190ba161472435ed96a6e37d567f8dc97f9bbfca214206d83e2a4932184': '0137c190ba161472435ed96a6e37d567f8dc97f9bbfca214206d83e2a4932184'},
                                                                                                   '__delattr__': {'f7ccb7b231364c4d509be3a1172e696edbc0401ce3c665aa2df006834b1d0e05': 'f7ccb7b231364c4d509be3a1172e696edbc0401ce3c665aa2df006834b1d0e05'},
                                                                                                   '__hash__': {'8a3d4fecaa32782bdeb9d03af0ff10207692c481e2973776c104406fbbb2a00e': '8a3d4fecaa32782bdeb9d03af0ff10207692c481e2973776c104406fbbb2a00e'}}},
 'demiflow.execution.native_queue.dataflow_acquisition_call.AcquisitionCall': {'old_module': 'demiflow.execution.dataflow_acquisition_call',
                                                                               'old_source': '597a8f30caa94c77fcb6b25b68e6793d45d2f3608ffd5b8e7a8ec4dd39bda40a',
                                                                               'source': '363465db148c43004e97af506c76bcdb1068292803748a255bd15cc6ebced63a',
                                                                               'methods': {'__init__': ['6ed653d87436ea1a16c4fdd5d83027371db16c1e2e86e0f09eabd3659fe20583',
                                                                                                        '6ed653d87436ea1a16c4fdd5d83027371db16c1e2e86e0f09eabd3659fe20583'],
                                                                                           'bind': ['1047150a7ea9a0a43c69c427276a1ae0700d5c00d00ee439c54147b62fa6a5dc',
                                                                                                    '1047150a7ea9a0a43c69c427276a1ae0700d5c00d00ee439c54147b62fa6a5dc'],
                                                                                           'snapshot_state': ['281996a89183592ec2a76c431bd515496a5a1c0ce517247932aceaaf97df407b',
                                                                                                              '281996a89183592ec2a76c431bd515496a5a1c0ce517247932aceaaf97df407b'],
                                                                                           'restore_state': ['b6541d1c19222fea934c06c713227ffb2b34e9b455ac55da9c5c4fbaae8016ec',
                                                                                                             'b6541d1c19222fea934c06c713227ffb2b34e9b455ac55da9c5c4fbaae8016ec'],
                                                                                           '__call__': ['02292d155e58ccde35f0e75eed45eae60fce79de267ba402bd1d56a23a0a3aa6',
                                                                                                        '15864c89112752aeb587ddb71aedf94e871c79cfdc49d23ba56d55a7e10060e2']},
                                                                               'optimization_variants': {'0': {'__init__': {'6ed653d87436ea1a16c4fdd5d83027371db16c1e2e86e0f09eabd3659fe20583': '6ed653d87436ea1a16c4fdd5d83027371db16c1e2e86e0f09eabd3659fe20583'},
                                                                                                               'bind': {'1047150a7ea9a0a43c69c427276a1ae0700d5c00d00ee439c54147b62fa6a5dc': '1047150a7ea9a0a43c69c427276a1ae0700d5c00d00ee439c54147b62fa6a5dc'},
                                                                                                               'snapshot_state': {'281996a89183592ec2a76c431bd515496a5a1c0ce517247932aceaaf97df407b': '281996a89183592ec2a76c431bd515496a5a1c0ce517247932aceaaf97df407b'},
                                                                                                               'restore_state': {'b6541d1c19222fea934c06c713227ffb2b34e9b455ac55da9c5c4fbaae8016ec': 'b6541d1c19222fea934c06c713227ffb2b34e9b455ac55da9c5c4fbaae8016ec'},
                                                                                                               '__call__': {'02292d155e58ccde35f0e75eed45eae60fce79de267ba402bd1d56a23a0a3aa6': '15864c89112752aeb587ddb71aedf94e871c79cfdc49d23ba56d55a7e10060e2'}},
                                                                                                         '1': {'__init__': {'6ed653d87436ea1a16c4fdd5d83027371db16c1e2e86e0f09eabd3659fe20583': '6ed653d87436ea1a16c4fdd5d83027371db16c1e2e86e0f09eabd3659fe20583'},
                                                                                                               'bind': {'1047150a7ea9a0a43c69c427276a1ae0700d5c00d00ee439c54147b62fa6a5dc': '1047150a7ea9a0a43c69c427276a1ae0700d5c00d00ee439c54147b62fa6a5dc'},
                                                                                                               'snapshot_state': {'281996a89183592ec2a76c431bd515496a5a1c0ce517247932aceaaf97df407b': '281996a89183592ec2a76c431bd515496a5a1c0ce517247932aceaaf97df407b'},
                                                                                                               'restore_state': {'b6541d1c19222fea934c06c713227ffb2b34e9b455ac55da9c5c4fbaae8016ec': 'b6541d1c19222fea934c06c713227ffb2b34e9b455ac55da9c5c4fbaae8016ec'},
                                                                                                               '__call__': {'02292d155e58ccde35f0e75eed45eae60fce79de267ba402bd1d56a23a0a3aa6': '15864c89112752aeb587ddb71aedf94e871c79cfdc49d23ba56d55a7e10060e2'}},
                                                                                                         '2': {'__init__': {'6ed653d87436ea1a16c4fdd5d83027371db16c1e2e86e0f09eabd3659fe20583': '6ed653d87436ea1a16c4fdd5d83027371db16c1e2e86e0f09eabd3659fe20583'},
                                                                                                               'bind': {'1047150a7ea9a0a43c69c427276a1ae0700d5c00d00ee439c54147b62fa6a5dc': '1047150a7ea9a0a43c69c427276a1ae0700d5c00d00ee439c54147b62fa6a5dc'},
                                                                                                               'snapshot_state': {'281996a89183592ec2a76c431bd515496a5a1c0ce517247932aceaaf97df407b': '281996a89183592ec2a76c431bd515496a5a1c0ce517247932aceaaf97df407b'},
                                                                                                               'restore_state': {'b6541d1c19222fea934c06c713227ffb2b34e9b455ac55da9c5c4fbaae8016ec': 'b6541d1c19222fea934c06c713227ffb2b34e9b455ac55da9c5c4fbaae8016ec'},
                                                                                                               '__call__': {'02292d155e58ccde35f0e75eed45eae60fce79de267ba402bd1d56a23a0a3aa6': '15864c89112752aeb587ddb71aedf94e871c79cfdc49d23ba56d55a7e10060e2'}}},
                                                                               'variants': {'__init__': {'6ed653d87436ea1a16c4fdd5d83027371db16c1e2e86e0f09eabd3659fe20583': '6ed653d87436ea1a16c4fdd5d83027371db16c1e2e86e0f09eabd3659fe20583'},
                                                                                            'bind': {'1047150a7ea9a0a43c69c427276a1ae0700d5c00d00ee439c54147b62fa6a5dc': '1047150a7ea9a0a43c69c427276a1ae0700d5c00d00ee439c54147b62fa6a5dc'},
                                                                                            'snapshot_state': {'281996a89183592ec2a76c431bd515496a5a1c0ce517247932aceaaf97df407b': '281996a89183592ec2a76c431bd515496a5a1c0ce517247932aceaaf97df407b'},
                                                                                            'restore_state': {'b6541d1c19222fea934c06c713227ffb2b34e9b455ac55da9c5c4fbaae8016ec': 'b6541d1c19222fea934c06c713227ffb2b34e9b455ac55da9c5c4fbaae8016ec'},
                                                                                            '__call__': {'02292d155e58ccde35f0e75eed45eae60fce79de267ba402bd1d56a23a0a3aa6': '15864c89112752aeb587ddb71aedf94e871c79cfdc49d23ba56d55a7e10060e2'}}},
 'demiflow.execution.sources.literal_source.LiteralSourceReference': {'old_module': 'demiflow.execution.literal_source',
                                                                      'old_source': '8348d18a5138ba595f97c105efdd67c8a8c34f64c2336ea1edb56efb4c1f9ee3',
                                                                      'source': 'a95189eb2df6f70a749fa7c1a6460d938389aee0bb341f296ef09bfaa8533584',
                                                                      'methods': {'__init__': ['15d22ac1354664b437c9f6bcd1491af3f8a745abb1b561d9eaf10bf3aa3afb82',
                                                                                               '15d22ac1354664b437c9f6bcd1491af3f8a745abb1b561d9eaf10bf3aa3afb82'],
                                                                                  '__repr__': ['67deaf69a2f6a014e3d72d57980f0891fb4525004ae6aee457d30e6039a467fc',
                                                                                               '67deaf69a2f6a014e3d72d57980f0891fb4525004ae6aee457d30e6039a467fc'],
                                                                                  '__eq__': ['e85bcbefb6644f0266cc463d5986622fc303fce6d67beed2f0340e2410c6fe82',
                                                                                             'e85bcbefb6644f0266cc463d5986622fc303fce6d67beed2f0340e2410c6fe82'],
                                                                                  '__setattr__': ['9825bf8c29053108b8283c775dda062f6d284636155117372d087fba07e40d6a',
                                                                                                  '9825bf8c29053108b8283c775dda062f6d284636155117372d087fba07e40d6a'],
                                                                                  '__delattr__': ['35a7690238d62bdabf270d743dbbdbcfd815bbe8a685dca4e78cddfe870f6bdb',
                                                                                                  '35a7690238d62bdabf270d743dbbdbcfd815bbe8a685dca4e78cddfe870f6bdb'],
                                                                                  '__hash__': ['634bb8370d1a15ed62178d2c0275a60966a64a945d5dccd2592da68e09990be1',
                                                                                               '634bb8370d1a15ed62178d2c0275a60966a64a945d5dccd2592da68e09990be1']},
                                                                      'optimization_variants': {'0': {'__init__': {'15d22ac1354664b437c9f6bcd1491af3f8a745abb1b561d9eaf10bf3aa3afb82': '15d22ac1354664b437c9f6bcd1491af3f8a745abb1b561d9eaf10bf3aa3afb82'},
                                                                                                      '__repr__': {'67deaf69a2f6a014e3d72d57980f0891fb4525004ae6aee457d30e6039a467fc': '67deaf69a2f6a014e3d72d57980f0891fb4525004ae6aee457d30e6039a467fc'},
                                                                                                      '__eq__': {'e85bcbefb6644f0266cc463d5986622fc303fce6d67beed2f0340e2410c6fe82': 'e85bcbefb6644f0266cc463d5986622fc303fce6d67beed2f0340e2410c6fe82'},
                                                                                                      '__setattr__': {'9825bf8c29053108b8283c775dda062f6d284636155117372d087fba07e40d6a': '9825bf8c29053108b8283c775dda062f6d284636155117372d087fba07e40d6a'},
                                                                                                      '__delattr__': {'35a7690238d62bdabf270d743dbbdbcfd815bbe8a685dca4e78cddfe870f6bdb': '35a7690238d62bdabf270d743dbbdbcfd815bbe8a685dca4e78cddfe870f6bdb'},
                                                                                                      '__hash__': {'634bb8370d1a15ed62178d2c0275a60966a64a945d5dccd2592da68e09990be1': '634bb8370d1a15ed62178d2c0275a60966a64a945d5dccd2592da68e09990be1'}},
                                                                                                '1': {'__init__': {'15d22ac1354664b437c9f6bcd1491af3f8a745abb1b561d9eaf10bf3aa3afb82': '15d22ac1354664b437c9f6bcd1491af3f8a745abb1b561d9eaf10bf3aa3afb82'},
                                                                                                      '__repr__': {'67deaf69a2f6a014e3d72d57980f0891fb4525004ae6aee457d30e6039a467fc': '67deaf69a2f6a014e3d72d57980f0891fb4525004ae6aee457d30e6039a467fc'},
                                                                                                      '__eq__': {'e85bcbefb6644f0266cc463d5986622fc303fce6d67beed2f0340e2410c6fe82': 'e85bcbefb6644f0266cc463d5986622fc303fce6d67beed2f0340e2410c6fe82'},
                                                                                                      '__setattr__': {'9825bf8c29053108b8283c775dda062f6d284636155117372d087fba07e40d6a': '9825bf8c29053108b8283c775dda062f6d284636155117372d087fba07e40d6a'},
                                                                                                      '__delattr__': {'35a7690238d62bdabf270d743dbbdbcfd815bbe8a685dca4e78cddfe870f6bdb': '35a7690238d62bdabf270d743dbbdbcfd815bbe8a685dca4e78cddfe870f6bdb'},
                                                                                                      '__hash__': {'634bb8370d1a15ed62178d2c0275a60966a64a945d5dccd2592da68e09990be1': '634bb8370d1a15ed62178d2c0275a60966a64a945d5dccd2592da68e09990be1'}},
                                                                                                '2': {'__init__': {'15d22ac1354664b437c9f6bcd1491af3f8a745abb1b561d9eaf10bf3aa3afb82': '15d22ac1354664b437c9f6bcd1491af3f8a745abb1b561d9eaf10bf3aa3afb82'},
                                                                                                      '__repr__': {'67deaf69a2f6a014e3d72d57980f0891fb4525004ae6aee457d30e6039a467fc': '67deaf69a2f6a014e3d72d57980f0891fb4525004ae6aee457d30e6039a467fc'},
                                                                                                      '__eq__': {'e85bcbefb6644f0266cc463d5986622fc303fce6d67beed2f0340e2410c6fe82': 'e85bcbefb6644f0266cc463d5986622fc303fce6d67beed2f0340e2410c6fe82'},
                                                                                                      '__setattr__': {'9825bf8c29053108b8283c775dda062f6d284636155117372d087fba07e40d6a': '9825bf8c29053108b8283c775dda062f6d284636155117372d087fba07e40d6a'},
                                                                                                      '__delattr__': {'35a7690238d62bdabf270d743dbbdbcfd815bbe8a685dca4e78cddfe870f6bdb': '35a7690238d62bdabf270d743dbbdbcfd815bbe8a685dca4e78cddfe870f6bdb'},
                                                                                                      '__hash__': {'634bb8370d1a15ed62178d2c0275a60966a64a945d5dccd2592da68e09990be1': '634bb8370d1a15ed62178d2c0275a60966a64a945d5dccd2592da68e09990be1'}}},
                                                                      'variants': {'__init__': {'15d22ac1354664b437c9f6bcd1491af3f8a745abb1b561d9eaf10bf3aa3afb82': '15d22ac1354664b437c9f6bcd1491af3f8a745abb1b561d9eaf10bf3aa3afb82'},
                                                                                   '__repr__': {'67deaf69a2f6a014e3d72d57980f0891fb4525004ae6aee457d30e6039a467fc': '67deaf69a2f6a014e3d72d57980f0891fb4525004ae6aee457d30e6039a467fc'},
                                                                                   '__eq__': {'e85bcbefb6644f0266cc463d5986622fc303fce6d67beed2f0340e2410c6fe82': 'e85bcbefb6644f0266cc463d5986622fc303fce6d67beed2f0340e2410c6fe82'},
                                                                                   '__setattr__': {'9825bf8c29053108b8283c775dda062f6d284636155117372d087fba07e40d6a': '9825bf8c29053108b8283c775dda062f6d284636155117372d087fba07e40d6a'},
                                                                                   '__delattr__': {'35a7690238d62bdabf270d743dbbdbcfd815bbe8a685dca4e78cddfe870f6bdb': '35a7690238d62bdabf270d743dbbdbcfd815bbe8a685dca4e78cddfe870f6bdb'},
                                                                                   '__hash__': {'634bb8370d1a15ed62178d2c0275a60966a64a945d5dccd2592da68e09990be1': '634bb8370d1a15ed62178d2c0275a60966a64a945d5dccd2592da68e09990be1'}}},
 'demiflow.execution.pipeline.package_loader.PipelineBundleRef': {'old_module': 'demiflow.execution.contracts.pipeline',
                                                                  'old_source': '98b4717cfa7c3989411c3021a9254e69756b405ecb75ee134220aa0667d27f9a',
                                                                  'source': '3958b7f612a3ee4e994cd599d6072967e9311696ec15d68bb42908795f924212',
                                                                  'methods': {'verify': ['31ec7cb21402632dbfbd1956b78fd9767be4c8248c781a0b79d0dc0faa39ea75',
                                                                                         '31ec7cb21402632dbfbd1956b78fd9767be4c8248c781a0b79d0dc0faa39ea75'],
                                                                              '__init__': ['1160210c14c53311d72fd764a0fc1ed18df6591a0df055c5e9f4ffc2731fb4ea',
                                                                                           '1160210c14c53311d72fd764a0fc1ed18df6591a0df055c5e9f4ffc2731fb4ea'],
                                                                              '__repr__': ['67deaf69a2f6a014e3d72d57980f0891fb4525004ae6aee457d30e6039a467fc',
                                                                                           '67deaf69a2f6a014e3d72d57980f0891fb4525004ae6aee457d30e6039a467fc'],
                                                                              '__eq__': ['a001fdd3e4237dbc4724652ec51b502fca9ee599741f88d58aef5727abbf3d8c',
                                                                                         'a001fdd3e4237dbc4724652ec51b502fca9ee599741f88d58aef5727abbf3d8c'],
                                                                              '__setattr__': ['1cb0e135959a328c6ec61f977088e7aa7807920d69c99239780a7e6a2e814e22',
                                                                                              '1cb0e135959a328c6ec61f977088e7aa7807920d69c99239780a7e6a2e814e22'],
                                                                              '__delattr__': ['93db58b361d824c3ceb16749d360c57da8811f900202c7cb96d512c3b4e01f9f',
                                                                                              '93db58b361d824c3ceb16749d360c57da8811f900202c7cb96d512c3b4e01f9f'],
                                                                              '__hash__': ['91dbc5fa91f37140801ffc3a92a3e8e8c09519ba2332a5e72eac28a267695316',
                                                                                           '91dbc5fa91f37140801ffc3a92a3e8e8c09519ba2332a5e72eac28a267695316']}},
 'demiflow.execution.hosts.python_environment.PipelineExecutionEnvironment': {'old_module': 'demiflow.execution.contracts.pipeline',
                                                                              'old_source': '98b4717cfa7c3989411c3021a9254e69756b405ecb75ee134220aa0667d27f9a',
                                                                              'source': 'f933813669c13ed0ebef0652c9b538e907cf260467b041a3538dcc3382ff318d',
                                                                              'methods': {'verify_artifacts': ['070cb54c8615d2beb10ec9a24dee186f6229d9c7dc7201e70b98ff46c7f9b5ad',
                                                                                                               '78e2d5e76b1221193bc0a6f4d5f125d42dc88b57938d0bcac2557c8f874ec1dd'],
                                                                                          'declaration_id': ['64b1184d8cdbd9488b927ef2f3b1ef49d7df9727197e9976028c782d9f215ac4',
                                                                                                             '64b1184d8cdbd9488b927ef2f3b1ef49d7df9727197e9976028c782d9f215ac4'],
                                                                                          '__init__': ['6e92dc26b0914151de7382589d0bc3484c0e52eec5df7c58774fd529f0d29b7e',
                                                                                                       '6e92dc26b0914151de7382589d0bc3484c0e52eec5df7c58774fd529f0d29b7e'],
                                                                                          '__repr__': ['67deaf69a2f6a014e3d72d57980f0891fb4525004ae6aee457d30e6039a467fc',
                                                                                                       '67deaf69a2f6a014e3d72d57980f0891fb4525004ae6aee457d30e6039a467fc'],
                                                                                          '__eq__': ['13ae98adb94dd524916a603e79dae67ed655e197ac32bc7c067d343d053b59e6',
                                                                                                     '13ae98adb94dd524916a603e79dae67ed655e197ac32bc7c067d343d053b59e6'],
                                                                                          '__setattr__': ['c2c53af6b94a1ed05f621a83e2b64c58673d94c345a3c385f445a41f68e22ea5',
                                                                                                          'c2c53af6b94a1ed05f621a83e2b64c58673d94c345a3c385f445a41f68e22ea5'],
                                                                                          '__delattr__': ['dc41a73d7bbd7f98f9d6f4999103cdc268cfea04630bfb826d7acbbb0b0d8ea1',
                                                                                                          'dc41a73d7bbd7f98f9d6f4999103cdc268cfea04630bfb826d7acbbb0b0d8ea1'],
                                                                                          '__hash__': ['af542e97dd5e649b0270dc1633398d276147c51901ee916da61dbab926ab4ee8',
                                                                                                       'af542e97dd5e649b0270dc1633398d276147c51901ee916da61dbab926ab4ee8']},
                                                                              'optimization_variants': {'0': {'verify_artifacts': {'070cb54c8615d2beb10ec9a24dee186f6229d9c7dc7201e70b98ff46c7f9b5ad': '78e2d5e76b1221193bc0a6f4d5f125d42dc88b57938d0bcac2557c8f874ec1dd'},
                                                                                                              'declaration_id': {'64b1184d8cdbd9488b927ef2f3b1ef49d7df9727197e9976028c782d9f215ac4': '64b1184d8cdbd9488b927ef2f3b1ef49d7df9727197e9976028c782d9f215ac4'},
                                                                                                              '__init__': {'6e92dc26b0914151de7382589d0bc3484c0e52eec5df7c58774fd529f0d29b7e': '6e92dc26b0914151de7382589d0bc3484c0e52eec5df7c58774fd529f0d29b7e'},
                                                                                                              '__repr__': {'67deaf69a2f6a014e3d72d57980f0891fb4525004ae6aee457d30e6039a467fc': '67deaf69a2f6a014e3d72d57980f0891fb4525004ae6aee457d30e6039a467fc'},
                                                                                                              '__eq__': {'13ae98adb94dd524916a603e79dae67ed655e197ac32bc7c067d343d053b59e6': '13ae98adb94dd524916a603e79dae67ed655e197ac32bc7c067d343d053b59e6'},
                                                                                                              '__setattr__': {'c2c53af6b94a1ed05f621a83e2b64c58673d94c345a3c385f445a41f68e22ea5': 'c2c53af6b94a1ed05f621a83e2b64c58673d94c345a3c385f445a41f68e22ea5'},
                                                                                                              '__delattr__': {'dc41a73d7bbd7f98f9d6f4999103cdc268cfea04630bfb826d7acbbb0b0d8ea1': 'dc41a73d7bbd7f98f9d6f4999103cdc268cfea04630bfb826d7acbbb0b0d8ea1'},
                                                                                                              '__hash__': {'af542e97dd5e649b0270dc1633398d276147c51901ee916da61dbab926ab4ee8': 'af542e97dd5e649b0270dc1633398d276147c51901ee916da61dbab926ab4ee8'}},
                                                                                                        '1': {'verify_artifacts': {'070cb54c8615d2beb10ec9a24dee186f6229d9c7dc7201e70b98ff46c7f9b5ad': '78e2d5e76b1221193bc0a6f4d5f125d42dc88b57938d0bcac2557c8f874ec1dd'},
                                                                                                              'declaration_id': {'64b1184d8cdbd9488b927ef2f3b1ef49d7df9727197e9976028c782d9f215ac4': '64b1184d8cdbd9488b927ef2f3b1ef49d7df9727197e9976028c782d9f215ac4'},
                                                                                                              '__init__': {'6e92dc26b0914151de7382589d0bc3484c0e52eec5df7c58774fd529f0d29b7e': '6e92dc26b0914151de7382589d0bc3484c0e52eec5df7c58774fd529f0d29b7e'},
                                                                                                              '__repr__': {'67deaf69a2f6a014e3d72d57980f0891fb4525004ae6aee457d30e6039a467fc': '67deaf69a2f6a014e3d72d57980f0891fb4525004ae6aee457d30e6039a467fc'},
                                                                                                              '__eq__': {'13ae98adb94dd524916a603e79dae67ed655e197ac32bc7c067d343d053b59e6': '13ae98adb94dd524916a603e79dae67ed655e197ac32bc7c067d343d053b59e6'},
                                                                                                              '__setattr__': {'c2c53af6b94a1ed05f621a83e2b64c58673d94c345a3c385f445a41f68e22ea5': 'c2c53af6b94a1ed05f621a83e2b64c58673d94c345a3c385f445a41f68e22ea5'},
                                                                                                              '__delattr__': {'dc41a73d7bbd7f98f9d6f4999103cdc268cfea04630bfb826d7acbbb0b0d8ea1': 'dc41a73d7bbd7f98f9d6f4999103cdc268cfea04630bfb826d7acbbb0b0d8ea1'},
                                                                                                              '__hash__': {'af542e97dd5e649b0270dc1633398d276147c51901ee916da61dbab926ab4ee8': 'af542e97dd5e649b0270dc1633398d276147c51901ee916da61dbab926ab4ee8'}},
                                                                                                        '2': {'verify_artifacts': {'070cb54c8615d2beb10ec9a24dee186f6229d9c7dc7201e70b98ff46c7f9b5ad': '78e2d5e76b1221193bc0a6f4d5f125d42dc88b57938d0bcac2557c8f874ec1dd'},
                                                                                                              'declaration_id': {'64b1184d8cdbd9488b927ef2f3b1ef49d7df9727197e9976028c782d9f215ac4': '64b1184d8cdbd9488b927ef2f3b1ef49d7df9727197e9976028c782d9f215ac4'},
                                                                                                              '__init__': {'6e92dc26b0914151de7382589d0bc3484c0e52eec5df7c58774fd529f0d29b7e': '6e92dc26b0914151de7382589d0bc3484c0e52eec5df7c58774fd529f0d29b7e'},
                                                                                                              '__repr__': {'67deaf69a2f6a014e3d72d57980f0891fb4525004ae6aee457d30e6039a467fc': '67deaf69a2f6a014e3d72d57980f0891fb4525004ae6aee457d30e6039a467fc'},
                                                                                                              '__eq__': {'13ae98adb94dd524916a603e79dae67ed655e197ac32bc7c067d343d053b59e6': '13ae98adb94dd524916a603e79dae67ed655e197ac32bc7c067d343d053b59e6'},
                                                                                                              '__setattr__': {'c2c53af6b94a1ed05f621a83e2b64c58673d94c345a3c385f445a41f68e22ea5': 'c2c53af6b94a1ed05f621a83e2b64c58673d94c345a3c385f445a41f68e22ea5'},
                                                                                                              '__delattr__': {'dc41a73d7bbd7f98f9d6f4999103cdc268cfea04630bfb826d7acbbb0b0d8ea1': 'dc41a73d7bbd7f98f9d6f4999103cdc268cfea04630bfb826d7acbbb0b0d8ea1'},
                                                                                                              '__hash__': {'af542e97dd5e649b0270dc1633398d276147c51901ee916da61dbab926ab4ee8': 'af542e97dd5e649b0270dc1633398d276147c51901ee916da61dbab926ab4ee8'}}},
                                                                              'variants': {'verify_artifacts': {'070cb54c8615d2beb10ec9a24dee186f6229d9c7dc7201e70b98ff46c7f9b5ad': '78e2d5e76b1221193bc0a6f4d5f125d42dc88b57938d0bcac2557c8f874ec1dd'},
                                                                                           'declaration_id': {'64b1184d8cdbd9488b927ef2f3b1ef49d7df9727197e9976028c782d9f215ac4': '64b1184d8cdbd9488b927ef2f3b1ef49d7df9727197e9976028c782d9f215ac4'},
                                                                                           '__init__': {'6e92dc26b0914151de7382589d0bc3484c0e52eec5df7c58774fd529f0d29b7e': '6e92dc26b0914151de7382589d0bc3484c0e52eec5df7c58774fd529f0d29b7e'},
                                                                                           '__repr__': {'67deaf69a2f6a014e3d72d57980f0891fb4525004ae6aee457d30e6039a467fc': '67deaf69a2f6a014e3d72d57980f0891fb4525004ae6aee457d30e6039a467fc'},
                                                                                           '__eq__': {'13ae98adb94dd524916a603e79dae67ed655e197ac32bc7c067d343d053b59e6': '13ae98adb94dd524916a603e79dae67ed655e197ac32bc7c067d343d053b59e6'},
                                                                                           '__setattr__': {'c2c53af6b94a1ed05f621a83e2b64c58673d94c345a3c385f445a41f68e22ea5': 'c2c53af6b94a1ed05f621a83e2b64c58673d94c345a3c385f445a41f68e22ea5'},
                                                                                           '__delattr__': {'dc41a73d7bbd7f98f9d6f4999103cdc268cfea04630bfb826d7acbbb0b0d8ea1': 'dc41a73d7bbd7f98f9d6f4999103cdc268cfea04630bfb826d7acbbb0b0d8ea1'},
                                                                                           '__hash__': {'af542e97dd5e649b0270dc1633398d276147c51901ee916da61dbab926ab4ee8': 'af542e97dd5e649b0270dc1633398d276147c51901ee916da61dbab926ab4ee8'}}},
 'demiflow.execution.contracts.pipeline.FrozenRequirements': {'old_module': 'demiflow.execution.requirements',
                                                              'old_source': '01da04645cc97e9d1e437dc3e11a10015af900d5f2f131a46a3e5af0cc1603ba',
                                                              'source': '4a08ddee2020d6dfb9938a648013cf97fe8ccf4399b8e1e80bf1f90eacda638e',
                                                              'methods': {'__init__': ['da31e3a42db5e0c6eed76af516ee9731cb015000a95d06be82895cca7e8c8b71',
                                                                                       'da31e3a42db5e0c6eed76af516ee9731cb015000a95d06be82895cca7e8c8b71'],
                                                                          '__repr__': ['67deaf69a2f6a014e3d72d57980f0891fb4525004ae6aee457d30e6039a467fc',
                                                                                       '67deaf69a2f6a014e3d72d57980f0891fb4525004ae6aee457d30e6039a467fc'],
                                                                          '__eq__': ['1e9d1e2773f308e22d46ad83bd2feb23a1a3f3280cefa9d24d39f711d266a97a',
                                                                                     '1e9d1e2773f308e22d46ad83bd2feb23a1a3f3280cefa9d24d39f711d266a97a'],
                                                                          '__setattr__': ['7771762d25d6fa848fffbe1baefb3cf73269691d5a21f9292880120beca99000',
                                                                                          '7771762d25d6fa848fffbe1baefb3cf73269691d5a21f9292880120beca99000'],
                                                                          '__delattr__': ['f323de17c011059ca4122a519264de344341e2dfa3d621f4f8c9b3a3beff85c6',
                                                                                          'f323de17c011059ca4122a519264de344341e2dfa3d621f4f8c9b3a3beff85c6'],
                                                                          '__hash__': ['3a3a6f905931f61a10288cce5676928f5c961e7eed0f7280fd0155de0e09b0fd',
                                                                                       '3a3a6f905931f61a10288cce5676928f5c961e7eed0f7280fd0155de0e09b0fd']}},
 'demiflow.execution.contracts.pipeline.ResolvedWheel': {'old_module': 'demiflow.execution.requirements',
                                                         'old_source': '01da04645cc97e9d1e437dc3e11a10015af900d5f2f131a46a3e5af0cc1603ba',
                                                         'source': '4a08ddee2020d6dfb9938a648013cf97fe8ccf4399b8e1e80bf1f90eacda638e',
                                                         'methods': {'__post_init__': ['7cadbf92742c278603f58141c8856bac3a3efed3719e542db585db859eeb2702',
                                                                                       '7cadbf92742c278603f58141c8856bac3a3efed3719e542db585db859eeb2702'],
                                                                     'to_dict': ['c0867e0810059819887f877378b63461a0b965fc47a5d92e6cd809e7f50f57b8',
                                                                                 'c0867e0810059819887f877378b63461a0b965fc47a5d92e6cd809e7f50f57b8'],
                                                                     '__init__': ['a0796a3314810e3d3836e85d44b74994bc72816c47646a18710bdf0322bf2434',
                                                                                  'a0796a3314810e3d3836e85d44b74994bc72816c47646a18710bdf0322bf2434'],
                                                                     '__repr__': ['67deaf69a2f6a014e3d72d57980f0891fb4525004ae6aee457d30e6039a467fc',
                                                                                  '67deaf69a2f6a014e3d72d57980f0891fb4525004ae6aee457d30e6039a467fc'],
                                                                     '__eq__': ['56ddbe9419fa3a374e59651dcd9e37770e86a840f3bf96cffb1ab44aeff9c950',
                                                                                '56ddbe9419fa3a374e59651dcd9e37770e86a840f3bf96cffb1ab44aeff9c950'],
                                                                     '__setattr__': ['e8c1a9528cdaeae9f06bd7ca36463600f374f6d03b82b49070fd72391d449508',
                                                                                     'e8c1a9528cdaeae9f06bd7ca36463600f374f6d03b82b49070fd72391d449508'],
                                                                     '__delattr__': ['d871cdb838a8cc6df6dbe6aec8664cbd74b7da9d7986d314ba63ab185cc84a9c',
                                                                                     'd871cdb838a8cc6df6dbe6aec8664cbd74b7da9d7986d314ba63ab185cc84a9c'],
                                                                     '__hash__': ['e7209ca4223361dad6b73aadf221b20ae503d23355bdf1bc87b3c90a9bf90a13',
                                                                                  'e7209ca4223361dad6b73aadf221b20ae503d23355bdf1bc87b3c90a9bf90a13']}},
 'demiflow.execution.contracts.pipeline.PythonEnvironmentResolution': {'old_module': 'demiflow.execution.requirements',
                                                                       'old_source': '01da04645cc97e9d1e437dc3e11a10015af900d5f2f131a46a3e5af0cc1603ba',
                                                                       'source': '4a08ddee2020d6dfb9938a648013cf97fe8ccf4399b8e1e80bf1f90eacda638e',
                                                                       'methods': {'to_dict': ['3164671caf673dba6a2f559e2d9d736ac746ff0ea0248721efb82e7b51b4f1c3',
                                                                                               '3164671caf673dba6a2f559e2d9d736ac746ff0ea0248721efb82e7b51b4f1c3'],
                                                                                   '__init__': ['9b7f4792e5c97302a8cdfb565dc99573518708e503e6ea1167e3099793f021d2',
                                                                                                '9b7f4792e5c97302a8cdfb565dc99573518708e503e6ea1167e3099793f021d2'],
                                                                                   '__repr__': ['67deaf69a2f6a014e3d72d57980f0891fb4525004ae6aee457d30e6039a467fc',
                                                                                                '67deaf69a2f6a014e3d72d57980f0891fb4525004ae6aee457d30e6039a467fc'],
                                                                                   '__eq__': ['9918bf476b6ee50ba4aeef3afdaf7a1ca7f427fba0561d41ff6396f6a4d4f36b',
                                                                                              '9918bf476b6ee50ba4aeef3afdaf7a1ca7f427fba0561d41ff6396f6a4d4f36b'],
                                                                                   '__setattr__': ['ee3f6cc0f824f58de51c93ad70d794b778960a47c630836bf87dbbfb5ec11cc4',
                                                                                                   'ee3f6cc0f824f58de51c93ad70d794b778960a47c630836bf87dbbfb5ec11cc4'],
                                                                                   '__delattr__': ['b3643ae6b000d3dbaef565d5123848369a914f27b73d7c0883eff96b5d23362f',
                                                                                                   'b3643ae6b000d3dbaef565d5123848369a914f27b73d7c0883eff96b5d23362f'],
                                                                                   '__hash__': ['986337d6ad34cee1491223e7ff2ced824e3a808a3584c4c16592b7838a0b2cab',
                                                                                                '986337d6ad34cee1491223e7ff2ced824e3a808a3584c4c16592b7838a0b2cab']}},
 'demiflow.execution.worker.stream_resources.ObservedQueue': {'old_module': 'demiflow.execution.request_limits',
                                                              'old_source': '54bc1fa489826c74c7d67432fdeaba9fb9e54131d681936b5c2ea18180c117e9',
                                                              'source': 'a8d8c5fa58915728302c1a8b927618fa4616ddde86dfcd5501a3264cd19701ff',
                                                              'methods': {'__init__': ['e57607d3509439597143d7ee612488134c85410e28ccbf043567b8c32b9a48c7',
                                                                                       'e57607d3509439597143d7ee612488134c85410e28ccbf043567b8c32b9a48c7'],
                                                                          'full': ['0ac55e8e05d6c22d5d7ea8695916bfb5f01fcfd97c6c4375a6e4d8f9a7e37b81',
                                                                                   '0ac55e8e05d6c22d5d7ea8695916bfb5f01fcfd97c6c4375a6e4d8f9a7e37b81'],
                                                                          'put_nowait': ['0dabd40ac21bcb3974113239619d48387c5975004c072c8a095f1667bdce5570',
                                                                                         '6dc19610b1f055108b41dce283f6fe31f77a1b76cdb577635a06549224c802bb'],
                                                                          'get_nowait': ['a34929c61ee5e66df278413202ab479035d962d86a2edcbae66e127c430cc438',
                                                                                         '77a333d49524d51dbca48bd3cabe16888a3f14302718cc5ff73ff0a5d6fa4495']},
                                                              'optimization_variants': {'0': {'__init__': {'e57607d3509439597143d7ee612488134c85410e28ccbf043567b8c32b9a48c7': 'e57607d3509439597143d7ee612488134c85410e28ccbf043567b8c32b9a48c7'},
                                                                                              'full': {'0ac55e8e05d6c22d5d7ea8695916bfb5f01fcfd97c6c4375a6e4d8f9a7e37b81': '0ac55e8e05d6c22d5d7ea8695916bfb5f01fcfd97c6c4375a6e4d8f9a7e37b81'},
                                                                                              'put_nowait': {'0dabd40ac21bcb3974113239619d48387c5975004c072c8a095f1667bdce5570': '6dc19610b1f055108b41dce283f6fe31f77a1b76cdb577635a06549224c802bb'},
                                                                                              'get_nowait': {'a34929c61ee5e66df278413202ab479035d962d86a2edcbae66e127c430cc438': '77a333d49524d51dbca48bd3cabe16888a3f14302718cc5ff73ff0a5d6fa4495'}},
                                                                                        '1': {'__init__': {'e57607d3509439597143d7ee612488134c85410e28ccbf043567b8c32b9a48c7': 'e57607d3509439597143d7ee612488134c85410e28ccbf043567b8c32b9a48c7'},
                                                                                              'full': {'0ac55e8e05d6c22d5d7ea8695916bfb5f01fcfd97c6c4375a6e4d8f9a7e37b81': '0ac55e8e05d6c22d5d7ea8695916bfb5f01fcfd97c6c4375a6e4d8f9a7e37b81'},
                                                                                              'put_nowait': {'0dabd40ac21bcb3974113239619d48387c5975004c072c8a095f1667bdce5570': '6dc19610b1f055108b41dce283f6fe31f77a1b76cdb577635a06549224c802bb'},
                                                                                              'get_nowait': {'a34929c61ee5e66df278413202ab479035d962d86a2edcbae66e127c430cc438': '77a333d49524d51dbca48bd3cabe16888a3f14302718cc5ff73ff0a5d6fa4495'}},
                                                                                        '2': {'__init__': {'e57607d3509439597143d7ee612488134c85410e28ccbf043567b8c32b9a48c7': 'e57607d3509439597143d7ee612488134c85410e28ccbf043567b8c32b9a48c7'},
                                                                                              'full': {'0ac55e8e05d6c22d5d7ea8695916bfb5f01fcfd97c6c4375a6e4d8f9a7e37b81': '0ac55e8e05d6c22d5d7ea8695916bfb5f01fcfd97c6c4375a6e4d8f9a7e37b81'},
                                                                                              'put_nowait': {'0dabd40ac21bcb3974113239619d48387c5975004c072c8a095f1667bdce5570': '6dc19610b1f055108b41dce283f6fe31f77a1b76cdb577635a06549224c802bb'},
                                                                                              'get_nowait': {'a34929c61ee5e66df278413202ab479035d962d86a2edcbae66e127c430cc438': '77a333d49524d51dbca48bd3cabe16888a3f14302718cc5ff73ff0a5d6fa4495'}}},
                                                              'variants': {'__init__': {'e57607d3509439597143d7ee612488134c85410e28ccbf043567b8c32b9a48c7': 'e57607d3509439597143d7ee612488134c85410e28ccbf043567b8c32b9a48c7'},
                                                                           'full': {'0ac55e8e05d6c22d5d7ea8695916bfb5f01fcfd97c6c4375a6e4d8f9a7e37b81': '0ac55e8e05d6c22d5d7ea8695916bfb5f01fcfd97c6c4375a6e4d8f9a7e37b81'},
                                                                           'put_nowait': {'0dabd40ac21bcb3974113239619d48387c5975004c072c8a095f1667bdce5570': '6dc19610b1f055108b41dce283f6fe31f77a1b76cdb577635a06549224c802bb'},
                                                                           'get_nowait': {'a34929c61ee5e66df278413202ab479035d962d86a2edcbae66e127c430cc438': '77a333d49524d51dbca48bd3cabe16888a3f14302718cc5ff73ff0a5d6fa4495'}}}}

_LAYOUT_SOURCE_TYPES.update(LAYOUT_RELOCATIONS)

def _contextmanager_original(value, module):
    """Authenticate stdlib wrapping without executing the supplied function."""
    import contextlib
    wrapped = vars(value).get('__wrapped__')
    if (value.__globals__ is not vars(contextlib)
            or type(wrapped) is not FunctionType or wrapped.__globals__ is not vars(module)
            or value.__code__.co_freevars != ('func',)
            or not value.__closure__ or len(value.__closure__) != 1
            or value.__closure__[0].cell_contents is not wrapped):
        return None
    from inspect import isasyncgenfunction
    factory = contextlib.asynccontextmanager if isasyncgenfunction(wrapped) else contextlib.contextmanager
    template = factory(wrapped)
    if (identity(code_record(value.__code__)) != identity(code_record(template.__code__))
            or value.__defaults__ != template.__defaults__ or value.__kwdefaults__ != template.__kwdefaults__):
        return None
    return wrapped


def _layout_dependencies_match(entry):
    """Authenticate helpers used by an explicitly reviewed paired conversion.

    This does not decide whether arbitrary edits are equivalent. The finite
    recorded pairs are the inlined input helper and passive budget projection;
    altered helper code, defaults or import bindings invalidate those pairs.
    """
    from hashlib import sha256
    from importlib import import_module
    for item in entry.get('dependencies', ()):
        module = import_module(item['module'])
        parts = item['name'].split('.')
        fn = vars(module).get(parts[0])
        if len(parts) == 2 and isinstance(fn, type):
            # These dependencies are real ordinary methods, not properties or
            # static/class method descriptors with a different call shape.
            fn = type.__dict__['__dict__'].__get__(fn).get(parts[1])
        elif len(parts) != 1:
            return False
        if type(fn) is not FunctionType:
            return False
        bound = fn
        if item.get('contextmanager', False):
            fn = _contextmanager_original(fn, module)
            if fn is None:
                return False
        if fn.__globals__ is not vars(module) or fn.__closure__:
            return False
        source = Path(module.__file__).read_bytes()
        if sha256(source).hexdigest() != item['source']:
            return False
        code = identity(code_record(fn.__code__))
        declared = _declared_codes(source).get(item['name'])
        if (code not in item['codes'] or declared is None
                or code != identity(code_record(declared))
                or fn.__defaults__ != item['defaults'] or fn.__kwdefaults__ != item['kwdefaults']):
            return False
        for owner, name in item['bindings']:
            if vars(import_module(owner)).get(name) is not bound:
                return False
    return True


def _layout_source_type(value):
    module_name = getattr(value, '__module__', None)
    qualname = getattr(value, '__qualname__', '')
    if type(module_name) is not str or type(qualname) is not str:
        return None
    class_name = qualname.split('.', 1)[0]
    entry = (_LAYOUT_SOURCE_TYPES.get(module_name + '.' + qualname)
             or _LAYOUT_SOURCE_TYPES.get(module_name + '.' + class_name))
    module = sys.modules.get(module_name)
    if entry is None or module is None:
        return None
    if not _layout_dependencies_match(entry):
        return None
    if 'optimization_variants' in entry:
        # Inlining a helper can remove a wrapper docstring: the current body
        # is identical in normal/-OO mode while the original wrapper was not.
        variants = entry['optimization_variants'].get(str(sys.flags.optimize))
        if variants is None:
            return None
        entry = {**entry, 'variants': variants}
    if entry.get('kind') == 'function':
        if type(value) is not FunctionType:
            return None
        source = Path(module.__file__).read_bytes()
        from hashlib import sha256
        if sha256(source).hexdigest() != entry['source']:
            return None
        original = value
        if value.__globals__ is not vars(module):
            original = _contextmanager_original(value, module)
            if original is None or value not in _owners(module, qualname):
                return None
        code = identity(code_record(original.__code__))
        declared = _declared_codes(source).get(qualname)
        if declared is None:
            return None
        if 'kwdefault_owner' in entry:
            # A measured SDK keyword default is an actual slot, not a global
            # named "<lambda>". Authenticate its parent and original binding.
            owner_name, parameter = entry['kwdefault_owner']
            owner = vars(module).get(owner_name)
            if (type(owner) is not FunctionType or owner.__globals__ is not vars(module)
                    or owner.__qualname__ != owner_name
                    or type(owner.__kwdefaults__) is not dict
                    or owner.__kwdefaults__.get(parameter) is not original
                    or original.__globals__ is not vars(module) or original.__closure__
                    or original.__defaults__ is not None or original.__kwdefaults__ is not None
                    or code != identity(code_record(declared))
                    or _layout_source_type(owner) is None):
                return None
        elif not _matches_definition(original, module, declared, qualname, code, source):
            return None
        if 'variants' in entry:
            published = entry['variants'].get(code)
            return {**entry, 'code': (code, published)} if published is not None else None
        default = identity(code_record(_declared_codes(source, 0)[qualname]))
        if default != entry['code'][0]:
            return None
        return {**entry, 'code': (code, code if entry['code'][0] == entry['code'][1] else entry['code'][1])}
    cls = vars(module).get(class_name)
    if not isinstance(cls, type) or (value is not cls and value is not vars(cls).get(qualname.rsplit('.', 1)[-1])):
        return None
    try:
        from hashlib import sha256
        source = Path(module.__file__).read_bytes()
    except (OSError, AttributeError, TypeError):
        return None
    if sha256(source).hexdigest() != entry['source']:
        return None
    compiled = _declared_codes(source)
    default_codes = _declared_codes(source, 0)
    methods = {}
    from collections import Counter
    declarations = Counter()
    class_code = compiled.get(class_name)
    if class_code is None:
        return None
    declared_members = tuple(item for item in class_code.co_consts if isinstance(item, CodeType))
    # A recorded responsibility move may bind an original method directly from
    # its actual owner. Authenticate that exact ordinary function and class
    # slot; this does not admit arbitrary foreign methods or descriptors.
    method_owners = entry.get('method_owners', {})
    if method_owners:
        from importlib import import_module
        slots = type.__dict__['__dict__'].__get__(cls)
        for name, owner in method_owners.items():
            owner_module = import_module(owner['module'])
            actual = vars(owner_module).get(owner['name'])
            if (name not in entry['methods'] or type(actual) is not FunctionType
                    or slots.get(name) is not actual
                    or not _layout_dependencies_match({'dependencies': [owner]})):
                return None
    for name, member in vars(cls).items():
        functions = ((member.fget, member.fset, member.fdel) if isinstance(member, property)
                     else (member.__func__,) if isinstance(member, (classmethod, staticmethod)) else (member,))
        for method in functions:
            if type(method) is not FunctionType:
                continue
            code = identity(code_record(method.__code__))
            candidates = tuple(item for item in declared_members if item.co_qualname == method.__qualname__)
            if name in method_owners:
                owner = method_owners[name]
                owner_module = sys.modules[owner['module']]
                owner_source = Path(owner_module.__file__).read_bytes()
                original_code = identity(code_record(_declared_codes(owner_source, 0)[owner['name']]))
            elif candidates:
                declarations[method.__qualname__] += 1
                original = method
                if method.__globals__ is not vars(module):
                    original = _contextmanager_original(method, module)
                    if original is None:
                        return None
                original_live_code = identity(code_record(original.__code__))
                if not any(original_live_code == identity(code_record(item)) for item in candidates):
                    return None
                if original.__closure__:
                    if original.__code__.co_freevars != ('__class__',):
                        return None
                    try:
                        if original.__closure__[0].cell_contents is not cls:
                            return None
                    except ValueError:
                        return None
                if original is not method:
                    original_code = code
                else:
                    originals = tuple(item for item in default_codes[class_name].co_consts
                                      if isinstance(item, CodeType) and item.co_qualname == method.__qualname__)
                    # Getter and setter share a qualname. Pair their position
                    # in the class definition rather than discarding one.
                    position = next(i for i,item in enumerate(candidates)
                                    if original_live_code == identity(code_record(item)))
                    original_code = identity(code_record(originals[position]))
            else:
                if isinstance(member, property):
                    return None
                # dataclasses generate methods at class construction; their
                # measured bodies still bind to this class's actual module.
                wrapped = getattr(method, '__wrapped__', None)
                if wrapped is not None:
                    import dataclasses
                    if (method.__globals__ is not vars(dataclasses)
                            or type(wrapped) is not FunctionType
                            or wrapped.__globals__ is not vars(module)):
                        return None
                elif method.__globals__ is not vars(module):
                    # Protocol synthesizes these two methods in typing rather
                    # than the declaring module. Authenticate the actual stdlib
                    # definition and its one class cell, as well as the recorded
                    # method pins below; foreign globals are otherwise refused.
                    import typing
                    if (cls.__bases__ != (typing.Protocol,)
                            or method.__globals__ is not vars(typing)
                            or method.__defaults__ is not None
                            or method.__kwdefaults__ is not None):
                        return None
                    if name == '__init__':
                        if method is not typing._no_init_or_replace_init:
                            return None
                    elif name == '__subclasshook__':
                        parent = vars(typing.Protocol)['__init_subclass__'].__func__
                        if (not any(method.__code__ is item for item in parent.__code__.co_consts)
                                or method.__code__.co_freevars != ('cls',)
                                or method.__closure__ is None
                                or len(method.__closure__) != 1):
                            return None
                        try:
                            if method.__closure__[0].cell_contents is not cls:
                                return None
                        except ValueError:
                            return None
                    else:
                        return None
                original_code = code
            if not isinstance(member, (property, classmethod, staticmethod)):
                pins = entry['methods'].get(name)
                if pins is None or original_code != pins[0]:
                    return None
                if 'variants' in entry:
                    published = entry['variants'].get(name, {}).get(code)
                    if published is None:
                        return None
                else:
                    published = code if pins[0] == pins[1] else pins[1]
                methods[name] = (code, published)
    expected = Counter(item.co_qualname for item in declared_members)
    if declarations != expected or set(methods) != set(entry['methods']):
        return None
    return {**entry, 'methods': methods}


def persistent_source_module(value):
    """The authenticated original v3 name; no imports or process aliases."""
    entry = _layout_source_type(value)
    return entry['old_module'] if entry is not None and entry.get('preserve_identity', True) else value.__module__


def persistent_source_name(value):
    """Encode a measured original name without changing the live definition."""
    entry = _layout_source_type(value)
    if (entry is not None and entry.get('kind') == 'function'
            and entry.get('preserve_identity', True)):
        return entry.get('old_name', value.__qualname__)
    return value.__qualname__


def source_pickle_global(module, name):
    """Resolve only recorded historical GLOBAL names, never arbitrary paths."""
    for key, entry in _LAYOUT_SOURCE_TYPES.items():
        current_module = entry.get('module', key.rsplit('.', 1)[0])
        current_name = entry.get('name', key.rsplit('.', 1)[1])
        old_name = entry.get('old_name', current_name)
        # Current names remain normal imports, including after a future source
        # change. Only an actual relocated historical GLOBAL needs this table.
        if module != entry['old_module'] or module == current_module:
            continue
        if name == old_name:
            return current_module, current_name
        if name.startswith(old_name + '.') and name[len(old_name) + 1:] in entry.get('methods', {}):
            return current_module, current_name + name[len(old_name):]
    return None


def authentic_source_global(value, original_module):
    """A successful measured relocation, without the ordinary identity fallback."""
    from types import MethodType
    if type(value) is MethodType:
        value = value.__func__
    entry = _layout_source_type(value)
    return entry is not None and entry['old_module'] == original_module


def native_pickle_function(code, globals, name, argdefs, closure):
    """Authenticate the two recorded by-value SDK callback constructors.

    This recognizes measured complete code, not arbitrary source transformations.
    The caller must separately verify the recorded cloudpickle state before its
    load returns. No business function or closure is rewritten.
    """
    import pickle
    from importlib import import_module
    from demiflow.execution.contracts.recovery_contract import document, RecoveryContractError

    if type(code) is not CodeType or type(globals) is not dict:
        return None
    for key, entry in LAYOUT_RELOCATIONS.items():
        variants = entry.get('by_value')
        if variants is None:
            continue
        original_name = entry['old_name']
        # The identity encoder is a bounded SDK metadata codec, not a limit on
        # ordinary cloudpickle business code. Only these two advertised SDK
        # constructors enter it; all other factories keep the original path.
        if (globals.get('__name__') != entry['old_module']
                or original_name not in (code.co_name, code.co_qualname, name)):
            continue
        digest = identity(code_record(code))
        recorded = variants.get(digest)
        if (recorded is None or code.co_name != original_name or code.co_qualname != original_name
                or code.co_freevars or closure is not None or argdefs is not None or name is not None
                or not _same_module_file(globals.get('__file__'), code.co_filename)):
            raise pickle.UnpicklingError('Native callback differs from its recorded definition')
        initial = {k: v for k, v in globals.items() if k != '__file__'}
        # cloudpickle can share base globals between callbacks in one payload;
        # its ordinary factory may already have installed the builtin table.
        if '__builtins__' in initial:
            import builtins
            if initial.pop('__builtins__') is not vars(builtins):
                raise pickle.UnpicklingError('Native callback has modified builtins')
        try:
            same_globals = document(initial) == document(recorded['factory']['globals'])
        except RecoveryContractError as exc:
            raise pickle.UnpicklingError('Native callback has non-passive globals') from exc
        if not same_globals:
            raise pickle.UnpicklingError('Native callback has changed module globals')
        module = import_module(entry['module'])
        current = vars(module).get(entry['name'])
        if (type(current) is not FunctionType or not authentic_source_global(current, entry['old_module'])
                or current.__defaults__ is not None or current.__kwdefaults__ is not None
                or current.__closure__ is not None or vars(current) or current.__annotations__):
            raise pickle.UnpicklingError('Native callback owner differs from its audited definition')
        return current, recorded['state']
    return None
