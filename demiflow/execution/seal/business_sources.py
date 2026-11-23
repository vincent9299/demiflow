"""Bounded, content-pinned business imports delivered with each program."""
from dataclasses import dataclass
import hashlib
import inspect
import os
from pathlib import Path, PurePosixPath
import stat
import sys

from ..contracts.dataflow_errors import DataflowError
from ..recovery_contract import RecoveryContractError, identity

SOURCE_MEDIA = 'text/x-python'
MAX_SOURCES = 64


def source_media(name):
    return 'application/octet-stream' if ':' in name else SOURCE_MEDIA


def _package_name(name):
    if type(name) is not str or len(name) > 512 or not all(part.isidentifier() for part in name.split('.')):
        raise DataflowError('INVALID_CONTRACT', 'Invalid Python package or module name')
    return name


def _relative_member(value):
    if type(value) is not str or not value or len(value) > 512 or any(ch in value for ch in ('\\', ':', '\x00')):
        raise DataflowError('INVALID_CONTRACT', 'Invalid package member')
    path = PurePosixPath(value)
    if path.is_absolute() or '..' in path.parts or not path.parts or value != path.as_posix():
        raise DataflowError('INVALID_CONTRACT', 'Package member must be a canonical relative file')
    return path


def _member_chunks(path):
    # Anchor every directory descriptor before opening the next component.
    flags = os.O_RDONLY | os.O_NOFOLLOW | os.O_DIRECTORY
    fd = os.open(path.anchor, flags)
    try:
        for component in path.parts[1:-1]:
            child = os.open(component, flags, dir_fd=fd)
            os.close(fd)
            fd = child
        leaf = os.open(path.name, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK, dir_fd=fd)
        with os.fdopen(leaf, 'rb') as stream:
            info = os.fstat(leaf)
            if not stat.S_ISREG(info.st_mode):
                raise DataflowError('INVALID_CONTRACT', 'Package member must be a regular file')
            remaining = info.st_size
            while remaining:
                chunk = stream.read(min(65536, remaining))
                if not chunk:
                    raise DataflowError('CODE_MISMATCH', 'Package member changed during sealing')
                remaining -= len(chunk)
                yield chunk
            if stream.read(1):
                raise DataflowError('CODE_MISMATCH', 'Package member changed during sealing')
    except OSError as exc:
        raise DataflowError('CODE_MISMATCH', 'Cannot read a direct declared package member: ' + str(path)) from exc
    finally:
        os.close(fd)


@dataclass(frozen=True)
class SourceFile:
    path: Path
    digest: str
    size: int

    @classmethod
    def capture(cls, path):
        digest = hashlib.sha256()
        size = 0
        for chunk in _member_chunks(path):
            digest.update(chunk)
            size += len(chunk)
        return cls(path, digest.hexdigest(), size)

    def chunks(self):
        """Recheck the pinned bytes before the receiver publishes the file."""
        digest = hashlib.sha256()
        size = 0
        for chunk in _member_chunks(self.path):
            size += len(chunk)
            if size > self.size:
                raise DataflowError('CODE_MISMATCH', 'Business source changed during sealing')
            digest.update(chunk)
            yield chunk
        if size != self.size or digest.hexdigest() != self.digest:
            raise DataflowError('CODE_MISMATCH', 'Business source changed during sealing')


def _add_declared_members(target, sources):
    declared = inspect.getattr_static(target, '__demiflow_package_files__', None)
    if declared is None:
        return
    if type(declared) is not dict or len(declared) > MAX_SOURCES:
        raise DataflowError('INVALID_CONTRACT', 'Package files require a bounded package-to-tuple mapping')
    for package, files in declared.items():
        _package_name(package)
        module = sys.modules.get(package)
        # Only loaded business packages can contribute files. Declaring a name
        # must never import or execute that package to locate it.
        path = _classify_dependency_module(module) if module is not None else None
        if (path is None or path.name != '__init__.py' or not path.is_absolute() or type(files) is not tuple
                or Path(vars(module).get('__file__', '')) != path):
            raise DataflowError('INVALID_CONTRACT', 'Package files require a loaded business package and relative file tuple')
        if len(files) > MAX_SOURCES:
            raise DataflowError('BUDGET_EXCEEDED', 'Package files exceed the source count budget')
        seen = set()
        for item in files:
            relative = _relative_member(item)
            if relative in seen:
                raise DataflowError('INVALID_CONTRACT', 'Duplicate declared package file')
            seen.add(relative)
            member = path.parent / relative
            if member.suffix == '.py':
                parts = relative.parent.parts if relative.name == '__init__.py' else relative.with_suffix('').parts
                name = _package_name('.'.join((package, *parts)))
            else:
                name = package + ':' + item
            if name not in sources and len(sources) >= MAX_SOURCES:
                raise DataflowError('BUDGET_EXCEEDED', 'Package files exceed the source count budget')
            old = sources.get(name)
            source = SourceFile.capture(member)
            if old is not None and old != source:
                raise DataflowError('CODE_MISMATCH', 'One package member has conflicting source bytes')
            sources[name] = source


def business_sources(kind, value):
    from ...data.plan import LogicalPlan, MapOp
    if kind == 'callable':
        sources = business_dependency_modules(LogicalPlan((MapOp(callable=value),)))
        _add_declared_members(value.target, sources)
    elif kind == 'source':
        sources = business_dependency_modules(LogicalPlan(()), source=value)
    else:
        sources = {}
    # A package initializer executes before its member; pin it as actual code.
    for name in tuple(sources):
        if ':' in name:
            continue
        parts = name.split('.')
        source_path = sources[name].path
        folder = source_path.parent
        for length in range(1, len(parts)):
            parent = '.'.join(parts[:length])
            # Resolve from the member's owned source directory without importing
            # or executing a package merely to seal a program.
            levels = len(parts) - length - (0 if source_path.name == '__init__.py' else 1)
            directory = folder
            for _ in range(levels):
                directory = directory.parent
            path = directory / '__init__.py'
            if path.is_file() and parent not in sources:
                if len(sources) >= MAX_SOURCES:
                    raise DataflowError('BUDGET_EXCEEDED', 'Business imports exceed their source count budget')
                sources[parent] = SourceFile.capture(path)
    return sources


def source_identity(sources):
    return {name: {'sha256': source.digest, 'bytes': source.size,
                   'package': source.path.name == '__init__.py'}
            for name, source in sorted(sources.items())}


def stage_sources(sources, *, store):
    from dataclasses import asdict
    return {name: asdict(store.put(source.chunks(), max_bytes=max(1, source.size), media_type=source_media(name)))
            for name, source in sorted(sources.items())}


def mount_sources(refs, expected, *, store, cache):
    """Populate the managed worker cache only from sealed, hash-verified bytes."""
    from .plan import _artifact_from_json, _artifact_chunks
    from ..state_artifacts import atomic_chunks
    if not isinstance(refs, dict) or set(refs) != set(expected) or len(refs) > MAX_SOURCES:
        raise DataflowError('CORRUPT_ARTIFACT', 'Business import membership differs')
    if not refs:
        return
    root = Path(cache) / 'business' / identity(expected)
    for name, fact in expected.items():
        if ':' in name:
            package, _, item = name.partition(':')
            member = _relative_member(item)
            if member.suffix == '.py' or fact['package']:
                raise DataflowError('CORRUPT_ARTIFACT', 'Ambiguous sealed package member')
            relative = Path(*_package_name(package).split('.')) / member
        else:
            relative = Path(*_package_name(name).split('.'))
            relative = relative / '__init__.py' if fact['package'] else relative.with_suffix('.py')
        target = root / relative
        if any(path.is_symlink() for path in (target, *target.parents)):
            raise DataflowError('CORRUPT_ARTIFACT', 'Business code cache contains a symlink')
        ref = _artifact_from_json(refs[name])
        if ref.digest != fact['sha256'] or ref.size_bytes != fact['bytes']:
            raise DataflowError('CODE_MISMATCH', 'Business source differs from the program pin')
        chunks = _artifact_chunks(store, ref, media_type=source_media(name))
        if target.exists():
            for _ in chunks:
                pass
            cached = SourceFile.capture(target)
            if cached.size != ref.size_bytes or cached.digest != ref.digest:
                raise DataflowError('CODE_MISMATCH', 'Cached business code differs')
        else:
            atomic_chunks(target, chunks)
    # Existing imports are deliberately not replaced: recomputing the loaded
    # program pin detects stale modules. Fresh workers import this exact bundle.
    if str(root) not in sys.path:
        sys.path.insert(0, str(root))
    # A sealed namespace package must not resolve to an unrelated regular
    # package later on PYTHONPATH (e.g. another project's operators package).
    from importlib.machinery import ModuleSpec
    from types import ModuleType
    for name in expected:
        if ':' in name:
            continue
        parts = name.split('.')
        for length in range(1, len(parts)):
            parent = '.'.join(parts[:length])
            if parent in expected:
                continue
            location = str(root.joinpath(*parts[:length]))
            module = sys.modules.get(parent)
            if module is None:
                module = ModuleType(parent)
                module.__package__ = parent
                module.__spec__ = ModuleSpec(parent, loader=None, is_package=True)
                module.__path__ = module.__spec__.submodule_search_locations
                sys.modules[parent] = module
            if getattr(module, '__file__', None) is not None or not hasattr(module, '__path__'):
                raise DataflowError('CODE_MISMATCH', 'Business namespace is occupied by another package')
            if location not in module.__path__:
                module.__path__.append(location)


_DEP_MAX_MODULES = 64
_DEP_MAX_FUNCTIONS = 256
_DEP_MAX_NODES = 4096


def stable_code_identity(value):
    """Normalize ``describe`` output for cross-process cloudpickle stability.

    cloudpickle reconstructs a function-local class with a stripped ``__qualname__``
    (the enclosing ``<locals>`` scope does not exist in the worker), so describe's
    qualified-name label is not round-trip stable. The code-pin binds the actually
    executed code (``source_hash`` + ``code_record`` + closure), not that label, so
    reduce each function/class ``name`` to its final component. Structural plan
    identity is bound separately by the sealed logical_dag operator recheck.
    """
    if isinstance(value, dict):
        return {k: (v.rsplit(".", 1)[-1]
                    if k == "name" and isinstance(v, str) and "module" in value
                    else stable_code_identity(v))
                for k, v in value.items()}
    if isinstance(value, list):
        return [stable_code_identity(v) for v in value]
    return value


def _classify_dependency_module(module):
    """Classify a ``__globals__`` module: return a business source Path to pin, or
    None to skip (platform / stdlib / site-packages third-party / compiled ext).

    Passive — reads only module metadata and the source path, never executes code.
    A module that is neither stdlib nor platform nor third-party and has no
    resolvable Python source (dynamic, no ``__file__``) fail-closes: the code-pin
    refuses rather than silently leaving an unpinned business dependency.
    """
    import inspect
    import os
    import sys
    import site
    import sysconfig
    from pathlib import Path
    name = getattr(module, "__name__", "") or ""
    if name.split(".", 1)[0] in sys.stdlib_module_names:
        return None
    if name == "demiflow" or name.startswith("demiflow."):
        return None
    try:
        filename = inspect.getsourcefile(module)
    except (TypeError, OSError):
        filename = None
    module_filename = getattr(module, "__file__", None)
    filename = filename or module_filename
    if filename:
        path = Path(filename)
        try:
            resolved = path.resolve()
        except OSError:
            resolved = path
        if resolved.is_relative_to(Path(__file__).resolve().parents[2]):
            return None
        # A virtual environment may explicitly inherit its base installation.
        # Only accept active interpreter installation roots, never arbitrary
        # PYTHONPATH directories that merely end in "site-packages".
        roots = {sysconfig.get_path(key) for key in ('purelib', 'platlib')}
        roots.update(path for path in site.getsitepackages() if path in sys.path)
        for base in roots:
            if base and resolved.is_relative_to(Path(base).resolve()):
                return None
            if base and module_filename:
                # An installation may link a package to shared wheel files.
                # Keep its original installation ownership before following
                # that link; arbitrary sys.path entries are not installation
                # roots. Both source and module metadata must agree. Editable
                # sources outside this root remain business code to be pinned.
                installed = Path(os.path.abspath(base))
                if all(Path(os.path.abspath(value)).is_relative_to(installed)
                       for value in (filename, module_filename)):
                    return None
        if resolved.suffix == ".py" and resolved.is_file():
            return resolved
        return None
    if getattr(module, "__path__", None) is not None:
        return None
    raise RecoveryContractError(
        "Business dependency module has no resolvable source; code-pin refuses (fail-closed): " + name)


def _module_registered_by_value(module_name):
    """True if cloudpickle carries this module (or a parent package) by value, so
    its importable functions/classes still travel by value (workbench seal, F9).
    Mirrors cloudpickle's ``_is_registered_pickle_by_value`` via its public registry.
    """
    import cloudpickle
    registered = cloudpickle.list_registry_pickle_by_value()
    name = module_name
    while True:
        if name in registered:
            return True
        parent = name.rsplit(".", 1)[0]
        if parent == name:
            return False
        name = parent


def _static_reference_member(owner, name):
    import inspect
    value = inspect.getattr_static(owner, name, None)
    # Native staticmethod lookup returns the underlying function. Preserve
    # cloudpickle's import-by-name classification without running descriptors.
    if issubclass(type(owner), type) and type(value) is staticmethod:
        return value.__func__
    return value


def _is_by_reference_udf(function):
    """True if cloudpickle carries this UDF by reference (importable module +
    qualname, not registered by value), so the worker re-imports the identical
    module namespace. Mirrors cloudpickle's ``_should_pickle_by_reference``.

    By-value UDFs (lambdas, ``<locals>`` closures, ``__main__`` scripts, or functions
    in a ``register_pickle_by_value`` module) reconstruct a minimal ``__globals__`` on
    the worker that differs from the driver's full module namespace, so traversing
    them by reference would diverge the manifest; their code is pinned by ``describe``
    (source/bytecode/closure) and the cloudpickle payload.
    """
    import sys
    import inspect
    module_name = getattr(function, "__module__", None)
    if not module_name or module_name == "__main__":
        return False
    module = sys.modules.get(module_name)
    if module is None:
        return False
    obj = module
    for part in getattr(function, "__qualname__", "").split("."):
        if part == "<locals>":
            return False
        obj = _static_reference_member(obj, part)
        if obj is None:
            return False
    if obj is not function:
        return False
    return not _module_registered_by_value(module_name)


def _is_by_reference_class(cls):
    """True if cloudpickle carries this class by reference (importable module +
    qualname, not registered by value), so the worker re-imports it; its defining
    module is then pinnable. A by-value class (``<locals>``/``__main__``/registered
    module) is reconstructed on the worker, so its defining module is never pinned
    (R4) — only its captured state is walked.
    """
    import sys
    import inspect
    module_name = _class_module(cls)
    if not module_name or module_name == "__main__":
        return False
    module = sys.modules.get(module_name)
    if module is None:
        return False
    obj = module
    for part in type.__dict__["__qualname__"].__get__(cls, type(cls)).split("."):
        if part == "<locals>":
            return False
        obj = _static_reference_member(obj, part)
        if obj is None:
            return False
    if obj is not cls:
        return False
    return not _module_registered_by_value(module_name)


def code_global_names(code):
    """Global names a code object (and its nested code constants) references.

    Mirrors cloudpickle's ``_extract_code_globals``: only global-referencing opcodes
    (LOAD/STORE/DELETE_GLOBAL), not the broader ``co_names`` (which also lists
    attribute/import names). This is exactly the globals subset cloudpickle carries
    by value, so the driver's full namespace and the worker's reconstructed subset
    reach the same values (F9). Freevars (closure cells) are reached via ``__closure__``.
    """
    import dis
    import opcode
    import types
    global_ops = {opcode.opmap[name] for name in
                  ("LOAD_GLOBAL", "STORE_GLOBAL", "DELETE_GLOBAL")}
    names = set()

    def walk(co):
        for instr in dis.get_instructions(co):
            if instr.opcode in global_ops:
                names.add(instr.argval)
        for const in co.co_consts:
            if isinstance(const, types.CodeType):
                walk(const)

    walk(code)
    return names


def _by_value_reachable(function):
    """Values a by-value function carries, mirroring cloudpickle ``_function_getstate``:
    referenced globals subset + closure cells + ``__defaults__`` + ``__kwdefaults__``
    + ``__dict__`` + ``__annotations__``. A business module in any of these travels by
    name (the worker re-imports it), so it must be pinned (F7c).

    Passive — reads attributes only, never executes code. An empty cell (ValueError)
    is skipped, mirroring ``describe``.
    """
    namespace = function.__globals__
    values = [namespace[name] for name in code_global_names(function.__code__)
              if name in namespace]
    for cell in function.__closure__ or ():
        try:
            values.append(cell.cell_contents)
        except ValueError:
            pass
    values.extend(function.__defaults__ or ())
    values.extend((function.__kwdefaults__ or {}).values())
    values.extend(function.__dict__.values())
    values.extend((function.__annotations__ or {}).values())
    return values


def _class_namespace(cls):
    # Read the built-in type storage, not a metaclass attribute or descriptor.
    return type.__dict__["__dict__"].__get__(cls, type(cls))


def _class_mro(cls):
    return type.__dict__["__mro__"].__get__(cls, type(cls))


def _class_module(cls):
    name = type.__dict__["__module__"].__get__(cls, type(cls))
    if type(name) is not str:
        raise RecoveryContractError("Class module name must be a plain string")
    return name


def _instance_namespace(value):
    import types
    for base in _class_mro(type(value)):
        descriptor = _class_namespace(base).get("__dict__")
        if type(descriptor) is types.GetSetDescriptorType:
            return descriptor.__get__(value, type(value))
    return None


def _declared_slots(cls):
    """Actual native slots, including private names and dict/iterator declarations."""
    import types
    slots = []
    for base in _class_mro(cls):
        namespace = _class_namespace(base)
        if '__slots__' in namespace:
            slots.extend(name for name, descriptor in namespace.items()
                         if name not in ('__dict__', '__weakref__')
                         and type(descriptor) is types.MemberDescriptorType)
    return slots


def _instance_slots(value):
    import types
    # Member descriptors are native storage readers. An arbitrary property with
    # the same name is never invoked while sealing code.
    for base in _class_mro(type(value)):
        namespace = _class_namespace(base)
        for slot in _declared_slots(base):
            descriptor = namespace.get(slot)
            if type(descriptor) is types.MemberDescriptorType:
                try:
                    yield descriptor.__get__(value, type(value))
                except AttributeError:
                    pass


def _is_walkable_value(value):
    """True if a value is a composite the dependency walk recurses (not a leaf).

    Filtering leaves before enqueue keeps the value budget meaningful: primitives,
    bytes and opaque C objects without introspectable state carry no by-name
    business module and are skipped passively.
    """
    import functools
    import inspect
    import types
    kind = type(value)
    if value is None or issubclass(kind, (str, int, float, bool, bytes, bytearray)):
        return False
    if issubclass(kind, (types.ModuleType, functools.partial,
                          list, tuple, set, frozenset, dict)):
        return True
    if kind in (types.FunctionType, types.MethodType) or issubclass(kind, type):
        return True
    if type(value) in (staticmethod, classmethod, property) or _instance_namespace(value) is not None:
        return True
    return bool(_declared_slots(type(value)))


def _by_value_business_modules(function):
    """Business modules reachable from a by-value UDF's captured state (gap④ R2).

    cloudpickle carries a by-value closure's captured modules BY NAME (the worker
    re-imports them), but ``describe`` records them name-only and the by-reference
    walk excludes by-value UDFs entirely. This walks the closure's referenced
    globals + cells and recurses composite capture forms — nested by-value
    functions/classes, bound methods, ``functools.partial`` (func/args/keywords),
    containers (list/tuple/dict/set/frozenset) and instances (type module if
    by-reference + ``__dict__`` + declared slots) — to reach every captured business
    module. The returned modules seed the SAME module BFS, which pins their source
    and recurses their full namespace (consistent worker-side once re-imported by
    name). Bounded (function/value budgets), cycle-safe (visited ids), passive (no
    code execution) and fail-closed (over-budget refuses). ``__main__`` and by-value
    defining modules are never pinned (R4); their own code is bound by ``describe``.
    """
    import functools
    import inspect
    import sys
    import types
    from collections import deque

    modules, pending, visited = [], deque([function]), set()
    functions = nodes = 0

    def enqueue(values):
        pending.extend(v for v in values if _is_walkable_value(v))
        if len(pending) > _DEP_MAX_NODES:
            raise RecoveryContractError(
                "Business dependency walk exceeds value budget; code-pin refuses (fail-closed)")

    while pending:
        value = pending.popleft()
        marker = id(value)
        if marker in visited:
            continue
        visited.add(marker)
        nodes += 1
        if nodes > _DEP_MAX_NODES:
            raise RecoveryContractError(
                "Business dependency walk exceeds value budget; code-pin refuses (fail-closed)")
        kind = type(value)
        if issubclass(kind, types.ModuleType):
            name = types.ModuleType.__dict__["__dict__"].__get__(value, kind).get("__name__", "")
            if name and name != "__main__":
                modules.append(value)
            continue
        if type(value) in (staticmethod, classmethod):
            enqueue((value.__func__,))
            continue
        if type(value) is property:
            enqueue((value.fget, value.fset, value.fdel))
            continue
        if kind is types.MethodType:
            enqueue((value.__self__, value.__func__))
            continue
        if kind is types.FunctionType:
            functions += 1
            if functions > _DEP_MAX_FUNCTIONS:
                raise RecoveryContractError(
                    "Business dependency walk exceeds function budget; code-pin refuses (fail-closed)")
            if _is_by_reference_udf(value):
                module = sys.modules.get(value.__module__)
                if module is not None:
                    pending.append(module)
            else:
                enqueue(_by_value_reachable(value))
            continue
        if issubclass(kind, type):
            if _is_by_reference_class(value):
                module = sys.modules.get(_class_module(value))
                if module is not None:
                    pending.append(module)
            else:
                enqueue(_class_namespace(value).values())
                enqueue(type.__dict__["__bases__"].__get__(value, type(value)))
                enqueue((type(value),))
            continue
        if issubclass(kind, functools.partial):
            fields = functools.partial.__dict__
            enqueue((fields['func'].__get__(value, kind),))
            enqueue(fields['args'].__get__(value, kind))
            enqueue(dict.values(fields['keywords'].__get__(value, kind)))
            continue
        if issubclass(kind, dict):
            enqueue(dict.values(value))
            continue
        container = next((base for base in (list, tuple, set, frozenset)
                          if issubclass(kind, base)), None)
        if container is not None:
            enqueue(container.__iter__(value))
            continue
        cls = type(value)
        # cloudpickle serializes a by-value instance's class as well as its data.
        # Methods can carry by-name modules even when the instance dict is empty.
        # Re-enter the same BFS so cycles, function/value limits and source
        # digest/archive membership remain one authority.
        enqueue((cls,))
        state = _instance_namespace(value)
        if type(state) is dict:
            enqueue(state.values())
        enqueue(_instance_slots(value))
    return modules


def source_callable_roots(source):
    """Business callables a native source declaration carries, in input_path order.

    ``describe(source)`` binds a reducer's own source/bytecode/closure but never
    ``__globals__``, so an imported business helper escapes the code-pin exactly as
    a plan UDF's does (gap④). These roots seed the SAME module BFS in
    ``business_dependency_modules``; the dependency-traversal authority is not
    duplicated here.

    Only explicit native source types are walked, depth-first in structural order
    (a node's own callables, then ``left``/``right``/``inputs[i]``), so the root
    order is the stable input_path order and does not depend on dict iteration or
    object identity. Passive: no user property, ``__getattr__`` or callable is
    invoked. A nested ``PlanInput.plan``'s operations belong to that slice's own
    ``plan`` argument and are deliberately not re-walked. Shared substructures are
    visited per position (no implicit folding); a true cycle, an over-budget
    structure, or a callable that cannot be pinned fail closed.
    """
    import functools
    import inspect
    from ...data.plan import CallableSpec
    from ...data.sources import (
        FileSource, IterableSource, KeyedSource, PlanInput, SqlSource, UnionSource,
    )

    def pinnable(value):
        """Reduce a declared callable to the function whose code must be pinned."""
        if value is None or isinstance(value, (str, int, float, bool, bytes, bytearray)):
            return None
        if inspect.isfunction(value):
            return value
        if inspect.ismethod(value):
            return value.__func__
        if isinstance(value, CallableSpec):
            return pinnable(value.target)
        if isinstance(value, functools.partial):
            return pinnable(value.func)
        if callable(value):
            raise RecoveryContractError(
                "Source callable is not a pinnable function; code-pin refuses (fail-closed)")
        return None

    roots, pending = [], [(source, frozenset())]
    nodes = 0
    while pending:
        value, ancestors = pending.pop()
        if value is None:
            continue
        marker = id(value)
        if marker in ancestors:
            raise RecoveryContractError(
                "Source declaration is cyclic; code-pin refuses (fail-closed)")
        nodes += 1
        if nodes > _DEP_MAX_NODES:
            raise RecoveryContractError(
                "Source declaration exceeds value budget; code-pin refuses (fail-closed)")
        lineage = ancestors | {marker}
        children = []
        if type(value) is PlanInput:
            children.append(value.source)
        elif type(value) is KeyedSource:
            for name in ("reducer", "initial"):
                root = pinnable(getattr(value, name))
                if root is not None:
                    roots.append(root)
            kwargs = value.reducer_kwargs or {}
            for name in sorted(kwargs):
                root = pinnable(kwargs[name])
                if root is not None:
                    roots.append(root)
            children.extend((value.left, value.right))
        elif type(value) is UnionSource:
            children.extend(value.inputs)
        elif type(value) is SqlSource:
            root = pinnable(value.connection_factory)
            if root is not None:
                roots.append(root)
        elif type(value) is IterableSource:
            root = pinnable(value.factory)
            if root is not None:
                roots.append(root)
        elif type(value) is FileSource and value.format == 'csv':
            import pyarrow.csv as pacsv
            options = value.options.get('parse_options')
            if type(options) is pacsv.ParseOptions:
                callback = options.invalid_row_handler
                root = pinnable(callback)
                if root is not None:
                    # A native CSV partial carries its argument modules too;
                    # the existing bounded value walk owns those dependencies.
                    roots.append(callback if type(callback) is functools.partial else root)
        pending.extend((child, lineage) for child in reversed(children))
    return tuple(roots)


def business_dependency_modules(plan, *, source=None):
    """Transitive closure of business modules a plan's pure UDF maps import.

    ``describe`` binds a UDF's own source/bytecode/closure but never ``__globals__``,
    so an imported business helper escapes the code-pin (gap④). This walks each
    by-reference pure-UDF ``__globals__`` for business modules and recurses through
    their own namespaces, pinning every business module's source. A by-value front-end
    additionally reaches the business modules a closure/lambda/``__main__`` UDF
    CAPTURES (cells + instance/partial/container state, R2), seeding the same BFS.
    Platform (``demiflow.*``), stdlib and site-packages third-party modules are
    skipped — platform source is provenance, not business code; third-party env
    resolution is deferred (D5). Bounded (≤64 modules, plus
    function/value walk budgets), deterministic (sorted by name), cycle-safe (visited
    set) and passive (no code execution). Over-budget or an unresolvable business
    source fail-closes at seal. The worker recomputes this from its own actual imports,
    so an edited helper changes the manifest and the load gate refuses. Returns
    source file fingerprints sorted by module name.
    """
    import inspect
    import sys
    import types
    from collections import deque
    from ..plan_facts import operation_facts

    def udfs():
        for operation in plan.operations:
            try:
                if operation_facts(operation).get("native") is not None:
                    continue
            except (RuntimeError, ValueError, TypeError):
                pass
            target = getattr(getattr(operation, "callable", None), "target", None)
            function = target.__self__ if type(target) is types.MethodType else target
            if type(function) is types.FunctionType and _is_by_reference_udf(function):
                yield function

    def candidates(namespace):
        for value in list(namespace.values()):
            if issubclass(type(value), types.ModuleType):
                yield value
            elif type(value) is types.FunctionType or issubclass(type(value), type):
                module_name = value.__module__ if type(value) is types.FunctionType else _class_module(value)
                module = sys.modules.get(module_name)
                if module is not None:
                    yield module

    def by_value_udfs():
        for operation in plan.operations:
            try:
                if operation_facts(operation).get("native") is not None:
                    continue
            except (RuntimeError, ValueError, TypeError):
                pass
            target = getattr(getattr(operation, "callable", None), "target", None)
            function = target.__self__ if type(target) is types.MethodType else target
            if type(function) is types.FunctionType and not _is_by_reference_udf(function):
                yield function
            elif function is not None and type(function) is not types.FunctionType:
                # A callable class/instance/partial is serialized too. Its
                # by-value class methods and capture slots use the same walk.
                yield function

    pending = deque()
    for function in udfs():
        pending.extend(candidates(function.__globals__))
    # By-value front-end (gap④ R2): a closure/lambda/__main__ UDF is excluded above,
    # yet cloudpickle carries its captured business modules by name. Reach them
    # through cells + composite state and seed the SAME module BFS (unchanged).
    for function in by_value_udfs():
        pending.extend(_by_value_business_modules(function))
    # Source side of gap④: a native source's reducer/factory imports business
    # helpers through its own __globals__/captures, which describe(source) never
    # walks. Same roots, same by-reference split, same BFS and budgets.
    for function in source_callable_roots(source) if source is not None else ():
        if _is_by_reference_udf(function):
            pending.extend(candidates(function.__globals__))
        else:
            pending.extend(_by_value_business_modules(function))
    visited, found = set(), {}
    while pending:
        module = pending.popleft()
        name = getattr(module, "__name__", "")
        if not name or name in visited:
            continue
        visited.add(name)
        path = _classify_dependency_module(module)
        if path is None:
            continue
        if len(found) >= _DEP_MAX_MODULES:
            raise RecoveryContractError(
                "Business dependency manifest exceeds budget; code-pin refuses (fail-closed)")
        found[name] = SourceFile.capture(path)
        pending.extend(candidates(vars(module)))
    return {name: found[name] for name in sorted(found)}


def _business_dependency_manifest(plan, *, source=None):
    """Code-pin dependency digest: business module source identities, no paths.

    Paths are excluded so the driver-sealed and worker-recomputed manifests are
    identical for unedited code regardless of how each side resolved the module.
    """
    return {name: {"sha256": file.digest, "bytes": file.size}
            for name, file in business_dependency_modules(plan, source=source).items()}


def plan_code_pin(plan, *, source=None):
    """Code-sensitive identity of the plan's actually-executed operations.

    Mirrors ``plan_identity``'s operation identity: native domain facts for native
    actors (engine code expressed by native declarations), ``describe``
    for pure UDF maps so each business function's ``source_hash`` + ``code_record``
    is bound. Editing business code changes this pin even when cloudpickle carries
    an importable UDF by reference (pickle bytes unchanged). The ``dependencies``
    key pins the transitive business modules each UDF imports (``describe`` never
    walks ``__globals__``), so editing an imported helper also changes the pin.
    The worker recomputes it from the loaded plan alone, so it is the load-time
    code-pin authority. ``source`` adds the native source declaration's own
    callable dependencies (reducer/factory) to the same manifest; omitting it
    keeps the historical bytes exactly.
    """
    from ..recovery_identity import operation_identity
    operations = [operation_identity(operation) for operation in plan.operations]
    return {"operations": stable_code_identity(operations),
            "dependencies": _business_dependency_manifest(plan, source=source)}
