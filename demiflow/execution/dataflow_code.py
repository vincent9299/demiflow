"""Bounded, content-pinned business imports delivered with each program."""
import hashlib
import inspect
import os
from pathlib import Path, PurePosixPath
import stat
import sys

from .contracts.dataflow_errors import DataflowError
from .recovery_contract import identity

SOURCE_MEDIA = 'text/x-python'
MAX_SOURCES = 64
MAX_SOURCE_BYTES = 8 * 1024**2


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


def _read_member(path, limit):
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
            if info.st_size > limit:
                raise DataflowError('BUDGET_EXCEEDED', 'Package member exceeds the remaining source budget')
            data = stream.read(limit + 1)
            if len(data) != info.st_size:
                raise DataflowError('CODE_MISMATCH', 'Package member changed during sealing')
            return data
    except OSError as exc:
        raise DataflowError('CODE_MISMATCH', 'Cannot read a direct declared package member: ' + str(path)) from exc
    finally:
        os.close(fd)


def _add_declared_members(target, sources):
    declared = inspect.getattr_static(target, '__demiflow_package_files__', None)
    if declared is None:
        return
    if type(declared) is not dict or len(declared) > MAX_SOURCES:
        raise DataflowError('INVALID_CONTRACT', 'Package files require a bounded package-to-tuple mapping')
    total = sum(len(raw) for _, raw in sources.values())
    for package, files in declared.items():
        _package_name(package)
        module = sys.modules.get(package)
        # Only loaded business packages can contribute files. Declaring a name
        # must never import or execute that package to locate it.
        from .chain import _classify_dependency_module
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
            remaining = MAX_SOURCE_BYTES - total + (len(old[1]) if old is not None else 0)
            raw = _read_member(member, remaining)
            if old is not None and (old[0] != member or old[1] != raw):
                raise DataflowError('CODE_MISMATCH', 'One package member has conflicting source bytes')
            if old is None:
                total += len(raw)
            sources[name] = (member, raw)


def business_sources(kind, value):
    from .chain import _business_dependency_modules
    from ..data.plan import LogicalPlan, MapOp
    if kind == 'callable':
        sources = _business_dependency_modules(LogicalPlan((MapOp(callable=value),)))
        _add_declared_members(value.target, sources)
    elif kind == 'source':
        sources = _business_dependency_modules(LogicalPlan(()), source=value)
    else:
        sources = {}
    # A package initializer executes before its member; pin it as actual code.
    for name in tuple(sources):
        if ':' in name:
            continue
        parts = name.split('.')
        source_path = sources[name][0]
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
                if path.name != '__init__.py' or path.stat().st_size > MAX_SOURCE_BYTES:
                    raise DataflowError('CODE_MISMATCH', 'Business package has no bounded Python source')
                remaining = MAX_SOURCE_BYTES - sum(len(raw) for _, raw in sources.values())
                sources[parent] = (path, _read_member(path, remaining))
    if len(sources) > MAX_SOURCES or sum(len(raw) for _, raw in sources.values()) > MAX_SOURCE_BYTES:
        raise DataflowError('BUDGET_EXCEEDED', 'Business imports exceed their source budget')
    return sources


def source_identity(sources):
    return {name: {'sha256': hashlib.sha256(raw).hexdigest(), 'bytes': len(raw),
                   'package': path.name == '__init__.py'}
            for name, (path, raw) in sorted(sources.items())}


def stage_sources(sources, *, store):
    from dataclasses import asdict
    return {name: asdict(store.put(iter((raw,)), max_bytes=max(1, len(raw)), media_type=source_media(name)))
            for name, (_, raw) in sorted(sources.items())}


def mount_sources(refs, expected, *, store, cache):
    """Populate the managed worker cache only from sealed, hash-verified bytes."""
    from .seal.plan import _artifact_from_json, _read_bytes
    from .state_artifacts import atomic_bytes
    if (not isinstance(refs, dict) or set(refs) != set(expected)
            or len(refs) > MAX_SOURCES
            or sum(fact['bytes'] for fact in expected.values()) > MAX_SOURCE_BYTES):
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
        raw = _read_bytes(store, ref, max_bytes=MAX_SOURCE_BYTES, media_type=source_media(name))
        if target.exists():
            if target.stat().st_size != len(raw) or target.read_bytes() != raw:
                raise DataflowError('CODE_MISMATCH', 'Cached business code differs')
        else:
            target.parent.mkdir(parents=True, exist_ok=True)
            atomic_bytes(target, raw)
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
