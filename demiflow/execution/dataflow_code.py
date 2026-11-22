"""Bounded, content-pinned business imports delivered with each program."""
from dataclasses import dataclass
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
            source = SourceFile.capture(member)
            if old is not None and old != source:
                raise DataflowError('CODE_MISMATCH', 'One package member has conflicting source bytes')
            sources[name] = source


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
    from .seal.plan import _artifact_from_json, _artifact_chunks
    from .state_artifacts import atomic_chunks
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
