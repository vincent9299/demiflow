"""Bounded, content-pinned business imports delivered with each program."""
import hashlib
from pathlib import Path
import sys

from .contracts.dataflow_errors import DataflowError
from .recovery_contract import identity

SOURCE_MEDIA = 'text/x-python'
MAX_SOURCES = 64
MAX_SOURCE_BYTES = 8 * 1024**2


def business_sources(kind, value):
    from .chain import _business_dependency_modules
    from ..data.plan import LogicalPlan, MapOp
    if kind == 'callable':
        sources = _business_dependency_modules(LogicalPlan((MapOp(callable=value),)))
    elif kind == 'source':
        sources = _business_dependency_modules(LogicalPlan(()), source=value)
    else:
        sources = {}
    # A package initializer executes before its member; pin it as actual code.
    for name in tuple(sources):
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
                sources[parent] = (path, path.read_bytes())
    if len(sources) > MAX_SOURCES or sum(len(raw) for _, raw in sources.values()) > MAX_SOURCE_BYTES:
        raise DataflowError('BUDGET_EXCEEDED', 'Business imports exceed their source budget')
    return sources


def source_identity(sources):
    return {name: {'sha256': hashlib.sha256(raw).hexdigest(), 'bytes': len(raw),
                   'package': path.name == '__init__.py'}
            for name, (path, raw) in sorted(sources.items())}


def stage_sources(sources, *, store):
    from dataclasses import asdict
    return {name: asdict(store.put(iter((raw,)), max_bytes=max(1, len(raw)), media_type=SOURCE_MEDIA))
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
        if not name or any(not part.isidentifier() for part in name.split('.')):
            raise DataflowError('CORRUPT_ARTIFACT', 'Invalid Python module name')
        relative = Path(*name.split('.'))
        relative = relative / '__init__.py' if fact['package'] else relative.with_suffix('.py')
        target = root / relative
        if any(path.is_symlink() for path in (target, *target.parents)):
            raise DataflowError('CORRUPT_ARTIFACT', 'Business code cache contains a symlink')
        ref = _artifact_from_json(refs[name])
        if ref.digest != fact['sha256'] or ref.size_bytes != fact['bytes']:
            raise DataflowError('CODE_MISMATCH', 'Business source differs from the program pin')
        raw = _read_bytes(store, ref, max_bytes=MAX_SOURCE_BYTES, media_type=SOURCE_MEDIA)
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
