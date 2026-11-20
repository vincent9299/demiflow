"""Import, verify and seal the bounded Python source closure of a Workbench flow."""

import ast
import hashlib
import importlib.abc
import importlib.machinery
import importlib.util
import os
import sys
from pathlib import Path

from demiflow.forge.io import no_symlinks
from demiflow.forge.platform_environment import read_bounded
from demiflow.forge.schema import ForgeError

FORMAT = 'demiflow_workbench_shared_sources_v1'
ENVIRONMENT = 'DEMIFLOW_WORKBENCH_SHARED_SOURCES'
MAX_MODULES = 128
MAX_MODULE_BYTES = 1024**2
MAX_SOURCE_BYTES = 8 * 1024**2


def deployment_modules(root):
    """Read the host policy in the trusted parent, outside candidate imports."""
    setting = os.environ.get(ENVIRONMENT)
    if not setting:
        return None
    from demiflow.workbench.projects.io import read_control

    path = Path(setting)
    if not path.is_absolute() or '..' in path.parts or path.is_relative_to(root):
        raise ForgeError('Shared Python source policy must be a deployment file outside the workspace')
    value = read_control(no_symlinks(path), 256 * 1024)
    if (type(value) is not dict or set(value) != {'format', 'workspaces'}
            or value['format'] != FORMAT or type(value['workspaces']) is not dict
            or len(value['workspaces']) > 128):
        raise ForgeError('Invalid shared Python source policy')
    modules = value['workspaces'].get(str(root), {})
    if type(modules) is not dict:
        raise ForgeError('Invalid workspace shared Python source map')
    return modules


def _parts(name):
    parts = name.split('.') if type(name) is str and len(name) <= 256 else []
    if (not parts or len(parts) > 12 or any(not part.isidentifier() or part == '__init__' for part in parts)
            or parts[0] in sys.stdlib_module_names or parts[0] == 'demiflow'):
        raise ForgeError('Invalid shared Python module name')
    return parts


def _origin(root, name, value):
    parts = _parts(name)
    if type(value) is not str or len(value) > 4096:
        raise ForgeError('Invalid shared Python source path')
    path = Path(value)
    if (not path.is_absolute() or str(path) != value or '..' in path.parts or path.is_relative_to(root)
            or not (path.name == parts[-1] + '.py'
                    or path.name == '__init__.py' and path.parent.name == parts[-1])):
        raise ForgeError('Shared Python module must map to its named source file outside the workspace')
    return path


def sealed_path(name, origin):
    parts = _parts(name)
    return Path('source', *parts, '__init__.py') if Path(origin).name == '__init__.py' else Path(
        'source', *parts[:-1], parts[-1] + '.py')


def bound_files(binding):
    for name, row in binding.get('shared_sources', {}).items():
        yield name, Path(row['path']), sealed_path(name, row['path']), row['sha256']


def read_sealed(name, path, digest):
    raw = read_bounded(no_symlinks(path), MAX_MODULE_BYTES)
    if hashlib.sha256(raw).hexdigest() != digest:
        raise ForgeError('Sealed shared Python source differs: ' + name, code='conflict')
    return raw


class SharedSourceImports(importlib.abc.MetaPathFinder):
    """Import only the declared files. Ancestors without files are namespaces."""

    def __init__(self, paths, *, read, project_roots=()):
        self.paths = paths
        self.read = read
        self.project_package = ''
        self.namespaces = set()
        self.roots = {name.split('.')[0] for name in paths}
        for name in paths:
            parent = name.rpartition('.')[0]
            while parent:
                if parent in paths and paths[parent].name != '__init__.py':
                    raise ForgeError('Shared Python module conflicts with its package: ' + parent)
                self.namespaces.add(parent)
                parent = parent.rpartition('.')[0]
        for name in sorted(self.roots):
            if name in sys.modules or name in project_roots:
                raise ForgeError('Shared Python package conflicts with an existing module: ' + name)

    def set_project_package(self, package):
        qualified = '.'.join(package)
        if qualified:
            for name, source in self.paths.items():
                if (name == qualified or name.startswith(qualified + '.')
                        or qualified.startswith(name + '.') and source.name != '__init__.py'):
                    raise ForgeError('Shared Python module overlaps the selected project package: ' + name)
        self.project_package = qualified

    def find_spec(self, fullname, path=None, target=None):
        if self.project_package and (fullname == self.project_package or fullname.startswith(self.project_package + '.')):
            return None
        source = self.paths.get(fullname)
        if source is not None:
            read = self.read

            class Loader(importlib.machinery.SourceFileLoader):
                def get_code(self, name):
                    return compile(read(name), self.path, 'exec', dont_inherit=True)

            return importlib.util.spec_from_file_location(
                fullname, source, loader=Loader(fullname, str(source)),
                submodule_search_locations=[] if source.name == '__init__.py' else None)
        if fullname in self.namespaces:
            spec = importlib.machinery.ModuleSpec(fullname, None, is_package=True)
            spec.submodule_search_locations = []
            return spec
        if fullname.split('.')[0] in self.roots:
            raise ModuleNotFoundError('Shared Python module is outside the declared source files', name=fullname)
        return None


class SharedSources:
    def __init__(self, root, configured):
        if type(configured) is not dict or len(configured) > MAX_MODULES:
            raise ForgeError('Shared Python sources exceed the module budget')
        self.paths = {name: _origin(root, name, value) for name, value in configured.items()}
        if len(set(self.paths.values())) != len(self.paths):
            raise ForgeError('A shared Python file must have one module name')
        self.digests = {}
        self.total = 0
        local = set()
        for name in {name.split('.')[0] for name in self.paths}:
            if (root / name).is_dir() or (root / (name + '.py')).is_file():
                local.add(name)
        self.importer = SharedSourceImports(self.paths, read=self.read, project_roots=local)

    def read(self, name):
        raw = read_bounded(no_symlinks(self.paths[name]), MAX_MODULE_BYTES)
        digest = hashlib.sha256(raw).hexdigest()
        if name in self.digests:
            if self.digests[name] != digest:
                raise ForgeError('Shared Python source changed: ' + name, code='conflict')
        else:
            self.total += len(raw)
            if self.total > MAX_SOURCE_BYTES:
                raise ForgeError('Shared Python sources exceed the byte budget', code='resource_limit')
            self.digests[name] = digest
        return raw

    def binding(self):
        # Include declared lazy imports used only by the eventual row callback.
        # This also rechecks every import the compiler actually executed.
        for name in sorted(self.paths):
            self.read(name)
        return {name: dict(path=str(self.paths[name]), sha256=self.digests[name]) for name in sorted(self.paths)}


def package_parts(package):
    if package is None:
        return ()
    if type(package) is not str or len(package) > 256:
        raise ForgeError('Invalid workspace Python package')
    parts = tuple(package.split('.'))
    if len(parts) > 8 or any(not part.isidentifier() for part in parts):
        raise ForgeError('Invalid workspace Python package')
    return parts


def sealed_root(directory, binding):
    return directory.joinpath('source', *package_parts(binding.get('source_package')))


class PythonSources(importlib.abc.MetaPathFinder):
    """Read and pin local source, with scoped support for qualified package names."""

    def __init__(self, root, *, shared_imports=None):
        self.root = root
        self.shared_imports = shared_imports
        self.package = ()
        self.digests = {}
        self.total = 0

    def read(self, path):
        path = no_symlinks(Path(path))
        name = path.relative_to(self.root).as_posix()
        raw = read_bounded(path, 4 * 1024**2)
        digest = hashlib.sha256(raw).hexdigest()
        if name in self.digests and self.digests[name] != digest:
            raise ForgeError('Source changed during plan construction', code='conflict')
        if name not in self.digests:
            self.total += len(raw)
            if len(self.digests) >= 256 or self.total > 32 * 1024**2:
                raise ForgeError('Plan source closure exceeds budget', code='resource_limit')
        self.digests[name] = digest
        return raw

    def _learn_package(self, tree):
        for count, node in enumerate(ast.walk(tree)):
            if count >= 100_000:
                raise ForgeError('Python module exceeds import analysis budget', code='resource_limit')
            if isinstance(node, ast.Import):
                names = (alias.name for alias in node.names)
            elif isinstance(node, ast.ImportFrom) and not node.level and node.module:
                names = (node.module, *(f'{node.module}.{alias.name}' for alias in node.names))
            else:
                continue
            for name in names:
                parts = tuple(name.split('.'))
                for depth in range(1, min(8, len(parts) - 1) + 1):
                    prefix, relative = parts[:depth], parts[depth:]
                    if prefix != self.root.parts[-depth:] or not all(part.isidentifier() for part in parts):
                        continue
                    if prefix == self.package:
                        continue
                    local = self.root.joinpath(*relative)
                    if not (no_symlinks(local).is_dir() or no_symlinks(local.with_suffix('.py')).is_file()):
                        continue
                    if self.package:
                        raise ForgeError('项目的 Python 包名称与另一个已使用的包冲突：' + '.'.join(prefix),
                                         code='operation_rejected')
                    self._select_package(prefix)

    def _select_package(self, package):
        if package[0] in sys.modules and (self.shared_imports is None or package[0] not in self.shared_imports.roots):
            raise ForgeError('项目的 Python 包名称与已加载依赖冲突：' + package[0], code='operation_rejected')
        package_parts('.'.join(package))
        if self.shared_imports is not None:
            self.shared_imports.set_project_package(package)
        self.package = package

    def entry_module(self, path):
        tree = ast.parse(self.read(path), filename=str(path))
        self._learn_package(tree)
        relative = path.relative_to(self.root).with_suffix('').parts
        relative_to_project = any(isinstance(node, ast.ImportFrom) and node.level == len(relative)
                                  for node in ast.walk(tree))
        if not self.package and self.root.name.isidentifier() and (
                relative_to_project or no_symlinks(self.root / '__init__.py').is_file()):
            self._select_package((self.root.name,))
        return '.'.join((*self.package, *relative))

    def missing_import(self, error):
        error_path = None
        trace = error.__traceback__
        while trace is not None:
            path = Path(trace.tb_frame.f_code.co_filename)
            if path.is_relative_to(self.root):
                error_path = f'{path.relative_to(self.root).as_posix()}:{trace.tb_lineno}'
            trace = trace.tb_next
        where = f'（{error_path}）' if error_path else ''
        return ForgeError('管道依赖无法导入：' + str(error.name or error)[:256] + where +
                          '。请核对项目内的 Python 包和工作台运行环境中的依赖。', code='operation_rejected')

    def _source_spec(self, fullname, path, *, locations=None):
        sources = self

        class Loader(importlib.machinery.SourceFileLoader):
            def get_code(self, name):
                tree = ast.parse(sources.read(self.path), filename=self.path)
                sources._learn_package(tree)
                return compile(tree, self.path, 'exec', dont_inherit=True)

        return importlib.util.spec_from_file_location(fullname, path, loader=Loader(fullname, str(path)),
                                                     submodule_search_locations=locations)

    def find_spec(self, fullname, path=None, target=None):
        parts = tuple(fullname.split('.'))
        if parts == self.package[:len(parts)]:
            if parts == self.package:
                init = self.root / '__init__.py'
                if no_symlinks(init).is_file():
                    return self._source_spec(fullname, init, locations=[str(self.root)])
            # Ancestors are namespaces for this workspace only. Their filesystem
            # paths and initializers are never put on the module search path.
            spec = importlib.machinery.ModuleSpec(fullname, None, is_package=True)
            spec.submodule_search_locations = [str(self.root)] if parts == self.package else []
            return spec
        spec = importlib.machinery.PathFinder.find_spec(fullname, path)
        if spec and spec.origin and Path(spec.origin).is_relative_to(self.root):
            source = Path(spec.origin)
            if source.suffix != '.py':
                raise ForgeError('Project imports must use original Python source', code='operation_rejected')
            return self._source_spec(fullname, source, locations=spec.submodule_search_locations)
        return None

    def verify(self):
        for name in tuple(self.digests):
            self.read(self.root / name)
