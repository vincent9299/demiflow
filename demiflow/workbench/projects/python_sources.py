"""Bounded Python source closure for the selected workspace."""

import ast
import hashlib
import importlib.abc
import importlib.machinery
import importlib.util
import sys
from pathlib import Path

from demiflow.forge.io import no_symlinks
from demiflow.forge.platform_environment import read_bounded
from demiflow.forge.schema import ForgeError


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
