"""Project Python runtime: interpreter verification, single-package read-only loader, and confined launch."""
from __future__ import annotations

import ast
import hashlib
import importlib.abc
import importlib.machinery
import importlib.util
import os
from pathlib import Path
import site
import sys

from .io import ensure_directory, no_symlinks, publish_bytes
from .platform import bounded_json, content_digest
from .platform_environment import environment_inventory, read_bounded, regular_file_chunks
from .schema import ForgeError

SHARED_SOURCES_FORMAT = 'demiflow_workbench_shared_sources_v1'
SHARED_SOURCES_ENV = 'DEMIFLOW_WORKBENCH_SHARED_SOURCES'
FORMAT = SHARED_SOURCES_FORMAT
ENVIRONMENT = SHARED_SOURCES_ENV
MAX_SHARED_MODULES = 128
MAX_PROBE_BYTES = 512 * 1024
_ENV_LIMITS = dict(distributions=256, metadata_file_bytes=4 * 1024**2, metadata_bytes=16 * 1024**2)
_STARTUP_FILES = frozenset({'sitecustomize.py', 'usercustomize.py'})


def validate_python_executable(path) -> dict:
    """Validate an explicit executable project Python path without executing it."""
    if path is None or not isinstance(path, (str, Path)) or not str(path).strip():
        raise ForgeError('Configure an explicit executable project Python', code='invalid_request')
    p = Path(path)
    if not p.is_absolute() or '..' in p.parts or not p.is_file() or not os.access(p, os.X_OK):
        raise ForgeError('Configure an explicit executable project Python: ' + str(p), code='invalid_request')
    info = p.stat()
    return dict(
        path=str(p),
        binary=str(p.resolve()),
        device=info.st_dev,
        inode=info.st_ino,
        bytes=info.st_size,
        mtime_ns=info.st_mtime_ns,
    )


def _startup_config_digests() -> dict[str, str]:
    """Hash site-packages startup hooks (.pth, sitecustomize.py, usercustomize.py) into interpreter trust boundary."""
    digests: dict[str, str] = {}
    roots: list[Path] = []
    try:
        for entry in site.getsitepackages():
            p = Path(entry)
            if p.is_dir():
                roots.append(p.resolve())
    except Exception:
        pass
    seen_dirs: set[Path] = set()
    count = 0
    for base in sorted(set(roots)):
        if base in seen_dirs:
            continue
        seen_dirs.add(base)
        try:
            with os.scandir(base) as entries:
                for entry in sorted(entries, key=lambda e: e.name):
                    if not (entry.name.endswith('.pth') or entry.name in _STARTUP_FILES):
                        continue
                    count += 1
                    if count > 128:
                        break
                    try:
                        raw = read_bounded(Path(entry.path), 256 * 1024)
                    except (OSError, ForgeError):
                        digests[f'{base}/{entry.name}'] = 'unreadable'
                    else:
                        digests[f'{base}/{entry.name}'] = hashlib.sha256(raw).hexdigest()
        except OSError:
            continue
    return digests


def interpreter_identity(executable=None) -> dict:
    info = validate_python_executable(sys.executable if executable is None else executable)
    return dict(
        **info,
        prefix=str(Path(sys.prefix).resolve()),
        base_prefix=str(Path(sys.base_prefix).resolve()),
        startup_sha256=content_digest(_startup_config_digests()),
    )


def verify_interpreter(identity: dict) -> str:
    """Reject a changed project Python executable before starting any child interpreter."""
    if not isinstance(identity, dict) or not identity.get('path'):
        raise ForgeError('Invalid project Python identity', code='invalid_request')
    actual = validate_python_executable(identity['path'])
    if any(identity.get(key) != value for key, value in actual.items()):
        raise ForgeError('Configured project Python executable changed', code='conflict')
    return actual['path']


def current_environment(config: dict, *, check_conflicts: bool = False, probe_imports=(), strict: bool = True) -> dict:
    """Measure the current process Python environment without importing business code."""
    from importlib.machinery import PathFinder

    declared_reqs = list(config.get('requirements') or ())
    platform_reqs = list(config.get('platform_requirements') or declared_reqs)
    descriptor = dict(
        mode='source',
        name='project',
        version='0',
        extras={},
        requires=platform_reqs,
        project_sha256=content_digest(platform_reqs),
    )
    inventory = environment_inventory(descriptor, _ENV_LIMITS)
    shadowed: list[str] = []
    if check_conflicts:
        for name in config.get('packages') or ():
            if name in sys.stdlib_module_names or name in sys.modules or PathFinder.find_spec(name) is not None:
                shadowed.append(name)
    problems = [row for row in inventory['diagnostics'] if row['code'] != 'direct_requirement_not_attested']
    # Also map failures back to project-declared requirement specs for crisp diagnostics.
    req_by_norm: dict[str, str] = {}
    from packaging.requirements import Requirement
    for spec in declared_reqs:
        try:
            req = Requirement(spec)
            req_by_norm[req.name.lower().replace('_', '-')] = spec
        except Exception:
            pass
    missing_requirements: list[dict] = []
    for row in problems:
        norm = str(row.get('name', '')).lower().replace('_', '-')
        spec = req_by_norm.get(norm)
        missing_requirements.append(dict(
            name=row.get('name', ''),
            spec=spec or row.get('name', ''),
            code=row.get('code', 'missing'),
            detail=f"项目 Python 环境中依赖 {spec or row.get('name', '')} 校验失败（{row.get('code', 'missing')}）。",
        ))
    import_probe = None
    if probe_imports:
        found, missing = [], []
        for top in sorted(set(probe_imports)):
            if top in sys.stdlib_module_names or top == 'demiflow':
                found.append(top)
                continue
            try:
                spec_obj = PathFinder.find_spec(top)
            except (ImportError, ValueError, OSError):
                spec_obj = None
            if spec_obj is not None:
                found.append(top)
            else:
                missing.append(top)
        import_probe = dict(found=found, missing=missing)
    if strict:
        if shadowed:
            raise ForgeError('Dependency source shadows an installed or standard module: ' + shadowed[0], code='conflict')
        if problems:
            raise ForgeError('Project Python dependency verification failed: ' + problems[0]['name'], code='conflict')
    env_record = dict(interpreter=interpreter_identity(), inventory=inventory)
    if strict:
        return env_record
    return dict(
        environment=env_record,
        missing_requirements=missing_requirements,
        shadowed_packages=shadowed,
        import_probe=import_probe,
    )


def _probe_runtime_dirs(interpreter: dict) -> list[Path]:
    """Derive read-only Python runtime roots purely from filesystem metadata before spawning Python."""
    dirs: set[Path] = {Path(__file__).resolve().parents[2]}
    for key in ('prefix', 'base_prefix'):
        raw = interpreter.get(key)
        if raw:
            p = Path(raw).resolve()
            if p != Path('/') and p.exists():
                dirs.add(p)
    for key in ('path', 'binary'):
        raw = interpreter.get(key)
        if not raw:
            continue
        p = Path(raw)
        if p.exists():
            dirs.add(p.parent.resolve())
            cand = p.parent.parent.resolve() if p.parent.name in ('bin', 'Scripts') else p.parent.resolve()
            if cand != Path('/') and cand.exists():
                dirs.add(cand)
                cfg = cand / 'pyvenv.cfg'
                if cfg.is_file() and not cfg.is_symlink():
                    try:
                        for line in read_bounded(cfg, 8192).decode('utf-8', errors='replace').splitlines():
                            k, sep, v = line.partition('=')
                            if sep and k.strip() == 'home' and v.strip():
                                home_dir = Path(v.strip()).resolve()
                                if home_dir.exists() and home_dir != Path('/'):
                                    dirs.add(home_dir)
                                    base = home_dir.parent.resolve()
                                    if base != Path('/') and base.exists() and home_dir.name in ('bin', 'Scripts'):
                                        dirs.add(base)
                    except (OSError, ValueError):
                        pass
    for sys_dir in ('/usr', '/lib', '/lib64'):
        p = Path(sys_dir)
        if p.exists():
            dirs.add(p)
    return sorted(dirs)


def launch_confined_probe(
    command: list[str],
    *,
    interpreter: dict,
    seconds: float = 30,
    rss_bytes: int = 512 * 1024**2,
    cwd,
    env: dict[str, str],
    stdin=None,
):
    """Launch a strictly read-only, network-disabled interpreter probe inside Forge's verifier sandbox."""
    from .sandbox import SandboxSpec, launch_sandbox

    if sys.platform not in {'linux', 'darwin'}:
        raise ForgeError('No confined project Python probe backend on this platform', code='operation_rejected')
    cwd_path = Path(cwd).resolve()
    home = cwd_path / 'home'
    home.mkdir(mode=0o700, exist_ok=True)
    runtime_dirs = _probe_runtime_dirs(interpreter)
    spec = SandboxSpec(
        read_paths=(str(cwd_path),),
        traversal_paths=(),
        write_paths=(),
        runtime_paths=tuple(map(str, runtime_dirs)),
        protected_paths=(),
        denied_paths=(),
        private_home=str(home),
        role='verifier',
        seconds=seconds,
        rss_bytes=rss_bytes,
        direct_network=False,
    )
    launch = launch_sandbox
    if sys.platform == 'darwin':
        from .sandbox.backends.macos_seatbelt import MacosSeatbeltBackend
        launch = MacosSeatbeltBackend().launch
    return launch(
        command,
        spec=spec,
        env={**env, 'HOME': str(home), 'TMPDIR': str(home)},
        cwd=str(cwd_path),
        stdin=stdin,
    )


def probe_environment(
    python,
    config: dict,
    *,
    expected: dict | None = None,
    identity_only: bool = False,
    strict: bool = True,
    check_conflicts: bool = True,
    probe_imports=(),
    confined: bool | None = None,
) -> dict:
    """Inspect an explicitly provided project Python in a bounded isolated subprocess (no credentials)."""
    interpreter = validate_python_executable(python)
    if expected is not None:
        if interpreter['path'] != expected['path']:
            raise ForgeError('Project Python differs from the expected executable', code='conflict')
        verify_interpreter(expected)
    script = (
        'import json,sys;data={"prefix":sys.prefix};'
        'print(json.dumps({"format":"dep_probe","data":data}))'
        if identity_only else
        'import json,sys;from importlib.util import find_spec;from pathlib import Path;'
        'sys.path.insert(0,sys.argv[1]);spec=find_spec("demiflow");'
        'assert spec and spec.origin and Path(spec.origin).resolve() == Path(sys.argv[1])/"demiflow/__init__.py";'
        'from demiflow.forge.runtime import current_environment;'
        'payload=json.load(sys.stdin);'
        'data=current_environment(payload["config"],check_conflicts=payload["check_conflicts"],'
        'probe_imports=payload["probe_imports"],strict=payload["strict"]);'
        'print(json.dumps({"format":"dep_probe","data":data}))'
    )
    from .platform_worker import capture
    body = {
        'config': {
            'requirements': list(config.get('requirements') or ()),
            'platform_requirements': list(config.get('platform_requirements') or config.get('requirements') or ()),
            'packages': sorted(config.get('packages') or ()),
        },
        'check_conflicts': bool(check_conflicts),
        'probe_imports': list(probe_imports or ()),
        'strict': bool(strict),
    }
    launcher = None
    in_sandbox = os.environ.get('DEMIFLOW_FORGE_SANDBOX_ACTIVE') == '1'
    use_confined = False
    if confined is True:
        use_confined = not in_sandbox
    elif confined is None and sys.platform == 'linux' and not in_sandbox:
        from .sandbox import backend_readiness
        use_confined = bool(backend_readiness().get('available'))
    if use_confined:
        launcher = lambda command, *, stdin, cwd, env, **_: launch_confined_probe(
            command, interpreter=interpreter, seconds=30, rss_bytes=512 * 1024**2,
            cwd=cwd, env=env, stdin=stdin,
        )
    result = capture(
        [interpreter['path'], '-I', '-B', '-c', script, str(Path(__file__).resolve().parents[2])],
        body,
        {'timeout_seconds': 30, 'catalog_bytes': MAX_PROBE_BYTES, 'stderr_bytes': 8192},
        response_formats={'dep_probe'},
        input_limit=128 * 1024,
        tree_rss_limit=512 * 1024**2,
        inherit_authority=False,
        launcher=launcher,
    )['data']
    if not identity_only:
        env_obj = result if strict else result['environment']
        if any(env_obj['interpreter'].get(key) != val for key, val in interpreter.items()):
            raise ForgeError('Project interpreter supply differs from the inspected binary', code='conflict')
    return result


# ---------------------------------------------------------------------------
# Single-package read-only Python source loader & sealing helpers
# ---------------------------------------------------------------------------

def deployment_modules(root) -> dict | None:
    """Read host-deployed shared Python source map outside the workspace."""
    setting = os.environ.get(SHARED_SOURCES_ENV)
    if not setting:
        return None
    from .project_io import read_control

    path = Path(setting)
    if not path.is_absolute() or '..' in path.parts or path.is_relative_to(Path(root).absolute()):
        raise ForgeError('Shared Python source policy must be a deployment file outside the workspace')
    value = read_control(no_symlinks(path), 256 * 1024)
    if (type(value) is not dict or set(value) != {'format', 'workspaces'}
            or value['format'] != SHARED_SOURCES_FORMAT or type(value['workspaces']) is not dict
            or len(value['workspaces']) > 128):
        raise ForgeError('Invalid shared Python source policy')
    modules = value['workspaces'].get(str(Path(root).absolute()), {})
    if type(modules) is not dict:
        raise ForgeError('Invalid workspace shared Python source map')
    return modules


def _parts(name) -> list[str]:
    parts = name.split('.') if type(name) is str and len(name) <= 256 else []
    if (not parts or len(parts) > 12 or any(not part.isidentifier() or part == '__init__' for part in parts)
            or parts[0] in sys.stdlib_module_names or parts[0] == 'demiflow'):
        raise ForgeError('Invalid shared Python module name')
    return parts


def _origin(root: Path, name: str, value: str) -> Path:
    parts = _parts(name)
    if type(value) is not str or len(value) > 4096:
        raise ForgeError('Invalid shared Python source path')
    path = Path(value)
    if (not path.is_absolute() or str(path) != value or '..' in path.parts or path.is_relative_to(root)
            or not (path.name == parts[-1] + '.py'
                    or path.name == '__init__.py' and path.parent.name == parts[-1])):
        raise ForgeError('Shared Python module must map to its named source file outside the workspace')
    return path


def sealed_path(name: str, origin) -> Path:
    parts = _parts(name)
    return Path('source', *parts, '__init__.py') if Path(origin).name == '__init__.py' else Path(
        'source', *parts[:-1], parts[-1] + '.py'
    )


def bound_files(binding: dict):
    for name, row in binding.get('shared_sources', {}).items():
        yield name, Path(row['path']), sealed_path(name, row['path']), row['sha256']


def read_source_bytes(path) -> bytes:
    path = no_symlinks(Path(path))
    return read_bounded(path, path.stat().st_size)


def source_digest(path, maximum=None) -> str:
    digest = hashlib.sha256()
    for chunk in regular_file_chunks(path, maximum):
        digest.update(chunk)
    return digest.hexdigest()


def read_sealed(name: str, path, digest: str) -> bytes:
    raw = read_source_bytes(path)
    if hashlib.sha256(raw).hexdigest() != digest:
        raise ForgeError('Sealed shared Python source differs: ' + name, code='conflict')
    return raw


class SharedSourceImports(importlib.abc.MetaPathFinder):
    """Import ONLY the explicitly declared files. Never exposes parent/sibling directories to sys.path."""

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
                fullname,
                source,
                loader=Loader(fullname, str(source)),
                submodule_search_locations=[] if source.name == '__init__.py' else None,
            )
        if fullname in self.namespaces:
            spec = importlib.machinery.ModuleSpec(fullname, None, is_package=True)
            spec.submodule_search_locations = []
            return spec
        if fullname.split('.')[0] in self.roots:
            raise ModuleNotFoundError('Shared Python module is outside the declared source files', name=fullname)
        return None


class SharedSources:
    def __init__(self, root, configured, *, dependencies=None):
        from .dependencies import module_files
        root = Path(root).absolute()
        if type(configured) is not dict or len(configured) > MAX_SHARED_MODULES:
            raise ForgeError('Shared Python sources exceed the module budget')
        self.paths = {name: _origin(root, name, value) for name, value in configured.items()}
        self.managed = module_files(root, dependencies)
        if {name.partition('.')[0] for name in self.paths} & {name.partition('.')[0] for name in self.managed}:
            raise ForgeError('Project dependency conflicts with a deployment shared package', code='conflict')
        self.paths.update((name, Path(row['path'])) for name, row in self.managed.items())
        if len(set(self.paths.values())) != len(self.paths):
            raise ForgeError('A shared Python file must have one module name')
        self.digests = {name: row['sha256'] for name, row in self.managed.items()}
        local = set()
        for name in {name.split('.')[0] for name in self.paths}:
            if (root / name).is_dir() or (root / (name + '.py')).is_file():
                local.add(name)
        self.importer = SharedSourceImports(self.paths, read=self.read, project_roots=local)

    def read(self, name: str) -> bytes:
        managed = self.managed.get(name)
        raw = read_bounded(self.paths[name], managed['bytes']) if managed else read_source_bytes(self.paths[name])
        digest = hashlib.sha256(raw).hexdigest()
        if name in self.digests and self.digests[name] != digest:
            raise ForgeError('Shared Python source changed: ' + name, code='conflict')
        self.digests[name] = digest
        return raw

    def binding(self) -> dict:
        for name in sorted(self.paths):
            managed = self.managed.get(name)
            digest = source_digest(self.paths[name], managed['bytes'] if managed else None)
            if name in self.digests and self.digests[name] != digest:
                raise ForgeError('Shared Python source changed: ' + name, code='conflict')
            self.digests[name] = digest
        return {name: dict(path=str(self.paths[name]), sha256=self.digests[name]) for name in sorted(self.paths)}


def package_parts(package) -> tuple[str, ...]:
    if package is None:
        return ()
    if type(package) is not str or len(package) > 256:
        raise ForgeError('Invalid workspace Python package')
    parts = tuple(package.split('.'))
    if len(parts) > 8 or any(not part.isidentifier() for part in parts):
        raise ForgeError('Invalid workspace Python package')
    return parts


def sealed_root(directory: Path, binding: dict) -> Path:
    return Path(directory).joinpath('source', *package_parts(binding.get('source_package')))


class PythonSources(importlib.abc.MetaPathFinder):
    """Read and pin local project source, with scoped support for qualified package names."""

    def __init__(self, root, *, shared_imports=None):
        self.root = Path(root)
        self.shared_imports = shared_imports
        self.package: tuple[str, ...] = ()
        self.digests: dict[str, str] = {}

    def read(self, path) -> bytes:
        path = no_symlinks(Path(path))
        name = path.relative_to(self.root).as_posix()
        raw = read_source_bytes(path)
        digest = hashlib.sha256(raw).hexdigest()
        if name in self.digests and self.digests[name] != digest:
            raise ForgeError('Source changed during plan construction', code='conflict')
        if name not in self.digests and len(self.digests) >= 256:
            raise ForgeError('Plan source closure exceeds module count budget', code='resource_limit')
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

    def entry_module(self, path: Path) -> str:
        tree = ast.parse(self.read(path), filename=str(path))
        self._learn_package(tree)
        relative = path.relative_to(self.root).with_suffix('').parts
        relative_to_project = any(
            isinstance(node, ast.ImportFrom) and node.level == len(relative)
            for node in ast.walk(tree)
        )
        if not self.package and self.root.name.isidentifier() and (
                relative_to_project or no_symlinks(self.root / '__init__.py').is_file()):
            self._select_package((self.root.name,))
        return '.'.join((*self.package, *relative))

    def missing_import(self, error) -> ForgeError:
        error_path = None
        trace = error.__traceback__
        while trace is not None:
            path = Path(trace.tb_frame.f_code.co_filename)
            if path.is_relative_to(self.root):
                error_path = f'{path.relative_to(self.root).as_posix()}:{trace.tb_lineno}'
            trace = trace.tb_next
        where = f'（{error_path}）' if error_path else ''
        return ForgeError(
            '管道依赖无法导入：' + str(error.name or error)[:256] + where +
            '。请核对项目 pyproject.toml 声明的依赖包及项目 Python 环境。',
            code='operation_rejected',
        )

    def _source_spec(self, fullname, path, *, locations=None):
        sources = self

        class Loader(importlib.machinery.SourceFileLoader):
            def get_code(self, name):
                tree = ast.parse(sources.read(self.path), filename=self.path)
                sources._learn_package(tree)
                return compile(tree, self.path, 'exec', dont_inherit=True)

        return importlib.util.spec_from_file_location(
            fullname, path, loader=Loader(fullname, str(path)), submodule_search_locations=locations,
        )

    def find_spec(self, fullname, path=None, target=None):
        parts = tuple(fullname.split('.'))
        if parts == self.package[:len(parts)]:
            if parts == self.package:
                init = self.root / '__init__.py'
                if no_symlinks(init).is_file():
                    return self._source_spec(fullname, init, locations=[str(self.root)])
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
        for name, digest in self.digests.items():
            if source_digest(self.root / name) != digest:
                raise ForgeError('Source changed during plan construction', code='conflict')


def install_bound_imports(modules: dict, *, project_roots=(), project_package=()) -> SharedSourceImports:
    """Install the exact-name loader from a trusted immutable module inventory (sealed run / Notebook snapshot)."""
    paths = {name: Path(row['path']) for name, row in modules.items()}

    def read(name: str) -> bytes:
        row = modules[name]
        raw = read_bounded(paths[name], row['bytes'])
        if hashlib.sha256(raw).hexdigest() != row['sha256']:
            raise ForgeError('Bound dependency bytes changed: ' + name, code='conflict')
        return raw

    finder = SharedSourceImports(paths, read=read, project_roots=project_roots)
    finder.set_project_package(project_package)
    sys.meta_path.insert(0, finder)
    return finder


def install_live_project_imports(
    root,
    *,
    allowed_roots=None,
    denied_paths=(),
    shared_sources=None,
    project_roots=(),
    project_package=(),
) -> SharedSourceImports | None:
    """Resolve pyproject.toml LIVE at Python startup and install exact single-package imports.

    Editing pyproject.toml (e.g. changing local_ref v1 -> v2 within authorized read scope)
    or vendoring/removing packages takes effect on the very next python/pytest invocation
    without needing any lock refresh. Missing declared packages are skipped gracefully so
    partial setup can still run tests while unauthorized external paths are rejected.
    """
    from .dependencies import inspect_dependencies, module_files
    root_path = Path(root).absolute()
    inspected = inspect_dependencies(
        root_path,
        allowed_roots=allowed_roots,
        denied_paths=denied_paths,
        check_imports=True,
        check_environment=False,
        allow_partial=True,
    )
    if not inspected['declared'] or inspected['declaration_error']:
        return None
    # Fail loudly if any declared external local_ref violated host authorization or security bounds.
    for invalid in inspected['invalid_packages']:
        if invalid.get('code') == 'operation_rejected':
            raise ForgeError(invalid['detail'], code='operation_rejected')
    managed = module_files(root_path, inspected['packages'])
    if not managed:
        return None
    shared_roots = {name.partition('.')[0] for name in (shared_sources or ())}
    managed_roots = {name.partition('.')[0] for name in managed}
    if shared_roots & managed_roots:
        conflict = sorted(shared_roots & managed_roots)[0]
        raise ForgeError('Project dependency conflicts with a deployment shared package: ' + conflict, code='conflict')
    local_roots = set(project_roots)
    for name in managed_roots:
        if (root_path / name).is_dir() or (root_path / (name + '.py')).is_file():
            local_roots.add(name)
    return install_bound_imports(managed, project_roots=local_roots, project_package=project_package)


def import_bootstrap(directory, modules: dict, *, platform_root, project_roots=(), project_package=()) -> Path:
    """Write a fixed sitecustomize.py startup hook for a confined business child process."""
    directory = Path(directory)
    ensure_directory(directory)
    binding_path = directory / 'dependencies.json'
    bootstrap_path = directory / 'sitecustomize.py'
    configuration = dict(
        modules=modules,
        project_roots=sorted(project_roots),
        project_package=list(project_package),
        platform_root=str(platform_root),
    )
    publish_bytes(binding_path, bounded_json(configuration, 512 * 1024), create=False)
    body = r'''import json
import os
from pathlib import Path
try:
    from importlib.util import find_spec
    with Path(__file__).with_name('dependencies.json').open() as stream:
        binding = json.load(stream)
    package_root = Path(binding.pop('platform_root')) / 'demiflow'
    spec = find_spec('demiflow')
    if spec is None or not spec.origin or Path(spec.origin).resolve() != package_root / '__init__.py':
        raise RuntimeError('DemiFlow package origin changed')
    from demiflow.forge import runtime as forge_runtime
    if Path(forge_runtime.__file__).resolve() != package_root / 'forge/runtime.py':
        raise RuntimeError('DemiFlow source loader origin changed')
    forge_runtime.install_bound_imports(**binding)
except BaseException:
    os.write(2, b'DemiFlow dependency bootstrap failed\n')
    os._exit(78)
'''
    publish_bytes(bootstrap_path, body.encode(), create=False)
    return directory


def create_dependency_snapshot(
    snapshot_base: Path,
    root: Path,
    bound: dict,
    *,
    platform_root: Path | str,
    max_snapshots: int = 16,
) -> tuple[Path, Path]:
    """Freeze live verified B-class package bytes into an immutable snapshot directory + bootstrap hook."""
    from .dependencies import module_files
    snapshot_base = Path(snapshot_base)
    ensure_directory(snapshot_base)
    snapshot = snapshot_base / bound['digest']
    if not snapshot.exists():
        with os.scandir(snapshot_base) as entries:
            if sum(1 for _ in zip(range(max_snapshots), entries)) >= max_snapshots:
                raise ForgeError('Dependency snapshots exceed their retention budget', code='resource_limit')
    ensure_directory(snapshot)
    modules = {}
    for name, proof in module_files(root, bound).items():
        source = Path(proof['path'])
        parts = name.split('.')
        target = snapshot.joinpath('sources', *parts)
        target = target / '__init__.py' if source.name == '__init__.py' else target.with_suffix('.py')
        raw = read_bounded(source, proof['bytes'])
        if hashlib.sha256(raw).hexdigest() != proof['sha256']:
            raise ForgeError('Dependency source changed during snapshot freeze: ' + name, code='inputs_changed')
        ensure_directory(target.parent)
        try:
            publish_bytes(target, raw)
        except ForgeError:
            if not target.exists() or read_bounded(target, proof['bytes']) != raw:
                raise ForgeError('Dependency snapshot differs from expected bytes: ' + name, code='conflict')
        modules[name] = {**proof, 'path': str(target)}
    bootstrap = import_bootstrap(snapshot / 'bootstrap', modules, platform_root=platform_root)
    return snapshot, bootstrap


def launch_confined_python(
    root: Path,
    command: list[str],
    *,
    interpreter: dict,
    dependencies: dict | None = None,
    source_modules: dict | None = None,
    read_paths=(),
    seconds: float = 60,
    rss_bytes: int = 2 * 1024**3,
    cwd,
    env: dict[str, str],
    stdin=None,
):
    """Launch a verifier-confined business Python subprocess using Forge's sandbox and dependency module paths."""
    from .dependencies import module_files
    from .sandbox import SandboxSpec, launch_sandbox

    if sys.platform != 'linux':
        raise ValueError('Confined project inspection requires the Linux sandbox backend')
    root = Path(root).absolute()
    cwd_path = Path(cwd)
    home = cwd_path / 'home'
    home.mkdir(mode=0o700, exist_ok=True)
    files = {Path(p) for p in read_paths}
    source_files = {Path(row['path']) for row in module_files(root, dependencies).values()}
    source_files.update(map(Path, (source_modules or {}).values()))
    traversal = {root, *(path.parent for path in source_files)}
    files.update(source_files)
    runtime_dirs = {
        Path(__file__).resolve().parents[2],
        Path(interpreter['prefix']),
        Path(interpreter['base_prefix']),
        Path(interpreter['binary']),
    }
    runtime_dirs.update(path for path in map(Path, ('/usr', '/lib', '/lib64')) if path.exists())
    if any(root.is_relative_to(path) for path in runtime_dirs):
        raise ValueError('Project Python runtime root must not cover the project directory')
    spec = SandboxSpec(
        read_paths=tuple(map(str, sorted(files | {cwd_path}))),
        traversal_paths=tuple(map(str, sorted(traversal))),
        write_paths=(),
        runtime_paths=tuple(sorted(map(str, runtime_dirs))),
        protected_paths=(),
        denied_paths=(),
        private_home=str(home),
        role='verifier',
        seconds=seconds,
        rss_bytes=rss_bytes,
    )
    return launch_sandbox(
        command,
        spec=spec,
        env={**env, 'HOME': str(home), 'TMPDIR': str(home)},
        cwd=cwd,
        stdin=stdin,
    )
