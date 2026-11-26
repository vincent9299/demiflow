"""Controller-owned read-only CLI and business-Python shims for development sandboxes.

Two strictly separated channels are exposed in a read-only directory on PATH:
1. ``demiflow``: allows ONLY ``demiflow forge ...`` subcommands so the agent uses
   the exact same Forge CLI as terminal/IDE workflows without exposing the
   workbench's Python environment to business code.
2. ``python`` / ``python3`` / ``pytest``: created ONLY when the project explicitly
   configures its own business Python interpreter. Never falls back to the
   workbench host Python. Adds a pure read-only ``demiflow`` SDK fallback loader
   (never platform site-packages), generic qualified project-package resolution,
   declared read-only ``shared-sources.json`` imports, and sandbox-safe pytest
   defaults so tests run cleanly inside Landlock + seccomp.
"""
from __future__ import annotations

import json
import os
from pathlib import Path
import shlex
import shutil
import sys
import tempfile

from demiflow.workbench.python_sources import deployment_modules
from demiflow.workbench.projects.registry import project_dependencies

_FORGE_SHIM_ENTRY = """\
import os
import sys
from pathlib import Path

sdk_root = sys.argv[1]
runtime_cfg = sys.argv[2]
mode = sys.argv[3]
args = sys.argv[4:]

if not args or args[0] != 'forge':
    sys.stderr.write('开发沙箱仅放行 `demiflow forge ...` 子命令。\\n')
    raise SystemExit(2)
if mode == 'restricted' and len(args) >= 2 and args[1] in {'review', 'develop'}:
    sys.stderr.write(
        '工作台受限沙箱内的独立评审由宿主自动执行（或调用 request_pipeline_review 工具），'
        '请勿在沙箱内直接运行 `demiflow forge review`。\\n'
    )
    raise SystemExit(2)
if mode == 'restricted' and len(args) >= 2 and args[1] == 'init':
    sys.stderr.write(
        '工作台受限沙箱内 `.forge/` 与 `workspace.json` 由宿主受控管理，请勿直接运行 `demiflow forge init`；'
        '如需创建缺失的标准目录或登记/更新 Flow 入口，请调用宿主工具 `manage_project_layout`。\\n'
    )
    raise SystemExit(2)

os.environ['DEMIFLOW_FORGE_RUNTIME_CONFIG'] = runtime_cfg
sys.path.insert(0, sdk_root)

import click
from demiflow.forge.cli import register

@click.group(name='demiflow')
def _cli():
    pass

register(_cli)
if mode == 'restricted' and len(args) >= 2 and args[1] == 'dep' and (
        len(args) < 3 or args[2] not in {'list', 'verify'}):
    raise SystemExit('Dependency updates require the trusted project owner')
from demiflow.forge.dependencies import host_approval
with open(runtime_cfg) as stream:
    import json
    approved = json.load(stream)
with host_approval(approved['root'], approved.get('dependency_lock'), approved.get('python')):
    _cli.main(args=args, prog_name='demiflow')
"""

_SITECUSTOMIZE = """\
import importlib.abc
import importlib.machinery
import importlib.util
import json
import os
from pathlib import Path
import sys


def _install():
    binding_path = Path(__file__).resolve().parent.parent / 'py_binding.json'
    if not binding_path.is_file():
        return
    try:
        cfg = json.loads(binding_path.read_text(encoding='utf-8'))
    except (OSError, ValueError):
        return

    sdk_root = cfg.get('sdk_root')
    project_root = Path(cfg['project_root']) if cfg.get('project_root') else None
    shared_raw = cfg.get('shared_sources') or {}

    # 1. Pure read-only demiflow SDK fallback (never adds workbench site-packages).
    if sdk_root:
        sdk_pkg = Path(sdk_root) / 'demiflow'
        need_sdk = False
        try:
            spec = importlib.util.find_spec('demiflow')
            if spec is None or not spec.origin:
                need_sdk = True
            else:
                with open(spec.origin, 'rb'):
                    pass
        except (OSError, ImportError, ValueError):
            need_sdk = True
        if need_sdk and (sdk_pkg / '__init__.py').is_file():
            class _DemiflowSdkFinder(importlib.abc.MetaPathFinder):
                def find_spec(self, fullname, path=None, target=None):
                    if fullname != 'demiflow' and not fullname.startswith('demiflow.'):
                        return None
                    parts = fullname.split('.')[1:]
                    base = sdk_pkg.joinpath(*parts) if parts else sdk_pkg
                    init = base / '__init__.py'
                    if init.is_file():
                        return importlib.util.spec_from_file_location(
                            fullname, str(init),
                            submodule_search_locations=[str(base)],
                        )
                    py = base.with_suffix('.py')
                    if py.is_file():
                        return importlib.util.spec_from_file_location(fullname, str(py))
                    return None
            sys.meta_path.insert(0, _DemiflowSdkFinder())

    # 2. Generic qualified project package resolution (e.g. parent.project.module)
    #    without ever listing blocked ancestor directories.
    if project_root is not None and project_root.is_dir():
        root_str = str(project_root)
        if root_str not in sys.path:
            sys.path.insert(0, root_str)
        root_parts = project_root.parts

        class _ProjectPackageFinder(importlib.abc.MetaPathFinder):
            def __init__(self):
                self._prefix = None

            def _detect_prefix(self, parts):
                if self._prefix is not None:
                    return self._prefix
                max_d = min(4, len(root_parts) - 1, len(parts))
                for depth in range(max_d, 0, -1):
                    cand = root_parts[-depth:]
                    if parts[:depth] == cand and all(p.isidentifier() for p in cand):
                        rem = parts[depth:]
                        if not rem:
                            self._prefix = cand
                            return cand
                        target = project_root.joinpath(*rem)
                        if target.is_dir() or target.with_suffix('.py').is_file():
                            self._prefix = cand
                            return cand
                return None

            def find_spec(self, fullname, path=None, target=None):
                parts = tuple(fullname.split('.'))
                if not parts or parts[0] in sys.stdlib_module_names or parts[0].startswith('_'):
                    return None
                prefix = self._prefix
                if prefix is None:
                    # Check if fullname could be an ancestor of a suffix-matched package.
                    max_d = min(4, len(root_parts) - 1)
                    for depth in range(max_d, 1, -1):
                        cand = root_parts[-depth:]
                        if len(parts) < depth and cand[:len(parts)] == parts and all(p.isidentifier() for p in cand):
                            # Verify at least one local package/module exists inside project_root.
                            spec = importlib.machinery.ModuleSpec(fullname, None, is_package=True)
                            spec.submodule_search_locations = []
                            return spec
                    prefix = self._detect_prefix(parts)
                if prefix is None:
                    return None
                if len(parts) < len(prefix) and prefix[:len(parts)] == parts:
                    spec = importlib.machinery.ModuleSpec(fullname, None, is_package=True)
                    spec.submodule_search_locations = []
                    return spec
                if parts[:len(prefix)] != prefix:
                    return None
                rem = parts[len(prefix):]
                base = project_root.joinpath(*rem) if rem else project_root
                init = base / '__init__.py'
                if init.is_file():
                    return importlib.util.spec_from_file_location(
                        fullname, str(init), submodule_search_locations=[str(base)]
                    )
                if base.is_dir():
                    spec = importlib.machinery.ModuleSpec(fullname, None, is_package=True)
                    spec.submodule_search_locations = [str(base)]
                    return spec
                py = base.with_suffix('.py')
                if py.is_file():
                    return importlib.util.spec_from_file_location(fullname, str(py))
                return None

        sys.meta_path.append(_ProjectPackageFinder())

    managed = cfg.get('project_dependencies') or {}
    locked_environment = cfg.get('dependency_environment')
    if locked_environment:
        sys.path.insert(0, sdk_root)
        try:
            sdk_pkg = Path(sdk_root) / 'demiflow'
            spec = importlib.util.find_spec('demiflow')
            if spec is None or not spec.origin or Path(spec.origin).resolve() != sdk_pkg / '__init__.py':
                raise RuntimeError('Project Python loaded another DemiFlow package')
            from demiflow.forge.dependencies import current_environment
            if current_environment({'requirements': cfg['dependency_requirements']}) != locked_environment:
                raise RuntimeError('Project Python environment changed')
            from demiflow.workbench.python_sources import install_bound_imports
            if managed:
                install_bound_imports(managed)
        except BaseException:
            os.write(2, b'Unable to load the approved project Python dependencies\\n')
            os._exit(78)

    # 3. Declared read-only shared Python sources (from shared-sources.json).
    if shared_raw:
        shared_paths = {name: Path(p) for name, p in shared_raw.items()}
        namespaces = set()
        for name in shared_paths:
            parent = name.rpartition('.')[0]
            while parent:
                namespaces.add(parent)
                parent = parent.rpartition('.')[0]

        class _DeclaredSharedFinder(importlib.abc.MetaPathFinder):
            def find_spec(self, fullname, path=None, target=None):
                src = shared_paths.get(fullname)
                if src is not None:
                    return importlib.util.spec_from_file_location(
                        fullname, str(src),
                        submodule_search_locations=[] if src.name == '__init__.py' else None,
                    )
                if fullname in namespaces:
                    spec = importlib.machinery.ModuleSpec(fullname, None, is_package=True)
                    spec.submodule_search_locations = []
                    return spec
                return None

        sys.meta_path.append(_DeclaredSharedFinder())

    # 4. Keep pytest config discovery inside project_root so Landlock-blocked
    #    ancestor pytest.ini/pyproject.toml files never crash collection.
    _argv = getattr(sys, 'orig_argv', None) or sys.argv
    if project_root is not None and any('pytest' in str(a) for a in _argv[:4]):
        try:
            from _pytest.config import findpaths as _fp
            _orig_load = _fp.load_config_dict_from_file

            def _bounded_load(filepath):
                try:
                    if not Path(filepath).resolve().is_relative_to(project_root):
                        return None
                except OSError:
                    return None
                try:
                    return _orig_load(filepath)
                except PermissionError:
                    return None

            _fp.load_config_dict_from_file = _bounded_load
        except Exception:
            pass


_install()
"""


def _declared_shared_modules(root: Path) -> dict[str, Path]:
    try:
        configured = deployment_modules(root.resolve(strict=True))
    except Exception:
        return {}
    if not configured:
        return {}
    resolved = {}
    for name, raw_path in configured.items():
        if not isinstance(name, str) or not isinstance(raw_path, str):
            continue
        candidate = Path(raw_path)
        if candidate.is_absolute() and candidate.is_file():
            resolved[name] = candidate.resolve()
    return resolved


class SandboxShims:
    """Lifecycle wrapper for controller-generated read-only sandbox shims."""

    def __init__(self, project, *, execution_mode: str, project_python: str | None = None,
                 input_access: bool = False):
        self.root = Path(project['root']).resolve()
        self.execution_mode = execution_mode
        self.project_python = project_python if (isinstance(project_python, str) and project_python.strip()) else None
        from demiflow.forge.dependencies import module_files, verify_interpreter
        locked = project_dependencies(self.root, project={**project, 'python': self.project_python})
        if locked is not None:
            verify_interpreter(locked['environment']['interpreter'])
        self.dependency_lock = project.get('dependency_lock')
        managed = module_files(self.root, locked)
        self.directory = Path(tempfile.mkdtemp(prefix='demiflow-dev-shims-'))
        os.chmod(self.directory, 0o700)
        self.bin = self.directory / 'bin'
        self.bin.mkdir(mode=0o700)
        sdk_root = str(Path(__file__).resolve().parents[3])

        shared = _declared_shared_modules(self.root) if self.project_python else {}
        shared_roots = {name.partition('.')[0] for name in shared}
        if shared_roots & {name.partition('.')[0] for name in managed}:
            self.close()
            raise ValueError('Project dependency conflicts with a deployment shared package')
        self.shared_read_paths = tuple(sorted(set(shared.values()) | {Path(row['path']) for row in managed.values()}, key=str))
        shared_roots = sorted({name.split('.')[0] for name in shared})

        # 1. Always write Forge runtime configuration + restricted `demiflow` CLI shim.
        runtime_cfg_path = self.directory / 'forge_runtime.json'
        runtime_cfg = {
            'root': str(self.root),
            'execution_mode': execution_mode,
            'python': self.project_python,
            'dependency_lock': self.dependency_lock,
            'writes': list(project.get('writes') or []),
            'shared_sources': shared_roots,
            'input_access': bool(input_access),
            'dev_network': bool(project.get('dev_network', False)),
        }
        runtime_cfg_path.write_text(json.dumps(runtime_cfg, ensure_ascii=False, indent=2), encoding='utf-8')
        os.chmod(runtime_cfg_path, 0o600)

        forge_entry = self.directory / 'forge_shim_entry.py'
        forge_entry.write_text(_FORGE_SHIM_ENTRY, encoding='utf-8')
        os.chmod(forge_entry, 0o600)

        demiflow_bin = self.bin / 'demiflow'
        demiflow_bin.write_text(
            '#!/bin/sh\n'
            f'exec {shlex.quote(sys.executable)} -IB {shlex.quote(str(forge_entry))} '
            f'{shlex.quote(sdk_root)} {shlex.quote(str(runtime_cfg_path))} '
            f'{shlex.quote(execution_mode)} "$@"\n',
            encoding='utf-8',
        )
        os.chmod(demiflow_bin, 0o700)

        # 2. Only expose business python / pytest shims when explicitly configured for this project.
        if self.project_python:
            site_dir = self.directory / 'site'
            site_dir.mkdir(mode=0o700)
            binding_path = self.directory / 'py_binding.json'
            binding = {
                'sdk_root': sdk_root,
                'project_root': str(self.root),
                'project_dependencies': managed,
                'shared_sources': {name: str(path) for name, path in sorted(shared.items())},
            }
            if locked is not None:
                binding.update(dependency_environment=locked['environment'], dependency_requirements=locked['requirements'])
            binding_path.write_text(json.dumps(binding, ensure_ascii=False), encoding='utf-8')
            os.chmod(binding_path, 0o600)

            sitecustomize = site_dir / 'sitecustomize.py'
            sitecustomize.write_text(_SITECUSTOMIZE, encoding='utf-8')
            os.chmod(sitecustomize, 0o600)

            py_script = (
                '#!/bin/sh\n'
                f'export PYTHONPATH={shlex.quote(str(site_dir))}"${{PYTHONPATH:+:$PYTHONPATH}}"\n'
                'if [ -z "${PYTEST_ADDOPTS:-}" ]; then\n'
                f'  export PYTEST_ADDOPTS="--rootdir={str(self.root)} -p no:cacheprovider -s"\n'
                'fi\n'
                f'exec {shlex.quote(self.project_python)} "$@"\n'
            )
            for name in ('python', 'python3'):
                target = self.bin / name
                target.write_text(py_script, encoding='utf-8')
                os.chmod(target, 0o700)

            pytest_bin = self.bin / 'pytest'
            pytest_bin.write_text(
                '#!/bin/sh\n'
                f'exec {shlex.quote(str(self.bin / "python"))} -m pytest "$@"\n',
                encoding='utf-8',
            )
            os.chmod(pytest_bin, 0o700)

    @property
    def read_paths(self) -> tuple[Path, ...]:
        return self.shared_read_paths

    @property
    def runtime_paths(self) -> tuple[Path, ...]:
        return (self.directory,)

    def close(self) -> None:
        shutil.rmtree(self.directory, ignore_errors=True)
