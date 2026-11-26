"""Controller-owned read-only CLI and business-Python shims for Forge sandboxes.

Two strictly separated channels are exposed in a read-only directory on PATH:
1. ``demiflow``: allows ONLY ``demiflow forge ...`` subcommands so the agent uses
   the exact same Forge CLI as terminal/IDE workflows without exposing the host
   Python environment to business code. In restricted mode, ``forge dep vendor``
   and ``forge dep remove`` dispatch through the authenticated one-turn host
   control channel, keeping ``packages/`` and ``.forge/`` OS read-only.
2. ``python`` / ``python3`` / ``pytest``: created ONLY when the project explicitly
   configures its own business Python interpreter. Never falls back to the host
   Python. Installs a pure read-only ``demiflow`` SDK fallback loader, qualified
   project-package resolution, live ``pyproject.toml`` single-package B-class
   loading via ``demiflow.forge.runtime``, declared ``shared-sources.json``
   imports, and sandbox-safe pytest defaults.
"""
from __future__ import annotations

import json
import os
from pathlib import Path
import secrets
import shlex
import shutil
import sys
import tempfile

from demiflow.forge.runtime import deployment_modules, validate_python_executable

_FORGE_SHIM_ENTRY = """\
import json
import os
from pathlib import Path
import sys

sdk_root = sys.argv[1]
runtime_cfg = sys.argv[2]
mode = sys.argv[3]
dep_binding_path = sys.argv[4]
args = sys.argv[5:]

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
        '如需创建缺失的标准目录、登记 Flow 入口或编排声明，请调用宿主工具 `manage_project_layout`。\\n'
    )
    raise SystemExit(2)

os.environ['DEMIFLOW_FORGE_RUNTIME_CONFIG'] = runtime_cfg
sys.path.insert(0, sdk_root)

if mode == 'restricted' and len(args) >= 2 and args[1] == 'dep':
    sub = args[2] if len(args) >= 3 else ''
    if sub == 'install':
        sys.stderr.write(
            '受限开发沙箱保持离线；安装 A 类环境依赖须由受信项目所有者在终端显式执行 '
            '`demiflow forge dep install --root <PROJECT> --python <PYTHON>`。\\n'
        )
        raise SystemExit(2)
    if sub in {'vendor', 'remove'}:
        from demiflow.forge.control_client import call
        as_json = '--json' in args[3:]
        positionals = []
        i = 3
        while i < len(args):
            tok = args[i]
            if tok == '--json':
                i += 1
                continue
            if tok in {'--root', '--python'}:
                i += 2
                continue
            if tok.startswith('--root=') or tok.startswith('--python='):
                i += 1
                continue
            if tok in {'-h', '--help'}:
                positionals = None
                break
            if tok.startswith('-'):
                sys.stderr.write(f'Unsupported option for dep {sub}: {tok}\\n')
                raise SystemExit(2)
            positionals.append(tok)
            i += 1
        if positionals is not None:
            if sub == 'vendor':
                if len(positionals) != 2:
                    sys.stderr.write('Usage: demiflow forge dep vendor NAME SOURCE --root DIR [--json]\\n')
                    raise SystemExit(2)
                payload = {'action': 'vendor', 'name': positionals[0], 'source': str(Path(positionals[1]).absolute())}
            else:
                if len(positionals) != 1:
                    sys.stderr.write('Usage: demiflow forge dep remove NAME --root DIR [--json]\\n')
                    raise SystemExit(2)
                payload = {'action': 'remove', 'name': positionals[0]}
            try:
                binding_file = Path(dep_binding_path)
                if not binding_file.is_file():
                    raise ValueError('Active host control channel is unavailable for dep updates')
                token, tid, epoch = json.loads(binding_file.read_text(encoding='utf-8'))
                if not token or not tid or not epoch:
                    raise ValueError('Active host control channel is inactive')
                answer = call('', token, tid, epoch, 'forge/dep', payload)
            except Exception as exc:
                err = {'status': 'failed', 'diagnostics': [{'code': 'operation_rejected', 'detail': str(exc)}]}
                sys.stdout.write(json.dumps(err, ensure_ascii=False, indent=None if as_json else 2) + '\\n')
                raise SystemExit(1)
            sys.stdout.write(json.dumps(answer, ensure_ascii=False, indent=None if as_json else 2) + '\\n')
            raise SystemExit(0)

import click
from demiflow.forge.cli import register

@click.group(name='demiflow')
def _cli():
    pass

register(_cli)
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
    allowed_read_roots = cfg.get('allowed_read_roots')
    denied_paths = cfg.get('denied_paths') or ()

    # 1. Pure read-only demiflow SDK fallback (never adds workbench site-packages).
    if sdk_root:
        sdk_pkg = Path(sdk_root) / 'demiflow'
        need_sdk = False
        try:
            spec = importlib.util.find_spec('demiflow')
            if spec is None or not spec.origin or Path(spec.origin).resolve() != (sdk_pkg / '__init__.py').resolve():
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
                    max_d = min(4, len(root_parts) - 1)
                    for depth in range(max_d, 1, -1):
                        cand = root_parts[-depth:]
                        if len(parts) < depth and cand[:len(parts)] == parts and all(p.isidentifier() for p in cand):
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

    # 3. LIVE pyproject.toml single-package B-class dependency loader via forge.runtime.
    #    Reads current pyproject.toml at startup so editing local_ref (v1 -> v2) or
    #    vendoring/removing packages takes effect on the very next python/pytest call.
    if project_root is not None and sdk_root and (project_root / 'pyproject.toml').is_file():
        try:
            from demiflow.forge.runtime import install_live_project_imports
            install_live_project_imports(
                project_root,
                allowed_roots=allowed_read_roots,
                denied_paths=denied_paths,
                shared_sources=shared_raw,
            )
        except BaseException as exc:
            os.write(2, ('Unable to load project Python dependencies: ' + str(exc) + '\\n').encode('utf-8', errors='replace'))
            os._exit(78)

    # 4. Declared read-only shared Python sources (from shared-sources.json).
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

    # 5. Keep pytest config discovery inside project_root so Landlock-blocked
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
    """Lifecycle wrapper for controller-generated read-only Forge sandbox shims."""

    def __init__(
        self,
        project,
        *,
        execution_mode: str,
        project_python: str | None = None,
        input_access: bool = False,
        allowed_read_roots=None,
        denied_paths=(),
    ):
        self.root = Path(project['root']).resolve()
        self.execution_mode = execution_mode
        self.project_python = project_python if (isinstance(project_python, str) and project_python.strip()) else None
        if self.project_python:
            validate_python_executable(self.project_python)
        self.allowed_read_roots = (
            tuple(str(Path(p).resolve()) for p in allowed_read_roots if p)
            if allowed_read_roots is not None else None
        )
        self.denied_paths = tuple(str(Path(p).resolve()) for p in (denied_paths or ()) if p)
        self.directory = Path(tempfile.mkdtemp(prefix='demiflow-dev-shims-'))
        os.chmod(self.directory, 0o700)
        self.bin = self.directory / 'bin'
        self.bin.mkdir(mode=0o700)
        self._dep_binding_path = self.directory / 'dep_control.json'
        self.clear_control_binding()
        sdk_root = str(Path(__file__).resolve().parents[3])

        shared = _declared_shared_modules(self.root) if self.project_python else {}
        self.shared_read_paths = tuple(sorted(set(shared.values()), key=str))
        shared_roots = sorted({name.split('.')[0] for name in shared})

        # 1. Always write Forge runtime configuration + restricted `demiflow` CLI shim.
        runtime_cfg_path = self.directory / 'forge_runtime.json'
        runtime_cfg = {
            'root': str(self.root),
            'execution_mode': execution_mode,
            'python': self.project_python,
            'allowed_read_roots': list(self.allowed_read_roots) if self.allowed_read_roots is not None else None,
            'denied_paths': list(self.denied_paths),
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
            f'{shlex.quote(execution_mode)} {shlex.quote(str(self._dep_binding_path))} "$@"\n',
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
                'allowed_read_roots': list(self.allowed_read_roots) if self.allowed_read_roots is not None else None,
                'denied_paths': list(self.denied_paths),
                'shared_sources': {name: str(path) for name, path in sorted(shared.items())},
            }
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

    def set_control_binding(self, token: str, turn_id: str, epoch: str) -> None:
        payload = json.dumps([token, turn_id, epoch])
        tmp = self.directory / ('.dep-control-' + secrets.token_hex(6) + '.json')
        tmp.write_text(payload, encoding='utf-8')
        os.chmod(tmp, 0o600)
        os.replace(tmp, self._dep_binding_path)

    def clear_control_binding(self) -> None:
        payload = json.dumps(['', '', ''])
        tmp = self.directory / ('.dep-control-' + secrets.token_hex(6) + '.json')
        tmp.write_text(payload, encoding='utf-8')
        os.chmod(tmp, 0o600)
        os.replace(tmp, self._dep_binding_path)

    @property
    def read_paths(self) -> tuple[Path, ...]:
        return self.shared_read_paths

    @property
    def runtime_paths(self) -> tuple[Path, ...]:
        return (self.directory,)

    def close(self) -> None:
        shutil.rmtree(self.directory, ignore_errors=True)
