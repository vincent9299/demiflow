"""Project-scoped Jupyter Kernel launcher with optional Landlock read-only confinement.

Jupyter Server stays on the workbench Python; the Kernel runs on the project's
configured Python without silently falling back to the workbench environment.
When sandbox mode is ``readonly``, Linux Landlock enforces filesystem read-only
isolation on the project root, data locations, and Python prefixes while leaving
local ZMQ loopback communication intact.
"""
from __future__ import annotations

import ctypes
import json
import os
from pathlib import Path
import platform
import subprocess
import sys

READ = (1 << 2) | (1 << 3)
EXEC = 1 << 0
WRITE = (1 << 1) | (1 << 4) | (1 << 5) | (1 << 7) | (1 << 8)
FILE_READ = 1 << 2
FILE_WRITE = 1 << 1
FILE_EXEC = 1 << 0


def _fail(message: str) -> None:
    sys.stderr.write(f'[DemiFlow Notebook Kernel] {message}\n')
    sys.stderr.flush()
    raise SystemExit(1)


def _python_prefixes(python_path: str) -> list[str]:
    paths: list[Path] = []
    for raw in (Path(python_path), Path(python_path).resolve()):
        if raw.exists():
            paths.append(raw.parent)
            if raw.parent.name in {'bin', 'Scripts'} and raw.parent.parent not in {Path('/'), Path('/usr')}:
                paths.append(raw.parent.parent)
    result: list[str] = []
    for candidate in paths:
        try:
            resolved = candidate.resolve(strict=True)
        except OSError:
            continue
        if resolved != Path('/'):
            result.append(str(resolved))
    return sorted(set(result))


def _check_ipykernel(python_path: str, *, isolated=False) -> None:
    try:
        probe = subprocess.run(
            [python_path, '-I', '-B', '-c', 'from importlib.util import find_spec; assert find_spec("ipykernel")']
            if isolated else [python_path, '-c', 'import ipykernel'],
            env={'PATH': os.defpath, 'LANG': 'C.UTF-8'} if isolated else None,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.PIPE,
            text=True,
            timeout=15,
        )
    except OSError as exc:
        _fail(f'无法启动项目 Python 解释器 ({python_path}): {exc}')
    except subprocess.TimeoutExpired:
        _fail(f'检查项目 Python 的 ipykernel 超时: {python_path}')
    if probe.returncode != 0:
        _fail(
            f'项目配置的 Python 环境 ({python_path}) 缺少 ipykernel，无法启动 Notebook 内核。'
            f'请在该环境中安装 ipykernel（例如：{python_path} -m pip install ipykernel），'
            '或在「项目设置」中更换项目 Python 环境。'
        )


def _apply_landlock(*, read_paths: list[str], exec_paths: list[str], write_paths: list[str], require_truncate=False) -> None:
    if platform.system() != 'Linux' or platform.machine() != 'x86_64':
        _fail('Notebook 内核只读沙箱需要 Linux x86-64 Landlock 支持。')
    libc = ctypes.CDLL(None, use_errno=True)

    def checked(result: int, label: str) -> int:
        if result < 0:
            raise OSError(ctypes.get_errno(), label)
        return result

    abi = checked(libc.syscall(444, 0, 0, 1), 'Landlock unavailable')
    if abi < 1:
        _fail('当前 Linux 内核不支持 Landlock ABI 1，无法启用只读沙箱。')

    if require_truncate and abi < 3:
        _fail('受控依赖的只读 Notebook 内核需要支持安全截断控制的 Linux Landlock ABI 3。')
    truncate = 1 << 14 if require_truncate else 0

    class Rules(ctypes.Structure):
        _fields_ = [('handled', ctypes.c_uint64)]

    class PathRule(ctypes.Structure):
        _pack_ = 1
        _fields_ = [('access', ctypes.c_uint64), ('fd', ctypes.c_int32)]

    rules = Rules(((1 << 13) - 1) | truncate)
    ruleset = checked(libc.syscall(444, ctypes.byref(rules), ctypes.sizeof(rules), 0), 'Landlock ruleset')
    try:
        groups = (
            (read_paths, READ, FILE_READ),
            (exec_paths, READ | EXEC, FILE_READ | FILE_EXEC),
            (write_paths, READ | WRITE | truncate, FILE_READ | FILE_WRITE | truncate),
        )
        for group_paths, dir_rights, file_rights in groups:
            seen: set[str] = set()
            for raw in group_paths:
                if not raw:
                    continue
                try:
                    resolved = str(Path(raw).resolve(strict=True))
                except OSError:
                    continue
                if resolved in seen:
                    continue
                if resolved == '/' and group_paths is not read_paths:
                    continue
                seen.add(resolved)
                is_dir = os.path.isdir(resolved)
                access = dir_rights if is_dir else file_rights
                fd = os.open(resolved, os.O_PATH | os.O_CLOEXEC | os.O_NOFOLLOW)
                try:
                    attr = PathRule(access, fd)
                    checked(libc.syscall(445, ruleset, 1, ctypes.byref(attr), 0), f'Landlock rule: {resolved}')
                finally:
                    os.close(fd)
        checked(libc.prctl(38, 1, 0, 0, 0), 'PR_SET_NO_NEW_PRIVS')
        checked(libc.syscall(446, ruleset, 0), 'Landlock restrict_self')
    finally:
        os.close(ruleset)


def _dependency_snapshot(config, root, code_root, python):
    """Freeze approved source bytes before starting a new kernel."""
    import hashlib

    sys.path.insert(0, code_root)
    from demiflow.forge.dependencies import binding, module_files, probe_environment
    from demiflow.forge.io import ensure_directory, publish_bytes
    from demiflow.forge.platform_environment import read_bounded
    from demiflow.forge.schema import ForgeError
    from demiflow.workbench.python_sources import import_bootstrap

    locked = binding(root, approval=config['dependency_lock'], python=python)
    if probe_environment(python, locked, expected=locked['environment']['interpreter']) != locked['environment']:
        _fail('The approved project Python environment changed')
    directory = Path(config['runtime_dir']).parent / 'dependency-kernels'
    ensure_directory(directory)
    snapshot = directory / locked['digest']
    if not snapshot.exists():
        with os.scandir(directory) as entries:
            if sum(1 for _ in zip(range(16), entries)) >= 16:
                _fail('Notebook dependency snapshots exceed their retention budget')
    ensure_directory(snapshot)
    modules = {}
    for name, proof in module_files(root, locked).items():
        source = Path(proof['path'])
        parts = name.split('.')
        target = snapshot.joinpath('sources', *parts)
        target = target / '__init__.py' if source.name == '__init__.py' else target.with_suffix('.py')
        raw = read_bounded(source, proof['bytes'])
        if hashlib.sha256(raw).hexdigest() != proof['sha256']:
            _fail('The approved Notebook dependency changed during preparation')
        ensure_directory(target.parent)
        try:
            publish_bytes(target, raw)
        except ForgeError:
            if not target.exists() or read_bounded(target, proof['bytes']) != raw:
                _fail('The Notebook dependency snapshot differs from its lock')
        modules[name] = {**proof, 'path': str(target)}
    bootstrap = import_bootstrap(snapshot / 'bootstrap', modules, platform_root=code_root)
    return snapshot, bootstrap, locked['environment']['interpreter']['prefix']


def main() -> None:
    if len(sys.argv) < 3:
        _fail('Missing kernel config or connection file')
    config_path = Path(sys.argv[1])
    connection_file = sys.argv[2]
    config = json.loads(config_path.read_text(encoding='utf-8'))

    if config.get('dependency_error'):
        _fail(config['dependency_error'])
    target_python = config.get('python')
    if not target_python:
        _fail('当前项目尚未配置业务 Python 环境（Notebook 内核不继承平台 Python）。请在「项目设置」中指定该项目的独立 Python 解释器（需安装 ipykernel）。')
    py_path = Path(target_python)
    if not py_path.is_absolute() or not py_path.is_file() or not os.access(py_path, os.X_OK):
        _fail(f'项目配置的 Python 解释器不可用: {target_python}。请在「项目设置」中重新配置。')

    root = str(Path(config['root']).resolve(strict=True))
    code_root = str(Path(config['code_root']).resolve(strict=True))
    dependency = _dependency_snapshot(config, root, code_root, target_python) if config.get('dependency_lock') else None
    _check_ipykernel(target_python, isolated=dependency is not None)
    tmp_dir = Path(config['tmp_dir'])
    home_dir = tmp_dir / 'home'
    mpl_dir = tmp_dir / 'matplotlib'
    ipy_dir = tmp_dir / 'ipython'
    for directory in (tmp_dir, home_dir, mpl_dir, ipy_dir):
        directory.mkdir(parents=True, mode=0o700, exist_ok=True)

    sandbox_mode = config.get('sandbox_mode', 'off')
    if sandbox_mode not in {'readonly', 'off'}:
        _fail(f'不支持的 Notebook 沙箱模式: {sandbox_mode}')

    if sandbox_mode == 'readonly':
        # Grant read-only traversal/reads across '/' so Path.resolve(), Lance
        # preview symlink checks, and read-only external inputs work, while
        # denying ALL writes everywhere except tmp_dir, standard /dev nodes,
        # and explicitly whitelisted project subdirectories.
        read_paths = [
            '/',
            root,
            config['runtime_dir'],
            '/etc',
            '/proc',
            '/sys',
            *config.get('locations', ()),
        ]
        if dependency:
            read_paths = [root, config['runtime_dir'], str(dependency[0]), '/etc/ssl', '/etc/ld.so.cache',
                          *config.get('locations', ())]
        exec_paths = [
            '/usr',
            '/bin',
            '/sbin',
            '/lib',
            '/lib64',
            code_root,
            *_python_prefixes(target_python),
            *_python_prefixes(sys.executable),
        ]
        write_paths = [
            str(tmp_dir),
            '/dev/null',
            '/dev/zero',
            '/dev/urandom',
            '/dev/random',
            '/dev/shm',
        ]
        for rel in (config.get('write_paths') or ('data', 'notebooks')):
            candidate = (Path(root) / rel).resolve()
            if not candidate.is_relative_to(Path(root)):
                _fail(f'无效的允许写入路径: {rel}')
            if dependency:
                protected = (Path(root) / 'packages', Path(root) / 'pyproject.toml',
                             Path(root) / '.forge', dependency[0], Path(dependency[2]))
                if any(candidate.is_relative_to(path) or path.is_relative_to(candidate) for path in protected):
                    _fail('Notebook write scope overlaps project dependencies')
            candidate.mkdir(parents=True, exist_ok=True)
            write_paths.append(str(candidate))
        if platform.system() == 'Linux':
            _apply_landlock(read_paths=read_paths, exec_paths=exec_paths, write_paths=write_paths,
                            require_truncate=dependency is not None)
        elif platform.system() != 'Darwin':
            _fail('当前操作系统不支持原生 Notebook 只读内核沙箱（请使用 Linux、macOS 或 WSL2/容器环境）。')

    env = dict(os.environ) if dependency is None else {
        key: os.environ[key] for key in ('LANG', 'LC_ALL', 'TZ') if key in os.environ
    }
    existing_py_path = env.get('PYTHONPATH', '')
    env['PYTHONPATH'] = (os.pathsep.join((str(dependency[1]), code_root)) if dependency
                         else f'{existing_py_path}:{code_root}' if existing_py_path else code_root)
    env['DEMIFLOW_NOTEBOOK_ROOT'] = root
    env['DEMIFLOW_NOTEBOOK_SANDBOX'] = sandbox_mode
    env['HOME'] = str(home_dir)
    env['TMPDIR'] = str(tmp_dir)
    env['TEMP'] = str(tmp_dir)
    env['TMP'] = str(tmp_dir)
    env['DEMIFLOW_FORGE_DOMAIN'] = str(tmp_dir / 'forge')
    env['DEMIFLOW_LOCATION_CACHE'] = str(tmp_dir / 'locations')
    env['DEMIFLOW_RESOURCE_ROOT'] = str(tmp_dir / 'resources')
    env['MPLCONFIGDIR'] = str(mpl_dir)
    env['IPYTHONDIR'] = str(ipy_dir)
    env['PYTHONDONTWRITEBYTECODE'] = '1'
    py_bin = str(py_path.parent)
    env['PATH'] = f'{py_bin}:{env.get("PATH", "/usr/bin:/bin")}'

    os.chdir(root)
    kernel_cmd = [target_python, '-m', 'ipykernel_launcher', '-f', connection_file]
    if sandbox_mode == 'readonly' and platform.system() == 'Darwin':
        if not Path('/usr/bin/sandbox-exec').is_file():
            _fail('macOS Notebook 只读沙箱需要 /usr/bin/sandbox-exec。')
        sys.path.insert(0, code_root)
        from demiflow.workbench.conversations.sandbox import SandboxSpec
        from demiflow.workbench.conversations.sandbox_backends.macos_seatbelt import build_seatbelt_profile

        spec = SandboxSpec(
            read_paths=tuple(p for p in read_paths if p != '/' and Path(p).exists()),
            write_paths=tuple(p for p in write_paths if p not in {'/dev/null', '/dev/zero', '/dev/urandom', '/dev/random', '/dev/shm'} and Path(p).exists()),
            runtime_paths=tuple(p for p in exec_paths if Path(p).exists()),
            protected_paths=(),
            denied_paths=(),
            private_home=str(tmp_dir),
            direct_network=False,
        )
        profile = build_seatbelt_profile(spec)
        os.execve('/usr/bin/sandbox-exec', ['/usr/bin/sandbox-exec', '-p', profile, *kernel_cmd], env)
    os.execve(target_python, kernel_cmd, env)


if __name__ == '__main__':
    main()
