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


def _check_ipykernel(python_path: str) -> None:
    try:
        probe = subprocess.run(
            [python_path, '-c', 'import ipykernel'],
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


def _apply_landlock(*, read_paths: list[str], exec_paths: list[str], write_paths: list[str]) -> None:
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

    class Rules(ctypes.Structure):
        _fields_ = [('handled', ctypes.c_uint64)]

    class PathRule(ctypes.Structure):
        _pack_ = 1
        _fields_ = [('access', ctypes.c_uint64), ('fd', ctypes.c_int32)]

    rules = Rules((1 << 13) - 1)
    ruleset = checked(libc.syscall(444, ctypes.byref(rules), ctypes.sizeof(rules), 0), 'Landlock ruleset')
    try:
        groups = (
            (read_paths, READ, FILE_READ),
            (exec_paths, READ | EXEC, FILE_READ | FILE_EXEC),
            (write_paths, READ | WRITE, FILE_READ | FILE_WRITE),
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


def main() -> None:
    if len(sys.argv) < 3:
        _fail('Missing kernel config or connection file')
    config_path = Path(sys.argv[1])
    connection_file = sys.argv[2]
    config = json.loads(config_path.read_text(encoding='utf-8'))

    target_python = config.get('python')
    if not target_python:
        _fail('当前项目已设为「不挂载 Python 环境」，Notebook 内核无法启动。请在「项目设置」中配置项目 Python 环境。')
    py_path = Path(target_python)
    if not py_path.is_absolute() or not py_path.is_file() or not os.access(py_path, os.X_OK):
        _fail(f'项目配置的 Python 解释器不可用: {target_python}。请在「项目设置」中重新配置。')

    _check_ipykernel(target_python)

    root = str(Path(config['root']).resolve(strict=True))
    code_root = str(Path(config['code_root']).resolve(strict=True))
    tmp_dir = Path(config['tmp_dir'])
    home_dir = tmp_dir / 'home'
    mpl_dir = tmp_dir / 'matplotlib'
    ipy_dir = tmp_dir / 'ipython'
    for directory in (tmp_dir, home_dir, mpl_dir, ipy_dir):
        directory.mkdir(parents=True, mode=0o700, exist_ok=True)

    sandbox_mode = config.get('sandbox_mode', 'readonly')
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
        for rel in config.get('write_paths', ()):
            candidate = (Path(root) / rel).resolve()
            if not candidate.is_relative_to(Path(root)):
                _fail(f'无效的允许写入路径: {rel}')
            candidate.mkdir(parents=True, exist_ok=True)
            write_paths.append(str(candidate))
        _apply_landlock(read_paths=read_paths, exec_paths=exec_paths, write_paths=write_paths)

    env = dict(os.environ)
    existing_py_path = env.get('PYTHONPATH', '')
    env['PYTHONPATH'] = f'{existing_py_path}:{code_root}' if existing_py_path else code_root
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
    os.execve(target_python, [target_python, '-m', 'ipykernel_launcher', '-f', connection_file], env)


if __name__ == '__main__':
    main()
