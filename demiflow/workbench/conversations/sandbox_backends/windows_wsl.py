"""Windows WSL2 sandbox backend: delegates Windows hosts to WSL2 Linux Landlock.

Reuses the exact Linux Landlock + seccomp confinement inside WSL2 instead of
maintaining a divergent Windows-only ACL/AppContainer sandbox. Reports
``available=False`` honestly when WSL2 is absent (never fakes confinement).
"""
from __future__ import annotations

import json
from pathlib import Path
import platform
import re
import secrets
import shutil
import subprocess
import threading

from . import file_digest, validate_spec_portable

_PROBE_LOCK = threading.Lock()
_PROBE = None
_WIN_DRIVE = re.compile(r'^([A-Za-z]):[\\/](.*)$')


def windows_to_wsl_path(path: str) -> str:
    """Convert a Windows drive path (e.g. ``C:\\work\\proj``) to ``/mnt/c/work/proj``."""
    text = str(path)
    if text.startswith('/'):
        return text
    match = _WIN_DRIVE.match(text)
    if not match:
        raise ValueError('Windows WSL sandbox requires an absolute drive or POSIX path: ' + text)
    drive = match.group(1).lower()
    rest = match.group(2).replace('\\', '/')
    return f'/mnt/{drive}/{rest}'.rstrip('/') or f'/mnt/{drive}'


def detect_wsl() -> str | None:
    return shutil.which('wsl.exe') or shutil.which('wsl')


def build_wsl_spec_payload(spec, command, env, cwd, *, validated: dict | None = None) -> dict:
    data = validated if validated is not None else validate_spec_portable(spec, command, env, cwd)
    return dict(
        read_paths=[windows_to_wsl_path(p) for p in data['read_paths']],
        write_paths=[windows_to_wsl_path(p) for p in data['write_paths']],
        runtime_paths=[windows_to_wsl_path(p) for p in data['runtime_paths']],
        protected_paths=[windows_to_wsl_path(p) for p in data['protected_paths']],
        denied_paths=[windows_to_wsl_path(p) for p in data['denied_paths']],
        private_home=windows_to_wsl_path(data['private_home']),
        role=spec.role,
        seconds=spec.seconds,
        rss_bytes=spec.rss_bytes,
        processes=spec.processes,
        file_bytes=spec.file_bytes,
        private_bytes=spec.private_bytes,
        direct_network=data['direct_network'],
        read_policy=data['read_policy'],
        command=data['command'],
        env=data['env'],
        cwd=windows_to_wsl_path(data['cwd']),
    )


class WindowsWslBackend:
    name = 'windows_wsl'

    def identity(self) -> dict:
        return dict(
            backend='windows-wsl2-landlock-v1',
            implementation=file_digest(Path(__file__), Path(__file__).with_name('__init__.py')),
        )

    def launch(self, command, *, spec, env, cwd, lease_fd=None, control_socket=None, model_socket=None):
        if platform.system() != 'Windows':
            raise RuntimeError('Windows WSL sandbox backend requires Windows')
        wsl = detect_wsl()
        if not wsl:
            raise RuntimeError('wsl.exe not found; install WSL2 or Docker Desktop for restricted mode on Windows')
        payload = build_wsl_spec_payload(spec, command, env, cwd)
        wsl_code_root = windows_to_wsl_path(str(Path(__file__).resolve().parents[4]))
        runner = (
            'import json, sys; '
            f'sys.path.insert(0, {wsl_code_root!r}); '
            'from demiflow.workbench.conversations.sandbox import SandboxSpec, launch_sandbox; '
            'd = json.loads(sys.argv[1]); '
            's = SandboxSpec(read_paths=tuple(d["read_paths"]), write_paths=tuple(d["write_paths"]), '
            'runtime_paths=tuple(d["runtime_paths"]), protected_paths=tuple(d["protected_paths"]), '
            'denied_paths=tuple(d["denied_paths"]), private_home=d["private_home"], role=d["role"], '
            'seconds=d["seconds"], rss_bytes=d["rss_bytes"], processes=d["processes"], '
            'file_bytes=d["file_bytes"], private_bytes=d["private_bytes"], direct_network=d["direct_network"], '
            'read_policy=d.get("read_policy", "scoped")); '
            'p = launch_sandbox(d["command"], spec=s, env=d["env"], cwd=d["cwd"]); '
            'raise SystemExit(p.wait())'
        )
        proc = subprocess.Popen(
            [wsl, '--', 'python3', '-I', '-B', '-c', runner, json.dumps(payload, separators=(',', ':'))],
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            close_fds=True,
        )
        proc.execution_assurance = 'enforced'
        proc.execution_evidence = dict(self.identity(), execution_id=secrets.token_hex(16))
        return proc

    def readiness(self) -> dict:
        global _PROBE
        ident = self.identity()
        with _PROBE_LOCK:
            if _PROBE is not None and _PROBE['implementation'] == ident['implementation']:
                return dict(_PROBE)
            result = dict(
                available=False,
                backend=ident['backend'],
                implementation=ident['implementation'],
                identity_model='windows_wsl2_linux_landlock',
                reason='',
            )
            if platform.system() != 'Windows':
                result['reason'] = 'Windows WSL2 沙箱仅在 Windows 上可用。'
                _PROBE = result
                return dict(result)
            wsl = detect_wsl()
            if not wsl:
                result['reason'] = '未检测到 WSL2（请安装 WSL2 或 Docker Desktop 以启用 Windows 受限沙箱）。'
                _PROBE = result
                return dict(result)
            try:
                wsl_code_root = windows_to_wsl_path(str(Path(__file__).resolve().parents[4]))
                probe = subprocess.run(
                    [wsl, '--', 'python3', '-I', '-B', '-c',
                     f'import json, sys; sys.path.insert(0, {wsl_code_root!r}); '
                     'from demiflow.workbench.conversations.sandbox import backend_readiness; '
                     'print(json.dumps(backend_readiness()))'],
                    capture_output=True, text=True, timeout=15,
                )
                if probe.returncode != 0:
                    raise RuntimeError((probe.stderr or probe.stdout)[:256])
                inner = json.loads(probe.stdout)
                if not inner.get('available'):
                    raise RuntimeError(inner.get('reason') or 'WSL2 Linux Landlock probe failed')
                result.update(available=True, inner_backend=inner.get('backend'),
                              reason='Windows WSL2 + Linux Landlock 沙箱可用。')
            except (OSError, RuntimeError, ValueError, subprocess.SubprocessError) as exc:
                result['reason'] = 'Windows WSL2 沙箱不可用：' + type(exc).__name__ + ': ' + str(exc)[:256]
            _PROBE = result
            return dict(result)
