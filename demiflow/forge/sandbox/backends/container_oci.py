"""Hardened OCI container sandbox backend (Docker / Podman / OrbStack).

Provides identical directory-level physical isolation across Linux, macOS, and
Windows hosts where a container daemon is available:
- ``--read-only`` root filesystem, ``--cap-drop=ALL``, ``no-new-privileges``
- Same-path bind mounts (``:rw`` for write_paths & private_home, ``:ro`` for
  read_paths, runtime_paths, and protected overlays)
- Unmounted host paths do not exist inside the container at all, while paths
  inside ``write_paths`` have full native permissions (chmod/truncate/pytest).
"""
from __future__ import annotations

import os
from pathlib import Path
import secrets
import shutil
import subprocess
import tempfile
import threading

from . import file_digest, validate_spec_portable

_PROBE_LOCK = threading.Lock()
_PROBE = None
DEFAULT_IMAGE = 'python:3.11-slim'


def detect_oci_runtime() -> str | None:
    preferred = os.environ.get('DEMIFLOW_SANDBOX_OCI_BIN', '').strip()
    candidates = [preferred] if preferred else ['docker', 'podman']
    for name in candidates:
        if not name:
            continue
        found = shutil.which(name)
        if found:
            return found
    return None


def _safe_mount_path(path: str) -> str:
    if any(ch in path for ch in (':', '\n', '\r', '\0')):
        raise ValueError('OCI bind mount path contains forbidden delimiter: ' + path)
    return path


def build_oci_command(runtime: str, image: str, command, *, spec, env, cwd,
                      validated: dict | None = None) -> list[str]:
    """Build hardened ``docker run`` / ``podman run`` argv from a SandboxSpec."""
    if not image or any(ch in image for ch in (' ', '\n', '\r', '\0')) or image.startswith('-'):
        raise ValueError('Invalid OCI sandbox image reference')
    data = validated if validated is not None else validate_spec_portable(spec, command, env, cwd)
    args = [
        runtime, 'run', '--rm', '-i', '--init',
        '--read-only',
        '--cap-drop=ALL',
        '--security-opt=no-new-privileges:true',
        f'--pids-limit={int(spec.processes)}',
        f'--memory={int(spec.rss_bytes)}',
        f'--network={"bridge" if data["direct_network"] else "none"}',
    ]
    if hasattr(os, 'getuid') and hasattr(os, 'getgid'):
        args.extend(['--user', f'{os.getuid()}:{os.getgid()}'])

    # Mount read-only paths first, then writable paths, then read-only protected overlays.
    mounted = set()
    for path in (*data['read_paths'], *data['runtime_paths']):
        clean = _safe_mount_path(path)
        if clean not in mounted:
            args.extend(['-v', f'{clean}:{clean}:ro'])
            mounted.add(clean)
    for path in (*data['write_paths'], data['private_home']):
        clean = _safe_mount_path(path)
        args.extend(['-v', f'{clean}:{clean}:rw'])
        mounted.add(clean)
    for path in data['protected_paths']:
        clean = _safe_mount_path(path)
        if Path(clean).exists():
            args.extend(['-v', f'{clean}:{clean}:ro'])

    args.extend(['-w', _safe_mount_path(data['cwd'])])
    for key, val in sorted(data['env'].items()):
        args.extend(['-e', f'{key}={val}'])
    args.append(image)
    args.extend(data['command'])
    return args


class ContainerOciBackend:
    name = 'container_oci'

    def __init__(self, *, image: str | None = None):
        self._image = image

    @property
    def image(self) -> str:
        return (self._image or os.environ.get('DEMIFLOW_SANDBOX_IMAGE') or DEFAULT_IMAGE).strip()

    def identity(self) -> dict:
        return dict(
            backend='container-oci-v1',
            image=self.image,
            implementation=file_digest(Path(__file__), Path(__file__).with_name('__init__.py')),
        )

    def launch(self, command, *, spec, env, cwd, lease_fd=None, control_socket=None, model_socket=None):
        runtime = detect_oci_runtime()
        if not runtime:
            raise RuntimeError('No OCI container runtime (docker/podman) found on PATH')
        data = validate_spec_portable(spec, command, env, cwd)
        argv = build_oci_command(runtime, self.image, data['command'], spec=spec, env=env, cwd=cwd, validated=data)
        proc = subprocess.Popen(
            argv,
            cwd=data['cwd'],
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            close_fds=True,
            start_new_session=True,
        )
        proc.execution_assurance = 'enforced'
        proc.execution_evidence = dict(self.identity(), runtime=runtime, execution_id=secrets.token_hex(16))
        return proc

    def readiness(self) -> dict:
        global _PROBE
        ident = self.identity()
        with _PROBE_LOCK:
            if _PROBE is not None and _PROBE['implementation'] == ident['implementation'] and _PROBE.get('image') == self.image:
                return dict(_PROBE)
            result = dict(
                available=False,
                backend=ident['backend'],
                image=self.image,
                implementation=ident['implementation'],
                identity_model='oci_rootless_readonly_container',
                reason='',
            )
            runtime = detect_oci_runtime()
            if not runtime:
                result['reason'] = '未检测到 docker 或 podman 容器引擎。'
                _PROBE = result
                return dict(result)
            try:
                info = subprocess.run([runtime, 'info'], capture_output=True, text=True, timeout=5)
                if info.returncode != 0:
                    raise RuntimeError('容器守护进程不可用: ' + (info.stderr or info.stdout)[:200])
                from demiflow.forge.sandbox.sandbox import SandboxSpec

                with tempfile.TemporaryDirectory(prefix='forge-oci-probe-') as base:
                    root = Path(base).resolve()
                    home = root / 'home'; home.mkdir(mode=0o700)
                    source = root / 'source'; source.mkdir()
                    readonly = root / 'readonly'; readonly.write_text('preserve')
                    control = root / 'control'; control.write_text('secret')
                    spec = SandboxSpec(
                        read_paths=(str(readonly),),
                        write_paths=(str(source),),
                        runtime_paths=(),
                        protected_paths=(str(readonly),),
                        denied_paths=(str(control),),
                        private_home=str(home),
                        seconds=15,
                    )
                    probe_code = (
                        'import os, pathlib, sys\n'
                        'source, readonly, control = sys.argv[1:]\n'
                        'p = pathlib.Path(source, "ok"); p.write_text("allowed"); os.chmod(p, 0o600)\n'
                        'assert pathlib.Path(readonly).read_text() == "preserve"\n'
                        'assert not pathlib.Path(control).exists()\n'
                        'try:\n'
                        '    pathlib.Path(readonly).write_text("bad")\n'
                        '    raise RuntimeError("Readonly mount was writable")\n'
                        'except OSError:\n'
                        '    pass\n'
                        'print("ready")\n'
                    )
                    proc = self.launch(['python3', '-I', '-B', '-c', probe_code, str(source), str(readonly), str(control)],
                                       spec=spec, env={'PATH': '/usr/local/bin:/usr/bin:/bin'}, cwd=str(source))
                    try:
                        out, err = proc.communicate(timeout=20)
                    finally:
                        if proc.poll() is None:
                            proc.kill(); proc.communicate(timeout=5)
                    if proc.returncode != 0 or out.strip() != b'ready' or readonly.read_text() != 'preserve':
                        raise RuntimeError('OCI probe failed: ' + err.decode(errors='replace')[:256])
                    result.update(available=True, runtime=runtime,
                                  reason='OCI 容器沙箱可用：根文件系统只读、项目目录内原生全权限、未挂载目录物理不可见。')
            except (OSError, RuntimeError, ValueError, subprocess.SubprocessError) as exc:
                result['reason'] = 'OCI 容器沙箱不可用：' + type(exc).__name__ + ': ' + str(exc)[:256]
            _PROBE = result
            return dict(result)
