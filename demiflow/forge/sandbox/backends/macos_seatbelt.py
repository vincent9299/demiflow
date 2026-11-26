"""macOS Seatbelt (sandbox-exec) backend: zero-dependency directory confinement.

Translates SandboxSpec into a minimal ``(version 1) (deny default)`` Seatbelt
profile where project ``write_paths`` and ``private_home`` have full native
file permissions (including chmod/truncate/pytest) while paths outside the
project directory, protected metadata, and denied paths remain OS-enforced.
"""
from __future__ import annotations

import os
from pathlib import Path
import platform
import secrets
import shutil
import subprocess
import tempfile
import threading

from . import file_digest, validate_spec_portable

_PROBE_LOCK = threading.Lock()
_PROBE = None
_SANDBOX_EXEC = '/usr/bin/sandbox-exec'

_MACOS_SYSTEM_READ = (
    '/System/Library',
    '/usr/lib',
    '/usr/share',
    '/usr/bin',
    '/bin',
    '/private/var/db/dyld',
    '/Library/Frameworks',
    '/dev/null',
    '/dev/urandom',
    '/dev/random',
)


def _sb_quote(path: str) -> str:
    if '\0' in path or '\n' in path or '\r' in path:
        raise ValueError('Seatbelt path contains forbidden control character')
    escaped = path.replace('\\', '\\\\').replace('"', '\\"')
    return f'"{escaped}"'


def _rule_for_path(op: str, path: str, *, allow: bool = True) -> str:
    verb = 'allow' if allow else 'deny'
    # Match both the exact path and any descendant under it.
    return f'({verb} {op} (subpath {_sb_quote(path)}) (literal {_sb_quote(path)}))'


def build_seatbelt_profile(spec, *, validated: dict | None = None) -> str:
    """Compile a validated SandboxSpec into an immutable Seatbelt (.sb) profile."""
    data = validated if validated is not None else validate_spec_portable(spec, ['/bin/sh'], {}, spec.private_home)
    lines = [
        '(version 1)',
        '(deny default)',
        '(allow process-exec)',
        '(allow process-fork)',
        '(allow signal (target self))',
        '(allow sysctl-read)',
        '(allow mach-lookup)',
        '(allow ipc-posix-shm-read-data)',
        '(allow file-read-metadata)',
    ]
    # System runtime / host read policy.
    if data['read_policy'] == 'inherit_os':
        # Inherit the invoking OS user's native read permissions; writes remain
        # strictly confined to write_paths + private_home + /dev/null.
        lines.append('(allow file-read*)')
    else:
        for sys_path in _MACOS_SYSTEM_READ:
            lines.append(_rule_for_path('file-read*', sys_path, allow=True))
    lines.append('(allow file-write* (literal "/dev/null"))')

    # Read & runtime paths (read-only).
    if data['read_policy'] != 'inherit_os':
        for path in (*data['read_paths'], *data['runtime_paths']):
            lines.append(_rule_for_path('file-read*', path, allow=True))

    # Writable project paths & private HOME (full native permissions inside scope).
    for path in (*data['write_paths'], data['private_home']):
        if data['read_policy'] != 'inherit_os':
            lines.append(_rule_for_path('file-read*', path, allow=True))
        lines.append(_rule_for_path('file-write*', path, allow=True))

    # Explicit overrides: protected paths stay read-only; denied paths are unreadable & unwritable.
    # Seatbelt evaluates rules with last-match precedence for overlapping filters.
    for path in data['protected_paths']:
        lines.append(_rule_for_path('file-write*', path, allow=False))
    for path in data['denied_paths']:
        lines.append(_rule_for_path('file-read*', path, allow=False))
        lines.append(_rule_for_path('file-write*', path, allow=False))

    # Network confinement.
    if data['direct_network']:
        lines.append('(allow network-outbound)')
        lines.append('(allow network-inbound (local ip "localhost:*"))')
    else:
        lines.append('(deny network*)')
        lines.append('(allow network-outbound (remote ip "localhost:*"))')

    return '\n'.join(lines) + '\n'


class MacosSeatbeltBackend:
    name = 'macos_seatbelt'

    def identity(self) -> dict:
        return dict(
            backend='macos-seatbelt-v1',
            implementation=file_digest(Path(__file__), Path(__file__).with_name('__init__.py')),
        )

    def launch(self, command, *, spec, env, cwd, lease_fd=None, control_socket=None, model_socket=None):
        if platform.system() != 'Darwin':
            raise RuntimeError('macOS Seatbelt sandbox backend requires macOS (Darwin)')
        if not Path(_SANDBOX_EXEC).is_file():
            raise RuntimeError('macOS sandbox-exec binary is unavailable')
        data = validate_spec_portable(spec, command, env, cwd)
        profile = build_seatbelt_profile(spec, validated=data)
        proc = subprocess.Popen(
            [_SANDBOX_EXEC, '-p', profile, *data['command']],
            cwd=data['cwd'],
            env=data['env'],
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            close_fds=True,
            start_new_session=True,
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
                identity_model='macos_seatbelt_invoking_user',
                reason='',
            )
            if platform.system() != 'Darwin':
                result['reason'] = 'macOS Seatbelt 沙箱仅在 macOS 上可用。'
                _PROBE = result
                return dict(result)
            if not Path(_SANDBOX_EXEC).is_file():
                result['reason'] = '未找到 /usr/bin/sandbox-exec。'
                _PROBE = result
                return dict(result)
            try:
                from demiflow.forge.sandbox.sandbox import SandboxSpec

                py = shutil.which('python3') or '/usr/bin/python3'
                with tempfile.TemporaryDirectory(prefix='forge-seatbelt-probe-') as base:
                    root = Path(base).resolve()
                    home = root / 'home'; home.mkdir(mode=0o700)
                    source = root / 'source'; source.mkdir()
                    readonly = root / 'readonly'; readonly.write_text('preserve')
                    control = root / 'control'; control.write_text('secret')
                    spec = SandboxSpec(
                        read_paths=(str(readonly),),
                        write_paths=(str(source),),
                        runtime_paths=('/usr',),
                        protected_paths=(str(readonly),),
                        denied_paths=(str(control),),
                        private_home=str(home),
                        seconds=5,
                    )
                    probe_code = (
                        'import os, pathlib, sys\n'
                        'source, readonly, control = sys.argv[1:]\n'
                        'p = pathlib.Path(source, "ok"); p.write_text("allowed"); os.chmod(p, 0o600)\n'
                        'assert pathlib.Path(readonly).read_text() == "preserve"\n'
                        'for op in (lambda: pathlib.Path(control).read_text(), '
                        'lambda: pathlib.Path(readonly).write_text("bad")):\n'
                        '    try: op()\n'
                        '    except OSError: continue\n'
                        '    raise RuntimeError("Seatbelt boundary violated")\n'
                        'print("ready")\n'
                    )
                    proc = self.launch([py, '-I', '-B', '-c', probe_code, str(source), str(readonly), str(control)],
                                       spec=spec, env={'PATH': '/usr/bin:/bin'}, cwd=str(source))
                    try:
                        out, err = proc.communicate(timeout=8)
                    finally:
                        if proc.poll() is None:
                            proc.kill(); proc.communicate(timeout=5)
                    if proc.returncode != 0 or out.strip() != b'ready' or readonly.read_text() != 'preserve':
                        raise RuntimeError('Seatbelt probe failed: ' + err.decode(errors='replace')[:256])
                    result.update(available=True, reason='macOS Seatbelt 目录边界沙箱可用，项目可写目录内支持完整原生权限。')
            except (OSError, RuntimeError, ValueError, subprocess.SubprocessError) as exc:
                result['reason'] = 'macOS Seatbelt 沙箱探测未通过：' + type(exc).__name__ + ': ' + str(exc)[:256]
            _PROBE = result
            return dict(result)
