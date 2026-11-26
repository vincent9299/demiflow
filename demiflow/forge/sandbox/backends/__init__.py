"""Pluggable cross-platform SandboxBackend abstraction over one unified SandboxSpec.

Supported backends:
- ``linux_landlock``: Linux x86-64 user/pid/net namespaces + Landlock + seccomp.
  Works on bare-metal Linux AND inside nested Docker containers without privileges.
- ``macos_seatbelt``: macOS native ``/usr/bin/sandbox-exec`` Seatbelt profile.
  Zero external dependencies; full native permissions inside project write_paths.
- ``container_oci``: Hardened Docker/Podman OCI container with ``--read-only``
  root, ``--cap-drop=ALL``, and explicit same-path ``:ro`` / ``:rw`` mounts.
- ``windows_wsl``: Delegates Windows hosts into WSL2's ``linux_landlock`` backend.
"""
from __future__ import annotations

import hashlib
import math
import os
from pathlib import Path
import platform
from typing import Protocol, runtime_checkable

BACKEND_NAMES = ('auto', 'linux_landlock', 'macos_seatbelt', 'container_oci', 'windows_wsl')


@runtime_checkable
class SandboxBackend(Protocol):
    name: str

    def identity(self) -> dict: ...

    def readiness(self) -> dict: ...

    def launch(self, command, *, spec, env, cwd, lease_fd=None, control_socket=None, model_socket=None): ...


def file_digest(*paths: Path) -> str:
    digest = hashlib.sha256()
    for path in paths:
        data = path.read_bytes()
        digest.update(path.name.encode('utf-8') + b'\0' + data)
    return digest.hexdigest()


def _contains(parent: str, child: str) -> bool:
    p = parent.rstrip('/\\') or parent
    c = child.rstrip('/\\') or child
    if p == c:
        return True
    sep = '\\' if '\\' in p and '/' not in p else '/'
    return c.startswith(p + sep)


def validate_spec_portable(spec, command, env, cwd) -> dict:
    """OS-agnostic validation of SandboxSpec invariants shared by non-Linux backends."""
    from demiflow.forge.sandbox.sandbox import SandboxSpec

    if type(spec) is not SandboxSpec or spec.role not in {'developer', 'readonly', 'verifier'}:
        raise ValueError('A trusted development sandbox specification is required')
    if spec.read_policy not in {'scoped', 'inherit_os'}:
        raise ValueError('Sandbox read_policy must be scoped or inherit_os')
    if spec.read_policy == 'inherit_os' and spec.role != 'developer':
        raise ValueError('Only developer role may inherit host read permissions')
    if spec.write_policy not in {'scoped', 'project_root'}:
        raise ValueError('Sandbox write_policy must be scoped or project_root')
    if spec.write_policy == 'project_root' and spec.role != 'developer':
        raise ValueError('Only developer role may use project_root write policy')
    if spec.direct_network and spec.role == 'verifier':
        raise ValueError('Verifier sandbox cannot enable direct network access')
    if (type(spec.seconds) not in (int, float) or not math.isfinite(spec.seconds)
            or spec.seconds < 0 or spec.seconds > 86400
            or (spec.seconds == 0 and spec.role != 'developer')):
        raise ValueError('Sandbox resource limit is invalid')
    for value, maximum in ((spec.rss_bytes, 16 * 1024**3),
                           (spec.processes, 64), (spec.file_bytes, 64 * 1024**2),
                           (spec.private_bytes, 512 * 1024**2)):
        if type(value) not in (int, float) or not math.isfinite(value) or not 0 < value <= maximum:
            raise ValueError('Sandbox resource limit is invalid')
    if spec.role != 'developer' and spec.write_paths:
        raise ValueError('Read-only/reviewer and verifier roles cannot write source paths')
    if type(command) not in (list, tuple) or not 1 <= len(command) <= 256:
        raise ValueError('Sandbox command exceeds argument budget')
    if any(type(arg) is not str or '\0' in arg for arg in command):
        raise ValueError('Sandbox command requires clean string arguments')
    if type(env) is not dict or len(env) > 128 or any(type(k) is not str or type(v) is not str for k, v in env.items()):
        raise ValueError('Sandbox environment must be an explicit bounded mapping')
    if any(k in env for k in ('LD_PRELOAD', 'LD_LIBRARY_PATH', 'DYLD_INSERT_LIBRARIES', 'PYTHONHOME')):
        raise ValueError('Sandbox loader injection environment is not permitted')
    if 'PYTHONPATH' in env and spec.role != 'verifier':
        raise ValueError('Only fixed verifier definitions may supply scoped PYTHONPATH')
    if not isinstance(spec.private_home, str) or not spec.private_home or '\0' in spec.private_home:
        raise ValueError('Sandbox private HOME is required')

    def clean_list(values):
        if type(values) not in (list, tuple):
            raise ValueError('Sandbox paths must be a list or tuple')
        out = []
        for item in values:
            if type(item) is not str or not item or '\0' in item or '\n' in item or len(item) > 4096:
                raise ValueError('Sandbox paths must be bounded clean path strings')
            if item in {'/', '\\'}:
                raise ValueError('The host filesystem root cannot be granted')
            out.append(item)
        return tuple(sorted(set(out)))

    read_paths = clean_list(spec.read_paths)
    write_paths = clean_list(spec.write_paths)
    runtime_paths = clean_list(spec.runtime_paths)
    protected_paths = clean_list(spec.protected_paths)
    denied_paths = clean_list(spec.denied_paths)

    for allowed in write_paths:
        if any(_contains(allowed, dep) or _contains(dep, allowed) for dep in runtime_paths):
            raise ValueError('Sandbox write scope overlaps a trusted runtime path')
        if spec.write_policy != 'project_root':
            if any(_contains(allowed, prot) or _contains(prot, allowed) for prot in protected_paths):
                raise ValueError('Sandbox write scope overlaps a protected read-only path')
        if any(_contains(allowed, den) or _contains(den, allowed) for den in denied_paths):
            raise ValueError('Sandbox write scope overlaps a denied path')

    environment = dict(env, HOME=spec.private_home, TMPDIR=spec.private_home)
    if not spec.direct_network:
        for key in ('HTTP_PROXY', 'HTTPS_PROXY', 'ALL_PROXY', 'http_proxy', 'https_proxy', 'all_proxy', 'NO_PROXY', 'no_proxy'):
            environment.pop(key, None)

    return dict(
        read_paths=read_paths,
        write_paths=write_paths,
        runtime_paths=runtime_paths,
        protected_paths=protected_paths,
        denied_paths=denied_paths,
        private_home=spec.private_home,
        command=list(command),
        env=environment,
        cwd=str(cwd),
        direct_network=bool(spec.direct_network),
        read_policy=spec.read_policy,
        write_policy=spec.write_policy,
    )


def _registry() -> dict[str, SandboxBackend]:
    from .container_oci import ContainerOciBackend
    from .linux_landlock import LinuxLandlockBackend
    from .macos_seatbelt import MacosSeatbeltBackend
    from .windows_wsl import WindowsWslBackend

    return {
        'linux_landlock': LinuxLandlockBackend(),
        'macos_seatbelt': MacosSeatbeltBackend(),
        'container_oci': ContainerOciBackend(),
        'windows_wsl': WindowsWslBackend(),
    }


def select_backend(preferred: str | None = None, *, system: str | None = None) -> SandboxBackend:
    """Select the qualified SandboxBackend for the current OS or explicit override."""
    choice = (preferred or os.environ.get('DEMIFLOW_SANDBOX_BACKEND') or 'auto').strip().lower()
    if choice not in BACKEND_NAMES:
        raise ValueError(f'Unsupported sandbox backend: {choice!r}. Choose one of: {", ".join(BACKEND_NAMES)}')
    backends = _registry()
    if choice != 'auto':
        return backends[choice]

    host_os = system or platform.system()
    if host_os == 'Linux':
        # Default to linux_landlock so both bare-metal Linux and nested Docker
        # containers work out of the box without requiring privileged docker.sock.
        return backends['linux_landlock']
    if host_os == 'Darwin':
        # Default to zero-dependency native Seatbelt on macOS; container_oci is
        # selectable explicitly via DEMIFLOW_SANDBOX_BACKEND=container_oci.
        return backends['macos_seatbelt']
    if host_os == 'Windows':
        wsl = backends['windows_wsl']
        if wsl.readiness().get('available'):
            return wsl
        oci = backends['container_oci']
        if oci.readiness().get('available'):
            return oci
        return wsl
    return backends['container_oci']


def backend_readiness(preferred: str | None = None) -> dict:
    return select_backend(preferred).readiness()


def execution_identity(preferred: str | None = None) -> dict:
    return select_backend(preferred).identity()
