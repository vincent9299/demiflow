"""One external provider launch boundary, independent of the CLI protocol.

Restricted execution requires an explicit controller-bound sandbox specification
and passes through the same externally enforced boundary for every provider.
"""
from __future__ import annotations

import os
from pathlib import Path
import subprocess
import sys

from demiflow.resources.process_observation import UNIT_ENV
from demiflow.workbench.conversations.common import reject


def runtime_paths(config, *, project_python=None):
    paths = [Path('/usr'), Path('/lib'), Path('/lib64'), Path(sys.base_prefix), Path(sys.prefix),
             Path(__file__).resolve().parents[3]]
    executable = Path(config['executable']).resolve()
    paths.append(executable.parent.parent if executable.name == 'bin.js' or executable.suffix == '.js' else executable)
    if config.get('node'): paths.append(Path(config['node']).resolve())
    if project_python:
        py = Path(project_python)
        if py.is_absolute():
            paths.append(py.parent)
            if py.parent.name in {'bin', 'Scripts'} and py.parent.parent not in {Path('/'), Path('/usr')}:
                paths.append(py.parent.parent)
            resolved = py.resolve()
            if resolved.parent.name in {'bin', 'Scripts'} and resolved.parent.parent not in {Path('/'), Path('/usr')}:
                paths.append(resolved.parent.parent)
    return tuple(sorted({str(path.absolute()) for path in paths}))


def execution_facts(mode, *, sandbox_backend=None):
    """Return implementation facts, never provider claims or check results."""
    if type(mode) is not str or mode not in {'trusted_local', 'restricted'}:
        reject('Unsupported development execution mode')
    if mode == 'trusted_local':
        return dict(available=True, filesystem_enforcement='unsupported',
            anti_tamper=False,
            reason='可信本地模式：使用宿主权限，无 OS 文件/进程隔离。应用范围检查不能阻止同 UID 工具越界或联网。')
    from demiflow.workbench.conversations.sandbox_backends import select_backend
    backend = select_backend(sandbox_backend)
    facts = backend.readiness()
    enforcement = ('landlock_seccomp' if backend.name == 'linux_landlock' else facts['backend']) if facts['available'] else 'unverified'
    return dict(available=facts['available'],
        filesystem_enforcement=enforcement,
        anti_tamper=facts['available'], reason=facts['reason'],
        backend=facts['backend'], implementation=facts['implementation'],
        host_identity=facts['identity_model'])


def require_available(mode):
    facts = execution_facts(mode)
    if not facts['available']:
        reject(facts['reason'])


def launch_provider(*, mode, adapter, config, home, root, lease_fd,
                    sandbox_spec=None, control_socket=None, model_socket=None, sandbox_backend=None):
    """Prepare and spawn one provider under the explicitly admitted mode.

Admission precedes adapter preparation, which may create files or read model
credentials. The caller retains subreaping, budgets, transport and cleanup.
The inherited unit FD preserves the existing cooperative ownership protocol;
it is not proof that hostile descendants stopped and cannot authorize L2.
"""
    if mode == 'restricted':
        from demiflow.workbench.conversations.sandbox import SandboxSpec, specification
        from demiflow.workbench.conversations.sandbox_backends import select_backend, validate_spec_portable
        if type(sandbox_spec) is not SandboxSpec:
            reject('Restricted provider execution requires an explicit controller sandbox scope')
        if Path(home).resolve() != Path(sandbox_spec.private_home).resolve():
            reject('Restricted provider HOME differs from its controller-bound private scope')
        backend = select_backend(sandbox_backend)
        # Validate before the trusted adapter creates profiles. This also rejects
        # symlink/hardlink poisoning left in an earlier untrusted provider HOME.
        if backend.name == 'linux_landlock':
            specification(sandbox_spec, [sys.executable], {}, root)
        else:
            validate_spec_portable(sandbox_spec, [sys.executable], {}, root)
        if sandbox_spec.direct_network:
            private = dict(config)
        else:
            if config.get('native_credentials') or config.get('credential_env'):
                if not config.get('_boundary') or model_socket is None:
                    reject('Restricted model authentication requires the fixed external credential broker')
            private = {key: value for key, value in config.items()
                       if key not in {'native_credentials', 'credential_env', 'inherit_proxy'}}
        command, environment = adapter.launch(private, home, root)
        if config.get('_cell_bin') and adapter.name == 'codex':
            wrapper = Path(__file__).parents[1] / 'notebooks/data_cli.py'
            command = [sys.executable, '-IB', str(wrapper), '--provider', *command]
        process = backend.launch(command, spec=sandbox_spec, env=environment, cwd=root,
            lease_fd=lease_fd, control_socket=control_socket, model_socket=model_socket)
        return process
    require_available(mode)
    if type(lease_fd) is not int or lease_fd < 3:
        reject('Invalid development process lease descriptor')
    os.fstat(lease_fd)
    command, environment = adapter.launch(config, home, root)
    environment = dict(environment)
    environment[UNIT_ENV] = str(lease_fd)
    launcher = Path(__file__).with_name('provider_exec.py')
    return subprocess.Popen(
        [sys.executable, '-I', '-B', str(launcher), str(os.getpid()), *command],
        cwd=home, env=environment,
        stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
        shell=False, close_fds=True, start_new_session=True,
        pass_fds=(lease_fd,))
