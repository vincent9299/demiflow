"""Fixed development sandbox; no mount, ACL or production-identity mutation.

The caller is the trusted controller. Source scope, executable dependencies and
protected paths are explicit. Host UID mapping is retained, all capabilities
are dropped, and PID/network namespaces plus Landlock/seccomp enforce access.
"""
from __future__ import annotations

from dataclasses import dataclass
import fcntl
import ctypes
import hashlib
import json
import math
import os
from pathlib import Path
import platform
import secrets
import select
import socket
import stat
import subprocess
import sys
import tempfile
import threading

_PROBE_LOCK = threading.Lock()
_PROBE = None


def implementation_digest():
    digest = hashlib.sha256()
    for name in ('sandbox.py', 'sandbox_entry.py', 'sandbox_child.py', 'sandbox_native.py', 'sandbox_relay.py'):
        with Path(__file__).with_name(name).open('rb') as stream:
            source = stream.read(512 * 1024 + 1)
        if len(source) > 512 * 1024:
            raise RuntimeError('Sandbox implementation exceeds qualification byte budget')
        digest.update(name.encode() + b'\0' + source)
    return digest.hexdigest()


def execution_identity():
    """Bounded identity for receipt verification; never runs a capability probe."""
    return dict(backend='linux-user-pid-net-landlock-seccomp-v1', implementation=implementation_digest())


@dataclass(frozen=True)
class SandboxSpec:
    read_paths: tuple[str, ...]
    write_paths: tuple[str, ...]
    runtime_paths: tuple[str, ...]
    protected_paths: tuple[str, ...]
    private_home: str
    denied_paths: tuple[str, ...] = ()
    role: str = 'developer'
    seconds: float = 120
    rss_bytes: int = 2 * 1024**3
    processes: int = 32
    file_bytes: int = 64 * 1024**2
    private_bytes: int = 256 * 1024**2


def _paths(values, *, existing=True):
    if type(values) not in (list, tuple) or len(values) > 128:
        raise ValueError('Sandbox paths exceed the fixed scope budget')
    result = []
    for value in values:
        path = Path(value)
        if not path.is_absolute() or len(str(path)) > 4096:
            raise ValueError('Sandbox paths must be bounded absolute paths')
        physical = path.resolve(strict=existing)
        if existing:
            info = physical.stat()
            if not stat.S_ISDIR(info.st_mode) and not stat.S_ISREG(info.st_mode):
                raise ValueError('Sandbox paths must name regular files or directories')
        if physical == Path('/'):
            raise ValueError('The host filesystem root cannot be granted')
        result.append(str(physical))
    return sorted(set(result))


def _contains(parent, path):
    return path == parent or Path(path).is_relative_to(parent)


def specification(spec, command, env, cwd):
    if platform.system() != 'Linux' or platform.machine() != 'x86_64':
        raise ValueError('The qualified development sandbox requires Linux x86-64')
    if type(spec) is not SandboxSpec or spec.role not in {'developer', 'readonly', 'verifier'}:
        raise ValueError('A trusted development sandbox specification is required')
    for value, maximum in ((spec.seconds, 3600), (spec.rss_bytes, 16 * 1024**3),
                           (spec.processes, 64), (spec.file_bytes, 64 * 1024**2),
                           (spec.private_bytes, 512 * 1024**2)):
        if type(value) not in (int, float) or not math.isfinite(value) or not 0 < value <= maximum:
            raise ValueError('Sandbox resource limit is invalid')
    read = _paths(spec.read_paths)
    write = _paths(spec.write_paths)
    runtime = _paths(spec.runtime_paths)
    protected = _paths(spec.protected_paths, existing=False)
    # The control plane must never execute modules writable by this domain on
    # a later launch/gate. Platform development therefore needs a separate,
    # read-only installed control plane, not the checkout being edited.
    protected.extend(_paths([str(Path(__file__).resolve().parents[2]), sys.executable,
                             sys.prefix, sys.base_prefix, '/usr', '/lib', '/lib64']))
    # These are fixed launcher dependencies, not additional child read grants.
    # In particular /usr owns unshare and /lib* own its loader/native libraries.
    denied = _paths(spec.denied_paths, existing=False)
    home = _paths([spec.private_home])[0]
    if not Path(home).is_dir() or Path(home).stat().st_mode & 0o077:
        raise ValueError('Sandbox HOME must be a private directory')
    if spec.role != 'developer' and write:
        raise ValueError('Read-only/reviewer and verifier roles cannot write source paths')
    write.append(home)
    denied.extend(['/proc', '/sys', '/run'])
    for allowed in read + write + runtime:
        if _contains('/dev', allowed) or _contains(allowed, '/dev'):
            raise ValueError('Device paths cannot be supplied as sandbox scopes')
        if any(_contains(allowed, forbidden) or _contains(forbidden, allowed) for forbidden in denied):
            raise ValueError('Sandbox allowlist overlaps a protected path')
    for allowed in write:
        if any(_contains(allowed, dependency) or _contains(dependency, allowed) for dependency in runtime):
            raise ValueError('Sandbox write scope overlaps a trusted runtime path')
        if any(_contains(allowed, forbidden) or _contains(forbidden, allowed) for forbidden in protected):
            raise ValueError('Sandbox write scope overlaps a protected read-only path')
    for value in write:
        path = Path(value)
        if path.is_file() and path.stat().st_nlink != 1:
            raise ValueError('Hard-linked writable files are outside the sandbox scope')
        if path.is_dir():
            count = 0
            pending = [path]
            while pending:
                with os.scandir(pending.pop()) as entries:
                    for entry in entries:
                        count += 1
                        if count > 4096:
                            raise ValueError('Writable tree scope inventory exceeds budget')
                        if value != home and entry.name in {'.git', '.forge', '.codex', '.dsh', 'AGENTS.md'}:
                            raise ValueError('Writable tree contains protected development metadata')
                        info = entry.stat(follow_symlinks=False)
                        if stat.S_ISLNK(info.st_mode) or stat.S_ISREG(info.st_mode) and info.st_nlink != 1:
                            raise ValueError('Writable tree contains a link alias')
                        if stat.S_ISDIR(info.st_mode):
                            pending.append(entry.path)
    if type(command) not in (list, tuple) or not 1 <= len(command) <= 256:
        raise ValueError('Sandbox command exceeds argument budget')
    if any(type(arg) is not str or '\0' in arg for arg in command) or not Path(command[0]).is_absolute():
        raise ValueError('Sandbox command requires an absolute executable and string arguments')
    if type(env) is not dict or len(env) > 128 or any(type(k) is not str or type(v) is not str for k, v in env.items()):
        raise ValueError('Sandbox environment must be an explicit bounded mapping')
    if any(k in env for k in ('LD_PRELOAD', 'LD_LIBRARY_PATH', 'PYTHONHOME')):
        raise ValueError('Sandbox loader injection environment is not permitted')
    if 'PYTHONPATH' in env:
        if spec.role != 'verifier' or not env['PYTHONPATH']:
            raise ValueError('Only fixed verifier definitions may supply scoped PYTHONPATH')
        for entry in env['PYTHONPATH'].split(os.pathsep):
            physical = _paths([entry])[0]
            if entry != physical or not any(_contains(parent, physical) for parent in read + runtime):
                raise ValueError('Verifier PYTHONPATH must use physically scoped read/runtime paths')
    environment = dict(env, HOME=home, TMPDIR=home)
    for name in ('HTTP_PROXY', 'HTTPS_PROXY', 'ALL_PROXY', 'http_proxy', 'https_proxy', 'all_proxy', 'NO_PROXY', 'no_proxy'):
        environment.pop(name, None)
    from .sandbox_native import tree_usage
    source_roots = [path for path in write if path != home]
    source_roots = [path for path in source_roots if not any(other != path and _contains(other, path) for other in source_roots)]
    if any(_contains(path, home) or _contains(home, path) for path in source_roots):
        raise ValueError('Source write scope must be separate from private HOME')
    baseline_bytes, baseline_files = tree_usage(source_roots, 4096)
    # One fixed harmless device is needed by shells and native CLI subprocesses.
    # Never grant the device directory or caller-selected character devices.
    null = os.stat('/dev/null')
    if not stat.S_ISCHR(null.st_mode) or null.st_rdev != os.makedev(1, 3):
        raise ValueError('The fixed null device identity is unsupported')
    read.append('/dev/null')
    write.append('/dev/null')
    identities = {path: [os.stat(path).st_dev, os.stat(path).st_ino] for path in read + write + runtime}
    return dict(read_paths=read, write_paths=write, runtime_paths=runtime, identities=identities,
                command=list(command), env=environment, cwd=str(Path(cwd).resolve(strict=True)),
                seconds=spec.seconds, rss_bytes=spec.rss_bytes, processes=spec.processes, file_bytes=spec.file_bytes,
                private_home=home, private_bytes=spec.private_bytes, source_roots=source_roots,
                source_baseline_bytes=baseline_bytes, source_baseline_files=baseline_files)


def launch_sandbox(command, *, spec, env, cwd, lease_fd=None, control_socket=None, model_socket=None):
    """Return Popen only after the confined executable has successfully exec'd.

    terminate/kill target the host launcher: parent-death kills namespace PID 1,
    and the kernel terminates its complete descendant domain. No fallback.
    """
    implementation = execution_identity()
    value = specification(spec, command, env, cwd)
    pairs = []
    value['channels'] = {}
    for name, target in (('control', control_socket), ('model', model_socket)):
        if target is None:
            continue
        path = Path(target)
        if not path.is_absolute() or path != path.resolve(strict=True) or not stat.S_ISSOCK(path.stat().st_mode):
            raise ValueError('Fixed channel must name a physical trusted Unix socket')
        if spec.role == 'verifier':
            raise ValueError('Verifier sandbox cannot receive model/control channels')
        host, child = socket.socketpair(socket.AF_UNIX, socket.SOCK_SEQPACKET)
        pairs.append((host, child, str(path)))
        value['channels'][name] = child.fileno()
    raw = json.dumps(value, separators=(',', ':')).encode()
    if len(raw) > 131072:
        for host, child, _ in pairs:
            host.close(); child.close()
        raise ValueError('Sandbox specification exceeds launch byte budget')
    # This deployment's Python lacks os.memfd_create despite kernel support.
    libc = ctypes.CDLL(None, use_errno=True)
    fd = libc.syscall(319, b'forge-sandbox-spec', 1 | 2)
    if fd < 0:
        raise OSError(ctypes.get_errno(), 'Cannot create immutable sandbox specification')
    status_read, status_write = os.pipe2(os.O_CLOEXEC)
    process = None
    try:
        os.write(fd, raw)
        os.lseek(fd, 0, os.SEEK_SET)
        fcntl.fcntl(fd, 1033, 1 | 2 | 4 | 8)  # F_ADD_SEALS: seal/write/grow/shrink
        inherited = (fd, status_write) + (() if lease_fd is None else (lease_fd,)) + tuple(child.fileno() for _, child, _ in pairs)
        process = subprocess.Popen([sys.executable, '-I', '-B', str(Path(__file__).with_name('sandbox_entry.py')),
            str(os.getpid()), str(fd), str(status_write)], cwd=cwd,
            env={'PATH': '/usr/bin:/bin', 'LANG': 'C.UTF-8'}, stdin=subprocess.PIPE,
            stdout=subprocess.PIPE, stderr=subprocess.PIPE, close_fds=True, pass_fds=inherited, start_new_session=True)
        os.close(status_write)
        status_write = -1
        from demiflow.workbench.conversations.sandbox_relay import start_host
        for host, child, target in pairs:
            child.close()
            start_host(host, target, spec.seconds)
        if not select.select([status_read], [], [], 7)[0]:
            raise RuntimeError('Sandbox admission timed out')
        response = os.read(status_read, 4096)
        if response != b'ready\n':
            raise RuntimeError('Sandbox admission failed: ' + response.decode(errors='replace'))
        if execution_identity() != implementation:
            raise RuntimeError('Sandbox implementation changed during admission')
        process.execution_assurance = 'enforced'
        process.execution_evidence = dict(implementation, execution_id=secrets.token_hex(16))
        return process
    except BaseException:
        if process is not None:
            process.kill()
            process.communicate(timeout=5)
        for host, child, _ in pairs:
            host.close(); child.close()
        raise
    finally:
        os.close(fd)
        os.close(status_read)
        if status_write >= 0:
            os.close(status_write)


def backend_readiness():
    """One actual scratch confinement check per implementation/process.

    This reports host readiness. Provider protocols, model authorization and
    task acceptance remain separate, and each launch installs all restrictions.
    """
    global _PROBE
    identity = implementation_digest()
    with _PROBE_LOCK:
        if _PROBE is not None and _PROBE['implementation'] == identity:
            return dict(_PROBE)
        result = dict(available=False, implementation=identity,
            backend='linux-user-pid-net-landlock-seccomp-v1', host_uid=os.getuid(),
            identity_model='invoking_host_uid_with_zero_capabilities', reason='')
        try:
            with tempfile.TemporaryDirectory(prefix='forge-sandbox-readiness-') as base:
                root = Path(base)
                home = root / 'home'; home.mkdir(mode=0o700)
                source = root / 'source'; source.mkdir()
                readonly = root / 'readonly'; readonly.write_text('preserve')
                control = root / 'control'; control.write_text('secret')
                spec = SandboxSpec(read_paths=(str(readonly),), write_paths=(str(source),),
                    runtime_paths=('/usr', '/lib', '/lib64'), protected_paths=(str(readonly),),
                    denied_paths=(str(control),), private_home=str(home), seconds=3)
                code = '''import json,os,pathlib,socket,sys
source,readonly,control=sys.argv[1:]
pathlib.Path(source,'ok').write_text('allowed')
assert pathlib.Path(readonly).read_text()=='preserve'
for operation in (lambda:pathlib.Path(control).read_text(),lambda:os.truncate(readonly,0),
 lambda:os.chmod(readonly,0o777),lambda:os.open(readonly,os.O_RDONLY|os.O_TRUNC),
 lambda:pathlib.Path('/proc/self/environ').read_bytes(),lambda:socket.socket(socket.AF_UNIX)):
 try:operation()
 except OSError:continue
 raise RuntimeError('Required boundary operation was allowed')
print('ready')
'''
                process = launch_sandbox(['/usr/bin/python3', '-I', '-B', '-c', code,
                    str(source), str(readonly), str(control)], spec=spec, env={}, cwd=source)
                try:
                    stdout, stderr = process.communicate(timeout=5)
                finally:
                    if process.poll() is None:
                        process.kill(); process.communicate(timeout=5)
                if process.returncode != 0 or stdout != b'ready\n' or readonly.read_text() != 'preserve':
                    raise RuntimeError('Confinement behavior probe failed')
                result.update(available=True, reason='实际进程外边界可用；宿主 UID 保持映射，全部能力已清零，模型出口仍需任务绑定服务。')
        except (OSError, RuntimeError, ValueError, subprocess.SubprocessError) as error:
            result['reason'] = '受限执行启动关闭：' + type(error).__name__ + ': ' + str(error)[:512]
        _PROBE = result
        return dict(result)
