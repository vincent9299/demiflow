"""Trusted PID 1: exec confinement, task deadline and kernel-owned child domain.

The host /proc mount is never exposed to the untrusted child. PID 1 can use it
for bounded observation; killing PID 1 kills every descendant in this namespace.
"""
import json
import math
import os
from pathlib import Path
import resource
import select
import signal
import socket
import stat
import sys
import threading
import time

sys.path.insert(0, str(Path(__file__).resolve().parent))
from sandbox_native import drop_capabilities, landlock, prctl, seccomp, tree_usage
from sandbox_relay import loopback, namespace_channel, namespace_listener


def close_except(keep):
    for name in os.listdir('/proc/self/fd'):
        fd = int(name)
        if fd not in keep:
            try:
                os.close(fd)
            except OSError:
                pass


def _process_fields(proc_stat):
    return bounded_contents(proc_stat).decode().rsplit(')', 1)[1].split()


def _open_process_fds(pid, proc_stat, deadline):
    try:
        return os.scandir(f'/proc/{pid}/fd')
    except PermissionError:
        # During exit Linux may revoke fd access before reporting a zombie.
        # The open stat file stays bound to this task even if its PID is reused.
        while True:
            try:
                proc_stat.seek(0)
                fields = _process_fields(proc_stat)
            except (FileNotFoundError, ProcessLookupError):
                return None
            if int(fields[17]) != 1:
                raise  # the thread group still has other members
            if fields[0] in ('Z', 'X', 'x'):
                return None
            remaining = deadline - time.monotonic()
            if not (int(fields[6]) & 0x4) or remaining <= 0:  # Linux PF_EXITING
                raise
            time.sleep(min(.005, remaining))


def sample(owner, maximum, *, files, deadline):
    pending, seen, rss = [owner], set(), 0
    anonymous, seen_files, descriptors = 0, set(), 0
    # One bounded exit observation window shared by all tasks in this sample.
    exit_deadline = min(deadline, time.monotonic() + .1)
    while pending:
        if len(seen) > maximum or len(pending) > maximum:
            raise RuntimeError('Sandbox process budget exceeded')
        pid = pending.pop()
        if pid in seen:
            continue
        seen.add(pid)
        try:
            with open(f'/proc/{pid}/stat', 'rb', buffering=0) as proc_stat:
                fields = _process_fields(proc_stat)
                rss += int(fields[21]) * os.sysconf('SC_PAGE_SIZE')
                entries = _open_process_fds(pid, proc_stat, exit_deadline) if files else None
            if entries is not None:
                with entries:
                    for entry in entries:
                        descriptors += 1
                        if descriptors > 8192:
                            raise RuntimeError('Sandbox observed descriptor budget exceeded')
                        try:
                            info = entry.stat()
                        except (FileNotFoundError, ProcessLookupError):
                            continue  # a closed FD or exiting task has released its reference
                        identity = info.st_dev, info.st_ino
                        if stat.S_ISREG(info.st_mode) and info.st_nlink == 0 and identity not in seen_files:
                            seen_files.add(identity)
                            anonymous += info.st_size
            with os.scandir(f'/proc/{pid}/task') as tasks:
                for index, task in enumerate(tasks):
                    if index >= 256:
                        raise RuntimeError('Sandbox thread budget exceeded')
                    try:
                        pending.extend(int(p) for p in bounded_read(task.path + '/children').split())
                    except (FileNotFoundError, ProcessLookupError):
                        pass
        except (FileNotFoundError, ProcessLookupError):
            pass
    return rss, anonymous


def bounded_read(path):
    with open(path, 'rb') as stream:
        return bounded_contents(stream)


def bounded_contents(stream):
    raw = stream.read(32769)
    if len(raw) > 32768:
        raise RuntimeError('Sandbox process observation exceeded byte budget')
    return raw


def main():
    if os.getpid() != 1:
        raise RuntimeError('Sandbox launcher did not become namespace PID 1')
    spec_fd, status_fd = map(int, sys.argv[1:3])
    chunks, total = [], 0
    while True:
        chunk = os.read(spec_fd, 65536)
        if not chunk:
            break
        total += len(chunk)
        if total > 2 * 1024 * 1024:
            raise RuntimeError('Sandbox launch specification exceeded byte budget')
        chunks.append(chunk)
    raw = b''.join(chunks)
    spec = json.loads(raw)
    os.close(spec_fd)
    status = bounded_read('/proc/self/status').decode()
    host_pid = int(next(line for line in status.splitlines() if line.startswith('NSpid:')).split()[1])
    prctl(4, 0, 0, 0, 0)  # trusted PID 1 memory/FDs are not dumpable
    channels = []
    spec['env']['DEMIFLOW_FORGE_SANDBOX_ACTIVE'] = '1'
    if spec.get('channels') and not spec.get('direct_network'):
        loopback()
    for name, fd in spec.get('channels', {}).items():
        listener = namespace_listener()
        spec['env']['DEMIFLOW_' + name.upper() + '_ENDPOINT'] = '127.0.0.1:' + str(listener.getsockname()[1])
        channels.append((socket.socket(fileno=fd), listener))
    placeholder = '__DEMIFLOW_MODEL_ENDPOINT__'
    if any(placeholder in argument for argument in spec['command']):
        endpoint = spec['env'].get('DEMIFLOW_MODEL_ENDPOINT')
        if endpoint is None:
            raise RuntimeError('Model endpoint placeholder requires the fixed model channel')
        spec['command'] = [argument.replace(placeholder, endpoint) for argument in spec['command']]
    read_error, write_error = os.pipe2(os.O_CLOEXEC)
    child = os.fork()
    if child == 0:
        try:
            os.close(read_error)
            os.chdir(spec['cwd'])
            rlimits = [
                (resource.RLIMIT_NOFILE, 2048),
                (resource.RLIMIT_FSIZE, spec['file_bytes']),
                (resource.RLIMIT_CORE, 0),
            ]
            if spec['seconds'] > 0:
                rlimits.append((resource.RLIMIT_CPU, max(1, int(spec['seconds']))))
            for kind, cap in rlimits:
                _, hard = resource.getrlimit(kind)
                cap = cap if hard == resource.RLIM_INFINITY else min(cap, hard)
                resource.setrlimit(kind, (cap, cap))
            close_except({0, 1, 2, write_error})
            landlock(spec['read_paths'], spec['write_paths'], spec['runtime_paths'], spec['identities'],
                     read_policy=spec['read_policy'], traversal_paths=spec['traversal_paths'],
                     denied_paths=spec['denied_paths'], credential_read_path=spec['credential_read_path'])
            drop_capabilities()
            seccomp(direct_network=bool(spec.get('direct_network', False)), verifier=spec.get('role') == 'verifier')
            os.execve(spec['command'][0], spec['command'], spec['env'])
        except BaseException as error:
            os.write(write_error, (type(error).__name__ + ': ' + str(error))[:2048].encode())
            os._exit(126)
    os.close(write_error)
    if not select.select([read_error], [], [], spec['admission_seconds'])[0]:
        raise RuntimeError('Sandbox child confinement timed out')
    failure = os.read(read_error, 2049)
    os.close(read_error)
    if failure:
        os.write(status_fd, b'error:' + failure)
        return 126
    relay_failed = threading.Event()
    for channel, listener in channels:
        threading.Thread(target=namespace_channel,
            args=(channel, listener, spec['seconds'], relay_failed), daemon=True).start()
    os.write(status_fd, b'ready\n')
    os.close(status_fd)
    deadline = math.inf if spec['seconds'] == 0 else time.monotonic() + spec['seconds']
    last_disk = 0
    while time.monotonic() < deadline:
        # Namespace PID 1 adopts orphaned grandchildren (e.g. shell pipelines).
        # Reap every exited child before sampling so dead tasks do not consume
        # the live process budget.
        while True:
            try:
                pid, state = os.waitpid(-1, os.WNOHANG)
            except ChildProcessError:
                return 125
            if not pid:
                break
            if pid == child:
                return os.waitstatus_to_exitcode(state) if os.WIFEXITED(state) else 128 + os.WTERMSIG(state)
        if relay_failed.is_set():
            sys.stderr.write('[sandbox] terminated (125): control/model channel relay failed\n')
            sys.stderr.flush()
            return 125
        check_disk = time.monotonic() - last_disk >= .2
        try:
            rss, anonymous = sample(host_pid, spec['processes'], files=check_disk, deadline=deadline)
        except Exception as exc:
            sys.stderr.write(f'[sandbox] terminated (125): process/resource limit ({exc})\n')
            sys.stderr.flush()
            return 125
        if rss > spec['rss_bytes']:
            sys.stderr.write(f'[sandbox] terminated (125): rss_bytes={rss} exceeded limit={spec["rss_bytes"]}\n')
            sys.stderr.flush()
            return 125
        if check_disk:
            last_disk = time.monotonic()
            try:
                private_size, private_files = tree_usage([spec['private_home']])
                source_size, source_files = tree_usage(spec['source_roots'])
            except (FileNotFoundError, NotADirectoryError):
                time.sleep(.05)
                continue
            except Exception as exc:
                sys.stderr.write(f'[sandbox] terminated (125): disk scan failed ({type(exc).__name__}: {exc})\n')
                sys.stderr.flush()
                return 125
            total_bytes = private_size + max(0, source_size - spec['source_baseline_bytes']) + anonymous
            total_files = private_files + max(0, source_files - spec['source_baseline_files'])
            if total_bytes > spec['private_bytes'] or total_files > 8192:
                sys.stderr.write(
                    f'[sandbox] terminated (125): disk/files exceeded '
                    f'(bytes={total_bytes}/{spec["private_bytes"]}, files={total_files}/8192)\n'
                )
                sys.stderr.flush()
                return 125
        time.sleep(.05)
    sys.stderr.write(f'[sandbox] terminated (124): wall-clock deadline ({spec["seconds"]}s) exceeded\n')
    sys.stderr.flush()
    return 124


if __name__ == '__main__':
    try:
        result = main()
    except BaseException as exc:
        try:
            sys.stderr.write(f'[sandbox] terminated (126): {type(exc).__name__}: {exc}\n')
            sys.stderr.flush()
        except OSError:
            pass
        try:
            os.write(int(sys.argv[2]), ('error:' + type(exc).__name__ + ': ' + str(exc))[:2048].encode())
        except OSError:
            pass
        result = 126
    # Exiting namespace PID 1 is the kernel's whole-domain kill operation.
    os._exit(result)
