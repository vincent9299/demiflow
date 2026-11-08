"""Fixed outer lifecycle guard; no job API, scheduling or durable run ledger.

Lives outside the analysis slice in an explicitly budgeted host service. It
starts only the reviewed controller/watchdog in their pre-provisioned roles.
A pinned cgroup.kill FD covers create/start races without reading state volume.
The enclosing host manager must kill the whole service subtree on supervisor
failure; automatic restart is forbidden until pending cleanup is reconciled.
"""
from __future__ import annotations

import argparse
import ctypes
import fcntl
import os
from pathlib import Path
import select
import signal
import stat
import subprocess
import threading
import time

from demiflow.workbench.process import process_identity
from demiflow.forge.io import no_symlinks
from demiflow.forge.schema import ForgeError
from demiflow.workbench.analysis import contracts as c
from demiflow.workbench.analysis.deployment import Deployment
from demiflow.workbench.analysis.database import private_directory
from demiflow.workbench.analysis.owned_cgroup import identity
from demiflow.workbench.analysis.owned_cgroup import read_control
from demiflow.workbench.analysis.watchdog import ParentWatch

STARTUP_SECONDS = 60
HEARTBEAT_SECONDS = 3
STOP_OBSERVATION_SECONDS = 5


class PinnedHierarchy:
    """Only the originally empty, dedicated deployment subtree is owned.

    Open FDs pin kernel objects across rename/removal; never reopen a kill path
    after failure. No supervisor code deletes cgroups, files or reservations.
    """

    def __init__(self, root):
        self.root = no_symlinks(Path(root))
        if not self.root.is_relative_to(Path('/sys/fs/cgroup')) or self.root == Path('/sys/fs/cgroup'):
            c.reject('Supervisor requires its fixed dedicated cgroup subtree')
        self.fds = []
        self.membership = {}
        self.roles = {}
        self.original = identity(self.root)
        self.closed = False
        try:
            with Path('/proc/self/cgroup').open('rb') as source:
                own = source.read(4097)
            if len(own) > 4096 or not own.startswith(b'0::/') or len(own.splitlines()) != 1:
                c.reject('Supervisor cannot determine its outer cgroup')
            own_path = Path('/sys/fs/cgroup') / own.decode('ascii').strip()[4:]
            if own_path == self.root or own_path.is_relative_to(self.root):
                c.reject('Supervisor must live outside the analysis failure domain')
            if read_control(self.root, 'cgroup.procs'):
                c.reject('Delegation root contains unexpected processes')
            self.events = self._open(self.root/'cgroup.events', os.O_RDONLY)
            if self.populated():
                c.reject('Supervisor never adopts live jobs; reconcile original deployment first', code='busy')
            self.kill_fd = self._open(self.root/'cgroup.kill', os.O_WRONLY)
            for role in ('controller', 'watchdog'):
                self.roles[role] = identity(self.root/role)
                self.membership[role] = self._open(self.root/role/'cgroup.procs', os.O_WRONLY)
            if identity(self.root) != self.original:
                c.reject('Supervisor delegation changed during capture', code='conflict')
        except BaseException:
            self.close()
            raise

    def _open(self, path, flags):
        fd = os.open(no_symlinks(path), flags | os.O_NOFOLLOW | os.O_CLOEXEC)
        self.fds.append(fd)
        return fd

    def populated(self):
        os.lseek(self.events, 0, os.SEEK_SET)
        raw = os.read(self.events, 4097)
        words = raw.decode('ascii').split()
        if len(raw) > 4096 or len(words) % 2:
            c.reject('Malformed pinned cgroup events')
        fields = dict(zip(words[::2], words[1::2]))
        if fields.get('populated') not in {'0', '1'}:
            c.reject('No definite pinned population observation')
        return fields['populated'] == '1'

    def verify(self):
        if identity(self.root) != self.original or any(identity(self.root/role) != owned for role, owned in self.roles.items()):
            c.reject('Original supervised hierarchy changed before launch', code='conflict')

    def kill(self):
        # The root contains only this pair and their admitted jobs. This also
        # stops a runc create that has not yet populated its new job leaf.
        if os.write(self.kill_fd, b'1\n') != 2:
            c.reject('Pinned hierarchy kill was incomplete', code='operation_failed')

    def close(self):
        if not self.closed:
            for fd in self.fds:
                os.close(fd)
            self.closed = True


class Supervisor:
    def __init__(self, deployment, binding):
        if not isinstance(deployment, Deployment) or threading.active_count() != 1:
            c.reject('Supervisor requires an admitted deployment and one thread')
        self.deployment, self.binding = deployment, str(no_symlinks(Path(binding)))
        root = private_directory(deployment.paths.state/'runtime')
        self.lock_fd = os.open(no_symlinks(root/'supervisor.lock'), os.O_CREAT | os.O_RDWR | os.O_NOFOLLOW, 0o600)
        try:
            lock = os.fstat(self.lock_fd)
            if not stat.S_ISREG(lock.st_mode) or stat.S_IMODE(lock.st_mode) != 0o600 or lock.st_uid != os.geteuid():
                c.reject('Supervisor lock is not an owned private regular file')
            fcntl.flock(self.lock_fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
            self.groups = PinnedHierarchy(deployment.paths.delegated_cgroup)
        except BaseException:
            os.close(self.lock_fd)
            raise
        self.children = {}
        self.running = True
        self.reason = None
        self.closed = False
        # Loaded before fork; only fixed libc calls and cgroup attach in child.
        self.libc = ctypes.CDLL(None, use_errno=True)

    def _launch(self, role):
        if role in self.children:
            c.reject('Supervisor cannot restart or replace an original child')
        self.groups.verify()
        reader, writer = os.pipe2(os.O_NONBLOCK | os.O_CLOEXEC)
        release = self.deployment.value['owner_release']
        args = [release['controller_python_path'], '-I', '-S', '-B', release['helper_binary_path'],
                'supervisor-child', '--binding', self.binding, '--mode', self.deployment.mode,
                '--role', role, '--heartbeat-fd', str(writer)]
        parent_pid = os.getpid()
        def enter_role():
            # PR_SET_PDEATHSIG is only a second guard. The enclosing host unit
            # must also kill all descendants if this supervisor itself dies.
            if self.libc.prctl(1, signal.SIGKILL, 0, 0, 0) != 0 or os.getppid() != parent_pid:
                os._exit(125)
            if os.write(self.groups.membership[role], b'0\n') != 2:
                os._exit(125)
            os.set_inheritable(self.groups.membership[role], False)
            os.umask(0o077)
        try:
            child = subprocess.Popen(args, stdin=subprocess.DEVNULL, stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL, cwd='/', close_fds=True, pass_fds=(writer, self.groups.membership[role]), preexec_fn=enter_role,
                env=dict(PATH='/usr/sbin:/usr/bin:/bin', PYTHONNOUSERSITE='1'))
        except BaseException:
            os.close(reader)
            raise
        finally:
            os.close(writer)
        original = process_identity(child.pid)
        # Retain Popen before observing identity so even an immediate exit is
        # reaped, and failed startup cannot leave an untracked child.
        item = dict(process=child, reader=reader, watch=None, ready=False, heartbeat=time.monotonic())
        self.children[role] = item
        if original is None:
            c.reject('Original supervised child exited before capture', code='operation_failed')
        item['watch'] = ParentWatch(original)

    def _poll(self):
        for role, item in self.children.items():
            watch = item['watch']
            if item['process'].poll() is not None or watch is None or watch.stopped():
                c.reject('Original supervised ' + role + ' exited', code='operation_failed')
            try:
                raw = os.read(item['reader'], 128)
            except BlockingIOError:
                raw = None
            if raw == b'':
                c.reject('Original supervised control pipe closed', code='operation_failed')
            if raw:
                if len(raw) > 64 or any(byte not in b'RH' for byte in raw):
                    c.reject('Supervised heartbeat is invalid or excessive')
                if b'R' in raw:
                    if item['ready'] or raw.count(b'R') != 1:
                        c.reject('Supervised readiness cannot be replayed')
                    item['ready'] = True
                if not item['ready']:
                    c.reject('Heartbeat preceded role readiness')
                item['heartbeat'] = time.monotonic()
            if role == 'watchdog' and item['ready'] and time.monotonic()-item['heartbeat'] > HEARTBEAT_SECONDS:
                c.reject('Independent watchdog stopped making progress', code='operation_failed')

    def _await_ready(self, role):
        end = time.monotonic() + STARTUP_SECONDS
        while self.running:
            self._poll()
            if self.children[role]['ready']:
                return
            if time.monotonic() >= end:
                c.reject('Supervised role startup timed out', code='operation_failed')
            select.select([item['reader'] for item in self.children.values()], [], [], 0.1)
        c.reject('Supervisor stopped during startup', code='operation_failed')

    def run(self):
        try:
            self._launch('watchdog'); self._await_ready('watchdog')
            self._launch('controller'); self._await_ready('controller')
            while self.running:
                self._poll()
                select.select([item['reader'] for item in self.children.values()], [], [], 0.1)
            self.reason = 'requested_stop'
        except (OSError, ForgeError) as exc:
            self.reason = str(exc)[:512]
        finally:
            # Kill precedes logging or any persistent write. Never release a
            # runtime lease: zero population is not namespace/file cleanup.
            result = self.stop()
        return dict(reason=self.reason, **result)

    def stop(self):
        self.running = False
        self.groups.kill()
        end = time.monotonic() + STOP_OBSERVATION_SECONDS
        while self.groups.populated() and time.monotonic() < end:
            time.sleep(0.05)
        stopped = not self.groups.populated()
        for item in self.children.values():
            item['process'].poll()  # reap direct children without blocking IO
        return dict(processes_observed_stopped=stopped, cleanup_proven=False,
                    restart_allowed=False, namespace_file_cleanup_proven=False)

    def close(self):
        if self.closed:
            return
        for item in self.children.values():
            if item['watch'] is not None:
                item['watch'].close()
            os.close(item['reader'])
        self.groups.close()
        os.close(self.lock_fd)
        self.closed = True


def _child(args):
    # Internal entry used only by the fixed supervisor argv. A regular file or
    # blocking pipe cannot redirect heartbeats onto the persistent state volume.
    fd = args.heartbeat_fd
    if fd is None or fd < 3 or not stat.S_ISFIFO(os.fstat(fd).st_mode) or os.get_blocking(fd):
        c.reject('Supervisor child requires an inherited nonblocking pipe')
    os.set_inheritable(fd, False)  # never leak it to runc, Jupyter or native jobs
    def emit(value=b'H'):
        # A stalled/full/broken supervisor pipe terminates this helper role.
        if os.write(fd, value) != 1:
            c.reject('Supervisor heartbeat failed', code='operation_failed')
    deployment = Deployment.load(args.binding, mode=args.mode, role=args.role)
    if args.role == 'watchdog':
        from demiflow.workbench.analysis.watchdog_service import WatchdogService
        service = WatchdogService(deployment, heartbeat=emit)
    else:
        from demiflow.workbench.analysis.controller import Controller
        service = Controller(deployment)
    signal.signal(signal.SIGTERM, lambda *_: setattr(service, 'running', False))
    signal.signal(signal.SIGINT, lambda *_: setattr(service, 'running', False))
    try:
        emit(b'R')
        service.serve()
    finally:
        service.close()
        os.close(fd)
    return 0


def main(argv=None):
    import json
    parser = argparse.ArgumentParser(prog='demiflow-analysis-supervisor')
    parser.add_argument('operation', choices=('supervisor', 'supervisor-child'))
    parser.add_argument('--binding', required=True)
    parser.add_argument('--mode', choices=('production', 'calibration'), default='production')
    parser.add_argument('--role', choices=('controller', 'watchdog'))
    parser.add_argument('--heartbeat-fd', type=int)
    args = parser.parse_args(argv)
    try:
        if args.operation == 'supervisor-child':
            if args.role is None:
                c.reject('Supervised child role is required')
            return _child(args)
        if args.role is not None or args.heartbeat_fd is not None:
            c.reject('Outer supervisor does not accept a child override')
        deployment = Deployment.load(args.binding, mode=args.mode, role='watchdog')
        supervisor = Supervisor(deployment, args.binding)
        signal.signal(signal.SIGTERM, lambda *_: setattr(supervisor, 'running', False))
        signal.signal(signal.SIGINT, lambda *_: setattr(supervisor, 'running', False))
        try:
            result = supervisor.run()
        finally:
            supervisor.close()
        print(json.dumps(result))
        # Stop always requires reconciliation; never ask the manager to restart.
        return 0 if result['reason'] == 'requested_stop' and result['processes_observed_stopped'] else 1
    except (OSError, ForgeError) as exc:
        print(json.dumps(dict(error=str(exc)[:1024], activation_allowed=False, cleanup_proven=False)))
        return 1


if __name__ == '__main__':
    raise SystemExit(main())
