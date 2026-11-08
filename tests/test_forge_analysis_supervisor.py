"""Real pidfd/heartbeat child faults with synthetic hierarchy control only."""
import os
import subprocess
import sys
import time
from types import SimpleNamespace

import pytest

from demiflow.workbench.process import process_identity
from demiflow.workbench.analysis import supervisor
from demiflow.workbench.analysis.watchdog import ParentWatch
from demiflow.forge.schema import ForgeError


class Hierarchy:
    def __init__(self, children):
        self.children = children
        self.kills = 0
    def kill(self):
        self.kills += 1
        for item in self.children.values():
            if item['process'].poll() is None: item['process'].kill()
    def populated(self): return any(item['process'].poll() is None for item in self.children.values())
    def close(self): pass


def guard(monkeypatch, mode):
    service = supervisor.Supervisor.__new__(supervisor.Supervisor)
    service.children = {}; service.running = True; service.reason = None; service.closed = False
    service.groups = Hierarchy(service.children)
    service.lock_fd = os.open('/dev/null', os.O_RDONLY)
    monkeypatch.setattr(supervisor, 'HEARTBEAT_SECONDS', .35)
    monkeypatch.setattr(supervisor, 'STARTUP_SECONDS', 2)
    monkeypatch.setattr(supervisor, 'STOP_OBSERVATION_SECONDS', 1)
    def launch(role):
        reader, writer = os.pipe2(os.O_NONBLOCK | os.O_CLOEXEC)
        script = '''import os,sys,time
fd=int(sys.argv[1]); mode=sys.argv[2]
os.write(fd,b'R')
if mode=='exit': time.sleep(.2); os._exit(7)
if mode=='heartbeat':
 while True: os.write(fd,b'H'); time.sleep(.05)
else: time.sleep(8)
'''
        role_mode = mode if role == 'watchdog' else 'idle'
        child = subprocess.Popen([sys.executable, '-I', '-S', '-B', '-c', script, str(writer), role_mode],
            stdin=subprocess.DEVNULL, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, pass_fds=(writer,))
        os.close(writer)
        service.children[role] = dict(process=child, reader=reader, watch=ParentWatch(process_identity(child.pid)),
            ready=False, heartbeat=time.monotonic())
    service._launch = launch
    return service


@pytest.mark.parametrize('mode', ['exit', 'idle'])
def test_watchdog_death_or_stalled_heartbeat_stops_pair_without_storage_or_restart(monkeypatch, mode, record_property):
    service = guard(monkeypatch, mode)
    try:
        started = time.monotonic()
        result = service.run()
        elapsed = time.monotonic()-started
        assert service.groups.kills == 1
        assert result['processes_observed_stopped']
        assert not result['cleanup_proven'] and not result['restart_allowed']
        assert elapsed < 3
        assert ('exited' in result['reason'] or 'pipe closed' in result['reason']) if mode == 'exit' else 'progress' in result['reason']
        record_property('fault_to_pair_stop_seconds', elapsed)
        record_property('evidence_layer', 'real processes/pidfd/pipe; simulated cgroup hierarchy')
    finally:
        service.groups.kill()
        for item in service.children.values(): item['process'].wait(timeout=2)
        service.close()


def test_controller_exit_also_stops_original_watchdog(monkeypatch):
    service = guard(monkeypatch, 'heartbeat')
    try:
        service._launch('watchdog'); service._await_ready('watchdog')
        service._launch('controller'); service._await_ready('controller')
        service.children['controller']['process'].kill()
        service.children['controller']['process'].wait(timeout=2)
        with pytest.raises(ForgeError, match='controller exited'): service._poll()
        assert service.stop()['processes_observed_stopped']
        assert service.children['watchdog']['process'].poll() is not None
    finally:
        service.groups.kill()
        for item in service.children.values(): item['process'].wait(timeout=2)
        service.close()


def test_unadmitted_or_non_cgroup_target_cannot_become_a_supervisor(tmp_path):
    with pytest.raises(ForgeError): supervisor.Supervisor(None, tmp_path/'binding')
    with pytest.raises(ForgeError): supervisor.PinnedHierarchy(tmp_path)
    assert not list(tmp_path.iterdir())


def test_internal_child_rejects_regular_file_or_blocking_pipe(tmp_path):
    path = tmp_path/'heartbeat'; path.write_bytes(b'unchanged')
    fd = os.open(path, os.O_WRONLY)
    try:
        with pytest.raises(ForgeError, match='nonblocking pipe'):
            supervisor._child(SimpleNamespace(heartbeat_fd=fd))
    finally: os.close(fd)
    assert path.read_bytes() == b'unchanged'
    read, write = os.pipe()
    try:
        with pytest.raises(ForgeError, match='nonblocking pipe'):
            supervisor._child(SimpleNamespace(heartbeat_fd=write))
    finally: os.close(read); os.close(write)
