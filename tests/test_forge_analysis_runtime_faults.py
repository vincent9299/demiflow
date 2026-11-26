"""Controlled faults: real processes/pipes/pidfd; cgroup/OCI writes are doubles.

An injected blocking persistence function models fsync that never returns. It
is not evidence of actual device behavior or cgroup process/mount cleanup.
"""
import os
from pathlib import Path
import select
import subprocess
import sys
import time
from types import SimpleNamespace
from unittest.mock import Mock

import pytest

from demiflow.workbench.analysis.runtime import Runtime
from demiflow.workbench.analysis.watchdog import JobWatch
from demiflow.forge.process import process_identity
from demiflow.forge.schema import ForgeError

ID = 'j' + 'a'*32
ROOT = Path(__file__).resolve().parents[1]


def runtime_fixture():
    root, jobs, leaf = ({'device':1, 'inode':i} for i in (2,3,4))
    runtime = Runtime.__new__(Runtime)
    runtime.groups = SimpleNamespace(root_identity=root, jobs_identity=jobs, _root=lambda:None,
                                    kill=Mock(), verify=Mock())
    runtime.leases = {ID:dict(job_id=ID, role='native', state='created', reason=None,
        delegation_identity=root, jobs_identity=jobs, cgroup=dict(job_id=ID, role='native',
        delegation_identity=root, jobs_identity=jobs, cgroup_identity=leaf))}
    runtime._save = Mock()
    runtime._command = Mock()
    runtime.register_watch = Mock(return_value=True)
    return runtime


def test_failed_save_keeps_stop_fence_and_cannot_start_old_created_lease():
    runtime = runtime_fixture()
    runtime._save.side_effect = OSError('injected full state volume')
    with pytest.raises(OSError): runtime.stop(ID)
    assert runtime.groups.kill.call_count == 1
    assert runtime.leases[ID]['state'] == 'created'
    with pytest.raises(ForgeError, match='stop fence'):
        runtime.start(ID, authorize=lambda:True)
    runtime._command.assert_not_called()


def test_failed_kill_preserves_cleanup_intent_without_claiming_free():
    runtime = runtime_fixture()
    runtime.groups.kill.side_effect = OSError('injected cgroup write failure')
    with pytest.raises(OSError): runtime.stop(ID)
    saved = runtime._save.call_args.args[0]
    assert saved['state'] == 'cleanup_pending'
    assert ID in runtime.stop_fences
    assert runtime.leases[ID]['state'] != 'cleanup_complete'


@pytest.mark.parametrize('failure', ['watch_before', 'command', 'save_started', 'watch_after'])
def test_start_failure_stops_original_group_and_keeps_generation_unreplayable(failure):
    runtime = runtime_fixture()
    if failure == 'watch_before': runtime.register_watch.return_value = False
    elif failure == 'command': runtime._command.side_effect = OSError('start response lost')
    elif failure == 'save_started': runtime._save.side_effect = OSError('save response lost')
    else: runtime.register_watch.side_effect = [True, False]
    with pytest.raises((OSError, ForgeError)):
        runtime.start(ID, authorize=lambda:True)
    assert runtime.groups.kill.call_count == 1
    assert ID in runtime.stop_fences
    assert runtime.leases[ID]['state'] != 'cleanup_complete'
    if failure == 'watch_before': runtime._command.assert_not_called()


def test_owned_kill_is_observed_while_persistent_write_remains_blocked(record_property):
    # Actual independent process blocks on a pipe inside _save; the parent
    # requires the kill event first and verifies no accepted/cleanup reply.
    script = '''
import os, sys
from test_forge_analysis_runtime_faults import runtime_fixture, ID
runtime=runtime_fixture()
runtime.groups.kill.side_effect=lambda *args:print('kill-original',flush=True)
def blocked(value):
    print('save-blocked',flush=True)
    sys.stdin.buffer.read(1)
    raise OSError('injected persistent IO failure')
runtime._save=blocked
try: runtime.stop(ID)
except OSError: print('pending-fenced',ID in runtime.stop_fences,flush=True)
'''
    started = time.monotonic()
    process = subprocess.Popen([sys.executable, '-B', '-u', '-c', script], cwd=ROOT,
        env=dict(os.environ, PYTHONPATH=str(ROOT)+os.pathsep+str(ROOT/'tests'), PYTHONDONTWRITEBYTECODE='1'),
        stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    try:
        assert select.select([process.stdout], [], [], 5)[0]
        assert process.stdout.readline().strip() == b'kill-original'
        elapsed = time.monotonic()-started
        assert process.stdout.readline().strip() == b'save-blocked'
        assert process.poll() is None
        assert not select.select([process.stdout], [], [], .1)[0]
        process.stdin.write(b'x'); process.stdin.flush()
        output, error = process.communicate(timeout=5)
        assert process.returncode == 0, error
        assert output.strip() == b'pending-fenced True'
        record_property('spawn_to_mock_kill_seconds', elapsed)
        record_property('evidence_layer', 'real subprocess/blocked pipe; fake cgroup kill; no disk fault or OCI')
    finally:
        if process.poll() is None: process.kill(); process.wait(timeout=2)
        for stream in (process.stdin, process.stdout, process.stderr): stream.close()


def test_empty_create_window_retains_stop_until_late_population():
    runtime = runtime_fixture()
    groups = runtime.groups
    groups.populated = Mock(side_effect=[False, True])
    parent = process_identity(os.getpid())
    watcher = JobWatch(groups, runtime.leases[ID]['cgroup'], controller=parent, web_parent=parent,
                       deadline=time.monotonic()+10)
    try:
        assert watcher.tick(stop_requested=True)['process_observed_stopped']
        assert watcher.tick()['reason'] == 'stop_requested'
        groups.kill.assert_called_once()
    finally: watcher.close()
