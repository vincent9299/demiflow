"""Coordinator DC1/DC2/DC3 fault windows, with real owned fake workers."""
import json
import os
from pathlib import Path
import signal
import sqlite3
import subprocess
import time

import pytest

from demiflow.workbench.conversations import service as module
from demiflow.workbench.conversations.common import root_identity
from demiflow.workbench.conversations.service import Service
from demiflow.workbench.conversations.store import StorageUnavailable
from demiflow.workbench.conversations.store import Store
from demiflow.forge.schema import ForgeError
from test_forge_development_chat import configuration, wait_turn


def wait_running(service, tid):
    until = time.monotonic() + 10
    while time.monotonic() < until:
        if service.store.turn(tid)['state'] == 'running': return
        time.sleep(.03)
    raise AssertionError(service.store.turn(tid))


@pytest.mark.parametrize('boundary', ['directory', 'unit', 'ownership', 'ownership_after', 'pipe', 'phase_before', 'phase_after', 'popen'])
def test_unstarted_failure_recovers_same_identity_without_fd_leaks(tmp_path, monkeypatch, boundary):
    root, path, _ = configuration(tmp_path)
    service = Service(path)
    sid = service.create('wfixture', root, 'create', 'codex')['id']
    before_fds = set(os.listdir('/proc/self/fd'))
    original_open, original_mkdir, original_update = os.open, Path.mkdir, service.store.update
    failure = OSError('injected ' + boundary)
    def fail(*args, **kwargs): raise failure
    def open_unit(p, *args, **kwargs):
        if str(p).endswith('.unit'): raise failure
        return original_open(p, *args, **kwargs)
    def mkdir(p, *args, **kwargs):
        if p == service.root / sid: raise failure
        return original_mkdir(p, *args, **kwargs)
    def update(tid, **changes):
        if boundary == 'ownership' and 'process' in changes: raise failure
        if boundary == 'ownership_after' and 'process' in changes:
            original_update(tid, **changes)
            raise failure
        if changes.get('launch_phase') == 'spawning':
            if boundary == 'phase_before': raise failure
            if boundary == 'phase_after':
                original_update(tid, **changes)
                raise failure
        return original_update(tid, **changes)
    try:
        with monkeypatch.context() as m:
            if boundary == 'directory': m.setattr(Path, 'mkdir', mkdir)
            elif boundary == 'unit': m.setattr(os, 'open', open_unit)
            elif boundary == 'pipe': m.setattr(os, 'pipe', fail)
            elif boundary == 'popen': m.setattr(module.subprocess, 'Popen', fail)
            else: m.setattr(service.store, 'update', update)
            with pytest.raises(OSError, match='injected'):
                service.start('wfixture', root, sid, 'first', 0, 'normal')
        turn = service.store.submission(sid, 'first')
        assert turn['cleanup'] == 'complete' and turn['state'] == 'failed'
        assert turn['dispatch'] == 'not_sent'
        assert service.recover('wfixture', root, sid, 1)['recovery']['can_continue']
        assert not service.workers and not service.worker_targets
        assert set(os.listdir('/proc/self/fd')) == before_fds
        replay = service.start('wfixture', root, sid, 'first', 0, 'normal')
        assert (replay['id'], replay['epoch']) == (turn['id'], turn['epoch'])
        assert not service.workers
        next_turn = service.start('wfixture', root, sid, 'second', 1, 'normal')
        assert wait_turn(service, next_turn['id'])['state'] == 'completed'
    finally: service.close()


def test_admitted_failure_with_unavailable_cleanup_recovers_after_restart(tmp_path, monkeypatch):
    root, path, _ = configuration(tmp_path)
    service = Service(path)
    sid = service.create('wfixture', root, 'create', 'codex')['id']
    original_open = os.open
    def open_unit(p, *args, **kwargs):
        if str(p).endswith('.unit'): raise OSError('injected unit failure')
        return original_open(p, *args, **kwargs)
    with monkeypatch.context() as m:
        m.setattr(os, 'open', open_unit)
        m.setattr(service.store, 'finish_unstarted', lambda *a: (_ for _ in ()).throw(StorageUnavailable('offline', code='busy')))
        with pytest.raises(OSError): service.start('wfixture', root, sid, 'first', 0, 'normal')
    saved = service.store.submission(sid, 'first')
    assert saved['launch_phase'] == 'admitted' and saved['cleanup'] != 'complete' and saved['process'] is None
    service.close()
    service = Service(path)
    try:
        recovered = service.recover('wfixture', root, sid, 1)
        assert recovered['recovery']['can_continue'] and recovered['turn']['id'] == saved['id']
        assert recovered['turn']['state'] == 'failed'
        assert service.start('wfixture', root, sid, 'first', 0, 'normal')['id'] == saved['id']
        assert not service.workers
    finally: service.close()


def test_recovery_rejects_replaced_unit_and_worker_rejects_stale_identity(tmp_path):
    root, path, _ = configuration(tmp_path)
    service = Service(path)
    sid = service.create('wfixture', root, 'create', 'codex')['id']
    try:
        saved, _ = service.store.submit('wfixture', sid, 'first', 0, 'normal', root_identity(root), service.policy_hash)
        unit = service.root / sid / (saved['id'] + '.unit')
        unit.parent.mkdir(); unit.touch()
        info = unit.stat(); identity = dict(device=info.st_dev, inode=info.st_ino)
        ownership = dict(unit_path=str(unit), unit_identity=identity, nonce=saved['epoch'])
        service.store.update(saved['id'], process=json.dumps(ownership), launch_phase='spawning')
        with pytest.raises(ForgeError, match='identity'):
            service.store.claim_worker(saved['id'], 'wrong-epoch', identity, {'pid':os.getpid()})
        with pytest.raises(ForgeError, match='identity'):
            service.store.claim_worker(saved['id'], saved['epoch'], dict(device=info.st_dev, inode=info.st_ino + 1), {'pid':os.getpid()})
        replacement = unit.with_suffix('.replacement'); replacement.touch(); replacement.replace(unit)
        assert not service.recover('wfixture', root, sid, 1)['recovery']['can_continue']
        with pytest.raises(ForgeError, match='writer'):
            service.start('wfixture', root, sid, 'second', 1, 'normal')
        assert service.store.turn(saved['id'])['launch_phase'] == 'spawning'
    finally: service.close()


def test_legacy_missing_launch_phase_is_not_upgraded_to_unstarted_proof(tmp_path):
    root, path, _ = configuration(tmp_path)
    service = Service(path)
    sid = service.create('wfixture', root, 'legacy', 'codex')['id']
    saved, _ = service.store.submit('wfixture', sid, 'first', 0, 'retained requirement', root_identity(root), service.policy_hash)
    service.close()
    with service.store.connect(write=True) as db:
        db.execute('ALTER TABLE turns DROP COLUMN launch_phase')
    service = Service(path)
    try:
        assert service.store.turn(saved['id'])['launch_phase'] == 'unknown'
        assert service.store.turn(saved['id'])['text'] == 'retained requirement'
        assert not service.recover('wfixture', root, sid, 1)['recovery']['can_continue']
    finally: service.close()


def test_restart_inventory_cannot_overwrite_a_newer_cleanup_receipt(tmp_path, monkeypatch):
    root, path, _ = configuration(tmp_path)
    service = Service(path)
    sid = service.create('wfixture', root, 'create', 'codex')['id']
    saved, _ = service.store.submit('wfixture', sid, 'first', 0, 'normal', root_identity(root), service.policy_hash)
    service.close()
    original = Store.unresolved
    def inventory(store):
        rows = original(store)
        store.update(saved['id'], state='failed', cleanup='complete', dispatch='acknowledged')
        return rows
    with monkeypatch.context() as m:
        m.setattr(Store, 'unresolved', inventory)
        service = Service(path)
    try:
        done = service.store.turn(saved['id'])
        assert done['cleanup'] == 'complete' and done['state'] == 'failed'
        assert done['dispatch'] == 'acknowledged'
    finally: service.close()


@pytest.mark.parametrize('running', [False, True])
def test_lost_popen_return_keeps_reservation_across_restart_until_actual_drain(tmp_path, monkeypatch, running):
    root, path, _ = configuration(tmp_path)
    service = Service(path)
    sid = service.create('wfixture', root, 'create', 'codex')['id']
    original_popen, spawned = module.subprocess.Popen, []
    children = set()
    message = 'hang' if running else 'normal'
    def spawn_then_interrupt(*args, **kwargs):
        child = original_popen(*args, **kwargs)
        spawned.append(child)
        if running:
            wait_running(service, service.store.submission(sid, 'first')['id'])
            from demiflow.resources.process_observation import descendants
            until = time.monotonic() + 5
            while len(children) < 2 and time.monotonic() < until:
                children.update(descendants(child.pid)); time.sleep(.02)
            assert len(children) >= 2
        child.send_signal(signal.SIGSTOP)
        os.waitpid(child.pid, os.WUNTRACED)
        raise KeyboardInterrupt('interrupted real Popen return')
    before_fds = set(os.listdir('/proc/self/fd'))
    try:
        with monkeypatch.context() as m:
            m.setattr(module.subprocess, 'Popen', spawn_then_interrupt)
            with pytest.raises(KeyboardInterrupt): service.start('wfixture', root, sid, 'first', 0, message)
        saved = service.store.submission(sid, 'first')
        assert spawned[0].poll() is None and saved['cleanup'] != 'complete'
        assert saved['id'] in {t['id'] for t in service.store.unresolved()}
        assert set(os.listdir('/proc/self/fd')) == before_fds
        assert not service.recover('wfixture', root, sid, 1)['recovery']['can_continue']
        assert service.start('wfixture', root, sid, 'first', 0, message)['id'] == saved['id']
        with pytest.raises(ForgeError, match='writer'):
            service.start('wfixture', root, sid, 'second', 1, 'normal')
        service.close(); service = Service(path)
        assert not service.recover('wfixture', root, sid, 1)['recovery']['can_continue']
        with pytest.raises(ForgeError, match='writer'):
            service.start('wfixture', root, sid, 'second', 1, 'normal')
        spawned[0].send_signal(signal.SIGCONT); spawned[0].wait(timeout=10)
        done = wait_turn(service, saved['id'])
        assert done['cleanup'] == 'complete'
        assert done['dispatch'] == ('acknowledged' if running else 'not_sent')
        assert json.loads(done['result'])['reason'] == 'parent_closed'
        assert not any(Path('/proc', str(pid)).exists() for pid in children)
        assert service.recover('wfixture', root, sid, 1)['recovery']['can_continue']
        second = service.start('wfixture', root, sid, 'second', 1, 'normal')
        assert wait_turn(service, second['id'])['state'] == 'completed'
    finally:
        for child in spawned:
            if child.poll() is None:
                child.send_signal(signal.SIGCONT)
                child.wait(timeout=10)
        service.close()


@pytest.mark.parametrize('when', ['before', 'after'])
def test_worker_identity_commit_failure_does_not_overstate_cleanup(tmp_path, monkeypatch, when):
    root, path, _ = configuration(tmp_path)
    service = Service(path)
    sid = service.create('wfixture', root, 'create', 'codex')['id']
    wrapper = tmp_path / 'worker_commit_fault.py'
    wrapper.write_text('''import sys
sys.path.insert(0, %r)
from demiflow.workbench.conversations.store import Store
from demiflow.workbench.conversations.worker import run
original = Store.claim_worker
def claim(*args):
    if %r == 'after': original(*args)
    raise OSError('injected worker identity persistence failure')
Store.claim_worker = claim
run(sys.argv[1], sys.argv[2], int(sys.argv[3]), int(sys.argv[4]))
''' % (str(Path(__file__).resolve().parents[1]), when))
    original_popen = module.subprocess.Popen
    def launch(command, **kwargs):
        command = list(command); command[3] = str(wrapper)
        return original_popen(command, **kwargs)
    try:
        with monkeypatch.context() as m:
            m.setattr(module.subprocess, 'Popen', launch)
            tid = service.start('wfixture', root, sid, 'first', 0, 'normal')['id']
        service.workers[tid][0].wait(timeout=10)
        recovered = service.recover('wfixture', root, sid, 1)
        assert recovered['recovery']['can_continue'] is (when == 'before')
        assert recovered['turn']['dispatch'] == 'not_sent'
        if when == 'after':
            with pytest.raises(ForgeError, match='writer'):
                service.start('wfixture', root, sid, 'second', 1, 'normal')
    finally: service.close()


def test_wrong_cancel_identity_no_side_effect_and_database_failure_stops_only_confirmed_target(tmp_path, monkeypatch):
    root, path, config = configuration(tmp_path)
    other = tmp_path / 'other'; other.mkdir()
    (other / 'AGENTS.md').write_text('Other project'); (other / 'code.py').write_text('# other')
    config['projects']['other'] = dict(config['projects']['wfixture'], root=str(other))
    path.write_text(json.dumps(config)); service = Service(path)
    try:
        sid = service.create('wfixture', root, 'first', 'codex')['id']
        wrong = service.create('wfixture', root, 'wrong', 'codex')['id']
        other_sid = service.create('other', other, 'other', 'codex')['id']
        tid = service.start('wfixture', root, sid, 'first', 0, 'hang')['id']
        other_tid = service.start('other', other, other_sid, 'first', 0, 'hang')['id']
        wait_running(service, tid); wait_running(service, other_tid)
        pipes = {t:service.workers[t][1] for t in (tid, other_tid)}
        for workspace, project, session, turn in [
                ('wfixture', root, wrong, tid), ('other', other, sid, tid),
                ('wfixture', root, sid, other_tid), ('wfixture', root, sid, 'missing')]:
            with pytest.raises(ForgeError): service.cancel(workspace, project, session, turn, 'bad')
        with monkeypatch.context() as m:
            m.setattr(service.store, 'cancel', lambda *a: (_ for _ in ()).throw(ForgeError('business refusal', code='busy')))
            with pytest.raises(ForgeError, match='business refusal'):
                service.cancel('wfixture', root, sid, tid, 'not-storage')
        for t in pipes:
            assert service.workers[t][1] == pipes[t]; os.fstat(pipes[t])
            assert service.store.turn(t)['stop_at'] is None and service.store.turn(t)['state'] == 'running'
        with monkeypatch.context() as m:
            m.setattr(sqlite3, 'connect', lambda *a, **k: (_ for _ in ()).throw(sqlite3.OperationalError('injected database offline')))
            with pytest.raises(ForgeError, match='does not belong'):
                service.cancel('wfixture', root, wrong, tid, 'bad-offline')
            assert service.workers[tid][1] == pipes[tid]
            with pytest.raises(StorageUnavailable): service.cancel('wfixture', root, sid, tid, 'good-offline')
        assert service.workers[tid][1] == -1
        assert wait_turn(service, tid)['state'] == 'cancelled'
        assert service.store.turn(other_tid)['state'] == 'running'
        assert service.workers[other_tid][1] == pipes[other_tid]
        service.cancel('other', other, other_sid, other_tid, 'stop-other')
        assert wait_turn(service, other_tid)['cleanup'] == 'complete'
    finally: service.close()
