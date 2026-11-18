"""Real isolated provider and gate processes; all projects and commands are fixtures."""
import json
import os
from pathlib import Path
import subprocess
import sys
import time

import pytest

from demiflow.forge.schema import ForgeError
from demiflow.workbench.conversations.common import root_identity, wire
from demiflow.workbench.conversations.context import bridge_spec, ProtocolBridge
from demiflow.workbench.conversations.development import initial
from demiflow.workbench.conversations.service import Service
from test_forge_development_chat import configuration


@pytest.fixture
def managed_project(tmp_path):
    from test_forge_development import DevelopmentTests
    fixture = DevelopmentTests(); fixture.setUp()
    _, path, value = configuration(tmp_path)
    value['projects']['wfixture'] = dict(root=str(fixture.root), kind='demiforge_registered',
        phase='implementation', pipeline='orders', writes=['orders'], watch=['orders'], sources=['AGENTS.md'],
        non_goals=['No production/platform changes'], authorization='Isolated fixture development', allow_tests=True)
    path.write_text(json.dumps(value))
    try:
        yield fixture.root, path, value
    finally:
        fixture.doCleanups()


def gate_done(service, tid, timeout=20):
    until = time.monotonic() + timeout
    while time.monotonic() < until:
        turn = service.store.turn(tid)
        if turn['cleanup'] == 'complete' and turn['gate_cleanup'] == 'complete':
            development = json.loads(turn['development'])
            if not development['busy']: return turn, development
        time.sleep(.05)
    raise AssertionError(service.store.public_turn(service.store.turn(tid)))


def wait_phase(service, tid, phase):
    until = time.monotonic() + 15
    while time.monotonic() < until:
        turn = service.store.turn(tid)
        if turn['gate_phase'] == phase: return turn
        if turn['cleanup'] == turn['gate_cleanup'] == 'complete':
            raise AssertionError(service.store.public_turn(turn))
        time.sleep(.05)
    raise AssertionError(service.store.public_turn(service.store.turn(tid)))


def task_file(service, sid):
    return service.root / 'forge-development' / 'wfixture' / sid / 'task.json'


@pytest.mark.parametrize('provider', ['codex', 'deepseek'])
def test_automatic_gate_preserves_session_baseline_and_never_claims_review(managed_project, provider):
    root, path, config = managed_project
    if provider == 'deepseek':
        executable = path.parent / 'fake-managed-acp'
        fixture = Path(__file__).with_name('devchat_fake_acp.py')
        executable.write_text('#!' + sys.executable + '\nimport runpy\nrunpy.run_path(' + repr(str(fixture)) + ',run_name="__main__")\n')
        executable.chmod(0o700)
        config['providers'] = {'deepseek': dict(executable=str(executable), version='0.2.1-rc.2', model='fixture')}
        path.write_text(json.dumps(config))
    legacy = (root / '.demiforge/tasks/test/task.json').read_bytes()
    service = Service(path)
    try:
        sid = service.create_project('wfixture', root, 'create', provider)['id']
        first = service.start_project('wfixture', root, sid, 'first', 0, 'managed-edit original intent')
        # No show/events calls are needed to drive the background gate.
        turn, result = gate_done(service, first['id'])
        assert turn['state'] == 'completed' and result['state'] == 'review_unavailable', result
        assert result['gates'] == dict(check='passed', verify='passed', review='unavailable')
        assert result['assurance'] == 'gate_only' and result['accepted_fingerprint'] is None
        original = json.loads(task_file(service, sid).read_text())
        assert original['goal'] == 'managed-edit original intent'
        assert original['reads'] and turn['delivery'] == turn['dispatch'] == 'acknowledged'
        assert 'managed-edit original intent' in (service.root / sid / 'received-context.txt').read_text()
        assert not (root / '.demiforge/tasks' / sid).exists()
        duplicate = service.start_project('wfixture', root, sid, 'first', 0, 'managed-edit original intent')
        assert duplicate['id'] == first['id']
        second = service.start_project('wfixture', root, sid, 'second', 1, 'Continue the same original scope')
        _, again = gate_done(service, second['id'])
        assert again['state'] == 'review_unavailable'  # zero changes this turn, task still differs
        current = json.loads(task_file(service, sid).read_text())
        assert current['baseline'] == original['baseline'] and current['goal'] == original['goal']
        assert len([n for n in current['notes'] if n['kind'] == 'decision']) == 2
        assert current['check']['changed'] == ['orders/operators/map.py']
        assert (root / '.demiforge/tasks/test/task.json').read_bytes() == legacy
        prompt = (service.root / sid / 'received-context.txt').read_text()
        assert 'managed-edit original intent' in prompt and 'Continue the same original scope' in prompt
        compact = service.compact('wfixture', root, sid, 'compact', 2)
        until = time.monotonic() + 10
        while service.store.turn(compact['id'])['cleanup'] != 'complete' and time.monotonic() < until: time.sleep(.05)
        assert service.store.turn(compact['id'])['state'] == 'completed'
        resumed = service.start_project('wfixture', root, sid, 'after-compact', 3, 'Preserve all prior human requirements')
        gate_done(service, resumed['id'])
        prompt = (service.root / sid / 'received-context.txt').read_text()
        assert 'managed-edit original intent' in prompt and 'Continue the same original scope' in prompt
        assert 'Preserve all prior human requirements' in prompt
        assert json.loads(task_file(service, sid).read_text())['baseline'] == original['baseline']
        events = service.store.events('wfixture', sid)['events']
        assert any(e['type'] == 'development.state' and e['data']['state'] == 'verifying' for e in events)
        with (root / 'orders/operators/map.py').open('a') as stream:
            stream.write('# later external editor change\n')
        stale = service.show('wfixture', sid)['turn']['development']
        assert stale['state'] == 'stale' and stale['gates']['verify'] == 'stale'
    finally:
        service.close()


def test_discussion_skips_verifier_and_managed_tools_cannot_grant_acceptance(managed_project):
    root, path, config = managed_project
    service = Service(path)
    try:
        sid = service.create_project('wfixture', root, 'create', 'codex')['id']
        turn = service.start_project('wfixture', root, sid, 'read', 0, 'Explain the existing source')
        _, result = gate_done(service, turn['id'])
        assert result['state'] == 'not_applicable' and result['gates']['verify'] == 'missing'
        manifest = json.loads(service.store.turn(turn['id'])['context'])
        assert manifest['binding']['fingerprint'] == result['source_fingerprint']
        task = json.loads(task_file(service, sid).read_text())
        assert task['verification'] is None and task['managed']['acceptance'] is None
        operations = bridge_spec(config['projects']['wfixture'])['inputSchema']['properties']['operation']['enum']
        assert not set(operations) & {'verify', 'self_review', 'handoff', 'refresh'}
        bridge = ProtocolBridge(service.store, turn['id'], config['projects']['wfixture'], service.root / sid / turn['id'])
        for operation in ('verify', 'self_review', 'handoff', 'check'):
            with pytest.raises(ForgeError, match='cannot execute'):
                bridge.invoke(dict(operation=operation, arguments={}))
        root.rename(root.with_name('moved-project'))
        history = service.show('wfixture', sid)
        assert history['turn']['id'] == turn['id']
        assert history['turn']['development']['state'] == 'stale'
        assert any(event['type'] == 'message.user' for event in service.store.events('wfixture', sid)['events'])
    finally:
        service.close()


def test_context_source_refresh_keeps_first_baseline(managed_project):
    root, path, _ = managed_project
    service = Service(path)
    try:
        sid = service.create_project('wfixture', root, 'create', 'codex')['id']
        first = service.start_project('wfixture', root, sid, 'edit-entry', 0, 'managed-entry update')
        gate_done(service, first['id'])
        original = json.loads(task_file(service, sid).read_text())['baseline']
        second = service.start_project('wfixture', root, sid, 'continue', 1, 'Read current code and continue')
        turn, result = gate_done(service, second['id'])
        assert turn['delivery'] == 'acknowledged' and result['gates']['check'] == 'passed', result
        assert json.loads(task_file(service, sid).read_text())['baseline'] == original
    finally:
        service.close()


def slow_verifier(root, marker):
    project = root / '.demiforge/project.json'
    config = json.loads(project.read_text())
    command = ('import json,os,subprocess,sys,time; from pathlib import Path; '
        'child=subprocess.Popen([sys.executable,"-c","import os,time;os.setsid();time.sleep(60)"]); '
        f'Path({str(marker)!r}).write_text(json.dumps([os.getpid(),child.pid])); '
        'print("verification started",flush=True);time.sleep(60)')
    config['pipelines']['orders']['verify'] = [dict(id='slow', argv=[sys.executable, '-c', command], env={}, timeout=60)]
    project.write_text(json.dumps(config))


@pytest.mark.parametrize('stop', ['cancel', 'deadline'])
def test_gate_cancel_and_deadline_drain_detached_verifier(managed_project, tmp_path, stop):
    root, path, config = managed_project
    marker = tmp_path / 'verifier-pids.json'
    slow_verifier(root, marker)
    if stop == 'deadline':
        config['limits']['turn_seconds'] = 4
        path.write_text(json.dumps(config))
    service = Service(path)
    sentinel = subprocess.Popen([sys.executable, '-c', 'import time;time.sleep(60)'])
    try:
        sid = service.create_project('wfixture', root, 'create', 'codex')['id']
        started = time.monotonic()
        turn = service.start_project('wfixture', root, sid, 'edit', 0, 'managed-edit verify')
        wait_phase(service, turn['id'], 'verifying')
        with pytest.raises(ForgeError, match='writer'):
            service.start_project('wfixture', root, sid, 'overlap', 1, 'No overlapping writes')
        until = time.monotonic() + 3
        while not marker.exists() and time.monotonic() < until: time.sleep(.03)
        pids = json.loads(marker.read_text())
        if stop == 'cancel':
            value = service.cancel('wfixture', root, sid, turn['id'], 'stop')
            assert value['development']['state'] == 'stopping'
        saved, result = gate_done(service, turn['id'], timeout=10)
        assert result['state'] in {'cancelled', 'unknown', 'failed'} and result['accepted_fingerprint'] is None
        assert not result['busy'] and saved['gate_cleanup'] == 'complete'
        assert sentinel.poll() is None
        assert not any(Path('/proc', str(pid)).exists() for pid in pids)
        if stop == 'deadline': assert time.monotonic() - started < 8
        assert service.recover('wfixture', root, sid, 1)['recovery']['can_continue']
    finally:
        service.close(); sentinel.terminate(); sentinel.wait(timeout=5)


def test_restart_never_replays_unknown_gate_or_releases_unproven_lease(managed_project):
    root, path, _ = managed_project
    service = Service(path)
    sid = service.create_project('wfixture', root, 'create', 'codex')['id']
    with service.lock:
        turn, _ = service.store.submit('wfixture', sid, 'uncertain', 0, 'Original user intent',
            root_identity(root), service.policy_hash, development=initial(sid))
        service.store.update(turn['id'], cleanup='complete', state='completed', gate_phase='verifying', gate_cleanup='unknown',
            development=wire(dict(initial(sid), state='unknown', reason='Lost verification receipt')).decode())
    service.close(); service = Service(path)
    try:
        time.sleep(.3)
        assert not service.workers and not service.gate_workers
        assert not service.recover('wfixture', root, sid, 1)['recovery']['can_continue']
        with pytest.raises(ForgeError, match='writer'):
            service.start_project('wfixture', root, sid, 'retry', 1, 'Continue without replay')
        assert not task_file(service, sid).exists()
    finally:
        service.close()


def test_managed_pre_spawn_failure_releases_busy_projection(managed_project, monkeypatch):
    root, path, _ = managed_project
    service = Service(path)
    try:
        sid = service.create_project('wfixture', root, 'create', 'codex')['id']
        with monkeypatch.context() as patch:
            patch.setattr(subprocess, 'Popen', lambda *a, **kw: (_ for _ in ()).throw(OSError('fixture spawn failure')))
            with pytest.raises(OSError, match='spawn failure'):
                service.start_project('wfixture', root, sid, 'fail', 0, 'Never dispatched user request')
        recovered = service.recover('wfixture', root, sid, 1)
        assert recovered['recovery']['can_continue']
        assert recovered['turn']['development']['state'] == 'failed'
        assert not recovered['turn']['development']['busy']
        next_turn = service.start_project('wfixture', root, sid, 'continue', 1, 'Continue original user request')
        _, value = gate_done(service, next_turn['id'])
        assert value['state'] == 'not_applicable'
        assert json.loads(task_file(service, sid).read_text())['goal'] == 'Never dispatched user request'
    finally:
        service.close()


def test_queued_gate_rejects_changed_deployment_before_any_verifier(managed_project):
    root, path, config = managed_project
    service = Service(path)
    sid = service.create_project('wfixture', root, 'create', 'codex')['id']
    service.gate_wakeup.set(); service.gate_thread.join(timeout=2)
    turn, _ = service.store.submit('wfixture', sid, 'queued', 0, 'Original user intent',
        root_identity(root), service.policy_hash, development=initial(sid))
    service.store.update(turn['id'], delivery='acknowledged', dispatch='acknowledged')
    service.store.finish_provider(turn['id'], state='completed', cleanup='complete', result=dict(provider_completed=True))
    service.close()
    config['projects']['wfixture']['allow_tests'] = False
    path.write_text(json.dumps(config))
    service = Service(path)
    try:
        _, result = gate_done(service, turn['id'])
        assert result['state'] == 'stale' and 'policy' in result['reason']
        assert not service.workers and not service.gate_workers
        assert not task_file(service, sid).exists()
    finally:
        service.close()


def test_gate_drains_when_retained_homes_and_evidence_exceed_shared_disk_budget(managed_project, tmp_path):
    from demiflow.workbench.conversations.common import private_directory_bytes
    root, path, config = managed_project
    marker = tmp_path / 'disk-verifier-pids.json'
    slow_verifier(root, marker)
    project_path = root / '.demiforge/project.json'
    project = json.loads(project_path.read_text())
    command = project['pipelines']['orders']['verify'][0]
    command['argv'][-1] = command['argv'][-1].replace('print("verification started",flush=True);',
        'Path("{scratch}/payload.bin").write_bytes(b"x" * (3 * 1024**2));print("verification started",flush=True);')
    project_path.write_text(json.dumps(project))
    config['limits'].update(provider_bytes=1024**2, private_total_bytes=4 * 1024**2)
    path.write_text(json.dumps(config))
    service = Service(path)
    try:
        retained = service.root / 'retained-provider'; retained.mkdir(mode=0o700)
        (retained / 'history.bin').write_bytes(b'x' * (2 * 1024**2))
        sid = service.create_project('wfixture', root, 'create', 'codex')['id']
        turn = service.start_project('wfixture', root, sid, 'edit', 0, 'managed-edit check shared disk')
        saved, result = gate_done(service, turn['id'])
        assert result['reason'] == 'resource_limit' and result['accepted_fingerprint'] is None
        assert saved['gate_cleanup'] == 'complete' and not result['busy']
        sizes = private_directory_bytes(service.root, entries=service.deployment.limits.sessions + 64)
        limit = service.deployment.limits.private_total_bytes
        assert sizes['retained-provider'] < limit and sizes['forge-development'] < limit
        assert sum(sizes.values()) > limit
        assert 'chat.sqlite3' not in sizes and 'control.reserve' not in sizes
        assert not any(Path('/proc', str(pid)).exists() for pid in json.loads(marker.read_text()))
        assert (retained / 'history.bin').stat().st_size == 2 * 1024**2
    finally:
        service.close()


def test_ancestor_guidance_changes_stale_gate_and_cannot_be_rebound(managed_project):
    from demiflow.forge.development.managed import ManagedTasks
    root, path, _ = managed_project
    ancestor = root.parent / 'AGENTS.md'
    original = 'Preserve the original ancestor fixture requirement.\n'
    ancestor.write_text(original)
    service = Service(path)
    try:
        sid = service.create_project('wfixture', root, 'create', 'codex')['id']
        first = service.start_project('wfixture', root, sid, 'norm-change', 0, 'managed-norm-alter fixture')
        turn, result = gate_done(service, first['id'])
        assert turn['dispatch'] == 'acknowledged'
        assert original in (service.root / sid / 'received-context.txt').read_text()
        assert result['state'] == 'stale' and 'passed' not in result['gates'].values(), result
        saved = json.loads(task_file(service, sid).read_text())
        baseline = saved['managed']['bound_contract']['context_baseline']
        assert str(ancestor) in json.dumps(baseline)
        assert saved['verification'] is None
        manifest = json.loads(turn['context'])
        intent = next(source for source in manifest['sources'] if source['source_id'] == 'controller-task-intent')
        assert intent['version'] == manifest['binding']['fingerprint']
        second = service.start_project('wfixture', root, sid, 'continue', 1, 'Continue without changing the original contract')
        next_turn, _ = gate_done(service, second['id'])
        assert next_turn['dispatch'] == 'not_sent'
        assert json.loads(task_file(service, sid).read_text())['managed']['bound_contract']['context_baseline'] == baseline
        assert ManagedTasks(service.root, 'wfixture', root).status(sid)['development_state'] == 'stale'
    finally:
        service.close()


def test_new_worker_can_finish_legacy_turn_without_migrating_live_database(tmp_path):
    from demiflow.workbench.conversations.policy import Limits
    from demiflow.workbench.conversations.store import Store
    root = tmp_path / 'project'; root.mkdir()
    store = Store(tmp_path / 'legacy-state', Limits(control_reserve=4096), initialize=True)
    session = store.create('legacy', root_identity(root), 'codex', 'create', 'policy')
    turn, _ = store.submit('legacy', session['id'], 'send', 0, 'Preserve the ongoing legacy conversation',
        root_identity(root), 'policy')
    new_columns = {'development', 'development_control', 'gate_process', 'gate_phase', 'gate_cleanup'}
    with store.connect(write=True) as db:
        for column in sorted(new_columns):
            db.execute('ALTER TABLE turns DROP COLUMN ' + column)
    # A running old Service has not initialized/migrated its schema. The newly
    # spawned worker must write its ordinary completion receipt without doing so.
    worker_store = Store(store.root, store.limits)
    worker_store.finish_provider(turn['id'], state='completed', cleanup='complete',
        result=dict(provider_completed=True, cleanup='complete', changed_files=0))
    saved = worker_store.turn(turn['id'])
    assert saved['state'] == 'completed' and saved['cleanup'] == 'complete'
    assert json.loads(saved['result'])['provider_completed'] is True
    assert saved['text'] == 'Preserve the ongoing legacy conversation'
    with worker_store.connect() as db:
        assert not new_columns & {row[1] for row in db.execute('PRAGMA table_info(turns)')}
