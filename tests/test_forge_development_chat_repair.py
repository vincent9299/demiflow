"""Finite internal segments share one real user request and its original budget."""
import json
import os
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
import sys
import threading
import time
from types import SimpleNamespace

import pytest

from demiflow.forge.schema import ForgeError
from demiflow.workbench.conversations.common import root_identity, wire
from demiflow.workbench.conversations.development import initial
from demiflow.workbench.conversations.policy import Limits
from demiflow.workbench.conversations.service import Service
from demiflow.workbench.conversations.store import Store
from test_forge_development_chat_managed import managed_project, gate_done, task_file


def repair_provider(root, path, config):
    executable = path.parent / 'repair-provider'
    fixture = Path(__file__).with_name('devchat_fake_repair_provider.py')
    executable.write_text('#!' + sys.executable + '\nimport runpy\nrunpy.run_path(' + repr(str(fixture)) + ',run_name="__main__")\n')
    executable.chmod(0o700)
    config['providers']['codex']['executable'] = str(executable)
    config['limits']['turn_seconds'] = 30
    path.write_text(json.dumps(config))
    project_path = root / '.demiforge/project.json'
    project = json.loads(project_path.read_text())
    project['pipelines']['orders']['verify'] = [dict(id='repair-result', argv=[sys.executable, '-c',
        'from pathlib import Path; assert "# repaired" in Path("orders/operators/map.py").read_text(), "fixture needs implementation repair"'], env={}, timeout=5)]
    project_path.write_text(json.dumps(project))


@pytest.mark.parametrize('mode,rounds,final', [('success', 1, 'review_unavailable'), ('no-progress', 1, 'failed'), ('progress', 3, 'failed')])
def test_internal_repairs_share_task_request_deadline_and_stop_loss(managed_project, mode, rounds, final):
    root, path, config = managed_project
    repair_provider(root, path, config)
    service = Service(path)
    try:
        sid = service.create_project('wfixture', root, 'create', 'codex')['id']
        request = 'repair-fixture:' + mode
        turn = service.start_project('wfixture', root, sid, 'request', 0, request)
        saved, result = gate_done(service, turn['id'], timeout=35)
        control = json.loads(saved['development_control'])
        assert result['state'] == final, result
        assert control['repair_round'] == rounds
        assert control['deadline'] == saved['created'] + 30
        calls = [json.loads(line) for line in (service.root / sid / 'provider-calls.jsonl').read_text().splitlines()]
        assert len(calls) == rounds + 1 and len({call['submission'] for call in calls}) == rounds + 1
        assert calls[0]['message'] == request
        assert all('不是新的真人授权' in call['message'] for call in calls[1:])
        assert len(control['segments']) == rounds + 1
        assert len({segment['epoch'] for segment in control['segments']}) == rounds + 1
        task = json.loads(task_file(service, sid).read_text())
        assert task['goal'] == request
        assert len([note for note in task['notes'] if note['kind'] == 'decision']) == 1
        with service.store.connect() as db:
            assert db.execute('SELECT count(*) FROM turns WHERE session=?', (sid,)).fetchone()[0] == 1
            rows = db.execute("SELECT body FROM events WHERE session=?", (sid,)).fetchall()
            assert sum(json.loads(row['body'])['type'] == 'message.user' for row in rows) == 1
        assert service.store.session(sid)['revision'] == 1
        if mode == 'no-progress': assert control['failure_repeats'] == 2
        if mode == 'progress': assert 'budget exhausted' in result['reason']
    finally:
        service.close()


def pending_repair(tmp_path):
    root = tmp_path / 'project'; root.mkdir()
    store = Store(tmp_path / 'state', Limits(control_reserve=4096), initialize=True)
    session = store.create('w', root_identity(root), 'codex', 'c', 'p')
    turn, _ = store.submit('w', session['id'], 's', 0, 'Original human intent', root_identity(root), 'p', development=initial(session['id']))
    store.update(turn['id'], cleanup='complete', state='completed', gate_cleanup='pending', gate_phase='checking')
    value = dict(initial(session['id']), state='failed', gates=dict(check='failed', verify='missing', review='unavailable'), source_fingerprint='same')
    store.schedule_repair(turn['id'], value, [dict(rule_id='DF-04', reason='Fix the implementation', evidence='untrusted tool output')])
    store.gate_state(turn['id'], json.loads(store.turn(turn['id'])['development']), phase='repair_pending', cleanup='complete')
    return root, store, session, turn


def test_pending_repair_holds_writer_lease_and_cancel_never_restarts(tmp_path):
    root, store, session, turn = pending_repair(tmp_path)
    with pytest.raises(ForgeError, match='writer'):
        store.submit('w', session['id'], 'other', 1, 'Another message', root_identity(root), 'p')
    store.cancel('w', session['id'], turn['id'])
    assert store.begin_repair(turn['id']) is None
    saved = store.turn(turn['id'])
    assert json.loads(saved['development'])['state'] == 'cancelled'
    assert saved['gate_phase'] == 'finished' and saved['cleanup'] == 'complete'


def test_pending_repair_deadline_is_not_reset_on_reload(tmp_path):
    root, store, session, turn = pending_repair(tmp_path)
    saved = store.turn(turn['id']); control = json.loads(saved['development_control'])
    control['deadline'] = time.time() - 1
    store.update(turn['id'], development_control=wire(control).decode())
    reopened = Store(store.root, store.limits)
    assert reopened.begin_repair(turn['id']) is None
    assert json.loads(reopened.turn(turn['id'])['development_control'])['deadline'] == control['deadline']
    assert json.loads(reopened.turn(turn['id'])['development_control'])['repair_round'] == 0


@pytest.mark.parametrize('repairable,action,expected', [
    (True, 'fix_within_scope', 'repair_pending'),
    (False, 'fix_within_scope', 'stale'),
    (None, 'fix_within_scope', 'stale'),
    (True, 'inspect_uncertain_execution', 'unknown'),
])
def test_current_failure_requires_explicit_domain_repairability(tmp_path, monkeypatch, repairable, action, expected):
    from demiflow.workbench.conversations import development
    root, store, session, turn = pending_repair(tmp_path)
    store.update(turn['id'], gate_phase='claimed', gate_cleanup='pending')
    current = dict(task_id=session['id'], source_fingerprint='current-candidate', accepted_fingerprint='old-candidate',
        development_state='stale', changed_total=1, gates=dict(check='failed', verify='stale', review='stale'),
        violations=['DF-01 context changed: refresh and read changed sources'], feedback=[dict(rule_id='DF-01',
            reason='Current candidate requires refreshed context', path=None, evidence=[], allowed_next_action=action)])
    if repairable is not None: current['repairable'] = repairable
    class Tasks:
        def status(self, *_): return current
        def check(self, *_, **__): return current
    monkeypatch.setattr(development, '_load', lambda *_: (SimpleNamespace(limits=store.limits), store,
        store.turn(turn['id']), session, dict(allow_tests=True)))
    monkeypatch.setattr(development, 'project_tasks', lambda *_: Tasks())
    development.execute('unused', turn['id'])
    saved = store.turn(turn['id'])
    assert json.loads(saved['development'])['state'] == expected
    assert saved['gate_phase'] == ('repair_pending' if expected == 'repair_pending' else 'finished')


@pytest.fixture
def independent_reviewer():
    """Actual one-shot HTTP protocol; deterministic response, no paid model."""
    calls = []
    class Handler(BaseHTTPRequestHandler):
        def log_message(self, *_): pass
        def do_POST(self):
            request = json.loads(self.rfile.read(int(self.headers['Content-Length'])))
            calls.append(request)
            assert self.path == '/v1/responses'
            if request['input'] == 'fixture development transport':
                raw = json.dumps(dict(status='completed', model='fixture', output=[])).encode()
                self.send_response(200); self.send_header('Content-Length', str(len(raw))); self.end_headers(); self.wfile.write(raw)
                return
            assert request['tools'] == [] and request['tool_choice'] == 'none' and request['store'] is False
            material = json.loads(request['input'].split('\n\n', 1)[1])
            assert material['original_goal'].startswith('repair-fixture:')
            assert material['verification']['verification_source'] == 'live_tree_rechecked'
            failed = material['original_goal'] in {'repair-fixture:review', 'repair-fixture:review-exhaust'} and '# reviewed' not in json.dumps(material['differences'])
            if material['decisions'][-1]['text'] == 'repair-fixture:followup-review':
                failed = '# reviewed follow-up implementation' not in json.dumps(material['differences'])
            claims = {f'DF-0{i}': dict(status='pass', reason='Fixture inspected the bound original goal and implementation',
                evidence=['orders/operators/map.py']) for i in range(1, 8)}
            if failed:
                claims['DF-04'].update(status='fail', reason='The fixture requires the reviewed implementation marker')
            response = dict(status='completed', model='fixture', output=[dict(type='message', role='assistant', status='completed',
                content=[dict(type='output_text', text=json.dumps(dict(claims=claims)))])])
            raw = json.dumps(response).encode()
            self.send_response(200); self.send_header('Content-Length', str(len(raw))); self.end_headers(); self.wfile.write(raw)
    server = ThreadingHTTPServer(('127.0.0.1', 0), Handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True); thread.start()
    try: yield 'http://127.0.0.1:' + str(server.server_port) + '/v1', calls
    finally: server.shutdown(); server.server_close(); thread.join(timeout=2)


@pytest.mark.parametrize('mode,reviews,expected', [('success', 1, 'accepted'), ('review', 2, 'accepted'), ('review-exhaust', 2, 'failed')])
def test_independent_review_and_bounded_repair_complete_same_root_turn(managed_project, independent_reviewer, monkeypatch, mode, reviews, expected):
    root, path, config = managed_project
    repair_provider(root, path, config)
    endpoint, calls = independent_reviewer
    monkeypatch.setenv('FIXTURE_REVIEW_KEY', 'offline-fixture-only')
    config['providers']['codex'].update(base_url=endpoint, credential_env='FIXTURE_REVIEW_KEY')
    path.write_text(json.dumps(config))
    service = Service(path)
    try:
        sid = service.create_project('wfixture', root, 'create', 'codex')['id']
        started = service.start_project('wfixture', root, sid, 'request', 0, 'repair-fixture:' + mode)
        saved, result = gate_done(service, started['id'], timeout=35)
        assert result['state'] == expected, saved['result'] + '\n' + json.dumps(result)
        control = json.loads(saved['development_control'])
        assert control['review_attempts'] == reviews and control['repair_round'] == 1
        assert len(calls) == reviews
        assert result['assurance'] == 'gate_only'
        record = json.loads(task_file(service, sid).read_text())
        assert len([note for note in record['notes'] if note['kind'] == 'decision']) == 1
        assert len(control['segments']) == 2
        if expected == 'accepted':
            assert result['source_fingerprint'] == result['accepted_fingerprint']
            assert set(result['gates'].values()) == {'passed'}
            service.close(); service = Service(path)
            reread = service.show('wfixture', sid)['turn']['development']
            assert reread['state'] == 'accepted' and reread['review_attempts'] == reviews
            if mode == 'success':
                cancelled, _ = service.store.submit('wfixture', sid, 'cancel-before-dispatch', 1,
                    'Further discussion', root_identity(root), service.policy_hash, development=initial(sid))
                service.store.cancel('wfixture', sid, cancelled['id'])
                service.store.finish_unstarted(service.store.turn(cancelled['id']))
                assert service.show('wfixture', sid)['turn']['development']['state'] == 'cancelled'
        else:
            assert 'review budget' in result['reason'].lower()
    finally: service.close()


def test_unknown_repair_dispatch_is_never_replayed(managed_project):
    root, path, config = managed_project
    repair_provider(root, path, config)
    service = Service(path)
    try:
        sid = service.create_project('wfixture', root, 'create', 'codex')['id']
        turn = service.start_project('wfixture', root, sid, 'request', 0, 'repair-fixture:disconnect')
        saved, result = gate_done(service, turn['id'], timeout=35)
        assert saved['dispatch'] == 'unknown', saved['result']
        assert result['state'] == 'unknown', result
        assert json.loads(saved['development_control'])['repair_round'] == 1
        service.close(); service = Service(path)
        time.sleep(.2)
        assert service.store.turn(turn['id'])['epoch'] == saved['epoch']
        assert len((service.root / sid / 'provider-calls.jsonl').read_text().splitlines()) == 2
    finally: service.close()


def test_actual_restricted_developer_verifier_broker_and_review_acceptance(managed_project, independent_reviewer, monkeypatch, tmp_path):
    from test_forge_development_runtime import combined_configuration
    root, path, config = managed_project
    repair_provider(root, path, config)
    host = tmp_path/'host-partition'; host.mkdir()
    host_config, _, _, domain, _ = combined_configuration(host, root, monkeypatch)
    partition = json.loads(host_config.read_text())
    config.update({key: partition[key] for key in ('state_root', 'runtime_domain', 'host_budget')})
    domain.chmod(0o700)
    endpoint, calls = independent_reviewer
    monkeypatch.setenv('FIXTURE_REVIEW_KEY', 'offline-fixture-only')
    config['mode'] = 'restricted'
    config['limits']['turn_seconds'] = 45
    config['providers']['codex'].update(base_url=endpoint, credential_env='FIXTURE_REVIEW_KEY')
    path.write_text(json.dumps(config))
    declared = root / '.demiforge/project.json'
    registration = json.loads(declared.read_text())
    registration['pipelines']['orders']['verify'][0]['env'] = {'PYTHONPATH': '{project}'}
    declared.write_text(json.dumps(registration))
    service = Service(path)
    try:
        sid = service.create_project('wfixture', root, 'create', 'codex')['id']
        started = service.start_project('wfixture', root, sid, 'request', 0, 'repair-fixture:success')
        saved, result = gate_done(service, started['id'], timeout=45)
        assert result['state'] == 'accepted' and result['assurance'] == 'enforced', (result, saved['result'])
        control = json.loads(saved['development_control'])
        assert control['repair_round'] == 1 and control['review_attempts'] == 1
        assert all(segment['execution_assurance'] == 'enforced' for segment in control['segments'])
        assert len(calls) == 3  # two confined developer segments, one no-tools review
        assert len((service.root / sid / 'provider/provider-calls.jsonl').read_text().splitlines()) == 2
        record = json.loads(task_file(service, sid).read_text())
        assert set(record['managed']['execution_evidence']) == {'developer', 'verifier'}
        assert record['managed']['bound_contract']['request']['contract']['require_enforcement'] is True
        service.close(); service = Service(path)
        assert service.show('wfixture', sid)['turn']['development']['assurance'] == 'enforced'
    finally: service.close()


@pytest.mark.parametrize('failure,reviews', [('context', 1), ('verifier', 1), ('review', 2)])
def test_restarted_accepted_session_repairs_new_context_and_accepts_again(managed_project, independent_reviewer, monkeypatch, tmp_path, failure, reviews):
    from test_forge_development_runtime import combined_configuration
    root, path, config = managed_project
    repair_provider(root, path, config)
    host = tmp_path/'host-partition'; host.mkdir()
    host_config, _, _, domain, _ = combined_configuration(host, root, monkeypatch)
    partition = json.loads(host_config.read_text())
    config.update({key: partition[key] for key in ('state_root', 'runtime_domain', 'host_budget')})
    domain.chmod(0o700)
    endpoint, calls = independent_reviewer
    monkeypatch.setenv('FIXTURE_REVIEW_KEY', 'offline-fixture-only')
    config['mode'] = 'restricted'
    config['limits']['turn_seconds'] = 45
    config['providers']['codex'].update(base_url=endpoint, credential_env='FIXTURE_REVIEW_KEY')
    path.write_text(json.dumps(config))
    service = Service(path)
    try:
        sid = service.create_project('wfixture', root, 'create', 'codex')['id']
        first = service.start_project('wfixture', root, sid, 'first', 0, 'repair-fixture:success')
        first_saved, accepted = gate_done(service, first['id'], timeout=45)
        assert accepted['state'] == 'accepted' and accepted['assurance'] == 'enforced', accepted
        original = json.loads(task_file(service, sid).read_text())
        first_history = first_saved['development']
        first_fingerprint = accepted['accepted_fingerprint']
        service.close(); service = Service(path)
        assert service.show('wfixture', sid)['turn']['development']['state'] == 'accepted'
        second = service.start_project('wfixture', root, sid, 'second', 1, 'repair-fixture:followup-'+failure)
        saved, result = gate_done(service, second['id'], timeout=45)
        assert result['state'] == 'accepted' and result['assurance'] == 'enforced', result
        control = json.loads(saved['development_control'])
        assert control['repair_round'] == 1 and control['review_attempts'] == reviews
        assert control['deadline'] == saved['created'] + 45
        assert len(control['segments']) == 2 and all(s['execution_assurance'] == 'enforced' for s in control['segments'])
        assert result['accepted_fingerprint'] == result['source_fingerprint'] != first_fingerprint
        assert result['gates'] == dict(check='passed', verify='passed', review='passed')
        updated = json.loads(task_file(service, sid).read_text())
        assert updated['baseline'] == original['baseline'] and updated['goal'] == original['goal']
        assert updated['managed']['bound_contract'] == original['managed']['bound_contract']
        assert len([note for note in updated['notes'] if note['kind'] == 'decision']) == 2
        if failure == 'context': assert any(note['kind'] == 'context-refresh' for note in updated['notes'])
        assert service.store.turn(first['id'])['development'] == first_history
        assert len(calls) == 5 + reviews  # Four actual confined segments; one original plus fresh follow-up reviews.
        review_inputs = [json.loads(call['input'].split('\n\n', 1)[1]) for call in calls if call['input'] != 'fixture development transport']
        assert len(review_inputs) == 1 + reviews and len({item['fingerprint'] for item in review_inputs}) == len(review_inputs)
        provider_calls = [json.loads(line) for line in (service.root/sid/'provider/provider-calls.jsonl').read_text().splitlines()]
        assert len(provider_calls) == 4 and len({call['submission'] for call in provider_calls}) == 4
        assert '不是新的真人授权' in provider_calls[-1]['message']
        with service.store.connect() as db:
            assert db.execute('SELECT count(*) FROM turns WHERE session=?', (sid,)).fetchone()[0] == 2
        service.close(); service = Service(path)
        assert service.show('wfixture', sid)['turn']['development']['state'] == 'accepted'
    finally: service.close()


def test_service_restart_resumes_only_drained_pending_repair(managed_project, monkeypatch):
    root, path, config = managed_project
    repair_provider(root, path, config)
    service = Service(path)
    try:
        monkeypatch.setattr(service.store, 'begin_repair', lambda _: None)
        sid = service.create_project('wfixture', root, 'create', 'codex')['id']
        started = service.start_project('wfixture', root, sid, 'request', 0, 'repair-fixture:success')
        until = time.monotonic()+15
        while time.monotonic() < until:
            with service.lock:
                service._reap()
                saved = service.store.turn(started['id'])
                if (saved['gate_phase'] == 'repair_pending' and saved['gate_cleanup'] == 'complete'
                        and not service.workers and not service.gate_workers): break
            time.sleep(.03)
        assert saved['gate_phase'] == 'repair_pending' and saved['gate_cleanup'] == 'complete', saved['result']
        # Simulate loss after the durable outbox and confirmed process drain,
        # without graceful shutdown's intentional user cancellation.
        with service.lock:
            service._reap()
            assert not service.workers and not service.gate_workers
            service.closed = True
        service.gate_wakeup.set(); service.gate_thread.join(timeout=3)
        os.close(service.owner)
        service = Service(path)
        resumed, development = gate_done(service, started['id'])
        assert development['state'] == 'review_unavailable', development
        control = json.loads(resumed['development_control'])
        assert control['repair_round'] == 1 and len(control['segments']) == 2
        assert control['deadline'] == json.loads(saved['development_control'])['deadline']
        assert len((service.root / sid / 'provider-calls.jsonl').read_text().splitlines()) == 2
    finally: service.close()


def test_cancel_running_repair_drains_without_starting_another_segment(managed_project):
    root, path, config = managed_project
    repair_provider(root, path, config)
    service = Service(path)
    try:
        sid = service.create_project('wfixture', root, 'create', 'codex')['id']
        started = service.start_project('wfixture', root, sid, 'request', 0, 'repair-fixture:hold')
        until = time.monotonic()+15
        while time.monotonic() < until:
            saved = service.store.turn(started['id'])
            if json.loads(saved['development_control'])['repair_round'] == 1 and saved['dispatch'] == 'unknown': break
            time.sleep(.03)
        assert json.loads(saved['development_control'])['repair_round'] == 1
        service.cancel('wfixture', root, sid, started['id'], 'stop')
        saved, development = gate_done(service, started['id'])
        assert development['state'] == 'cancelled' and not development['busy']
        assert saved['cleanup'] == saved['gate_cleanup'] == 'complete'
        assert json.loads(saved['development_control'])['repair_round'] == 1
        service.close(); service = Service(path)
        assert service.store.turn(started['id'])['epoch'] == saved['epoch']
        assert len((service.root / sid / 'provider-calls.jsonl').read_text().splitlines()) <= 2
    finally: service.close()


def test_uncertain_verifier_timeout_never_starts_an_automatic_replay(managed_project, tmp_path):
    root, path, config = managed_project
    repair_provider(root, path, config)
    marker = tmp_path / 'verifier-invocations'
    registration_path = root / '.demiforge/project.json'
    registration = json.loads(registration_path.read_text())
    registration['pipelines']['orders']['verify'] = [dict(id='uncertain', timeout=1, env={}, argv=[sys.executable, '-c',
        'from pathlib import Path; import time; p=Path('+repr(str(marker))+'); p.write_text((p.read_text() if p.exists() else "")+"called\\n"); time.sleep(5)'])]
    registration_path.write_text(json.dumps(registration))
    service = Service(path)
    try:
        sid = service.create_project('wfixture', root, 'create', 'codex')['id']
        started = service.start_project('wfixture', root, sid, 'request', 0, 'repair-fixture:success')
        saved, value = gate_done(service, started['id'])
        assert value['state'] == 'unknown' and 'not be automatically replayed' in value['reason']
        assert json.loads(saved['development_control'])['repair_round'] == 0
        assert marker.read_text() == 'called\n'
        service.close(); service = Service(path)
        time.sleep(.2)
        assert marker.read_text() == 'called\n' and not service.workers and not service.gate_workers
    finally: service.close()


@pytest.mark.parametrize('blocking', ['cancel', 'deadline', 'unproven_cleanup'])
def test_acceptance_requires_drain_and_serialized_uncancelled_budget(tmp_path, blocking):
    root, store, session, turn = pending_repair(tmp_path)
    store.update(turn['id'], gate_phase='ready', gate_cleanup='pending', delivery='acknowledged', dispatch='acknowledged')
    if blocking == 'cancel': store.cancel('w', session['id'], turn['id'])
    if blocking == 'deadline':
        control = json.loads(store.turn(turn['id'])['development_control']); control['deadline'] = time.time()-1
        store.update(turn['id'], development_control=wire(control).decode())
    calls = []
    def accept(value):
        calls.append(value)
        return dict(value, state='accepted', busy=False)
    if blocking == 'unproven_cleanup':
        with pytest.raises(ForgeError, match='confirmed owned-process cleanup'):
            store.accept_drained_gate(turn['id'], accept, drained=False)
        assert store.turn(turn['id'])['gate_cleanup'] == 'pending'
    else:
        result = store.accept_drained_gate(turn['id'], accept, drained=True)
        assert result['state'] == ('cancelled' if blocking == 'cancel' else 'unknown')
        assert store.turn(turn['id'])['gate_cleanup'] == 'complete'
    assert calls == []


def test_gate_operation_only_publishes_ready_before_supervisor_acceptance(managed_project, monkeypatch):
    from demiflow.workbench.conversations import development
    root, path, _ = managed_project
    service = Service(path)
    service.gate_wakeup.set(); service.gate_thread.join(timeout=2)
    calls = []
    value = dict(task_id='fixture', source_fingerprint='candidate', accepted_fingerprint=None,
        ready=True, development_state='incomplete', gates=dict(check='passed', verify='passed', review='passed'), changed_total=1)
    class Tasks:
        def status(self, *_): return value
        def check(self, *_, **__): return value
        def verify(self, *_, **__): return value
        def evaluate_acceptance(self, *_, **__): calls.append(True); return value
    monkeypatch.setattr(development, 'project_tasks', lambda *_: Tasks())
    try:
        sid = service.create_project('wfixture', root, 'create', 'codex')['id']
        turn, _ = service.store.submit('wfixture', sid, 'request', 0, 'Original user intent',
            root_identity(root), service.policy_hash, development=initial(sid))
        service.store.update(turn['id'], state='completed', cleanup='complete', gate_cleanup='pending',
            dispatch='acknowledged', delivery='acknowledged', gate_phase='claimed')
        development.execute(service.config_path, turn['id'])
        saved = service.store.turn(turn['id'])
        assert saved['gate_phase'] == 'ready' and saved['gate_cleanup'] == 'pending'
        assert json.loads(saved['development'])['busy'] and calls == []
        service.store.cancel('wfixture', sid, turn['id'])
        service.store.accept_drained_gate(turn['id'], lambda _: development.projection(Tasks().evaluate_acceptance()), drained=True)
        assert calls == []
    finally: service.close()


def test_registered_external_readonly_and_fixed_script_verifier_are_scoped(managed_project, tmp_path):
    from demiflow.workbench.conversations.development import verification_launcher
    from demiflow.workbench.conversations.policy import Deployment
    root, path, _ = managed_project
    external = root.parent/'platform'
    (external/'api.py').write_text('VALUE="registered readonly dependency"\n')
    entry = root/'orders/tests/registered-verifier'
    entry.write_text('#!'+sys.executable+'\nimport api;print(api.VALUE)\n'); entry.chmod(0o700)
    scratch = tmp_path/'verifier-home'; scratch.mkdir(mode=0o700)
    deployment = Deployment.load(path)
    launch = verification_launcher(deployment, dict(workspace='wfixture', id='fixture', provider='codex'), deployment.projects['wfixture'])
    process = launch([str(entry)], cwd=root, scratch=scratch, timeout=5,
        env=dict(PATH='/usr/bin:/bin', PYTHONPATH=str(external), PYTHONDONTWRITEBYTECODE='1'))
    output, error = process.communicate(timeout=8)
    assert process.returncode == 0 and output == b'registered readonly dependency\n', error
    assert process.execution_assurance == 'enforced'


def test_cancelled_provider_cannot_obtain_late_automatic_file_approval(managed_project):
    root, path, config = managed_project
    repair_provider(root, path, config)
    service = Service(path)
    try:
        sid = service.create_project('wfixture', root, 'create', 'codex')['id']
        started = service.start_project('wfixture', root, sid, 'request', 0, 'repair-fixture:cancel-approval')
        until = time.monotonic()+10
        while service.store.turn(started['id'])['state'] != 'running' and time.monotonic() < until: time.sleep(.03)
        service.cancel('wfixture', root, sid, started['id'], 'stop')
        _, development = gate_done(service, started['id'])
        assert development['state'] == 'cancelled'
        assert (service.root/sid/'late-approval-issued').is_file()
        assert not (service.root/sid/'late-approval-reply.json').exists()
        assert '# forbidden after cancellation' not in (root/'orders/operators/map.py').read_text()
        assert not any(event['type'] == 'approval.automatic' for event in service.store.events('wfixture', sid)['events'])
    finally: service.close()


def test_direct_restricted_legacy_worker_never_executes_project_launcher(managed_project, tmp_path):
    from test_forge_development_chat import wait_turn
    root, path, config = managed_project
    config['mode'] = 'restricted'
    config['projects']['wfixture']['task_id'] = 'test'
    path.write_text(json.dumps(config))
    marker = tmp_path/'legacy-launcher-executed'
    (root/'.demiforge/forge').write_text('from pathlib import Path\nPath('+repr(str(marker))+').write_text("unsafe")\n')
    service = Service(path)
    try:
        sid = service.create_project('wfixture', root, 'create', 'codex')['id']
        turn, _ = service.store.submit('wfixture', sid, 'direct', 0, 'Bypass the normal capability UI',
            root_identity(root), service.policy_hash)
        with service.lock: service._launch_provider(turn, service.store.session(sid))
        saved = wait_turn(service, turn['id'])
        assert saved['state'] == 'failed'
        assert 'controller-managed' in json.loads(saved['result'])['reason']
        assert not marker.exists()
    finally: service.close()
