"""Host transactions preserve engineering evidence and delivery provenance."""
import json
import fcntl
import os

import pytest

from demiflow.forge.development import assess_turn
from demiflow.workbench.conversations.common import root_identity
from test_forge_development_chat_http import app
from test_forge_development_chat import wait_turn
from demiflow.forge.schema import ForgeError


def start(store, service, workspace, root, sid, revision=0, text='Keep the original intent', options=None):
    return store.submit(workspace, sid, 'submit-'+str(revision), revision, text, root_identity(root),
                        service.policy_hash, forge_options=options)[0]


@pytest.mark.parametrize('existing', [False, True])
def test_readonly_planning_then_edit_in_same_restricted_conversation(tmp_path, monkeypatch, existing):
    from demiflow.workbench.api import WorkspaceServer
    from test_forge_development_runtime import combined_configuration

    root, host = tmp_path / 'orders', tmp_path / 'host'
    root.mkdir()
    host.mkdir()
    path, registry, workspace, domain, _ = combined_configuration(host, root, monkeypatch)
    config = json.loads(path.read_text())
    config['mode'] = 'restricted'
    config['projects'][workspace].update(kind='pipeline', phase='implementation', watch=['notes.py', 'pyproject.toml'])
    domain.chmod(0o700)
    monkeypatch.setenv('PLANNING_FIXTURE_KEY', 'offline-fixture-only')
    config['providers']['codex'].update(base_url='http://127.0.0.1:1/v1', credential_env='PLANNING_FIXTURE_KEY')
    path.write_text(json.dumps(config))
    declaration = root / 'pyproject.toml'
    original = '[project]\nname = "initial"\n'
    if existing:
        declaration.write_text(original)
    server = WorkspaceServer(registry=registry, development_config=path)
    try:
        service = server.development
        if not existing:
            assert service.prewarm(workspace, root, provider_id='codex')['status'] == 'skipped'
        sid = service.create_project(workspace, root, 'create', 'codex')['id']
        assert service.capabilities(workspace, root)['start'], service.capabilities(workspace, root)
        plan = service.start_project(workspace, root, sid, 'plan', 0, 'Plan the migration.', auto_review=True)
        saved = wait_turn(service, plan['id'])
        assert saved['state'] == 'completed', saved['result']
        assert json.loads(saved['result'])['changed_files'] == 0
        events = service.store.events(workspace, sid)['events']
        assert not {event['type'] for event in events} & {'change.observed', 'review.requested', 'review.started'}
        assert (declaration.read_text() if declaration.exists() else None) == (original if existing else None)
        edit = service.start_project(workspace, root, sid, 'edit', 1, 'write project declaration fixture')
        saved = wait_turn(service, edit['id'])
        assert saved['state'] == 'completed', saved['result']
        assert (root / 'pyproject.toml').read_text() == '[project]\nname = "demo"\n'
        events = service.store.events(workspace, sid, events[-1]['sequence'], 50)['events']
        changes = [item for e in events if e['turn_id'] == edit['id'] and e['type'] == 'change.observed'
                   for item in e['data']['files']]
        assert [item['path'] for item in changes] == ['pyproject.toml']
        if existing:
            assert changes[0]['before']['bytes'] == len(original)
            assert any(e['data'].get('warm_reused') for e in events if e['type'] == 'provider.bound')
        else:
            assert changes[0]['before'] is None
    finally:
        server.server_close()


def test_author_cleanup_preserves_empty_declarations_owned_or_saved_by_others(tmp_path):
    from demiflow.forge.sandbox import writable_project_declaration

    declaration = tmp_path / 'pyproject.toml'
    declaration.touch()
    with writable_project_declaration(tmp_path):
        pass
    assert declaration.exists()
    declaration.unlink()
    with writable_project_declaration(tmp_path):
        replacement = tmp_path / 'replacement'
        replacement.touch()
        replacement.replace(declaration)
    assert declaration.exists()
    declaration.unlink()
    with writable_project_declaration(tmp_path):
        declaration.write_text('temporary content')
        declaration.write_text('')
    assert declaration.read_bytes() == b''


def test_large_goal_and_diagnostics_commit_without_losing_terminal(tmp_path):
    with app(tmp_path) as (server, workspace, root):
        service = server.development; store = service.store
        sid = service.create_project(workspace, root, 'new', 'codex')['id']
        turn = start(store, service, workspace, root, sid)
        goal = '完整目标' * 6000
        diagnostics = [dict(rule='FL-02', path='flows/a.py', detail='error ' + str(i) + 'x'*180) for i in range(80)]
        checks = dict(native_flow=dict(status='failed', diagnostics=diagnostics, diagnostic_count=80))
        report = dict(status='passed', goal=goal, scope=dict(mode='observed_changes'), findings=[], checks=[], gaps=[])
        store.finish_provider(turn['id'], state='completed', cleanup='complete', result=dict(pipeline_checks=checks), review=report)
        done = store.turn(turn['id']); result = json.loads(done['result'])
        assert done['state'] == 'completed' and done['cleanup'] == 'complete'
        assert result['pipeline_review'] == 'passed'
        assert result['pipeline_checks']['native_flow']['diagnostic_count'] == 80
        events = []
        while True:
            page = store.events(workspace, sid, after=events[-1]['sequence'] if events else 0)
            events.extend(page['events'])
            if page['next_sequence'] == page['last_sequence']:
                break
        assert sum(e['type'] == 'check.detail' for e in events) == 80
        fragments = [e['data']['json_part'] for e in events if e['type'] == 'review.material']
        assert json.loads(''.join(fragments)) == goal


def test_deferred_feedback_is_recorded_then_superseded_by_new_user_request(tmp_path):
    with app(tmp_path) as (server, workspace, root):
        service = server.development; store = service.store
        sid = service.create_project(workspace, root, 'new', 'codex')['id']
        turn = start(store, service, workspace, root, sid)
        store.enqueue(workspace, sid, turn['id'], 'human-next', 'New user requirement',
                      forge_options=dict(origin='user', auto_review=True))
        receipt = assess_turn(root, author=dict(provider='codex', session='native', completed=True,
            acknowledged=True, cleanup='complete'), goal='goal', inspect=False,
            protected_violations=['workspace.json'], submission=turn['id'])
        store.finish_provider(turn['id'], state='completed', cleanup='complete', result={}, forge=receipt)
        events = store.events(workspace, sid)['events']
        feedback = next(e['data'] for e in events if e['type'] == 'forge.feedback')
        assert feedback['status'] == 'deferred'
        assert len(store.show(workspace, sid)['queued_messages']) == 1
        store.submit(workspace, sid, 'human-next', 1, 'New user requirement', root_identity(root),
                     service.policy_hash, forge_options=dict(origin='user', auto_review=True))
        statuses = [e['data'] for e in store.events(workspace, sid)['events'] if e['type'] == 'forge.feedback_status']
        assert statuses == [dict(id=feedback['id'], origin_turn=turn['id'], status='superseded')]
        assert 'Forge 自动返工' not in store.review_inputs(sid, include_paths=False)[0]


def test_old_observation_failure_does_not_poison_new_delivery(tmp_path):
    with app(tmp_path) as (server, workspace, root):
        service = server.development; store = service.store
        sid = service.create_project(workspace, root, 'new', 'codex')['id']
        first = start(store, service, workspace, root, sid)
        store.finish_provider(first['id'], state='failed', cleanup='complete', result=dict(reason='Change observation incomplete: old'))
        boundary = start(store, service, workspace, root, sid, 1, 'Close the delivery')
        store.finish_provider(boundary['id'], state='completed', cleanup='complete', result={},
            review=dict(status='passed', scope=dict(mode='observed_changes')))
        start(store, service, workspace, root, sid, 2, 'Continue with new work')
        goal, sources = store.review_inputs(sid, include_paths=True)
        assert goal.startswith('Keep the original intent') and goal.endswith('Continue with new work')
        assert sources == {}


def test_removing_queued_feedback_records_cancellation_once(tmp_path):
    with app(tmp_path) as (server, workspace, root):
        service = server.development; store = service.store
        sid = service.create_project(workspace, root, 'new', 'codex')['id']
        turn = start(store, service, workspace, root, sid)
        receipt = assess_turn(root, author=dict(provider='codex', session='native', completed=True,
            acknowledged=True, cleanup='complete'), goal='goal', inspect=False,
            protected_violations=['workspace.json'], submission=turn['id'])
        store.finish_provider(turn['id'], state='completed', cleanup='complete', result={}, forge=receipt)
        queued = store.show(workspace, sid)['queued_messages']
        assert len(queued) == 1
        feedback = receipt['feedback']['id']
        original = store.events(workspace, sid)['events']
        store.change_queue(workspace, sid, feedback, 'remove')
        store.change_queue(workspace, sid, feedback, 'remove')
        assert not store.show(workspace, sid)['queued_messages']
        events = store.events(workspace, sid)['events']
        assert events[:len(original)] == original
        assert [(e['type'], e['data']) for e in events[len(original):]] == [
            ('forge.feedback_status', dict(id=feedback, status='cancelled'))]


def test_new_turn_recovers_committed_but_unretired_writer(tmp_path, monkeypatch):
    from demiflow.forge.development_state import claim_writer, start_writer, retire_writer
    with app(tmp_path) as (server, workspace, root), (tmp_path/'old.unit').open('wb') as unit:
        service = server.development; store = service.store
        sid = service.create_project(workspace, root, 'new', 'codex')['id']
        turn = start(store, service, workspace, root, sid)
        fcntl.flock(unit, fcntl.LOCK_SH)
        info = os.fstat(unit.fileno())
        key = claim_writer(root, 'retirement-fixture-'+sid, unit.name, dict(device=info.st_dev, inode=info.st_ino))
        start_writer(key)
        try:
            store.update(turn['id'], process=json.dumps(dict(forge_writer=key)))
            # Simulate the post-commit crash/lock-contention window. A reader can
            # already see completed, while the common claim still exists.
            with monkeypatch.context() as patch:
                patch.setattr(store, 'retire_writer', lambda tid: False)
                store.finish_provider(turn['id'], state='completed', cleanup='complete', result={})
            next_turn = service.start(workspace, root, sid, 'next', 1, 'Continue after the committed completion')
            assert wait_turn(service, next_turn['id'])['state'] == 'completed'
        finally:
            retire_writer(key, terminal=True, cleanup='complete')


@pytest.mark.parametrize('state,cleanup', [('completed','unknown'), ('running','complete')])
def test_writer_recovery_requires_both_terminal_and_cleanup(tmp_path, state, cleanup):
    from demiflow.forge.development_state import claim_writer, retire_writer
    with app(tmp_path) as (server, workspace, root), (tmp_path/'unknown.unit').open('wb') as unit:
        service = server.development; store = service.store
        sid = service.create_project(workspace, root, 'new', 'codex')['id']
        turn = start(store, service, workspace, root, sid)
        info = os.fstat(unit.fileno())
        key = claim_writer(root, 'unproven-'+sid, unit.name, dict(device=info.st_dev, inode=info.st_ino))
        try:
            store.update(turn['id'], process=json.dumps(dict(forge_writer=key)), state=state, cleanup=cleanup)
            store.retire_completed_writers(root)
            with pytest.raises(ForgeError, match='active or unresolved'):
                claim_writer(root, 'another-'+sid, unit.name, dict(device=info.st_dev, inode=info.st_ino))
        finally:
            retire_writer(key, terminal=True, cleanup='complete')
