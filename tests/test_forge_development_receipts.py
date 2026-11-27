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


def test_project_root_write_policy_requires_no_synthetic_pyproject_placeholder(tmp_path):
    from demiflow.forge.sandbox import build_development_sandbox_spec

    project = tmp_path / 'empty_project'
    project.mkdir()
    home = tmp_path / 'home'
    home.mkdir(mode=0o700)
    spec = build_development_sandbox_spec(project, private_home=home, writes=())
    assert spec.write_policy == 'project_root'
    assert spec.write_paths == (str(project.resolve()),)
    assert not (project / 'pyproject.toml').exists()
    assert str((project / '.forge').resolve()) in spec.protected_paths
    assert str((project / 'packages').resolve()) in spec.protected_paths
    assert str((project / 'data_tools').resolve()) in spec.protected_paths


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
