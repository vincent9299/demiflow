"""Review delivery through the normal author channel, durable session and chat projection."""
import hashlib
import json

import pytest

from demiflow.forge.schema import ForgeError
from demiflow.workbench.conversations.changes import capture, differences
from demiflow.workbench.conversations.common import root_identity
from demiflow.workbench.conversations.pipeline_review import recent_review
from test_forge_development_chat import wait_turn
from test_forge_development_chat_http import app
from test_forge_development_chat_presentation import event, presentation, push


def test_author_requests_one_independent_review_and_replays_unavailable_model(tmp_path):
    with app(tmp_path, acp=True, guided=True) as (server, workspace, root):
        service = server.development
        session = service.create_project(workspace, root, 'new', 'deepseek')['id']
        turn = service.start_project(workspace, root, session, 'submit', 0, 'request delivery review fixture')
        done = wait_turn(service, turn['id'])
        assert done['state'] == 'completed' and done['cleanup'] == 'complete', done['result']
        result = json.loads(done['result'])
        assert 'pipeline_checks' in result and result['pipeline_review'] == 'incomplete', result
        replay = service.store.replay(workspace, session)['events']
        events = [ev for ev in replay if ev['type'].startswith('review.')]
        assert [ev['type'] for ev in events].count('review.requested') == 1
        assert next(ev for ev in events if ev['type']=='review.started')['data']['request_id'] == turn['id']
        assert events[-1]['type'] == 'review.completed'
        assert events[-1]['data']['scope']['mode'] == 'observed_changes'
        assert events[-1]['data']['scope']['changed'] == ['flows/fixture.py']
        assert events[-1]['data']['goal'] == 'request delivery review fixture'
        assert '凭证' in str(recent_review(service.store, session))
        other = service.create_project(workspace, root, 'another', 'deepseek', new_conversation=True)['id']
        assert recent_review(service.store, other) is None


def test_real_observations_across_turns_and_only_whole_delivery_closes_scope(tmp_path):
    with app(tmp_path, guided=True) as (server, workspace, root):
        service, name = server.development, 'flows/one.py'
        store = service.store
        session = service.create_project(workspace, root, 'new', 'codex')['id']
        def begin(revision, goal):
            turn, _ = store.submit(workspace, session, 'seq-'+str(revision), revision, goal,
                                   root_identity(root), service.policy_hash)
            return turn['id']
        def edit(tid, value):
            baseline = capture(root, ['flows'])
            store.update(tid, baseline=json.dumps(baseline))
            if value is None:
                (root/name).unlink()
            else:
                (root/name).write_text(value)
            for change in differences(baseline, capture(root, ['flows']), ['flows']):
                store.event(tid, 'change.observed', dict(files=[change]))
        def finish(tid, status, mode):
            store.finish_provider(tid, state='completed', cleanup='complete', result={},
                                  review=dict(status=status, scope=dict(mode=mode)))
        tid = begin(0, 'Add a flow')
        edit(tid, 'def run(): return 1')
        finish(tid, 'invalid', 'observed_changes')
        tid = begin(1, 'Change output')
        edit(tid, 'def run(): return 2')
        finish(tid, 'passed', 'entry')
        two = hashlib.sha256(b'def run(): return 2').hexdigest()
        assert store.review_inputs(session, include_paths=True) == ('Add a flow\n\nChange output',
                                                                      {name:dict(before=None,after_sha256=two)})
        tid = begin(2, 'Deliver')
        finish(tid, 'passed', 'observed_changes')
        tid = begin(3, 'Remove old flow')
        edit(tid, None)
        assert store.review_inputs(session, include_paths=True) == ('Remove old flow',
                                                                      {name:dict(before='def run(): return 2',after_sha256=None)})
        finish(tid, 'incomplete', 'observed_changes')
        tid = begin(4, 'a'*8192)
        finish(tid, 'incomplete', 'observed_changes')
        begin(5, 'Review selected entry now')
        with pytest.raises(ForgeError, match='交付目标'):
            store.review_inputs(session, include_paths=True)
        assert store.review_inputs(session, include_paths=False) == ('Review selected entry now', {})


@pytest.mark.parametrize('cancel',[False, True])
def test_terminal_receipt_preserves_unfinished_review_under_budget_or_cancel(tmp_path, cancel):
    with app(tmp_path) as (server, workspace, root):
        service = server.development
        store = service.store
        session = service.create_project(workspace, root, 'new', 'codex')['id']
        turn,_ = store.submit(workspace, session, 'submit', 0, 'Deliver', root_identity(root), service.policy_hash)
        store.event(turn['id'], 'review.started', {'request_id':turn['id']})
        if cancel:
            store.cancel(workspace, session, turn['id'])
        store.finish_provider(turn['id'], state='completed', cleanup='complete', result={},
                              review=dict(status='passed', gaps=['x'*(store.limits.event_bytes+1)]))
        final = store.replay(workspace, session)['events'][-1]['data']
        assert final['state'] == ('cancelled' if cancel else 'completed')
        result = json.loads(final['result'])
        assert result['pipeline_review'] == 'incomplete' and result['review_reason']


def test_browser_restores_terminal_review_from_parent_receipt():
    with presentation() as (page,expect,errors):
        push(page,[event(1,'review.started',{}),
                   event(2,'turn.state',dict(state='interrupted',cleanup='unknown'))])
        expect(page.locator('.dfc-answers .dfc-summary')).to_have_text('未完成')
        push(page,[event(3,'turn.state',dict(state='completed',cleanup='complete',
                       result=dict(pipeline_review='invalid')),turn='truncated')])
        expect(page.locator('.dfc-answers .dfc-summary').last).to_have_text('需要修改')
        assert not errors

