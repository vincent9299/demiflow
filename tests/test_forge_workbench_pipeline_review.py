"""Review delivery through the normal author channel, durable session and chat projection."""
import hashlib
import json
import os
import sys
import time
from types import SimpleNamespace

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
        first = service.start_project(workspace, root, session, 'first', 0, 'Describe the flow')
        assert wait_turn(service, first['id'])['state'] == 'completed'
        until = time.monotonic() + 5
        while time.monotonic()<until and not service.warm_workers[session].idle:
            time.sleep(.05)
        warm = service.warm_workers[session]
        assert warm.idle
        # Exercise an idle period longer than the control bridge's accept timeout.
        time.sleep(.3)
        turn = service.start_project(workspace, root, session, 'submit', 1, 'request delivery review fixture')
        done = wait_turn(service, turn['id'])
        assert done['state'] == 'completed' and done['cleanup'] == 'complete', done['result']
        result = json.loads(done['result'])
        assert 'pipeline_checks' in result and result['pipeline_review'] == 'incomplete', result
        replay = service.store.replay(workspace, session)['events']
        assert any(ev['type']=='provider.bound' and ev['data'].get('warm_reused')
                   for ev in service.store.events(workspace,session)['events'] if ev['turn_id']==turn['id'])
        events = [ev for ev in replay if ev['type'].startswith('review.')]
        assert all(ev['turn_id']==turn['id'] for ev in events)
        assert [ev['type'] for ev in events].count('review.requested') == 1
        assert next(ev for ev in events if ev['type']=='review.started')['data']['request_id'] == turn['id']
        assert events[-1]['type'] == 'review.completed'
        assert events[-1]['data']['scope']['mode'] == 'observed_changes'
        assert events[-1]['data']['scope']['changed'] == ['flows/fixture.py']
        assert events[-1]['data']['goal'] == 'Describe the flow\n\nrequest delivery review fixture'
        assert warm.process.wait(timeout=5)==0
        # Free static checks fail on the synthetic fixture, triggering one bounded rework round
        # without re-running the paid independent LLM review.
        assert result.get('auto_rework', {}).get('scheduled') is True, result
        until = time.monotonic() + 20
        while time.monotonic() < until:
            current = service.store.show(workspace, session)
            rework = current.get('turn')
            if (current['revision'] >= 3 and rework and rework['id'] != turn['id']
                    and rework['state'] == 'completed' and rework['cleanup'] == 'complete'
                    and not current.get('queued_messages')):
                break
            time.sleep(.05)
        current = service.store.show(workspace, session)
        assert current['revision'] == 3 and current['turn']['state'] == 'completed' and current['turn']['cleanup'] == 'complete', current
        follow = service.start_project(workspace, root, session, 'follow', current['revision'], 'Explain the previous review')
        later = wait_turn(service,follow['id'])
        assert later['state']=='completed'
        assert 'forge_review_result' in later['context']
        all_events = service.store.replay(workspace,session)['events']
        assert sum(ev['type']=='review.completed' for ev in all_events)==1
        assert any(ev['type']=='history.gap' for ev in all_events if ev['turn_id'] not in {first['id'], turn['id']})
        assert '凭证' in str(recent_review(service.store, session))
        other = service.create_project(workspace, root, 'another', 'deepseek', new_conversation=True)['id']
        assert recent_review(service.store, other) is None


def test_admission_failure_closes_broker_and_preserves_completed_turn_checks(tmp_path, monkeypatch):
    from demiflow.workbench.conversations import execution, model_broker
    from demiflow.workbench.conversations.pipeline_review import DeliveryReview, run_deployment_reviewer
    from demiflow.workbench.conversations.sandbox import SandboxSpec

    with app(tmp_path, acp=True, guided=True) as (server, workspace, root):
        service = server.development
        session = service.create_project(workspace, root, 'new', 'deepseek')['id']
        store = service.store
        turn, _ = store.submit(workspace, session, 'r', 0, 'Inspect entry', root_identity(root), service.policy_hash)
        (root/'flows/entry.py').write_text('def run(): return 1')
        review = DeliveryReview(store, turn, {'root':root})
        review.request({'entry':'flows/entry.py:run'})
        closed = []
        broker = SimpleNamespace(socket_path='/unused', child_config={'model':'fake'}, close=lambda:closed.append(True))
        monkeypatch.setattr(model_broker, 'start_model_broker', lambda *a, **k:broker)
        def deny(**kwargs):
            assert kwargs['sandbox_spec'].role == 'readonly'
            raise RuntimeError('Sandbox admission timed out')
        monkeypatch.setattr(execution,'launch_provider',deny)
        deployment = SimpleNamespace(mode='restricted',providers={'deepseek':dict(model_provider='deepseek-account')})
        spec = SandboxSpec((),(),(),(),str(tmp_path/'author'))
        def invoke(prompt):
            return run_deployment_reviewer(prompt, deployment=deployment, turn=turn, session={'provider':'deepseek'},
                scratch=tmp_path/'scratch', broker_home=tmp_path/'model', author_spec=spec,lease_fd=3,
                deadline=time.time()+5,check_cancel=lambda:None)
        checks = {'native_flow':{'status':'passed','diagnostic_count':0}}
        report = review.run(invoke,checks,lambda:None)
        assert closed == [True] and 'admission timed out' in str(report['diagnostics'])
        store.finish_provider(turn['id'], state='completed', cleanup='complete', result={'pipeline_checks':checks},
                              review=report)
        done=store.turn(turn['id'])
        assert done['state']=='completed' and done['cleanup']=='complete'
        assert json.loads(done['result'])==dict(pipeline_review='incomplete',pipeline_checks=checks)


def test_trusted_reviewer_inherits_turn_lease_and_parent_death_signal(tmp_path, monkeypatch):
    from demiflow.forge import review_deepseek
    from demiflow.workbench.conversations.execution import UNIT_ENV
    from demiflow.workbench.conversations.pipeline_review import run_deployment_reviewer

    script = '''
import ctypes,json,os,sys
sys.stdin.read()
sig=ctypes.c_int()
assert ctypes.CDLL(None).prctl(2,ctypes.byref(sig),0,0,0)==0
for row in [
    dict(type='session',sessionId='session-12345678-0000-1111-2222-abcdef123456',cwd=os.getcwd()),
    dict(type='status',phase='turn_end',reason=dict(kind='completed')),
    dict(type='final',text=json.dumps(dict(inode=os.fstat(int(os.environ[sys.argv[1]])).st_ino, sig=sig.value)))
]:
    print(json.dumps(row),flush=True)
'''
    def launch(config, home, *, model_channel):
        assert model_channel is None
        return [sys.executable,'-c',script,UNIT_ENV], dict(HOME=str(home))
    monkeypatch.setattr(review_deepseek, 'prepare_launch', launch)
    deployment = SimpleNamespace(mode='trusted_local', providers={'deepseek':{'model':'fixture',
                                     'model_provider':'deepseek-account'}})
    scratch = tmp_path/'turn'; scratch.mkdir()
    with (tmp_path/'lease').open('wb') as lease:
        result = run_deployment_reviewer('test', deployment=deployment, session={'provider':'deepseek'}, turn={},
            scratch=scratch, broker_home=None, author_spec=None, lease_fd=lease.fileno(),
            deadline=time.time()+10, check_cancel=lambda:None)
        assert result['report'] == dict(inode=os.fstat(lease.fileno()).st_ino, sig=9)


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


def test_forge_auto_review_and_rework_helpers():
    from demiflow.workbench.conversations.worker import _has_code_changes, _build_rework_prompt
    assert _has_code_changes([{'path': 'flows/main.py'}]) is True
    assert _has_code_changes([{'path': 'deployment.json'}]) is True
    assert _has_code_changes([{'path': 'parameters/default.json'}]) is True
    assert _has_code_changes([{'path': 'README.md'}]) is False
    assert _has_code_changes([{'path': 'docs/notes.md'}]) is False
    # Passed checks & suggestion-only review produce no automatic rework.
    assert _build_rework_prompt(
        {'native_flow': {'status': 'passed'}, 'project_layout': {'status': 'passed'}},
        {'status': 'passed', 'findings': [{'severity': 'suggestion', 'path': 'flows/a.py', 'line': 1, 'impact': 'style'}]},
    ) is None
    # Hard static failure or blocking review finding produces structured rework prompt.
    prompt = _build_rework_prompt(
        {'native_flow': {'status': 'failed', 'diagnostics': [{'rule': 'FL-03', 'path': 'flows/a.py', 'line': 12, 'detail': 'bad arg'}]}},
        {'status': 'invalid', 'findings': [{'severity': 'blocking', 'path': 'operators/b.py', 'line': 8,
                                            'trigger': 'empty input', 'impact': 'crashes', 'suggestion': 'guard None'}]},
    )
    assert prompt is not None
    assert '[Forge 自动返工 1/1]' in prompt
    assert 'FL-03' in prompt and 'flows/a.py:12' in prompt
    assert 'operators/b.py:8' in prompt and 'guard None' in prompt


def test_forge_auto_rework_deduplicates_and_respects_python_config(tmp_path):
    from demiflow.workbench.conversations.worker import _build_rework_prompt
    proj = tmp_path / 'proj_no_py'
    proj.mkdir()
    (proj / 'workspace.json').write_text('{"entries": {}}\n')

    # Repeated diagnostics across flows must collapse to one line with count,
    # allowing later distinct diagnostics to surface within the 6-item cap.
    repeated = [
        {'rule': 'FL-02', 'path': 'operators/bindings.py', 'line': 33, 'detail': 'bad call'},
        {'rule': 'FL-02', 'path': 'operators/bindings.py', 'line': 33, 'detail': 'bad call'},
        {'rule': 'FL-02', 'path': 'operators/bindings.py', 'line': 33, 'detail': 'bad call'},
        {'rule': 'FL-02', 'path': 'operators/bindings.py', 'line': 33, 'detail': 'bad call'},
        {'rule': 'FL-07', 'path': 'operators/bindings.py', 'line': 47, 'detail': 'With'},
        {'rule': 'FL-07', 'path': 'operators/bindings.py', 'line': 47, 'detail': 'With'},
        {'rule': 'FL-07', 'path': 'operators/other.py', 'line': 9, 'detail': 'distinct issue'},
    ]
    prompt = _build_rework_prompt(
        {'native_flow': {'status': 'failed', 'diagnostics': repeated}},
        None,
        project_root=proj,
    )
    assert prompt.count('operators/bindings.py:33') == 1
    assert '（共 4 处）' in prompt
    assert '（共 2 处）' in prompt
    assert 'operators/other.py:9' in prompt
    assert '未配置业务 Python' in prompt

    # With a configured business Python, prompt asks to run project tests.
    (proj / 'workspace.json').write_text('{"entries": {}, "python": "/opt/venv/bin/python"}\n')
    prompt_with_py = _build_rework_prompt(
        {'native_flow': {'status': 'failed', 'diagnostics': repeated[:1]}},
        None,
        project_root=proj,
    )
    assert '项目测试验证' in prompt_with_py
    assert '未配置业务 Python' not in prompt_with_py

    # Slimmed workspace.json without "python" still honors workbench-configured project_python.
    (proj / 'workspace.json').write_text('{"entries": {}}\n')
    prompt_with_registry_py = _build_rework_prompt(
        {'native_flow': {'status': 'failed', 'diagnostics': repeated[:1]}},
        None,
        project_root=proj,
        project_python='/opt/workbench/venv/bin/python',
    )
    assert '项目测试验证' in prompt_with_registry_py
    assert '未配置业务 Python' not in prompt_with_registry_py
