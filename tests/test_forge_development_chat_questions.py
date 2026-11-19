"""Project requests and real clarification preserve the same bound history."""
import json
from pathlib import Path
import sys
import time
from types import SimpleNamespace

import pytest

from demiflow.forge.schema import ForgeError
from demiflow.workbench.conversations import development
from demiflow.workbench.conversations.context import ProtocolBridge
from demiflow.workbench.conversations.service import Service
from test_forge_development_chat_managed import managed_project, gate_done, task_file
from test_forge_development_chat_repair import pending_repair, independent_reviewer


@pytest.mark.parametrize('stage', ['status', 'check', 'verify'])
def test_pending_questions_stop_before_remaining_gates(tmp_path, monkeypatch, stage):
    _, store, session, turn = pending_repair(tmp_path)
    store.update(turn['id'], gate_phase='claimed', gate_cleanup='pending')
    called = []
    current = dict(task_id=session['id'], source_fingerprint='candidate', accepted_fingerprint='old',
        development_state='stale', changed_total=1, gates=dict(check='passed', verify='passed', review='passed'))
    def answer(name):
        called.append(name)
        return dict(current, **(dict(development_state='needs_input', pending_questions=[dict(note_index=1,text='Threshold?')],
            pending_question_count=1, ready=False, repairable=False) if name == stage else {}))
    class Tasks:
        def status(self, *_): return answer('status')
        def check(self, *_, **__): return answer('check')
        def verify(self, *_, **__): return answer('verify')
    monkeypatch.setattr(development, '_load', lambda *_: (SimpleNamespace(limits=store.limits, mode='trusted_local'), store,
        store.turn(turn['id']), session, dict(allow_tests=True)))
    monkeypatch.setattr(development, 'project_tasks', lambda *_: Tasks())
    development.execute('unused', turn['id'])
    saved = store.turn(turn['id']); result = json.loads(saved['development'])
    assert result['state'] == 'needs_input' and result['busy']  # supervisor still owns cleanup
    assert development.projection(answer(stage))['busy'] is False
    assert result['pending_question_count'] == 1 and saved['gate_phase'] == 'finished'
    assert called[:-1] == ['status', 'check', 'verify'][:['status','check','verify'].index(stage)+1]
    assert json.loads(saved['development_control'])['review_attempts'] == 0


def test_resolve_bridge_accepts_only_index_and_injects_real_message_references(tmp_path):
    root, store, session, turn = pending_repair(tmp_path)
    store.update(turn['id'], cleanup='pending')
    bridge = ProtocolBridge(store, turn['id'], {}, tmp_path)
    calls = []
    bridge.task_id = session['id']
    bridge.managed_tasks = SimpleNamespace(resolve_question=lambda *args, **kwargs: calls.append((args,kwargs)))
    bridge.invoke(dict(operation='resolve_question', arguments=dict(note_index=2)))
    assert calls == [((session['id'],2),dict(answer_event_ids=[turn['id']+':0']))]
    key=store.request(turn['id'],dict(rpc_id='approval',method='item/fileChange/requestApproval',params={},allow_once=True))
    pending=store.pending(turn['id'])[0]
    store.respond('w',session['id'],turn['id'],key,pending['digest'],'allow_once');store.resolved(key)
    assert development.question_answer_events(store,turn)==[turn['id']+':0']
    for extra in ({'answer':'Fabricated answer'}, {'answer_event_ids':['old:0']}, {'turn_id':'other'}):
        with pytest.raises(ForgeError):
            bridge.invoke(dict(operation='resolve_question', arguments=dict(note_index=2, **extra)))
    assert len(calls) == 1
    store.cancel('w', session['id'], turn['id'])
    with pytest.raises(ForgeError, match='no longer active'):
        bridge.invoke(dict(operation='resolve_question', arguments=dict(note_index=2)))


def test_native_human_answer_resolves_question_without_another_message(managed_project, independent_reviewer, monkeypatch):
    root,path,config=managed_project
    executable=path.parent/'project-provider';fixture=Path(__file__).with_name('devchat_fake_project_provider.py')
    executable.write_text('#!'+sys.executable+'\nimport runpy\nrunpy.run_path('+repr(str(fixture))+',run_name="__main__")\n');executable.chmod(0o700)
    endpoint,calls=independent_reviewer;monkeypatch.setenv('FIXTURE_REVIEW_KEY','offline-fixture-only')
    config['providers']['codex'].update(executable=str(executable),base_url=endpoint,credential_env='FIXTURE_REVIEW_KEY')
    path.write_text(json.dumps(config));service=Service(path)
    try:
        sid=service.create_project('wfixture',root,'create','codex')['id']
        started=service.start_project('wfixture',root,sid,'native',0,'repair-fixture:native-question')
        until=time.monotonic()+12
        while time.monotonic()<until and not service.store.pending(started['id']):time.sleep(.05)
        pending=service.store.pending(started['id'])[0]
        service.input('wfixture',root,sid,started['id'],pending['id'],pending['digest'],dict(width='64'))
        saved,result=gate_done(service,started['id'])
        assert result['state']=='accepted',(result,saved['result'])
        record=json.loads(task_file(service,sid).read_text());question=next(n for n in record['notes'] if n['kind']=='question')
        assert question['resolved']['answer_event_ids']==[pending['id']]
        assert len(calls)==1
        bundle=json.loads(calls[0]['input'].split('\n\n',1)[1])
        assert bundle['questions'][0]['answers']==[dict(event_id=pending['id'],text='{"width":"64"}')]
        with service.store.connect() as db:assert db.execute('SELECT count(*) FROM turns WHERE session=?',(sid,)).fetchone()[0]==1
    finally:service.close()


def test_actual_restricted_project_question_answer_preserves_acceptance_history(managed_project, independent_reviewer, monkeypatch, tmp_path):
    from test_forge_development_runtime import combined_configuration
    root, path, config = managed_project
    executable = path.parent/'project-provider'
    fixture = Path(__file__).with_name('devchat_fake_project_provider.py')
    executable.write_text('#!'+sys.executable+'\nimport runpy\nrunpy.run_path('+repr(str(fixture))+',run_name="__main__")\n')
    executable.chmod(0o700)
    host = tmp_path/'host'; host.mkdir()
    host_config, _, _, domain, _ = combined_configuration(host, root, monkeypatch)
    partition = json.loads(host_config.read_text()); domain.chmod(0o700)
    config.update({key:partition[key] for key in ('state_root','runtime_domain','host_budget')})
    endpoint, calls = independent_reviewer
    monkeypatch.setenv('FIXTURE_REVIEW_KEY', 'offline-fixture-only')
    config['mode']='restricted'; config['limits']['turn_seconds']=45
    config['providers']['codex'].update(executable=str(executable),base_url=endpoint,credential_env='FIXTURE_REVIEW_KEY')
    path.write_text(json.dumps(config))
    service = Service(path)
    try:
        sid = service.create_project('wfixture',root,'create','codex')['id']
        first = service.start_project('wfixture',root,sid,'first',0,'repair-fixture:project-initial')
        first_saved, accepted = gate_done(service,first['id'],timeout=45)
        assert accepted['state']=='accepted' and accepted['assurance']=='enforced', (accepted,first_saved['result'])
        original=json.loads(task_file(service,sid).read_text()); original_history=first_saved['development']
        question=service.start_project('wfixture',root,sid,'question',1,'Create an image quality operator')
        saved, pending=gate_done(service,question['id'],timeout=45)
        assert pending['state']=='needs_input' and not pending['busy'], (pending,saved['result'])
        assert pending['pending_question_count']==1 and len(calls)==1
        events=[e for e in service.store.events('wfixture',sid)['events'] if e['turn_id']==question['id']]
        assert not any(e['type']=='development.state' and e['data']['state'] in {'checking','verifying','reviewing','repair_pending'} for e in events)
        waiting=json.loads(task_file(service,sid).read_text())
        assert waiting['managed']['acceptance']==original['managed']['acceptance']
        service.close(); service=Service(path)
        assert service.show('wfixture',sid)['turn']['development']['state']=='needs_input'
        answer=service.start_project('wfixture',root,sid,'answer',2,'Keep images whose width is at least 64 pixels')
        saved, result=gate_done(service,answer['id'],timeout=45)
        assert result['state']=='accepted' and result['assurance']=='enforced', (result,saved['result'])
        updated=json.loads(task_file(service,sid).read_text())
        assert updated['baseline']==original['baseline'] and updated['goal']==original['goal']
        assert service.store.turn(first['id'])['development']==original_history
        q=next(note for note in updated['notes'] if note['kind']=='question')
        assert q['resolved']['answer_event_ids']==[answer['id']+':0']
        assert len(calls)==2 and len([n for n in updated['notes'] if n['kind']=='decision'])==3
        context=json.loads(saved['context'])
        assert context['current_request']==dict(turn_id=answer['id'],text='Keep images whose width is at least 64 pixels',event_ids=[answer['id']+':0'])
        prompt=(service.root/sid/'provider/received-context.txt').read_text()
        assert 'initial_goal' in prompt and 'current_request' in prompt and 'Create an image quality operator' in prompt
        assert 'What minimum image width' in prompt
    finally:
        service.close()
