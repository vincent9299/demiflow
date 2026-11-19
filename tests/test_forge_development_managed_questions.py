"""Human-answer provenance and domain waiting gates; only isolated fixtures."""
import json
from unittest.mock import patch

import pytest

from demiflow.forge.development.managed import ManagedTasks
from demiflow.forge.development.project import ForgeError
from test_forge_development_managed import managed, pass_gates


def test_pending_question_short_circuits_and_preserves_prior_evidence(managed):
    api, _, authority = managed
    pass_gates(api, authority)
    old = api.evaluate_acceptance('session')
    record = api.store.root / 'session/task.json'
    original = json.loads(record.read_text())
    api.note('session', 'decision', 'Current request', event_id='turn:0')
    index = api.note('session', 'question', 'Which image quality threshold?')['note_index']
    pending = api.status('session')
    assert pending['development_state'] == 'needs_input'
    assert pending['pending_question_count'] == 1
    assert pending['pending_questions'] == [dict(note_index=index, text='Which image quality threshold?')]
    assert not pending['ready'] and not pending['repairable']
    assert pending['source_fingerprint'] != old['source_fingerprint']
    with patch('demiflow.forge.development.tasks.check', side_effect=AssertionError('pending check ran')):
        assert api.verify('session')['development_state'] == 'needs_input'
        assert api.evaluate_acceptance('session')['development_state'] == 'needs_input'
    with pytest.raises(ForgeError, match='needs_input'):
        api.review_material('session', pending['source_fingerprint'])
    saved = json.loads(record.read_text())
    for key in ('baseline', 'readonly_baseline', 'binding', 'goal', 'check', 'verification', 'review'):
        assert saved[key] == original[key]
    assert saved['managed']['acceptance'] == original['managed']['acceptance']


@pytest.mark.parametrize('answer', [['old:0'], ['same:0'], ['forged:0'], ['progress'], [], ['new:0', 'new:0']])
def test_question_rejects_old_same_turn_forged_and_nonhuman_answers(managed, answer):
    api, _, _ = managed
    api.note('session', 'decision', 'Earlier message', event_id='old:0')
    api.note('session', 'decision', 'Current message before model question', event_id='same:0')
    index = api.note('session', 'question', 'Need a real answer')['note_index']
    api.note('session', 'progress', 'Model supplied answer', event_id='progress')
    api.note('session', 'decision', 'Later human answer', event_id='new:0')
    with pytest.raises(ForgeError):
        api.resolve_question('session', index, answer_event_ids=answer)
    assert api.status('session')['pending_question_count'] == 1


def test_exact_answer_resumes_after_restart_without_rebasing(managed):
    api, project, authority = managed
    pass_gates(api, authority); old = api.evaluate_acceptance('session')
    baseline = json.loads((api.store.root / 'session/task.json').read_text())['baseline']
    index = api.note('session', 'question', 'Specify input and threshold')['note_index']
    api.note('session', 'decision', 'Use the fixture images, ', event_id='answer:0')
    api.note('session', 'decision', 'threshold 0.2.', event_id='answer:1')
    api = ManagedTasks(api.store.root.parent.parent, 'fixture', project.root, review_authority=authority)
    result = api.resolve_question('session', index, answer_event_ids=['answer:0', 'answer:1'])
    assert result['duplicate'] is False
    before = (api.store.root / 'session/task.json').read_bytes()
    assert api.resolve_question('session', index, answer_event_ids=['answer:0', 'answer:1'])['duplicate']
    assert (api.store.root / 'session/task.json').read_bytes() == before
    with pytest.raises(ForgeError, match='different answer'):
        api.resolve_question('session', index, answer_event_ids=['answer:1'])
    context = api.context('session')
    assert context['questions'][0]['resolved'] is True
    assert ''.join(a['text'] for a in context['questions'][0]['answers']) == 'Use the fixture images, threshold 0.2.'
    assert context['current_request']['text'] == 'Use the fixture images, threshold 0.2.'
    assert api.status('session')['pending_question_count'] == 0
    assert not api.status('session')['ready']
    pass_gates(api, authority)
    material = api.review_material('session', api.status('session')['source_fingerprint'])['input']
    assert material['questions'] == context['questions']
    assert material['current_request'] == context['current_request']
    accepted = api.evaluate_acceptance('session')
    assert accepted['development_state'] == 'accepted'
    assert accepted['source_fingerprint'] != old['source_fingerprint']
    assert json.loads((api.store.root / 'session/task.json').read_text())['baseline'] == baseline


def test_pending_projection_is_bounded_without_hiding_remaining_questions(managed):
    api, _, _ = managed
    for index in range(70):
        api.note('session', 'question', str(index) + 'x' * 3000)
    current = api.status('session')
    assert current['pending_question_count'] == 70
    assert current['pending_questions_truncated'] is True
    assert len(json.dumps(current['pending_questions'], ensure_ascii=False).encode()) < 17000
    assert current['development_state'] == 'needs_input'


def test_review_prompt_distinguishes_current_request_and_binding_history():
    from demiflow.workbench.conversations.review import _prompt
    prompt = _prompt(dict(current_request=dict(text='Create an image quality flow'),
                          original_goal='Earlier name normalization fixture', questions=[]))
    assert 'current controller-recorded request' in prompt
    assert 'exact human question/answer clarifications' in prompt
    assert 'unchanged requirements, authorized scope' in prompt
    assert 'earlier completed objective' in prompt
    assert 'DF-01 through DF-07' in prompt
