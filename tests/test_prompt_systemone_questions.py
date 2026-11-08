"""Native multi-question contract through the public Dataset prompt operator."""
import copy
import json
import threading
import uuid
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import pytest
import yaml

from demiflow.data.api import DataAPI
from demiflow.operator_llm.errors import PromptResponseContractError
from demiflow.operator_llm.parser import parse_prompt_pack
from demiflow.operator_llm.systemone import render_questions


DEFINITION = {
    'schema_version': 'demiflow_prompt_pack_v2',
    'prompts': {'match': {
        'version': 'multi-v1',
        'model': {'name': 'mock-decision', 'transport': 'systemone',
                  'base_url_env': 'TEST_QUESTIONS_URL'},
        'schema_retries': 0,
        'systemone': {'type': 'noul', 'instructions': 'Is the image about {{ concept }}?',
                      'for_each': {'input': 'candidates', 'key': 'id',
                                   'max_questions': 50, 'max_bytes': 131072}},
        'response_schema': {'type': 'object', 'required': ['result'],
                            'properties': {'result': {'type': 'object',
                                'required': ['answers', 'errors'],
                                'properties': {'answers': {'type': 'object', 'additionalProperties': True},
                                               'errors': {'type': 'object', 'additionalProperties': True}}}}},
        'template': 'Evaluate each candidate independently. {{ image | image }}',
    }},
}
ITEMS = [{'id': 'a', 'concept': '剪刀'}, {'id': 'b', 'concept': '桥'}]


@pytest.fixture
def server(monkeypatch):
    state = {'requests': [], 'answers': None}
    class Handler(BaseHTTPRequestHandler):
        def do_POST(self):
            body = json.loads(self.rfile.read(int(self.headers['Content-Length'])))
            state['requests'].append(body)
            answers = state['answers']
            if answers is None:
                answers = {key: {'type': 'noul', 'noul': 0.9} for key in body['questions']}
            wire = json.dumps({'answers': answers}).encode()
            self.send_response(200)
            self.send_header('Content-Type', 'application/json')
            self.end_headers()
            self.wfile.write(wire)
        def log_message(self, *args):
            pass
    srv = ThreadingHTTPServer(('127.0.0.1', 0), Handler)
    thread = threading.Thread(target=srv.serve_forever, daemon=True)
    thread.start()
    monkeypatch.setenv('TEST_QUESTIONS_URL', f'http://127.0.0.1:{srv.server_port}/v1')
    yield state
    srv.shutdown()
    srv.server_close()
    thread.join()


def execute(tmp_path, items=ITEMS, *, definition=DEFINITION, image='data:image/png;base64,aW1hZ2U=', revision='v1'):
    return DataAPI().from_items([{'candidates': items, 'image': image}]).map_prompt_async(
        'match', config=parse_prompt_pack(yaml.safe_dump(definition, sort_keys=False)),
        inputs={'candidates': 'candidates', 'image': 'image'}, output='answer',
        options={'trust_env': False, 'journal_dir': str(tmp_path / 'calls'), 'model_revision': revision},
    ).checkpoint(tmp_path / f'{uuid.uuid4().hex}.jsonl', version='fixture').take_all()[0]['answer']


def test_questions_share_one_image_request_and_replay_requires_order_and_content(server, tmp_path):
    assert execute(tmp_path) == {'answers': {'a': {'type': 'noul', 'noul': 0.9},
                                           'b': {'type': 'noul', 'noul': 0.9}}, 'errors': {}}
    execute(tmp_path)
    assert len(server['requests']) == 1
    body = server['requests'][0]
    assert body['images'] == ['data:image/png;base64,aW1hZ2U=']
    assert list(body['questions']) == ['a', 'b']
    assert body['questions']['a']['instructions'] == 'Is the image about 剪刀?'
    execute(tmp_path, list(reversed(ITEMS)))
    execute(tmp_path, [{'id': 'a', 'concept': '树'}, ITEMS[1]])
    execute(tmp_path, image='data:image/png;base64,b3RoZXI=')
    execute(tmp_path, revision='new-revision')
    assert len(server['requests']) == 5


def test_single_remaining_question_keeps_multi_answer_contract(server, tmp_path):
    assert execute(tmp_path, ITEMS[:1]) == {
        'answers': {'a': {'type': 'noul', 'noul': 0.9}}, 'errors': {}}


@pytest.mark.parametrize('answer,error', [
    (None, 'missing_answer'), ({'type': 'noul', 'noul': 1.2}, 'invalid_answer'),
    ({'type': 'noul', 'noul': '0.9'}, 'invalid_answer'),
    ({'type': 'noul', 'noul': True}, 'invalid_answer'),
])
def test_partial_results_preserve_valid_scores_without_fabricating_zero(server, tmp_path, answer, error):
    server['answers'] = {'a': {'type': 'noul', 'noul': 0.9}}
    if answer is not None:
        server['answers']['b'] = answer
    assert execute(tmp_path) == {'answers': {'a': {'type': 'noul', 'noul': 0.9}},
                                 'errors': {'b': error}}


def test_unknown_answer_cannot_be_attached_to_a_candidate(server, tmp_path):
    server['answers'] = {'unknown': {'type': 'noul', 'noul': 0.9}}
    with pytest.raises(PromptResponseContractError, match='unknown IDs'):
        execute(tmp_path)


@pytest.mark.parametrize('items', [[], ITEMS * 26, [ITEMS[0], ITEMS[0]],
                                   [{'id': 'a', 'concept': 'x' * 131073}]])
def test_invalid_or_over_budget_questions_fail_before_http(server, tmp_path, items):
    with pytest.raises(ValueError):
        execute(tmp_path, items)
    assert not server['requests']


def test_question_text_is_data_and_not_recursively_rendered():
    question = DEFINITION['prompts']['match']['systemone']
    value = render_questions(question, {'candidates': [{'id': 'a', 'concept': '{{ image | image }}'}]})
    assert value['questions']['a']['instructions'] == 'Is the image about {{ image | image }}?'


def test_choice_question_uses_existing_criteria_validation(server, tmp_path):
    definition = copy.deepcopy(DEFINITION)
    question = definition['prompts']['match']['systemone']
    question.update(type='choice', criteria={'yes': 'matches', 'no': 'does not match'})
    server['answers'] = {'a': {'type': 'choice', 'choice': 'yes',
                             'probabilities': {'yes': 0.9, 'no': 0.1}}}
    assert execute(tmp_path, ITEMS[:1], definition=definition)['answers']['a']['choice'] == 'yes'


def test_character_budget_counts_rendered_questions():
    from demiflow.operator_llm.tokens import CharacterCounter
    prompt = parse_prompt_pack(yaml.safe_dump(DEFINITION)).prompt_definitions['match']
    values = {'image': 'data:image/png;base64,aW1hZ2U=', 'candidates': ITEMS[:1]}
    first = CharacterCounter().prompt(prompt, values)
    values['candidates'] = ITEMS
    assert CharacterCounter().prompt(prompt, values) > first
