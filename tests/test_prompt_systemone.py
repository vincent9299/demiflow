import json
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import pytest

from demiflow.data.api import DataAPI
from demiflow.operator_llm.errors import PromptDefinitionError, PromptResponseContractError
from demiflow.operator_llm.parser import parse_prompt_pack


PACK = '''
schema_version: demiflow_prompt_pack_v2
prompts:
  match:
    version: v1
    model:
      name: mock-decision
      transport: systemone
      base_url_env: TEST_SYSTEMONE_URL
    schema_retries: 0
    systemone:
      type: noul
      instructions: Does the text define the same concept and scope?
    response_schema:
      type: object
      additionalProperties: false
      required: [result]
      properties:
        result:
          type: object
          additionalProperties: false
          required: [type, noul]
          properties:
            type: {type: string, enum: [noul]}
            noul: {type: number, minimum: 0, maximum: 1}
    template: |
      {{ payload | json }}
'''


@pytest.fixture
def server(monkeypatch):
    state = {'requests': [], 'probability': 0.875}
    class Handler(BaseHTTPRequestHandler):
        def do_POST(self):
            body = json.loads(self.rfile.read(int(self.headers['Content-Length'])))
            state['requests'].append({'body': body, 'path': self.path})
            value = json.dumps({
                'model': 'mock-decision',
                'answers': {'match': {'type': 'noul', 'noul': state['probability']}},
                'usage': {'input_tokens': 10, 'output_tokens': 0},
            }).encode()
            self.send_response(200)
            self.send_header('Content-Type', 'application/json')
            self.end_headers()
            self.wfile.write(value)
        def log_message(self, *args):
            pass
    srv = ThreadingHTTPServer(('127.0.0.1', 0), Handler)
    worker = threading.Thread(target=srv.serve_forever, daemon=True)
    worker.start()
    monkeypatch.setenv('TEST_SYSTEMONE_URL', f'http://127.0.0.1:{srv.server_port}/v1')
    yield state
    srv.shutdown()
    srv.server_close()
    worker.join()


def review(ctx, journal=None):
    options = {'trust_env': False}
    if journal is not None:
        options['journal_dir'] = str(journal)
    return ctx.from_items([{'item': {'concept_name': '剪刀', 'text': '剪刀是双刃工具。'}}]).map_prompt_async(
        'match', config=parse_prompt_pack(PACK), options=options,
        inputs={'payload': 'item'}, output='answer',
    )


def test_systemone_noul_uses_existing_prompt_node_and_journal(server, tmp_path):
    journal = tmp_path / 'calls'
    first = review(DataAPI(), journal).checkpoint(tmp_path / 'first.jsonl', version='v1').take_all()
    second = review(DataAPI(), journal).checkpoint(tmp_path / 'second.jsonl', version='v1').take_all()
    expected = {'type': 'noul', 'noul': 0.875}
    assert first[0]['answer'] == second[0]['answer'] == expected
    assert len(server['requests']) == 1
    request = server['requests'][0]
    assert request['path'] == '/v1/systemone'
    assert json.loads(request['body']['state']) == {'concept_name': '剪刀', 'text': '剪刀是双刃工具。'}
    assert request['body']['questions'] == {
        'match': {'type': 'noul', 'instructions': 'Does the text define the same concept and scope?'}
    }
    assert 'messages' not in request['body']


def test_systemone_rejects_invalid_probability(server, tmp_path):
    server['probability'] = 1.2
    with pytest.raises(PromptResponseContractError, match='noul'):
        review(DataAPI()).checkpoint(tmp_path / 'invalid.jsonl', version='v1').take_all()


def test_systemone_requires_explicit_transport_and_noul_contract():
    with pytest.raises(PromptDefinitionError, match='model.transport=systemone'):
        parse_prompt_pack(PACK.replace('transport: systemone', 'transport: openai_compatible\n      api_key_env: TEST_KEY'))
    with pytest.raises(PromptDefinitionError, match='zero schema retries'):
        parse_prompt_pack(PACK.replace('schema_retries: 0', 'schema_retries: 1'))
