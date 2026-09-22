"""Offline authors use native rendering, strict response binding and schema checks."""
import asyncio
import json

import pytest

from demiflow.operator_llm.client import AsyncOperatorLLMClient
from demiflow.operator_llm.model import OperatorLLMRequest
from demiflow.operator_llm.offline import submit_response
from demiflow.operator_llm.parser import parse_prompt_pack
from demiflow.operator_llm.template import render_template
from demiflow.standalone import local_data

PACK = '''
schema_version: demiflow_prompt_pack_v2
prompts:
  author:
    version: v1
    model:
      name: fixture-author
      transport: openai_compatible
      base_url: http://127.0.0.1:1/v1
      api_key_env: OFFLINE_FIXTURE_KEY
    schema_retries: 0
    template: "{{ task }}\\n{{ images | numbered_image }}"
    response_schema:
      type: object
      required: [result]
      properties:
        result: {type: integer}
      additionalProperties: false
'''


def context(tmp_path):
    return local_data(prompt_packs={'p.yaml': parse_prompt_pack(PACK)},
                      prompt_options={'offline_dir': str(tmp_path)}, max_prompt_requests=0)


def evaluate(ctx, images=()):
    rows = []
    (ctx.from_items([{'task': 'original context', 'images': list(images)}])
        .map_prompt_async('author', config='p.yaml', inputs=['task', 'images'],
                          output='answer', call_output='call', error_output='error')
        .map(lambda row: {**row, 'applied': True})
        .map_async(lambda row: rows.append(row) or row).run_stream())
    assert rows[0]['applied']
    return rows[0]


def test_pending_and_resume_use_native_schema_without_provider_call(tmp_path, monkeypatch):
    monkeypatch.delenv('OFFLINE_FIXTURE_KEY', raising=False)
    ctx = context(tmp_path)
    first = evaluate(ctx)
    assert first['error']['type'] == 'PromptResponsePending'
    request = next((tmp_path / 'requests').glob('*.json'))
    response = tmp_path / 'responses' / request.name
    submit_response(request, response, {'result': 7}, model='fixture-author', metadata={'author': 'fixture'})
    actual = evaluate(ctx)
    assert actual['answer'] == 7
    assert actual['call']['offline_metadata'] == {'author': 'fixture'}
    assert ctx.prompt_usage()['provider_requests_started'] == 0
    assert len(list((tmp_path / 'requests').glob('*.json'))) == 1


def test_offline_and_http_context_are_identical_including_images(tmp_path, monkeypatch):
    monkeypatch.setenv('OFFLINE_FIXTURE_KEY', 'fixture-only')
    images = ['data:image/png;base64,aGVsbG8=', 'data:image/png;base64,d29ybGQ=']
    evaluate(context(tmp_path), images)
    offline = json.loads(next((tmp_path / 'requests').glob('*.json')).read_text())
    prompt = parse_prompt_pack(PACK).prompt_definitions['author']
    request = OperatorLLMRequest(prompt.name, prompt.version, prompt.model.name,
        render_template(prompt.template, {'task': 'original context', 'images': images}),
        response_schema=prompt.response_schema)
    async def http_record():
        client = AsyncOperatorLLMClient(prompt.model)
        try:
            return client.record(request)
        finally:
            await client.aclose()
    assert asyncio.run(http_record())['payload']['messages'] == offline['messages']


@pytest.mark.parametrize('mutation', ['binding', 'model', 'schema'])
def test_bad_submitted_responses_do_not_pass(tmp_path, mutation):
    ctx = context(tmp_path)
    evaluate(ctx)
    request = next((tmp_path / 'requests').glob('*.json'))
    response = tmp_path / 'responses' / request.name
    submit_response(request, response, {'result': 7}, model='fixture-author')
    record = json.loads(response.read_text())
    if mutation == 'binding': record['request_sha256'] = 'wrong'
    if mutation == 'model': record['model'] = 'wrong'
    if mutation == 'schema': record['content'] = {'result': 'wrong'}
    response.write_text(json.dumps(record))
    row = evaluate(ctx)
    assert 'error' in row and 'answer' not in row
    if mutation == 'schema': assert row['error']['type'] == 'PromptResponseContractError'


def test_modified_request_and_conflicting_response_cannot_be_overwritten(tmp_path):
    ctx = context(tmp_path)
    evaluate(ctx)
    request = next((tmp_path / 'requests').glob('*.json'))
    response = tmp_path / 'responses' / request.name
    submit_response(request, response, {'result': 7}, model='fixture-author')
    with pytest.raises(ValueError):
        submit_response(request, response, {'result': 8}, model='fixture-author')
    record = json.loads(request.read_text())
    record['messages'][1]['content'] = 'tampered'
    request.write_text(json.dumps(record))
    with pytest.raises(ValueError, match='request changed'):
        submit_response(request, response, {'result': 7}, model='fixture-author')
    assert 'error' in evaluate(ctx)


def test_sync_map_after_async_preserves_arguments_and_failure_boundary(tmp_path):
    data = local_data()
    def add(row, increment, *, suffix):
        return {'value': row['value'] + increment, 'label': suffix}
    actual = (data.from_items([{'value': 2}]).map_async(lambda row: row)
        .map(add, fn_args=[3], fn_kwargs={'suffix': 'mapped'})
        .checkpoint(tmp_path / 'ok.jsonl', version='1').take_all())
    assert actual == [{'value': 5, 'label': 'mapped'}]
    def fail(row):
        raise ValueError('business validation failed')
    with pytest.raises(ValueError, match='business validation failed'):
        (data.from_items([{'value': 2}]).map_async(lambda row: row).map(fail)
         .checkpoint(tmp_path / 'failed.jsonl', version='1'))
    assert not (tmp_path / 'failed.jsonl').exists()
