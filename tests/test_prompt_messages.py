"""Caller-owned histories use the existing single-completion transport."""
import copy
import json
import yaml
import pytest
from demiflow.data.api import DataAPI
from demiflow.operator_llm.parser import parse_prompt_pack
from demiflow.operator_llm.messages import validate_messages
from demiflow.operator_llm.offline import submit_response
from demiflow.operator_llm.tokens import CharacterCounter
from test_map_prompt_async import PACK, server
from test_prompt_http_stream import sse_server, event, chunk


def pack(**changes):
    raw = yaml.safe_load(PACK)
    prompt = raw['prompts']['enrich']
    prompt.pop('template')
    prompt.update(input_mode='messages', response_format='text', **changes)
    return parse_prompt_pack(yaml.safe_dump(raw))


def history():
    return [{'role': 'user', 'content': [{'type': 'text', 'text': 'literal {{ x }}'},
            {'type': 'image_url', 'image_url': {'url': 'data:image/png;base64,aGVsbG8=', 'detail': 'high'}}]},
            {'role': 'assistant', 'content': 'no'},
            {'role': 'user', 'content': [{'type': 'text', 'text': 'next'}]}]


def node(messages, **kwargs):
    return DataAPI().from_items([{'messages': messages}]).map_prompt_async(
        'enrich', config=pack(), inputs={'messages': 'messages'}, output='answer',
        call_output='call', error_output='error', **kwargs)


def test_http_exact_history_replay_and_changed_assistant(server, tmp_path):
    messages = history()
    before = copy.deepcopy(messages)
    options = {'sqlite_journal': {'path': str(tmp_path / 'calls.sqlite'), 'max_requests': 2}}
    for i in range(2):
        rows = node(messages, options=options).checkpoint(tmp_path / f'out{i}.jsonl', version='v1').take_all()
        assert rows[0]['answer'] == '{"result": "ok"}'
    assert len(server['requests']) == 1
    assert messages == before
    body = server['requests'][0]['body']
    assert body['messages'] == messages
    assert body['response_format'] == {'type': 'text'}
    messages[1]['content'] = 'different answer'
    node(messages, options=options).run_stream()
    assert len(server['requests']) == 2


def test_offline_preserves_history_and_plain_text(tmp_path):
    options = {'offline_dir': str(tmp_path / 'offline')}
    node(history(), options=options).run_stream()
    request = next((tmp_path / 'offline/requests').glob('*.json'))
    assert json.loads(request.read_text())['messages'] == history()
    submit_response(request, tmp_path / 'offline/responses' / request.name, 'a sheep\nan oil painting', model='mock-model')
    rows = node(history(), options=options).checkpoint(tmp_path / 'out.jsonl', version='v1').take_all()
    assert rows[0]['answer'] == 'a sheep\nan oil painting'


def test_sse_plain_text_and_sync_parity(sse_server, tmp_path):
    sse_server['respond'] = lambda body, index: event(chunk('caption\n', 'stop')) + event('[DONE]')
    options = {'stream': True, 'verify_model': False}
    rows = node(history(), options=options).checkpoint(tmp_path / 'out.jsonl', version='v1').take_all()
    assert rows[0]['answer'] == 'caption\n'
    rows = DataAPI().from_items([{'m': history()}]).map_prompt('enrich', config=pack(),
        inputs={'messages': 'm'}, output='answer', options=options).take_all()
    assert rows[0]['answer'] == 'caption\n'
    assert all(r['messages'] == history() for r in sse_server['requests'])


def test_sync_without_options_preserves_messages(server):
    rows = DataAPI().from_items([{'m': history()}]).map_prompt('enrich', config=pack(),
        inputs={'messages': 'm'}, output='answer').take_all()
    assert rows[0]['answer'] == '{"result": "ok"}'
    assert server['requests'][0]['body']['messages'] == history()


@pytest.mark.parametrize('change', [{'template': 'unexpected'}, {'schema_retries': 1},
    {'message_limits': {'max_bytes': 0}}, {'input_mode': 'conversation'}])
def test_invalid_mode_contract(change):
    raw = yaml.safe_load(PACK)
    p = raw['prompts']['enrich']
    p.pop('template')
    p.update(input_mode='messages', **{k: v for k, v in change.items() if k != 'input_mode'})
    if 'input_mode' in change:
        p['input_mode'] = change['input_mode']
    with pytest.raises(ValueError):
        parse_prompt_pack(yaml.safe_dump(raw))


@pytest.mark.parametrize('limits', [{'max_bytes': 100}, {'max_messages': 2}, {'max_parts': 1}])
def test_limits_reject_before_copy(limits):
    with pytest.raises(ValueError):
        validate_messages(history(), limits)


def test_image_limit_and_unsupported_inputs():
    messages = history()
    messages[0]['content'] *= 2
    with pytest.raises(ValueError, match='max_images'):
        validate_messages(messages, {'max_images': 1})
    for value in ([], [{'role': 'tool', 'content': 'x'}], [{'role': 'user', 'content': None}],
                  [{'role': 'user', 'content': [{'type': 'audio', 'data': 'x'}]}]):
        with pytest.raises(ValueError):
            validate_messages(value)


def test_bad_row_never_calls_provider_and_counter_covers_history(server, tmp_path):
    rows = node([{'role': 'tool', 'content': 'bad'}]).checkpoint(tmp_path / 'out.jsonl', version='v1').take_all()
    assert rows[0]['error'] and not server['requests']
    assert CharacterCounter().prompt(pack().prompt_definitions['enrich'], {'messages': history()}) == CharacterCounter().messages(history())


def test_messages_binding_excludes_template_variables():
    with pytest.raises(ValueError):
        DataAPI().from_items([]).map_prompt_async('enrich', config=pack(), inputs={'payload': 'x'}, output='out')
