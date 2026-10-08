"""One self-contained file per operator family; only local fixture services execute."""
from copy import deepcopy
import json
import yaml
import pytest

from demiflow import data
from demiflow.agent import load_agent_config
from demiflow.operator_llm.parser import load_prompt_pack
from demiflow.operator_llm.errors import PromptPackError
from test_map_prompt_async import PACK, server
from test_agentmap_async import collect, resource, api_call
from test_codex_agent import app_server, events
from test_prompt_http_stream import sse_server, event, chunk


def entries(tmp_path, *, codex=None, operators=()):
    task = yaml.safe_load(PACK)['prompts']['enrich']
    model = task.pop('model')
    agent = {'schema_version': 'demiflow_agent_v2', 'runtime': 'codex' if codex else 'demiflow',
        'model': {'name': 'mock-model'} if codex else model, 'tasks': {'enrich': task},
        'operators': list(operators), 'budgets': {'max_requests': 4},
        'options': {'codex_agent': {'bin': str(codex)}, 'timeout_s': 4} if codex else {}}
    if operators:
        agent['resources'] = 'resources'
    path = tmp_path / 'agent.yaml'
    path.write_text(yaml.safe_dump(agent))
    prompt = tmp_path / 'prompt.yaml'
    prompt.write_text(PACK)
    return path, prompt, agent


def node(path, **kw):
    return data.from_items([{'item': 'q', 'resources': kw.pop('resources', {})}]).agentmap_async(
        'enrich', config=path, inputs={'payload': 'item'}, output='answer', error_output='error',
        call_output='call', **kw)


def test_yaml_http_repairs_and_reads_native_arguments(tmp_path, server, resource):
    path, _, _ = entries(tmp_path, operators=('read_documents',))
    replies = [ {'api_calls': [{'method': 'read_documents', 'arguments': {'document_ids': ['D1']}}], 'response': None},
        {'api_calls': [api_call(resource)], 'response': None},
        {'api_calls': [], 'response': {'result': 'verified'}}]
    server['respond'] = lambda body, index: replies[index]
    row = collect(node(path, resources={'D1': resource}))[0]
    assert row['answer'] == 'verified' and len(server['requests']) == 3
    observations = row['call']['environment']['observations']
    assert observations[0]['result']['code'] == 'invalid_arguments'
    assert observations[1]['call']['arguments'] == api_call(resource)['arguments']


def test_yaml_codex_owns_model_and_native_tools(tmp_path, app_server, monkeypatch):
    monkeypatch.setenv('DF_TEST_CODEX_MODE', 'direct')
    path, _, _ = entries(tmp_path, codex=app_server[0])
    agent = load_agent_config(path)
    assert agent.prompt_pack.required_environment_names == ()
    row = collect(node(path, options={'sqlite_journal': {'path': str(tmp_path / 'calls.sqlite')}}))[0]
    assert row['answer'] == 'done'
    start = next(e['received']['params'] for e in events(app_server)
                 if e.get('received', {}).get('method') == 'thread/start')
    assert start['model'] == 'mock-model' and start['dynamicTools'] == []


@pytest.mark.parametrize('complete', [True, False])
def test_yaml_stream_survives_tool_turns_repairs_and_replay(tmp_path, sse_server, resource, complete):
    """Exercise the actual agent YAML -> multi-turn HTTP SSE -> journal path."""
    path, _, raw = entries(tmp_path, operators=('read_documents',))
    raw['options'] = {'stream': True, 'trust_env': False, 'read_timeout_s': 2,
                      'request_options': {'response_format': {'type': 'json_object'}}}
    path.write_text(yaml.safe_dump(raw))
    replies = [
        {'api_calls': [api_call(resource)], 'response': None},
        {'api_calls': [], 'response': {'result': 123}},  # valid SSE, invalid business schema
        {'api_calls': [], 'response': {'result': 'verified'}},
    ]

    def respond(body, index):
        text = json.dumps(replies[index], ensure_ascii=False)
        return (event(chunk(text[:len(text)//2])) + event(chunk(text[len(text)//2:], 'stop'))
                + (event('[DONE]') if complete or index < 2 else b''))

    sse_server['respond'] = respond
    ds = node(path, resources={'D1': resource},
              options={'sqlite_journal': {'path': str(tmp_path / 'calls.sqlite')}})
    first = collect(ds)[0]
    assert len(sse_server['requests']) == 3
    assert all(body['stream'] is True and body['stream_options']['include_usage'] is True
               and body['response_format'] == {'type': 'json_object'}
               for body in sse_server['requests'])
    trace = first['call'] if complete else first['error']['call']
    assert trace['environment']['observations'][0]['call']['arguments'] == api_call(resource)['arguments']
    assert 'Full qualifying context.' in json.dumps(sse_server['requests'][1], ensure_ascii=False)
    if complete:
        assert first['answer'] == 'verified'
        assert all(turn['stream_complete'] for turn in trace['environment']['turns'])
    else:
        assert first.get('answer') is None
        assert first['error']['category'] == 'incomplete_response'
    replay = collect(ds)[0]
    assert len(sse_server['requests']) == 3  # no new request or non-stream fallback
    if complete:
        assert replay['answer'] == 'verified'
        assert all(turn['reused'] for turn in replay['call']['environment']['turns'])
    else:
        assert replay['error']['category'] == 'uncertain_call'


def test_family_mismatch_and_split_configuration_fail_before_execution(tmp_path):
    path, prompt, _ = entries(tmp_path)
    with pytest.raises(PromptPackError):
        load_prompt_pack(path)
    with pytest.raises(PromptPackError):
        load_agent_config(prompt)
    with pytest.raises(TypeError, match='one agent config'):
        node(path, environment=object())
    with pytest.raises(ValueError, match='belong in config'):
        node(path, options={'request_options': {'max_tokens': 50}})


def test_agent_file_has_no_prompt_file_dependency(tmp_path):
    path, prompt, raw = entries(tmp_path)
    first = load_agent_config(path)
    ordinary = load_prompt_pack(prompt)
    assert first.prompt_pack.prompts[0].template == ordinary.prompts[0].template
    prompt.unlink()
    assert load_agent_config(path).prompt_pack.content_hash == first.prompt_pack.content_hash
    raw['tasks']['enrich']['template'] = 'Changed task: {{ payload | json }}'
    path.write_text(yaml.safe_dump(raw))
    assert load_agent_config(path).prompt_pack.content_hash != first.prompt_pack.content_hash
    raw['tasks']['enrich']['model'] = {'name': 'hidden-model'}
    path.write_text(yaml.safe_dump(raw))
    with pytest.raises(PromptPackError, match='only version'):
        load_agent_config(path)


@pytest.mark.parametrize('limit', [None, 100, 1, 0])
def test_node_cannot_expand_configured_budget(tmp_path, server, limit):
    path, _, raw = entries(tmp_path)
    raw['budgets']['max_requests'] = 1
    path.write_text(yaml.safe_dump(raw))
    server['respond'] = lambda body, index: {'api_calls': [{'method': 'not_allowed', 'arguments': {}}], 'response': None}
    row = collect(node(path, max_requests=limit))[0]
    assert row['error']['type'] == 'PromptBudgetExceededError'
    assert len(server['requests']) == (0 if limit == 0 else 1)


@pytest.mark.parametrize('change', [
    {'schema_version': 'demiflow_prompt_pack_v2'}, {'unknown': 1},
    {'runtime': 'other'}, {'tasks': '../tasks.yaml'}, {'tasks': '/tmp/tasks.yaml'},
    {'budgets': {}}, {'budgets': {'max_requests': True}}, {'operators': ['unknown']},
    {'runtime': 'codex'}, {'options': {'codex_agent': {}}},
])
def test_invalid_entries_rejected_without_execution(tmp_path, change):
    path, _, raw = entries(tmp_path)
    raw.update(deepcopy(change))
    path.write_text(yaml.safe_dump(raw))
    with pytest.raises((ValueError, PromptPackError)):
        load_agent_config(path)


def test_size_and_file_reference_boundaries(tmp_path):
    path, _, _ = entries(tmp_path)
    path.write_text(' ' * (1024 * 1024 + 1))
    with pytest.raises(PromptPackError, match='exceeds'):
        load_agent_config(path)
    path, _, raw = entries(tmp_path)
    raw['tasks'] = 'tasks.yaml'
    path.write_text(yaml.safe_dump(raw))
    with pytest.raises(PromptPackError, match='inline definitions'):
        load_agent_config(path)
