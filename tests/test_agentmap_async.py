"""Row-local native operator interaction, using only local HTTP and fixed fixtures."""
import asyncio
from dataclasses import replace
import json

import pytest
from demiflow import data
from demiflow.agent import AgentConfig
from demiflow.collect.documents import store_document
from demiflow.collect.reading import PromptContext, read_documents
from demiflow.data.plan import AgentMapOp
from demiflow.environment import OperatorEnvironment
from demiflow.operator_llm.parser import parse_prompt_pack
from demiflow.operator_llm.tokens import CharacterBudget
from test_map_prompt_async import PACK, server


@pytest.fixture
def resource(tmp_path):
    ref = store_document(tmp_path / 'docs', b'Previously supplied statement.\n\nFull qualifying context.',
        url='https://fixture.invalid/a', final_url='https://fixture.invalid/a',
        content_type='text/plain', retrieved_at='2026-01-01T00:00:00Z')
    return {'document_ref': ref, 'url': 'https://fixture.invalid/a', 'bindings': [], 'eligible': True}


def api_call(document=None, **overrides):
    request = dict(documents=[document] if document is not None else [], questions=[{'id': 'q', 'text': 'qualifying'}],
                   retained=[], requests=[], new_chars=6000, total_chars=6000)
    request.update(overrides)
    return {'method': 'read_documents', 'arguments': {'request': request}}


def collect(stream):
    rows = []
    stream.map(lambda row: rows.append(row) or row).run_stream()
    return rows


def configured_agent(pack, environment, options=None, max_requests=1000):
    from demiflow.operator_llm.model import PromptModel
    pack = parse_prompt_pack(pack) if isinstance(pack, str) else pack
    if environment.runtime == 'codex':
        pack = replace(pack, prompts=tuple(replace(p, model=PromptModel(p.model.name, 'codex')) for p in pack.prompts))
    return AgentConfig(pack, environment, options or {}, max_requests)


def agent(rows, environment=None, *, pack=PACK, agent_config=None, **kwargs):
    environment = environment or OperatorEnvironment(resources='resources')
    if agent_config is not None:
        environment = OperatorEnvironment.from_yaml(agent_config, **vars(environment))
    options = dict(kwargs.pop('options', {}) or {})
    storage = {key: options.pop(key) for key in tuple(options)
               if key in ('sqlite_journal', 'journal_dir', 'offline_store', 'offline_dir')}
    return data.from_items(rows).agentmap_async('enrich', config=configured_agent(pack, environment, options),
        inputs={'payload': 'item'}, output='answer', call_output='call', error_output='error',
        options=storage, **kwargs)


def yaml_agent(tmp_path, *, operators=('read_documents',), max_turns=4, max_calls_per_turn=2):
    import yaml
    value = dict(schema_version='demiflow_agent_v1', operators=list(operators), max_turns=max_turns,
                 max_calls_per_turn=max_calls_per_turn)
    path = tmp_path / 'agent.yaml'
    path.write_text(yaml.safe_dump(value))
    return path


@pytest.mark.parametrize('read_rounds', [0, 1, 2])
def test_yaml_agent_model_controls_number_and_content_of_reads(server, resource, read_rounds, tmp_path):
    """The same configured node may finish directly, read once, or read again."""
    def respond(body, index):
        sent = json.dumps(body['messages'])
        if index == 1:
            assert 'Previously supplied statement.' in sent
        if index == 2:
            assert 'Full qualifying context.' in sent
        if index < read_rounds:
            return {'api_calls': [api_call(resource, questions=[], requests=[{
                'request_id': 'r' + str(index), 'document_ref': resource['document_ref'],
                'block_ids': [f'b{index:06d}'], 'bindings': []}])], 'response': None}
        return {'api_calls': [], 'response': {'result': 'done'}}
    server['respond'] = respond
    row = collect(agent([{'item': 'question', 'resources': {'D1': resource}}], agent_config=yaml_agent(tmp_path)))[0]
    assert row['answer'] == 'done' and len(server['requests']) == read_rounds + 1
    observations = row['call']['environment']['observations']
    assert len(observations) == read_rounds
    assert [o['result']['materials'][0]['block_id'] for o in observations] == [
        f'b{i:06d}' for i in range(read_rounds)]



def test_yaml_visibility_is_enforced_and_not_just_prompt_text(server, resource, monkeypatch, tmp_path):
    import demiflow.collect.reading as module
    async def forbidden(*args, **kwargs):
        pytest.fail('disabled API must never execute')
    monkeypatch.setattr(module, 'read_documents', forbidden)
    server['respond'] = lambda body, index: ({'api_calls': [api_call(resource)], 'response': None} if index == 0
        else {'api_calls': [], 'response': {'result': 'no tool available'}})
    row = collect(agent([{'item': 'question', 'resources': {'D1': resource}}],
                        agent_config=yaml_agent(tmp_path, operators=())))[0]
    assert row['answer'] == 'no tool available' and len(server['requests']) == 2
    assert row['call']['environment']['observations'][0]['result']['code'] == 'operator_not_allowed'
    sent = json.dumps(server['requests'][0]['body']['messages'])
    assert 'read_documents' not in sent and 'new_chars' not in sent



@pytest.mark.parametrize('ceiling', ['yaml_turns', 'node_turns', 'yaml_calls', 'node_requests'])
def test_yaml_and_node_budgets_are_hard_ceilings(server, resource, ceiling, tmp_path):
    declaration = OperatorEnvironment(resources='resources', max_turns=1 if ceiling == 'node_turns' else 4)
    policy = yaml_agent(tmp_path, max_turns=1 if ceiling == 'yaml_turns' else 4,
                      max_calls_per_turn=1 if ceiling == 'yaml_calls' else 2)
    server['respond'] = lambda body, index: {
        'api_calls': [api_call(resource)] * (2 if ceiling == 'yaml_calls' else 1), 'response': None}
    row = collect(agent([{'item': 'question', 'resources': {'D1': resource}}],
                        environment=declaration, agent_config=policy,
                        max_requests=1 if ceiling == 'node_requests' else 8))[0]
    assert 'error' in row and 'answer' not in row
    assert len(server['requests']) == 1
    expected_observations = 1 if ceiling == 'node_requests' else 0
    assert len(row['error']['call']['environment']['observations']) == expected_observations



@pytest.mark.parametrize('policy', [None, {}, {'operators': 'read_documents'},
    {'operators': ['read_documents', 'read_documents']}, {'operators': ['read_documents'], 'extra': 1},
    {'operators': ['read_documents'], 'max_turns': True},
    {'operators': ['read_documents'], 'max_turns': 0},
    {'operators': ['read_documents'], 'max_calls_per_turn': 9}])
def test_malformed_agent_yaml_is_rejected(policy, tmp_path):
    import yaml
    value = {'schema_version': 'demiflow_agent_v1', **policy} if isinstance(policy, dict) else policy
    path = tmp_path / 'agent.yaml'
    path.write_text(yaml.safe_dump(value))
    with pytest.raises(ValueError):
        OperatorEnvironment.from_yaml(path, resources='resources')


def test_unknown_yaml_operator_rejected_at_declaration(tmp_path):
    with pytest.raises(ValueError, match='Unknown standard operator'):
        agent([], agent_config=yaml_agent(tmp_path, operators=('run_python',)))


def test_agent_and_prompt_yaml_contracts_stay_separate(tmp_path):
    import yaml
    from demiflow.operator_llm.errors import PromptDefinitionError, PromptPackError
    path = yaml_agent(tmp_path, max_turns=6)
    with pytest.raises(PromptPackError):
        parse_prompt_pack(path.read_text())
    original_prompt = parse_prompt_pack(PACK).prompt_definitions['enrich']
    environment = OperatorEnvironment.from_yaml(path, resources='resources')
    assert environment.max_turns == 6
    assert environment.prompt(original_prompt).template.source.count(original_prompt.template.source) == 1
    legacy = tmp_path / 'prompts.yaml'
    legacy.write_text(PACK)
    with pytest.raises(ValueError, match='demiflow_agent_v1'):
        OperatorEnvironment.from_yaml(legacy, resources='resources')
    # The closed prompt contract must continue rejecting an agent field.
    value = yaml.safe_load(PACK)
    value['prompts']['enrich']['agent'] = {'operators': ['read_documents']}
    with pytest.raises(PromptDefinitionError, match='unsupported field: agent'):
        parse_prompt_pack(yaml.safe_dump(value))
    path.write_bytes(b'x' * (64 * 1024 + 1))
    with pytest.raises(ValueError, match='64 KiB'):
        OperatorEnvironment.from_yaml(path, resources='resources')


def test_two_turn_native_read_and_replay(server, resource, tmp_path):
    def respond(body, index):
        if index == 0:
            return {'api_calls': [api_call(resource)], 'response': None}
        sent = json.dumps(body['messages'])
        assert 'Full qualifying context.' in sent and 'readings' in sent
        return {'api_calls': [], 'response': {'result': 'based on full context'}}
    server['respond'] = respond
    for replay in (False, True):
        stream = agent([{'item': 'question', 'resources': {'D1': resource}}],
                       options={'journal_dir': str(tmp_path / 'calls')}, max_requests=2)
        assert len(stream._plan.operations) == 1 and isinstance(stream._plan.operations[0], AgentMapOp)
        row = collect(stream)[0]
        assert 'error' not in row, row.get('error')
        assert row['answer'] == 'based on full context'
        assert row['call']['reused'] is replay
        metrics = stream._stages[-1].call_metrics.summary()
        assert metrics['call_records'] == 2 and metrics['reused'] == (2 if replay else 0)
        assert metrics['input_tokens'] == (0 if replay else 20)
        trace = row['call']['environment']
        assert len(trace['turns']) == 2 and len(trace['observations']) == 1
        assert any(
            m['text'] == 'Full qualifying context.' for m in trace['observations'][0]['result']['materials'])
    assert len(server['requests']) == 2



@pytest.mark.parametrize('reuse_workers', [0, 2])
@pytest.mark.parametrize('max_tasks', [1, 2048])
def test_dataset_and_direct_read_share_contract(resource, reuse_workers, max_tasks):
    prompt = parse_prompt_pack(PACK).prompt_definitions['enrich']
    context = PromptContext(prompt, CharacterBudget(60000), lambda row, reading: {'payload': reading})
    request = api_call(resource)['arguments']['request']
    direct = asyncio.run(read_documents(request, context=context))
    outer = collect(data.from_items([{'request': request}]).read_documents(
        request='request', output='reading', context=context, reuse_workers=reuse_workers,
        reuse_worker_max_tasks=max_tasks))[0]['reading']
    assert outer == direct
    assert direct['status'] == 'ok' and direct['material_chars'] > 0 and direct['prompt_chars'] > 0
    tiny = asyncio.run(read_documents({**request, 'new_chars': 1, 'total_chars': 1}, context=context))
    assert tiny['materials'] == [] and tiny['readings'][0]['unread_ranges']



@pytest.mark.parametrize('kind', ['unknown', 'foreign'])
def test_out_of_scope_reference_is_observation_not_io(server, resource, monkeypatch, kind):
    import demiflow.collect.reading as module
    async def denied(*args, **kwargs):
        pytest.fail('out-of-scope document must not reach reader')
    monkeypatch.setattr(module, 'read_documents', denied)
    foreign = {**resource, 'document_ref': {'uri': 'file:///not-allowed', 'sha256': 'a' * 64}}
    call = api_call({'$ref': 'other_row'} if kind == 'unknown' else foreign)
    server['respond'] = lambda body, index: ({'api_calls': [call], 'response': None} if index == 0
        else {'api_calls': [], 'response': {'result': 'cannot verify'}})
    row = collect(agent([{'item': 'question', 'resources': {'D1': resource}}]))[0]
    assert row['answer'] == 'cannot verify'
    assert row['call']['environment']['observations'][0]['result']['code'] == 'invalid_arguments'



@pytest.mark.parametrize('limit', ['turns', 'requests', 'context', 'observation'])
def test_budgets_fail_without_fabricated_final(server, resource, limit):
    declaration = OperatorEnvironment(resources='resources')
    changes = {'turns': {'max_turns': 1}, 'context': {'max_context_chars': 1},
               'observation': {'max_observation_chars': 1}, 'requests': {}}[limit]
    server['respond'] = lambda body, index: {'api_calls': [api_call(resource)], 'response': None}
    row = collect(agent([{'item': 'question', 'resources': {'D1': resource}}],
        environment=replace(declaration, **changes), max_requests=1 if limit == 'requests' else 4))[0]
    assert 'answer' not in row and 'error' in row
    assert 'budget' in row['error']['detail'].lower() or 'exceed' in row['error']['detail'].lower()
    assert len(server['requests']) == (0 if limit == 'context' else 1)



def test_corrupt_document_and_byte_cap_are_visible(resource):
    from pathlib import Path
    from urllib.parse import urlsplit, unquote
    prompt = parse_prompt_pack(PACK).prompt_definitions['enrich']
    context = PromptContext(prompt, CharacterBudget(60000), lambda row, reading: {'payload': reading})
    scope = OperatorEnvironment(resources='resources', max_document_bytes=1).bind({'D1': resource})
    result = asyncio.run(scope.invoke(api_call(resource), context=context))
    assert result['readings'][0]['status'] == 'read_failed' and result['materials'] == []
    Path(unquote(urlsplit(resource['document_ref']['uri']).path)).write_text('corrupt')
    scope = OperatorEnvironment(resources='resources').bind({'D1': resource})
    result = asyncio.run(scope.invoke(api_call(resource), context=context))
    assert result['readings'][0]['status'] == 'read_failed' and result['materials'] == []



def test_concurrent_rows_do_not_share_history(server, resource):
    def respond(body, index):
        sent = json.dumps(body['messages'])
        if 'Full qualifying context.' not in sent:
            return {'api_calls': [api_call(resource)], 'response': None}
        return {'api_calls': [], 'response': {'result': 'A' if 'row_A' in sent else 'B'}}
    server['respond'] = respond
    rows = collect(agent([{'item': 'row_' + name, 'resources': {'D1': resource}} for name in ('A', 'B')],
                         concurrency=2, max_requests=4))
    assert sorted(r['answer'] for r in rows) == ['A', 'B']
    assert all(len(r['call']['environment']['turns']) == 2 for r in rows)
    assert all(not ('row_A' in json.dumps(r['body']) and 'row_B' in json.dumps(r['body']))
               for r in server['requests'])



def test_unsupported_native_tools_rejected_at_declaration():
    with pytest.raises(ValueError, match='runtime settings'):
        agent([], options={'codex_exec': {}})
    with pytest.raises(ValueError, match='environment operators'):
        agent([], options={'request_options': {'tools': [{'type': 'web_search'}]}})


def test_cancel_during_operator_is_propagated_and_client_closes(resource, monkeypatch):
    import demiflow.collect.reading as module
    from demiflow.operator_llm import runtime
    entered, drained, closed = [], [], []
    class Client:
        async def prepare_lookup(self, request):
            return None
        async def execute(self, request):
            raise AssertionError('use runtime stub below')
        async def aclose(self):
            closed.append(True)
    async def turn(self, *args, **kwargs):
        self._clients['fixture'] = Client()
        return {'api_calls': [api_call(resource)], 'response': None}, {}
    async def read(*args, **kwargs):
        entered.append(True)
        try:
            await asyncio.Event().wait()
        finally:
            drained.append(True)
    monkeypatch.setattr(runtime.AsyncOperatorLLMRuntime, 'call_with_trace', turn)
    monkeypatch.setattr(module, 'read_documents', read)
    async def run():
        actor = agent([])._stages[-1]
        task = asyncio.create_task(actor({'item': 'question', 'resources': {'D1': resource}}))
        while not entered:
            await asyncio.sleep(0)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
        await actor.aclose()
    asyncio.run(run())
    assert entered and drained and closed



@pytest.mark.parametrize('bad_call,code', [
    ({'method': 'read_documents', 'arguments': {'document_ids': ['D1'], 'query': 'q'}}, 'invalid_arguments'),
    ({'method': 'read_documents', 'arguments': {}}, 'invalid_arguments'),
    ({'method': 'read_documents', 'arguments': {'request': {'documents': 'wrong'}}}, 'invalid_arguments'),
    ({'method': 'read_documents', 'arguments': {'request': {}, 'context': {}}}, 'invalid_arguments'),
    ({'method': 'read_documents', 'arguments': {'request': {}, 'max_bytes': 999}}, 'invalid_arguments'),
    ({'method': 'run_python', 'arguments': {'code': 'pass'}}, 'operator_not_allowed'),
    ({'method': 'read_documents'}, 'invalid_arguments'),
    (None, 'invalid_arguments'),
])
def test_model_repairs_call_before_native_execution(server, resource, monkeypatch, bad_call, code):
    import demiflow.collect.reading as module
    native = module.read_documents
    invoked = []
    async def tracked(request, **kwargs):
        invoked.append(request)
        return await native(request, **kwargs)
    monkeypatch.setattr(module, 'read_documents', tracked)
    def respond(body, index):
        sent = json.dumps(body['messages'])
        if index == 0:
            schema = body['response_format']['json_schema']['schema'] if 'response_format' in body else None
            if schema:
                assert 'document_ids' not in json.dumps(schema) and 'new_chars' in json.dumps(schema)
            return {'api_calls': [bad_call], 'response': None}
        if index == 1:
            assert not invoked and code in sent
            return {'api_calls': [api_call(resource)], 'response': None}
        assert len(invoked) == 1 and 'Full qualifying context.' in sent
        return {'api_calls': [], 'response': {'result': 'verified'}}
    server['respond'] = respond
    row = collect(agent([{'item': 'question', 'resources': {'D1': resource}}], max_requests=3))[0]
    assert row['answer'] == 'verified'
    assert len(row['call']['environment']['turns']) == 3
    assert row['call']['environment']['observations'][0]['result']['code'] == code
    assert invoked[0]['questions'][0]['text'] == 'qualifying'



@pytest.mark.parametrize('invalid,code', [
    ('not an object', 'invalid_response'),
    ({'response': {'result': 'missing envelope'}}, 'invalid_response'),
    ({'api_calls': [api_call()], 'response': {'result': 'conflict'}}, 'invalid_response'),
    ({'api_calls': [], 'response': None}, 'invalid_response'),
    ({'api_calls': [], 'response': {'result': 123}}, 'invalid_final_response'),
])
def test_invalid_response_repair_uses_turn_budget_and_replays(server, tmp_path, invalid, code):
    def respond(body, index):
        if index == 0:
            return invalid
        assert code in json.dumps(body['messages'])
        return {'api_calls': [], 'response': {'result': 'repaired'}}
    server['respond'] = respond
    options = {'journal_dir': str(tmp_path / 'calls')}
    for replay in (False, True):
        stream = agent([{'item': 'question', 'resources': {}}],
            pack=PACK.replace('schema_retries: 0', 'schema_retries: 1'),
            environment=OperatorEnvironment(resources='resources', max_turns=2),
            options=options, max_requests=0 if replay else 2)
        row = collect(stream)[0]
        assert row['answer'] == 'repaired', row
        trace = row['call']['environment']
        assert len(trace['turns']) == 2 and len(trace['observations']) == 1
        assert trace['observations'][0]['result']['code'] == code
        assert row['call']['reused'] is replay
        metrics = stream._stages[-1].call_metrics.summary()
        assert metrics['call_records'] == 2
        assert metrics['input_tokens'] == (0 if replay else 20)
    assert len(server['requests']) == 2


@pytest.mark.parametrize('invalid', [
    {'api_calls': [], 'response': {'result': 123}}, 'malformed',
])
def test_repeated_invalid_responses_stop_at_turn_limit(server, invalid):
    server['respond'] = lambda body, index: invalid
    row = collect(agent([{'item': 'question', 'resources': {}}],
        pack=PACK.replace('schema_retries: 0', 'schema_retries: 1'),
        environment=OperatorEnvironment(resources='resources', max_turns=2)))[0]
    assert row['error']['type'] == 'PromptBudgetExceededError' and 'answer' not in row
    assert len(server['requests']) == 2
    assert len(row['error']['call']['environment']['turns']) == 2
    assert len(row['error']['call']['environment']['observations']) == 2


def test_repairs_share_global_request_limit_across_rows(server):
    server['respond'] = lambda body, index: {'api_calls': [], 'response': {'result': 123}}
    rows = collect(agent([{'item': str(i), 'resources': {}} for i in range(2)],
                         concurrency=2, max_requests=3))
    assert len(server['requests']) == 3 and len(rows) == 2
    assert all(row['error']['type'] == 'PromptBudgetExceededError' and 'answer' not in row for row in rows)


@pytest.mark.parametrize('failure', [OSError('source unavailable'), TimeoutError('read timed out')])
def test_operator_failure_is_feedback(server, resource, monkeypatch, failure):
    import demiflow.collect.reading as module
    async def fail(*args, **kwargs):
        raise failure
    monkeypatch.setattr(module, 'read_documents', fail)
    def respond(body, index):
        if index == 0:
            return {'api_calls': [api_call(resource)], 'response': None}
        assert 'execution_error' in json.dumps(body['messages']) and str(failure) in json.dumps(body['messages'])
        return {'api_calls': [], 'response': {'result': 'cannot verify'}}
    server['respond'] = respond
    row = collect(agent([{'item': 'question', 'resources': {'D1': resource}}]))[0]
    assert row['answer'] == 'cannot verify'



@pytest.mark.parametrize('failure', [RuntimeError('broken internal invariant'), TypeError('implementation bug')])
def test_internal_operator_failure_stops_loop(server, resource, monkeypatch, failure):
    import demiflow.collect.reading as module
    async def fail(*args, **kwargs):
        raise failure
    monkeypatch.setattr(module, 'read_documents', fail)
    server['respond'] = lambda body, index: {'api_calls': [api_call(resource)], 'response': None}
    row = collect(agent([{'item': 'question', 'resources': {'D1': resource}}]))[0]
    assert row['error']['type'] == type(failure).__name__ and len(server['requests']) == 1



def test_response_cap_fails_before_operator_execution(server, resource, monkeypatch):
    import demiflow.collect.reading as module
    async def forbidden(*args, **kwargs):
        pytest.fail('oversized response must not execute')
    monkeypatch.setattr(module, 'read_documents', forbidden)
    server['respond'] = lambda body, index: {'api_calls': [api_call(resource, questions=[{'id': 'q', 'text': 'x' * 200}])], 'response': None}
    row = collect(agent([{'item': 'question', 'resources': {'D1': resource}}],
        environment=OperatorEnvironment(resources='resources', max_response_chars=100)))[0]
    assert row['error']['type'] == 'PromptBudgetExceededError'
    assert len(row['error']['call']['environment']['turns']) == 1 and len(server['requests']) == 1



def test_all_yaml_limits_can_tighten_node_policy(tmp_path):
    import yaml
    from demiflow.environment import LIMITS
    limits = dict(max_turns=2, max_calls_per_turn=1, max_context_chars=10000, max_material_chars=2000,
        max_observation_chars=4000, max_response_chars=1000, max_document_bytes=4096,
        max_resources=2, document_concurrency=1, timeout_s=2.5,
        max_tool_images=2, max_tool_image_bytes=1024, max_tool_image_pixels=1000,
        max_tool_image_total_bytes=2048, max_tool_image_candidates=4)
    path = tmp_path / 'agent.yaml'
    path.write_text(yaml.safe_dump({'schema_version': 'demiflow_agent_v1', 'operators': ['read_documents'], **limits}))
    environment = OperatorEnvironment.from_yaml(path, resources='resources', max_context_chars=9000)
    assert {key: getattr(environment, key) for key in LIMITS} == {**limits, 'max_context_chars': 9000}
    for key in LIMITS:
        path.write_text(yaml.safe_dump({'schema_version': 'demiflow_agent_v1', 'operators': [], key: False}))
        with pytest.raises(ValueError):
            OperatorEnvironment.from_yaml(path, resources='resources')


@pytest.mark.parametrize('malformed', [
    '{invalid JSON', '{"api_calls":[],"response":NaN}',
    '{"api_calls":[],"response":{},"response":{"result":"duplicate"}}',
])
def test_offline_repair_read_final_with_images_and_output_mapping(server, resource, tmp_path, malformed):
    from demiflow.data.api import DataAPI
    from demiflow.operator_llm.call_ref import read_call
    from demiflow.operator_llm.sqlite_offline import submit_response
    picture = 'data:image/png;base64,aGVsbG8='
    pack = PACK.replace('{{ payload | json }}', '{{ payload | json }}\n      {{ pictures | numbered_image }}')
    ctx = DataAPI()
    def run():
        return collect(ctx.from_items([{'item': 'question', 'resources': {'D1': resource}, 'pixels': [picture]}])
            .agentmap_async('enrich', config=configured_agent(pack, OperatorEnvironment(resources='resources')),
                inputs={'payload': 'item', 'pictures': 'pixels'}, outputs={'result': 'verdict'},
                call_output='call', error_output='error', max_requests=0,
                options={'offline_store': {'path': str(tmp_path / 'calls.sqlite')}}))[0]
    responses = [malformed, {'api_calls': [api_call(resource)], 'response': None},
                 {'api_calls': [], 'response': {'result': 'verified'}}]
    for index, response in enumerate(responses):
        row = run()
        assert row['error']['type'] == 'PromptResponsePending', row
        trace = row['error']['call']['environment']
        assert len(trace['turns']) == index + 1
        ref = trace['turns'][-1]['request_ref']
        request = read_call(ref)
        sent = json.dumps(request['messages'])
        assert picture in sent and 'question' in sent
        if index >= 1:
            assert 'invalid_response' in sent
        if index == 2:
            assert 'Full qualifying context.' in sent
        submit_response(None, ref, response, model='mock-model')
    for _ in range(2):
        row = run()
        assert row['verdict'] == 'verified' and 'error' not in row
        assert len(row['call']['environment']['turns']) == 3
    assert not server['requests'] and ctx.prompt_usage()['provider_requests_started'] == 0



def test_accumulated_history_stops_before_next_model_request(server, resource, monkeypatch):
    import demiflow.collect.reading as module
    async def fail(*args, **kwargs):
        raise OSError('x' * 10000)
    monkeypatch.setattr(module, 'read_documents', fail)
    server['respond'] = lambda body, index: {'api_calls': [api_call(resource)], 'response': None}
    row = collect(agent([{'item': 'question', 'resources': {'D1': resource}}],
        environment=OperatorEnvironment(resources='resources', max_context_chars=10000,
                                        max_observation_chars=12000)))[0]
    assert row['error']['type'] == 'PromptBudgetExceededError'
    assert 'max_context_chars' in row['error']['detail'] and len(server['requests']) == 1
    observations = row['error']['call']['environment']['observations']
    assert len(observations) == 1 and observations[0]['result']['message'] == 'x' * 10000



def test_partial_document_failure_remains_visible(server, resource, tmp_path):
    missing = {**resource, 'document_ref': {'uri': (tmp_path / 'missing.json').as_uri(), 'sha256': 'a' * 64}}
    def respond(body, index):
        if index == 0:
            return {'api_calls': [api_call(resource, documents=[resource, missing])], 'response': None}
        sent = json.dumps(body['messages'])
        assert 'read_failed' in sent and 'Full qualifying context.' in sent
        return {'api_calls': [], 'response': {'result': 'only D1 could be read'}}
    server['respond'] = respond
    row = collect(agent([{'item': 'question', 'resources': {'D1': resource, 'D2': missing}}]))[0]
    assert row['answer'] == 'only D1 could be read'
    readings = row['call']['environment']['observations'][0]['result']['readings']
    assert [r['status'] for r in readings] == ['ok', 'read_failed']



def test_model_transport_failure_is_not_an_agent_repair(server):
    server['status'] = 503
    row = collect(agent([{'item': 'question', 'resources': {}}]))[0]
    assert 'answer' not in row and len(server['requests']) == 1
    assert row['error']['call']['http_status'] == 503
    assert row['error']['call']['environment']['observations'] == []


def test_native_request_is_forwarded_without_rewrite_or_default_filling(resource, monkeypatch):
    """Agent invocation and Dataset invocation use the same request value."""
    import demiflow.collect.reading as module
    received = []
    async def native(request, *, context, document_concurrency, timeout_s, max_bytes):
        received.append((request, context, document_concurrency, timeout_s, max_bytes))
        return {'status': 'ok', 'fixture': True}
    monkeypatch.setattr(module, 'read_documents', native)
    request = {'documents': [{**resource, 'bindings': ['custom'], 'eligible': False}],
               'questions': [{'id': 'custom', 'text': 'model supplied question'}],
               'new_chars': 37, 'total_chars': 81}
    env = OperatorEnvironment(resources='resources')
    context = object()
    result = asyncio.run(env.bind({'D1': resource}).invoke(
        {'method': 'read_documents', 'arguments': {'request': request}}, context=context))
    assert result == {'status': 'ok', 'fixture': True}
    assert received[0][0] is request and received[0][1] is context
    assert received[0][2:] == (2, 30, 8 * 1024 * 1024)
    assert 'requests' not in request and 'retained' not in request
    assert request['documents'][0]['bindings'] == ['custom']
    assert request['documents'][0]['eligible'] is False
    assert request['new_chars'] == 37 and request['total_chars'] == 81
