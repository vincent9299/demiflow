"""Codex agent callbacks through an actual, isolated stdio subprocess (no model)."""
import asyncio
from dataclasses import replace
import json
import os
from pathlib import Path
import sys

import pytest

from demiflow import data
from demiflow.environment import OperatorEnvironment
from demiflow.operator_llm.codex_agent import validate_options
from demiflow.operator_llm.parser import parse_prompt_pack
from demiflow.operator_llm.sqlite_journal import SQLitePromptJournal
from test_agentmap_async import resource, collect, configured_agent
from test_map_prompt_async import PACK


@pytest.fixture
def app_server(tmp_path, monkeypatch):
    path = tmp_path / 'fake-codex-agent'
    path.write_text('#!' + sys.executable + '\n' + r'''
import json, os, pathlib, sys, time
capture = pathlib.Path(os.environ['DF_TEST_CODEX_CAPTURE'])
mode = os.environ.get('DF_TEST_CODEX_MODE', 'read')
thread, turn = 'thread-' + str(os.getpid()), 'turn-' + str(os.getpid())
def record(value):
    with capture.open('a') as f:
        f.write(json.dumps({'pid': os.getpid(), **value}) + '\n')
def send(value):
    print(json.dumps(value), flush=True)
def receive():
    value = json.loads(sys.stdin.readline()); record({'received': value}); return value
def notify(method, params):
    send({'method': method, 'params': {'threadId': thread, **params}})
def call(arguments, tool='read_documents', **kw):
    send({'id': 'callback-' + str(time.monotonic()), 'method': 'item/tool/call',
          'params': {'threadId': thread, 'turnId': turn, 'callId': 'call', 'tool': tool, 'arguments': arguments, **kw}})
    response = receive()
    assert 'result' in response, response
    return response['result']
record({'cwd': os.getcwd(), 'args': sys.argv[1:]})
request = receive(); assert request['method'] == 'initialize'
assert request['params']['capabilities']['experimentalApi'] is True
send({'id': request['id'], 'result': {'userAgent': 'fixture'}})
assert receive()['method'] == 'initialized'
request = receive(); assert request['method'] == 'thread/start'
params = request['params']; assert params['ephemeral']
assert params['sandbox'] == ('workspace-write' if mode == 'artifact' else 'read-only')
assert params['approvalPolicy'] == 'never'
assert 'baseInstructions' not in params and 'developerInstructions' not in params
for tool in params['dynamicTools']:
    assert tool['type'] == 'function'
    if mode != 'generic':
        assert tool['name'] == 'read_documents'
        assert set(tool['inputSchema']['properties']) == {'request'}
send({'id': request['id'], 'result': {'thread': {'id': thread},
      'model': 'wrong' if mode == 'wrong_model' else params['model']}})
request = receive(); assert request['method'] == 'turn/start'
assert request['params']['outputSchema']['properties']['result']['type'] == 'string'
notify('turn/started', {'turn': {'id': turn, 'status': 'inProgress', 'items': []}})
if mode != 'early':
    send({'id': request['id'], 'result': {'turn': {'id': turn, 'status': 'inProgress', 'items': []}}})
source = '\n'.join(p.get('text', '') for p in request['params']['input'])
state = json.JSONDecoder().raw_decode(source.rsplit('当前执行环境：\n', 1)[1])[0] if '当前执行环境：\n' in source else {'resources': {}}
args = {'request': {'documents': list(state['resources'].values())[:1],
                   'questions': [{'id': 'q', 'text': 'qualifying'}],
                   'new_chars': 6000, 'total_chars': 6000}}
if mode == 'disconnect_write' and 'row_A' in source:
    os.close(0)
    send({'id': 'broken-callback', 'method': 'item/tool/call',
          'params': {'threadId': thread, 'turnId': turn, 'callId': 'call', 'tool': 'read_documents', 'arguments': args}})
    time.sleep(20)
elif mode == 'sleep':
    time.sleep(20)
elif mode == 'stderr':
    sys.stderr.write('x' * 16384); sys.stderr.flush(); time.sleep(20)
elif mode == 'wide':
    sys.stdout.write('x' * 65536 + '\n'); sys.stdout.flush(); time.sleep(20)
elif mode == 'large_event':
    notify('ignored', {'text': 'x' * (5 * 1024**2)})
elif mode == 'events':
    for i in range(100): notify('ignored', {'sequence': i})
elif mode == 'unsupported':
    send({'id': 'approval', 'method': 'item/commandExecution/requestApproval', 'params': {}})
    assert receive()['error']['code'] == -32601
    time.sleep(20)
elif mode == 'foreign':
    call(args, threadId='another-row')
elif mode == 'over_calls':
    for i in range(6): call(args)
elif mode == 'namespace':
    assert not call(args, namespace='unknown')['success']
    assert call(args)['success']
elif mode == 'disabled':
    result = call(args)
    assert not result['success']
elif mode == 'repair':
    bad = json.loads(os.environ['DF_TEST_CODEX_BAD'])
    result = call(bad.get('arguments'), bad.get('method', 'read_documents'))
    assert result['success'] is False
    assert json.loads(result['contentItems'][0]['text'])['result']['status'] == 'error'
    result = call(args)
    assert result['success'] is True
elif mode == 'generic':
    for invocation in json.loads(os.environ['DF_TEST_CODEX_CALLS']):
        result = call(invocation['arguments'], invocation['method'])
        record({'tool_response': result})
        for part in result['contentItems']:
            if part['type'] == 'inputImage':
                import base64
                from PIL import Image
                import io
                raw = base64.b64decode(part['imageUrl'].split(',', 1)[1])
                with Image.open(io.BytesIO(raw)) as pixels:
                    assert pixels.size == (8, 6) and pixels.getpixel((0, 0)) == (255, 0, 0)
elif mode in ('read', 'early', 'echo'):
    result = call(args)
    assert result['success'] is True
    result = json.loads(result['contentItems'][0]['text'])
    assert result['environment']['used_operator_calls'] == 1
    assert 'Full qualifying context.' in json.dumps(result)
if mode == 'early':
    send({'id': request['id'], 'result': {'turn': {'id': turn, 'status': 'inProgress', 'items': []}}})
if mode in ('search', 'native'):
    notify('item/started', {'item': {'id': 'search', 'type': 'webSearch', 'query': 'fixture'}})
    notify('item/completed', {'item': {'id': 'search', 'type': 'webSearch', 'query': 'fixture'}})
content = os.environ.get('DF_TEST_CODEX_FINAL', '{"result":"done"}')
if mode == 'artifact':
    assert 'features.image_generation=true' in sys.argv
    assert 'features.view_image=true' in sys.argv and 'features.shell_tool=true' in sys.argv
    context = json.loads(source.rsplit('CODEX_EXEC_FILE_CONTEXT\n', 1)[1])
    directory = pathlib.Path(context['artifact_directory'])
    target = directory / 'source.bin'
    kind = os.environ.get('DF_TEST_CODEX_ARTIFACT', 'file')
    if kind == 'file':
        target.write_bytes(b'generated file bytes')
    elif kind == 'many':
        for name in ('a', 'b'): (directory / name).write_bytes(b'x' * 8)
    elif kind in ('symlink', 'hardlink'):
        other = directory.parent / 'other.bin'
        other.write_bytes(b'outside')
        target.symlink_to(other) if kind == 'symlink' else os.link(other, target)
    elif kind == 'fifo': os.mkfifo(target)
    elif kind == 'nested': target.mkdir()
    elif kind == 'directory_symlink':
        directory.rmdir()
        other = directory.parent / 'other'
        other.mkdir()
        directory.symlink_to(other, target_is_directory=True)
    for image in context['input_images']:
        assert pathlib.Path(image['path']).exists()
    import base64
    attached = [p['url'] for p in request['params']['input'] if p['type'] == 'image']
    assert [pathlib.Path(p['path']).read_bytes() for p in context['input_images']] == [
        base64.b64decode(url.split(',', 1)[1]) for url in attached]
    record({'file_context': context})
if mode == 'native':
    assert params['dynamicTools'] == [] and '当前执行环境：' not in source
    assert 'features.shell_tool=true' in sys.argv
    assert 'api_calls' not in source and 'remaining_operator_calls' not in source
    path = os.environ['DF_TEST_CODEX_DOCUMENT']
    assert path in source
    text = pathlib.Path(path).read_text()
    notify('item/completed', {'item': {'id': 'read', 'type': 'commandExecution', 'aggregatedOutput': text}})
    content = json.dumps({'result': text})
if mode == 'echo':
    source = '\n'.join(p.get('text', '') for p in request['params']['input'])
    content = json.dumps({'result': 'row_A' if 'row_A' in source else 'row_B'})
notify('thread/tokenUsage/updated', {'turnId': turn, 'tokenUsage': {'total': {'inputTokens': 12, 'outputTokens': 3}}})
notify('item/completed', {'item': {'id': 'answer', 'type': 'agentMessage', 'phase': 'final_answer', 'text': content}})
notify('turn/completed', {'turn': {'id': turn, 'status': 'failed' if mode == 'failed' else 'completed', 'items': []}})
time.sleep(20) # Client must stop this process, including on successful completion.
''')
    path.chmod(0o755)
    capture = tmp_path / 'rpc.jsonl'
    monkeypatch.setenv('DF_TEST_CODEX_CAPTURE', str(capture))
    return path, capture


def stream(app_server, resource, tmp_path, *, rows=None, environment=None, settings=None, limit=2,
           timeout=20, concurrency=1, journal=True, **kwargs):
    # Reading starts isolated Python workers. Allow startup on a shared runner;
    # timeout/budget tests below supply their own deliberately small deadline.
    binary, _ = app_server
    options = {'codex_agent': {'bin': str(binary), **(settings or {})}, 'timeout_s': timeout}
    storage = {'sqlite_journal': {'path': str(tmp_path / 'calls.sqlite')}} if journal else {}
    return data.from_items(rows if rows is not None else [{'item': 'question', 'resources': {'D1': resource}}]).agentmap_async(
        'enrich', config=configured_agent(PACK, environment or OperatorEnvironment(resources='resources', runtime='codex'), options),
        inputs={'payload': 'item'}, output='answer',
        call_output='call', error_output='error', concurrency=concurrency, options=storage, max_requests=limit, **kwargs)


def events(app_server):
    return [json.loads(line) for line in app_server[1].read_text().splitlines()]


def assert_stopped(app_server):
    for event in events(app_server):
        if 'cwd' in event:
            assert not Path(event['cwd']).exists()
            with pytest.raises(ProcessLookupError):
                os.kill(event['pid'], 0)


def test_read_callback_same_native_operator_and_replay(app_server, resource, tmp_path, monkeypatch):
    import demiflow.collect.reading as module
    native = module.read_documents
    calls = []
    async def tracked(request, **kwargs):
        calls.append(request)
        return await native(request, **kwargs)
    monkeypatch.setattr(module, 'read_documents', tracked)
    for replay in (False, True):
        ds = stream(app_server, resource, tmp_path, limit=0 if replay else 1)
        row = collect(ds)[0]
        assert 'error' not in row, row.get('error')
        assert row['answer'] == 'done' and row['call']['reused'] is replay
        assert row['call']['budget_unit'] == 'codex_session'
        assert len(row['call']['environment']['observations']) == 1
        assert ds._stages[-1].call_metrics.summary()['input_tokens'] == (0 if replay else 12)
    assert len(calls) == 2 # Replay revalidates documents without a model session.
    assert len([e for e in events(app_server) if e.get('received', {}).get('method') == 'turn/start']) == 1
    assert_stopped(app_server)


@pytest.mark.parametrize('bad', [
    {}, {'arguments': {'document_ids': ['D1']}}, {'arguments': {'document_ids': ['D2'], 'query': 'q'}},
    {'arguments': {'document_ids': ['D1'], 'query': 1}}, {'arguments': 'not-an-object'},
    {'method': 'run_python', 'arguments': {}},
    {'arguments': {'document_ids': ['D1'], 'query': 'q', 'timeout_s': 999}},
])
def test_bad_arguments_reach_codex_for_repair(app_server, resource, tmp_path, monkeypatch, bad):
    monkeypatch.setenv('DF_TEST_CODEX_MODE', 'repair')
    monkeypatch.setenv('DF_TEST_CODEX_BAD', json.dumps(bad))
    row = collect(stream(app_server, resource, tmp_path))[0]
    assert row.get('answer') == 'done', row.get('error')
    observations = row['call']['environment']['observations']
    assert observations[0]['result']['status'] == 'error' and observations[1]['result']['status'] == 'ok'
    starts = [e for e in events(app_server) if e.get('received', {}).get('method') == 'turn/start']
    assert len(starts) == 1  # No demiflow model loop.


@pytest.mark.parametrize('mode', ['early', 'search', 'direct'])
def test_protocol_order_zero_calls_and_native_search(app_server, resource, tmp_path, monkeypatch, mode):
    monkeypatch.setenv('DF_TEST_CODEX_MODE', mode)
    row = collect(stream(app_server, resource, tmp_path, settings={'web_search': 'live'}))[0]
    assert row.get('answer') == 'done', row.get('error')
    assert row['call']['native_searches'] == int(mode == 'search')
    assert len(row['call']['environment']['observations']) == int(mode == 'early')
    args = events(app_server)[0]['args']
    assert 'web_search="live"' in args and 'features.shell_tool=false' in args


def test_native_tools_without_callback_state_or_resource_column(app_server, resource, tmp_path, monkeypatch):
    import demiflow.collect.reading as module
    async def forbidden(*args, **kwargs):
        pytest.fail('Native tools must not call a demiflow operator')
    monkeypatch.setattr(module, 'read_documents', forbidden)
    document = tmp_path / 'native.txt'
    document.write_text('Read with native file tools.')
    monkeypatch.setenv('DF_TEST_CODEX_MODE', 'native')
    monkeypatch.setenv('DF_TEST_CODEX_DOCUMENT', str(document))
    policy = tmp_path / 'native.yaml'
    policy.write_text('schema_version: demiflow_agent_v1\nruntime: codex\noperators: []\n')
    environment = OperatorEnvironment.from_yaml(policy)
    original = parse_prompt_pack(PACK).prompt_definitions['enrich']
    assert environment.prompt(original).template is original.template
    for replay in (False, True):
        row = collect(stream(app_server, resource, tmp_path,
            rows=[{'item': str(document)}], environment=environment,
            settings={'web_search': 'live', 'shell_tool': True}, limit=0 if replay else 1))[0]
        assert row.get('answer') == document.read_text(), row.get('error')
        assert row['call']['environment']['observations'] == []
        assert row['call']['native_searches'] == 1 and row['call']['reused'] is replay
    assert len([e for e in events(app_server) if 'cwd' in e]) == 1
    assert_stopped(app_server)


def test_native_tools_can_coexist_with_operator_callbacks(app_server, resource, tmp_path):
    row = collect(stream(app_server, resource, tmp_path, settings={'shell_tool': True, 'web_search': 'live'}))[0]
    assert row.get('answer') == 'done', row.get('error')
    assert row['call']['environment']['observations'][0]['result']['status'] == 'ok'
    assert 'features.shell_tool=true' in events(app_server)[0]['args']


@pytest.mark.parametrize('value', ['true', 1, None])
def test_native_shell_option_requires_boolean(value):
    with pytest.raises(ValueError, match='shell_tool must be boolean'):
        validate_options({'codex_agent': {'shell_tool': value}})


def test_callbacks_still_require_row_resources():
    with pytest.raises(ValueError, match='resources must name a row column'):
        OperatorEnvironment(runtime='codex')


def test_disabled_message_cap_accepts_event_larger_than_default(app_server, resource, tmp_path, monkeypatch):
    monkeypatch.setenv('DF_TEST_CODEX_MODE', 'large_event')
    row = collect(stream(app_server, resource, tmp_path, settings={'max_message_bytes': None}))[0]
    assert row.get('answer') == 'done', row.get('error')
    assert_stopped(app_server)


@pytest.mark.parametrize('value', [0, -1, True, 'none'])
def test_message_cap_accepts_only_positive_integer_or_null(value):
    with pytest.raises(ValueError, match='max_message_bytes'):
        validate_options({'codex_agent': {'max_message_bytes': value}})


@pytest.mark.parametrize('mode,settings,env_options,timeout', [
    ('over_calls', {}, {'max_operator_calls': 1}, 4),
    ('wide', {'max_message_bytes': 1024}, {}, 4),
    ('wide', {'max_message_bytes': None, 'max_output_bytes': 1024}, {}, 4),
    ('events', {'max_events': 6}, {}, 4),
    ('events', {'max_output_bytes': 1024, 'max_message_bytes': 1024}, {}, 4),
    ('stderr', {'max_stderr_bytes': 32}, {}, 4),
    ('read', {'max_rss_bytes': 1}, {}, 4),
    ('sleep', {}, {}, .3),
    ('read', {}, {'max_observation_chars': 32}, 4),
    ('read', {}, {'max_response_chars': 4}, 4),
])
def test_bounds_stop_process_and_retain_uncertain_journal(app_server, resource, tmp_path, monkeypatch,
                                                         mode, settings, env_options, timeout):
    monkeypatch.setenv('DF_TEST_CODEX_MODE', mode)
    row = collect(stream(app_server, resource, tmp_path, settings=settings, timeout=timeout,
        environment=OperatorEnvironment(resources='resources', runtime='codex', **env_options)))[0]
    assert 'error' in row and 'answer' not in row
    assert row['error']['type'] in ('PromptBudgetExceededError', 'TimeoutError'), row['error']
    journal = SQLitePromptJournal(tmp_path / 'calls.sqlite')
    assert journal.stats()['uncertain'] == 1
    journal.close()
    if app_server[1].exists():
        assert_stopped(app_server)


@pytest.mark.parametrize('mode', ['foreign', 'unsupported', 'failed', 'wrong_model'])
def test_protocol_failures_no_silent_retry(app_server, resource, tmp_path, monkeypatch, mode):
    monkeypatch.setenv('DF_TEST_CODEX_MODE', mode)
    row = collect(stream(app_server, resource, tmp_path))[0]
    assert row['error']['type'] == 'PromptResponseContractError'
    before = len(events(app_server))
    row = collect(stream(app_server, resource, tmp_path))[0]
    assert row['error']['type'] == 'UncertainPromptCall'
    assert len(events(app_server)) == before
    assert_stopped(app_server)


@pytest.mark.parametrize('final', ['{"wrong":true}', '{"result":1}', '{"result":"a","result":"b"}', 'invalid'])
def test_final_validated_against_original_schema(app_server, resource, tmp_path, monkeypatch, final):
    monkeypatch.setenv('DF_TEST_CODEX_FINAL', final)
    row = collect(stream(app_server, resource, tmp_path))[0]
    assert 'error' in row and 'answer' not in row
    assert_stopped(app_server)


def test_concurrent_row_scope_and_callback_history(app_server, resource, tmp_path, monkeypatch):
    monkeypatch.setenv('DF_TEST_CODEX_MODE', 'echo')
    rows = [{'item': name, 'resources': {'D1': resource}} for name in ['row_A', 'row_B']]
    result = collect(stream(app_server, resource, tmp_path, rows=rows, concurrency=2))
    assert sorted(row.get('answer', str(row.get('error'))) for row in result) == ['row_A', 'row_B']
    assert all(len(row['call']['environment']['observations']) == 1 for row in result)
    assert len({e['pid'] for e in events(app_server)}) == 2
    assert_stopped(app_server)


def test_node_and_persistent_session_budget(app_server, resource, tmp_path, monkeypatch):
    monkeypatch.setenv('DF_TEST_CODEX_MODE', 'direct')
    for item in ['A', 'B']:
        rows = [{'item': item, 'resources': {}}]
        row = collect(stream(app_server, resource, tmp_path, rows=rows, limit=1))[0]
        if item == 'A':
            assert row['answer'] == 'done'
        else:
            assert row['error']['type'] == 'PromptBudgetExceededError'
    assert len([e for e in events(app_server) if 'cwd' in e]) == 1


def test_cancel_reaps_process_and_marks_uncertain(app_server, resource, tmp_path, monkeypatch):
    monkeypatch.setenv('DF_TEST_CODEX_MODE', 'sleep')
    actor = stream(app_server, resource, tmp_path)._stages[-1]
    async def run():
        task = asyncio.create_task(actor({'item': 'q', 'resources': {'D1': resource}}))
        try:
            async with asyncio.timeout(3):
                while not app_server[1].exists() or 'turn/start' not in app_server[1].read_text():
                    await asyncio.sleep(.01)
            task.cancel()
            with pytest.raises(asyncio.CancelledError):
                await task
        finally:
            await actor.aclose()
    asyncio.run(run())
    assert_stopped(app_server)
    journal = SQLitePromptJournal(tmp_path / 'calls.sqlite')
    assert journal.stats()['uncertain'] == 1
    journal.close()


def test_yaml_runtime_and_tightened_operator_budget(tmp_path):
    path = tmp_path / 'agent.yaml'
    path.write_text('schema_version: demiflow_agent_v1\nruntime: codex\noperators: [read_documents]\nmax_operator_calls: 3\n')
    env = OperatorEnvironment.from_yaml(path, resources='resources', max_operator_calls=2)
    assert env.runtime == 'codex' and env.max_operator_calls == 2
    prompt = parse_prompt_pack(PACK).prompt_definitions['enrich']
    assert env.prompt(prompt).response_schema == prompt.response_schema
    assert env.prompt(prompt).template.source.count(prompt.template.source) == 1
    with pytest.raises(ValueError, match='match agent YAML'):
        OperatorEnvironment.from_yaml(path, resources='resources', runtime='demiflow')
    path.write_text(path.read_text() + 'max_turns: 4\n')
    with pytest.raises(ValueError, match='not model turn limits'):
        OperatorEnvironment.from_yaml(path, resources='resources')


@pytest.mark.parametrize('options', [
    {'codex_agent': {'unknown': True}}, {'codex_agent': {'web_search': 'yes'}},
    {'codex_agent': {'max_events': True}}, {'codex_agent': {}, 'timeout_s': float('inf')},
    {'codex_agent': {}, 'offline_dir': 'x'}, {'codex_agent': {}, 'request_options': {}},
])
def test_configuration_rejected_without_start(options):
    with pytest.raises(ValueError):
        validate_options(options)


def test_cached_session_revalidates_documents_without_paid_restart(app_server, resource, tmp_path):
    from urllib.parse import urlparse, unquote
    row = collect(stream(app_server, resource, tmp_path))[0]
    assert row['answer'] == 'done'
    before = len(events(app_server))
    path = Path(unquote(urlparse(resource['document_ref']['uri']).path))
    path.write_bytes(b'changed fixed document')
    row = collect(stream(app_server, resource, tmp_path, limit=0))[0]
    assert row['error']['type'] == 'PromptResponseContractError'
    assert 'observations changed' in row['error']['detail']
    assert len(events(app_server)) == before


def test_images_and_business_schema_are_passed_to_codex(app_server, resource, tmp_path, monkeypatch):
    monkeypatch.setenv('DF_TEST_CODEX_MODE', 'direct')
    pack = PACK.replace('{{ payload | json }}', '{{ payload | json }}\n      {{ images | numbered_image }}')
    image = 'data:image/png;base64,aGVsbG8='
    ds = data.from_items([{'item': 'q', 'pixels': [image], 'resources': {}}]).agentmap_async(
        'enrich', config=configured_agent(pack, OperatorEnvironment(resources='resources', runtime='codex'),
            {'codex_agent': {'bin': str(app_server[0])}, 'timeout_s': 4}),
        inputs={'payload': 'item', 'images': 'pixels'}, output='answer', max_requests=1)
    assert collect(ds)[0]['answer'] == 'done'
    turn = next(e['received']['params'] for e in events(app_server)
                if e.get('received', {}).get('method') == 'turn/start')
    assert {'type': 'image', 'url': image} in turn['input']
    assert turn['outputSchema'] == parse_prompt_pack(PACK).prompt_definitions['enrich'].response_schema


def test_initial_input_limits_reject_before_start(app_server, resource, tmp_path):
    row = collect(stream(app_server, resource, tmp_path, settings={'max_input_bytes': 128}))[0]
    assert row['error']['type'] == 'PromptBudgetExceededError'
    assert not app_server[1].exists()
    row = collect(stream(app_server, resource, tmp_path,
        environment=OperatorEnvironment(resources='resources', runtime='codex', max_context_chars=100)))[0]
    assert 'error' in row and not app_server[1].exists()


def test_disabled_operator_returns_error_to_codex(app_server, resource, tmp_path, monkeypatch):
    import demiflow.collect.reading as module
    async def forbidden(*args, **kwargs):
        pytest.fail('disabled native operator executed')
    monkeypatch.setattr(module, 'read_documents', forbidden)
    monkeypatch.setenv('DF_TEST_CODEX_MODE', 'disabled')
    row = collect(stream(app_server, resource, tmp_path,
        environment=OperatorEnvironment(resources='resources', runtime='codex', operators=())))[0]
    assert row.get('answer') == 'done', row.get('error')
    assert row['call']['environment']['observations'][0]['result']['code'] == 'operator_not_allowed'


def test_namespace_error_replays_without_changing_invocation(app_server, resource, tmp_path, monkeypatch):
    monkeypatch.setenv('DF_TEST_CODEX_MODE', 'namespace')
    for reused in (False, True):
        row = collect(stream(app_server, resource, tmp_path))[0]
        assert row.get('answer') == 'done', row.get('error')
        assert row['call']['reused'] is reused
        observed = row['call']['environment']['observations']
        assert observed[0]['namespace'] == 'unknown'
        assert observed[0]['result']['code'] == 'operator_not_allowed'
        assert observed[1]['result']['status'] == 'ok'


def artifact_settings(tmp_path, **kw):
    return {'image_generation': True, 'view_image': True, 'shell_tool': True,
            'artifact_store': {'directory': str(tmp_path / 'objects')}, **kw}


def test_native_artifacts_survive_cleanup_and_replay(app_server, resource, tmp_path, monkeypatch):
    from demiflow.objects import ObjectRef
    monkeypatch.setenv('DF_TEST_CODEX_MODE', 'artifact')
    settings = artifact_settings(tmp_path)
    first = collect(stream(app_server, resource, tmp_path, settings=settings))[0]
    assert first.get('answer') == 'done', first.get('error')
    artifact = first['call']['artifacts'][0]
    assert ObjectRef(**artifact['object_ref']).read() == b'generated file bytes'
    assert first['call']['image_generation'] is True
    assert_stopped(app_server)
    before = len(events(app_server))
    again = collect(stream(app_server, resource, tmp_path, settings=settings, limit=0))[0]
    assert again['call']['reused'] and again['call']['artifacts'] == first['call']['artifacts']
    assert len(events(app_server)) == before
    changed = collect(stream(app_server, resource, tmp_path,
        settings={**settings, 'max_artifact_files': 3}, limit=0))[0]
    assert changed['error']['type'] == 'PromptBudgetExceededError'
    assert len(events(app_server)) == before


@pytest.mark.parametrize('kind,limits', [
    ('symlink', {}), ('hardlink', {}), ('directory_symlink', {}), ('fifo', {}), ('nested', {}),
    ('many', {'max_artifact_files': 1}), ('file', {'max_artifact_bytes': 16}),
    ('many', {'max_artifact_bytes': 10}),
])
def test_artifact_boundaries_are_technical_failures(app_server, resource, tmp_path, monkeypatch, kind, limits):
    monkeypatch.setenv('DF_TEST_CODEX_MODE', 'artifact')
    monkeypatch.setenv('DF_TEST_CODEX_ARTIFACT', kind)
    settings = artifact_settings(tmp_path, **limits)
    row = collect(stream(app_server, resource, tmp_path, settings=settings))[0]
    assert row['error']['type'] == 'PromptResponseContractError'
    call = row['error']['call']
    assert call['execution_status'] == 'artifact_failed' and call['artifact_error']
    assert call['artifacts'] == []
    assert_stopped(app_server)
    before = len(events(app_server))
    again = collect(stream(app_server, resource, tmp_path, settings=settings, limit=0))[0]
    assert again['error']['call']['execution_status'] == 'artifact_failed'
    assert again['error']['call']['reused']
    assert len(events(app_server)) == before


@pytest.mark.parametrize('mutation', ['missing', 'corrupt', 'grown'])
def test_cached_artifact_is_verified_without_new_session(app_server, resource, tmp_path, monkeypatch, mutation):
    from urllib.parse import urlparse, unquote
    monkeypatch.setenv('DF_TEST_CODEX_MODE', 'artifact')
    settings = artifact_settings(tmp_path)
    row = collect(stream(app_server, resource, tmp_path, settings=settings))[0]
    ref = row['call']['artifacts'][0]['object_ref']
    path = Path(unquote(urlparse(ref['uri']).path))
    if mutation == 'missing': path.unlink()
    elif mutation == 'corrupt': path.write_bytes(b'changed file bytes!!')
    else: path.write_bytes(b'x' * 1024)
    before = len(events(app_server))
    again = collect(stream(app_server, resource, tmp_path, settings=settings, limit=0))[0]
    assert again['error']['type'] == 'PromptResponseContractError'
    assert again['error']['call']['reused']
    assert len(events(app_server)) == before


def test_artifact_storage_failure_retains_response(app_server, resource, tmp_path, monkeypatch):
    from demiflow.objects import LocalObjectStore
    monkeypatch.setenv('DF_TEST_CODEX_MODE', 'artifact')
    def fail(*args, **kw):
        raise OSError('fixture disk full')
    monkeypatch.setattr(LocalObjectStore, 'put', fail)
    row = collect(stream(app_server, resource, tmp_path, settings=artifact_settings(tmp_path)))[0]
    assert row['error']['call']['execution_status'] == 'artifact_failed'
    assert 'disk full' in row['error']['call']['artifact_error']
    journal = SQLitePromptJournal(tmp_path / 'calls.sqlite')
    try:
        saved = next(iter(journal.calls()))['response']
        assert saved['content'] == {'result': 'done'} and saved['events']
    finally:
        journal.close()


@pytest.mark.parametrize('settings', [
    {'image_generation': 'true'}, {'view_image': 1}, {'image_generation': True},
    {'artifact_store': {'directory': 'relative'}}, {'max_artifact_bytes': 1},
    {'artifact_store': {'directory': '/tmp/unused'}, 'max_artifact_files': 0},
])
def test_invalid_image_options_fail_before_start(settings):
    with pytest.raises(ValueError):
        validate_options({'codex_agent': settings})


def test_native_image_files_match_attachment_order(app_server, tmp_path, monkeypatch):
    import base64
    monkeypatch.setenv('DF_TEST_CODEX_MODE', 'artifact')
    pack = PACK.replace('{{ payload | json }}', '{{ payload | json }}\n      {{ images | numbered_image }}')
    images = ['data:image/png;base64,' + base64.b64encode(raw).decode() for raw in (b'first', b'second')]
    ds = data.from_items([{'item': 'text material precedes images', 'pixels': images}]).agentmap_async(
        'enrich', config=configured_agent(pack, OperatorEnvironment(runtime='codex', operators=()),
            {'codex_agent': {'bin': str(app_server[0]), **artifact_settings(tmp_path)}, 'timeout_s': 4}),
        inputs={'payload': 'item', 'images': 'pixels'}, output='answer', max_requests=1)
    assert collect(ds)[0]['answer'] == 'done'
    context = next(e['file_context'] for e in events(app_server) if 'file_context' in e)
    assert [i['image_number'] for i in context['input_images']] == [1, 2]
    turn = next(e['received']['params'] for e in events(app_server)
                if e.get('received', {}).get('method') == 'turn/start')
    assert [p['url'] for p in turn['input'] if p['type'] == 'image'] == images
    assert_stopped(app_server)


def test_cancel_during_export_drains_io_before_cleanup(app_server, tmp_path, monkeypatch):
    import threading
    import demiflow.operator_llm.codex_agent as module
    from demiflow.operator_llm.model import OperatorLLMRequest, TextPart
    from demiflow.objects import ObjectRef
    monkeypatch.setenv('DF_TEST_CODEX_MODE', 'artifact')
    started, release = threading.Event(), threading.Event()
    original = module.collect_artifacts
    persisted = []
    def slow_export(directory, record, **kwargs):
        started.set()
        assert release.wait(5)
        assert directory.exists()
        original(directory, record, **kwargs)
        persisted.extend(record['artifacts'])
    monkeypatch.setattr(module, 'collect_artifacts', slow_export)
    definition = parse_prompt_pack(PACK).prompt_definitions['enrich']
    request = OperatorLLMRequest('enrich', 'v1', 'mock-model', (TextPart('task'),),
                                response_schema=definition.response_schema)
    async def run():
        client = module.CodexAgentClient({'codex_agent': {'bin': str(app_server[0]), **artifact_settings(tmp_path)},
            'timeout_s': 4, 'sqlite_journal': {'path': str(tmp_path / 'calls.sqlite')}},
            OperatorEnvironment(runtime='codex', operators=()).bind({}), definition, {}, 1)
        client.prepare(request)
        task = asyncio.create_task(client.execute(request))
        try:
            for _ in range(400):
                if started.is_set(): break
                await asyncio.sleep(.01)
            assert started.is_set()
            work = Path(next(e['cwd'] for e in events(app_server) if 'cwd' in e))
            task.cancel()
            await asyncio.sleep(.03)
            assert not task.done() and work.exists()
        finally:
            release.set()
        try:
            with pytest.raises(asyncio.CancelledError):
                await task
            assert not work.exists()
        finally:
            await client.aclose()
    asyncio.run(run())
    assert ObjectRef(**persisted[0]['object_ref']).read() == b'generated file bytes'
    assert_stopped(app_server)


def test_disabled_image_defaults_preserve_text_session_cache_identity(app_server, resource, tmp_path, monkeypatch):
    monkeypatch.setenv('DF_TEST_CODEX_MODE', 'direct')
    first = collect(stream(app_server, resource, tmp_path))[0]
    assert first['answer'] == 'done'
    journal = SQLitePromptJournal(tmp_path / 'calls.sqlite')
    try:
        saved = next(iter(journal.calls()))['request']
        assert 'view_image' not in saved['settings'] and 'image_generation' not in saved['settings']
    finally:
        journal.close()
    before = len(events(app_server))
    same = collect(stream(app_server, resource, tmp_path,
        settings={'view_image': False, 'image_generation': False}, limit=0))[0]
    assert same['call']['reused'] and same['answer'] == 'done'
    changed = collect(stream(app_server, resource, tmp_path, settings={'view_image': True}, limit=0))[0]
    assert changed['error']['type'] == 'PromptBudgetExceededError'
    assert len(events(app_server)) == before


def test_broken_callback_pipe_is_row_failure_and_other_rows_finish(app_server, resource, tmp_path, monkeypatch):
    monkeypatch.setenv('DF_TEST_CODEX_MODE', 'disconnect_write')
    rows = [{'item': name, 'resources': {'D1': resource}} for name in ('row_A', 'row_B')]
    results = {row['item']: row for row in collect(stream(app_server, resource, tmp_path, rows=rows, concurrency=2))}
    assert results['row_A']['error']['type'] == 'PromptResponseContractError'
    assert 'connection lost while sending' in results['row_A']['error']['detail']
    assert 'answer' in results['row_B'] and 'error' not in results['row_B']
    before = len(events(app_server))
    again = {row['item']: row for row in collect(stream(app_server, resource, tmp_path, rows=rows, concurrency=2))}
    assert again['row_A']['error']['type'] == 'UncertainPromptCall'
    assert again['row_B']['call']['reused'] is True
    assert len(events(app_server)) == before
    assert_stopped(app_server)
