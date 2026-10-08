"""Arbitrary native API dispatch and field-declared images; no model service."""
import asyncio
import io
import json
from pathlib import Path

import pytest
from PIL import Image

from demiflow import data
from demiflow.environment import OperatorEnvironment
from demiflow.operator_api import catalog_identity
from operators import fixtures
from demiflow.objects import LocalObjectStore
from test_agentmap_async import configured_agent, collect, server
from test_codex_agent import app_server, events, assert_stopped
from test_map_prompt_async import PACK


def schema(fields, required=()):
    return {'type': 'object', 'properties': fields, 'required': list(required), 'additionalProperties': False}


@pytest.fixture
def APIs(tmp_path, monkeypatch):
    monkeypatch.setattr(fixtures, 'calls', [])
    raw = io.BytesIO()
    Image.new('RGB', (8, 6), 'red').save(raw, format='PNG')
    ref = LocalObjectStore(tmp_path / 'images').put(raw.getvalue()).to_dict()
    declared = {
        'fixture_scale': {'fn': 'operators.fixtures:scale',
            'arguments': schema({'value': {'type': 'integer'}}, ['value']),
            'description': 'Native scale API.',
            'fixed_schema': schema({'multiplier': {'type': 'integer'}}, ['multiplier']), 'replay': 'verify'},
        'fixture_pictures': {'fn': 'operators.fixtures:pictures',
            'arguments': schema({'query': {'type': 'string', 'maxLength': 256}}, ['query']),
            'description': 'Native candidate API.',
            'fixed_schema': schema({'source': schema({'uri': {'type': 'string'}, 'sha256': {'type': 'string'}},
                                                    ['uri', 'sha256'])}, ['source']), 'replay': 'verify'}}
    settings = {'fixture_scale': {'arguments': {'multiplier': 3}}, 'fixture_pictures': {
        'arguments': {'source': ref}, 'result_images': {'items_field': 'candidates', 'uri_field': 'uri', 'sha256_field': 'sha'}}}
    return fixtures.calls, settings, ref, declared


def declaration(APIs, runtime, **kw):
    return OperatorEnvironment(runtime=runtime, operators=APIs[3],
                               operator_settings=APIs[1], **kw)


def node(environment, options=None, storage=None):
    return data.from_items([{'q': 'author'}]).agentmap_async('enrich',
        config=configured_agent(PACK, environment, options, max_requests=4),
        inputs={'payload': 'q'}, output='answer', error_output='error', call_output='call', options=storage)


def test_http_dispatches_two_native_apis_and_attaches_images(APIs, server):
    calls = [{'method': 'fixture_scale', 'arguments': {'value': 7}},
             {'method': 'fixture_pictures', 'arguments': {'query': 'red object'}}]
    def respond(body, index):
        if index == 0:
            text = json.dumps(body['messages'])
            assert 'fixture_scale' in text and 'read_documents' not in text
            return {'api_calls': calls, 'response': None}
        parts = [p for m in body['messages'] for p in m['content'] if isinstance(p, dict)]
        assert len([p for p in parts if p.get('type') == 'image_url']) == 1
        return {'api_calls': [], 'response': {'result': 'done'}}
    server['respond'] = respond
    row = collect(node(declaration(APIs, 'demiflow')))[0]
    assert row.get('answer') == 'done', row.get('error')
    observations = row['call']['environment']['observations']
    assert [o['call'] for o in observations] == calls
    assert observations[0]['result'] == [21]
    assert observations[1]['result']['candidates'][0]['_distance'] == .25
    assert observations[1]['images'][0]['status'] == 'attached'
    assert len(APIs[0]) == 2


def test_codex_generic_calls_receive_actual_image_blocks(APIs, app_server, monkeypatch):
    monkeypatch.setenv('DF_TEST_CODEX_MODE', 'generic')
    calls = [{'method': 'fixture_scale', 'arguments': {'value': 7}},
             {'method': 'fixture_pictures', 'arguments': {'query': 'red object'}}]
    monkeypatch.setenv('DF_TEST_CODEX_CALLS', json.dumps(calls))
    row = collect(node(declaration(APIs, 'codex'), {'codex_agent': {'bin': str(app_server[0])}, 'timeout_s': 4}))[0]
    assert row.get('answer') == 'done', row.get('error')
    responses = [e['tool_response'] for e in events(app_server) if 'tool_response' in e]
    assert json.loads(responses[0]['contentItems'][0]['text'])['result'] == [21]
    assert [p['type'] for p in responses[1]['contentItems']] == ['inputText', 'inputText', 'inputImage']
    receipt = row['call']['environment']['observations'][1]['images'][0]
    assert receipt['object_ref'] == APIs[2] and receipt['status'] == 'attached'
    assert 'base64' not in json.dumps(row['call'])
    assert_stopped(app_server)


def test_image_budget_and_read_failure_remain_visible(APIs, app_server, monkeypatch):
    monkeypatch.setenv('DF_TEST_CODEX_MODE', 'generic')
    monkeypatch.setenv('DF_TEST_CODEX_CALLS', json.dumps([
        {'method': 'fixture_pictures', 'arguments': {'query': 'one'}},
        {'method': 'fixture_pictures', 'arguments': {'query': 'again'}}]))
    row = collect(node(declaration(APIs, 'codex', max_tool_images=1),
        {'codex_agent': {'bin': str(app_server[0])}, 'timeout_s': 4}))[0]
    observations = row['call']['environment']['observations']
    first, second = (o['images'][0] for o in observations)
    assert first['image_id'] == second['image_id']
    assert [first['status'], second['status']] == ['attached', 'not_attached_budget']
    assert len(APIs[0]) == 2 and len(observations[1]['result']['candidates']) == 1


def test_fixed_parameters_cannot_be_overridden(APIs):
    env = declaration(APIs, 'codex')
    async def invoke():
        with pytest.raises(ValueError, match='multiplier'):
            await env.bind({}).invoke({'method': 'fixture_scale', 'arguments': {'value': 2, 'multiplier': 999}}, context=None)
    asyncio.run(invoke())
    assert not APIs[0]


def test_catalog_identity_and_config_validation(APIs):
    env = declaration(APIs, 'codex')
    assert {t['name'] for t in env.tool_definitions()} == {'fixture_scale', 'fixture_pictures'}
    assert len(__import__('json').loads(catalog_identity(env._definitions))) == 2
    with pytest.raises(ValueError, match='Unknown standard'):
        OperatorEnvironment(operators=('not_a_native_api',))
    with pytest.raises(ValueError, match='fixed arguments'):
        OperatorEnvironment(operators={'fixture_scale': APIs[3]['fixture_scale']})
    with pytest.raises(ValueError, match='selected APIs'):
        OperatorEnvironment(operators=(), operator_settings={'fixture_scale': {}})
    with pytest.raises(ValueError, match='result_images'):
        OperatorEnvironment(runtime='codex', operators={'fixture_scale': APIs[3]['fixture_scale']},
            operator_settings={'fixture_scale': {'arguments': {'multiplier': 2}, 'result_images': {'uri_field': 'uri'}}})


def test_bad_api_arguments_are_returned_for_repair(APIs, app_server, monkeypatch):
    monkeypatch.setenv('DF_TEST_CODEX_MODE', 'generic')
    monkeypatch.setenv('DF_TEST_CODEX_CALLS', json.dumps([
        {'method': 'fixture_scale', 'arguments': {'value': 'wrong'}},
        {'method': 'fixture_scale', 'arguments': {'value': 2}},
        {'method': 'not_exposed', 'arguments': {}}]))
    row = collect(node(declaration(APIs, 'codex'), {'codex_agent': {'bin': str(app_server[0])}, 'timeout_s': 4}))[0]
    assert row.get('answer') == 'done', row.get('error')
    observed = row['call']['environment']['observations']
    assert observed[0]['result']['code'] == 'invalid_arguments'
    assert observed[1]['result'] == [6]
    assert observed[2]['result']['code'] == 'operator_not_allowed'
    assert APIs[0] == [('scale', 2, 3)]


@pytest.mark.parametrize('limit', ['bytes', 'pixels', 'candidates', 'corrupt'])
def test_image_admission_limits_and_bad_bytes(APIs, app_server, monkeypatch, limit):
    from urllib.parse import urlsplit, unquote
    limits = {'bytes': {'max_tool_image_bytes': 8}, 'pixels': {'max_tool_image_pixels': 4},
              'candidates': {'max_tool_image_candidates': 1}, 'corrupt': {}}[limit]
    if limit == 'corrupt':
        Path(unquote(urlsplit(APIs[2]['uri']).path)).write_bytes(b'changed')
    monkeypatch.setenv('DF_TEST_CODEX_MODE', 'generic')
    monkeypatch.setenv('DF_TEST_CODEX_CALLS', json.dumps([{'method': 'fixture_pictures',
        'arguments': {'query': 'pair' if limit == 'candidates' else 'one'}}]))
    row = collect(node(declaration(APIs, 'codex', **limits),
        {'codex_agent': {'bin': str(app_server[0])}, 'timeout_s': 4}))[0]
    if limit == 'candidates':
        assert row['error']['type'] == 'PromptBudgetExceededError'
        assert_stopped(app_server)
        return
    assert row.get('answer') == 'done', row.get('error')
    observation = row['call']['environment']['observations'][0]
    assert observation['images'][0]['status'] == 'unavailable'
    assert len(observation['result']['candidates']) == 1
    response = next(e['tool_response'] for e in events(app_server) if 'tool_response' in e)
    assert not any(p['type'] == 'inputImage' for p in response['contentItems'])


def test_recorded_api_is_not_reexecuted_and_cached_pixels_are_verified(APIs, app_server, monkeypatch, tmp_path):
    from urllib.parse import urlsplit, unquote
    # A paid/stateful native API explicitly requests recorded replay. Its
    # implementation is called once, while actual returned files are verified.
    APIs[3]['fixture_pictures']['replay'] = 'recorded'
    monkeypatch.setenv('DF_TEST_CODEX_MODE', 'generic')
    monkeypatch.setenv('DF_TEST_CODEX_CALLS', json.dumps([{'method': 'fixture_pictures', 'arguments': {'query': 'one'}}]))
    env = declaration(APIs, 'codex')
    options = {'codex_agent': {'bin': str(app_server[0])}, 'timeout_s': 4}
    storage = {'sqlite_journal': {'path': str(tmp_path / 'api-calls.sqlite')}}
    first = collect(node(env, options, storage))[0]
    assert first.get('answer') == 'done', first.get('error')
    before = len(events(app_server))
    cached = collect(node(env, options, storage))[0]
    assert cached['call']['reused'] and len(APIs[0]) == 1
    assert len(events(app_server)) == before
    Path(unquote(urlsplit(APIs[2]['uri']).path)).unlink()
    failed = collect(node(env, options, storage))[0]
    assert 'error' in failed and failed['error']['call']['reused']
    assert len(APIs[0]) == 1 and len(events(app_server)) == before
    with pytest.raises(ValueError, match='Recorded-only'):
        declaration(APIs, 'demiflow')


def test_single_agent_yaml_exposes_functions_without_registration(APIs, tmp_path, server):
    import yaml
    from demiflow.agent import load_agent_config
    task = yaml.safe_load(PACK)['prompts']['enrich']
    model = task.pop('model')
    path = tmp_path / 'agent.yaml'
    path.write_text(yaml.safe_dump({'schema_version': 'demiflow_agent_v2', 'runtime': 'demiflow',
        'model': model, 'tasks': {'enrich': task}, 'operators': {'fixture_scale': {**APIs[3]['fixture_scale'], 'fixed_arguments': {'multiplier': 3}}}, 'budgets': {'max_requests': 2}}))
    server['respond'] = lambda body, index: ({'api_calls': [{'method': 'fixture_scale', 'arguments': {'value': 4}}],
        'response': None} if not index else {'api_calls': [], 'response': {'result': 'done'}})
    cfg = load_agent_config(path)
    row = collect(data.from_items([{'q': 'test'}]).agentmap_async('enrich', config=cfg,
        inputs={'payload': 'q'}, output='answer', call_output='call'))[0]
    assert row['answer'] == 'done' and row['call']['environment']['observations'][0]['result'] == [12]


def test_thread_api_cancellation_drains_native_work(APIs):
    import threading
    entered, release = threading.Event(), threading.Event()
    fixtures.entered, fixtures.release = entered, release
    operators = {'fixture_blocking': {'fn': 'operators.fixtures:blocking',
        'arguments': schema({'value': {'type': 'integer'}}, ['value']),
        'description': 'Bounded blocking API.', 'execution': 'thread'}}
    async def run():
        scope = OperatorEnvironment(runtime='codex', operators=operators).bind({})
        task = asyncio.create_task(scope.invoke({'method': 'fixture_blocking', 'arguments': {'value': 1}}, context=None))
        try:
            async with asyncio.timeout(2):
                while not entered.is_set():
                    await asyncio.sleep(.01)
            task.cancel()
            await asyncio.sleep(.02)
            assert not task.done()
        finally:
            release.set()
        with pytest.raises(asyncio.CancelledError):
            await task
    asyncio.run(run())


def counter_config(**overrides):
    return {'actor': 'operators.fixtures:Counter', 'description': 'Accumulate within this row.',
            'arguments': schema({'value': {'type': 'integer'}}, ['value']),
            'init': {'start': 10}, 'replay': 'verify', **overrides}


@pytest.mark.parametrize('target', ['os:system', 'json:loads', 'test_operator_api:schema',
                                    'pipeline.operaters.fn:run'])
def test_custom_implementation_outside_operator_directory_is_rejected(target):
    with pytest.raises(ValueError, match='operators/'):
        OperatorEnvironment(operators={'bad': {'fn': target, 'description': 'bad', 'arguments': schema({})}})


def test_reexport_is_not_an_operator_implementation(tmp_path, monkeypatch):
    import operators
    folder = tmp_path / 'operators'; folder.mkdir()
    (folder/'alias.py').write_text('from os import system\n')
    monkeypatch.setattr(operators, '__path__', [*operators.__path__, str(folder)])
    with pytest.raises(ValueError, match='defined in'):
        OperatorEnvironment(operators={'bad': {'fn': 'operators.alias:system', 'description': 'bad',
            'arguments': schema({'command': {'type': 'string'}}, ['command'])}})


def test_same_tool_name_can_have_different_configs_without_global_registration(APIs):
    from copy import deepcopy
    a = declaration(APIs, 'codex')
    bconfig = deepcopy(APIs[3])
    bconfig['fixture_scale']['version'] = 'second'
    b = OperatorEnvironment(runtime='codex', operators=bconfig,
        operator_settings={**APIs[1], 'fixture_scale': {'arguments': {'multiplier': 5}}})
    async def invoke():
        call = {'method': 'fixture_scale', 'arguments': {'value': 2}}
        assert await a.bind({}).invoke(call, context=None) == [6]
        assert await b.bind({}).invoke(call, context=None) == [10]
    asyncio.run(invoke())
    assert a.identity() != b.identity()


@pytest.mark.parametrize('failure', [None, 'start', 'call', 'cancel'])
def test_actor_scope_is_lazy_row_local_and_cleans_all_exit_paths(APIs, failure):
    env = OperatorEnvironment(runtime='codex', operators={'counter': counter_config(
        init={'start': 10, 'fail_start': failure == 'start'})})
    assert fixtures.calls == []  # No actor construction during config parsing.
    async def invoke():
        scope = env.bind({})
        try:
            with pytest.raises(ValueError):
                await scope.invoke({'method': 'counter', 'arguments': {'value': 'invalid'}}, context=None)
            assert fixtures.calls == []  # Invalid arguments must not allocate resources.
            call = {'method': 'counter', 'arguments': {'value': -1 if failure == 'call' else -2 if failure == 'cancel' else 1}}
            if failure in ('start', 'call'):
                with pytest.raises(OSError):
                    await scope.invoke(call, context=None)
            elif failure == 'cancel':
                fixtures.entered.clear()
                task = asyncio.create_task(scope.invoke(call, context=None))
                async with asyncio.timeout(2):
                    while not fixtures.entered.is_set():
                        await asyncio.sleep(.001)
                task.cancel()
                with pytest.raises(asyncio.CancelledError):
                    await task
            else:
                assert await scope.invoke(call, context=None) == 11
                assert await scope.invoke(call, context=None) == 12
                other = env.bind({})
                try:
                    assert await other.invoke(call, context=None) == 11
                finally:
                    await other.aclose()
        finally:
            await scope.aclose()
            await scope.aclose()  # Idempotent; no double close.
    asyncio.run(invoke())
    ids = [c[1] for c in fixtures.calls if c[0] == 'construct']
    assert len(ids) == (2 if failure is None else 1)
    for identity in ids:
        names = [c[0] for c in fixtures.calls if len(c) > 1 and c[1] == identity]
        assert names.count('start') == names.count('stop') == names.count('close') == 1
        assert names[-1] == 'close'


def test_actor_thread_cancellation_drains_before_close(APIs):
    fixtures.entered.clear(); fixtures.release.clear()
    env = OperatorEnvironment(runtime='codex', operators={'counter': counter_config(
        actor='operators.fixtures:BlockingCounter', execution='thread')})
    async def run():
        scope = env.bind({})
        task = asyncio.create_task(scope.invoke({'method': 'counter', 'arguments': {'value': 1}}, context=None))
        try:
            async with asyncio.timeout(2):
                while not fixtures.entered.is_set():
                    await asyncio.sleep(.001)
            task.cancel()
            await asyncio.sleep(.02)
            assert not task.done()
        finally:
            fixtures.release.set()
            try:
                await task
            except asyncio.CancelledError:
                pass
            await scope.aclose()
    asyncio.run(run())
    names = [c[0] for c in fixtures.calls]
    assert names.index('finished') < names.index('close')


@pytest.mark.parametrize('runtime', ['demiflow', 'codex'])
def test_yaml_actor_runs_and_closes_through_real_runtime(APIs, tmp_path, server, app_server, monkeypatch, runtime):
    import yaml
    from demiflow.agent import load_agent_config
    task = yaml.safe_load(PACK)['prompts']['enrich']; model = task.pop('model')
    path = tmp_path / 'actor.yaml'
    options = {'codex_agent': {'bin': str(app_server[0])}, 'timeout_s': 4} if runtime == 'codex' else {}
    path.write_text(yaml.safe_dump({'schema_version': 'demiflow_agent_v2', 'runtime': runtime,
        'model': {'name': 'mock-model'} if runtime == 'codex' else model,
        'operators': {'counter': counter_config(replay='recorded' if runtime == 'codex' else 'verify')},
        'tasks': {'enrich': task},
        'budgets': {'max_requests': 4}, 'options': options}))
    cfg = load_agent_config(path)
    assert not fixtures.calls
    calls = [{'method': 'counter', 'arguments': {'value': 1}},
             {'method': 'counter', 'arguments': {'value': 2}}]
    monkeypatch.setenv('DF_TEST_CODEX_MODE', 'generic')
    monkeypatch.setenv('DF_TEST_CODEX_CALLS', json.dumps(calls))
    server['respond'] = lambda body, index: ({'api_calls': calls, 'response': None} if index == 0
        else {'api_calls': [], 'response': {'result': 'done'}})
    ds = data.from_items([{'q': 'one'}]).agentmap_async('enrich', config=cfg,
        inputs={'payload': 'q'}, output='answer', error_output='error', call_output='call',
        options={'sqlite_journal': {'path': str(tmp_path / 'actor.sqlite')}})
    row = collect(ds)[0]
    assert row.get('answer') == 'done', row.get('error')
    assert [o['result'] for o in row['call']['environment']['observations']] == [11, 13]
    assert [c[0] for c in fixtures.calls] == ['construct', 'start', 'invoke', 'invoke', 'stop', 'close']
    if runtime == 'codex':
        cached = collect(ds)[0]
        assert cached['call']['reused']
        assert [c[0] for c in fixtures.calls] == ['construct', 'start', 'invoke', 'invoke', 'stop', 'close']


def test_standard_yaml_forms_preserve_historical_contract_identity():
    import hashlib
    from demiflow.operator_api import definitions
    old_hashes = {'read_documents': 'ffd300c0b3ac2c65aa2cd8017585438d771c9252ff4ce9b65912601835c1bb81', 'map_embeddings': '129b807a0364a0ceadda7bbce4899416a482b521104cd4b38cd5285880085be4', 'search_vectors': '1aee1b85d52668ea9018c06d050d194b7ec41f66555e7b8653c9820d33aed32d'}
    for name, expected in old_hashes.items():
        resolved, _ = definitions((name,))
        native = resolved[name]['function']
        direct, _ = definitions({name: {'fn': native}})
        assert catalog_identity(resolved) == catalog_identity(direct)
        body = json.dumps(json.loads(catalog_identity(resolved))[0], ensure_ascii=False,
                          sort_keys=True, separators=(',', ':'))
        assert hashlib.sha256(body.encode()).hexdigest() == expected
    old = OperatorEnvironment(runtime='codex', operators=('read_documents',), resources='docs')
    direct = OperatorEnvironment(runtime='codex', operators={
        'read_documents': {'fn': 'demiflow.collect.reading:read_documents'}}, resources='docs')
    assert old.identity() == direct.identity()


def test_row_actor_cleanup_does_not_run_outer_action_cleanup(APIs, monkeypatch):
    from demiflow.execution import resource_registry
    outer = []
    monkeypatch.setattr(resource_registry, 'stream_cleanups', lambda: [lambda: outer.append(True)])
    async def run():
        scope = OperatorEnvironment(runtime='codex', operators={'counter': counter_config()}).bind({})
        try:
            assert await scope.invoke({'method': 'counter', 'arguments': {'value': 1}}, context=None) == 11
        finally:
            await scope.aclose()
    asyncio.run(run())
    assert outer == []
    assert fixtures.calls[-1][0] == 'close'
