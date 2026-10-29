"""Image output shares the text actor, runtime, journal and HTTP client."""
import asyncio
import base64
import io
import json
import threading
from email.parser import BytesParser
from email.policy import default
from types import SimpleNamespace

import httpx
import pytest
from PIL import Image

from demiflow import data
from demiflow.execution.file_ref import JsonArtifactRef, save_json_artifact
from demiflow.objects import LocalObjectStore, ObjectRef
from demiflow.operator_llm import parse_prompt_pack
from demiflow.operator_llm.runtime import PromptActor
from demiflow.operator_llm.sqlite_journal import SQLitePromptJournal


def picture(color='red', fmt='PNG'):
    with Image.new('RGB', (24, 16), color) as image:
        buffer = io.BytesIO()
        image.save(buffer, format=fmt)
        return buffer.getvalue()


def model(api='images_json'):
    return {'backend': 'modelhub', 'model': 'fixture', 'api': api,
        'base_url': 'http://127.0.0.1:8016/v1', 'api_key_env': '',
        'revision': 'fixture-v1', 'parameters': {'seed': 42}}


def pack(configuration=None, **changes):
    definition = {'version': '1', 'response_format': 'image', 'model': configuration or model(),
        'template': '{{ instruction }}\n{{ caption }}{{ images | image }}'}
    return parse_prompt_pack(json.dumps({'schema_version': 'demiflow_prompt_pack_v2',
        'prompts': {'generate': definition | changes}}))


def row(instruction='draw a tool', images=None):
    return {'instruction': instruction, 'caption': '完整图注 {{ 不再展开 }}', 'images': images or [], 'id': 7}


def node(tmp_path, rows=None, *, configuration=None, **changes):
    arguments = dict(config=pack(configuration), inputs=['instruction', 'caption', 'images'],
        output='image', call_output='call', error_output='error', max_requests=16,
        options={'sqlite_journal': {'path': tmp_path / 'calls.sqlite'}, 'object_store': tmp_path / 'objects'})
    return data.from_items([row()] if rows is None else rows).map_prompt_async('generate', **(arguments | changes))


def collect(dataset):
    rows = []
    dataset.map(lambda value: rows.append(value) or value).run_stream(log_every=0)
    return rows


def install_http(monkeypatch, handler=None):
    original, seen = httpx.AsyncClient, []
    async def respond(request):
        seen.append(request)
        if handler is not None:
            result = handler(request)
            return await result if hasattr(result, '__await__') else result
        return httpx.Response(200, json={'data': [{'b64_json': base64.b64encode(picture()).decode()}]})
    monkeypatch.setattr(httpx, 'AsyncClient', lambda **options: original(transport=httpx.MockTransport(respond), **options))
    return seen


def test_shared_actor_and_exact_request(tmp_path, monkeypatch):
    seen = install_http(monkeypatch)
    dataset = node(tmp_path, [row(images=[picture('blue')])])
    assert type(dataset._stages[-1]) is PromptActor
    assert not hasattr(dataset, 'map_image_async')
    result = collect(dataset)[0]
    assert result['id'] == 7 and result['error'] is None
    assert ObjectRef(**result['image']).read() == picture()
    saved = JsonArtifactRef(**result['call']['input_ref']).read()
    assert saved['body'] == json.loads(seen[0].content)
    assert saved['body']['prompt'] == 'draw a tool\n完整图注 {{ 不再展开 }}'
    assert saved['endpoint'] == '/images/edits'
    assert base64.b64decode(saved['body']['image'][0].split(',')[1]) == picture('blue')
    replay = collect(node(tmp_path, [row(images=[picture('blue')])], max_requests=0))[0]
    assert replay['call']['reused'] and replay['image'] == result['image'] and len(seen) == 1
    assert replay['call']['request_id'] == result['call']['request_id']


@pytest.mark.parametrize('references', [[], [picture('blue'), picture('green')]])
def test_legacy_record_replays_without_migration(tmp_path, monkeypatch, references):
    seen = install_http(monkeypatch)
    source = {'template': {'name': 'generate', 'version': '1',
        'template': '{{ instruction }}\n{{ caption }}{{ images | image }}'},
        'backend': 'modelhub', 'endpoint': '/images/edits' if references else '/images/generations',
        'base_url': model()['base_url'], 'model': 'fixture', 'model_path': None, 'revision': 'fixture-v1',
        'body': {'model': 'fixture', 'prompt': 'draw a tool\n完整图注 {{ 不再展开 }}', 'seed': 42, 'n': 1}}
    if references:
        source['body']['image'] = ['data:image/png;base64,' + base64.b64encode(raw).decode() for raw in references]
    image = LocalObjectStore(tmp_path / 'objects').put(picture()).to_dict()
    input_ref = save_json_artifact(tmp_path / 'calls.inputs', source).to_dict()
    journal = SQLitePromptJournal(tmp_path / 'calls.sqlite', max_requests=1)
    assert journal.reserve(source)
    journal.response(source, {'image': image, 'seconds': 1.5, 'input_ref': input_ref})
    identity = journal.references(source)['request_id']
    journal.close()
    result = collect(node(tmp_path, [row(images=references)], max_requests=0))[0]
    assert result['image'] == image and result['call']['input_ref'] == input_ref
    assert result['call']['request_id'] == identity and result['call']['seconds'] == 1.5
    assert result['call']['reused'] and not seen


@pytest.mark.parametrize('api', ['images_json', 'images', 'openrouter_images', 'chat'])
def test_protocols_preserve_order_mime_and_bytes(tmp_path, monkeypatch, api):
    def respond(request):
        url = 'data:image/png;base64,' + base64.b64encode(picture()).decode()
        body = ({'choices': [{'message': {'images': [{'image_url': {'url': url}}]}}]}
                if api == 'chat' else {'data': [{'url': url}]})
        return httpx.Response(200, json=body)
    seen = install_http(monkeypatch, respond)
    images = [picture(fmt=fmt) for fmt in ('JPEG', 'PNG', 'WEBP')]
    options = {'sqlite_journal': {'path': tmp_path / 'calls.sqlite'},
               'object_store': tmp_path / 'objects', 'image_encoding': 'preserve'}
    result = collect(node(tmp_path, [row(images=images)], configuration=model(api), options=options))[0]
    assert result['error'] is None
    source = JsonArtifactRef(**result['call']['input_ref']).read()
    assert source['image_encoding'] == 'preserve'
    body = source['body']
    urls = (body['image'] if api == 'images_json' else body['image[]'] if api == 'images' else
        [part['image_url']['url'] for part in (body['input_references'] if api == 'openrouter_images'
         else body['messages'][0]['content'][1:])])
    assert [base64.b64decode(url.split(',')[1]) for url in urls] == images
    assert [url.split(';')[0] for url in urls] == ['data:image/jpeg', 'data:image/png', 'data:image/webp']
    if api == 'images':
        message = BytesParser(policy=default).parsebytes(b'Content-Type: ' +
            seen[0].headers['content-type'].encode() + b'\r\n\r\n' + seen[0].content)
        files = [part for part in message.iter_parts() if part.get_filename()]
        assert [part.get_filename() for part in files] == ['reference_1.jpg', 'reference_2.png', 'reference_3.webp']
        assert [part.get_payload(decode=True) for part in files] == images
    else:
        assert json.loads(seen[0].content) == body


@pytest.mark.parametrize('fmt', ['GIF', 'BMP', 'TIFF'])
def test_other_formats_convert_to_png(tmp_path, monkeypatch, fmt):
    seen = install_http(monkeypatch)
    result = collect(node(tmp_path, [row(images=[picture(fmt=fmt)])], options={
        'sqlite_journal': {'path': tmp_path / 'calls.sqlite'}, 'object_store': tmp_path / 'objects',
        'image_encoding': 'preserve'}))[0]
    assert result['error'] is None and json.loads(seen[0].content)['image'][0].startswith('data:image/png;')


@pytest.mark.parametrize('limits,values,match', [
    ({'max_text_bytes': 3}, row(), 'text'),
    ({'max_pixels': 100}, row(images=[picture()]), 'max_pixels'),
    ({'max_image_bytes': 10}, row(images=[picture()]), 'bounded image'),
    ({'max_total_pixels': 600}, row(images=[picture(), picture()]), 'aggregate'),
    ({'max_images': 1}, row(images=[picture(), picture()]), 'max_images'),
    ({'max_request_bytes': 100}, row(images=[picture()]), 'request'),
])
def test_limits_before_reservation(tmp_path, monkeypatch, limits, values, match):
    seen = install_http(monkeypatch)
    result = collect(node(tmp_path, [values], options={'sqlite_journal': {'path': tmp_path / 'calls.sqlite'},
        'object_store': tmp_path / 'objects', 'limits': limits}))[0]
    assert match in result['error'] and result['image'] is None and not seen
    assert not (tmp_path / 'calls.sqlite').exists()


def test_budget_before_artifacts_and_durable_across_nodes(tmp_path, monkeypatch):
    seen = install_http(monkeypatch)
    assert 'budget' in collect(node(tmp_path, max_requests=0))[0]['error'].lower()
    assert not seen and not (tmp_path / 'calls.inputs').exists()
    assert collect(node(tmp_path, max_requests=1))[0]['image']
    assert 'budget' in collect(node(tmp_path, [row('different')], max_requests=1))[0]['error'].lower()
    assert len(seen) == 1


@pytest.mark.parametrize('changes', [
    {'concurrency': 9}, {'concurrency': True}, {'queue_depth': 9}, {'max_requests': None},
    {'output': 'call'}, {'inputs': ['instruction']}, {'output': None, 'outputs': {'result': 'image'}},
    {'options': {}}, {'options': {'unknown': 1}}, {'token_budget': object()},
])
def test_invalid_declaration_has_no_io(tmp_path, changes):
    with pytest.raises((TypeError, ValueError)):
        node(tmp_path, [], **changes)
    assert not (tmp_path / 'calls.sqlite').exists()


@pytest.mark.parametrize('changes', [
    {'schema_retries': 1}, {'input_mode': 'messages'}, {'response_schema': {}}, {'unknown': True},
    {'model': {'backend': 'unsupported', 'model': 'x'}},
])
def test_invalid_pack(changes):
    with pytest.raises(ValueError):
        pack(**changes)


@pytest.mark.parametrize('rows', [[], [row()]])
def test_empty_and_skipped_do_not_open_journal(tmp_path, monkeypatch, rows):
    seen = install_http(monkeypatch)
    collect(node(tmp_path, rows, when=lambda value: False, max_requests=0))
    assert not seen and not (tmp_path / 'calls.sqlite').exists()


@pytest.mark.parametrize('concurrency', [4, 8])
def test_http_concurrency(tmp_path, monkeypatch, concurrency):
    entered, ready = [], asyncio.Event()
    async def respond(request):
        entered.append(request)
        if len(entered) == concurrency:
            ready.set()
        await asyncio.wait_for(ready.wait(), 5)
        return httpx.Response(200, json={'data': [{'b64_json': base64.b64encode(picture()).decode()}]})
    install_http(monkeypatch, respond)
    results = collect(node(tmp_path, [row(str(number)) for number in range(concurrency)],
        concurrency=concurrency, queue_depth=concurrency))
    assert len(entered) == len(results) == concurrency and all(value['image'] for value in results)


def test_managed_service_reservation_replay_and_failure(tmp_path, monkeypatch):
    from demiflow.services import ManagedHTTPService, ModelServiceError
    from demiflow.services.http import _HTTPOwner
    events = []
    async def start(owner):
        assert list((tmp_path / 'calls.inputs').glob('*.json'))
        events.append('start')
        owner._process = SimpleNamespace(poll=lambda: None)
    async def release(owner):
        if owner._process is not None:
            events.append('stop')
            owner._process = None
    monkeypatch.setattr(_HTTPOwner, '_start', start)
    monkeypatch.setattr(_HTTPOwner, '_release', release)
    seen = install_http(monkeypatch)
    service = ManagedHTTPService(['fixture'], base_url=model()['base_url'], expected_model='fixture')
    assert collect(node(tmp_path, service=service))[0]['image']
    assert collect(node(tmp_path, service=service, max_requests=0))[0]['call']['reused']
    assert len(seen) == 1 and events == ['start', 'stop']
    async def fail(owner):
        raise ModelServiceError('GPU occupied')
    monkeypatch.setattr(_HTTPOwner, '_start', fail)
    with pytest.raises(Exception, match='GPU occupied'):
        collect(node(tmp_path, [row('new')], service=service))
    assert len(seen) == 1 and len(list((tmp_path / 'calls.inputs').glob('*.json'))) == 2


def test_failure_not_retried(tmp_path, monkeypatch):
    seen = install_http(monkeypatch, lambda request: httpx.Response(500, json={'error': 'fixture'}))
    first = collect(node(tmp_path))[0]
    assert first['error'] and first['call']['input_ref']
    assert collect(node(tmp_path))[0]['error'] and len(seen) == 1


def test_download_has_no_credentials(tmp_path, monkeypatch):
    monkeypatch.setenv('FIXTURE_KEY', 'secret')
    def respond(request):
        if request.url.host == 'result.example':
            assert 'authorization' not in request.headers
            return httpx.Response(200, content=picture())
        assert request.headers['authorization'] == 'Bearer secret'
        return httpx.Response(200, json={'data': [{'url': 'https://result.example/image'}]})
    seen = install_http(monkeypatch, respond)
    assert collect(node(tmp_path, configuration=model() | {'api_key_env': 'FIXTURE_KEY'}))[0]['image']
    assert len(seen) == 2


def test_response_limit(tmp_path, monkeypatch):
    seen = install_http(monkeypatch)
    result = collect(node(tmp_path, options={'sqlite_journal': {'path': tmp_path / 'calls.sqlite'},
        'object_store': tmp_path / 'objects', 'limits': {'max_response_bytes': 10}}))[0]
    assert 'max_response_bytes' in result['error'] and len(seen) == 1


def test_diffusers_cancel_drains_and_saves_response(tmp_path, monkeypatch):
    from demiflow.operator_llm.diffusers_transport import DiffusersTransport
    started, finish, events = threading.Event(), threading.Event(), []
    def generate(transport, source):
        events.append('generate')
        started.set()
        assert finish.wait(5)
        return picture()
    def release(transport):
        assert finish.is_set()
        events.append('close')
    monkeypatch.setattr(DiffusersTransport, 'generate', generate)
    monkeypatch.setattr(DiffusersTransport, 'release', release)
    configuration = {'backend': 'diffusers', 'model': 'fixture', 'model_path': '/fixture',
                     'device': 'cpu', 'seed': 42, 'parameters': {}}
    actor = node(tmp_path, configuration=configuration)._stages[-1]
    async def execute():
        task = asyncio.create_task(actor(row()))
        assert await asyncio.to_thread(started.wait, 5)
        task.cancel()
        await asyncio.sleep(.02)
        assert not task.done()
        finish.set()
        with pytest.raises(asyncio.CancelledError):
            await task
        await actor.aclose()
    asyncio.run(execute())
    assert events == ['generate', 'close']
    assert collect(node(tmp_path, configuration=configuration, max_requests=0))[0]['call']['reused']
    assert events.count('generate') == 1
    with pytest.raises(ValueError, match='concurrency'):
        node(tmp_path, configuration=configuration, concurrency=2)


def test_sync_execution_rejects_image_output(tmp_path):
    dataset = data.from_items([row()]).map_prompt('generate', config=pack(),
        inputs=['instruction', 'caption', 'images'], output='image')
    with pytest.raises(Exception, match='map_prompt_async'):
        dataset.take(1)


def test_corrupt_and_animated_images_do_not_reserve(tmp_path, monkeypatch):
    seen = install_http(monkeypatch)
    buffer = io.BytesIO()
    with Image.new('RGB', (16, 16), 'red') as first, Image.new('RGB', (16, 16), 'blue') as second:
        first.save(buffer, format='GIF', save_all=True, append_images=[second])
    results = collect(node(tmp_path, [row(images=[b'bad']), row(images=[buffer.getvalue()]),
        row(images=[picture(fmt='JPEG')[:-20]])], options={
            'sqlite_journal': {'path': tmp_path / 'calls.sqlite'},
            'object_store': tmp_path / 'objects', 'image_encoding': 'preserve'}))
    assert len(results) == 3 and all(result['error'] for result in results)
    assert not seen and not (tmp_path / 'calls.sqlite').exists()


def test_png_expansion_and_preserve_budget(tmp_path, monkeypatch):
    import random
    with Image.frombytes('RGB', (256, 256), random.Random(42).randbytes(256 * 256 * 3)) as image:
        buffer = io.BytesIO()
        image.save(buffer, format='JPEG', quality=70)
    raw = buffer.getvalue()
    assert len(raw) < 64 * 1024
    seen = install_http(monkeypatch)
    options = {'sqlite_journal': {'path': tmp_path / 'calls.sqlite'},
        'object_store': tmp_path / 'objects', 'limits': {'max_image_bytes': 64 * 1024}}
    assert 'Encoded image' in collect(node(tmp_path, [row(images=[raw])], options=options))[0]['error']
    assert not seen and not (tmp_path / 'calls.sqlite').exists()
    assert collect(node(tmp_path, [row(images=[raw])], options=options | {'image_encoding': 'preserve'}))[0]['image']
    assert base64.b64decode(json.loads(seen[0].content)['image'][0].split(',')[1]) == raw


def test_shared_service_slots_use_common_runtime(tmp_path, monkeypatch):
    from contextlib import asynccontextmanager
    from demiflow.services.shared_http import SharedHTTPService, _SharedOwner
    events, active, peak = [], 0, 0
    semaphore = asyncio.Semaphore(1)
    async def ready(owner):
        events.append('ready')
    async def close(owner):
        events.append('close')
    @asynccontextmanager
    async def slot(owner):
        async with semaphore:
            yield
    monkeypatch.setattr(_SharedOwner, 'ensure_ready', ready)
    monkeypatch.setattr(_SharedOwner, 'request_slot', slot)
    monkeypatch.setattr(_SharedOwner, 'aclose', close)
    async def respond(request):
        nonlocal active, peak
        active += 1
        peak = max(peak, active)
        await asyncio.sleep(.01)
        active -= 1
        return httpx.Response(200, json={'data': [{'b64_json': base64.b64encode(picture()).decode()}]})
    seen = install_http(monkeypatch, respond)
    service = SharedHTTPService(root=tmp_path, name='fixture', request_concurrency=1,
        configuration={'command': ['fixture'], 'base_url': model()['base_url'], 'expected_model': 'fixture'})
    results = collect(node(tmp_path, [row(str(number)) for number in range(4)], service=service,
        concurrency=4, queue_depth=4))
    assert all(result['image'] for result in results) and len(seen) == 4 and peak == 1
    assert events.count('close') == 1


@pytest.mark.parametrize('http_metadata', [False, True])
def test_diffusers_native_contract_without_weights(tmp_path, monkeypatch, http_metadata):
    from demiflow.operator_llm.diffusers_transport import DiffusersTransport
    seen = []
    class Model:
        def __call__(self, **arguments):
            seen.append(arguments)
            assert arguments['image'][0].mode == 'RGB'
            assert arguments['generator'].initial_seed() == 42
            return SimpleNamespace(images=[Image.new('RGB', (24, 16), 'red')])
    monkeypatch.setattr(DiffusersTransport, 'load_model', lambda transport: setattr(transport, 'model', Model()))
    configuration = {'backend': 'diffusers', 'model': 'fixture', 'model_path': '/fixture',
        'device': 'cpu', 'seed': 42, 'parameters': {'num_inference_steps': 3}}
    if http_metadata:
        monkeypatch.delenv('UNUSED_GATEWAY_KEY', raising=False)
        configuration.update(api_key_env='UNUSED_GATEWAY_KEY', base_url='http://unused.invalid/v1')
    result = collect(node(tmp_path, [row(images=[picture('blue')])], configuration=configuration))[0]
    assert result['image'] and seen[0]['num_inference_steps'] == 3
    assert collect(node(tmp_path, [row(images=[picture('blue')])], configuration=configuration, max_requests=0))[0]['call']['reused']
    assert len(seen) == 1


@pytest.mark.parametrize('count', [1, 2, 3])
@pytest.mark.parametrize('with_caption', [False, True])
def test_interleaved_parts_independent_final_text_and_replay(tmp_path, monkeypatch, count, with_caption):
    def reply(request):
        return httpx.Response(200, json={'choices': [{'message': {'content': [
            {'type': 'image_url', 'image_url': {'url': 'data:image/png;base64,' + base64.b64encode(picture()).decode()}}]}}]})
    seen = install_http(monkeypatch, reply)
    template = 'Reference rules\n'
    values = {'instruction': 'draw {{literal}} {% text_segment %}'}
    for i in range(count):
        template += f'Concept {i}\n{{{{ image{i} | image }}}}'
        values[f'image{i}'] = [picture(('red', 'green', 'blue')[i])]
        if with_caption:
            template += f'{{{{ caption{i} }}}}'
            values[f'caption{i}'] = f'Caption {i} {{% text_segment %}}'
    template += '{% text_segment %}Generate: {{ instruction }}'
    config = pack(model('chat_interleaved'), template=template)
    options = dict(options={'sqlite_journal': {'path': tmp_path/'calls.sqlite'}, 'object_store':tmp_path/'objects',
                           'image_encoding':'preserve'}, inputs=list(values), config=config, max_requests=1,
                   output='image',call_output='call',error_output='error')
    def execute(**updates):
        return collect(data.from_items([values]).map_prompt_async('generate', **(options|updates)))[0]
    result = execute()
    assert result['error'] is None
    content = json.loads(seen[0].content)['messages'][0]['content']
    assert content[-1] == {'type':'text','text':'Generate: '+values['instruction']}
    assert content[-2]['type'] == ('text' if with_caption else 'image_url')
    images = [base64.b64decode(p['image_url']['url'].split(',')[1]) for p in content if p['type']=='image_url']
    assert images == [values[f'image{i}'][0] for i in range(count)]
    if with_caption:
        for i in range(count):
            image_position = [k for k,p in enumerate(content) if p['type']=='image_url'][i]
            assert content[image_position+1]['text'].startswith(values[f'caption{i}'])
    saved = JsonArtifactRef(**result['call']['input_ref']).read()
    assert saved['body'] == json.loads(seen[0].content)
    replay = execute(max_requests=0)
    assert replay['call']['reused'] and len(seen)==1 and replay['image']==result['image']
    # Same flattened text, different segment boundary is a different request.
    merged = pack(model('chat_interleaved'), template=template.replace('{% text_segment %}', ''))
    changed = execute(config=merged, max_requests=2)
    assert changed['call']['request_id'] != result['call']['request_id'] and len(seen)==2


def test_text_boundary_is_static_and_existing_flat_api_is_unchanged(tmp_path, monkeypatch):
    from demiflow.operator_llm.template import compile_template, render_template
    from demiflow.operator_llm.model import TextPart
    marker = '{% text_segment %}'
    template = compile_template(marker+'A{{ text }}'+marker+marker+'B'+marker)
    assert render_template(template, {'text':marker}) == (TextPart('A'+marker),TextPart('B'))
    assert render_template(compile_template('A{{ text }}B'), {'text':'C'}) == (TextPart('ACB'),)
    seen=install_http(monkeypatch)
    config=pack(template='A{{ images | image }}B'+marker+'{{ instruction }}{{ caption }}')
    result=collect(node(tmp_path,[row(images=[picture()])],config=config))[0]
    assert not result['error']
    assert json.loads(seen[0].content)['prompt']=='ABdraw a tool完整图注 {{ 不再展开 }}'


def test_interleaved_limits_before_http_and_reservation(tmp_path, monkeypatch):
    from demiflow.operator_llm.template import compile_template
    from demiflow.operator_llm.errors import PromptTemplateSyntaxError
    with pytest.raises(PromptTemplateSyntaxError,match='255'):
        compile_template('{% text_segment %}'*256+'text')
    seen=install_http(monkeypatch)
    config=pack(model('chat_interleaved'), template='a{% text_segment %}'*255+'{{ instruction }}{{ caption }}{{ images | image }}')
    result=collect(node(tmp_path,[row(images=[picture()])],config=config))[0]
    assert '256 content parts' in result['error'] and not seen
    assert result['call'] is None
    config=pack(model('chat_interleaved'))
    result=collect(node(tmp_path,[row(images=[picture()])],config=config,
        options={'sqlite_journal':{'path':tmp_path/'limit.sqlite'},'object_store':tmp_path/'objects',
                 'limits':{'max_text_bytes':2}}))[0]
    assert 'max_text_bytes' in result['error'] and not seen
