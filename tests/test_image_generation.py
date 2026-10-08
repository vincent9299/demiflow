"""Native image requests: exact template/pixels, budgets, replay and uncertainty."""
import asyncio
import base64
import io
import json
import pytest
from PIL import Image
from demiflow import data
from demiflow.image_generation import ImageGenerator
from demiflow.execution.file_ref import JsonArtifactRef
from demiflow.operator_llm.journal import UncertainPromptCall


def picture(color='red'):
    b=io.BytesIO(); Image.new('RGB',(8,6),color).save(b,format='PNG'); return b.getvalue()


def actor(tmp_path, **kwargs):
    return ImageGenerator(template={'name':'answer','version':'1','template':'Reference:\n{{ question }}{{ images | image }}'},
        model={'backend':'modelhub','api':'openrouter_images','model':'route/model','provider_model':'exact/model',
               'base_url':'http://fixture','api_key_env':'FIXTURE_KEY','parameters':{},'timeout_s':5},
        inputs={'question':'question','images':'images'},output='image',call_output='call',error_output='error',
        journal_path=tmp_path/'calls.sqlite',object_store=tmp_path/'objects',max_requests=3,**kwargs)


def test_complete_request_is_persisted_before_transport_and_replayed(tmp_path,monkeypatch):
    op=actor(tmp_path); seen=[]
    def remote(self,request,images):
        artifacts=list((tmp_path/'calls.inputs').glob('*.json'))
        assert len(artifacts)==1
        seen.append(request)
        return picture()
    monkeypatch.setattr(ImageGenerator,'remote',remote)
    values={'question':'Keep {{ literal }}','images':[picture('red'),picture('blue')]}
    output,call,_=op.generate(values)
    exact=JsonArtifactRef(**call['input_ref']).read()
    assert exact==seen[0]
    assert exact['body']['prompt']=='Reference:\nKeep {{ literal }}'
    assert exact['body']['model']=='exact/model'
    assert [base64.b64decode(r['image_url']['url'].split(',')[1]) for r in exact['body']['input_references']]==values['images']
    assert op.generate(values)[0]==output and len(seen)==1
    op.journal.close()


def test_error_is_not_retried_and_input_is_retained(tmp_path,monkeypatch):
    op=actor(tmp_path); calls=[]
    def fail(*args): calls.append(1); raise RuntimeError('provider failed')
    monkeypatch.setattr(ImageGenerator,'remote',fail)
    with pytest.raises(RuntimeError,match='provider failed') as error: op.generate({'question':'q','images':[]})
    assert JsonArtifactRef(**error.value.image_call['input_ref']).read()['body']['prompt']=='Reference:\nq'
    with pytest.raises(UncertainPromptCall): op.generate({'question':'q','images':[]})
    assert len(calls)==1
    op.journal.close()


@pytest.mark.parametrize('limits,values,match',[
    ({'max_text_bytes':3},{'question':'oversize','images':[]},'text'),
    ({'max_pixels':10},{'question':'q','images':[picture()]},'max_pixels'),
    ({'max_total_pixels':60},{'question':'q','images':[picture(),picture()]},'aggregate'),
    ({'max_images':1},{'question':'q','images':[picture(),picture()]},'max_images'),
    ({'max_request_bytes':100},{'question':'q','images':[picture()]},'request'),
])
def test_limits_reject_before_reservation(tmp_path,monkeypatch,limits,values,match):
    op=actor(tmp_path,limits=limits)
    monkeypatch.setattr(ImageGenerator,'remote',lambda *a:pytest.fail('must reject before provider'))
    with pytest.raises(ValueError,match=match): op.generate(values)
    assert not (tmp_path/'calls.sqlite').exists()


def test_native_node_preserves_rows_and_drains(tmp_path,monkeypatch):
    monkeypatch.setattr(ImageGenerator,'remote',lambda *args:picture())
    spec=actor(tmp_path)
    ds=data.from_items([{'question':'q','images':[],'id':1}]).map_image_async(
        template=spec.spec,model=spec.config,inputs=spec.inputs,output='image',call_output='call',error_output='error',
        journal_path=tmp_path/'stream.sqlite',object_store=tmp_path/'objects',max_requests=1)
    assert type(ds._plan.operations[-1]).__name__=='ImageMapOp'
    collected=[]
    ds.map(lambda row:collected.append(row) or row).run_stream(log_every=0)
    assert len(collected)==1 and collected[0]['id']==1 and collected[0]['image'] and not collected[0]['error']


def test_http_body_and_response_bound(tmp_path,monkeypatch):
    import httpx
    op=actor(tmp_path); client=httpx.Client; seen=[]
    def handle(request):
        seen.append(json.loads(request.content));return httpx.Response(200,json={'data':[{'b64_json':base64.b64encode(picture()).decode()}]})
    monkeypatch.setattr(httpx,'Client',lambda **kw:client(transport=httpx.MockTransport(handle),**kw))
    op.generate({'question':'q','images':[picture('blue')]})
    assert seen[0]['prompt']=='Reference:\nq' and len(seen[0]['input_references'])==1
    op.journal.close()
    small=actor(tmp_path/'small',limits={'max_response_bytes':10})
    with pytest.raises(ValueError,match='max_response_bytes'):small.generate({'question':'q','images':[]})
    small.journal.close()


def test_request_budget_rejects_before_writing_image_artifacts(tmp_path,monkeypatch):
    from demiflow.operator_llm.errors import PromptBudgetExceededError
    op=actor(tmp_path);op.journal.limit=0
    monkeypatch.setattr(ImageGenerator,'remote',lambda *args:pytest.fail('budget exhausted'))
    with pytest.raises(PromptBudgetExceededError):op.generate({'question':'q','images':[picture()]})
    assert not (tmp_path/'calls.inputs').exists()
    op.journal.close()


def test_cancellation_drains_native_work_before_close(tmp_path,monkeypatch):
    import threading
    started,finish=threading.Event(),threading.Event()
    op=actor(tmp_path)
    def remote(*args):
        started.set();assert finish.wait(5);return picture()
    monkeypatch.setattr(ImageGenerator,'remote',remote)
    async def run():
        task=asyncio.create_task(op({'question':'q','images':[]}))
        assert await asyncio.to_thread(started.wait,5)
        task.cancel();await asyncio.sleep(0)
        assert not task.done()
        finish.set()
        with pytest.raises(asyncio.CancelledError):await task
        await op.aclose()
    asyncio.run(run())


@pytest.mark.parametrize('concurrency',[4,8])
def test_remote_requests_overlap_and_local_shared_actor_is_rejected(tmp_path,monkeypatch,concurrency):
    import threading
    barrier=threading.Barrier(concurrency);entered=[]
    def remote(self,request,images):
        entered.append(request['body']['prompt']);barrier.wait(timeout=5);return picture()
    monkeypatch.setattr(ImageGenerator,'remote',remote)
    op=actor(tmp_path);rows=[{'question':str(i),'images':[]} for i in range(concurrency)]
    kwargs=dict(template=op.spec,model=op.config,inputs=op.inputs,output='image',call_output='call',error_output='error',
        journal_path=tmp_path/'parallel.sqlite',object_store=tmp_path/'objects',max_requests=concurrency,concurrency=concurrency,queue_depth=concurrency)
    done=[]
    data.from_items(rows).map_image_async(**kwargs).map(lambda row:done.append(row) or row).run_stream(log_every=0)
    assert len(entered)==len(done)==concurrency and all(r['image'] and not r['error'] for r in done)
    with pytest.raises(ValueError,match='Diffusers actor concurrency'):
        data.from_items(rows).map_image_async(**{**kwargs,'model':{**op.config,'backend':'diffusers'}})


def test_managed_image_service_starts_once_for_eight_requests_and_skips_replay(tmp_path, monkeypatch):
    import threading
    from types import SimpleNamespace
    from demiflow.services import ManagedHTTPService
    from demiflow.services.http import _HTTPOwner
    events, requests = [], []
    async def start(owner):
        events.append('start')
        await asyncio.sleep(.01)
        owner._process = SimpleNamespace(poll=lambda: None)
    async def release(owner):
        if owner._process is not None:
            events.append('stop')
            owner._process = None
    monkeypatch.setattr(_HTTPOwner, '_start', start)
    monkeypatch.setattr(_HTTPOwner, '_release', release)
    barrier = threading.Barrier(8)
    def remote(self, request, images):
        assert events == ['start'] and images == []
        requests.append(request['body']['prompt'])
        barrier.wait(timeout=5)
        return picture()
    monkeypatch.setattr(ImageGenerator, 'remote', remote)
    spec = actor(tmp_path)
    service = ManagedHTTPService(['fixture'], base_url='http://127.0.0.1:8003/v1',
                                 expected_model='exact/model')
    kwargs = dict(template=spec.spec, model={**spec.config, 'base_url':service.base_url},
        inputs=spec.inputs, output='image', call_output='call', error_output='error',
        journal_path=tmp_path/'managed.sqlite', object_store=tmp_path/'objects',
        max_requests=8, concurrency=8, queue_depth=8, service=service)
    rows = [{'question':str(i),'images':[]} for i in range(8)]
    results = []
    data.from_items(rows).map_image_async(**kwargs).map(lambda row:results.append(row) or row).run_stream(log_every=0)
    assert len(requests) == len(results) == 8 and all(r['image'] and not r['error'] for r in results)
    assert events == ['start', 'stop']
    data.from_items(rows).map_image_async(**kwargs).run_stream(log_every=0)
    assert len(requests) == 8 and events == ['start', 'stop']  # All cached: no deployment.


def test_managed_service_failure_stops_node_and_retains_input(tmp_path, monkeypatch):
    from demiflow.services import ManagedHTTPService, ModelServiceError
    from demiflow.services.http import _HTTPOwner
    async def fail(owner):
        raise ModelServiceError('GPU occupied')
    monkeypatch.setattr(_HTTPOwner, '_start', fail)
    monkeypatch.setattr(ImageGenerator, 'remote', lambda *a:pytest.fail('service not ready'))
    spec = actor(tmp_path)
    service = ManagedHTTPService(['fixture'], base_url='http://127.0.0.1:8003/v1')
    op = ImageGenerator(template=spec.spec, model={**spec.config,'base_url':service.base_url},
        inputs=spec.inputs, output='image', call_output='call', error_output='error',
        journal_path=tmp_path/'failed.sqlite', object_store=tmp_path/'objects', max_requests=1, service=service)
    async def run():
        try:
            with pytest.raises(ModelServiceError, match='GPU occupied'):
                await op({'question':'unaltered question','images':[]})
        finally:
            await op.aclose()
    asyncio.run(run())
    saved = list((tmp_path/'failed.inputs').glob('*.json'))
    assert len(saved) == 1


@pytest.mark.parametrize('with_images',[False,True])
def test_local_json_images_sends_exact_ordered_request(tmp_path,monkeypatch,with_images):
    import httpx
    op=actor(tmp_path)
    op.config['api']='images_json'
    client=httpx.Client
    requests=[]
    def respond(req):
        requests.append((req.url.path,json.loads(req.content)))
        return httpx.Response(200,json={'data':[{'b64_json':base64.b64encode(picture()).decode()}]})
    monkeypatch.setattr(httpx,'Client',lambda **kw:client(transport=httpx.MockTransport(respond),**kw))
    images=[picture('red'),picture('blue')] if with_images else []
    _,call,_=op.generate({'question':'  complete prompt\n','images':images})
    saved=JsonArtifactRef(**call['input_ref']).read()
    assert requests==[(saved['endpoint'],saved['body'])]
    assert requests[0][0]==('/images/edits' if with_images else '/images/generations')
    assert [base64.b64decode(url.split(',')[1]) for url in saved['body'].get('image',[])]==images
    assert saved['body']['prompt']=='Reference:\n  complete prompt\n'
    op.journal.close()


def test_two_image_actors_share_service_slots_and_last_user_shutdown(tmp_path,monkeypatch):
    import socket, sys, threading, time
    from demiflow.services.shared_http import SharedHTTPService
    from demiflow.services.manage import status_service, stop_service
    with socket.socket() as sock:
        sock.bind(('127.0.0.1',0));port=sock.getsockname()[1]
    service=SharedHTTPService(root=tmp_path,name='image-test',request_concurrency=1,stop_when_idle=True,
        configuration={'command':[sys.executable,'-m','http.server',str(port),'--bind','127.0.0.1'],
            'base_url':f'http://127.0.0.1:{port}', 'ready_path':'/', 'startup_timeout_s':10,'shutdown_timeout_s':1})
    active=0;peak=0;lock=threading.Lock()
    def remote(*args):
        nonlocal active,peak
        with lock: active+=1;peak=max(peak,active)
        time.sleep(.02)
        with lock: active-=1
        return picture()
    monkeypatch.setattr(ImageGenerator,'remote',remote)
    spec=actor(tmp_path/'spec')
    operators=[ImageGenerator(template=spec.spec,model={**spec.config,'base_url':service.configuration['base_url']},
        inputs=spec.inputs,output='image',call_output='call',error_output='error',service=service,
        journal_path=tmp_path/f'{i}.sqlite',object_store=tmp_path/'objects',max_requests=1) for i in range(2)]
    async def run():
        a,b=operators
        try:
            results=await asyncio.gather(a({'question':'a','images':[]}),b({'question':'b','images':[]}))
            assert all(r['image'] and not r['error'] for r in results) and peak==1
            await a.aclose()
            assert status_service(tmp_path,'image-test')['alive']
        finally:
            await a.aclose();await b.aclose()
    try:
        asyncio.run(run())
        assert not status_service(tmp_path,'image-test')['alive']
    finally:
        stop_service(tmp_path,'image-test',timeout_s=5)


def encoded_picture(fmt, size=(24, 16)):
    with Image.new('RGB', size, 'orange') as im:
        buf = io.BytesIO()
        im.save(buf, format=fmt)
        return buf.getvalue()


@pytest.mark.parametrize('api', ['images_json', 'openrouter_images', 'chat', 'images'])
def test_preserve_formats_exact_wire_artifact_and_replay(tmp_path, monkeypatch, api):
    import httpx
    from email.parser import BytesParser
    from email.policy import default
    op = actor(tmp_path, image_encoding='preserve')
    op.config['api'] = api
    images = [encoded_picture(fmt) for fmt in ('JPEG', 'PNG', 'WEBP')]
    client, seen = httpx.Client, []
    def respond(req):
        seen.append(req)
        url = 'data:image/png;base64,' + base64.b64encode(picture()).decode()
        result = ({'choices': [{'message': {'images': [{'image_url': {'url': url}}]}}]}
                  if api == 'chat' else {'data': [{'b64_json': url.split(',')[1]}]})
        return httpx.Response(200, json=result)
    monkeypatch.setattr(httpx, 'Client', lambda **kw: client(transport=httpx.MockTransport(respond), **kw))
    try:
        output, call, _ = op.generate({'question': 'original question', 'images': images})
        saved = JsonArtifactRef(**call['input_ref']).read()
        assert saved['image_encoding'] == 'preserve'
        body = saved['body']
        urls = (body['image'] if api == 'images_json' else body['image[]'] if api == 'images' else
                [p['image_url']['url'] for p in (body['input_references'] if api == 'openrouter_images'
                 else body['messages'][0]['content'][1:])])
        assert [url.split(';')[0] for url in urls] == ['data:image/jpeg', 'data:image/png', 'data:image/webp']
        assert [base64.b64decode(url.split(',')[1]) for url in urls] == images
        if api == 'images':
            msg = BytesParser(policy=default).parsebytes(
                b'Content-Type: ' + seen[0].headers['content-type'].encode() + b'\r\n\r\n' + seen[0].content)
            files = [p for p in msg.iter_parts() if p.get_filename()]
            assert [p.get_filename() for p in files] == ['reference_1.jpg', 'reference_2.png', 'reference_3.webp']
            assert [p.get_content_type() for p in files] == ['image/jpeg', 'image/png', 'image/webp']
            assert [p.get_payload(decode=True) for p in files] == images
        else:
            assert json.loads(seen[0].content) == body
        assert op.generate({'question': 'original question', 'images': images})[0] == output
        assert len(seen) == 1
    finally:
        op.journal.close()


@pytest.mark.parametrize('fmt', ['GIF', 'BMP', 'TIFF'])
def test_preserve_converts_other_single_frame_formats(tmp_path, fmt):
    op = actor(tmp_path, image_encoding='preserve')
    prompt, images = op.render({'question': 'q', 'images': [encoded_picture(fmt)]})
    assert images[0].startswith(b'\x89PNG')
    with Image.open(io.BytesIO(images[0])) as im:
        assert im.size == (24, 16) and im.mode == 'RGB'
    assert op.request(prompt, images)['body']['input_references'][0]['image_url']['url'].startswith('data:image/png;')


def test_preserve_rejects_animation_and_corrupt_payload_before_reservation(tmp_path, monkeypatch):
    op = actor(tmp_path, image_encoding='preserve')
    monkeypatch.setattr(ImageGenerator, 'remote', lambda *a: pytest.fail('must not call provider'))
    buf = io.BytesIO()
    with Image.new('RGB', (16, 16), 'red') as one, Image.new('RGB', (16, 16), 'blue') as two:
        one.save(buf, format='GIF', save_all=True, append_images=[two])
    with pytest.raises(ValueError, match='single-frame'):
        op.generate({'question': 'q', 'images': [buf.getvalue()]})
    with pytest.raises((ValueError, OSError)):
        op.generate({'question': 'q', 'images': [encoded_picture('JPEG')[:-20]]})
    assert not (tmp_path / 'calls.sqlite').exists()


def test_png_expansion_limit_and_preserve_avoids_it(tmp_path, monkeypatch):
    import random
    with Image.frombytes('RGB', (256, 256), random.Random(42).randbytes(256 * 256 * 3)) as im:
        buf = io.BytesIO()
        im.save(buf, format='JPEG', quality=70)
    jpeg = buf.getvalue()
    limit = 64 * 1024
    assert len(jpeg) < limit
    values = {'question': 'q', 'images': [jpeg]}
    png = actor(tmp_path / 'png', limits={'max_image_bytes': limit})
    monkeypatch.setattr(ImageGenerator, 'remote', lambda *a: picture())
    with pytest.raises(ValueError, match='Encoded image exceeds'):
        png.generate(values)
    assert not (tmp_path / 'png/calls.sqlite').exists()
    keep = actor(tmp_path / 'preserve', image_encoding='preserve', limits={'max_image_bytes': limit})
    try:
        _, call, _ = keep.generate(values)
        url = JsonArtifactRef(**call['input_ref']).read()['body']['input_references'][0]['image_url']['url']
        assert base64.b64decode(url.split(',')[1]) == jpeg
    finally:
        keep.journal.close()


@pytest.mark.parametrize('limits,match', [
    ({'max_image_bytes': 100}, 'bounded image'),
    ({'max_pixels': 100}, 'max_pixels'),
    ({'max_total_pixels': 600}, 'aggregate'),
    ({'max_request_bytes': 1000}, 'request'),
])
def test_preserve_keeps_resource_limits_before_reservation(tmp_path, limits, match):
    op = actor(tmp_path, image_encoding='preserve', limits=limits)
    with pytest.raises(ValueError, match=match):
        op.generate({'question': 'q', 'images': [encoded_picture('JPEG')] * 2})
    assert not (tmp_path / 'calls.sqlite').exists()


def test_encoding_policy_identity_and_default_compatibility(tmp_path, monkeypatch):
    from demiflow.execution.artifacts import digest
    plain = actor(tmp_path)
    explicit = actor(tmp_path, image_encoding='png')
    keep = actor(tmp_path, image_encoding='preserve')
    values = {'question': 'q', 'images': [picture()]}
    legacy = plain.request(*plain.render(values))
    assert explicit.request(*explicit.render(values)) == legacy
    assert 'image_encoding' not in legacy
    preserved = keep.request(*keep.render(values))
    assert digest(preserved) != digest(legacy)
    empty = {'question': 'q', 'images': []}
    assert keep.request(*keep.render(empty)) == plain.request(*plain.render(empty))
    with pytest.raises(ValueError, match='image_encoding'):
        actor(tmp_path, image_encoding='jpeg')
    monkeypatch.setattr(ImageGenerator, 'remote', lambda *a: picture())
    rows = []
    ds = data.from_items([values]).map_image_async(template=plain.spec, model=plain.config,
        inputs=plain.inputs, output='image', call_output='call', error_output='error',
        journal_path=tmp_path / 'node.sqlite', object_store=tmp_path / 'objects', max_requests=1,
        image_encoding='preserve')
    assert ds._plan.operations[-1].image_encoding == 'preserve'
    ds.map(lambda row: rows.append(row) or row).run_stream(log_every=0)
    assert rows[0]['error'] is None
    assert JsonArtifactRef(**rows[0]['call']['input_ref']).read()['image_encoding'] == 'preserve'
