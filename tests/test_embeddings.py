"""Real HTTP, object reads and journals; deterministic fixture vectors, no GPU."""
import asyncio
import base64
from dataclasses import replace
import hashlib
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import io
import json
import sqlite3
import threading
import time

import pytest
from PIL import Image

from demiflow import data
from demiflow.embeddings import EmbeddingModel
from demiflow.embeddings.payload import EmbeddingProtocolError
from demiflow.execution.adaptive_requests import AdaptiveRequestGate
from demiflow.operator_llm.journal import UncertainPromptCall
from demiflow.operator_llm.sqlite_journal import SQLitePromptJournal


@pytest.fixture
def server():
    state = dict(bodies=[], active=0, peak=0, delay=0, status=200, response=None)
    lock = threading.Lock()
    class Handler(BaseHTTPRequestHandler):
        def do_POST(self):
            body = json.loads(self.rfile.read(int(self.headers['Content-Length'])))
            with lock:
                state['bodies'].append(body)
                state['active'] += 1
                state['peak'] = max(state['peak'], state['active'])
            try:
                if state.get('on_request'):
                    state['on_request']()
                time.sleep(state['delay'])
                values = body.get('input', body.get('messages'))
                response = dict(model='fixture', data=[dict(index=i, embedding=[3., 4., float(i)])
                                for i in reversed(range(len(values)))], usage={'prompt_tokens': 7})
                if state['response']:
                    response = state['response'](response)
                self.send_response(state['status'])
                self.end_headers()
                self.wfile.write(json.dumps(response).encode())
            except (BrokenPipeError, ConnectionResetError):
                pass
            finally:
                with lock:
                    state['active'] -= 1
        def log_message(self, *args):
            pass
    http = ThreadingHTTPServer(('127.0.0.1', 0), Handler)
    worker = threading.Thread(target=http.serve_forever, daemon=True)
    worker.start()
    state['model'] = EmbeddingModel('fixture', 'fixed-revision', 3,
                                    f'http://127.0.0.1:{http.server_port}/v1')
    yield state
    http.shutdown()
    http.server_close()
    worker.join()


def node(server, tmp_path, rows=None, **kwargs):
    return data.from_items(rows if rows is not None else [{'text': str(i)} for i in range(5)]).map_embeddings(
        model=kwargs.pop('model', server['model']), inputs=kwargs.pop('inputs', {'text': 'text'}),
        batch_size=kwargs.pop('batch_size', 2), call_output='call', error_output='error',
        options={'sqlite_journal': {'path': str(tmp_path / 'calls.sqlite')}}, **kwargs)


@pytest.mark.parametrize('expiry,expected_connections', [(0, 3), (5, 1)])
def test_keepalive_policy_controls_actual_connections(expiry, expected_connections):
    ports = set()
    class Handler(BaseHTTPRequestHandler):
        protocol_version = 'HTTP/1.1'
        def do_POST(self):
            self.rfile.read(int(self.headers['Content-Length']))
            ports.add(self.client_address[1])
            body = json.dumps({'data': [{'index': 0, 'embedding': [3., 4.]}]}).encode()
            self.send_response(200)
            self.send_header('Content-Length', str(len(body)))
            self.end_headers()
            self.wfile.write(body)
        def log_message(self, *args):
            pass
    http = ThreadingHTTPServer(('127.0.0.1', 0), Handler)
    worker = threading.Thread(target=http.serve_forever, daemon=True)
    worker.start()
    try:
        model = EmbeddingModel('fixture', 'fixed', 2, f'http://127.0.0.1:{http.server_port}/v1')
        rows = data.from_items([{'text': str(i)} for i in range(3)]).map_embeddings(
            model=model, inputs={'text': 'text'}, batch_size=1, concurrency=1,
            options={'keepalive_expiry_s': expiry}).materialize().take(3)
        assert len(rows) == 3 and len(ports) == expected_connections
    finally:
        http.shutdown()
        http.server_close()
        worker.join()


def test_batch_indices_normalization_replay_and_plan(server, tmp_path):
    from demiflow.data.plan import EmbeddingMapOp
    ds = node(server, tmp_path, concurrency=2, queue_depth=1)
    assert isinstance(ds._plan.operations[-1], EmbeddingMapOp)
    first = ds.materialize().take_all()
    assert len(first) == 5 and len(server['bodies']) == 3
    assert all(row['embedding'][0] == pytest.approx(.6) for row in first if row['call']['index'] == 0)
    assert all(sum(v*v for v in row['embedding']) == pytest.approx(1) for row in first)
    assert all(row['error'] is None and not row['call']['reused'] for row in first)
    again = ds.materialize().take_all()
    assert len(server['bodies']) == 3 and all(row['call']['reused'] for row in again)
    assert ds._stages[-1]._pool is ds._stages[-1]._client is None


def test_images_fallback_sha_decode_and_text_share_chat_protocol(server, tmp_path):
    image = tmp_path / 'image.png'
    Image.new('RGB', (12, 8), (42, 0, 0)).save(image)
    sha = hashlib.sha256(image.read_bytes()).hexdigest()
    model = replace(server['model'], input_format='chat')
    rows = [{'image': [{'uri': (tmp_path / 'missing.png').as_uri(), 'sha256': sha},
                       {'uri': image.as_uri(), 'sha256': sha}]},
            {'image': {'uri': image.as_uri(), 'sha256': 'a' * 64}}]
    output = node(server, tmp_path, rows, model=model, inputs={'image': 'image'}).materialize().take_all()
    assert sum(row['error'] is not None for row in output) == 1
    good = next(row for row in output if row['error'] is None)
    assert good['call']['image_uri'] == image.as_uri()
    content = server['bodies'][0]['messages'][0][0]['content'][0]
    with Image.open(io.BytesIO(base64.b64decode(content['image_url']['url'].split(',')[1]))) as actual:
        assert actual.getpixel((0, 0)) == (42, 0, 0)
    node(server, tmp_path, [{'text': 'red object'}], model=model).materialize()
    assert server['bodies'][-1]['messages'] == [[{'role': 'user', 'content': [{'type': 'text', 'text': 'red object'}]}]]
    with sqlite3.connect(tmp_path / 'calls.sqlite') as db:
        requests = ' '.join(row[0] for row in db.execute('SELECT request_json FROM calls'))
    assert 'data:image' not in requests and 'data_uri_sha256' in requests


@pytest.mark.parametrize('fault', ['dimension', 'duplicate', 'nan', 'zero', 'missing', 'model'])
def test_invalid_response_is_durable_and_never_published(server, tmp_path, fault):
    def invalid(response):
        if fault == 'dimension': response['data'][0]['embedding'] = [1]
        if fault == 'duplicate': response['data'][1]['index'] = response['data'][0]['index']
        if fault == 'nan': response['data'][0]['embedding'][0] = float('nan')
        if fault == 'zero': response['data'][0]['embedding'] = [0, 0, 0]
        if fault == 'missing': response['data'].pop()
        if fault == 'model': response['model'] = 'wrong-model'
        return response
    server['response'] = invalid
    ds = node(server, tmp_path, [{'text': 'a'}, {'text': 'b'}])
    for _ in range(2):
        with pytest.raises(EmbeddingProtocolError):
            ds.materialize()
    assert len(server['bodies']) == 1


def test_http_error_is_durable_and_requires_explicit_recovery(server, tmp_path):
    import httpx
    server['status'] = 503
    ds = node(server, tmp_path, [{'text': 'a'}])
    with pytest.raises(httpx.HTTPStatusError): ds.materialize()
    server['status'] = 200
    with pytest.raises(httpx.HTTPStatusError): ds.materialize()
    assert len(server['bodies']) == 1
    with sqlite3.connect(tmp_path / 'calls.sqlite') as db:
        key = db.execute('SELECT request_key FROM calls').fetchone()[0]
    journal = SQLitePromptJournal(tmp_path / 'calls.sqlite')
    try:
        journal.requeue_http_errors([key], expected_statuses=[503], actor='test',
                                   reason='explicit retry', operation_id='test-retry')
    finally:
        journal.close()
    assert len(ds.materialize().take_all()) == 1 and len(server['bodies']) == 2


def test_adaptive_gate_bounds_fresh_requests_and_excludes_replay(server, tmp_path):
    server['delay'] = .06
    gate = AdaptiveRequestGate(dict(initial_concurrency=1, max_concurrency=3,
        window_s=.001, min_samples=1, recovery_s=.001, increase_step=1))
    ds = node(server, tmp_path, [{'text': str(i)} for i in range(9)],
              batch_size=1, concurrency=3, queue_depth=1, request_gate=gate)
    ds.materialize()
    assert 1 < server['peak'] <= 3 and gate.capacity == 3 and gate.admitted == 9
    ds.materialize()
    assert gate.admitted == 0 and gate.fresh_responses == 0


def test_duplicate_concurrent_batches_singleflight(server, tmp_path):
    server['delay'] = .05
    result = node(server, tmp_path, [{'text': 'same'}] * 6, batch_size=1, concurrency=3).materialize().take_all()
    assert len(result) == 6 and len(server['bodies']) == 1


def test_readonly_miss_and_empty_inputs_never_dispatch(server, tmp_path):
    node(server, tmp_path, [{'text': 'stored'}]).materialize()
    ds = data.from_items([{'text': 'absent'}]).map_embeddings(model=server['model'],
        inputs={'text': 'text'}, options={'sqlite_journal': {'path': str(tmp_path / 'calls.sqlite'), 'read_only': True}})
    with pytest.raises(LookupError, match='read-only'): ds.materialize()
    assert node(server, tmp_path, []).materialize().take_all() == []
    result = node(server, tmp_path, [{'text': ''}]).materialize().take_all()
    assert result[0]['embedding'] is None and result[0]['error']
    assert len(server['bodies']) == 1


def test_cached_requests_need_no_credentials_and_missing_key_is_not_reserved(server, tmp_path, monkeypatch):
    model = replace(server['model'], api_key_env='TEST_EMBEDDING_KEY')
    monkeypatch.setenv('TEST_EMBEDDING_KEY', 'fixture-key')
    node(server, tmp_path, [{'text': 'stored'}], model=model).materialize()
    monkeypatch.delenv('TEST_EMBEDDING_KEY')
    assert node(server, tmp_path, [{'text': 'stored'}], model=model).materialize().take_all()[0]['call']['reused']
    with pytest.raises(ValueError, match='API key'):
        node(server, tmp_path, [{'text': 'fresh'}], model=model).materialize()
    with sqlite3.connect(tmp_path / 'calls.sqlite') as db:
        assert db.execute('SELECT count(*) FROM calls').fetchone()[0] == 1


def test_service_cleanup_and_cached_only_no_start(server, tmp_path, monkeypatch):
    from demiflow.services import ManagedHTTPService
    state = dict(started=0, closed=0)
    class Owner:
        async def ensure_ready(self): state['started'] += 1
        async def aclose(self): state['closed'] += 1
    service = ManagedHTTPService(['unused'], base_url=server['model'].base_url, root=tmp_path)
    monkeypatch.setattr(service, 'bind', lambda _: Owner())
    ds = node(server, tmp_path, [{'text': 'a'}], service=service)
    ds.materialize()
    ds.materialize()
    assert state == dict(started=1, closed=2)


def test_real_managed_process_is_reaped_and_replay_does_not_spawn(tmp_path, monkeypatch):
    import os
    import signal
    import socket
    import sys
    from demiflow.services import ManagedHTTPService
    from demiflow.services import http as service_module
    with socket.socket() as probe:
        probe.bind(('127.0.0.1', 0))
        port = probe.getsockname()[1]
    script = tmp_path / 'embedding_server.py'
    script.write_text('''import json
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
class Handler(BaseHTTPRequestHandler):
    def do_GET(self):
        self.send_response(200); self.end_headers()
        self.wfile.write(b'{"data":[{"id":"fixture"}]}')
    def do_POST(self):
        body=json.loads(self.rfile.read(int(self.headers['Content-Length'])))
        self.send_response(200); self.end_headers()
        self.wfile.write(json.dumps({'model':'fixture','data':[
            {'index':i,'embedding':[3,4,0]} for i in range(len(body['input']))]}).encode())
    def log_message(self,*args): pass
ThreadingHTTPServer(('127.0.0.1', ''' + str(port) + '''), Handler).serve_forever()
''')
    endpoint = f'http://127.0.0.1:{port}/v1'
    service = ManagedHTTPService([sys.executable, str(script)], base_url=endpoint,
        expected_model='fixture', root=tmp_path, startup_timeout_s=5,
        shutdown_timeout_s=.5, poll_interval_s=.01)
    processes = []
    original = service_module.subprocess.Popen
    def spawn(*args, **kwargs):
        process = original(*args, **kwargs)
        processes.append(process)
        return process
    monkeypatch.setattr(service_module.subprocess, 'Popen', spawn)
    try:
        fixture = {'model': EmbeddingModel('fixture', 'fixed', 3, endpoint)}
        ds = node(fixture, tmp_path, [{'text': 'a'}], service=service)
        assert len(ds.materialize().take_all()) == 1
        assert len(processes) == 1 and processes[0].poll() is not None
        assert ds.materialize().take_all()[0]['call']['reused']
        assert len(processes) == 1
    finally:
        for process in processes:
            if process.poll() is None:
                os.killpg(process.pid, signal.SIGKILL)
                process.wait()


def test_budget_and_blocking_io_stay_bounded(server, tmp_path, monkeypatch):
    from demiflow.embeddings import runtime
    from demiflow.operator_llm.errors import PromptBudgetExceededError
    original = runtime.prepare
    state = dict(active=0, peak=0, threads=set())
    lock = threading.Lock()
    def slow_prepare(*args, **kwargs):
        with lock:
            state['active'] += 1
            state['peak'] = max(state['peak'], state['active'])
            state['threads'].add(threading.current_thread().name)
        try:
            time.sleep(.03)
            return original(*args, **kwargs)
        finally:
            with lock: state['active'] -= 1
    monkeypatch.setattr(runtime, 'prepare', slow_prepare)
    ds = node(server, tmp_path, batch_size=1, concurrency=4, max_requests=2)
    with pytest.raises(PromptBudgetExceededError): ds.materialize()
    assert len(server['bodies']) <= 2 and state['peak'] <= 2
    assert all(name.startswith('demiflow-embeddings') for name in state['threads'])
    assert state['active'] == 0


def test_budget_without_journal_is_atomic_across_workers(server):
    from demiflow.operator_llm.errors import PromptBudgetExceededError
    ds = data.from_items([{'text': str(i)} for i in range(10)]).map_embeddings(
        model=server['model'], inputs={'text': 'text'}, batch_size=1, concurrency=4, max_requests=1)
    with pytest.raises(PromptBudgetExceededError): ds.materialize()
    assert len(server['bodies']) <= 1


@pytest.mark.parametrize('prefetch', [0, 2])
def test_cooperative_stop_drains_only_admitted_calls(server, tmp_path, monkeypatch, prefetch):
    import demiflow.execution.stream as stream
    from demiflow.execution.request_limits import ServiceStopped
    monkeypatch.setattr(stream, '_WATCHDOG_INTERVAL', .01)
    stop = tmp_path / 'STOP'
    server['delay'] = .15
    def stop_after_two():
        if len(server['bodies']) == 2: stop.touch()
    server['on_request'] = stop_after_two
    ds = node(server, tmp_path, batch_size=1, concurrency=2, queue_depth=1, prefetch_batches=prefetch)
    with pytest.raises(ServiceStopped, match='operator_stop_file'):
        ds.run_stream(stop_file=stop)
    assert len(server['bodies']) == 2
    with sqlite3.connect(tmp_path / 'calls.sqlite') as db:
        assert db.execute('SELECT count(*) FROM calls WHERE response_json IS NOT NULL').fetchone()[0] == 2
        assert db.execute('SELECT count(*) FROM calls WHERE error_json IS NOT NULL').fetchone()[0] == 0
    assert ds._stages[-1]._pool is ds._stages[-1]._prepare_pool is None


@pytest.mark.asyncio
async def test_cancellation_drains_journal_and_closes_pool(server, tmp_path):
    from demiflow.execution.stream import _arun, _materialize, StreamStats
    from demiflow.execution.stream_resources import StreamResources
    server['delay'] = 1
    ds = node(server, tmp_path, [{'text': 'a'}])
    resources = StreamResources(ds._stages)
    async def execute():
        try:
            await resources.prepare()
            await _arun(iter([{'text': 'a'}]), _materialize(ds._plan), StreamStats(),
                        on_progress=None, on_drain=None, log_every=0, cancellation=None)
        finally:
            await resources.close()
    task = asyncio.create_task(execute())
    for _ in range(100):
        if server['bodies']: break
        await asyncio.sleep(.01)
    assert server['bodies']
    task.cancel()
    with pytest.raises(asyncio.CancelledError): await task
    assert ds._stages[-1]._pool is ds._stages[-1]._client is None
    with sqlite3.connect(tmp_path / 'calls.sqlite') as db:
        assert db.execute('SELECT error_json FROM calls').fetchone()[0]


def test_declaration_validates_limits_and_pooling_command(server, tmp_path):
    from demiflow.services.vllm import vllm_config, _service_command
    for options in ({'concurrency': 0}, {'batch_size': True}, {'queue_depth': 0}):
        with pytest.raises(ValueError): node(server, tmp_path, **options)
    with pytest.raises(ValueError):
        node(server, tmp_path, inputs={'image': 'image'})
    spec = vllm_config(dict(model_path='models/test', runner='pooling',
                            chat_template='models/test/template.jinja', dtype='bfloat16'))
    argv, _ = _service_command(spec, {'base_url': server['model'].base_url, 'name': 'fixture'}, tmp_path)
    assert argv[argv.index('--runner') + 1] == 'pooling'
    assert argv[argv.index('--chat-template') + 1] == str(tmp_path / 'models/test/template.jinja')


@pytest.mark.parametrize('format', ['JPEG', 'PNG', 'WEBP'])
@pytest.mark.parametrize('icc', [False, True])
def test_original_transport_preserves_bytes_and_normalized_pixels(tmp_path, format, icc):
    from demiflow.embeddings.payload import image_content
    from demiflow.embeddings.runtime import runtime_options
    image = tmp_path / 'picture'
    from PIL import ImageCms
    profile = ImageCms.ImageCmsProfile(ImageCms.createProfile('sRGB')).tobytes() if icc else None
    Image.new('RGB', (31, 17), (92, 37, 203)).save(image, format=format,
        **({'icc_profile': profile} if profile else {}))
    raw = image.read_bytes()
    ref = dict(uri=image.as_uri(), sha256=hashlib.sha256(raw).hexdigest())
    timings = {}
    optimized, _ = image_content(ref, runtime_options(None), transport='original_if_compatible', timings=timings)
    legacy, _ = image_content(ref, runtime_options(None))
    direct = base64.b64decode(optimized['image_url']['url'].split(',')[1])
    assert direct == raw and timings['original_images'] == 1
    with Image.open(io.BytesIO(direct)) as a, Image.open(io.BytesIO(
            base64.b64decode(legacy['image_url']['url'].split(',')[1]))) as b:
        assert a.size == b.size and a.convert('RGB').tobytes() == b.tobytes()
        assert a.info.get('icc_profile') == b.info.get('icc_profile')


@pytest.mark.parametrize('kind', ['orientation', 'alpha', 'palette', 'cmyk', 'animation'])
def test_original_transport_falls_back_without_changing_pixels(tmp_path, kind):
    from demiflow.embeddings.payload import image_content
    from demiflow.embeddings.runtime import runtime_options
    image = tmp_path / 'picture'
    pic = Image.new({'alpha': 'RGBA', 'palette': 'P', 'cmyk': 'CMYK'}.get(kind, 'RGB'), (31, 17))
    options = {}
    format = 'JPEG' if kind in {'cmyk', 'orientation'} else 'PNG'
    if kind == 'orientation':
        exif = Image.Exif(); exif[274] = 6
        options['exif'] = exif
    if kind == 'animation':
        options.update(save_all=True, append_images=[Image.new('RGB', (31, 17), 'red')], duration=100)
    pic.save(image, format=format, **options)
    pic.close()
    raw = image.read_bytes()
    ref = dict(uri=image.as_uri(), sha256=hashlib.sha256(raw).hexdigest())
    timings = {}
    optimized, _ = image_content(ref, runtime_options({'png_compress_level': 1}),
                                 transport='original_if_compatible', timings=timings)
    legacy, _ = image_content(ref, runtime_options(None))
    assert timings['png_images'] == 1
    with Image.open(io.BytesIO(base64.b64decode(optimized['image_url']['url'].split(',')[1]))) as a, \
         Image.open(io.BytesIO(base64.b64decode(legacy['image_url']['url'].split(',')[1]))) as b:
        assert a.mode == 'RGB' and a.size == b.size and a.tobytes() == b.tobytes()


def test_original_transport_still_rejects_corruption_and_budgets(tmp_path):
    from demiflow.embeddings.payload import image_content, EmbeddingInputError, _LimitedBuffer
    from demiflow.embeddings.runtime import runtime_options
    path = tmp_path / 'broken.jpg'
    Image.new('RGB', (512, 512), 'red').save(path, format='JPEG')
    raw = path.read_bytes()[:-100]
    path.write_bytes(raw)
    ref = dict(uri=path.as_uri(), sha256=hashlib.sha256(raw).hexdigest())
    for options in ({}, {'max_image_bytes': 10}, {'max_decode_pixels': 100}):
        with pytest.raises(EmbeddingInputError):
            image_content(ref, runtime_options(options), transport='original_if_compatible')
    with _LimitedBuffer(5) as buffer:
        buffer.write(b'1234')
        with pytest.raises(ValueError, match='max_image_bytes'): buffer.write(b'56')
        assert len(buffer.getvalue()) == 4


def test_png_fallback_omits_oversized_profile_without_changing_rgb(tmp_path):
    from PIL import ImageOps, PngImagePlugin
    from demiflow.embeddings.payload import image_content
    from demiflow.embeddings.runtime import runtime_options
    path = tmp_path / 'large-profile.jpg'
    Image.new('CMYK', (31, 17), (12, 65, 103, 17)).save(path,
        icc_profile=b'x' * (PngImagePlugin.MAX_TEXT_CHUNK + 1))
    raw = path.read_bytes()
    timings = {}
    content, _ = image_content({'uri': path.as_uri(), 'sha256': hashlib.sha256(raw).hexdigest()},
        runtime_options(None), transport='original_if_compatible', timings=timings)
    with Image.open(path) as original, Image.open(io.BytesIO(
            base64.b64decode(content['image_url']['url'].split(',')[1]))) as normalized:
        normalized.load()
        assert normalized.mode == 'RGB'
        assert normalized.tobytes() == ImageOps.exif_transpose(original).convert('RGB').tobytes()
        assert 'icc_profile' not in normalized.info
    assert timings['oversized_png_profiles_omitted'] == 1


def test_monochrome_transparency_fallback_is_valid_rgb(tmp_path):
    from PIL import ImageOps
    from demiflow.embeddings.payload import image_content
    from demiflow.embeddings.runtime import runtime_options
    path = tmp_path / 'monochrome.png'
    Image.new('1', (31, 17), 1).save(path, transparency=255)
    raw = path.read_bytes()
    content, _ = image_content({'uri': path.as_uri(), 'sha256': hashlib.sha256(raw).hexdigest()},
        runtime_options(None), transport='original_if_compatible')
    with Image.open(path) as original, Image.open(io.BytesIO(
            base64.b64decode(content['image_url']['url'].split(',')[1]))) as normalized:
        normalized.load()
        assert normalized.mode == 'RGB'
        assert normalized.tobytes() == ImageOps.exif_transpose(original).convert('RGB').tobytes()


@pytest.mark.parametrize('dpi', [None, (72, 72)])
def test_invalid_exif_preserves_stored_pixels_and_does_not_pass_bad_metadata(tmp_path, dpi):
    from demiflow.embeddings.payload import image_content
    from demiflow.embeddings.runtime import runtime_options
    path = tmp_path / 'invalid-exif.jpg'
    Image.new('RGB', (31, 17), (81, 36, 199)).save(path,
        exif=b'Exif\x00\x00not-tiff-at-all', **({'dpi': dpi} if dpi else {}))
    raw = path.read_bytes()
    timings = {}
    content, _ = image_content({'uri': path.as_uri(), 'sha256': hashlib.sha256(raw).hexdigest()},
        runtime_options(None), transport='original_if_compatible', timings=timings)
    with Image.open(path) as original, Image.open(io.BytesIO(
            base64.b64decode(content['image_url']['url'].split(',')[1]))) as normalized:
        assert normalized.tobytes() == original.convert('RGB').tobytes()
        assert not normalized.getexif()
    assert timings['invalid_exif_omitted'] == 1


def test_prefetch_overlaps_cpu_http_and_profiles_without_exceeding_request_limit(server, tmp_path, monkeypatch):
    from demiflow.embeddings import runtime
    original = runtime.prepare
    http_started, second_prepared = threading.Event(), threading.Event()
    overlaps = []
    def prepare(*args, **kwargs):
        if args[0][0]['text'] == 'second':
            assert http_started.wait(5)
            overlaps.append(server['active'] == 1)
            result = original(*args, **kwargs)
            second_prepared.set()
            return result
        return original(*args, **kwargs)
    def request():
        if len(server['bodies']) == 1:
            http_started.set()
            assert second_prepared.wait(5)
    server['on_request'] = request
    monkeypatch.setattr(runtime, 'prepare', prepare)
    profile = tmp_path / 'profile.json'
    ds = data.from_items([{'text': 'first'}, {'text': 'second'}, {'text': 'third'}]).map_embeddings(
        model=server['model'], inputs={'text': 'text'}, batch_size=1, concurrency=1,
        prefetch_batches=1, call_output='call', options={'prepare_workers': 1, 'io_workers': 1,
            'profile_path': str(profile), 'sqlite_journal': {'path': str(tmp_path / 'calls.sqlite')}})
    rows = ds.materialize().take_all()
    assert overlaps == [True] and server['peak'] == 1 and len(rows) == 3
    assert all(row['call']['timings']['prepare_s'] >= 0 for row in rows)
    report = json.loads(profile.read_text())
    assert report['profiled_batches'] == 3 and report['call_records'] == 3
    assert report['phases']['journal_s']['total'] > 0
    assert ds._stages[-1]._prepare_pool is ds._stages[-1]._pool is None


def test_transport_identity_and_prefetch_validation(server, tmp_path):
    legacy = server['model']
    assert replace(legacy, image_transport='original_if_compatible').fingerprint != legacy.fingerprint
    for value in (-1, True):
        with pytest.raises(ValueError, match='prefetch_batches'):
            node(server, tmp_path, prefetch_batches=value)


@pytest.mark.parametrize('format', ['text', 'chat'])
def test_single_serialization_keeps_exact_http_bytes_and_journal_key(server, format):
    from demiflow.embeddings.payload import prepare
    from demiflow.embeddings.runtime import runtime_options
    from demiflow.embeddings.model import canonical
    from demiflow.operator_llm.journal import RequestRecord
    model = replace(server['model'], input_format=format,
        request_options={'dimensions_hint': 1.5, 'z': {'nested': ['中文', None, True]}},
        encoding_parameters={'revision_note': '引号"\\\n', 'number': .125})
    *_, encoded, request = prepare([{'text': '中文"\\\n'}, {'text': '第二张'}],
        model=model, inputs={'text': 'text'}, options=runtime_options(None), error_output=None)
    assert encoded == canonical(request['body']).encode()
    assert request.request_key == RequestRecord(request).request_key


def test_profile_write_failure_still_closes_owned_resources(server, tmp_path):
    path = tmp_path / 'directory'; path.mkdir()
    ds = data.from_items([{'text': 'a'}]).map_embeddings(model=server['model'], inputs={'text': 'text'},
        options={'profile_path': str(path)})
    with pytest.raises(ExceptionGroup): ds.materialize()
    actor = ds._stages[-1]
    assert actor._pool is actor._prepare_pool is actor._response_pool is actor._client is None and not actor._started


@pytest.mark.asyncio
async def test_response_pool_progresses_while_journal_worker_is_busy(server):
    from demiflow.embeddings.runtime import EmbeddingActor
    actor = EmbeddingActor(model=server['model'], inputs={'text': 'text'}, output='embedding',
        call_output=None, error_output=None, concurrency=1, label='pool_test',
        options={'io_workers': 1, 'response_workers': 1}, max_requests=None, service=None, request_gate=None)
    busy, released = threading.Event(), threading.Event()
    def slow_journal():
        busy.set()
        assert released.wait(5)
    await actor.astart()
    task = asyncio.create_task(actor._io(slow_journal))
    try:
        assert await asyncio.to_thread(busy.wait, 5)
        name = await asyncio.wait_for(actor._io(lambda: threading.current_thread().name,
                                                _response=True), 2)
        assert name.startswith('demiflow-embeddings-response')
    finally:
        released.set()
        await task
        await actor.aclose()
    assert actor._pool is actor._prepare_pool is actor._response_pool is None


def test_byte_packing_large_singleton_errors_and_exact_replay(server, tmp_path):
    from demiflow.embeddings.model import canonical
    rows = [{'id': i, 'text': value} for i, value in enumerate(
        ['a'*90, 'b'*90, None, 'c'*650, 'd'*90, 'e'*90, 'x'*1100])]
    options = {'max_request_bytes': 1024, 'batch_request_bytes': 300,
               'sqlite_journal': {'path': str(tmp_path/'packing.sqlite')}}
    def plan(concurrency):
        return data.from_items(rows).map_embeddings(model=server['model'], inputs={'text':'text'},
            batch_size=7, concurrency=concurrency, call_output='call', error_output='error', options=options)
    first = plan(1).materialize().take_all()
    assert sorted(r['id'] for r in first) == list(range(7))
    assert sum(r['error'] is not None for r in first) == 2
    assert [len(b['input']) for b in server['bodies']] == [2, 1, 2]
    assert all(len(canonical(b).encode()) <= 1024 for b in server['bodies'])
    assert all(len(canonical(b).encode()) <= 300 or len(b['input']) == 1 for b in server['bodies'])
    for row in first:
        if not row['error']:
            assert row['embedding'][2] == pytest.approx(row['call']['index'] / (25 + row['call']['index']**2)**.5)
            assert row['call']['response_ref'].get('attempt', 1) == 1
    second = plan(3).materialize().take_all()
    assert len(server['bodies']) == 3
    assert all(r['call']['reused'] for r in second if not r['error'])
    assert {r['id']:r['embedding'] for r in second} == {r['id']:r['embedding'] for r in first}


def test_byte_packing_decodes_each_image_once_and_preserves_pixels(server, tmp_path, monkeypatch):
    from demiflow.embeddings import payload
    seen=[]
    original=payload.image_content
    def observed(value, *args, **kwargs):
        seen.append(value['sha256'])
        return original(value, *args, **kwargs)
    monkeypatch.setattr(payload, 'image_content', observed)
    rows=[]
    for i, size in enumerate([24, 32, 128, 40, 48]):
        path=tmp_path/f'{i}.png'
        Image.effect_noise((size,size),100).convert('RGB').save(path)
        rows.append({'id':i, 'image':{'uri':path.as_uri(),'sha256':hashlib.sha256(path.read_bytes()).hexdigest()}})
    model=replace(server['model'], input_format='chat', image_transport='original_if_compatible')
    result=data.from_items(rows).map_embeddings(model=model, inputs={'image':'image'},
        batch_size=5, call_output='call', error_output='error',
        options={'batch_request_bytes':8000,'max_request_bytes':128*1024}).materialize().take_all()
    assert len(result)==len(seen)==5 and all(r['error'] is None for r in result)
    payloads=[base64.b64decode(m[0]['content'][0]['image_url']['url'].split(',')[1])
              for b in server['bodies'] for m in b['messages']]
    assert [hashlib.sha256(p).hexdigest() for p in payloads] == seen
    assert len(server['bodies']) > 1


@pytest.mark.parametrize('target', [0, -1, True, 1.5, 1025])
def test_invalid_packing_budget_rejected_before_execution(server, target):
    with pytest.raises(ValueError, match='batch_request_bytes'):
        data.from_items([{'text':'a'}]).map_embeddings(model=server['model'], inputs={'text':'text'},
            options={'max_request_bytes':1024, 'batch_request_bytes':target})
    assert not server['bodies']


@pytest.mark.parametrize('target', [0, -1, True, 1.5])
def test_invalid_pixel_packing_budget_rejected_before_execution(server, target):
    with pytest.raises(ValueError, match='batch_decode_pixels'):
        data.from_items([{'text':'a'}]).map_embeddings(model=server['model'], inputs={'text':'text'},
            options={'batch_decode_pixels':target})
    assert not server['bodies']


def test_split_failure_replays_committed_groups_without_dispatching_tail(server, tmp_path):
    def malformed_second(response):
        if len(server['bodies']) == 2:
            response['data'][0]['embedding'] = [1]
        return response
    server['response'] = malformed_second
    values = [{'text': chr(97+i)*150} for i in range(4)]
    ds = data.from_items(values).map_embeddings(model=server['model'], inputs={'text':'text'},
        batch_size=4, call_output='call', error_output='error', options={
            'batch_request_bytes':250, 'max_request_bytes':1024,
            'sqlite_journal':{'path':str(tmp_path/'split_failure.sqlite')}})
    for _ in range(2):
        with pytest.raises(EmbeddingProtocolError):
            ds.materialize()
    assert len(server['bodies']) == 2
    with sqlite3.connect(tmp_path/'split_failure.sqlite') as db:
        assert db.execute('SELECT count(*) FROM calls WHERE response_json IS NOT NULL').fetchone()[0] == 2
    assert ds._stages[-1]._prepare_pool is None


@pytest.mark.parametrize('transport', ['png','original_if_compatible'])
def test_fast_image_serialization_preserves_exact_canonical_request(tmp_path, transport):
    from demiflow.embeddings.payload import prepare_batches
    from demiflow.embeddings.runtime import runtime_options
    from demiflow.embeddings.model import canonical
    from demiflow.operator_llm.journal import request_key
    path=tmp_path/'image.jpg';Image.new('RGB',(29,31),(32,81,157)).save(path)
    model=EmbeddingModel('fixtüré','r',3,'http://localhost:8002/v1',input_format='chat',image_transport=transport)
    row={'image':{'uri':path.as_uri(),'sha256':hashlib.sha256(path.read_bytes()).hexdigest()}}
    opts=runtime_options({'batch_request_bytes':4096})
    groups=list(prepare_batches([row],model=model,inputs={'image':'image'},options=opts,error_output='error'))
    assert len(groups)==1
    _,invalid,_,encoded,request=groups[0]
    assert not invalid and encoded==canonical(request['body']).encode()
    assert request.request_key==request_key(dict(request))


def test_history_audit_can_be_skipped_without_losing_action_metrics(server, tmp_path, monkeypatch):
    from demiflow.operator_llm import call_ref
    def forbidden(path):
        raise AssertionError('Must not scan historical response bodies at action drain')
    monkeypatch.setattr(call_ref,'journal_totals',forbidden)
    ds=data.from_items([{'text':'one'},{'text':'two'}]).map_embeddings(
        model=server['model'],inputs={'text':'text'},batch_size=1,call_output='call',
        options={'collect_journal_totals':False,'sqlite_journal':{'path':str(tmp_path/'history.sqlite')}})
    for reused in [0,2]:
        stats=ds.run_stream();metrics=stats.metrics
        model=next(iter(metrics['models'].values()))
        assert model['call_records']==2 and model['reused']==reused
        assert metrics['native_journal_totals']=={}
        assert metrics['native_journal_totals_skipped'][0]['reason']=='collect_journal_totals=False'
    assert len(server['bodies'])==2


def test_compressible_images_pack_by_decoded_pixels_not_encoded_bytes(server, tmp_path):
    model=replace(server['model'],input_format='chat',image_transport='original_if_compatible')
    rows=[]
    for i,size in enumerate([64,64,128,64]):
        p=tmp_path/f'flat{i}.png';Image.new('RGB',(size,size),(i,0,0)).save(p)
        rows.append({'id':i,'image':{'uri':p.as_uri(),'sha256':hashlib.sha256(p.read_bytes()).hexdigest()}})
    ds=data.from_items(rows).map_embeddings(model=model,inputs={'image':'image'},batch_size=4,
        call_output='call',error_output='error',options={'batch_request_bytes':32*1024,
        'batch_decode_pixels':9000,'max_decode_pixels':20000})
    out=ds.materialize().take_all()
    assert [len(b['messages']) for b in server['bodies']]==[2,1,1]
    assert sorted(r['id'] for r in out)==[0,1,2,3] and all(r['error'] is None for r in out)
    assert [r['call']['image_pixels'] for r in out]==[4096,4096,16384,4096]
