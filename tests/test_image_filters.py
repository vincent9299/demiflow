import asyncio
import gzip
import io

import httpx
import pytest
from PIL import Image

from demiflow.collect.image_fetch import ImageFetchPolicy
from demiflow.collect.image_filters import ImageFilters, ImageHeaderProbe, header_dimensions
from demiflow.collect.web import WebClient
from test_image_fetch import execute, network, pixels, session


def test_filter_config_validation_and_boundaries():
    assert ImageFetchPolicy(filters={}).filters is None
    assert ImageFetchPolicy(filters={'min_long_side': 1024}).filters == ImageFilters(min_long_side=1024)
    for bad in (0, -1, True, '1024', 1.5, 2**63):
        with pytest.raises(ValueError):
            ImageFetchPolicy(filters={'min_long_side': bad})
    with pytest.raises(TypeError):
        ImageFetchPolicy(filters={'unknown_condition': 100})
    policy = ImageFilters(min_width=6, min_height=6, min_long_side=8, min_short_side=6, min_pixels=48)
    assert policy.rejection(8, 6) is None
    assert policy.rejection(6, 8) is None
    assert policy.rejection(8, 5).startswith('min_height:')
    assert ImageFilters(min_long_side=8).rejection(7, 7).startswith('min_long_side:')
    assert ImageFilters(min_short_side=6).rejection(8, 5).startswith('min_short_side:')
    assert ImageFilters(min_pixels=49).rejection(8, 6).startswith('min_pixels:')


@pytest.mark.parametrize('fmt,options', [
    ('PNG', {}), ('JPEG', {}), ('JPEG', {'progressive': True}), ('GIF', {}),
    ('BMP', {}), ('WEBP', {}), ('WEBP', {'lossless': True}),
])
def test_headers_agree_with_decoded_sizes(fmt, options):
    stream = io.BytesIO()
    Image.new('RGB', (257, 113), 'red').save(stream, format=fmt, **options)
    raw = stream.getvalue()
    assert header_dimensions(raw[:128*1024]) == (257, 113)
    # Every short prefix must remain safe; ambiguous input defers to decode.
    for length in range(min(len(raw), 100)):
        assert header_dimensions(raw[:length]) in (None, (257, 113))


def test_header_probe_memory_is_bounded_and_unknown_defers():
    probe = ImageHeaderProbe(ImageFilters(min_long_side=1024))
    for _ in range(32):
        probe.feed(b'not an image' * 1024)
        assert len(probe.prefix) <= probe.max_bytes
    assert probe.done and not probe.prefix


def test_declared_size_skips_http_and_different_metadata_gets_new_receipt(tmp_path, monkeypatch):
    calls = network(monkeypatch, pixels((16, 12)))
    request = {'request_id': 'r', 'url': 'https://images.example/a', 'declared_width': 8, 'declared_height': 6}
    policy = {'filters': {'min_long_side': 16}}
    bad = execute(session(tmp_path, **policy), [request])[0]['result']
    assert bad['status'] == 'filtered' and bad['filter_stage'] == 'declared'
    assert bad['image_ref'] is None and bad['attempts'] == [] and not calls
    assert execute(session(tmp_path, **policy), [request])[0]['result'] == bad
    request.update(declared_width=16, declared_height=12)
    good = execute(session(tmp_path, **policy), [request])[0]['result']
    assert good['status'] == 'ok' and good['width'] == 16 and len(calls) == 1


@pytest.mark.parametrize('metadata', [
    {'declared_width': 8}, {'declared_width': True, 'declared_height': 6},
    {'declared_width': 8, 'declared_height': -1}, {'declared_width': 2**31, 'declared_height': 6},
])
def test_invalid_declared_dimensions_rejected_before_http(tmp_path, monkeypatch, metadata):
    calls = network(monkeypatch, pixels())
    request = {'request_id': 'r', 'url': 'https://images.example/a', **metadata}
    with pytest.raises(ValueError, match='declared_width'):
        execute(session(tmp_path, filters={'min_long_side': 8}), [request])
    assert not calls


@pytest.mark.parametrize('metadata,reason', [
    ({'declared_file_bytes': 1000001}, 'max_bytes:'),
    ({'mime_type': 'application/pdf'}, 'mime_type:'),
    ({'declared_width': 1000, 'declared_height': 1000}, 'max_pixels:'),
])
def test_metadata_policy_prevents_http_and_unknown_metadata_defers(tmp_path, monkeypatch, metadata, reason):
    calls = network(monkeypatch, pixels())
    policy = dict(max_bytes=1000000, max_pixels=10000, allowed_mime_types=['image/png'])
    request = {'request_id': 'r', 'url': 'https://images.example/a', **metadata}
    result = execute(session(tmp_path, **policy), [request])[0]['result']
    assert result['status'] == 'filtered' and result['filter_stage'] == 'declared'
    assert result['reason'].startswith(reason) and not calls
    # Changed metadata must not replay the previous filter result.
    request = {'request_id': 'r', 'url': 'https://images.example/a', 'mime_type': 'application/octet-stream'}
    result = execute(session(tmp_path, **policy), [request])[0]['result']
    assert result['status'] == 'ok' and len(calls) == 1


def test_actual_format_checks_policy_even_if_source_metadata_claims_allowed(tmp_path, monkeypatch):
    network(monkeypatch, pixels())
    result = execute(session(tmp_path, allowed_mime_types=['image/jpeg']), [
        {'request_id': 'r', 'url': 'https://images.example/a', 'mime_type': 'image/jpeg'}])[0]['result']
    assert result['status'] == 'filtered' and result['filter_stage'] == 'decoded'
    assert result['content_type'] == 'image/png'


def test_header_rejection_stops_stream_closes_connection_and_does_not_retry(tmp_path, monkeypatch):
    reads, closes, requests = [], [], []

    class Stream(httpx.AsyncByteStream):
        async def __aiter__(self):
            reads.append('head')
            head = pixels()
            yield head + b'x' * (16384 - len(head))
            reads.append('tail')
            yield b'x' * 16384

        async def aclose(self):
            closes.append(True)

    def handler(request):
        requests.append(str(request.url))
        if request.url.path == '/redirect':
            return httpx.Response(302, headers={'location': '/small'}, stream=httpx.ByteStream(b''))
        return httpx.Response(200, stream=Stream())

    def client(self, search=False):
        if self.client is None:
            self.client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
        return self.client

    monkeypatch.setattr(WebClient, '_client', client)
    web = session(tmp_path, filters={'min_long_side': 1024})
    web.options['retries'] = 2
    request = {'request_id': 'r', 'url': 'https://images.example/redirect',
               'declared_width': 5630, 'declared_height': 4000}
    result = execute(web, [request])[0]['result']
    assert result['status'] == 'filtered' and result['filter_stage'] == 'header'
    assert (result['width'], result['height']) == (8, 6) and result['image_ref'] is None
    assert result['final_url'] == 'https://images.example/small'
    assert result['attempts'][0]['http_status'] == 200 and len(result['attempts']) == 1
    assert reads == ['head'] and closes == [True] and len(requests) == 2
    assert execute(web, [request])[0]['result'] == result and len(requests) == 2
    assert not list((tmp_path / 'objects').glob('*/*'))


def test_gzip_header_and_http_errors_are_distinguished(tmp_path, monkeypatch):
    calls = []

    def handler(request):
        calls.append(str(request.url))
        code = 404 if request.url.path == '/missing' else 200
        return httpx.Response(code, stream=httpx.ByteStream(gzip.compress(pixels())), headers={'content-encoding': 'gzip'})

    def client(self, search=False):
        if self.client is None:
            self.client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
        return self.client

    monkeypatch.setattr(WebClient, '_client', client)
    requests = [{'request_id': path, 'url': 'https://images.example/' + path} for path in ('small', 'missing')]
    results = execute(session(tmp_path, filters={'min_long_side': 1024}), requests)
    assert results[0]['result']['filter_stage'] == 'header'
    assert results[1]['result']['status'] == 'http_error' and len(calls) == 2


def test_unknown_header_is_checked_after_full_decode(tmp_path, monkeypatch):
    stream = io.BytesIO()
    Image.new('RGB', (8, 6), 'red').save(stream, format='TIFF')
    calls = network(monkeypatch, stream.getvalue())
    request = {'request_id': 'r', 'url': 'https://images.example/a.tiff'}
    result = execute(session(tmp_path, filters={'min_long_side': 1024}), [request])[0]['result']
    assert result['status'] == 'filtered' and result['filter_stage'] == 'decoded'
    assert result['width'] == 8 and result['image_ref'] is None and len(calls) == 1
    # A rejection is not a successful URL index entry.
    assert session(tmp_path).image_library.lookup(request['url']) is None


def test_local_reuse_rechecks_filters_without_trusting_declared_size(tmp_path, monkeypatch):
    calls = network(monkeypatch, pixels())
    request = {'request_id': 'r', 'url': 'https://images.example/a'}
    original = execute(session(tmp_path), [request])[0]['result']
    stricter = execute(session(tmp_path, filters={'min_long_side': 1024}), [request])[0]['result']
    assert stricter['status'] == 'filtered' and stricter['filter_stage'] == 'decoded'
    assert stricter['origin'] == 'local' and stricter['attempts'] == [] and len(calls) == 1
    request.update(declared_width=1, declared_height=1)
    accepted = execute(session(tmp_path, filters={'min_long_side': 8}), [request])[0]['result']
    assert accepted['status'] == 'ok' and accepted['image_ref'] == original['image_ref'] and len(calls) == 1
    # Opting out retains the original receipt and original bytes.
    request.pop('declared_width'); request.pop('declared_height')
    assert execute(session(tmp_path), [request])[0]['result'] == original


def test_concurrent_filter_state_is_per_response(tmp_path, monkeypatch):
    def handler(request):
        size = (8, 6) if request.url.path == '/small' else (16, 12)
        return httpx.Response(200, stream=httpx.ByteStream(pixels(size)))

    def client(self, search=False):
        if self.client is None:
            self.client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
        return self.client

    monkeypatch.setattr(WebClient, '_client', client)

    async def run():
        web = session(tmp_path, filters={'min_long_side': 16})
        try:
            return await asyncio.gather(*(web.fetch_image({'url': 'https://images.example/' + path})
                                          for path in ('small', 'large')))
        finally:
            await web.aclose()

    results = asyncio.run(run())
    assert [result['status'] for result in results] == ['filtered', 'ok']
    assert [result['width'] for result in results] == [8, 16]


@pytest.mark.parametrize('static_code,expected_routes', [(200, ['static']), (503, ['static', 'session'])])
async def test_filtered_response_keeps_routes_healthy_and_survives_fallback(tmp_path, monkeypatch, static_code, expected_routes):
    from test_fetch_session_fallback import install, routes
    from demiflow.collect.session import WebSession
    from demiflow.collect.image_library import ImageLibrary
    monkeypatch.setenv('FETCH_TEST_BASE', 'http://fixture:password@proxy.example:3128')
    calls = install(monkeypatch, static=static_code, payload=pixels())
    web = WebSession(cache_path=tmp_path/'run.sqlite', object_directory=str(tmp_path/'objects'),
        image_library=ImageLibrary(str(tmp_path/'objects'), str(tmp_path/'index.sqlite')),
        image_policy={'filters': {'min_long_side': 1024}},
        retries=0, host_interval_s=0, fetch_proxy_routes=routes())
    try:
        result = await web.fetch_image({'url': 'https://images.example/a'})
        assert result['status'] == 'filtered' and result['filter_stage'] == 'header'
        assert calls == expected_routes
        transport = web.image_client.web
        if static_code == 200:
            assert not transport.fetch_session_pools
            with transport._db() as db:
                health = db.execute('SELECT failures,status FROM fetch_route_health').fetchall()
            assert health == [(0, 'ok')]
        else:
            assert [r['route_kind'] for r in result['attempts']] == ['static', 'session']
            assert result['attempts'][-1]['status'] == 'filtered'
            pool = transport.fetch_session_pools['*']
            assert pool.metrics['created'] == 1 and all(not route['bad'] for route in pool.slots)
    finally:
        await web.aclose()


async def test_cancel_and_owner_close_release_unknown_header_stream(tmp_path, monkeypatch):
    entered, closed = asyncio.Event(), asyncio.Event()

    class Stream(httpx.AsyncByteStream):
        async def __aiter__(self):
            yield b'unknown header'.ljust(16384, b'x')
            entered.set()
            await asyncio.Event().wait()

        async def aclose(self):
            closed.set()

    def client(self, search=False):
        if self.client is None:
            self.client = httpx.AsyncClient(transport=httpx.MockTransport(
                lambda request: httpx.Response(200, stream=Stream())))
        return self.client

    monkeypatch.setattr(WebClient, '_client', client)
    web = session(tmp_path, concurrency=1, filters={'min_long_side': 1024})
    task = asyncio.create_task(web.fetch_image({'url': 'https://images.example/cancel'}))
    try:
        await asyncio.wait_for(entered.wait(), 2)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
        # In-flight URL requests are shared and shielded from an individual
        # waiter. Dataset owner teardown closes the session and cancels them.
        image_client = web.image_client
        await web.aclose()
        assert closed.is_set()
        async with asyncio.timeout(1):
            async with image_client.gate, web.image_library.claim('https://images.example/cancel'):
                pass
    finally:
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)
        await web.aclose()
